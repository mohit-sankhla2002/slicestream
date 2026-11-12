//! The ingest pipeline shared by every transport (DIMSE, STOW-RS, import).
//!
//! Order per instance: spool → parse header → validate → commit blob (rename +
//! directory fsync) → index transaction → acknowledge → broadcast. A blob may
//! therefore exist without an index row (an orphan, removed by
//! [`Ingestor::recover`]), but an index row never exists without its blob.
//!
//! One process owns a storage root: [`Ingestor::open`] takes an exclusive
//! advisory lock on `<root>/LOCK` and holds it for the life of the
//! [`Ingestor`]. Every other safety mechanism here (the per-UID stripe locks,
//! [`Ingestor::recover`]) relies on that.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dicom_core::dictionary::{UidDictionary, UidType};
use dicom_dictionary_std::sop_class;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, broadcast};
use tracing::{error, warn};

use crate::{
    BlobHash, DuplicatePolicy, FsBlobStore, HeaderError, Index, InstanceHeader, InstanceRecord,
    SpoolError, SpooledBlob, StorageConfig, TempBlob, UpsertOutcome, read_header,
};

/// Capacity of the [`StoredEvent`] broadcast channel.
const EVENT_CAPACITY: usize = 1024;
/// Number of per-SOP-Instance-UID lock stripes.
const LOCK_STRIPES: usize = 64;
/// Chunk size used by [`Ingestor::import_file`].
const IMPORT_CHUNK: usize = 64 * 1024;
/// Lock file under the storage root, held exclusively by the owning process.
const LOCK_FILE: &str = "LOCK";

/// Where an instance came from. Recorded in the index `source` column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestSource {
    Dimse {
        calling_ae: String,
        called_ae: String,
    },
    Http {
        remote: Option<SocketAddr>,
    },
    Import {
        path: PathBuf,
    },
}

impl IngestSource {
    /// Value stored in the index `source` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            IngestSource::Dimse { .. } => "dimse",
            IngestSource::Http { .. } => "http",
            IngestSource::Import { .. } => "import",
        }
    }
}

/// Values the transport already knows (C-STORE command, STOW URL) that the
/// dataset must match. `None` means "not checked".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Expectations {
    pub sop_class_uid: Option<String>,
    pub sop_instance_uid: Option<String>,
    pub study_instance_uid: Option<String>,
}

/// A successfully stored (or already present) instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    pub sop_class_uid: String,
    pub sop_instance_uid: String,
    pub study_instance_uid: String,
    pub series_instance_uid: String,
    /// The blob the index now references for this instance.
    pub blob: BlobHash,
    /// The SOP Instance UID was already indexed; the index was left unchanged.
    pub duplicate: bool,
    /// The SOP Instance UID was already indexed and now points at new bytes.
    pub replaced: bool,
}

/// Broadcast after every successful ingest (including duplicates).
#[derive(Clone, Debug)]
pub struct StoredEvent {
    pub series_instance_uid: String,
    pub sop_instance_uid: String,
}

/// Result of [`Ingestor::recover`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub tmp_removed: usize,
    pub orphans_removed: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("unparseable DICOM: {0}")]
    Unparseable(String),
    #[error("missing required attribute {0}")]
    MissingAttribute(&'static str),
    #[error("{field} mismatch: expected {expected}, dataset has {actual}")]
    UidMismatch {
        field: &'static str,
        expected: String,
        actual: String,
    },
    #[error("study mismatch: expected {expected}, dataset has {actual}")]
    StudyMismatch { expected: String, actual: String },
    #[error("SOP class {0} is not a storage SOP class")]
    UnsupportedSopClass(String),
    #[error("instance exceeds limit of {limit} bytes")]
    TooLarge { limit: u64 },
    #[error("storage error: {0}")]
    Storage(#[from] io::Error),
    #[error("index error: {0}")]
    Index(#[from] sqlx::Error),
    #[error("storage root {} is in use by another process (lock on {} is held)", root.display(), root.join(LOCK_FILE).display())]
    Locked { root: PathBuf },
}

impl IngestError {
    /// DIMSE C-STORE status (and STOW-RS FailureReason) for this error.
    pub fn status(&self) -> u16 {
        match self {
            IngestError::Unparseable(_) | IngestError::MissingAttribute(_) => 0xC000,
            IngestError::UidMismatch { .. } => 0xA900,
            IngestError::StudyMismatch { .. } => 0xC409,
            IngestError::UnsupportedSopClass(_) => 0x0122,
            IngestError::TooLarge { .. }
            | IngestError::Storage(_)
            | IngestError::Index(_)
            | IngestError::Locked { .. } => 0xA700,
        }
    }
}

impl From<SpoolError> for IngestError {
    fn from(e: SpoolError) -> Self {
        match e {
            SpoolError::TooLarge { limit } => IngestError::TooLarge { limit },
            SpoolError::Io(e) => IngestError::Storage(e),
        }
    }
}

impl From<HeaderError> for IngestError {
    fn from(e: HeaderError) -> Self {
        match e {
            HeaderError::Unparseable(s) => IngestError::Unparseable(s),
            HeaderError::Missing(name) => IngestError::MissingAttribute(name),
        }
    }
}

/// Whether `uid` names a storage SOP class.
///
/// Known standard UIDs are storage classes iff their dictionary entry is a
/// SOP class whose name ends in "Storage", ignoring the standard qualifiers
/// " (Retired)", " - For Presentation", " - For Processing" and " - Trial"
/// (so e.g. Digital X-Ray / Mammography "For Presentation" are accepted).
/// Unknown UIDs (private / vendor classes) are accepted.
pub fn is_storage_sop_class(uid: &str) -> bool {
    let uid = trim_uid(uid);
    match sop_class::registry().by_uid(uid) {
        None => true,
        Some(entry) => matches!(entry.r#type, UidType::SopClass) && is_storage_name(entry.name),
    }
}

fn is_storage_name(name: &str) -> bool {
    let mut n = name.strip_suffix(" (Retired)").unwrap_or(name);
    for qualifier in [" - For Presentation", " - For Processing", " - Trial"] {
        if let Some(rest) = n.strip_suffix(qualifier) {
            n = rest;
            break;
        }
    }
    n.ends_with("Storage")
}

/// UIDs from DIMSE commands may carry trailing NUL / space padding.
fn trim_uid(uid: &str) -> &str {
    uid.trim_end_matches(['\0', ' '])
}

fn validate(h: &InstanceHeader, expect: &Expectations) -> Result<(), IngestError> {
    let uid_checks = [
        ("SOPClassUID", &expect.sop_class_uid, &h.sop_class_uid),
        (
            "SOPInstanceUID",
            &expect.sop_instance_uid,
            &h.sop_instance_uid,
        ),
    ];
    for (field, expected, actual) in uid_checks {
        if let Some(expected) = expected
            && trim_uid(expected) != trim_uid(actual)
        {
            return Err(IngestError::UidMismatch {
                field,
                expected: expected.clone(),
                actual: actual.clone(),
            });
        }
    }
    if let Some(expected) = &expect.study_instance_uid
        && trim_uid(expected) != trim_uid(&h.study_instance_uid)
    {
        return Err(IngestError::StudyMismatch {
            expected: expected.clone(),
            actual: h.study_instance_uid.clone(),
        });
    }
    if !is_storage_sop_class(&h.sop_class_uid) {
        return Err(IngestError::UnsupportedSopClass(h.sop_class_uid.clone()));
    }
    Ok(())
}

/// Run blocking filesystem work off the async runtime.
async fn blocking<T, F>(f: F) -> Result<T, IngestError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    tokio::task::spawn_blocking(f).await.map_err(join_error)
}

fn join_error(e: tokio::task::JoinError) -> IngestError {
    error!(error = %e, "ingest task failed");
    IngestError::Storage(io::Error::other(e))
}

/// The ingest pipeline. Share it as `Arc<Ingestor>`.
#[derive(Debug)]
pub struct Ingestor {
    cfg: StorageConfig,
    shared: Arc<Shared>,
}

/// Create `root` and take an exclusive advisory lock on `root/LOCK`. The lock
/// is released when the returned file is closed. Blocking.
fn lock_root(root: &Path) -> Result<std::fs::File, IngestError> {
    std::fs::create_dir_all(root)?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(LOCK_FILE))?;
    // Called through the trait: newer std has an inherent `File::try_lock`
    // with a different error type, and the MSRV predates it.
    match fs4::FileExt::try_lock(&file) {
        Ok(()) => Ok(file),
        Err(fs4::TryLockError::WouldBlock) => Err(IngestError::Locked {
            root: root.to_path_buf(),
        }),
        Err(fs4::TryLockError::Error(e)) => Err(IngestError::Storage(e)),
    }
}

/// State used by the commit → index → cleanup critical section, which runs
/// in its own task and therefore needs owned (`Arc`) access.
#[derive(Debug)]
struct Shared {
    blobs: FsBlobStore,
    index: Index,
    on_duplicate: DuplicatePolicy,
    events: broadcast::Sender<StoredEvent>,
    /// Serializes commit → upsert → cleanup per SOP Instance UID (striped).
    /// Identical bytes imply the same SOP Instance UID, so every operation
    /// that can create, reference or delete a given blob is serialized here;
    /// without it, a cleanup could delete a blob that a concurrent ingest of
    /// the same instance has just committed and is about to index.
    ///
    /// These locks are per process. They are sufficient only because this
    /// process is the sole writer of the storage root (see `_root_lock`).
    locks: Box<[Mutex<()>]>,
    /// Exclusive lock on `<root>/LOCK`, held for the life of the ingestor.
    _root_lock: std::fs::File,
    #[cfg(test)]
    hooks: TestHooks,
}

/// Fault-injection and pause points for deterministic unit tests.
#[cfg(test)]
#[derive(Debug, Default)]
struct TestHooks {
    /// Make the next `upsert_instance` call fail.
    fail_next_upsert: std::sync::atomic::AtomicBool,
    /// One-shot pause inside the blob-delete closure: signal `.0`, then block
    /// until `.1` receives.
    pause_before_delete:
        std::sync::Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
}

#[cfg(test)]
impl TestHooks {
    fn upsert_should_fail(&self) -> bool {
        self.fail_next_upsert
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    fn before_delete(&self) {
        let pause = self.pause_before_delete.lock().unwrap().take();
        if let Some((reached, proceed)) = pause {
            let _ = reached.send(());
            let _ = proceed.recv();
        }
    }
}

impl Shared {
    fn lock_for(&self, sop_instance_uid: &str) -> &Mutex<()> {
        let mut h = std::hash::DefaultHasher::new();
        sop_instance_uid.hash(&mut h);
        &self.locks[(h.finish() % self.locks.len() as u64) as usize]
    }

    /// The critical section of [`Ingestor::ingest`]. Runs in its own task so
    /// that dropping the caller cannot interrupt it between steps.
    async fn store(
        self: Arc<Self>,
        spooled: SpooledBlob,
        mut rec: InstanceRecord,
    ) -> Result<Stored, IngestError> {
        let _guard = self.lock_for(&rec.header.sop_instance_uid).lock().await;

        let blobs = self.blobs.clone();
        let new_blob = blocking(move || blobs.commit(spooled)).await??;
        rec.blob = new_blob;

        #[cfg(test)]
        let forced_failure = self.hooks.upsert_should_fail();
        #[cfg(not(test))]
        let forced_failure = false;
        let upserted = if forced_failure {
            Err(sqlx::Error::PoolTimedOut)
        } else {
            self.index.upsert_instance(&rec, self.on_duplicate).await
        };
        let outcome = match upserted {
            Ok(o) => o,
            Err(e) => {
                self.discard_unreferenced(new_blob).await;
                return Err(IngestError::Index(e));
            }
        };

        let (blob, duplicate, replaced) = match outcome {
            UpsertOutcome::Inserted => (new_blob, false, false),
            UpsertOutcome::Duplicate { existing } => {
                if existing != new_blob {
                    self.discard_unreferenced(new_blob).await;
                }
                (existing, true, false)
            }
            UpsertOutcome::Replaced { previous } => {
                if previous != new_blob {
                    self.discard_unreferenced(previous).await;
                }
                (new_blob, false, true)
            }
        };

        let h = rec.header;
        // No receivers is fine.
        let _ = self.events.send(StoredEvent {
            series_instance_uid: h.series_instance_uid.clone(),
            sop_instance_uid: h.sop_instance_uid.clone(),
        });
        Ok(Stored {
            sop_class_uid: h.sop_class_uid,
            sop_instance_uid: h.sop_instance_uid,
            study_instance_uid: h.study_instance_uid,
            series_instance_uid: h.series_instance_uid,
            blob,
            duplicate,
            replaced,
        })
    }

    /// Delete `hash` unless an index row references it. Failures are logged,
    /// not returned: the instance outcome is already decided, and any blob
    /// left behind is an orphan that [`Ingestor::recover`] removes.
    async fn discard_unreferenced(self: &Arc<Self>, hash: BlobHash) {
        match self.index.blob_in_use(&hash).await {
            Ok(false) => {
                let shared = Arc::clone(self);
                match blocking(move || {
                    #[cfg(test)]
                    shared.hooks.before_delete();
                    shared.blobs.delete(&hash)
                })
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => warn!(blob = %hash.to_hex(), error = %e, "failed to delete blob"),
                    Err(e) => warn!(blob = %hash.to_hex(), error = %e, "failed to delete blob"),
                }
            }
            Ok(true) => {}
            Err(e) => {
                warn!(blob = %hash.to_hex(), error = %e, "cannot check blob use; keeping it");
            }
        }
    }
}

impl Ingestor {
    /// Lock the storage root, then open the blob store at `cfg.root` and the
    /// index at `cfg.root/index.sqlite`, creating both if missing.
    ///
    /// Fails with [`IngestError::Locked`] if another process (or another
    /// `Ingestor` in this process) already owns the root. The lock is held
    /// until the `Ingestor` is dropped.
    pub async fn open(cfg: StorageConfig) -> Result<Arc<Ingestor>, IngestError> {
        let root = cfg.root.clone();
        let (root_lock, blobs) = blocking(move || -> Result<_, IngestError> {
            let lock = lock_root(&root)?;
            Ok((lock, FsBlobStore::open(&root)?))
        })
        .await??;
        let index = Index::open(&cfg.root.join("index.sqlite")).await?;
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        let locks = (0..LOCK_STRIPES).map(|_| Mutex::new(())).collect();
        let shared = Arc::new(Shared {
            blobs,
            index,
            on_duplicate: cfg.on_duplicate,
            events,
            locks,
            _root_lock: root_lock,
            #[cfg(test)]
            hooks: TestHooks::default(),
        });
        Ok(Arc::new(Ingestor { cfg, shared }))
    }

    /// Start a new spool file limited to `max_instance_bytes`.
    ///
    /// If a `write_chunk` fails, drop the `TempBlob` (which removes its file);
    /// do not keep writing to it.
    pub fn create_temp(&self) -> Result<TempBlob, IngestError> {
        Ok(self.shared.blobs.create_temp(self.cfg.max_instance_bytes)?)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<StoredEvent> {
        self.shared.events.subscribe()
    }

    pub fn index(&self) -> &Index {
        &self.shared.index
    }

    pub fn blobs(&self) -> &FsBlobStore {
        &self.shared.blobs
    }

    pub fn config(&self) -> &StorageConfig {
        &self.cfg
    }

    /// Validate, commit and index a spooled instance.
    ///
    /// Returns only after the blob is renamed into place, its directory is
    /// fsynced and the index transaction has committed, so `Ok` may be
    /// acknowledged to the sender. On error the spool file is removed and no
    /// index row is written.
    ///
    /// Cancellation-safe: once validation passes, commit → index → cleanup →
    /// broadcast runs in a spawned task that completes (holding the per-UID
    /// lock throughout) even if this future is dropped.
    pub async fn ingest(
        &self,
        spooled: SpooledBlob,
        source: IngestSource,
        expect: Expectations,
    ) -> Result<Stored, IngestError> {
        let (spooled, header) = blocking(move || {
            let header = read_header(spooled.path());
            (spooled, header)
        })
        .await?;
        let header = header?;
        validate(&header, &expect)?;

        let calling_ae = match &source {
            IngestSource::Dimse { calling_ae, .. } => Some(calling_ae.clone()),
            _ => None,
        };
        let rec = InstanceRecord {
            header,
            blob: spooled.hash,
            size_bytes: spooled.len,
            source: source.as_str().to_owned(),
            calling_ae,
        };
        let shared = Arc::clone(&self.shared);
        tokio::spawn(shared.store(spooled, rec))
            .await
            .map_err(join_error)?
    }

    /// Startup-only crash recovery: remove every file in `tmp/`, then delete
    /// committed blobs that no index row references.
    ///
    /// Call this before accepting any writes. Running it concurrently with
    /// [`Ingestor::ingest`] could remove an in-flight spool file or a blob
    /// that has been committed but not yet indexed. No other process can be
    /// writing: the root lock taken by [`Ingestor::open`] excludes it.
    pub async fn recover(&self) -> Result<RecoveryReport, IngestError> {
        let blobs = self.shared.blobs.clone();
        let tmp_removed = blocking(move || blobs.sweep_tmp()).await??;
        let referenced: HashSet<BlobHash> = self.shared.index.blob_hashes().await?;
        let blobs = self.shared.blobs.clone();
        let orphans_removed = blocking(move || -> io::Result<usize> {
            let mut n = 0;
            for hash in blobs.list()? {
                if !referenced.contains(&hash) {
                    blobs.delete(&hash)?;
                    n += 1;
                }
            }
            Ok(n)
        })
        .await??;
        Ok(RecoveryReport {
            tmp_removed,
            orphans_removed,
        })
    }

    /// Ingest an existing Part-10 file by streaming a copy through a
    /// [`TempBlob`]. The source file is never modified.
    pub async fn import_file(&self, path: &Path) -> Result<Stored, IngestError> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut temp = self.create_temp()?;
        let mut buf = vec![0u8; IMPORT_CHUNK];
        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            // On error `temp` is dropped here, removing its file.
            temp.write_chunk(&buf[..n]).await?;
        }
        let spooled = temp.finish().await?;
        self.ingest(
            spooled,
            IngestSource::Import {
                path: path.to_path_buf(),
            },
            Expectations::default(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn spool(ing: &Ingestor, bytes: &[u8]) -> SpooledBlob {
        let mut t = ing.create_temp().unwrap();
        t.write_chunk(bytes).await.unwrap();
        t.finish().await.unwrap()
    }

    /// Dropping an `ingest` future mid-cleanup must not let a concurrent
    /// ingest of the same bytes index a blob that the cleanup then deletes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_ingest_cannot_delete_blob_indexed_by_another() {
        let d = tempfile::tempdir().unwrap();
        crate::phantom::write_phantom(&d.path().join("src"), 1, 16).unwrap();
        let bytes = std::fs::read(d.path().join("src/IM00001.dcm")).unwrap();
        let ing = Ingestor::open(StorageConfig {
            root: d.path().join("store"),
            max_instance_bytes: 1 << 30,
            on_duplicate: DuplicatePolicy::Keep,
        })
        .await
        .unwrap();

        // A: upsert fails, cleanup decides the new blob is unused and pauses
        // just before deleting it.
        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (proceed_tx, proceed_rx) = std::sync::mpsc::channel();
        *ing.shared.hooks.pause_before_delete.lock().unwrap() = Some((reached_tx, proceed_rx));
        ing.shared
            .hooks
            .fail_next_upsert
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let a = {
            let ing = Arc::clone(&ing);
            let spooled = spool(&ing, &bytes).await;
            tokio::spawn(async move {
                ing.ingest(
                    spooled,
                    IngestSource::Http { remote: None },
                    Expectations::default(),
                )
                .await
            })
        };
        tokio::task::spawn_blocking(move || reached_rx.recv().unwrap())
            .await
            .unwrap();

        // The caller goes away (client disconnect / association abort).
        a.abort();
        assert!(a.await.unwrap_err().is_cancelled());

        // B: same bytes.
        let mut b = {
            let ing = Arc::clone(&ing);
            let spooled = spool(&ing, &bytes).await;
            tokio::spawn(async move {
                ing.ingest(
                    spooled,
                    IngestSource::Http { remote: None },
                    Expectations::default(),
                )
                .await
            })
        };
        // Correct code keeps B waiting behind A's critical section, so this
        // times out; broken code lets B finish first. The assertions below
        // hold for correct code regardless of how long this wait is.
        let early = tokio::time::timeout(Duration::from_millis(200), &mut b).await;
        proceed_tx.send(()).unwrap();
        let stored = match early {
            Ok(r) => r.unwrap().unwrap(),
            Err(_) => b.await.unwrap().unwrap(),
        };

        let indexed = ing
            .index()
            .instance(&stored.sop_instance_uid)
            .await
            .unwrap()
            .expect("B indexed the instance");
        // Give a detached delete (broken code) time to land; correct code has
        // already finished A's delete before B could take the lock.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            ing.blobs().path_of(&indexed.blob).is_file(),
            "index references a blob that no longer exists"
        );
    }
}
