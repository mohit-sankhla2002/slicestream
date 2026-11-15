//! The ingest pipeline shared by every transport (DIMSE, STOW-RS, import).
//!
//! Order per instance: spool → parse header → validate → commit blob (rename +
//! directory fsync) → index transaction → acknowledge → broadcast. A blob may
//! therefore exist without an index row (an orphan, removed by
//! [`Ingestor::recover`]), but an index row never exists without its blob.
//! NIfTI volumes ([`Ingestor::ingest_nifti`]) follow the same order with a
//! `nifti_volumes` row in place of the `instances` row.
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
    BlobHash, BlobKind, DuplicatePolicy, FsBlobStore, HeaderError, Index, InstanceHeader,
    InstanceRecord, InstanceScan, NiftiMeta, SpoolError, SpooledBlob, StorageConfig, TempBlob,
    UpsertOutcome, scan_instance,
};

/// Capacity of the [`StoredEvent`] broadcast channel.
const EVENT_CAPACITY: usize = 1024;
/// Number of per-SOP-Instance-UID lock stripes.
const LOCK_STRIPES: usize = 64;
/// Chunk size used by [`Ingestor::import_file`].
const IMPORT_CHUNK: usize = 64 * 1024;
/// How many backfill scans run at once.
const BACKFILL_PARALLELISM: usize = 2;
/// Scan problem recorded when the scan panicked. The details are logged.
const SCAN_FAILED: &str = "the scan failed";
/// Scan problem recorded when the scan result could not be written. The
/// details are logged.
const SCAN_NOT_RECORDED: &str = "the scan could not be recorded";
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

/// Broadcast after every successful ingest (including duplicates), and
/// after an instance stored earlier is scanned (in the background or on
/// demand), since its frame table is new then.
#[derive(Clone, Debug)]
pub struct StoredEvent {
    pub series_instance_uid: String,
    pub sop_instance_uid: String,
}

/// A successfully stored (or already present) NIfTI volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredNifti {
    /// `nifti_volumes` row id.
    pub id: i64,
    /// The same bytes were already indexed; the existing row is unchanged.
    pub duplicate: bool,
    pub blob: BlobHash,
}

/// Result of [`Ingestor::recover`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub tmp_removed: usize,
    pub orphans_removed: usize,
    /// `uploads` rows that were still `running` and are now `aborted`.
    pub uploads_aborted: u64,
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

/// An [`IngestError`] plus the instance's SOP Class / Instance UIDs when the
/// header had been parsed before the failure (for STOW-RS
/// FailedSOPSequence items and logs).
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct IngestFailure {
    pub error: IngestError,
    pub sop_class_uid: Option<String>,
    pub sop_instance_uid: Option<String>,
}

impl IngestFailure {
    /// See [`IngestError::status`].
    pub fn status(&self) -> u16 {
        self.error.status()
    }

    fn for_instance(error: IngestError, h: &InstanceHeader) -> Self {
        IngestFailure {
            error,
            sop_class_uid: Some(h.sop_class_uid.clone()),
            sop_instance_uid: Some(h.sop_instance_uid.clone()),
        }
    }
}

impl From<IngestError> for IngestFailure {
    fn from(error: IngestError) -> Self {
        IngestFailure {
            error,
            sop_class_uid: None,
            sop_instance_uid: None,
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

/// A running [`Shared::scan_row`].
struct ScanTask {
    id: i64,
    blob: BlobHash,
    task: tokio::task::JoinHandle<Result<(), IngestError>>,
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
    stats: crate::stats::Stats,
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
    /// Instance ids whose blocking scan panics.
    panic_in_scan: std::sync::Mutex<Vec<i64>>,
    /// Instance ids whose scan task panics outside the blocking scan.
    panic_in_task: std::sync::Mutex<Vec<i64>>,
}

#[cfg(test)]
impl TestHooks {
    fn upsert_should_fail(&self) -> bool {
        self.fail_next_upsert
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    fn panics(list: &std::sync::Mutex<Vec<i64>>, id: i64) -> bool {
        list.lock().unwrap().contains(&id)
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
        let new_blob = blocking(move || blobs.commit(spooled, BlobKind::Dicom)).await??;
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
                self.discard_unreferenced(new_blob, BlobKind::Dicom).await;
                return Err(IngestError::Index(e));
            }
        };

        let (blob, duplicate, replaced) = match outcome {
            UpsertOutcome::Inserted => (new_blob, false, false),
            UpsertOutcome::Duplicate { existing } => {
                if existing != new_blob {
                    self.discard_unreferenced(new_blob, BlobKind::Dicom).await;
                }
                (existing, true, false)
            }
            UpsertOutcome::Replaced { previous } => {
                if previous != new_blob {
                    self.discard_unreferenced(previous, BlobKind::Dicom).await;
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

    /// Scan instance `id` (currently `blob`) and record the result, under
    /// the per-UID lock so it serializes with an overwrite of the same
    /// instance. Writes nothing if the row's blob changed. A recorded scan is
    /// announced as a [`StoredEvent`], so caches built from the row (the
    /// series catalog) pick up its frame table.
    ///
    /// Any failure for this row (a blob that is missing or unparseable, a
    /// panicking scan, a scan result the index will not take) is recorded as
    /// a scan problem, so the row is not retried. `Err` only when even that
    /// cannot be written.
    async fn scan_row(
        self: Arc<Self>,
        id: i64,
        uid: String,
        blob: BlobHash,
    ) -> Result<(), IngestError> {
        let _guard = self.lock_for(&uid).lock().await;
        #[cfg(test)]
        if TestHooks::panics(&self.hooks.panic_in_task, id) {
            panic!("injected scan task panic");
        }
        #[cfg(test)]
        let panic_in_scan = TestHooks::panics(&self.hooks.panic_in_scan, id);
        let path = self.blobs.path_of(&blob, BlobKind::Dicom);
        let scan = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if panic_in_scan {
                panic!("injected scan panic");
            }
            scan_instance(&path)
        })
        .await;
        let problem = match scan {
            Ok(Ok(scan)) => match self.index.write_scan(id, &blob, &scan).await {
                Ok(written) => {
                    if written {
                        // No receivers is fine.
                        let _ = self.events.send(StoredEvent {
                            series_instance_uid: scan.header.series_instance_uid,
                            sop_instance_uid: scan.header.sop_instance_uid,
                        });
                    }
                    return Ok(());
                }
                Err(e) => {
                    error!(id, blob = %blob.to_hex(), error = %e, "cannot record scan");
                    SCAN_NOT_RECORDED.to_owned()
                }
            },
            Ok(Err(e)) => format!("cannot scan stored blob: {e}"),
            Err(e) => {
                error!(id, blob = %blob.to_hex(), error = %e, "scan task failed");
                SCAN_FAILED.to_owned()
            }
        };
        warn!(id, blob = %blob.to_hex(), %problem, "recording scan failure");
        self.index.write_scan_failure(id, &blob, &problem).await?;
        Ok(())
    }

    /// Start [`Shared::scan_row`] in its own task.
    fn spawn_scan(self: &Arc<Self>, id: i64, uid: String, blob: BlobHash) -> ScanTask {
        ScanTask {
            id,
            blob,
            task: tokio::spawn(Arc::clone(self).scan_row(id, uid, blob)),
        }
    }

    /// Wait for a scan task. A panic in it (outside the scan itself) is
    /// recorded as a scan problem like any other failure of the row.
    async fn finish_scan(self: &Arc<Self>, scan: ScanTask) -> Result<(), IngestError> {
        let ScanTask { id, blob, task } = scan;
        match task.await {
            Ok(result) => result,
            Err(e) => {
                error!(id, blob = %blob.to_hex(), error = %e, "scan task failed");
                self.index
                    .write_scan_failure(id, &blob, SCAN_FAILED)
                    .await?;
                Ok(())
            }
        }
    }

    /// The critical section of [`Ingestor::ingest_nifti`]: commit → index →
    /// cleanup, serialized per blob hash (identical bytes, identical hash).
    /// Runs in its own task so that dropping the caller cannot interrupt it.
    async fn store_nifti(
        self: Arc<Self>,
        spooled: SpooledBlob,
        meta: NiftiMeta,
        upload_id: i64,
    ) -> Result<StoredNifti, IngestError> {
        let hash = spooled.hash;
        let size = spooled.len;
        let kind = if meta.compressed {
            BlobKind::NiftiGz
        } else {
            BlobKind::Nifti
        };
        let _guard = self.lock_for(&hash.to_hex()).lock().await;

        let blobs = self.blobs.clone();
        let blob = blocking(move || blobs.commit(spooled, kind)).await??;
        match self.index.insert_nifti(&blob, size, &meta, upload_id).await {
            Ok((id, duplicate)) => Ok(StoredNifti {
                id,
                duplicate,
                blob,
            }),
            Err(e) => {
                self.discard_unreferenced(blob, kind).await;
                Err(IngestError::Index(e))
            }
        }
    }

    /// Delete blob `hash` of `kind` unless an index row (either table)
    /// references the hash. Failures are logged, not returned: the outcome is
    /// already decided, and any blob left behind is an orphan that
    /// [`Ingestor::recover`] removes.
    async fn discard_unreferenced(self: &Arc<Self>, hash: BlobHash, kind: BlobKind) {
        match self.index.blob_in_use(&hash).await {
            Ok(false) => {
                let shared = Arc::clone(self);
                match blocking(move || {
                    #[cfg(test)]
                    shared.hooks.before_delete();
                    shared.blobs.delete(&hash, kind)
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
            stats: Default::default(),
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
    /// index row is written. Errors after the header was parsed carry the
    /// instance's UIDs.
    ///
    /// Cancellation-safe: once validation passes, commit → index → cleanup →
    /// broadcast runs in a spawned task that completes (holding the per-UID
    /// lock throughout) even if this future is dropped.
    pub async fn ingest(
        &self,
        spooled: SpooledBlob,
        source: IngestSource,
        expect: Expectations,
    ) -> Result<Stored, IngestFailure> {
        let start = std::time::Instant::now();
        let len = spooled.len;
        let result = self.ingest_inner(spooled, source, expect).await;
        match &result {
            Ok(stored) => {
                self.shared
                    .stats
                    .record_success(stored.duplicate, len, start.elapsed());
            }
            Err(_) => self.shared.stats.record_failure(),
        }
        result
    }

    /// Copy `reader` into a new spool file (blocking; for use on blocking
    /// threads), limited to `max_instance_bytes`. On error nothing is left in
    /// `tmp/`.
    pub fn spool_sync(&self, reader: &mut dyn io::Read) -> Result<SpooledBlob, IngestError> {
        Ok(self
            .shared
            .blobs
            .spool_sync(reader, self.cfg.max_instance_bytes)?)
    }

    /// Commit and index a spooled NIfTI volume as part of upload `upload_id`.
    ///
    /// The blob is stored exactly as received, as `.nii.gz` if
    /// `meta.compressed`, else `.nii`. Returns only after the blob is renamed
    /// into place, its directories are fsynced and the index transaction has
    /// committed. Identical bytes already indexed are a success with
    /// `duplicate: true` and the existing row's id. On an index error the new
    /// blob is removed if nothing references it.
    ///
    /// Cancellation-safe like [`Ingestor::ingest`]: the critical section runs
    /// in a spawned task.
    pub async fn ingest_nifti(
        &self,
        spooled: SpooledBlob,
        meta: NiftiMeta,
        upload_id: i64,
    ) -> Result<StoredNifti, IngestError> {
        let len = spooled.len;
        let shared = Arc::clone(&self.shared);
        let result = tokio::spawn(shared.store_nifti(spooled, meta, upload_id))
            .await
            .map_err(join_error)
            .and_then(|r| r);
        match &result {
            Ok(stored) => self
                .shared
                .stats
                .record_nifti_success(stored.duplicate, len),
            Err(_) => self.shared.stats.record_failure(),
        }
        result
    }

    /// Snapshot of the ingest counters and 60 s latency percentiles.
    pub fn stats(&self) -> crate::stats::IngestStats {
        self.shared.stats.snapshot()
    }

    async fn ingest_inner(
        &self,
        spooled: SpooledBlob,
        source: IngestSource,
        expect: Expectations,
    ) -> Result<Stored, IngestFailure> {
        let (spooled, scan) = blocking(move || {
            let scan = scan_instance(spooled.path());
            (spooled, scan)
        })
        .await?;
        let InstanceScan { header, data } = scan.map_err(IngestError::from)?;
        validate(&header, &expect).map_err(|e| IngestFailure::for_instance(e, &header))?;
        let (sop_class_uid, sop_instance_uid) = (
            header.sop_class_uid.clone(),
            header.sop_instance_uid.clone(),
        );

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
            data,
            scanned: true,
        };
        let shared = Arc::clone(&self.shared);
        tokio::spawn(shared.store(spooled, rec))
            .await
            .map_err(join_error)
            .and_then(|r| r)
            .map_err(|error| IngestFailure {
                error,
                sop_class_uid: Some(sop_class_uid),
                sop_instance_uid: Some(sop_instance_uid),
            })
    }

    /// Scan up to `limit` instances stored before scanning existed (or at an
    /// older scan version), two at a time. Returns how many rows it recorded;
    /// 0 means nothing is left to scan.
    ///
    /// A row that fails in any way (unreadable blob, panicking scan, a scan
    /// result the index will not take) is recorded as a scan problem, so it
    /// does not come back, and the batch goes on. A row for which even that
    /// cannot be written is logged and left for a later batch. `Err` when
    /// the rows cannot be listed, or when the batch recorded none of its
    /// rows, so the caller can wait before trying again.
    pub async fn backfill_batch(&self, limit: i64) -> Result<usize, IngestError> {
        let rows = self.shared.index.unscanned(limit).await?;
        let mut recorded = 0;
        let mut last_error = None;
        for pair in rows.chunks(BACKFILL_PARALLELISM) {
            let tasks: Vec<ScanTask> = pair
                .iter()
                .map(|(id, uid, blob)| self.shared.spawn_scan(*id, uid.clone(), *blob))
                .collect();
            for task in tasks {
                let id = task.id;
                match self.shared.finish_scan(task).await {
                    Ok(()) => recorded += 1,
                    Err(e) => {
                        error!(id, error = %e, "cannot record the scan; the row is retried later");
                        last_error = Some(e);
                    }
                }
            }
        }
        match last_error {
            Some(e) if recorded == 0 => Err(e),
            _ => Ok(recorded),
        }
    }

    /// Scan instance `id` now unless it is already scanned. Unknown ids are
    /// a no-op.
    pub async fn ensure_scanned(&self, id: i64) -> Result<(), IngestError> {
        let index = &self.shared.index;
        if index.stored_data(id).await?.is_some() {
            return Ok(());
        }
        let Some((uid, blob)) = index.instance_blob(id).await? else {
            return Ok(());
        };
        let task = self.shared.spawn_scan(id, uid, blob);
        self.shared.finish_scan(task).await
    }

    /// Startup-only crash recovery: remove every file in `tmp/`, mark
    /// `running` uploads `aborted`, then delete committed blobs (any kind)
    /// whose hash no `instances` or `nifti_volumes` row references.
    ///
    /// Call this before accepting any writes. Running it concurrently with
    /// [`Ingestor::ingest`] could remove an in-flight spool file or a blob
    /// that has been committed but not yet indexed. No other process can be
    /// writing: the root lock taken by [`Ingestor::open`] excludes it.
    pub async fn recover(&self) -> Result<RecoveryReport, IngestError> {
        let blobs = self.shared.blobs.clone();
        let tmp_removed = blocking(move || blobs.sweep_tmp()).await??;
        let uploads_aborted = self.shared.index.abort_running_uploads().await?;
        // If the index cannot be read completely this fails and nothing is
        // deleted.
        let referenced: HashSet<BlobHash> = self.shared.index.blob_hashes().await?;
        let blobs = self.shared.blobs.clone();
        let orphans_removed = blocking(move || -> io::Result<usize> {
            let mut n = 0;
            for (hash, kind) in blobs.list()? {
                if !referenced.contains(&hash) {
                    blobs.delete(&hash, kind)?;
                    n += 1;
                }
            }
            Ok(n)
        })
        .await??;
        Ok(RecoveryReport {
            tmp_removed,
            orphans_removed,
            uploads_aborted,
        })
    }

    /// Ingest an existing Part-10 file by streaming a copy through a
    /// [`TempBlob`]. The source file is never modified.
    pub async fn import_file(&self, path: &Path) -> Result<Stored, IngestFailure> {
        let mut file = tokio::fs::File::open(path)
            .await
            .map_err(IngestError::from)?;
        let mut temp = self.create_temp()?;
        let mut buf = vec![0u8; IMPORT_CHUNK];
        loop {
            let n = file.read(&mut buf).await.map_err(IngestError::from)?;
            if n == 0 {
                break;
            }
            // On error `temp` is dropped here, removing its file.
            temp.write_chunk(&buf[..n])
                .await
                .map_err(IngestError::from)?;
        }
        let spooled = temp.finish().await.map_err(IngestError::from)?;
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
            ing.blobs()
                .path_of(&indexed.blob, BlobKind::Dicom)
                .is_file(),
            "index references a blob that no longer exists"
        );
    }

    /// A scan that panics, or whose task panics, is recorded as a problem;
    /// the rest of the batch is still scanned.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backfill_records_panicked_scans() {
        let d = tempfile::tempdir().unwrap();
        let src = d.path().join("src");
        crate::phantom::write_phantom(&src, 3, 16).unwrap();
        let ing = Ingestor::open(StorageConfig {
            root: d.path().join("store"),
            max_instance_bytes: 1 << 30,
            on_duplicate: DuplicatePolicy::Keep,
        })
        .await
        .unwrap();
        for i in 1..=3 {
            ing.import_file(&src.join(format!("IM{i:05}.dcm")))
                .await
                .unwrap();
        }
        let pool = &ing.shared.index.pool;
        for q in [
            "DELETE FROM instance_data",
            "UPDATE instances SET scan_version = 0",
        ] {
            sqlx::query(q).execute(pool).await.unwrap();
        }
        let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM instances ORDER BY id")
            .fetch_all(pool)
            .await
            .unwrap();
        ing.shared.hooks.panic_in_scan.lock().unwrap().push(ids[0]);
        ing.shared.hooks.panic_in_task.lock().unwrap().push(ids[1]);

        assert_eq!(ing.backfill_batch(10).await.unwrap(), 3);
        assert!(ing.index().unscanned(10).await.unwrap().is_empty());
        let problem = |id: i64| {
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT scan_problem FROM instances WHERE id = ?1",
            )
            .bind(id)
            .fetch_one(pool)
        };
        // The panic message is logged, not stored.
        let failed = Some("the scan failed".to_owned());
        assert_eq!(problem(ids[0]).await.unwrap(), failed);
        assert_eq!(problem(ids[1]).await.unwrap(), failed);
        assert_eq!(problem(ids[2]).await.unwrap(), None);
        let data = ing.index().stored_data(ids[2]).await.unwrap().unwrap();
        assert!(data.frames.is_some());
        // ensure_scanned on a panicking row records it too.
        sqlx::query("UPDATE instances SET scan_version = 0 WHERE id = ?1")
            .bind(ids[0])
            .execute(pool)
            .await
            .unwrap();
        ing.ensure_scanned(ids[0]).await.unwrap();
        assert_eq!(problem(ids[0]).await.unwrap(), failed);
    }
}
