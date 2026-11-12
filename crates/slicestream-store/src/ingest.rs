//! The ingest pipeline shared by every transport (DIMSE, STOW-RS, import).
//!
//! Order per instance: spool → parse header → validate → commit blob (rename +
//! directory fsync) → index transaction → acknowledge → broadcast. A blob may
//! therefore exist without an index row (an orphan, removed by
//! [`Ingestor::recover`]), but an index row never exists without its blob.

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
use tracing::warn;

use crate::{
    BlobHash, FsBlobStore, HeaderError, Index, InstanceHeader, InstanceRecord, SpoolError,
    SpooledBlob, StorageConfig, TempBlob, UpsertOutcome, read_header,
};

/// Capacity of the [`StoredEvent`] broadcast channel.
const EVENT_CAPACITY: usize = 1024;
/// Number of per-SOP-Instance-UID lock stripes.
const LOCK_STRIPES: usize = 64;
/// Chunk size used by [`Ingestor::import_file`].
const IMPORT_CHUNK: usize = 64 * 1024;

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
}

impl IngestError {
    /// DIMSE C-STORE status (and STOW-RS FailureReason) for this error.
    pub fn status(&self) -> u16 {
        match self {
            IngestError::Unparseable(_) | IngestError::MissingAttribute(_) => 0xC000,
            IngestError::UidMismatch { .. } => 0xA900,
            IngestError::StudyMismatch { .. } => 0xC409,
            IngestError::UnsupportedSopClass(_) => 0x0122,
            IngestError::TooLarge { .. } | IngestError::Storage(_) | IngestError::Index(_) => {
                0xA700
            }
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
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| IngestError::Storage(io::Error::other(e)))
}

/// The ingest pipeline. Share it as `Arc<Ingestor>`.
#[derive(Debug)]
pub struct Ingestor {
    cfg: StorageConfig,
    blobs: FsBlobStore,
    index: Index,
    events: broadcast::Sender<StoredEvent>,
    /// Serializes commit → upsert → cleanup per SOP Instance UID (striped).
    /// Identical bytes imply the same SOP Instance UID, so every operation
    /// that can create, reference or delete a given blob is serialized here;
    /// without it, a cleanup could delete a blob that a concurrent ingest of
    /// the same instance has just committed and is about to index.
    locks: Box<[Mutex<()>]>,
}

impl Ingestor {
    /// Open the blob store at `cfg.root` and the index at
    /// `cfg.root/index.sqlite`, creating both if missing.
    pub async fn open(cfg: StorageConfig) -> Result<Arc<Ingestor>, IngestError> {
        let root = cfg.root.clone();
        let blobs = blocking(move || FsBlobStore::open(&root)).await??;
        let index = Index::open(&cfg.root.join("index.sqlite")).await?;
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        let locks = (0..LOCK_STRIPES).map(|_| Mutex::new(())).collect();
        Ok(Arc::new(Ingestor {
            cfg,
            blobs,
            index,
            events,
            locks,
        }))
    }

    /// Start a new spool file limited to `max_instance_bytes`.
    ///
    /// If a `write_chunk` fails, drop the `TempBlob` (which removes its file);
    /// do not keep writing to it.
    pub fn create_temp(&self) -> Result<TempBlob, IngestError> {
        Ok(self.blobs.create_temp(self.cfg.max_instance_bytes)?)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<StoredEvent> {
        self.events.subscribe()
    }

    pub fn index(&self) -> &Index {
        &self.index
    }

    pub fn blobs(&self) -> &FsBlobStore {
        &self.blobs
    }

    pub fn config(&self) -> &StorageConfig {
        &self.cfg
    }

    fn lock_for(&self, sop_instance_uid: &str) -> &Mutex<()> {
        let mut h = std::hash::DefaultHasher::new();
        sop_instance_uid.hash(&mut h);
        &self.locks[(h.finish() % self.locks.len() as u64) as usize]
    }

    /// Validate, commit and index a spooled instance.
    ///
    /// Returns only after the blob is renamed into place, its directory is
    /// fsynced and the index transaction has committed, so `Ok` may be
    /// acknowledged to the sender. On error the spool file is removed and no
    /// index row is written.
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

        let size_bytes = spooled.len;
        let _guard = self.lock_for(&header.sop_instance_uid).lock().await;

        let blobs = self.blobs.clone();
        let new_blob = blocking(move || blobs.commit(spooled)).await??;

        let calling_ae = match &source {
            IngestSource::Dimse { calling_ae, .. } => Some(calling_ae.clone()),
            _ => None,
        };
        let rec = InstanceRecord {
            header,
            blob: new_blob,
            size_bytes,
            source: source.as_str().to_owned(),
            calling_ae,
        };
        let outcome = match self
            .index
            .upsert_instance(&rec, self.cfg.on_duplicate)
            .await
        {
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
    async fn discard_unreferenced(&self, hash: BlobHash) {
        match self.index.blob_in_use(&hash).await {
            Ok(false) => {
                let blobs = self.blobs.clone();
                match blocking(move || blobs.delete(&hash)).await {
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

    /// Startup-only crash recovery: remove every file in `tmp/`, then delete
    /// committed blobs that no index row references.
    ///
    /// Call this before accepting any writes. Running it concurrently with
    /// [`Ingestor::ingest`] could remove an in-flight spool file or a blob
    /// that has been committed but not yet indexed.
    pub async fn recover(&self) -> Result<RecoveryReport, IngestError> {
        let blobs = self.blobs.clone();
        let tmp_removed = blocking(move || blobs.sweep_tmp()).await??;
        let referenced: HashSet<BlobHash> = self.index.blob_hashes().await?;
        let blobs = self.blobs.clone();
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
