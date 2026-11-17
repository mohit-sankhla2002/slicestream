//! Derived HTJ2K copies: the per-frame location table of a copy blob and the
//! state recorded for each instance.

use std::path::PathBuf;
use std::sync::Arc;

use slicestream_codec::{CodecError, Encoded, eligibility, encode_verified};
use slicestream_core::FrameInfo;
use slicestream_core::frame::read_frame_at;
use tracing::{error, warn};

use crate::frames::ByteReader;
use crate::ingest::{IngestError, Shared, rejects_row};
use crate::{BlobHash, BlobKind, FrameTable, Ingestor, StoredDataState};

/// Capacity of the priority queue of [`Ingestor::prioritize_copy`].
const PRIORITY_CAPACITY: usize = 1024;
/// Recorded when the encoder panicked. The details are logged.
const ENCODER_FAILED: &str = "the encoder failed";
/// Recorded when the copy could not be written to the index. The details
/// are logged.
const COPY_NOT_RECORDED: &str = "the copy could not be recorded";
const NO_FRAME_TABLE: &str = "no frame table";

const ENCODING_VERSION: u8 = 1;

/// Where one frame's codestream lives in the copy blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyFrame {
    pub offset: u64,
    pub len: u64,
    /// Cumulative end offsets (relative to the frame) of its progressive
    /// prefixes, in order.
    pub ends: Vec<u32>,
}

/// Locations of every frame of a copy blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyFrames(pub Vec<CopyFrame>);

impl CopyFrames {
    /// `[version u8 = 1][count u32 LE]`, then per frame
    /// `[offset u64][len u64][n u8][ends u32 x n]`, all little-endian.
    ///
    /// # Panics
    /// If there are more than `u32::MAX` frames or a frame has more than 255
    /// prefix ends; the encoder produces a handful per frame, so either is a
    /// bug, and a corrupt table must never be written silently.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![ENCODING_VERSION];
        let count = u32::try_from(self.0.len()).expect("frame count fits u32");
        out.extend_from_slice(&count.to_le_bytes());
        for f in &self.0 {
            out.extend_from_slice(&f.offset.to_le_bytes());
            out.extend_from_slice(&f.len.to_le_bytes());
            out.push(u8::try_from(f.ends.len()).expect("at most 255 prefix ends per frame"));
            for e in &f.ends {
                out.extend_from_slice(&e.to_le_bytes());
            }
        }
        out
    }

    /// Inverse of [`encode`](Self::encode). `None` for anything else,
    /// including trailing bytes.
    pub fn decode(bytes: &[u8]) -> Option<CopyFrames> {
        let mut r = ByteReader(bytes);
        if r.u8()? != ENCODING_VERSION {
            return None;
        }
        let mut frames = Vec::new();
        for _ in 0..r.u32()? {
            let offset = r.u64()?;
            let len = r.u64()?;
            let mut ends = Vec::new();
            for _ in 0..r.u8()? {
                ends.push(r.u32()?);
            }
            frames.push(CopyFrame { offset, len, ends });
        }
        r.0.is_empty().then_some(CopyFrames(frames))
    }
}

/// What is known about the HTJ2K copy of one instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyState {
    /// A verified copy blob (kind `j2cs`) with its frame locations.
    Ready { blob: BlobHash, frames: CopyFrames },
    /// The instance cannot have a copy; the reason is recorded once.
    NotEligible(String),
    /// Encoding or verification failed; the reason is recorded.
    Failed(String),
}

/// Read frame `frame` (0-based) of `source` and encode it, verified.
/// Blocking.
fn encode_frame(
    path: &std::path::Path,
    table: &FrameTable,
    frame: u32,
    info: &FrameInfo,
) -> Result<Encoded, CodecError> {
    let loc = table
        .location(frame)
        .ok_or_else(|| CodecError::BadFrame(format!("no frame {}", frame + 1)))?;
    let bytes = read_frame_at(path, &loc)
        .map_err(|e| CodecError::BadFrame(format!("cannot read frame {}: {e}", frame + 1)))?;
    encode_verified(&bytes, info)
}

impl Shared {
    /// Make, verify and record the copy of instance `id`. Does nothing for a
    /// unknown or unscanned instance, or one that has a current row. Holds
    /// the instance's lock stripe throughout, so concurrent calls make one
    /// copy.
    ///
    /// Everything that is the instance's own fault (not eligible, the
    /// encoder or verification failed or panicked, an index row the database
    /// refuses) is recorded, so the instance is not retried. `Err` only when
    /// nothing could be recorded for a reason that can pass (full disk, busy
    /// database); no row is left then.
    pub(crate) async fn make_copy(self: Arc<Self>, id: i64) -> Result<(), IngestError> {
        let Some((uid, _)) = self.index.instance_blob(id).await? else {
            return Ok(());
        };
        let _guard = self.lock_for(&uid).lock().await;
        // Re-read under the lock: an overwrite may have come first.
        let Some((_, source)) = self.index.instance_blob(id).await? else {
            return Ok(());
        };
        if self.index.copy_for(id, &source).await?.is_some() {
            return Ok(());
        }
        let table = match self.index.stored_data_for(id, &source).await? {
            StoredDataState::Ready(data) => data.frames,
            // Scanned later; recording a state now would be wrong.
            StoredDataState::Unscanned | StoredDataState::Replaced => return Ok(()),
        };
        let info = self.index.frame_info(id).await?;
        let (Some(table), Some(info)) = (table, info) else {
            let state = CopyState::NotEligible(NO_FRAME_TABLE.to_owned());
            return self.record_copy(id, &source, state, None).await;
        };
        if let Err(reason) = eligibility(&info) {
            return self
                .record_copy(id, &source, CopyState::NotEligible(reason.0), None)
                .await;
        }

        let path = self.blobs.path_of(&source, BlobKind::Dicom);
        let mut codestreams: Vec<u8> = Vec::new();
        let mut frames = Vec::new();
        for frame in 0..table.count() {
            #[cfg(test)]
            let panics = crate::ingest::TestHooks::panics(&self.hooks.panic_in_encode, id);
            let (path, table, info) = (path.clone(), table.clone(), info.clone());
            let encoded = tokio::task::spawn_blocking(move || {
                #[cfg(test)]
                if panics {
                    panic!("injected encoder panic");
                }
                encode_frame(&path, &table, frame, &info)
            })
            .await;
            let failure = match encoded {
                Ok(Ok(e)) => {
                    frames.push(CopyFrame {
                        offset: codestreams.len() as u64,
                        len: e.codestream.len() as u64,
                        ends: e.ends,
                    });
                    codestreams.extend_from_slice(&e.codestream);
                    continue;
                }
                Ok(Err(CodecError::NotEligible(reason))) => CopyState::NotEligible(reason),
                Ok(Err(e)) => CopyState::Failed(e.to_string()),
                Err(e) => {
                    error!(id, error = %e, "copy encoder failed");
                    CopyState::Failed(ENCODER_FAILED.to_owned())
                }
            };
            warn!(id, ?failure, "recording copy failure");
            return self.record_copy(id, &source, failure, None).await;
        }

        let blobs = self.blobs.clone();
        let blob = tokio::task::spawn_blocking(move || {
            let spooled = blobs
                .spool_sync(&mut codestreams.as_slice(), u64::MAX)
                .map_err(|e| match e {
                    crate::SpoolError::Io(e) => e,
                    other => std::io::Error::other(other),
                })?;
            blobs.commit(spooled, BlobKind::Htj2k)
        })
        .await
        .map_err(|e| {
            error!(id, error = %e, "copy commit task failed");
            IngestError::Storage(std::io::Error::other(e))
        })??;
        let state = CopyState::Ready {
            blob,
            frames: CopyFrames(frames),
        };
        self.record_copy(id, &source, state, Some(blob)).await
    }

    /// Write `state`. `copy_blob` is the blob just committed for it, if any:
    /// it is removed again unless something else uses it, when the write is
    /// refused or fails.
    async fn record_copy(
        self: &Arc<Self>,
        id: i64,
        source: &BlobHash,
        state: CopyState,
        copy_blob: Option<BlobHash>,
    ) -> Result<(), IngestError> {
        let result = match self.index.write_copy(id, source, &state).await {
            Ok(true) => return Ok(()),
            Ok(false) => Ok(()),
            Err(e) if rejects_row(&e) => {
                error!(id, error = %e, "cannot record copy");
                let state = CopyState::Failed(COPY_NOT_RECORDED.to_owned());
                self.index
                    .write_copy(id, source, &state)
                    .await
                    .map(|_| ())
                    .map_err(IngestError::Index)
            }
            Err(e) => Err(IngestError::Index(e)),
        };
        if let Some(blob) = copy_blob {
            self.discard_unreferenced(blob, BlobKind::Htj2k).await;
        }
        result
    }
}

impl Ingestor {
    /// Make the HTJ2K copy of instance `id` now, scanning it first if
    /// needed. See [`Shared::make_copy`] for what is recorded.
    pub async fn make_copy(&self, id: i64) -> Result<(), IngestError> {
        self.ensure_scanned(id).await?;
        let task = tokio::spawn(Arc::clone(&self.shared).make_copy(id));
        match task.await {
            Ok(result) => result,
            Err(e) => {
                error!(id, error = %e, "copy task failed");
                Err(IngestError::Storage(std::io::Error::other(e)))
            }
        }
    }

    /// Make up to `limit` copies, at most `parallel` instances at a time:
    /// first the instances of [`prioritize_copy`](Self::prioritize_copy),
    /// then the newest instances without a current copy row. Returns how
    /// many instances were handled. A row that fails on the database's side
    /// is logged and left for a later batch; `Err` when the candidates
    /// cannot be listed, or when every instance of the batch failed that
    /// way, so the caller can wait before trying again.
    pub async fn copy_batch(&self, limit: i64, parallel: usize) -> Result<usize, IngestError> {
        let wanted = usize::try_from(limit).unwrap_or(0);
        let mut ids: Vec<i64> = {
            let mut queue = self.shared.copy_queue.lock().unwrap();
            let n = wanted.min(queue.len());
            queue.drain(..n).collect()
        };
        let mut last_error = None;
        for id in &ids {
            if let Err(e) = self.ensure_scanned(*id).await {
                warn!(id, error = %e, "cannot scan a prioritized instance");
                last_error = Some(e);
            }
        }
        if ids.len() < wanted {
            for (id, _, _) in self.shared.index.copy_candidates(limit).await? {
                if ids.len() >= wanted {
                    break;
                }
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
        }
        let mut recorded = 0;
        for chunk in ids.chunks(parallel.max(1)) {
            let tasks: Vec<_> = chunk
                .iter()
                .map(|id| (*id, tokio::spawn(Arc::clone(&self.shared).make_copy(*id))))
                .collect();
            for (id, task) in tasks {
                match task.await {
                    Ok(Ok(())) => recorded += 1,
                    Ok(Err(e)) => {
                        error!(id, error = %e, "cannot record the copy; the row is retried later");
                        last_error = Some(e);
                    }
                    Err(e) => {
                        error!(id, error = %e, "copy task failed");
                        last_error = Some(IngestError::Storage(std::io::Error::other(e)));
                    }
                }
            }
        }
        match last_error {
            Some(e) if recorded == 0 => Err(e),
            _ => Ok(recorded),
        }
    }

    /// Have [`copy_batch`](Self::copy_batch) copy instance `id` before the
    /// others. The queue is in memory, holds 1024 instances and drops
    /// further requests when full; an instance is queued once.
    pub fn prioritize_copy(&self, id: i64) {
        let mut queue = self.shared.copy_queue.lock().unwrap();
        if queue.len() < PRIORITY_CAPACITY && !queue.contains(&id) {
            queue.push_back(id);
        }
    }

    /// The current `Ready` copy of instance `id` made from `blob` and the
    /// path of its file; `None` when there is none.
    pub async fn copy_frames(
        &self,
        id: i64,
        blob: &BlobHash,
    ) -> Result<Option<(CopyFrames, PathBuf)>, IngestError> {
        Ok(match self.shared.index.copy_for(id, blob).await? {
            Some(CopyState::Ready { blob, frames }) => {
                Some((frames, self.shared.blobs.path_of(&blob, BlobKind::Htj2k)))
            }
            _ => None,
        })
    }

    /// Encode the 1-based frames `numbers` of instance `id` now, verified,
    /// without storing anything, and queue the instance for its copy.
    /// [`IngestError::Unparseable`] when the instance, a frame or a frame
    /// table does not exist, the instance is not eligible, or encoding fails.
    pub async fn encode_transient(
        &self,
        id: i64,
        numbers: &[u32],
    ) -> Result<Vec<Encoded>, IngestError> {
        let unparseable = |s: &str| IngestError::Unparseable(s.to_owned());
        self.ensure_scanned(id).await?;
        let index = &self.shared.index;
        let Some((_, source)) = index.instance_blob(id).await? else {
            return Err(unparseable("unknown instance"));
        };
        let table = match index.stored_data_for(id, &source).await? {
            StoredDataState::Ready(data) => data.frames,
            _ => None,
        };
        let (Some(table), Some(info)) = (table, index.frame_info(id).await?) else {
            return Err(unparseable(NO_FRAME_TABLE));
        };
        eligibility(&info).map_err(|e| IngestError::Unparseable(e.0))?;
        let path = self.shared.blobs.path_of(&source, BlobKind::Dicom);
        let mut out = Vec::with_capacity(numbers.len());
        for &number in numbers {
            let frame = match number.checked_sub(1) {
                Some(f) if f < table.count() => f,
                _ => return Err(IngestError::Unparseable(format!("no frame {number}"))),
            };
            let (path, table, info) = (path.clone(), table.clone(), info.clone());
            let encoded =
                tokio::task::spawn_blocking(move || encode_frame(&path, &table, frame, &info))
                    .await
                    .map_err(|e| {
                        error!(id, error = %e, "transient encoder failed");
                        IngestError::Storage(std::io::Error::other(e))
                    })?;
            out.push(encoded.map_err(|e| IngestError::Unparseable(e.to_string()))?);
        }
        self.prioritize_copy(id);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_rejects_junk() {
        let f = CopyFrames(vec![
            CopyFrame {
                offset: 7,
                len: 100,
                ends: vec![10, 40, 100],
            },
            CopyFrame {
                offset: 107,
                len: 5,
                ends: vec![],
            },
        ]);
        assert_eq!(CopyFrames::decode(&f.encode()), Some(f.clone()));
        assert_eq!(CopyFrames::decode(b"junk"), None);
        let mut trailing = f.encode();
        trailing.push(0);
        assert_eq!(CopyFrames::decode(&trailing), None);
        let short = &f.encode()[..f.encode().len() - 1];
        assert_eq!(CopyFrames::decode(short), None);
    }

    /// A panicking encoder is recorded as failed; the other instances get
    /// their copies.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn encoder_panic_recorded_as_failed() {
        let d = tempfile::tempdir().unwrap();
        let src = d.path().join("src");
        crate::phantom::write_phantom(&src, 3, 16).unwrap();
        let ing = Ingestor::open(crate::StorageConfig {
            root: d.path().join("store"),
            max_instance_bytes: 1 << 30,
            on_duplicate: crate::DuplicatePolicy::Keep,
        })
        .await
        .unwrap();
        for i in 1..=3 {
            ing.import_file(&src.join(format!("IM{i:05}.dcm")))
                .await
                .unwrap();
        }
        let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM instances ORDER BY id")
            .fetch_all(&ing.shared.index.pool)
            .await
            .unwrap();
        ing.shared
            .hooks
            .panic_in_encode
            .lock()
            .unwrap()
            .push(ids[1]);

        assert_eq!(ing.copy_batch(10, 2).await.unwrap(), 3);
        let index = ing.index();
        let state = |id: i64| async move {
            let (_, source) = index.instance_blob(id).await.unwrap().unwrap();
            index.copy_for(id, &source).await.unwrap().unwrap()
        };
        assert_eq!(
            state(ids[1]).await,
            CopyState::Failed(ENCODER_FAILED.into())
        );
        assert!(matches!(state(ids[0]).await, CopyState::Ready { .. }));
        assert!(matches!(state(ids[2]).await, CopyState::Ready { .. }));
        assert!(index.copy_candidates(10).await.unwrap().is_empty());
    }
}
