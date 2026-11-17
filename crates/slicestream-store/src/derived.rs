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

/// Default number of on-demand encodes at once: half the cores, at least 1.
pub(crate) fn default_encode_slots() -> usize {
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    (cores / 2).max(1)
}

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

/// Why one frame could not be encoded.
enum FrameFailure {
    /// Reading the original failed in a way that can pass (I/O error).
    Transient(std::io::Error),
    /// The frame itself is bad, or encoding or verification failed.
    Codec(CodecError),
}

/// Read frame `frame` (0-based) of the file at `path` and encode it,
/// verified. Blocking.
fn encode_frame(
    path: &std::path::Path,
    table: &FrameTable,
    frame: u32,
    info: &FrameInfo,
) -> Result<Encoded, FrameFailure> {
    let bad = |m: String| FrameFailure::Codec(CodecError::BadFrame(m));
    let loc = table
        .location(frame)
        .ok_or_else(|| bad(format!("no frame {}", frame + 1)))?;
    let bytes = read_frame_at(path, &loc).map_err(|e| match e {
        // A file that is gone or too short is this instance's problem.
        slicestream_core::Error::Io { source, .. }
            if !matches!(
                source.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::UnexpectedEof
            ) =>
        {
            FrameFailure::Transient(source)
        }
        e => bad(format!("cannot read frame {}: {e}", frame + 1)),
    })?;
    encode_verified(&bytes, info).map_err(FrameFailure::Codec)
}

/// Marks instance `id` as being copied; removed on drop.
struct InFlight {
    shared: Arc<Shared>,
    id: i64,
}

impl InFlight {
    fn start(shared: &Arc<Shared>, id: i64) -> Option<InFlight> {
        shared
            .copy_inflight
            .lock()
            .unwrap()
            .insert(id)
            .then(|| InFlight {
                shared: Arc::clone(shared),
                id,
            })
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.shared.copy_inflight.lock().unwrap().remove(&self.id);
    }
}

impl Shared {
    /// Make, verify and record the copy of instance `id`; `true` when a row
    /// was written. Does nothing (`false`) for an unknown or unscanned
    /// instance, one that has a current row, one that is being copied by
    /// another call, or one that was overwritten meanwhile.
    ///
    /// Encoding and verification run without the instance's lock stripe; the
    /// stripe is held only to re-check the instance, commit the copy blob
    /// and write the row. Copy blobs are content-addressed and may be shared
    /// by several instances' rows, so a copy blob is never deleted here; a
    /// leftover is an orphan for [`Ingestor::recover`].
    ///
    /// Everything that is the instance's own fault (not eligible, the
    /// encoder or verification failed or panicked, a row the database
    /// refuses) is recorded, so the instance is not retried. `Err` for
    /// anything that can pass (I/O error, full disk, busy database,
    /// cancellation); no row is left then.
    pub(crate) async fn make_copy(self: Arc<Self>, id: i64) -> Result<bool, IngestError> {
        let Some((uid, source)) = self.index.instance_blob(id).await? else {
            return Ok(false);
        };
        if self.index.copy_for(id, &source).await?.is_some() {
            return Ok(false);
        }
        let Some(_in_flight) = InFlight::start(&self, id) else {
            return Ok(false);
        };
        let table = match self.index.stored_data_for(id, &source).await? {
            StoredDataState::Ready(data) => data.frames,
            // Scanned later; recording a state now would be wrong.
            StoredDataState::Unscanned | StoredDataState::Replaced => return Ok(false),
        };
        let info = self.index.frame_info(id).await?;
        let (Some(table), Some(info)) = (table, info) else {
            let state = CopyState::NotEligible(NO_FRAME_TABLE.to_owned());
            return self.record_copy(id, &source, state).await;
        };
        if let Err(reason) = eligibility(&info) {
            return self
                .record_copy(id, &source, CopyState::NotEligible(reason.0))
                .await;
        }

        let path = self.blobs.path_of(&source, BlobKind::Dicom);
        // Each codestream goes to the spool file as it is made; only the
        // frame table stays in memory.
        let mut temp = self.blobs.create_temp(u64::MAX)?;
        let mut frames = Vec::new();
        let mut offset = 0u64;
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
                    temp.write_chunk(&e.codestream).await?;
                    let len = e.codestream.len() as u64;
                    frames.push(CopyFrame {
                        offset,
                        len,
                        ends: e.ends,
                    });
                    offset += len;
                    continue;
                }
                Ok(Err(FrameFailure::Transient(e))) => return Err(IngestError::Storage(e)),
                Ok(Err(FrameFailure::Codec(CodecError::NotEligible(reason)))) => {
                    CopyState::NotEligible(reason)
                }
                Ok(Err(FrameFailure::Codec(e))) => CopyState::Failed(e.to_string()),
                Err(e) if e.is_panic() => {
                    error!(id, error = %e, "copy encoder failed");
                    CopyState::Failed(ENCODER_FAILED.to_owned())
                }
                // Cancelled: nothing is wrong with the instance.
                Err(e) => return Err(IngestError::Storage(std::io::Error::other(e))),
            };
            warn!(id, ?failure, "recording copy failure");
            return self.record_copy(id, &source, failure).await;
        }
        let spooled = temp.finish().await?;

        let _guard = self.lock_for(&uid).lock().await;
        // An overwrite or another writer may have come first.
        match self.index.instance_blob(id).await? {
            Some((_, current)) if current == source => {}
            _ => return Ok(false),
        }
        if self.index.copy_for(id, &source).await?.is_some() {
            return Ok(false);
        }
        let blobs = self.blobs.clone();
        let blob = tokio::task::spawn_blocking(move || blobs.commit(spooled, BlobKind::Htj2k))
            .await
            .map_err(|e| {
                error!(id, error = %e, "copy commit task failed");
                IngestError::Storage(std::io::Error::other(e))
            })??;
        let state = CopyState::Ready {
            blob,
            frames: CopyFrames(frames),
        };
        self.record_copy(id, &source, state).await
    }

    /// Write `state`; `true` when a row was written. A row the database
    /// refuses is replaced by a `Failed` row.
    async fn record_copy(
        &self,
        id: i64,
        source: &BlobHash,
        state: CopyState,
    ) -> Result<bool, IngestError> {
        #[cfg(test)]
        let injected = self.hooks.fail_next_write_copy.lock().unwrap().take();
        #[cfg(not(test))]
        let injected: Option<sqlx::Error> = None;
        let first = match injected {
            Some(e) => Err(e),
            None => self.index.write_copy(id, source, &state).await,
        };
        match first {
            Ok(written) => Ok(written),
            Err(e) if rejects_row(&e) => {
                error!(id, error = %e, "cannot record copy");
                let state = CopyState::Failed(COPY_NOT_RECORDED.to_owned());
                Ok(self.index.write_copy(id, source, &state).await?)
            }
            Err(e) => Err(IngestError::Index(e)),
        }
    }
}

impl Ingestor {
    /// Make the HTJ2K copy of instance `id` now, scanning it first if
    /// needed. `true` when a row was written; see [`Shared::make_copy`].
    pub async fn make_copy(&self, id: i64) -> Result<bool, IngestError> {
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
    /// many rows were written. A row that fails on the database's side
    /// is logged and left for a later batch; `Err` when the candidates
    /// cannot be listed, or when every instance of the batch failed that
    /// way, so the caller can wait before trying again. `stop` is asked
    /// before each group of `parallel` instances; once it returns `true`
    /// the batch ends after the group in flight, whose tasks are all
    /// awaited, so none outlives the call.
    pub async fn copy_batch(
        &self,
        limit: i64,
        parallel: usize,
        stop: impl Fn() -> bool,
    ) -> Result<usize, IngestError> {
        self.copy_some(limit, parallel, stop, true).await
    }

    /// [`copy_batch`](Self::copy_batch) over the instances of
    /// [`prioritize_copy`](Self::prioritize_copy) only (the instances a
    /// request asked for), never the other instances without a copy.
    pub async fn copy_requested(
        &self,
        limit: i64,
        parallel: usize,
        stop: impl Fn() -> bool,
    ) -> Result<usize, IngestError> {
        self.copy_some(limit, parallel, stop, false).await
    }

    /// Wait until an instance is queued by
    /// [`prioritize_copy`](Self::prioritize_copy). A request queued while
    /// nobody waited wakes the next call at once.
    pub async fn copy_request_waiting(&self) {
        self.shared.copy_requested.notified().await;
    }

    async fn copy_some(
        &self,
        limit: i64,
        parallel: usize,
        stop: impl Fn() -> bool,
        candidates: bool,
    ) -> Result<usize, IngestError> {
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
        if candidates && ids.len() < wanted {
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
            if stop() {
                break;
            }
            let tasks: Vec<_> = chunk
                .iter()
                .map(|id| (*id, tokio::spawn(Arc::clone(&self.shared).make_copy(*id))))
                .collect();
            for (id, task) in tasks {
                match task.await {
                    Ok(Ok(written)) => recorded += usize::from(written),
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
    /// others (and [`copy_requested`](Self::copy_requested) copy it at
    /// all). The queue is in memory, holds 1024 instances and drops
    /// further requests when full; an instance is queued once.
    pub fn prioritize_copy(&self, id: i64) {
        let mut queue = self.shared.copy_queue.lock().unwrap();
        if queue.len() < PRIORITY_CAPACITY && !queue.contains(&id) {
            queue.push_back(id);
            self.shared.copy_requested.notify_one();
        }
    }

    /// Run at most `permits` (at least 1) encodes of
    /// [`encode_transient`](Self::encode_transient) at once, across all
    /// callers; the others wait for a slot. The default is half the cores
    /// (at least 1). Encodes already running keep the slot they hold, so
    /// set it before serving requests.
    pub fn limit_on_demand_encodes(&self, permits: usize) {
        *self.shared.encode_slots.lock().unwrap() =
            Arc::new(tokio::sync::Semaphore::new(permits.max(1)));
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
    /// without storing anything, and queue the instance for its copy. Each
    /// frame waits for a slot of
    /// [`limit_on_demand_encodes`](Self::limit_on_demand_encodes), held
    /// until its encode ends even when the caller goes away.
    /// [`IngestError::NotFound`] for an unknown instance;
    /// [`IngestError::Unparseable`] when a frame or a frame table does not
    /// exist, the instance is not eligible, or encoding fails or panics.
    pub async fn encode_transient(
        &self,
        id: i64,
        numbers: &[u32],
    ) -> Result<Vec<Encoded>, IngestError> {
        let unparseable = |s: &str| IngestError::Unparseable(s.to_owned());
        self.ensure_scanned(id).await?;
        let index = &self.shared.index;
        let Some((_, source)) = index.instance_blob(id).await? else {
            return Err(IngestError::NotFound(format!("instance {id}")));
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
            let slots = Arc::clone(&self.shared.encode_slots.lock().unwrap());
            let slot = slots
                .acquire_owned()
                .await
                .map_err(|e| IngestError::Storage(std::io::Error::other(e)))?;
            #[cfg(test)]
            let (shared, panics) = (
                Arc::clone(&self.shared),
                crate::ingest::TestHooks::panics(&self.shared.hooks.panic_in_encode, id),
            );
            let encoded = tokio::task::spawn_blocking(move || {
                // Released when the encode ends, not when the caller stops
                // waiting for it.
                let _slot = slot;
                #[cfg(test)]
                let _gauge = crate::ingest::EncodeGauge::enter(&shared.hooks);
                #[cfg(test)]
                if panics {
                    panic!("injected encoder panic");
                }
                encode_frame(&path, &table, frame, &info)
            })
            .await
            .map_err(|e| {
                error!(id, error = %e, "transient encoder failed");
                if e.is_panic() {
                    // The instance's problem, as when encoding fails.
                    IngestError::Unparseable(ENCODER_FAILED.to_owned())
                } else {
                    IngestError::Storage(std::io::Error::other(e))
                }
            })?;
            out.push(encoded.map_err(|e| match e {
                FrameFailure::Transient(e) => IngestError::Storage(e),
                FrameFailure::Codec(e) => IngestError::Unparseable(e.to_string()),
            })?);
        }
        self.prioritize_copy(id);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

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

        assert_eq!(ing.copy_batch(10, 2, || false).await.unwrap(), 3);
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

    /// Instances of the phantom, imported into a fresh store.
    async fn phantom_store(d: &std::path::Path, n: usize) -> (Arc<Ingestor>, Vec<i64>) {
        let src = d.join("src");
        crate::phantom::write_phantom(&src, n, 16).unwrap();
        let ing = Ingestor::open(crate::StorageConfig {
            root: d.join("store"),
            max_instance_bytes: 1 << 30,
            on_duplicate: crate::DuplicatePolicy::Keep,
        })
        .await
        .unwrap();
        for i in 1..=n {
            ing.import_file(&src.join(format!("IM{i:05}.dcm")))
                .await
                .unwrap();
        }
        let ids = sqlx::query_scalar("SELECT id FROM instances ORDER BY id")
            .fetch_all(&ing.shared.index.pool)
            .await
            .unwrap();
        (ing, ids)
    }

    /// Concurrent on-demand encodes never run more at once than the limit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_demand_encodes_are_bounded() {
        let d = tempfile::tempdir().unwrap();
        let (ing, ids) = phantom_store(d.path(), 4).await;
        ing.limit_on_demand_encodes(2);
        *ing.shared.hooks.encode_delay.lock().unwrap() = Some(Duration::from_millis(100));
        let tasks: Vec<_> = (0..8)
            .map(|i| {
                let (ing, id) = (Arc::clone(&ing), ids[i % ids.len()]);
                tokio::spawn(async move { ing.encode_transient(id, &[1]).await })
            })
            .collect();
        for t in tasks {
            assert_eq!(t.await.unwrap().unwrap().len(), 1);
        }
        let peak = ing.shared.hooks.encodes_peak.load(Ordering::SeqCst);
        assert_eq!(peak, 2, "at most two encodes at once, and two did overlap");
    }

    /// A caller that goes away does not free its slot while its encode
    /// still runs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropped_caller_keeps_its_slot_until_the_encode_ends() {
        let d = tempfile::tempdir().unwrap();
        let (ing, ids) = phantom_store(d.path(), 2).await;
        ing.limit_on_demand_encodes(1);
        *ing.shared.hooks.encode_delay.lock().unwrap() = Some(Duration::from_millis(300));
        let first = {
            let (ing, id) = (Arc::clone(&ing), ids[0]);
            tokio::spawn(async move { ing.encode_transient(id, &[1]).await })
        };
        while ing.shared.hooks.encodes_running.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        first.abort();
        assert_eq!(ing.encode_transient(ids[1], &[1]).await.unwrap().len(), 1);
        assert_eq!(ing.shared.hooks.encodes_peak.load(Ordering::SeqCst), 1);
    }

    /// An encoder panic during an on-demand encode means "no HTJ2K for this
    /// instance", like an ineligible one, not a storage error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn on_demand_encoder_panic_is_unparseable() {
        let d = tempfile::tempdir().unwrap();
        let (ing, ids) = phantom_store(d.path(), 1).await;
        ing.shared
            .hooks
            .panic_in_encode
            .lock()
            .unwrap()
            .push(ids[0]);
        assert!(matches!(
            ing.encode_transient(ids[0], &[1]).await,
            Err(IngestError::Unparseable(_))
        ));
    }

    /// Two instances with identical pixels share one content-addressed copy
    /// blob. A failed write for the second must leave the first's copy intact.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_write_keeps_a_shared_copy_blob() {
        use dicom_core::{DataElement, PrimitiveValue, VR};
        use dicom_dictionary_std::tags;
        let d = tempfile::tempdir().unwrap();
        let src = d.path().join("src");
        crate::phantom::write_phantom(&src, 1, 16).unwrap();
        let ing = Ingestor::open(crate::StorageConfig {
            root: d.path().join("store"),
            max_instance_bytes: 1 << 30,
            on_duplicate: crate::DuplicatePolicy::Keep,
        })
        .await
        .unwrap();
        let first = src.join("IM00001.dcm");
        let second = src.join("twin.dcm");
        let mut obj = dicom_object::open_file(&first).unwrap();
        obj.put(DataElement::new(
            tags::SOP_INSTANCE_UID,
            VR::UI,
            PrimitiveValue::from("2.25.123456789012345678901234567890"),
        ));
        obj.write_to_file(&second).unwrap();
        ing.import_file(&first).await.unwrap();
        ing.import_file(&second).await.unwrap();
        let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM instances ORDER BY id")
            .fetch_all(&ing.shared.index.pool)
            .await
            .unwrap();
        let index = ing.index();
        let current = |id: i64| async move {
            let (_, source) = index.instance_blob(id).await.unwrap().unwrap();
            index.copy_for(id, &source).await.unwrap()
        };

        assert!(ing.make_copy(ids[0]).await.unwrap());
        let Some(CopyState::Ready { blob, .. }) = current(ids[0]).await else {
            panic!("no copy");
        };
        let file = ing.blobs().path_of(&blob, BlobKind::Htj2k);
        let intact = || {
            let bytes = std::fs::read(&file).unwrap();
            slicestream_codec::decode_full(&bytes).unwrap();
        };
        intact();

        // A write that fails for a reason that can pass: no row, an error,
        // and the shared copy blob survives.
        *ing.shared.hooks.fail_next_write_copy.lock().unwrap() = Some(sqlx::Error::PoolTimedOut);
        assert!(ing.make_copy(ids[1]).await.is_err());
        assert_eq!(current(ids[1]).await, None);
        intact();

        // A row the database refuses is recorded as failed.
        *ing.shared.hooks.fail_next_write_copy.lock().unwrap() =
            Some(sqlx::Error::Encode("too big".into()));
        assert!(ing.make_copy(ids[1]).await.unwrap());
        assert_eq!(
            current(ids[1]).await,
            Some(CopyState::Failed(COPY_NOT_RECORDED.into()))
        );
        intact();
        assert!(matches!(
            current(ids[0]).await,
            Some(CopyState::Ready { .. })
        ));
    }
}
