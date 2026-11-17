//! Progressive HTJ2K stream (`?progressive=htj2k`).
//!
//! A slice with an HTJ2K codestream goes out in two stages: a preview (the
//! codestream prefix up to the preview resolution), then the rest, in
//! [`StagedScheduler`] order. Every part, preview included, carries at most
//! 64 KiB. The codestream comes from the instance's copy, or, without one,
//! from an encode done on the spot (which also queues the copy). A slice
//! that is not eligible is sent whole as stored, without `part`, as in the
//! plain stream; so is a slice whose instance was replaced since the
//! connection's series snapshot was taken.
//!
//! Every binary message is gated by [`FlowWindow::can_send`], fed by the
//! client's acks. The client acks after every binary message it receives
//! (acks for messages handled in one event-loop turn may be coalesced), so
//! the unacknowledged bytes always drain and the gate always opens again.
//! Control messages are applied between parts: a `cursor` or `evicted`
//! message interrupts the stage in hand, which resumes where it stopped when
//! its turn comes again (an evicted slice starts over), so after a cursor
//! change at most one window of old priorities is still on its way.
//!
//! Transient codestreams are kept for at most [`TRANSIENT_MAX`] slices and
//! [`TRANSIENT_MAX_BYTES`] bytes per connection, least recently used dropped
//! first (one larger than the byte bound is not kept at all); one needed
//! again is encoded again, to the same bytes, so resume offsets stay valid.
//!
//! A slice first sent whole is single stage for the rest of the
//! connection. If, after the client evicts it, it can be had as a
//! codestream, its final stage carries the whole codestream from offset 0,
//! as the client holds no preview of it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use slicestream_codec::{HTJ2K_RPCL, eligibility, preview_level};
use slicestream_core::protocol::{self, ClientMessage, FramePart};
use slicestream_core::{FlowWindow, FrameInfo, Series, SliceRef, Stage, StagedScheduler};
use slicestream_store::{BlobHash, CopyFrames};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::error::TryRecvError;

use crate::AppState;

/// Largest payload of one part.
const PART_MAX: u64 = 65_536;
/// Transient codestreams kept per connection.
const TRANSIENT_MAX: usize = 64;
/// Bytes of transient codestreams kept per connection.
const TRANSIENT_MAX_BYTES: u64 = 64 << 20;

type Sink = SplitSink<WebSocket, Message>;

pub(crate) async fn run_progressive(
    socket: WebSocket,
    series: Arc<Series>,
    state: Arc<AppState>,
) -> anyhow::Result<()> {
    let (mut tx, rx) = socket.split();
    let hello = serde_json::json!({
        "type": "series",
        "series": series.summary(),
        "progressive": "htj2k",
    });
    tx.send(Message::Text(hello.to_string().into())).await?;

    let (mut ctl, reader) = crate::stream::spawn_reader(rx);
    let mut session = Session::new(series, state);
    // As in the plain stream: let the client say where it is looking first.
    if let Ok(Some(m)) = tokio::time::timeout(Duration::from_millis(100), ctl.recv()).await {
        session.apply(m);
    }
    let result = session.serve(&mut tx, &mut ctl).await;
    reader.abort();
    result
}

/// An HTJ2K codestream of one slice.
struct Codestream {
    /// The slice's info with the HTJ2K transfer syntax, for part headers.
    info: FrameInfo,
    data: Data,
    len: u64,
    /// End of the prefix holding resolutions `0..=r`, per `r`.
    ends: Vec<u32>,
    /// End of the preview stage.
    preview_end: u64,
}

enum Data {
    /// In the copy file at `path`, from `offset`.
    File { path: PathBuf, offset: u64 },
    /// Encoded for this connection.
    Transient(Bytes),
}

impl Codestream {
    fn new(info: &FrameInfo, data: Data, len: u64, ends: Vec<u32>, preview_size: u32) -> Self {
        let level = preview_level(info.rows, info.columns, preview_size);
        let preview_end = match ends.get(level).or(ends.last()) {
            Some(&end) => u64::from(end).min(len),
            None => len,
        };
        Codestream {
            info: FrameInfo {
                transfer_syntax: HTJ2K_RPCL.into(),
                ..info.clone()
            },
            data,
            len,
            ends,
            preview_end,
        }
    }

    fn is_transient(&self) -> bool {
        matches!(self.data, Data::Transient(_))
    }

    /// Highest resolution complete once bytes `[0, end)` are in.
    fn level_at(&self, end: u64) -> u32 {
        self.ends
            .iter()
            .rposition(|&e| u64::from(e) <= end)
            .unwrap_or(0) as u32
    }

    async fn read(&self, offset: u64, len: u64) -> anyhow::Result<Bytes> {
        match &self.data {
            Data::Transient(bytes) => Ok(bytes.slice(offset as usize..(offset + len) as usize)),
            Data::File { path, offset: at } => {
                let (path, at) = (path.clone(), at + offset);
                tokio::task::spawn_blocking(move || read_range(&path, at, len)).await?
            }
        }
    }
}

/// Bytes `[offset, offset + len)` of the file at `path`, by positioned read.
fn read_range(path: &Path, offset: u64, len: u64) -> anyhow::Result<Bytes> {
    #[cfg(unix)]
    {
        let loc = slicestream_core::FrameLocation::Bytes { offset, len };
        Ok(slicestream_core::frame::read_frame_at(path, &loc)?)
    }
    #[cfg(not(unix))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = std::fs::File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0; usize::try_from(len)?];
        file.read_exact(&mut buf)?;
        Ok(buf.into())
    }
}

/// How a slice is served, once known.
#[derive(Clone)]
enum Source {
    /// No codestream: the stored frame, whole, without `part`.
    Whole,
    Htj2k(Arc<Codestream>),
}

impl Source {
    fn is_transient(&self) -> bool {
        self.transient_bytes().is_some()
    }

    /// Size of a transient codestream; `None` for other sources.
    fn transient_bytes(&self) -> Option<u64> {
        match self {
            Source::Htj2k(c) if c.is_transient() => Some(c.len),
            _ => None,
        }
    }
}

/// Known sources per slice, with at most `max_transient` transient
/// codestreams of at most `max_bytes` bytes in all (least recently used
/// dropped first).
struct Sources {
    slots: Vec<Option<Source>>,
    /// Slices holding a transient codestream, least recently used first.
    transient: VecDeque<usize>,
    /// Bytes of the transient codestreams in `slots`.
    bytes: u64,
    max_transient: usize,
    max_bytes: u64,
}

impl Sources {
    fn new(len: usize, max_transient: usize, max_bytes: u64) -> Sources {
        Sources {
            slots: vec![None; len],
            transient: VecDeque::new(),
            bytes: 0,
            max_transient,
            max_bytes,
        }
    }

    /// The known source of `index`; a transient one counts as used.
    fn get(&mut self, index: usize) -> Option<Source> {
        let source = self.slots.get(index)?.clone()?;
        if source.is_transient() {
            self.transient.retain(|&i| i != index);
            self.transient.push_back(index);
        }
        Some(source)
    }

    /// Keep `source` for `index`. A transient codestream larger than
    /// `max_bytes` is not kept.
    fn insert(&mut self, index: usize, source: Source) {
        if index >= self.slots.len() {
            return;
        }
        self.remove(index);
        let Some(bytes) = source.transient_bytes() else {
            self.slots[index] = Some(source);
            return;
        };
        if bytes > self.max_bytes {
            return;
        }
        self.slots[index] = Some(source);
        self.transient.push_back(index);
        self.bytes += bytes;
        // The newest fits on its own, so it is never the one dropped.
        while self.transient.len() > self.max_transient || self.bytes > self.max_bytes {
            match self.transient.front() {
                Some(&old) => self.remove(old),
                None => break,
            }
        }
    }

    fn remove(&mut self, index: usize) {
        if let Some(bytes) = self
            .slots
            .get_mut(index)
            .and_then(Option::take)
            .and_then(|s| s.transient_bytes())
        {
            self.bytes -= bytes;
        }
        self.transient.retain(|&i| i != index);
    }

    #[cfg(test)]
    fn transient_len(&self) -> usize {
        self.transient.len()
    }

    /// Bytes of the transient codestreams kept, counted afresh.
    #[cfg(test)]
    fn transient_bytes(&self) -> u64 {
        let sum = self
            .transient
            .iter()
            .filter_map(|&i| self.slots[i].as_ref()?.transient_bytes())
            .sum();
        assert_eq!(sum, self.bytes);
        sum
    }
}

/// The message(s) being sent for one `(slice, stage)`.
enum Job {
    /// A stored frame, sent whole.
    Whole {
        index: usize,
        message: Bytes,
        len: u64,
    },
    /// Codestream bytes `[next, end)` of one stage, in parts of at most
    /// [`PART_MAX`].
    Stage {
        index: usize,
        stage: Stage,
        codestream: Arc<Codestream>,
        next: u64,
        end: u64,
    },
}

impl Job {
    fn index(&self) -> usize {
        match self {
            Job::Whole { index, .. } | Job::Stage { index, .. } => *index,
        }
    }

    /// Payload bytes of the next message.
    fn next_len(&self) -> u64 {
        match self {
            Job::Whole { len, .. } => *len,
            Job::Stage { next, end, .. } => (end - next).min(PART_MAX),
        }
    }
}

/// The store blob a slice of the series snapshot was read from.
fn snapshot_blob(slice: &SliceRef) -> Option<BlobHash> {
    BlobHash::from_hex(slice.path.file_stem()?.to_str()?)
}

struct Session {
    series: Arc<Series>,
    state: Arc<AppState>,
    sched: StagedScheduler,
    flow: FlowWindow,
    started: Instant,
    sources: Sources,
    /// Ready copies, by instance id.
    copies: HashMap<i64, Arc<(CopyFrames, PathBuf)>>,
    /// Where an interrupted stage resumes, by slice.
    resume: HashMap<usize, u64>,
    /// Slices whose parts stopped mid-frame (the codestream became
    /// unreadable); skipped until the client evicts them.
    abandoned: HashSet<usize>,
    job: Option<Job>,
}

impl Session {
    fn new(series: Arc<Series>, state: Arc<AppState>) -> Session {
        let len = series.len();
        Session {
            sched: StagedScheduler::new(len, state.scheduler, state.htj2k.preview_ahead),
            flow: FlowWindow::new(),
            started: Instant::now(),
            sources: Sources::new(len, TRANSIENT_MAX, TRANSIENT_MAX_BYTES),
            copies: HashMap::new(),
            resume: HashMap::new(),
            abandoned: HashSet::new(),
            job: None,
            series,
            state,
        }
    }

    fn now(&self) -> Duration {
        self.started.elapsed()
    }

    async fn serve(
        &mut self,
        tx: &mut Sink,
        ctl: &mut UnboundedReceiver<ClientMessage>,
    ) -> anyhow::Result<()> {
        loop {
            // Apply every pending control message; a cursor or eviction
            // re-plans, interrupting the job between two parts.
            let mut replan = false;
            loop {
                match ctl.try_recv() {
                    Ok(m) => replan |= self.apply(m),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return Ok(()),
                }
            }
            if replan {
                self.interrupt();
            }

            if self.job.is_none() {
                let Some((index, stage)) = self.sched.pop_next() else {
                    // Everything delivered; sleep until the client says something.
                    match ctl.recv().await {
                        Some(m) => {
                            self.apply(m);
                            continue;
                        }
                        None => return Ok(()),
                    }
                };
                self.job = self.start(index, stage).await?;
            }
            let Some(job) = &self.job else { continue };

            if !self.flow.can_send(job.next_len()) {
                // Wait for an ack (or a re-plan) to make room.
                match ctl.recv().await {
                    Some(m) => {
                        if self.apply(m) {
                            self.interrupt();
                        }
                        continue;
                    }
                    None => return Ok(()),
                }
            }
            self.send_next(tx).await?;
        }
    }

    /// Apply a control message; `true` when it changes priorities.
    fn apply(&mut self, msg: ClientMessage) -> bool {
        match msg {
            ClientMessage::Cursor { index, velocity } => self.sched.update(index, velocity),
            ClientMessage::Evicted { indices } => {
                for i in indices {
                    self.sched.mark_unsent(i);
                    self.resume.remove(&i);
                    self.sources.remove(i);
                    self.abandoned.remove(&i);
                    if self.job.as_ref().is_some_and(|j| j.index() == i) {
                        // Both stages start over; nothing to requeue.
                        self.job = None;
                    }
                }
            }
            ClientMessage::Ack { bytes } => {
                let now = self.now();
                self.flow.on_ack(bytes, now);
                return false;
            }
        }
        true
    }

    /// Hand the job back to the scheduler; its stage resumes where it
    /// stopped.
    fn interrupt(&mut self) {
        match self.job.take() {
            None => {}
            Some(Job::Whole { index, .. }) => self.sched.requeue_final(index),
            Some(Job::Stage {
                index, stage, next, ..
            }) => {
                self.resume.insert(index, next);
                match stage {
                    // The final is still unsent too, so this only requeues
                    // the preview.
                    Stage::Preview => self.sched.mark_unsent(index),
                    Stage::Final => self.sched.requeue_final(index),
                }
            }
        }
    }

    /// The job for `(index, stage)`, or `None` when nothing is to be sent.
    async fn start(&mut self, index: usize, stage: Stage) -> anyhow::Result<Option<Job>> {
        if self.abandoned.contains(&index) {
            return Ok(None);
        }
        let source = match self.sources.get(index) {
            Some(s) => s,
            None => {
                let s = self.resolve(index).await;
                self.sources.insert(index, s.clone());
                s
            }
        };
        Ok(match (source, stage) {
            (Source::Whole, Stage::Preview) => {
                // Only now known to be single stage; its final comes later.
                self.sched.set_single_stage(index);
                None
            }
            (Source::Whole, Stage::Final) => {
                let s = Arc::clone(&self.series);
                match tokio::task::spawn_blocking(move || s.load_frame(index)).await? {
                    Ok(frame) => Some(Job::Whole {
                        index,
                        len: frame.data.len() as u64,
                        message: protocol::encode_frame(&frame),
                    }),
                    Err(e) => {
                        tracing::warn!(index, "failed to load frame: {e}");
                        None
                    }
                }
            }
            (Source::Htj2k(codestream), Stage::Preview) => {
                // A resume offset past the preview belongs to the final.
                let next = match self.resume.get(&index) {
                    Some(&n) if n < codestream.preview_end => {
                        self.resume.remove(&index);
                        n
                    }
                    _ => 0,
                };
                (next < codestream.preview_end).then(|| Job::Stage {
                    index,
                    stage,
                    next,
                    end: codestream.preview_end,
                    codestream,
                })
            }
            (Source::Htj2k(codestream), Stage::Final) => {
                // A single-stage slice was sent whole before and evicted
                // since: the client has no preview of it.
                let start = if self.sched.is_single_stage(index) {
                    0
                } else {
                    codestream.preview_end
                };
                let next = self.resume.remove(&index).unwrap_or(0).max(start);
                if next >= codestream.len {
                    self.final_sent(index);
                    None
                } else {
                    Some(Job::Stage {
                        index,
                        stage,
                        next,
                        end: codestream.len,
                        codestream,
                    })
                }
            }
        })
    }

    /// Send the job's next message.
    async fn send_next(&mut self, tx: &mut Sink) -> anyhow::Result<()> {
        let Some(job) = self.job.as_ref() else {
            return Ok(());
        };
        let len = job.next_len();
        match job {
            Job::Whole { message, .. } => {
                tx.send(Message::Binary(message.clone())).await?;
                self.job = None;
            }
            Job::Stage {
                index,
                stage,
                codestream,
                next,
                end,
            } => {
                let (index, stage, offset) = (*index, *stage, *next);
                let done = offset + len >= *end;
                let codestream = Arc::clone(codestream);
                let payload = match codestream.read(offset, len).await {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(index, "cannot read the HTJ2K copy: {e:#}");
                        self.recover(index, stage, offset).await;
                        return Ok(());
                    }
                };
                let part = FramePart {
                    offset,
                    total: codestream.len,
                    level: codestream.level_at(offset + len),
                };
                let msg = protocol::encode_part(index, &codestream.info, part, &payload);
                tx.send(Message::Binary(msg)).await?;
                if done {
                    self.job = None;
                    if stage == Stage::Final {
                        self.final_sent(index);
                    }
                } else if let Some(Job::Stage { next, .. }) = self.job.as_mut() {
                    *next += len;
                }
            }
        }
        let now = self.now();
        self.flow.on_sent(len, now);
        Ok(())
    }

    /// The slice's codestream is fully sent: a transient one is dropped.
    fn final_sent(&mut self, index: usize) {
        if self
            .sources
            .slots
            .get(index)
            .cloned()
            .flatten()
            .is_some_and(|s| s.is_transient())
        {
            self.sources.remove(index);
        }
    }

    /// The copy of slice `index` could not be read at `next` of `stage`.
    /// Encode the frame again (the same bytes) and go on at `next`; failing
    /// that, send the frame whole if none of it was sent yet; otherwise stop
    /// sending the slice until the client evicts it.
    async fn recover(&mut self, index: usize, stage: Stage, next: u64) {
        self.job = None;
        self.sources.remove(index);
        if let Some(id) = self.series.slice(index).ok().and_then(SliceRef::instance) {
            self.copies.remove(&id);
        }
        let started = !(stage == Stage::Preview && next == 0);
        self.resume.remove(&index);
        if let Some(codestream) = self.transient(index).await {
            let end = match stage {
                Stage::Preview => codestream.preview_end,
                Stage::Final => codestream.len,
            };
            if next < end {
                self.sources
                    .insert(index, Source::Htj2k(Arc::clone(&codestream)));
                self.job = Some(Job::Stage {
                    index,
                    stage,
                    codestream,
                    next,
                    end,
                });
                return;
            }
        }
        if started {
            tracing::warn!(
                index,
                "slice left incomplete; the client may evict it to get it again"
            );
            self.abandoned.insert(index);
        } else {
            tracing::warn!(index, "sending the slice as stored");
            self.sources.insert(index, Source::Whole);
            self.sched.set_single_stage(index);
        }
    }

    /// How slice `index` is served: its instance's copy, else a transient
    /// encode when eligible, else whole. Whole too when the instance's
    /// current blob is not the one of the series snapshot.
    async fn resolve(&mut self, index: usize) -> Source {
        let series = Arc::clone(&self.series);
        let Ok(slice) = series.slice(index) else {
            return Source::Whole;
        };
        let (Some(id), Some(blob)) = (slice.instance(), snapshot_blob(slice)) else {
            return Source::Whole;
        };
        if eligibility(&slice.info).is_err() || !self.is_current(id, &blob).await {
            return Source::Whole;
        }
        let preview_size = self.state.htj2k.preview_size;
        if let Some(copy) = self.copy_of(id, &blob).await
            && let Some(f) = copy.0.0.get(slice.frame as usize)
        {
            let data = Data::File {
                path: copy.1.clone(),
                offset: f.offset,
            };
            let c = Codestream::new(&slice.info, data, f.len, f.ends.clone(), preview_size);
            return Source::Htj2k(Arc::new(c));
        }
        match self.transient(index).await {
            Some(c) => Source::Htj2k(c),
            None => Source::Whole,
        }
    }

    /// Slice `index` encoded now, when its snapshot blob is still the
    /// instance's current one (checked before and after the encode).
    async fn transient(&mut self, index: usize) -> Option<Arc<Codestream>> {
        let series = Arc::clone(&self.series);
        let slice = series.slice(index).ok()?;
        let (id, blob) = (slice.instance()?, snapshot_blob(slice)?);
        if eligibility(&slice.info).is_err() || !self.is_current(id, &blob).await {
            return None;
        }
        let ingestor = &self.state.ingestor;
        let mut encoded = match ingestor.encode_transient(id, &[slice.frame + 1]).await {
            Ok(e) if e.len() == 1 => e,
            Ok(_) => return None,
            Err(e) => {
                tracing::debug!(index, id, "no HTJ2K codestream, sending as stored: {e}");
                return None;
            }
        };
        if !self.is_current(id, &blob).await {
            return None;
        }
        let e = encoded.remove(0);
        let len = e.codestream.len() as u64;
        let data = Data::Transient(Bytes::from(e.codestream));
        let preview_size = self.state.htj2k.preview_size;
        Some(Arc::new(Codestream::new(
            &slice.info,
            data,
            len,
            e.ends,
            preview_size,
        )))
    }

    /// Whether instance `id` still holds `blob`.
    async fn is_current(&self, id: i64, blob: &BlobHash) -> bool {
        match self.state.ingestor.index().instance_blob(id).await {
            Ok(Some((_, current))) => current == *blob,
            Ok(None) => false,
            Err(e) => {
                tracing::warn!(id, "cannot look up the instance: {e}");
                false
            }
        }
    }

    /// The `Ready` copy of instance `id` made from `blob`, if any.
    async fn copy_of(&mut self, id: i64, blob: &BlobHash) -> Option<Arc<(CopyFrames, PathBuf)>> {
        if let Some(c) = self.copies.get(&id) {
            return Some(Arc::clone(c));
        }
        match self.state.ingestor.copy_frames(id, blob).await {
            Ok(Some(copy)) => {
                let copy = Arc::new(copy);
                self.copies.insert(id, Arc::clone(&copy));
                Some(copy)
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(id, "cannot look up the HTJ2K copy: {e}");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(rows: u16, columns: u16) -> FrameInfo {
        FrameInfo {
            rows,
            columns,
            bits_allocated: 16,
            bits_stored: 12,
            samples_per_pixel: 1,
            pixel_representation: 0,
            photometric_interpretation: "MONOCHROME2".into(),
            transfer_syntax: "1.2.840.10008.1.2.1".into(),
            rescale_slope: 1.0,
            rescale_intercept: 0.0,
            window_center: None,
            window_width: None,
        }
    }

    fn codestream(ends: Vec<u32>, preview_size: u32) -> Codestream {
        let len = ends.last().map_or(0, |&e| u64::from(e));
        let data = Data::Transient(Bytes::from(vec![0; len as usize]));
        Codestream::new(&info(512, 512), data, len, ends, preview_size)
    }

    #[test]
    fn preview_ends_at_preview_level() {
        // 512 x 512: 3 levels; 256 px is level 2.
        let c = codestream(vec![100, 300, 900, 4000], 256);
        assert_eq!(c.preview_end, 900);
        assert_eq!(c.info.transfer_syntax, HTJ2K_RPCL);
        // A preview as large as the frame is the whole codestream.
        assert_eq!(
            codestream(vec![100, 300, 900, 4000], 4096).preview_end,
            4000
        );
    }

    #[test]
    fn part_level_is_highest_complete_resolution() {
        let c = codestream(vec![100, 300, 900, 4000], 256);
        assert_eq!(c.level_at(900), 2);
        assert_eq!(c.level_at(899), 1);
        assert_eq!(c.level_at(3999), 2);
        assert_eq!(c.level_at(4000), 3);
    }

    #[test]
    fn every_part_is_at_most_64k() {
        let c = Arc::new(codestream(vec![10, 20, 100_000, 200_000], 256));
        for stage in [Stage::Preview, Stage::Final] {
            let job = Job::Stage {
                index: 0,
                stage,
                next: 30,
                end: c.len,
                codestream: Arc::clone(&c),
            };
            assert_eq!(job.next_len(), PART_MAX, "{stage:?}");
        }
        let tail = Job::Stage {
            index: 0,
            stage: Stage::Preview,
            next: 0,
            end: 1_000,
            codestream: c,
        };
        assert_eq!(tail.next_len(), 1_000);
    }

    #[test]
    fn sources_keep_the_most_recently_used_transients() {
        let transient = || Source::Htj2k(Arc::new(codestream(vec![10], 256)));
        let mut s = Sources::new(10, 3, u64::MAX);
        s.insert(0, transient());
        s.insert(1, transient());
        s.insert(2, Source::Whole);
        s.insert(3, transient());
        assert!(s.get(0).is_some(), "touch 0: 1 is now the oldest");
        s.insert(4, transient());
        assert_eq!(s.transient_len(), 3);
        assert!(s.slots[1].is_none(), "least recently used dropped");
        assert!(s.slots[0].is_some() && s.slots[3].is_some() && s.slots[4].is_some());
        assert!(s.slots[2].is_some(), "whole sources are not counted");
        s.remove(4);
        assert_eq!(s.transient_len(), 2);
    }

    /// Large transient codestreams are bounded by bytes, not only by
    /// count; one larger than the cap is not kept and evicts nothing.
    #[test]
    fn sources_keep_transients_under_the_byte_cap() {
        let big = |len: u64| {
            let data = Data::Transient(Bytes::new());
            Source::Htj2k(Arc::new(Codestream::new(
                &info(8192, 8192),
                data,
                len,
                vec![len as u32],
                256,
            )))
        };
        let mut s = Sources::new(20, TRANSIENT_MAX, TRANSIENT_MAX_BYTES);
        for i in 0..10 {
            s.insert(i, big(20 << 20));
            assert!(s.transient_bytes() <= TRANSIENT_MAX_BYTES, "after {i}");
        }
        assert_eq!(s.transient_len(), 3);
        assert!(s.slots[9].is_some() && s.slots[8].is_some() && s.slots[7].is_some());
        s.insert(10, big(TRANSIENT_MAX_BYTES + 1));
        assert!(s.slots[10].is_none(), "too large to keep");
        assert_eq!(s.transient_len(), 3, "nothing evicted for it");
        assert!(s.transient_bytes() <= TRANSIENT_MAX_BYTES);
    }

    // --- sessions over a real store ----------------------------------------

    use crate::SeriesCatalog;
    use crate::config::{Htj2kConfig, Htj2kMode};
    use slicestream_store::{DuplicatePolicy, Ingestor, StorageConfig};

    const PHANTOM: &str = "2.25.1000000000000000000000000000001";

    /// A session over `slices` phantom slices of 128 x 128 (one level; the
    /// preview is level 0), imported into a fresh store (copies only if
    /// `copies`).
    async fn session(slices: usize, copies: bool) -> (tempfile::TempDir, Session) {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        crate::phantom::write_phantom(&src, slices, 128).unwrap();
        let ingestor = Ingestor::open(StorageConfig {
            root: dir.path().join("store"),
            on_duplicate: DuplicatePolicy::Overwrite,
            ..StorageConfig::default()
        })
        .await
        .unwrap();
        for i in 1..=slices {
            ingestor
                .import_file(&src.join(format!("IM{i:05}.dcm")))
                .await
                .unwrap();
        }
        if copies {
            ingestor.copy_batch(1000, 2, || false).await.unwrap();
        }
        let state = Arc::new(AppState {
            catalog: SeriesCatalog::new(Arc::clone(&ingestor)),
            ingestor,
            scheduler: Default::default(),
            http: Default::default(),
            dicomweb: Default::default(),
            htj2k: Htj2kConfig {
                mode: Htj2kMode::OnDemand,
                preview_size: 64,
                ..Htj2kConfig::default()
            },
            metrics: None,
            gauges: Default::default(),
            archives: None,
        });
        let series = state.catalog.get(PHANTOM).await.unwrap().unwrap();
        (dir, Session::new(series, state))
    }

    /// Replace every instance with different pixels (same UIDs).
    async fn overwrite(dir: &Path, s: &Session) {
        let src = dir.join("src2");
        crate::phantom::write_phantom(&src, s.series.len(), 16).unwrap();
        for i in 1..=s.series.len() {
            s.state
                .ingestor
                .import_file(&src.join(format!("IM{i:05}.dcm")))
                .await
                .unwrap();
        }
    }

    fn cursor(s: &mut Session, index: usize) {
        s.apply(ClientMessage::Cursor {
            index,
            velocity: 0.0,
        });
    }

    async fn job_bytes(job: &Job) -> Vec<u8> {
        let Job::Stage { codestream, .. } = job else {
            panic!("not a codestream job");
        };
        codestream.read(0, codestream.len).await.unwrap().to_vec()
    }

    /// Scrolling through more slices than the bound keeps at most
    /// `TRANSIENT_MAX` transient codestreams; a dropped one is encoded
    /// again to the same bytes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transient_cache_stays_bounded_while_scrolling() {
        let n = TRANSIENT_MAX + 6;
        let (_dir, mut s) = session(n, false).await;
        let mut first = None;
        for i in 0..n {
            cursor(&mut s, i);
            assert_eq!(s.sched.pop_next(), Some((i, Stage::Preview)));
            let job = s.start(i, Stage::Preview).await.unwrap().expect("a job");
            if i == 0 {
                first = Some(job_bytes(&job).await);
            }
            assert!(s.sources.transient_len() <= TRANSIENT_MAX);
        }
        assert_eq!(s.sources.transient_len(), TRANSIENT_MAX);
        assert!(s.sources.slots[0].is_none(), "the oldest was dropped");
        s.apply(ClientMessage::Evicted { indices: vec![0] });
        cursor(&mut s, 0);
        assert_eq!(s.sched.pop_next(), Some((0, Stage::Preview)));
        let again = s.start(0, Stage::Preview).await.unwrap().unwrap();
        assert!(job_bytes(&again).await == first.unwrap());
    }

    /// An unreadable copy is encoded again and sending goes on at `next`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unreadable_copy_is_encoded_again() {
        let (_dir, mut s) = session(2, true).await;
        cursor(&mut s, 1);
        s.sched.pop_next();
        let job = s.start(1, Stage::Preview).await.unwrap().unwrap();
        let Job::Stage { codestream, .. } = &job else {
            panic!()
        };
        let Data::File { path, .. } = &codestream.data else {
            panic!("expected the copy")
        };
        let copy = job_bytes(&job).await;
        let at = codestream.preview_end + 3;
        std::fs::remove_file(path).unwrap();

        s.recover(1, Stage::Final, at).await;
        let Some(Job::Stage {
            index: 1,
            stage: Stage::Final,
            next,
            codestream,
            ..
        }) = &s.job
        else {
            panic!("expected the final to go on");
        };
        assert_eq!(*next, at);
        assert!(codestream.is_transient());
        assert!(codestream.read(0, codestream.len).await.unwrap() == copy);
        assert!(s.abandoned.is_empty());
    }

    /// Without a re-encode (the instance was replaced): a slice none of
    /// which was sent goes out whole; one already under way is left until
    /// the client evicts it. The replaced instance then streams whole.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unreadable_copy_of_replaced_instance() {
        let (dir, mut s) = session(2, true).await;
        for i in 0..2 {
            assert!(matches!(s.resolve(i).await, Source::Htj2k(_)));
        }
        overwrite(dir.path(), &s).await;

        s.recover(0, Stage::Preview, 0).await;
        assert!(matches!(s.sources.get(0), Some(Source::Whole)));
        assert!(!s.abandoned.contains(&0));

        s.recover(1, Stage::Final, 9).await;
        assert!(s.job.is_none());
        assert!(s.abandoned.contains(&1));
        assert!(s.start(1, Stage::Final).await.unwrap().is_none());

        s.apply(ClientMessage::Evicted { indices: vec![1] });
        assert!(!s.abandoned.contains(&1));
        assert!(
            matches!(s.resolve(1).await, Source::Whole),
            "a replaced instance is served whole from the snapshot"
        );
    }

    /// `evicted` mid-final forgets the resume offset: the preview comes
    /// again from 0, then the final from the preview's end.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn evicted_restarts_preview_then_final() {
        let (_dir, mut s) = session(3, true).await;
        cursor(&mut s, 2);
        assert_eq!(s.sched.pop_next(), Some((2, Stage::Preview)));
        assert_eq!(s.sched.pop_next(), Some((2, Stage::Final)));
        s.job = s.start(2, Stage::Final).await.unwrap();
        if let Some(Job::Stage { next, .. }) = s.job.as_mut() {
            *next += 5;
        }
        cursor(&mut s, 0);
        s.interrupt();
        assert!(s.resume.contains_key(&2));

        s.apply(ClientMessage::Evicted { indices: vec![2] });
        assert!(!s.resume.contains_key(&2));
        cursor(&mut s, 2);
        assert_eq!(s.sched.pop_next(), Some((2, Stage::Preview)));
        let Some(Job::Stage { next, .. }) = s.start(2, Stage::Preview).await.unwrap() else {
            panic!()
        };
        assert_eq!(next, 0);
        assert_eq!(s.sched.pop_next(), Some((2, Stage::Final)));
        let Some(Job::Stage {
            next, codestream, ..
        }) = s.start(2, Stage::Final).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(next, codestream.preview_end);
    }

    /// A slice that went out whole (single stage) and was evicted can
    /// later resolve to a codestream: the client holds none of it, so its
    /// final starts at 0, not at the preview's end, and completes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn whole_slice_resolving_to_a_codestream_after_eviction_starts_at_zero() {
        let (_dir, mut s) = session(2, true).await;
        // No re-encode possible while the original is away: sent whole.
        let original = s.series.slice(0).unwrap().path.clone();
        let away = original.with_extension("away");
        std::fs::rename(&original, &away).unwrap();
        s.recover(0, Stage::Preview, 0).await;
        assert!(matches!(s.sources.get(0), Some(Source::Whole)));
        std::fs::rename(&away, &original).unwrap();

        s.apply(ClientMessage::Evicted { indices: vec![0] });
        cursor(&mut s, 0);
        assert_eq!(s.sched.pop_next(), Some((0, Stage::Final)));
        let Some(Job::Stage {
            next,
            end,
            codestream,
            ..
        }) = s.start(0, Stage::Final).await.unwrap()
        else {
            panic!("expected the copy");
        };
        assert!(codestream.preview_end > 0);
        assert_eq!(next, 0, "the whole codestream is sent");
        assert_eq!(end, codestream.len);
    }
}
