//! Progressive HTJ2K stream (`?progressive=htj2k`).
//!
//! A slice with an HTJ2K codestream goes out in two stages: a preview (the
//! codestream prefix up to the preview resolution) in one part, then the rest
//! in parts of at most 64 KiB, in [`StagedScheduler`] order. The codestream
//! comes from the instance's copy, or, without one, from an encode done on
//! the spot (which also queues the copy). A slice that is not eligible is
//! sent whole as stored, without `part`, as in the plain stream.
//!
//! Every binary message is gated by a [`FlowWindow`] fed by the client's
//! acks, and control messages are applied between parts: a `cursor` message
//! interrupts a final stage, which resumes where it stopped when its turn
//! comes again, so after a cursor change at most one window of old
//! priorities is still on its way.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use slicestream_codec::{HTJ2K_RPCL, eligibility, preview_level};
use slicestream_core::protocol::{self, ClientMessage, FramePart};
use slicestream_core::{FlowWindow, FrameInfo, Series, Stage, StagedScheduler};
use slicestream_store::CopyFrames;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::error::TryRecvError;

use crate::AppState;

/// Largest payload of one part of a final stage.
const PART_MAX: u64 = 65_536;

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
    /// Encoded for this connection; dropped once the final stage is sent.
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

/// The message(s) being sent for one `(slice, stage)`.
enum Job {
    /// A stored frame, sent whole.
    Whole {
        index: usize,
        message: Bytes,
        len: u64,
    },
    /// Codestream bytes `[next, end)` of one stage: a preview in one part, a
    /// final in parts of at most [`PART_MAX`].
    Stage {
        index: usize,
        stage: Stage,
        codestream: Arc<Codestream>,
        next: u64,
        end: u64,
    },
}

impl Job {
    /// Payload bytes of the next message.
    fn next_len(&self) -> u64 {
        match self {
            Job::Whole { len, .. } => *len,
            Job::Stage {
                stage: Stage::Preview,
                next,
                end,
                ..
            } => end - next,
            Job::Stage {
                stage: Stage::Final,
                next,
                end,
                ..
            } => (end - next).min(PART_MAX),
        }
    }
}

struct Session {
    series: Arc<Series>,
    state: Arc<AppState>,
    sched: StagedScheduler,
    flow: FlowWindow,
    started: Instant,
    /// Per slice, once known.
    sources: Vec<Option<Source>>,
    /// Ready copies, by instance id.
    copies: HashMap<i64, Arc<(CopyFrames, PathBuf)>>,
    /// Where an interrupted final stage resumes, by slice.
    resume: HashMap<usize, u64>,
    job: Option<Job>,
}

impl Session {
    fn new(series: Arc<Series>, state: Arc<AppState>) -> Session {
        let len = series.len();
        Session {
            sched: StagedScheduler::new(len, state.scheduler, state.htj2k.preview_ahead),
            flow: FlowWindow::new(),
            started: Instant::now(),
            sources: vec![None; len],
            copies: HashMap::new(),
            resume: HashMap::new(),
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

    /// Hand the job back to the scheduler. A final stage resumes where it
    /// stopped.
    fn interrupt(&mut self) {
        match self.job.take() {
            None => {}
            Some(Job::Whole { index, .. }) => self.sched.requeue_final(index),
            // A preview is one message, so none of it was sent.
            Some(Job::Stage {
                index,
                stage: Stage::Preview,
                ..
            }) => self.sched.mark_unsent(index),
            Some(Job::Stage {
                index,
                stage: Stage::Final,
                next,
                ..
            }) => {
                self.resume.insert(index, next);
                self.sched.requeue_final(index);
            }
        }
    }

    /// The job for `(index, stage)`, or `None` when nothing is to be sent.
    async fn start(&mut self, index: usize, stage: Stage) -> anyhow::Result<Option<Job>> {
        let source = match self.sources.get(index).cloned().flatten() {
            Some(s) => s,
            None => {
                let s = self.resolve(index).await;
                self.sources[index] = Some(s.clone());
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
            (Source::Htj2k(codestream), Stage::Preview) => Some(Job::Stage {
                index,
                stage,
                next: 0,
                end: codestream.preview_end,
                codestream,
            }),
            (Source::Htj2k(codestream), Stage::Final) => {
                let next = self.resume.remove(&index).unwrap_or(codestream.preview_end);
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
                        tracing::warn!(
                            index,
                            "cannot read the HTJ2K copy, sending as stored: {e:#}"
                        );
                        self.fall_back_to_whole(index, stage);
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
        if let Some(Some(Source::Htj2k(c))) = self.sources.get(index)
            && matches!(c.data, Data::Transient(_))
        {
            self.sources[index] = None;
        }
    }

    /// The codestream cannot be read: serve the slice whole from now on.
    fn fall_back_to_whole(&mut self, index: usize, stage: Stage) {
        self.job = None;
        self.sources[index] = Some(Source::Whole);
        self.resume.remove(&index);
        self.sched.set_single_stage(index);
        if stage == Stage::Final {
            self.sched.requeue_final(index);
        }
    }

    /// How slice `index` is served: its instance's copy, else a transient
    /// encode when eligible, else whole.
    async fn resolve(&mut self, index: usize) -> Source {
        let series = Arc::clone(&self.series);
        let Ok(slice) = series.slice(index) else {
            return Source::Whole;
        };
        let Some(id) = slice.instance() else {
            return Source::Whole;
        };
        if eligibility(&slice.info).is_err() {
            return Source::Whole;
        }
        let preview_size = self.state.htj2k.preview_size;
        if let Some(copy) = self.copy_of(id).await
            && let Some(f) = copy.0.0.get(slice.frame as usize)
        {
            let data = Data::File {
                path: copy.1.clone(),
                offset: f.offset,
            };
            let c = Codestream::new(&slice.info, data, f.len, f.ends.clone(), preview_size);
            return Source::Htj2k(Arc::new(c));
        }
        match self
            .state
            .ingestor
            .encode_transient(id, &[slice.frame + 1])
            .await
        {
            Ok(mut encoded) if encoded.len() == 1 => {
                let e = encoded.remove(0);
                let len = e.codestream.len() as u64;
                let data = Data::Transient(Bytes::from(e.codestream));
                let c = Codestream::new(&slice.info, data, len, e.ends, preview_size);
                Source::Htj2k(Arc::new(c))
            }
            Ok(_) => Source::Whole,
            Err(e) => {
                tracing::debug!(index, id, "no HTJ2K codestream, sending as stored: {e}");
                Source::Whole
            }
        }
    }

    /// The `Ready` copy of instance `id` for its current blob, if any.
    async fn copy_of(&mut self, id: i64) -> Option<Arc<(CopyFrames, PathBuf)>> {
        if let Some(c) = self.copies.get(&id) {
            return Some(Arc::clone(c));
        }
        let ingestor = &self.state.ingestor;
        let blob = match ingestor.index().instance_blob(id).await {
            Ok(Some((_, blob))) => blob,
            Ok(None) => return None,
            Err(e) => {
                tracing::warn!(id, "cannot look up the instance: {e}");
                return None;
            }
        };
        match ingestor.copy_frames(id, &blob).await {
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

impl Job {
    fn index(&self) -> usize {
        match self {
            Job::Whole { index, .. } | Job::Stage { index, .. } => *index,
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
    fn final_parts_are_at_most_64k() {
        let c = Arc::new(codestream(vec![10, 20, 30, 200_000], 64));
        let job = Job::Stage {
            index: 0,
            stage: Stage::Final,
            next: 30,
            end: c.len,
            codestream: Arc::clone(&c),
        };
        assert_eq!(job.next_len(), PART_MAX);
        let preview = Job::Stage {
            index: 0,
            stage: Stage::Preview,
            next: 0,
            end: 100_000,
            codestream: c,
        };
        assert_eq!(preview.next_len(), 100_000, "a preview is one part");
    }
}
