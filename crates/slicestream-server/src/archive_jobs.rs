//! Archive upload jobs: `POST /api/archives` spools the body and queues a
//! job that runs [`process_upload`]; `GET /api/archives/{job}` reports its
//! progress and results. Jobs live in memory; the `uploads` row persists.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{Extensions, HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use serde::Serialize;
use serde_json::json;
use slicestream_archive::{Kind, SNIFF_LEN, detect};
use slicestream_store::{Ingestor, SpoolError, SpooledBlob, UploadCounts};
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::AppState;
use crate::archive_ingest::{
    EntryResult, NiftiSummary, PatientSummary, Progress, SHUTDOWN_REASON, UploadSource, count,
    process_upload,
};
use crate::config::ArchivesConfig;
use crate::stow::{authorized, remote_addr, unauthorized};

/// Entry results kept per job (newest last).
pub const MAX_JOB_ENTRIES: usize = 10_000;
/// Longest stored upload name, in characters.
const MAX_NAME_CHARS: usize = 255;
/// Free space is re-checked each time this many body bytes are spooled.
const FREE_SPACE_CHECK_BYTES: u64 = 64 * 1024 * 1024;

/// The archive job manager (one per server).
pub struct ArchiveJobs {
    ingestor: Arc<Ingestor>,
    cfg: ArchivesConfig,
    /// Permits for running jobs (`max_concurrent_jobs`).
    running: Arc<Semaphore>,
    inner: Mutex<Inner>,
    tasks: TaskTracker,
    shutdown: CancellationToken,
}

#[derive(Default)]
struct Inner {
    jobs: HashMap<String, Arc<Job>>,
    /// Finished job ids, oldest first.
    finished: VecDeque<String>,
    /// Reserved slots: uploads being received plus queued and running jobs.
    active: usize,
}

/// A place in the job queue, held from before the body is received until
/// the job ends. Released on drop.
pub struct Slot {
    jobs: Arc<ArchiveJobs>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.jobs.lock().active -= 1;
    }
}

struct Job {
    upload_id: i64,
    name: String,
    shutdown: CancellationToken,
    state: Mutex<JobState>,
}

struct JobState {
    status: &'static str,
    reason: Option<String>,
    counts: UploadCounts,
    bytes_expanded: u64,
    entries_seen: u64,
    entries: VecDeque<EntryResult>,
    patients: Vec<PatientSummary>,
    nifti: NiftiSummary,
}

/// `GET /api/archives/{job}` body.
#[derive(Serialize)]
struct JobView<'a> {
    job: &'a str,
    upload_id: i64,
    name: &'a str,
    status: &'static str,
    reason: Option<&'a str>,
    counts: &'a UploadCounts,
    bytes_expanded: u64,
    entries_seen: u64,
    entries: &'a VecDeque<EntryResult>,
    patients: &'a [PatientSummary],
    nifti: &'a NiftiSummary,
}

impl Job {
    fn state(&self) -> MutexGuard<'_, JobState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Progress for Job {
    fn entry(&self, r: &EntryResult) {
        let mut s = self.state();
        count(&mut s.counts, r);
        if s.entries.len() == MAX_JOB_ENTRIES {
            s.entries.pop_front();
        }
        s.entries.push_back(r.clone());
    }

    fn stats(&self, entries_seen: u64, bytes_expanded: u64) {
        let mut s = self.state();
        s.entries_seen = entries_seen;
        s.bytes_expanded = bytes_expanded;
    }

    fn shutdown(&self) -> Option<CancellationToken> {
        Some(self.shutdown.clone())
    }
}

impl ArchiveJobs {
    /// Jobs stop at their next entry once `shutdown` is cancelled.
    pub fn new(
        ingestor: Arc<Ingestor>,
        cfg: ArchivesConfig,
        shutdown: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            ingestor,
            running: Arc::new(Semaphore::new(cfg.max_concurrent_jobs.max(1))),
            cfg,
            inner: Mutex::new(Inner::default()),
            tasks: TaskTracker::new(),
            shutdown,
        })
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Queued plus running jobs allowed at once.
    fn queue_cap(&self) -> usize {
        self.cfg.max_concurrent_jobs.max(1).saturating_mul(4)
    }

    /// Reserve a queue place, or `None` when the queue is full.
    pub fn reserve(self: &Arc<Self>) -> Option<Slot> {
        let mut inner = self.lock();
        if inner.active >= self.queue_cap() {
            return None;
        }
        inner.active += 1;
        Some(Slot {
            jobs: Arc::clone(self),
        })
    }

    /// Queue upload `upload_id` (the spooled `file`) and return its job id.
    /// The spool file is removed when the job ends, whatever the outcome.
    pub fn submit(
        self: &Arc<Self>,
        slot: Slot,
        file: SpooledBlob,
        name: String,
        upload_id: i64,
        remote: Option<SocketAddr>,
    ) -> String {
        let id = format!("{:032x}", rand::random::<u128>());
        let job = Arc::new(Job {
            upload_id,
            name,
            shutdown: self.shutdown.clone(),
            state: Mutex::new(JobState {
                status: "queued",
                reason: None,
                counts: UploadCounts::default(),
                bytes_expanded: 0,
                entries_seen: 0,
                entries: VecDeque::new(),
                patients: Vec::new(),
                nifti: NiftiSummary::default(),
            }),
        });
        self.lock().jobs.insert(id.clone(), Arc::clone(&job));
        let jobs = Arc::clone(self);
        let job_id = id.clone();
        self.tasks.spawn(async move {
            jobs.run(&job, file, remote).await;
            jobs.finished(job_id);
            drop(slot);
        });
        id
    }

    async fn run(&self, job: &Arc<Job>, file: SpooledBlob, remote: Option<SocketAddr>) {
        let permit = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => None,
            p = Arc::clone(&self.running).acquire_owned() => p.ok(),
        };
        let Some(_permit) = permit else {
            // Shut down while queued: nothing was walked.
            if let Err(e) = self
                .ingestor
                .index()
                .finish_upload(job.upload_id, "aborted", &UploadCounts::default())
                .await
            {
                tracing::error!(upload_id = job.upload_id, error = %e, "cannot finish upload row");
            }
            let mut s = job.state();
            s.status = "aborted";
            s.reason = Some(SHUTDOWN_REASON.to_owned());
            return;
        };
        job.state().status = "running";
        let out = process_upload(
            Arc::clone(&self.ingestor),
            file.path().to_path_buf(),
            job.name.clone(),
            job.upload_id,
            UploadSource::Http { remote },
            self.cfg.limits(),
            Arc::clone(job) as Arc<dyn Progress>,
        )
        .await;
        drop(file);
        tracing::info!(
            upload_id = out.upload_id,
            status = out.status,
            reason = out.reason.as_deref().unwrap_or(""),
            "archive upload finished"
        );
        let mut s = job.state();
        s.status = out.status;
        s.reason = out.reason;
        s.counts = out.counts;
        s.entries_seen = out.entries_seen;
        s.bytes_expanded = out.bytes_expanded;
        s.patients = out.patients;
        s.nifti = out.nifti;
    }

    /// Record job `id` as finished and evict the oldest finished jobs
    /// beyond `job_retention`.
    fn finished(&self, id: String) {
        let mut inner = self.lock();
        inner.finished.push_back(id);
        while inner.finished.len() > self.cfg.job_retention.max(1) {
            if let Some(old) = inner.finished.pop_front() {
                inner.jobs.remove(&old);
            }
        }
    }

    /// Wait (at most `grace`) for every job to end; call after `shutdown`
    /// was cancelled. Returns `false` if jobs were still running.
    pub async fn wait(&self, grace: Duration) -> bool {
        self.tasks.close();
        tokio::time::timeout(grace, self.tasks.wait()).await.is_ok()
    }

    /// How long the upload body may stall between chunks.
    fn body_idle_timeout(&self) -> Duration {
        Duration::from_secs(self.cfg.body_idle_timeout_secs.max(1))
    }

    /// Whether the storage volume has room for `len` more bytes plus the
    /// `min_free_bytes` margin. Unknown free space counts as room.
    fn room_for(&self, len: u64) -> bool {
        match fs4::available_space(&self.ingestor.config().root) {
            Ok(free) => free >= len.saturating_add(self.cfg.min_free_bytes),
            Err(e) => {
                tracing::warn!(error = %e, "cannot read free space");
                true
            }
        }
    }
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

/// The upload name from `x-filename`: percent-decoded (the console sends
/// `encodeURIComponent(name)`; invalid UTF-8 is replaced), then its last
/// path segment without control or format characters, at most 255
/// characters; `"upload"` if empty.
pub fn upload_name(headers: &HeaderMap) -> String {
    let raw = headers
        .get("x-filename")
        .map(|v| String::from_utf8_lossy(&percent_decode(v.as_bytes())).into_owned())
        .unwrap_or_default();
    sanitize_name(&raw)
}

/// Decode `%XX` escapes; malformed escapes are kept as they are.
fn percent_decode(raw: &[u8]) -> Vec<u8> {
    let hex = |b: u8| (b as char).to_digit(16).map(|d| d as u8);
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'%'
            && let (Some(hi), Some(lo)) = (
                raw.get(i + 1).copied().and_then(hex),
                raw.get(i + 2).copied().and_then(hex),
            )
        {
            out.push(hi << 4 | lo);
            i += 3;
            continue;
        }
        out.push(raw[i]);
        i += 1;
    }
    out
}

/// Unicode format characters (general category Cf): invisible marks such
/// as zero-width spaces and bidi overrides that can disguise a name.
fn is_format(c: char) -> bool {
    matches!(
        c,
        '\u{AD}'
            | '\u{600}'..='\u{605}'
            | '\u{61C}'
            | '\u{6DD}'
            | '\u{70F}'
            | '\u{890}'..='\u{891}'
            | '\u{8E2}'
            | '\u{180E}'
            | '\u{200B}'..='\u{200F}'
            | '\u{202A}'..='\u{202E}'
            | '\u{2060}'..='\u{2064}'
            | '\u{2066}'..='\u{206F}'
            | '\u{FEFF}'
            | '\u{FFF9}'..='\u{FFFB}'
            | '\u{110BD}'
            | '\u{110CD}'
            | '\u{13430}'..='\u{1343F}'
            | '\u{1BCA0}'..='\u{1BCA3}'
            | '\u{1D173}'..='\u{1D17A}'
            | '\u{E0001}'
            | '\u{E0020}'..='\u{E007F}'
    )
}

/// See [`upload_name`].
fn sanitize_name(raw: &str) -> String {
    let last = raw.rsplit(['/', '\\']).next().unwrap_or("");
    let name: String = last
        .chars()
        .filter(|&c| !c.is_control() && !is_format(c))
        .take(MAX_NAME_CHARS)
        .collect();
    if name.trim().is_empty() {
        "upload".to_owned()
    } else {
        name
    }
}

/// `POST /api/archives`: one archive, NIfTI or DICOM file as the body.
pub async fn post_archive(
    State(state): State<Arc<AppState>>,
    ext: Extensions,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let Some(jobs) = state.archives.as_ref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !authorized(&state.http, &headers) {
        return unauthorized();
    }
    let max = state.http.max_request_bytes;
    let length = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if let Some(len) = length {
        if len > max {
            return StatusCode::PAYLOAD_TOO_LARGE.into_response();
        }
        if !jobs.room_for(len) {
            return error(StatusCode::INSUFFICIENT_STORAGE, "not enough free space");
        }
    }
    let Some(slot) = jobs.reserve() else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "too many archive jobs");
    };

    let spooled = match spool(jobs, &state.ingestor, body, max).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    match sniff(&spooled).await {
        Ok(Kind::Other) => return error(StatusCode::BAD_REQUEST, "unsupported file type"),
        Ok(_) => {}
        Err(e) => {
            tracing::error!(error = %e, "cannot read spooled upload");
            return error(StatusCode::INTERNAL_SERVER_ERROR, "storage error");
        }
    }

    let name = upload_name(&headers);
    let remote = remote_addr(&ext);
    let remote_text = remote.map(|a| a.to_string());
    let upload_id = match state
        .ingestor
        .index()
        .create_upload("http_archive", &name, remote_text.as_deref())
        .await
    {
        Ok(id) => id,
        Err(e) => {
            tracing::error!(error = %e, "cannot create upload row");
            return error(StatusCode::INTERNAL_SERVER_ERROR, "index error");
        }
    };
    let job = jobs.submit(slot, spooled, name, upload_id, remote);
    (
        StatusCode::ACCEPTED,
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(json!({ "job": job, "upload_id": upload_id })),
    )
        .into_response()
}

/// Stream `body` into a spool file in `tmp/` of at most `max` bytes.
///
/// A body that delivers nothing for `body_idle_timeout_secs` gets 408; free
/// space is checked at the first chunk and every
/// [`FREE_SPACE_CHECK_BYTES`] after (507 below `min_free_bytes`), which also
/// covers bodies without `Content-Length`. Any error removes the spool file.
async fn spool(
    jobs: &ArchiveJobs,
    ingestor: &Ingestor,
    body: Body,
    max: u64,
) -> Result<SpooledBlob, Response> {
    let storage_error = |e: &dyn std::fmt::Display| {
        tracing::error!(error = %e, "archive spool failed");
        error(StatusCode::INTERNAL_SERVER_ERROR, "storage error")
    };
    // On any error `temp` is dropped on return, removing its file.
    let mut temp = ingestor
        .blobs()
        .create_temp(max)
        .map_err(|e| storage_error(&e))?;
    let idle = jobs.body_idle_timeout();
    let mut written = 0u64;
    let mut next_check = 0u64;
    let mut stream = body.into_data_stream();
    loop {
        let Ok(next) = tokio::time::timeout(idle, stream.next()).await else {
            return Err(error(StatusCode::REQUEST_TIMEOUT, "request body timed out"));
        };
        let Some(chunk) = next else { break };
        let chunk = chunk.map_err(|e| {
            error(
                StatusCode::BAD_REQUEST,
                &format!("reading request body: {e}"),
            )
        })?;
        match temp.write_chunk(&chunk).await {
            Ok(()) => {}
            Err(SpoolError::TooLarge { .. }) => {
                return Err(StatusCode::PAYLOAD_TOO_LARGE.into_response());
            }
            Err(e) => return Err(storage_error(&e)),
        }
        written = written.saturating_add(chunk.len() as u64);
        if written >= next_check {
            if !jobs.room_for(0) {
                return Err(error(
                    StatusCode::INSUFFICIENT_STORAGE,
                    "not enough free space",
                ));
            }
            next_check = written.saturating_add(FREE_SPACE_CHECK_BYTES);
        }
    }
    temp.finish().await.map_err(|e| storage_error(&e))
}

/// Detect the spooled file's type from its first bytes.
async fn sniff(file: &SpooledBlob) -> std::io::Result<Kind> {
    let mut f = tokio::fs::File::open(file.path()).await?;
    let mut head = vec![0u8; SNIFF_LEN];
    let mut n = 0;
    while n < head.len() {
        let read = f.read(&mut head[n..]).await?;
        if read == 0 {
            break;
        }
        n += read;
    }
    Ok(detect(&head[..n]))
}

/// `GET /api/archives/{job}`: progress and results.
pub async fn get_archive(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    let job = state
        .archives
        .as_ref()
        .and_then(|jobs| jobs.lock().jobs.get(&id).cloned());
    let Some(job) = job else {
        return error(StatusCode::NOT_FOUND, "unknown job");
    };
    let s = job.state();
    let view = JobView {
        job: &id,
        upload_id: job.upload_id,
        name: &job.name,
        status: s.status,
        reason: s.reason.as_deref(),
        counts: &s.counts,
        bytes_expanded: s.bytes_expanded,
        entries_seen: s.entries_seen,
        entries: &s.entries,
        patients: &s.patients,
        nifti: &s.nifti,
    };
    (
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(view),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use slicestream_store::StorageConfig;

    fn name(v: &[u8]) -> String {
        let mut h = HeaderMap::new();
        h.insert("x-filename", HeaderValue::from_bytes(v).unwrap());
        upload_name(&h)
    }

    #[test]
    fn upload_names() {
        assert_eq!(upload_name(&HeaderMap::new()), "upload");
        assert_eq!(name(b"a/b\\c.zip"), "c.zip");
        assert_eq!(name(b"y\tz.zip"), "yz.zip");
        assert_eq!(sanitize_name("x\x7f\ny\u{9b}z.zip"), "xyz.zip");
        assert_eq!(name(b"dir/"), "upload");
        assert_eq!(name("é".repeat(300).as_bytes()), "é".repeat(255));
    }

    #[test]
    fn upload_names_are_percent_decoded() {
        assert_eq!(name(b"plain-name_1.zip"), "plain-name_1.zip");
        assert_eq!(name(b"my%20scan.zip"), "my scan.zip");
        assert_eq!(name(b"%C3%A9t%C3%A9.nii.gz"), "été.nii.gz");
        // Decoded before the last segment is taken: %2F still splits.
        assert_eq!(name(b"a%2Fb%5Cc.zip"), "c.zip");
        // Invalid UTF-8 after decoding is replaced, not rejected.
        assert_eq!(name(b"x%FFy.zip"), "x\u{fffd}y.zip");
        // Malformed escapes stay as they are.
        assert_eq!(name(b"100%.zip"), "100%.zip");
        assert_eq!(name(b"a%zz%4.zip"), "a%zz%4.zip");
        // Encoded control characters are still dropped.
        assert_eq!(name(b"a%0Ab%00.zip"), "ab.zip");
    }

    #[test]
    fn upload_names_drop_format_characters() {
        assert_eq!(sanitize_name("a\u{200B}b\u{200F}c.zip"), "abc.zip");
        assert_eq!(sanitize_name("evil\u{202E}piz.exe"), "evilpiz.exe");
        assert_eq!(sanitize_name("\u{2066}x\u{2069}\u{FEFF}.nii"), "x.nii");
        assert_eq!(sanitize_name("soft\u{AD}hyphen"), "softhyphen");
        assert_eq!(sanitize_name("\u{200E}\u{202A}"), "upload");
        assert_eq!(name(b"a%E2%80%AEb.zip"), "ab.zip");
        // Ordinary non-ASCII letters are kept.
        assert_eq!(sanitize_name("Müller 日本.zip"), "Müller 日本.zip");
    }

    #[tokio::test]
    async fn queue_cap_is_four_times_concurrency() {
        let dir = tempfile::tempdir().unwrap();
        let ingestor = Ingestor::open(StorageConfig {
            root: dir.path().to_path_buf(),
            ..StorageConfig::default()
        })
        .await
        .unwrap();
        let jobs = ArchiveJobs::new(
            ingestor,
            ArchivesConfig {
                max_concurrent_jobs: 1,
                ..ArchivesConfig::default()
            },
            CancellationToken::new(),
        );
        let slots: Vec<Slot> = (0..4).map(|_| jobs.reserve().unwrap()).collect();
        assert!(jobs.reserve().is_none());
        drop(slots);
        assert!(jobs.reserve().is_some());
    }

    #[tokio::test]
    async fn queued_job_aborts_on_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let ingestor = Ingestor::open(StorageConfig {
            root: dir.path().to_path_buf(),
            ..StorageConfig::default()
        })
        .await
        .unwrap();
        let shutdown = CancellationToken::new();
        let jobs = ArchiveJobs::new(
            Arc::clone(&ingestor),
            ArchivesConfig::default(),
            shutdown.clone(),
        );
        // Both run permits taken: the job stays queued.
        let held = Arc::clone(&jobs.running).acquire_many_owned(2).await;
        let mut temp = ingestor.blobs().create_temp(1 << 20).unwrap();
        temp.write_chunk(b"PK\x03\x04").await.unwrap();
        let spooled = temp.finish().await.unwrap();
        let upload = ingestor
            .index()
            .create_upload("http_archive", "a.zip", None)
            .await
            .unwrap();
        let slot = jobs.reserve().unwrap();
        let id = jobs.submit(slot, spooled, "a.zip".into(), upload, None);
        assert_eq!(jobs.lock().jobs[&id].state().status, "queued");

        shutdown.cancel();
        assert!(jobs.wait(Duration::from_secs(10)).await);
        drop(held);
        let job = Arc::clone(&jobs.lock().jobs[&id]);
        assert_eq!(job.state().status, "aborted");
        assert_eq!(job.state().reason.as_deref(), Some(SHUTDOWN_REASON));
        let row = ingestor.index().upload(upload).await.unwrap().unwrap();
        assert_eq!(row.status, "aborted");
        assert_eq!(jobs.lock().active, 0);
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
    }
}
