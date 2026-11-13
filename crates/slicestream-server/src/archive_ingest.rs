//! Upload processor: walk an uploaded file (or an import folder) with the
//! archive walker, ingest every DICOM and NIfTI leaf, record per-entry
//! results, then link NIfTI volumes to patients/studies and summarise.
//!
//! The walker runs on a blocking thread. Each leaf is spooled there with
//! [`Ingestor::spool_sync`] and handed over a bounded channel to an async
//! consumer that commits it, so extraction and committing overlap while at
//! most a few spooled entries wait on disk. Nothing is held whole in memory.

use std::collections::{HashMap, HashSet};
use std::io::{self, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;
use slicestream_archive::{
    Event, Kind, Leaf, Limits, Sink, WalkError, WalkStats, parse_nifti_header, walk_file,
    walk_file_continue,
};
use slicestream_store::{
    Expectations, IngestError, IngestSource, Ingestor, NiftiMeta, SpooledBlob, UploadCounts,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::linking;

/// Spooled entries that may wait for the consumer.
const CHANNEL_CAPACITY: usize = 8;

/// What happened to one entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryOutcome {
    Stored,
    Duplicate,
    Replaced,
    Skipped,
    Failed,
}

/// Result for one entry of an upload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EntryResult {
    /// Upload-relative path; nested archive members are joined with `!/`.
    pub path: String,
    /// `"dicom"`, `"nifti"`, `"archive"` or `"other"`.
    pub kind: &'static str,
    pub outcome: EntryOutcome,
    /// DICOM status as 4 hex digits (e.g. `"C000"`) for failed DICOM.
    pub code: Option<String>,
    pub reason: Option<String>,
    pub sop_instance_uid: Option<String>,
    pub size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PatientSummary {
    pub patient_id: String,
    pub patient_name: Option<String>,
    pub studies: Vec<StudySummary>,
}

/// One study touched by the upload. `series` and `instances` count the
/// stored or duplicate DICOM of this upload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StudySummary {
    pub study_uid: String,
    pub study_date: Option<String>,
    pub description: Option<String>,
    pub series: u64,
    pub instances: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct NiftiSummary {
    pub linked: u64,
    pub unassigned: u64,
}

/// Live progress of an upload (the HTTP job state and the CLI implement it).
/// Called from the async consumer, in order.
pub trait Progress: Send + Sync {
    fn entry(&self, r: &EntryResult);
    /// Walker totals so far: after each counted entry (bytes lag by up to
    /// one entry) and exactly after each walked file, also on abort.
    fn stats(&self, entries_seen: u64, bytes_expanded: u64);
    /// The server's shutdown token, if any. Once it is cancelled the upload
    /// stops at the next entry and ends `aborted` with reason
    /// [`SHUTDOWN_REASON`]; entries already stored stay stored.
    fn shutdown(&self) -> Option<CancellationToken> {
        None
    }
}

/// Abort reason of an upload stopped by [`Progress::shutdown`].
pub const SHUTDOWN_REASON: &str = "server shutting down";

/// Add one entry result to `counts` (as the `uploads` row counts it).
pub fn count(counts: &mut UploadCounts, r: &EntryResult) {
    let c = counts;
    match (r.outcome, r.kind) {
        (EntryOutcome::Stored | EntryOutcome::Replaced, "nifti") => c.nifti_stored += 1,
        (EntryOutcome::Stored | EntryOutcome::Replaced, _) => c.dicom_stored += 1,
        (EntryOutcome::Duplicate, "nifti") => c.nifti_duplicate += 1,
        (EntryOutcome::Duplicate, _) => c.dicom_duplicate += 1,
        (EntryOutcome::Skipped, _) => c.skipped += 1,
        (EntryOutcome::Failed, _) => c.failed += 1,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UploadOutcome {
    pub upload_id: i64,
    /// `"done"` or `"aborted"`.
    pub status: &'static str,
    pub reason: Option<String>,
    pub counts: UploadCounts,
    pub patients: Vec<PatientSummary>,
    pub nifti: NiftiSummary,
    /// Walker totals, including an aborted walk.
    pub entries_seen: u64,
    pub bytes_expanded: u64,
}

/// Where an upload came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadSource {
    Http { remote: Option<SocketAddr> },
    Import { root: PathBuf },
}

/// Process one uploaded file (archive, NIfTI or DICOM) as upload
/// `upload_id`: walk it, ingest its leaves, link NIfTI, finish the
/// `uploads` row. Entries already stored stay stored if the walk aborts.
pub async fn process_upload(
    ingestor: Arc<Ingestor>,
    file: PathBuf,
    display_name: String,
    upload_id: i64,
    source: UploadSource,
    limits: Limits,
    progress: Arc<dyn Progress>,
) -> UploadOutcome {
    let source = match source {
        UploadSource::Http { remote } => IngestSource::Http { remote },
        UploadSource::Import { root } => IngestSource::Import { path: root },
    };
    let tmp = tmp_dir(&ingestor);
    run(ingestor, upload_id, progress, Vec::new(), move |sink| {
        sink.source = source;
        let (stats, res) = walk_file(&file, &display_name, &tmp, &limits, sink);
        sink.stats(stats);
        res
    })
    .await
}

/// Process every file of `dir` (and its sub-directories when `recursive`)
/// as one upload `upload_id`, in path order. Each file is classified by
/// content: DICOM and NIfTI are entries named by their path relative to
/// `dir`; archives are walked under that name; anything else is skipped.
///
/// The folder is one upload for the limits: every file, loose or inside an
/// archive, counts toward `max_entries` and `max_expanded_bytes`. A file
/// that cannot be opened or read is a failed entry; the import goes on.
pub async fn process_dir(
    ingestor: Arc<Ingestor>,
    dir: &Path,
    recursive: bool,
    upload_id: i64,
    limits: Limits,
    progress: Arc<dyn Progress>,
) -> UploadOutcome {
    let mut pre = Vec::new();
    let files = list_files(dir, recursive, &mut pre).await;
    let tmp = tmp_dir(&ingestor);
    run(ingestor, upload_id, progress, pre, move |sink| {
        let mut spent = WalkStats::default();
        for (path, name) in files {
            sink.source = IngestSource::Import { path: path.clone() };
            let (stats, res) = walk_file_continue(&path, &name, &tmp, &limits, spent, sink);
            match res {
                // Nothing was counted: the file itself could not be opened
                // or read (a sink failure always follows a counted entry).
                Err(WalkError::Io(e)) if stats == spent => {
                    let r = failed(&name, "other", None, format!("cannot read file: {e}"));
                    sink.send(Item::Done(r)).map_err(WalkError::Io)?;
                }
                res => {
                    spent = stats;
                    sink.stats(spent);
                    res?;
                }
            }
        }
        Ok(())
    })
    .await
}

fn tmp_dir(ingestor: &Ingestor) -> PathBuf {
    ingestor.config().root.join("tmp")
}

/// Regular files under `dir` as `(path, name relative to dir)`, sorted.
/// Unreadable directories become failed entries in `failed`.
async fn list_files(
    dir: &Path,
    recursive: bool,
    failed_out: &mut Vec<EntryResult>,
) -> Vec<(PathBuf, String)> {
    let mut out = Vec::new();
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(d) = dirs.pop() {
        let (subdirs, files) = crate::list_dir(&d, &mut |p: &Path, reason: String| {
            failed_out.push(failed(&relative(dir, p), "other", None, reason));
        })
        .await;
        if recursive {
            dirs.extend(subdirs);
        }
        out.extend(files.into_iter().map(|p| {
            let name = relative(dir, &p);
            (p, name)
        }));
    }
    out.sort();
    out
}

/// `path` relative to `base`, `/`-separated.
fn relative(base: &Path, path: &Path) -> String {
    let rel = path.strip_prefix(base).unwrap_or(path);
    let parts: Vec<_> = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect();
    if parts.is_empty() {
        ".".to_owned()
    } else {
        parts.join("/")
    }
}

fn failed(path: &str, kind: &'static str, code: Option<String>, reason: String) -> EntryResult {
    EntryResult {
        path: path.to_owned(),
        kind,
        outcome: EntryOutcome::Failed,
        code,
        reason: Some(reason),
        sop_instance_uid: None,
        size: None,
    }
}

fn kind_str(kind: Kind) -> &'static str {
    match kind {
        Kind::Dicom => "dicom",
        Kind::Nifti => "nifti",
        _ => "other",
    }
}

/// Work handed from the walker thread to the consumer.
enum Item {
    Dicom {
        path: String,
        source: IngestSource,
        spooled: SpooledBlob,
    },
    Nifti {
        path: String,
        meta: NiftiMeta,
        spooled: SpooledBlob,
    },
    /// Settled on the walker thread (skipped or failed).
    Done(EntryResult),
    /// Walker totals so far.
    Stats(u64, u64),
}

/// The walker's sink: spools leaves and sends them to the consumer.
struct ChannelSink {
    ingestor: Arc<Ingestor>,
    tx: mpsc::Sender<Item>,
    source: IngestSource,
    /// Path and kind of the leaf last handed to `leaf`, so that an
    /// `EntryFailed` for it (its reader failed) reports the right kind.
    last_leaf: Option<(String, &'static str)>,
}

impl ChannelSink {
    /// Fails once the consumer is gone, which stops the walk.
    fn send(&self, item: Item) -> io::Result<()> {
        self.tx
            .blocking_send(item)
            .map_err(|_| io::Error::other("upload processing stopped"))
    }

    /// If the consumer is gone the next leaf stops the walk.
    fn stats(&self, s: WalkStats) {
        let _ = self.send(Item::Stats(s.entries_seen, s.bytes_expanded));
    }
}

/// Records whether the wrapped reader failed (vs. our own spool writes).
struct Tracked<'a> {
    inner: &'a mut dyn Read,
    failed: bool,
}

impl Read for Tracked<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let r = self.inner.read(buf);
        if let Err(e) = &r
            && e.kind() != io::ErrorKind::Interrupted
        {
            self.failed = true;
        }
        r
    }
}

impl Sink for ChannelSink {
    fn leaf(&mut self, leaf: Leaf<'_>) -> io::Result<()> {
        let path = leaf.path.to_owned();
        let kind = kind_str(leaf.kind);
        self.last_leaf = Some((path.clone(), kind));
        let meta = if leaf.kind == Kind::Nifti {
            match parse_nifti_header(leaf.head) {
                Ok(h) => Some(NiftiMeta {
                    version: h.version,
                    dims: h.dims,
                    pixdim: h.pixdim,
                    datatype: h.datatype,
                    descrip: h.descrip,
                    // From the walker's content check, never the name.
                    compressed: leaf.compressed,
                    original_name: path.rsplit('/').next().unwrap_or(&path).to_owned(),
                }),
                Err(_) => {
                    let r = failed(&path, kind, None, "invalid NIfTI header".into());
                    return self.send(Item::Done(r));
                }
            }
        } else {
            None
        };
        let mut reader = Tracked {
            inner: leaf.reader,
            failed: false,
        };
        let spooled = match self.ingestor.spool_sync(&mut reader) {
            Ok(s) => s,
            // Broken input or a limit abort: the partial spool is already
            // removed; the walker reports the entry (or aborts).
            Err(e) if reader.failed => return Err(io::Error::other(e)),
            Err(e @ IngestError::TooLarge { .. }) => {
                let code = (kind == "dicom").then(|| format!("{:04X}", e.status()));
                return self.send(Item::Done(failed(&path, kind, code, e.to_string())));
            }
            // Our own storage failed: stop the walk.
            Err(e) => return Err(io::Error::other(e)),
        };
        self.send(match meta {
            Some(meta) => Item::Nifti {
                path,
                meta,
                spooled,
            },
            None => Item::Dicom {
                path,
                source: self.source.clone(),
                spooled,
            },
        })
    }

    fn event(&mut self, ev: Event<'_>) {
        let r = match ev {
            Event::Skipped { path } => EntryResult {
                path: path.to_owned(),
                kind: "other",
                outcome: EntryOutcome::Skipped,
                code: None,
                reason: None,
                sop_instance_uid: None,
                size: None,
            },
            Event::ContainerFailed { path, reason } => failed(path, "archive", None, reason),
            Event::EntryFailed { path, reason } => {
                let kind = match &self.last_leaf {
                    Some((p, kind)) if p == path => kind,
                    _ => "other",
                };
                failed(path, kind, None, reason)
            }
        };
        // If the consumer is gone the next leaf stops the walk.
        let _ = self.send(Item::Done(r));
    }

    fn progress(&mut self, entries_seen: u64, bytes_expanded: u64) {
        let _ = self.send(Item::Stats(entries_seen, bytes_expanded));
    }
}

/// Per-study tallies of this upload's stored/duplicate DICOM.
#[derive(Default)]
struct StudyTally {
    series: HashSet<String>,
    instances: HashSet<String>,
}

/// Consumer-side state.
struct Collect {
    ingestor: Arc<Ingestor>,
    upload_id: i64,
    progress: Arc<dyn Progress>,
    counts: UploadCounts,
    /// `(path, patient_fk, study_fk)` of stored/duplicate DICOM.
    dicom: Vec<(String, i64, i64)>,
    /// `(nifti_id, path, duplicate)`.
    nifti: Vec<(i64, String, bool)>,
    studies: HashMap<i64, StudyTally>,
    entries_seen: u64,
    bytes_expanded: u64,
}

impl Collect {
    fn record(&mut self, r: EntryResult) {
        count(&mut self.counts, &r);
        self.progress.entry(&r);
    }

    async fn handle(&mut self, item: Item) {
        match item {
            Item::Done(r) => self.record(r),
            Item::Stats(entries, bytes) => {
                self.entries_seen = entries;
                self.bytes_expanded = bytes;
                self.progress.stats(entries, bytes);
            }
            Item::Dicom {
                path,
                source,
                spooled,
            } => {
                let size = spooled.len;
                let r = match self
                    .ingestor
                    .ingest(spooled, source, Expectations::default())
                    .await
                {
                    Ok(s) => {
                        self.note_dicom(&path, &s.sop_instance_uid, &s.series_instance_uid)
                            .await;
                        let outcome = if s.replaced {
                            EntryOutcome::Replaced
                        } else if s.duplicate {
                            EntryOutcome::Duplicate
                        } else {
                            EntryOutcome::Stored
                        };
                        EntryResult {
                            path,
                            kind: "dicom",
                            outcome,
                            code: None,
                            reason: None,
                            sop_instance_uid: Some(s.sop_instance_uid),
                            size: Some(size),
                        }
                    }
                    Err(f) => EntryResult {
                        code: Some(format!("{:04X}", f.status())),
                        sop_instance_uid: f.sop_instance_uid.clone(),
                        size: Some(size),
                        ..failed(&path, "dicom", None, f.error.to_string())
                    },
                };
                self.record(r);
            }
            Item::Nifti {
                path,
                meta,
                spooled,
            } => {
                let size = spooled.len;
                let r = match self
                    .ingestor
                    .ingest_nifti(spooled, meta, self.upload_id)
                    .await
                {
                    Ok(n) => {
                        self.nifti.push((n.id, path.clone(), n.duplicate));
                        EntryResult {
                            path,
                            kind: "nifti",
                            outcome: if n.duplicate {
                                EntryOutcome::Duplicate
                            } else {
                                EntryOutcome::Stored
                            },
                            code: None,
                            reason: None,
                            sop_instance_uid: None,
                            size: Some(size),
                        }
                    }
                    Err(e) => EntryResult {
                        size: Some(size),
                        ..failed(&path, "nifti", None, e.to_string())
                    },
                };
                self.record(r);
            }
        }
    }

    /// Remember a stored/duplicate instance for linking and the summary.
    async fn note_dicom(&mut self, path: &str, sop_uid: &str, series_uid: &str) {
        match self.ingestor.index().instance_links(sop_uid).await {
            Ok(Some((patient, study))) => {
                self.dicom.push((path.to_owned(), patient, study));
                let t = self.studies.entry(study).or_default();
                t.series.insert(series_uid.to_owned());
                t.instances.insert(sop_uid.to_owned());
            }
            Ok(None) => tracing::warn!(sop_uid, "stored instance not found in index"),
            Err(e) => tracing::warn!(sop_uid, error = %e, "cannot read instance links"),
        }
    }

    /// Link this upload's NIfTI; returns the summary. A volume that was
    /// already stored (duplicate) is only re-linked when a patient is found,
    /// so an earlier link is never cleared; when that is its current patient
    /// and no single study is found, its current study is kept.
    ///
    /// Path matching runs on a blocking thread; it is skipped entirely when
    /// the upload has no NIfTI.
    async fn link(&mut self) -> NiftiSummary {
        let mut summary = NiftiSummary::default();
        if self.nifti.is_empty() {
            return summary;
        }
        let index = self.ingestor.index();
        let pairs: Vec<(i64, String)> = self
            .nifti
            .iter()
            .map(|(id, p, _)| (*id, p.clone()))
            .collect();
        // Not needed after linking (the summary uses `studies`).
        let dicom = std::mem::take(&mut self.dicom);
        let links = match tokio::task::spawn_blocking(move || linking::link(&pairs, &dicom)).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(upload_id = self.upload_id, error = %e, "NIfTI linking failed");
                Vec::new()
            }
        };
        // On failure every volume counts as unassigned (or kept, if it was
        // a duplicate already linked).
        let links = if links.len() == self.nifti.len() {
            links
        } else {
            self.nifti
                .iter()
                .map(|(id, _, _)| (*id, None, None))
                .collect()
        };
        for ((id, patient, study), (_, _, duplicate)) in links.into_iter().zip(&self.nifti) {
            let linked = if patient.is_some() {
                // A duplicate already linked to this patient keeps its
                // study when this upload cannot pin one down.
                let study = if *duplicate && study.is_none() {
                    match index.nifti_volume(id).await {
                        Ok(Some(row)) if row.patient_fk == patient => row.study_fk,
                        _ => None,
                    }
                } else {
                    study
                };
                if let Err(e) = index.link_nifti(id, patient, study).await {
                    tracing::error!(nifti_id = id, error = %e, "cannot link NIfTI volume");
                }
                true
            } else if *duplicate {
                matches!(index.nifti_volume(id).await, Ok(Some(row)) if row.patient_fk.is_some())
            } else {
                false
            };
            if linked {
                summary.linked += 1;
            } else {
                summary.unassigned += 1;
            }
        }
        summary
    }

    async fn patients(&self) -> Vec<PatientSummary> {
        let mut by_patient: HashMap<i64, PatientSummary> = HashMap::new();
        for (study_fk, tally) in &self.studies {
            let info = match self.ingestor.index().study_info(*study_fk).await {
                Ok(Some(i)) => i,
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(study_fk, error = %e, "cannot read study");
                    continue;
                }
            };
            let p = by_patient
                .entry(info.patient_fk)
                .or_insert_with(|| PatientSummary {
                    patient_id: info.patient_id.clone(),
                    patient_name: info.patient_name.clone(),
                    studies: Vec::new(),
                });
            p.studies.push(StudySummary {
                study_uid: info.study_instance_uid,
                study_date: info.study_date,
                description: info.study_description,
                series: tally.series.len() as u64,
                instances: tally.instances.len() as u64,
            });
        }
        let mut out: Vec<PatientSummary> = by_patient.into_values().collect();
        for p in &mut out {
            p.studies.sort_by(|a, b| a.study_uid.cmp(&b.study_uid));
        }
        out.sort_by(|a, b| a.patient_id.cmp(&b.patient_id));
        out
    }
}

/// Run `walk` on a blocking thread with a [`ChannelSink`], consume its
/// items, then link, finish the upload row and summarise. `pre` are results
/// settled before the walk (e.g. unreadable directories).
async fn run<F>(
    ingestor: Arc<Ingestor>,
    upload_id: i64,
    progress: Arc<dyn Progress>,
    pre: Vec<EntryResult>,
    walk: F,
) -> UploadOutcome
where
    F: FnOnce(&mut ChannelSink) -> Result<(), WalkError> + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);
    // A token nobody cancels stands in when there is no shutdown token.
    let shutdown = progress.shutdown().unwrap_or_default();
    let mut sink = ChannelSink {
        ingestor: Arc::clone(&ingestor),
        tx,
        source: IngestSource::Http { remote: None },
        last_leaf: None,
    };
    // The sink (and its sender) is dropped when the walk returns or
    // unwinds, which ends the loop below.
    let walker = tokio::task::spawn_blocking(move || walk(&mut sink));

    let mut c = Collect {
        ingestor,
        upload_id,
        progress,
        counts: UploadCounts::default(),
        dicom: Vec::new(),
        nifti: Vec::new(),
        studies: HashMap::new(),
        entries_seen: 0,
        bytes_expanded: 0,
    };
    for r in pre {
        c.record(r);
    }
    let mut stopped = false;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled(), if !stopped => {
                // The walker's next send fails, which ends its walk.
                stopped = true;
                rx.close();
            }
            item = rx.recv() => match item {
                // After shutdown, spooled leaves still queued are dropped
                // (their spool files with them); results and totals are kept.
                Some(item @ (Item::Done(_) | Item::Stats(..))) => c.handle(item).await,
                Some(item) if !stopped => c.handle(item).await,
                Some(_) => {}
                None => break,
            },
        }
    }

    let (status, reason) = match walker.await {
        Ok(_) if stopped => ("aborted", Some(SHUTDOWN_REASON.to_owned())),
        Ok(Ok(())) => ("done", None),
        Ok(Err(WalkError::Aborted(r))) => ("aborted", Some(r)),
        Ok(Err(e @ WalkError::Io(_))) => {
            tracing::error!(upload_id, error = %e, "upload walk failed");
            ("aborted", Some(e.to_string()))
        }
        Err(e) => {
            tracing::error!(upload_id, error = %e, "upload walker panicked");
            ("aborted", Some("internal error".to_owned()))
        }
    };

    let nifti = c.link().await;
    if let Err(e) = c
        .ingestor
        .index()
        .finish_upload(upload_id, status, &c.counts)
        .await
    {
        tracing::error!(upload_id, error = %e, "cannot finish upload row");
    }
    let patients = c.patients().await;
    UploadOutcome {
        upload_id,
        status,
        reason,
        counts: c.counts,
        patients,
        nifti,
        entries_seen: c.entries_seen,
        bytes_expanded: c.bytes_expanded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slicestream_store::StorageConfig;

    struct Null;
    impl Progress for Null {
        fn entry(&self, _: &EntryResult) {}
        fn stats(&self, _: u64, _: u64) {}
    }

    #[tokio::test]
    async fn walker_panic_aborts_upload() {
        let dir = tempfile::tempdir().unwrap();
        let ingestor = Ingestor::open(StorageConfig {
            root: dir.path().to_path_buf(),
            ..StorageConfig::default()
        })
        .await
        .unwrap();
        let id = ingestor
            .index()
            .create_upload("http_archive", "x", None)
            .await
            .unwrap();
        let out = run(
            Arc::clone(&ingestor),
            id,
            Arc::new(Null),
            Vec::new(),
            |sink| {
                sink.send(Item::Done(failed("a", "other", None, "r".into())))
                    .unwrap();
                panic!("walker bug");
            },
        )
        .await;
        assert_eq!(out.status, "aborted");
        assert_eq!(out.reason.as_deref(), Some("internal error"));
        assert_eq!(out.counts.failed, 1);
        let row = ingestor.index().upload(id).await.unwrap().unwrap();
        assert_eq!(row.status, "aborted");
    }

    #[tokio::test]
    async fn consumer_gone_stops_walk() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let dir = tempfile::tempdir().unwrap();
        let ingestor = Ingestor::open(StorageConfig {
            root: dir.path().to_path_buf(),
            ..StorageConfig::default()
        })
        .await
        .unwrap();
        let err = tokio::task::spawn_blocking(move || {
            let mut sink = ChannelSink {
                ingestor,
                tx,
                source: IngestSource::Http { remote: None },
                last_leaf: None,
            };
            let mut data: &[u8] = &[0u8; 200];
            sink.leaf(Leaf {
                path: "IM1",
                kind: Kind::Dicom,
                compressed: false,
                head: &[],
                reader: &mut data,
            })
        })
        .await
        .unwrap();
        assert!(err.is_err());
        // The spooled blob was dropped with the failed send.
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
    }

    struct Cancelled(CancellationToken);
    impl Progress for Cancelled {
        fn entry(&self, _: &EntryResult) {}
        fn stats(&self, _: u64, _: u64) {}
        fn shutdown(&self) -> Option<CancellationToken> {
            Some(self.0.clone())
        }
    }

    #[tokio::test]
    async fn shutdown_stops_walk_at_next_entry() {
        let dir = tempfile::tempdir().unwrap();
        let ingestor = Ingestor::open(StorageConfig {
            root: dir.path().to_path_buf(),
            ..StorageConfig::default()
        })
        .await
        .unwrap();
        let id = ingestor
            .index()
            .create_upload("http_archive", "x", None)
            .await
            .unwrap();
        let token = CancellationToken::new();
        let progress = Arc::new(Cancelled(token.clone()));
        let (sent_tx, sent_rx) = std::sync::mpsc::channel();
        let walk = tokio::spawn(run(
            Arc::clone(&ingestor),
            id,
            progress,
            Vec::new(),
            move |sink| {
                // An endless walk: only a failed send ends it.
                let mut n = 0u64;
                loop {
                    let mut data: &[u8] = &[1u8; 64];
                    let spooled = sink.ingestor.spool_sync(&mut data).unwrap();
                    let item = Item::Dicom {
                        path: format!("e{n}"),
                        source: IngestSource::Http { remote: None },
                        spooled,
                    };
                    if sink.send(item).is_err() {
                        return Ok(());
                    }
                    n += 1;
                    let _ = sent_tx.send(n);
                }
            },
        ));
        // Let a few entries through, then shut down.
        tokio::task::spawn_blocking(move || {
            for _ in 0..3 {
                sent_rx.recv().unwrap();
            }
        })
        .await
        .unwrap();
        token.cancel();
        let out = tokio::time::timeout(std::time::Duration::from_secs(10), walk)
            .await
            .expect("walk stops")
            .unwrap();
        assert_eq!(out.status, "aborted");
        assert_eq!(out.reason.as_deref(), Some(SHUTDOWN_REASON));
        assert!(out.counts.failed >= 1, "{:?}", out.counts);
        let row = ingestor.index().upload(id).await.unwrap().unwrap();
        assert_eq!(row.status, "aborted");
        // Queued spools were removed.
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
    }

    #[test]
    fn relative_names() {
        let base = Path::new("/a/b");
        assert_eq!(relative(base, Path::new("/a/b/c/d.dcm")), "c/d.dcm");
        assert_eq!(relative(base, Path::new("/a/b")), ".");
    }
}
