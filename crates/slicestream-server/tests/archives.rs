//! Upload processor: archives walked, entries ingested, NIfTI linked.
//! All fixtures are synthesized in-test (no real patient data).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use slicestream_archive::Limits;
use slicestream_server::archive_ingest::{
    EntryOutcome, EntryResult, NiftiSummary, PatientSummary, Progress, StudySummary, UploadOutcome,
    UploadSource, process_dir, process_upload,
};
use slicestream_store::{Expectations, IngestSource, Ingestor, StorageConfig, UploadCounts};

mod support;
use support::*;

// ---------------------------------------------------------------- harness

#[derive(Default)]
struct Recorder {
    entries: Mutex<Vec<EntryResult>>,
    stats: Mutex<Vec<(u64, u64)>>,
}

impl Progress for Recorder {
    fn entry(&self, r: &EntryResult) {
        self.entries.lock().unwrap().push(r.clone());
    }
    fn stats(&self, entries_seen: u64, bytes_expanded: u64) {
        self.stats
            .lock()
            .unwrap()
            .push((entries_seen, bytes_expanded));
    }
}

impl Recorder {
    fn entries(&self) -> Vec<EntryResult> {
        self.entries.lock().unwrap().clone()
    }

    fn outcome_of(&self, path: &str) -> EntryResult {
        self.entries()
            .into_iter()
            .find(|e| e.path == path)
            .unwrap_or_else(|| panic!("no entry {path}: {:?}", self.entries()))
    }
}

struct Env {
    ingestor: Arc<Ingestor>,
    root: PathBuf,
    input: PathBuf,
    _dir: tempfile::TempDir,
}

async fn env_with(storage: impl FnOnce(&mut StorageConfig)) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let mut cfg = StorageConfig {
        root: root.clone(),
        ..StorageConfig::default()
    };
    storage(&mut cfg);
    let ingestor = Ingestor::open(cfg).await.unwrap();
    let input = dir.path().join("in");
    std::fs::create_dir_all(&input).unwrap();
    Env {
        ingestor,
        root,
        input,
        _dir: dir,
    }
}

async fn env() -> Env {
    env_with(|_| {}).await
}

impl Env {
    /// Write `bytes` as upload file `name` and process it.
    async fn upload(
        &self,
        name: &str,
        bytes: &[u8],
        limits: Limits,
    ) -> (UploadOutcome, Arc<Recorder>) {
        let file = self.input.join(name);
        std::fs::write(&file, bytes).unwrap();
        let id = self
            .ingestor
            .index()
            .create_upload("http_archive", name, None)
            .await
            .unwrap();
        let rec = Arc::new(Recorder::default());
        let out = process_upload(
            Arc::clone(&self.ingestor),
            file,
            name.to_string(),
            id,
            UploadSource::Http { remote: None },
            limits,
            rec.clone(),
        )
        .await;
        assert_eq!(out.upload_id, id);
        (out, rec)
    }

    async fn links(&self, sop_uid: &str) -> (i64, i64) {
        self.ingestor
            .index()
            .instance_links(sop_uid)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{sop_uid} not indexed"))
    }

    /// `(original_name, patient_fk, study_fk, compressed)` of NIfTI rows 1..=n.
    async fn nifti_rows(&self, n: i64) -> Vec<(String, Option<i64>, Option<i64>, bool)> {
        let mut out = Vec::new();
        for id in 1..=n {
            let r = self
                .ingestor
                .index()
                .nifti_volume(id)
                .await
                .unwrap()
                .unwrap();
            out.push((
                r.meta.original_name,
                r.patient_fk,
                r.study_fk,
                r.meta.compressed,
            ));
        }
        out.sort();
        out
    }

    fn assert_tmp_empty(&self) {
        let left: Vec<_> = std::fs::read_dir(self.root.join("tmp"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert!(left.is_empty(), "tmp not empty: {left:?}");
    }
}

fn patient(id: u32, studies: Vec<StudySummary>) -> PatientSummary {
    PatientSummary {
        patient_id: format!("P{id}"),
        patient_name: Some(format!("TEST^PATIENT{id}")),
        studies,
    }
}

fn study(patient: u32, study: u32, series: u64, instances: u64) -> StudySummary {
    StudySummary {
        study_uid: format!("2.25.9{patient}0{study}"),
        study_date: Some("20260101".into()),
        description: Some(format!("Study {study}")),
        series,
        instances,
    }
}

// ---------------------------------------------------------------- tests

#[tokio::test]
async fn zip_single_patient_counts() {
    let e = env().await;
    let z = zip(&[
        ("ct/IM1", &dicom(1, 1, 1, 1)),
        ("ct/IM2", &dicom(1, 1, 1, 2)),
        ("ct/IM3", &dicom(1, 1, 2, 3)),
        ("readme.txt", b"hello"),
    ]);
    let (out, rec) = e.upload("p1.zip", &z, Limits::default()).await;
    assert_eq!(out.status, "done");
    assert_eq!(out.reason, None);
    let counts = UploadCounts {
        dicom_stored: 3,
        skipped: 1,
        ..Default::default()
    };
    assert_eq!(out.counts, counts);
    assert_eq!(out.patients, vec![patient(1, vec![study(1, 1, 2, 3)])]);
    assert_eq!(
        out.nifti,
        NiftiSummary {
            linked: 0,
            unassigned: 0
        }
    );
    assert_eq!(out.entries_seen, 4);
    assert!(out.bytes_expanded > 0);

    let row = e
        .ingestor
        .index()
        .upload(out.upload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, "done");
    assert!(row.finished_at.is_some());
    assert_eq!(row.counts, counts);

    let im1 = rec.outcome_of("p1.zip!/ct/IM1");
    assert_eq!(im1.kind, "dicom");
    assert_eq!(im1.outcome, EntryOutcome::Stored);
    assert_eq!(im1.sop_instance_uid, Some(sop(1, 1, 1, 1)));
    assert_eq!(im1.size, Some(dicom(1, 1, 1, 1).len() as u64));
    let txt = rec.outcome_of("p1.zip!/readme.txt");
    assert_eq!((txt.kind, txt.outcome), ("other", EntryOutcome::Skipped));
    assert_eq!(rec.entries().len(), 4);
    // Live progress: one call per counted entry, then the final totals.
    let seen: Vec<u64> = rec.stats.lock().unwrap().iter().map(|s| s.0).collect();
    assert_eq!(seen, vec![1, 2, 3, 4, 4]);
    assert_eq!(
        rec.stats.lock().unwrap().last().copied(),
        Some((out.entries_seen, out.bytes_expanded))
    );
    // serde: outcome is lowercase.
    let v = serde_json::to_value(&im1).unwrap();
    assert_eq!(v["outcome"], "stored");
    assert_eq!(v["kind"], "dicom");
    e.assert_tmp_empty();
}

#[tokio::test]
async fn two_patient_nested_zip_links_each_nifti() {
    let e = env().await;
    let a = zip(&[
        ("ct/IM1", &dicom(1, 1, 1, 1)),
        ("ct/IM2", &dicom(1, 1, 1, 2)),
        ("x.nii.gz", &gz(&nifti(1))),
    ]);
    let b = zip(&[("ct/IM1", &dicom(2, 1, 1, 1)), ("seg/y.nii", &nifti(2))]);
    let all = zip(&[("a.zip", &a), ("b.zip", &b)]);
    let (out, rec) = e.upload("all.zip", &all, Limits::default()).await;
    assert_eq!(out.status, "done");
    assert_eq!(out.counts.dicom_stored, 3);
    assert_eq!(out.counts.nifti_stored, 2);
    assert_eq!(
        out.nifti,
        NiftiSummary {
            linked: 2,
            unassigned: 0
        }
    );
    assert_eq!(
        out.patients,
        vec![
            patient(1, vec![study(1, 1, 1, 2)]),
            patient(2, vec![study(2, 1, 1, 1)]),
        ]
    );
    let (p1, s1) = e.links(&sop(1, 1, 1, 1)).await;
    let (p2, s2) = e.links(&sop(2, 1, 1, 1)).await;
    assert_ne!(p1, p2);
    assert_eq!(
        e.nifti_rows(2).await,
        vec![
            ("x.nii.gz".to_string(), Some(p1), Some(s1), true),
            ("y.nii".to_string(), Some(p2), Some(s2), false),
        ]
    );
    let x = rec.outcome_of("all.zip!/a.zip!/x.nii.gz");
    assert_eq!((x.kind, x.outcome), ("nifti", EntryOutcome::Stored));
}

#[tokio::test]
async fn duplicates_count_for_linking() {
    let e = env().await;
    // The DICOM arrives first through the single-instance path.
    for inst in [1, 2] {
        let mut t = e.ingestor.create_temp().unwrap();
        t.write_chunk(&dicom(1, 1, 1, inst)).await.unwrap();
        let spooled = t.finish().await.unwrap();
        e.ingestor
            .ingest(
                spooled,
                IngestSource::Http { remote: None },
                Expectations::default(),
            )
            .await
            .unwrap();
    }
    let z = zip(&[
        ("IM1", &dicom(1, 1, 1, 1)),
        ("IM2", &dicom(1, 1, 1, 2)),
        ("seg.nii.gz", &gz(&nifti(3))),
    ]);
    let (out, rec) = e.upload("again.zip", &z, Limits::default()).await;
    assert_eq!(out.status, "done");
    assert_eq!(
        out.counts,
        UploadCounts {
            dicom_duplicate: 2,
            nifti_stored: 1,
            ..Default::default()
        }
    );
    assert_eq!(
        rec.outcome_of("again.zip!/IM1").outcome,
        EntryOutcome::Duplicate
    );
    assert_eq!(out.nifti.linked, 1);
    assert_eq!(out.patients, vec![patient(1, vec![study(1, 1, 1, 2)])]);
    let (p, s) = e.links(&sop(1, 1, 1, 1)).await;
    assert_eq!(e.nifti_rows(1).await[0].1, Some(p));
    assert_eq!(e.nifti_rows(1).await[0].2, Some(s));
}

#[tokio::test]
async fn duplicate_nifti_keeps_study_of_same_patient() {
    let e = env().await;
    let seg = gz(&nifti(4));
    let first = zip(&[("IM1", &dicom(1, 1, 1, 1)), ("seg.nii.gz", &seg)]);
    let (out, _) = e.upload("first.zip", &first, Limits::default()).await;
    assert_eq!(out.nifti.linked, 1);
    let (p, s) = e.links(&sop(1, 1, 1, 1)).await;
    assert_eq!(e.nifti_rows(1).await[0].1, Some(p));
    assert_eq!(e.nifti_rows(1).await[0].2, Some(s));

    // Same patient, two studies: the new link has no study.
    let second = zip(&[
        ("IM1", &dicom(1, 1, 1, 1)),
        ("IM2", &dicom(1, 2, 1, 1)),
        ("seg.nii.gz", &seg),
    ]);
    let (out, _) = e.upload("second.zip", &second, Limits::default()).await;
    assert_eq!(out.counts.nifti_duplicate, 1);
    assert_eq!(out.nifti.linked, 1);
    let row = &e.nifti_rows(1).await[0];
    assert_eq!((row.1, row.2), (Some(p), Some(s)));

    // A different patient replaces the link (study included).
    let third = zip(&[("IM1", &dicom(2, 1, 1, 1)), ("seg.nii.gz", &seg)]);
    e.upload("third.zip", &third, Limits::default()).await;
    let (p2, s2) = e.links(&sop(2, 1, 1, 1)).await;
    let row = &e.nifti_rows(1).await[0];
    assert_eq!((row.1, row.2), (Some(p2), Some(s2)));
}

#[tokio::test]
async fn encrypted_and_truncated_fail_alone() {
    let e = env().await;
    let enc = mark_encrypted(zip(&[("IM9", &dicom(9, 1, 1, 1))]));
    let whole = zip(&[("IM8", &dicom(8, 1, 1, 1))]);
    let cut = whole[..whole.len() / 2].to_vec();
    // A stored DICOM entry (larger than the sniffed head) with one flipped
    // pixel byte fails its CRC check only after the sink read all bytes:
    // the spool must be discarded, not ingested.
    let bad = dicom_sized(7, 1, 1, 1, 8192);
    let mut crc = zip_stored(&[("IM7", &bad)]);
    let at = crc.windows(4).position(|w| w == b"DICM").unwrap() + 4000;
    crc[at] ^= 0xFF;
    let outer = zip(&[
        ("enc.zip", &enc),
        ("cut.zip", &cut),
        ("crc.zip", &crc),
        ("IM1", &dicom(1, 1, 1, 1)),
    ]);
    let (out, rec) = e.upload("o.zip", &outer, Limits::default()).await;
    assert_eq!(out.status, "done");
    assert_eq!(out.counts.dicom_stored, 1);
    assert_eq!(out.counts.failed, 3);
    for p in ["o.zip!/enc.zip", "o.zip!/cut.zip"] {
        let r = rec.outcome_of(p);
        assert_eq!(
            (r.kind, r.outcome),
            ("archive", EntryOutcome::Failed),
            "{p}"
        );
        assert!(r.reason.is_some());
    }
    assert_eq!(
        rec.outcome_of("o.zip!/enc.zip").reason.as_deref(),
        Some("encrypted archive")
    );
    let r = rec.outcome_of("o.zip!/crc.zip!/IM7");
    assert_eq!((r.kind, r.outcome), ("dicom", EntryOutcome::Failed));
    assert!(
        e.ingestor
            .index()
            .instance_links(&sop(7, 1, 1, 1))
            .await
            .unwrap()
            .is_none()
    );
    e.assert_tmp_empty();
}

#[tokio::test]
async fn aborted_upload_keeps_stored_items() {
    let e = env().await;
    let ims: Vec<Vec<u8>> = (1..=5).map(|i| dicom(1, 1, 1, i)).collect();
    let len = ims[0].len() as u64;
    let names: Vec<String> = (1..=5).map(|i| format!("IM{i}")).collect();
    let entries: Vec<(&str, &[u8])> = names
        .iter()
        .zip(&ims)
        .map(|(n, b)| (n.as_str(), b.as_slice()))
        .collect();
    let z = zip_stored(&entries);
    let limits = Limits {
        max_expanded_bytes: 2 * len + len / 2,
        ..Limits::default()
    };
    let (out, rec) = e.upload("big.zip", &z, limits).await;
    assert_eq!(out.status, "aborted");
    // Walker totals survive the abort (the third entry crossed the limit).
    assert_eq!(out.entries_seen, 3);
    assert!(out.bytes_expanded > 2 * len, "{}", out.bytes_expanded);
    assert_eq!(
        rec.stats.lock().unwrap().last().copied(),
        Some((out.entries_seen, out.bytes_expanded))
    );
    assert!(out.reason.as_deref().is_some_and(|r| !r.is_empty()));
    assert_eq!(out.counts.dicom_stored, 2);
    let row = e
        .ingestor
        .index()
        .upload(out.upload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, "aborted");
    assert_eq!(row.counts.dicom_stored, 2);
    e.links(&sop(1, 1, 1, 1)).await;
    e.links(&sop(1, 1, 1, 2)).await;
    assert!(
        e.ingestor
            .index()
            .instance_links(&sop(1, 1, 1, 3))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(out.patients, vec![patient(1, vec![study(1, 1, 1, 2)])]);
    e.assert_tmp_empty();
}

#[tokio::test]
async fn tmp_empty_after_job() {
    let e = env().await;
    let inner = zip(&[("IM1", &dicom(1, 1, 1, 1)), ("x.nii.gz", &gz(&nifti(4)))]);
    let tgz = gz(&zip(&[("IM2", &dicom(1, 1, 1, 2))]));
    let outer = zip(&[
        ("inner.zip", &inner),
        ("more.zip.gz", &tgz),
        ("junk", b"junk"),
    ]);
    let (out, _) = e.upload("n.zip", &outer, Limits::default()).await;
    assert_eq!(out.status, "done");
    assert_eq!(out.counts.dicom_stored, 2);
    assert_eq!(out.counts.nifti_stored, 1);
    e.assert_tmp_empty();
}

#[tokio::test]
async fn oversized_entry_fails_alone() {
    let big = dicom(1, 1, 1, 1);
    let max = big.len() as u64 - 1;
    let e = env_with(|c| c.max_instance_bytes = max).await;
    let z = zip(&[("IM1", &big), ("x.nii", &nifti(5))]);
    let (out, rec) = e.upload("o.zip", &z, Limits::default()).await;
    assert_eq!(out.status, "done");
    assert_eq!(out.counts.failed, 1);
    assert_eq!(out.counts.nifti_stored, 1);
    let r = rec.outcome_of("o.zip!/IM1");
    assert_eq!((r.kind, r.outcome), ("dicom", EntryOutcome::Failed));
    assert_eq!(r.code.as_deref(), Some("A700"));
    e.assert_tmp_empty();
}

#[tokio::test]
async fn bare_nifti_is_compressed_by_content_and_unassigned() {
    let e = env().await;
    // The name says nothing; gzip is detected from the bytes.
    let (out, rec) = e
        .upload("upload.bin", &gz(&nifti(6)), Limits::default())
        .await;
    assert_eq!(out.status, "done");
    assert_eq!(out.counts.nifti_stored, 1);
    assert_eq!(
        out.nifti,
        NiftiSummary {
            linked: 0,
            unassigned: 1
        }
    );
    assert_eq!(
        e.nifti_rows(1).await,
        vec![("upload.bin".to_string(), None, None, true)]
    );
    assert_eq!(rec.outcome_of("upload.bin").kind, "nifti");
}

#[tokio::test]
async fn process_dir_with_archive_and_loose_nifti() {
    let e = env().await;
    let dir = e.input.join("study");
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    std::fs::write(dir.join("sub/IM1"), dicom(1, 1, 1, 1)).unwrap();
    std::fs::write(dir.join("sub/IM2"), dicom(1, 1, 1, 2)).unwrap();
    std::fs::write(dir.join("sub/x.nii"), nifti(7)).unwrap();
    std::fs::write(
        dir.join("pack.zip"),
        zip(&[("IM1", &dicom(2, 1, 1, 1)), ("y.nii.gz", &gz(&nifti(8)))]),
    )
    .unwrap();
    std::fs::write(dir.join("junk.txt"), b"not dicom").unwrap();

    let id = e
        .ingestor
        .index()
        .create_upload("import", &dir.display().to_string(), None)
        .await
        .unwrap();
    let rec = Arc::new(Recorder::default());
    let out = process_dir(
        Arc::clone(&e.ingestor),
        &dir,
        true,
        id,
        Limits::default(),
        rec.clone(),
    )
    .await;
    assert_eq!(out.status, "done");
    assert_eq!(
        out.counts,
        UploadCounts {
            dicom_stored: 3,
            nifti_stored: 2,
            skipped: 1,
            ..Default::default()
        }
    );
    assert_eq!(
        out.nifti,
        NiftiSummary {
            linked: 2,
            unassigned: 0
        }
    );
    let paths: Vec<String> = rec.entries().into_iter().map(|r| r.path).collect();
    assert_eq!(
        paths,
        vec![
            "junk.txt",
            "pack.zip!/IM1",
            "pack.zip!/y.nii.gz",
            "sub/IM1",
            "sub/IM2",
            "sub/x.nii",
        ]
    );
    let (p1, s1) = e.links(&sop(1, 1, 1, 1)).await;
    let (p2, s2) = e.links(&sop(2, 1, 1, 1)).await;
    assert_eq!(
        e.nifti_rows(2).await,
        vec![
            ("x.nii".to_string(), Some(p1), Some(s1), false),
            ("y.nii.gz".to_string(), Some(p2), Some(s2), true),
        ]
    );
    let row = e.ingestor.index().upload(id).await.unwrap().unwrap();
    assert_eq!(row.status, "done");
    assert_eq!(row.counts, out.counts);

    // Not recursive: only the top-level files.
    let id = e
        .ingestor
        .index()
        .create_upload("import", "x", None)
        .await
        .unwrap();
    let rec = Arc::new(Recorder::default());
    let out = process_dir(
        Arc::clone(&e.ingestor),
        &dir,
        false,
        id,
        Limits::default(),
        rec.clone(),
    )
    .await;
    assert_eq!(out.counts.dicom_duplicate, 1);
    assert_eq!(out.counts.nifti_duplicate, 1);
    assert_eq!(out.counts.skipped, 1);
    assert_eq!(rec.entries().len(), 3);
    e.assert_tmp_empty();
}

/// NIfTI-2 magic with a header cut short: detected as NIfTI, but the header
/// cannot be parsed.
fn short_nifti2() -> Vec<u8> {
    let mut v = vec![0u8; 100];
    v[0..4].copy_from_slice(&540i32.to_le_bytes());
    v[4..8].copy_from_slice(b"n+2\0");
    v
}

fn tar_gz(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut b = tar::Builder::new(Vec::new());
    for (name, data) in entries {
        let mut h = tar::Header::new_gnu();
        h.set_size(data.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        b.append_data(&mut h, name, *data).unwrap();
    }
    gz(&b.into_inner().unwrap())
}

#[tokio::test]
async fn invalid_nifti_header_fails_alone_in_tar_gz() {
    let e = env().await;
    // The bad NIfTI is not read at all; the walker must skip its bytes so
    // the following DICOM arrives intact. (Only an entry shorter than the
    // header can fail to parse after detection.)
    let bad = short_nifti2();
    let im = dicom(1, 1, 1, 1);
    let t = tar_gz(&[("bad.nii", &bad), ("IM1", &im)]);
    let (out, rec) = e.upload("t.tar.gz", &t, Limits::default()).await;
    assert_eq!(out.status, "done");
    let r = rec.outcome_of("t.tar.gz!/bad.nii");
    assert_eq!((r.kind, r.outcome), ("nifti", EntryOutcome::Failed));
    assert_eq!(r.reason.as_deref(), Some("invalid NIfTI header"));
    let r = rec.outcome_of("t.tar.gz!/IM1");
    assert_eq!(r.outcome, EntryOutcome::Stored);
    assert_eq!(r.size, Some(im.len() as u64));
    assert_eq!(out.counts.failed, 1);
    assert_eq!(out.counts.dicom_stored, 1);
    e.assert_tmp_empty();
}

async fn import(e: &Env, dir: &std::path::Path, limits: Limits) -> (UploadOutcome, Arc<Recorder>) {
    let id = e
        .ingestor
        .index()
        .create_upload("import", "x", None)
        .await
        .unwrap();
    let rec = Arc::new(Recorder::default());
    let out = process_dir(Arc::clone(&e.ingestor), dir, true, id, limits, rec.clone()).await;
    (out, rec)
}

#[tokio::test]
async fn process_dir_loose_files_count_toward_limits() {
    let e = env().await;
    let dir = e.input.join("many");
    std::fs::create_dir_all(&dir).unwrap();
    for i in 1..=4 {
        std::fs::write(dir.join(format!("IM{i}")), dicom(1, 1, 1, i)).unwrap();
    }
    let limits = Limits {
        max_entries: 2,
        ..Limits::default()
    };
    let (out, rec) = import(&e, &dir, limits).await;
    assert_eq!(out.status, "aborted");
    assert_eq!(out.reason.as_deref(), Some("too many entries (limit 2)"));
    assert_eq!(out.counts.dicom_stored, 2);
    let paths: Vec<String> = rec.entries().into_iter().map(|r| r.path).collect();
    assert_eq!(paths, vec!["IM1", "IM2"]);
    assert_eq!(out.entries_seen, 3);
    e.links(&sop(1, 1, 1, 2)).await;

    // Bytes too: loose files are counted like archive entries.
    let dir2 = e.input.join("bytes");
    std::fs::create_dir_all(&dir2).unwrap();
    let im = dicom(2, 1, 1, 1);
    std::fs::write(dir2.join("IM1"), &im).unwrap();
    std::fs::write(dir2.join("IM2"), dicom(2, 1, 1, 2)).unwrap();
    let limits = Limits {
        max_expanded_bytes: im.len() as u64 + 10,
        ..Limits::default()
    };
    let (out, _) = import(&e, &dir2, limits).await;
    assert_eq!(out.status, "aborted");
    assert_eq!(
        out.reason,
        Some(format!(
            "expanded size limit reached ({} bytes)",
            im.len() + 10
        ))
    );
    assert_eq!(out.counts.dicom_stored, 1);
    e.assert_tmp_empty();
}

#[tokio::test]
async fn process_dir_unreadable_file_fails_alone() {
    use std::os::unix::fs::PermissionsExt;
    let e = env().await;
    let dir = e.input.join("perm");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("IM1"), dicom(1, 1, 1, 1)).unwrap();
    std::fs::write(dir.join("IM2"), dicom(1, 1, 1, 2)).unwrap();
    std::fs::set_permissions(dir.join("IM1"), std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::File::open(dir.join("IM1")).is_ok() {
        return; // running as root: permissions are not enforced
    }
    let (out, rec) = import(&e, &dir, Limits::default()).await;
    assert_eq!(out.status, "done");
    let r = rec.outcome_of("IM1");
    assert_eq!(r.outcome, EntryOutcome::Failed);
    assert!(
        r.reason.as_deref().unwrap().starts_with("cannot read file"),
        "{r:?}"
    );
    assert_eq!(out.counts.dicom_stored, 1);
}
