#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

use std::sync::Arc;

use slicestream_store::{
    BlobHash, DuplicatePolicy, Index, InstanceHeader, InstanceRecord, NiftiMeta, StorageCounts,
    UploadCounts, UpsertOutcome, read_header,
};

fn phantom_headers(dir: &std::path::Path, n: usize) -> Vec<InstanceHeader> {
    phantom::write_phantom(dir, n, 32).unwrap();
    (1..=n)
        .map(|i| read_header(&dir.join(format!("IM{i:05}.dcm"))).unwrap())
        .collect()
}

fn record(header: InstanceHeader, fill: u8) -> InstanceRecord {
    InstanceRecord {
        header,
        blob: BlobHash([fill; 32]),
        size_bytes: 1234,
        source: "dimse".into(),
        calling_ae: Some("MODALITY1".into()),
    }
}

#[tokio::test]
async fn upsert_insert_then_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let idx = Index::open(&dir.path().join("index.db")).await.unwrap();
    let h = phantom_headers(&dir.path().join("p"), 1).remove(0);
    let uid = h.sop_instance_uid.clone();

    let a = record(h.clone(), 0xaa);
    assert_eq!(
        idx.upsert_instance(&a, DuplicatePolicy::Keep)
            .await
            .unwrap(),
        UpsertOutcome::Inserted
    );
    assert_eq!(
        idx.upsert_instance(&a, DuplicatePolicy::Keep)
            .await
            .unwrap(),
        UpsertOutcome::Duplicate { existing: a.blob }
    );

    let b = record(h.clone(), 0xbb);
    assert_eq!(
        idx.upsert_instance(&b, DuplicatePolicy::Keep)
            .await
            .unwrap(),
        UpsertOutcome::Duplicate { existing: a.blob }
    );
    let s = idx.instance(&uid).await.unwrap().unwrap();
    assert_eq!(s.blob, a.blob);
    assert_eq!(s.source, "dimse");
    assert_eq!(s.calling_ae.as_deref(), Some("MODALITY1"));
    assert_eq!(s.series_instance_uid, h.series_instance_uid);

    assert_eq!(
        idx.upsert_instance(&b, DuplicatePolicy::Overwrite)
            .await
            .unwrap(),
        UpsertOutcome::Replaced { previous: a.blob }
    );
    assert_eq!(idx.instance(&uid).await.unwrap().unwrap().blob, b.blob);
    assert!(idx.blob_in_use(&b.blob).await.unwrap());
    assert!(!idx.blob_in_use(&a.blob).await.unwrap());
    assert_eq!(
        idx.blob_hashes().await.unwrap(),
        std::collections::HashSet::from([b.blob])
    );
    assert!(idx.instance("2.25.999").await.unwrap().is_none());
}

#[tokio::test]
async fn series_slices_returns_frame_info() {
    let dir = tempfile::tempdir().unwrap();
    let idx = Index::open(&dir.path().join("index.db")).await.unwrap();
    for (i, h) in phantom_headers(&dir.path().join("p"), 3)
        .into_iter()
        .enumerate()
    {
        let r = record(h, i as u8);
        assert_eq!(
            idx.upsert_instance(&r, DuplicatePolicy::Keep)
                .await
                .unwrap(),
            UpsertOutcome::Inserted
        );
    }
    let slices = idx
        .series_slices("2.25.1000000000000000000000000000001")
        .await
        .unwrap();
    assert_eq!(slices.len(), 3);
    for s in &slices {
        assert_eq!(s.frame_info.rows, 32);
        assert_eq!(s.number_of_frames, 1);
        assert!(s.slice_position.is_some());
    }
    let series = idx.list_series().await.unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].instance_count, 3);
    assert_eq!(series[0].modality.as_deref(), Some("CT"));
    assert_eq!(
        series[0].study_instance_uid,
        "2.25.1000000000000000000000000000002"
    );
    assert!(idx.series_slices("2.25.404").await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_upserts_same_uid() {
    let dir = tempfile::tempdir().unwrap();
    let idx = Arc::new(Index::open(&dir.path().join("index.db")).await.unwrap());
    let h = phantom_headers(&dir.path().join("p"), 1).remove(0);
    let mut tasks = Vec::new();
    for i in 0..8u8 {
        let idx = idx.clone();
        let r = record(h.clone(), i);
        tasks.push(tokio::spawn(async move {
            idx.upsert_instance(&r, DuplicatePolicy::Keep).await
        }));
    }
    let mut inserted = 0;
    let mut dup = 0;
    for t in tasks {
        match t.await.unwrap().unwrap() {
            UpsertOutcome::Inserted => inserted += 1,
            UpsertOutcome::Duplicate { .. } => dup += 1,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!((inserted, dup), (1, 7));
}

#[tokio::test]
async fn reopen_keeps_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("index.db");
    let h = phantom_headers(&dir.path().join("p"), 1).remove(0);
    let r = record(h.clone(), 7);
    {
        let idx = Index::open(&db).await.unwrap();
        idx.upsert_instance(&r, DuplicatePolicy::Keep)
            .await
            .unwrap();
    }
    let idx = Index::open(&db).await.unwrap();
    assert_eq!(
        idx.instance(&h.sop_instance_uid)
            .await
            .unwrap()
            .unwrap()
            .blob,
        r.blob
    );
}

async fn seeded_index(dir: &std::path::Path) -> (Index, Vec<InstanceHeader>) {
    let idx = Index::open(&dir.join("index.db")).await.unwrap();
    let mut hs = phantom_headers(&dir.join("p"), 3);
    hs[2].series_instance_uid = format!("{}.99", hs[0].series_instance_uid);
    for (i, (h, size)) in hs.iter().zip([100, 200, 300]).enumerate() {
        let mut r = record(h.clone(), i as u8 + 1);
        r.size_bytes = size;
        idx.upsert_instance(&r, DuplicatePolicy::Keep)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    }
    (idx, hs)
}

#[tokio::test]
async fn storage_counts_empty() {
    let dir = tempfile::tempdir().unwrap();
    let idx = Index::open(&dir.path().join("index.db")).await.unwrap();
    assert_eq!(
        idx.storage_counts().await.unwrap(),
        StorageCounts::default()
    );
}

#[tokio::test]
async fn storage_counts_after_inserts() {
    let dir = tempfile::tempdir().unwrap();
    let (idx, _) = seeded_index(dir.path()).await;
    assert_eq!(
        idx.storage_counts().await.unwrap(),
        StorageCounts {
            studies: 1,
            series: 2,
            instances: 3,
            objects_bytes: 600,
            objects_count: 3,
            nifti_count: 0,
            nifti_bytes: 0,
        }
    );
}

#[tokio::test]
async fn console_series_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (idx, hs) = seeded_index(dir.path()).await;
    let rows = idx.console_series().await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].series_uid, hs[2].series_instance_uid);
    let a = &rows[1];
    assert_eq!(a.series_uid, hs[0].series_instance_uid);
    assert_eq!(a.instance_count, 2);
    assert_eq!(a.size_bytes, 300);
    assert_eq!(a.patient_name, hs[0].patient_name);
    assert_eq!(a.study_date, hs[0].study_date);
}

#[tokio::test]
async fn console_series_is_limited_to_newest() {
    let dir = tempfile::tempdir().unwrap();
    let idx = Index::open(&dir.path().join("index.db")).await.unwrap();
    let base = phantom_headers(&dir.path().join("p"), 1).remove(0);
    let n = slicestream_store::CONSOLE_SERIES_LIMIT + 1;
    for i in 0..n {
        let mut h = base.clone();
        h.series_instance_uid = format!("1.2.3.{i}");
        h.sop_instance_uid = format!("1.2.3.{i}.1");
        idx.upsert_instance(&record(h, 0xaa), DuplicatePolicy::Keep)
            .await
            .unwrap();
    }
    let rows = idx.console_series().await.unwrap();
    assert_eq!(rows.len(), slicestream_store::CONSOLE_SERIES_LIMIT);
    // Newest first: the last inserted series is on top, the first is cut.
    assert_eq!(rows[0].series_uid, format!("1.2.3.{}", n - 1));
    assert!(rows.iter().all(|r| r.series_uid != "1.2.3.0"));
}

fn nifti_meta(compressed: bool) -> NiftiMeta {
    NiftiMeta {
        version: 1,
        dims: vec![3, 4, 5, 6],
        pixdim: vec![1.0, 0.5, 0.5, 2.0],
        datatype: 4,
        descrip: "test volume".into(),
        compressed,
        original_name: "brain.nii.gz".into(),
    }
}

#[tokio::test]
async fn upload_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let idx = Index::open(&dir.path().join("index.db")).await.unwrap();
    let a = idx
        .create_upload("http_archive", "scan.zip", Some("10.0.0.1:5000"))
        .await
        .unwrap();
    let b = idx.create_upload("import", "/data", None).await.unwrap();
    assert_ne!(a, b);

    let row = idx.upload(a).await.unwrap().unwrap();
    assert_eq!(row.kind, "http_archive");
    assert_eq!(row.name, "scan.zip");
    assert_eq!(row.remote.as_deref(), Some("10.0.0.1:5000"));
    assert_eq!(row.status, "running");
    assert!(row.finished_at.is_none());
    assert_eq!(row.counts, UploadCounts::default());
    // RFC3339 with milliseconds, UTC.
    assert_eq!(row.started_at.len(), "2026-11-13T00:00:00.000Z".len());
    assert!(row.started_at.ends_with('Z'));

    let counts = UploadCounts {
        dicom_stored: 1,
        dicom_duplicate: 2,
        nifti_stored: 3,
        nifti_duplicate: 4,
        skipped: 5,
        failed: 6,
    };
    idx.finish_upload(a, "done", &counts).await.unwrap();
    let row = idx.upload(a).await.unwrap().unwrap();
    assert_eq!(row.status, "done");
    assert_eq!(row.counts, counts);
    assert!(row.finished_at.is_some());

    // Only the still-running upload is aborted.
    assert_eq!(idx.abort_running_uploads().await.unwrap(), 1);
    assert_eq!(idx.upload(a).await.unwrap().unwrap().status, "done");
    let rb = idx.upload(b).await.unwrap().unwrap();
    assert_eq!(rb.status, "aborted");
    assert!(rb.finished_at.is_some());
    assert_eq!(idx.abort_running_uploads().await.unwrap(), 0);
    assert!(idx.upload(9999).await.unwrap().is_none());
}

#[tokio::test]
async fn blob_hashes_include_nifti() {
    let dir = tempfile::tempdir().unwrap();
    let (idx, _) = seeded_index(dir.path()).await;
    let up = idx.create_upload("import", "x", None).await.unwrap();
    let nh = BlobHash([0x77; 32]);
    let (id, duplicate) = idx
        .insert_nifti(&nh, 4096, &nifti_meta(true), up)
        .await
        .unwrap();
    assert!(!duplicate);
    let (id2, duplicate) = idx
        .insert_nifti(&nh, 4096, &nifti_meta(true), up)
        .await
        .unwrap();
    assert!(duplicate);
    assert_eq!(id, id2);

    let hashes = idx.blob_hashes().await.unwrap();
    assert_eq!(hashes.len(), 4);
    assert!(hashes.contains(&nh));
    for fill in 1..=3u8 {
        assert!(hashes.contains(&BlobHash([fill; 32])));
    }
    assert!(idx.blob_in_use(&nh).await.unwrap());
    assert!(!idx.blob_in_use(&BlobHash([0x78; 32])).await.unwrap());
}

#[tokio::test]
async fn storage_counts_include_nifti() {
    let dir = tempfile::tempdir().unwrap();
    let (idx, _) = seeded_index(dir.path()).await;
    let up = idx.create_upload("import", "x", None).await.unwrap();
    idx.insert_nifti(&BlobHash([0x71; 32]), 1000, &nifti_meta(false), up)
        .await
        .unwrap();
    idx.insert_nifti(&BlobHash([0x72; 32]), 24, &nifti_meta(true), up)
        .await
        .unwrap();
    assert_eq!(
        idx.storage_counts().await.unwrap(),
        StorageCounts {
            studies: 1,
            series: 2,
            instances: 3,
            objects_bytes: 600,
            objects_count: 3,
            nifti_count: 2,
            nifti_bytes: 1024,
        }
    );
}

#[tokio::test]
async fn nifti_links_and_instance_links() {
    let dir = tempfile::tempdir().unwrap();
    let (idx, hs) = seeded_index(dir.path()).await;
    let (patient_fk, study_fk) = idx
        .instance_links(&hs[0].sop_instance_uid)
        .await
        .unwrap()
        .expect("indexed instance has links");
    assert!(idx.instance_links("1.2.3.missing").await.unwrap().is_none());

    let up = idx.create_upload("import", "x", None).await.unwrap();
    let (id, _) = idx
        .insert_nifti(&BlobHash([0x70; 32]), 10, &nifti_meta(false), up)
        .await
        .unwrap();
    let row = idx.nifti_volume(id).await.unwrap().unwrap();
    assert_eq!(row.patient_fk, None);
    assert_eq!(row.study_fk, None);
    assert_eq!(row.upload_fk, up);
    assert_eq!(row.meta, nifti_meta(false));
    assert_eq!(row.size_bytes, 10);

    idx.link_nifti(id, Some(patient_fk), Some(study_fk))
        .await
        .unwrap();
    let row = idx.nifti_volume(id).await.unwrap().unwrap();
    assert_eq!(row.patient_fk, Some(patient_fk));
    assert_eq!(row.study_fk, Some(study_fk));
    // Foreign keys are enforced.
    assert!(idx.link_nifti(id, Some(9999), None).await.is_err());
}
