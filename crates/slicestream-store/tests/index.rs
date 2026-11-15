#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;
mod support;

use std::path::Path;
use std::sync::Arc;

use slicestream_store::{
    BlobHash, DuplicatePolicy, FrameTable, Index, InstanceData, InstanceHeader, InstanceRecord,
    MetadataState, MetadataStateKind, NiftiMeta, SCAN_VERSION, Scope, StorageCounts,
    StoredDataState, StudyInfo, UploadCounts, UpsertOutcome, fold_name, read_header, scan_instance,
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
        data: no_data(),
        scanned: false,
    }
}

fn no_data() -> InstanceData {
    InstanceData {
        metadata: None,
        frames: None,
        bulk: vec![],
        offsets_valid: false,
        problem: None,
    }
}

/// A record built from scanning `path`, marked as scanned.
fn scanned_record(path: &Path, fill: u8) -> InstanceRecord {
    let scan = scan_instance(path).unwrap();
    let mut r = record(scan.header, fill);
    r.data = scan.data;
    r.scanned = true;
    r
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

#[tokio::test]
async fn study_info_by_fk() {
    let dir = tempfile::tempdir().unwrap();
    let (idx, hs) = seeded_index(dir.path()).await;
    let (patient_fk, study_fk) = idx
        .instance_links(&hs[0].sop_instance_uid)
        .await
        .unwrap()
        .unwrap();
    let info = idx.study_info(study_fk).await.unwrap().unwrap();
    assert_eq!(
        info,
        StudyInfo {
            patient_fk,
            patient_id: hs[0].patient_id.clone().unwrap_or_default(),
            patient_name: hs[0].patient_name.clone(),
            study_instance_uid: hs[0].study_instance_uid.clone(),
            study_date: hs[0].study_date.clone(),
            study_description: hs[0].study_description.clone(),
        }
    );
    assert!(idx.study_info(9999).await.unwrap().is_none());
}

async fn open(dir: &Path) -> Index {
    Index::open(&dir.join("index.db")).await.unwrap()
}

async fn sql_pool(dir: &Path) -> sqlx::SqlitePool {
    use sqlx::sqlite::SqliteConnectOptions;
    sqlx::SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(dir.join("index.db"))
            .create_if_missing(true),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn upsert_writes_instance_data() {
    let dir = tempfile::tempdir().unwrap();
    let idx = open(dir.path()).await;
    let file = support::native(&dir.path().join("a"), 2, 4, 4, 8);
    let r = scanned_record(&file, 1);
    assert!(r.data.metadata.is_some() && r.data.frames.is_some());
    idx.upsert_instance(&r, DuplicatePolicy::Keep)
        .await
        .unwrap();

    let found = idx
        .find_instances(&Scope::Study(support::STUDY_UID.into()))
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert!(found[0].scanned);
    let stored = idx.stored_data(found[0].id).await.unwrap().unwrap();
    assert_eq!(stored.frames, r.data.frames);
    assert_eq!(stored.bulk, r.data.bulk);
    assert!(stored.offsets_valid);
    assert_eq!(stored.problem, None);
    let meta = idx.metadata_batch(&[found[0].id]).await.unwrap();
    assert_eq!(
        meta,
        vec![(
            found[0].id,
            MetadataState::Ready(r.data.metadata.clone().unwrap())
        )]
    );

    let pool = sql_pool(dir.path()).await;
    let version: i64 = sqlx::query_scalar("SELECT scan_version FROM instances")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(version, SCAN_VERSION);
    let (name, folded, series_pps): (String, String, Option<String>) = sqlx::query_as(
        "SELECT st.patient_name, st.patient_name_folded, s.performed_procedure_step_start_date
         FROM studies st JOIN series s ON s.study_fk = st.id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(name, "SCAN^TEST");
    assert_eq!(folded, fold_name("SCAN^TEST"));
    assert_eq!(series_pps, None);
}

#[tokio::test]
async fn duplicate_keeps_instance_data() {
    let dir = tempfile::tempdir().unwrap();
    let idx = open(dir.path()).await;
    let first = support::native(&dir.path().join("a"), 2, 4, 4, 8);
    let second = support::native(&dir.path().join("b"), 3, 4, 4, 8);
    let a = scanned_record(&first, 1);
    idx.upsert_instance(&a, DuplicatePolicy::Keep)
        .await
        .unwrap();
    let b = scanned_record(&second, 2);
    assert!(matches!(
        idx.upsert_instance(&b, DuplicatePolicy::Keep)
            .await
            .unwrap(),
        UpsertOutcome::Duplicate { .. }
    ));
    let id = idx.unscanned(10).await.unwrap();
    assert!(id.is_empty());
    let found = idx
        .find_instances(&Scope::Study(support::STUDY_UID.into()))
        .await
        .unwrap();
    let stored = idx.stored_data(found[0].id).await.unwrap().unwrap();
    assert_eq!(stored.frames.unwrap().count(), 2);
    assert_eq!(found[0].blob, a.blob);
}

#[tokio::test]
async fn overwrite_replaces_instance_data() {
    let dir = tempfile::tempdir().unwrap();
    let idx = open(dir.path()).await;
    let a = scanned_record(&support::native(&dir.path().join("a"), 2, 4, 4, 8), 1);
    let b = scanned_record(&support::native(&dir.path().join("b"), 3, 4, 4, 8), 2);
    idx.upsert_instance(&a, DuplicatePolicy::Overwrite)
        .await
        .unwrap();
    assert!(matches!(
        idx.upsert_instance(&b, DuplicatePolicy::Overwrite)
            .await
            .unwrap(),
        UpsertOutcome::Replaced { .. }
    ));
    let found = idx
        .find_instances(&Scope::Study(support::STUDY_UID.into()))
        .await
        .unwrap();
    assert_eq!(found[0].blob, b.blob);
    let stored = idx.stored_data(found[0].id).await.unwrap().unwrap();
    assert_eq!(stored.frames, b.data.frames);
    assert_eq!(stored.frames.unwrap().count(), 3);
    let meta = idx.metadata_batch(&[found[0].id]).await.unwrap();
    assert_eq!(
        meta[0].1,
        MetadataState::Ready(b.data.metadata.clone().unwrap())
    );

    // Replacing with an unscanned record clears the old scan.
    let c = record(b.header.clone(), 3);
    idx.upsert_instance(&c, DuplicatePolicy::Overwrite)
        .await
        .unwrap();
    assert!(idx.stored_data(found[0].id).await.unwrap().is_none());
    assert_eq!(idx.unscanned(10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn study_patient_attrs_per_study_without_patient_id() {
    let dir = tempfile::tempdir().unwrap();
    let idx = open(dir.path()).await;
    let mut hs = phantom_headers(&dir.path().join("p"), 2);
    for (i, (h, name)) in hs.iter_mut().zip(["A^ONE", "B^TWO"]).enumerate() {
        h.patient_id = None;
        h.patient_name = Some(name.into());
        h.study_instance_uid = format!("2.25.77{i}");
        h.series_instance_uid = format!("2.25.88{i}");
        idx.upsert_instance(&record(h.clone(), i as u8), DuplicatePolicy::Keep)
            .await
            .unwrap();
    }
    let pool = sql_pool(dir.path()).await;
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT patient_name, patient_name_folded FROM studies ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        rows,
        vec![
            ("A^ONE".to_string(), "a^one".to_string()),
            ("B^TWO".to_string(), "b^two".to_string())
        ]
    );
    let patients: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM patients")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(patients, 1);
}

#[tokio::test]
async fn opens_store_created_before_0003() {
    let dir = tempfile::tempdir().unwrap();
    let old = dir.path().join("old-migrations");
    std::fs::create_dir(&old).unwrap();
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for name in ["0001_init.sql", "0002_uploads_nifti.sql"] {
        std::fs::copy(src.join(name), old.join(name)).unwrap();
    }
    let pool = sql_pool(dir.path()).await;
    sqlx::migrate::Migrator::new(old.as_path())
        .await
        .unwrap()
        .run(&pool)
        .await
        .unwrap();
    let hash = "ab".repeat(32);
    for sql in [
        "INSERT INTO patients (id, patient_id) VALUES (1, 'P1')",
        "INSERT INTO studies (id, study_instance_uid, patient_fk, created_at, updated_at)
         VALUES (1, '2.25.1', 1, 't', 't')",
        "INSERT INTO series (id, series_instance_uid, study_fk, created_at, updated_at)
         VALUES (1, '2.25.2', 1, 't', 't')",
    ] {
        sqlx::query(sql).execute(&pool).await.unwrap();
    }
    sqlx::query(
        "INSERT INTO instances (sop_instance_uid, series_fk, sop_class_uid, number_of_frames,
                                transfer_syntax, blob_sha256, size_bytes, source, received_at)
         VALUES ('2.25.3', 1, '1.2', 1, '1.2.840.10008.1.2.1', ?1, 10, 'stow', 't')",
    )
    .bind(&hash)
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;

    let idx = open(dir.path()).await;
    let pending = idx.unscanned(10).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].1, "2.25.3");
    assert_eq!(pending[0].2.to_hex(), hash);
    let found = idx
        .find_instances(&Scope::Study("2.25.1".into()))
        .await
        .unwrap();
    assert!(!found[0].scanned);
    assert!(idx.stored_data(found[0].id).await.unwrap().is_none());
}

#[tokio::test]
async fn find_instances_scopes_and_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let (idx, hs) = seeded_index(dir.path()).await;
    let (st, se0, se2) = (
        hs[0].study_instance_uid.clone(),
        hs[0].series_instance_uid.clone(),
        hs[2].series_instance_uid.clone(),
    );
    let study = idx.find_instances(&Scope::Study(st.clone())).await.unwrap();
    assert_eq!(study.len(), 3);
    assert_eq!(
        study
            .iter()
            .map(|i| i.series_uid.as_str())
            .collect::<Vec<_>>(),
        [&se0, &se0, &se2]
    );
    assert!(
        study
            .iter()
            .all(|i| i.study_uid == st && i.frame_info.is_some())
    );

    let series = idx
        .find_instances(&Scope::Series(st.clone(), se2.clone()))
        .await
        .unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].sop_uid, hs[2].sop_instance_uid);

    let inst = idx
        .find_instances(&Scope::Instance(
            st.clone(),
            se0.clone(),
            hs[1].sop_instance_uid.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(inst.len(), 1);
    assert_eq!(inst[0].sop_class_uid, hs[1].sop_class_uid);
    assert_eq!(inst[0].number_of_frames, 1);

    // The instance lives in series se2, not se0 (and not in another study).
    let wrong_series = Scope::Instance(st.clone(), se0, hs[2].sop_instance_uid.clone());
    assert!(idx.find_instances(&wrong_series).await.unwrap().is_empty());
    let wrong_study = Scope::Series("2.25.404".into(), se2);
    assert!(idx.find_instances(&wrong_study).await.unwrap().is_empty());
}

#[tokio::test]
async fn metadata_batch_states() {
    let dir = tempfile::tempdir().unwrap();
    let idx = open(dir.path()).await;
    let ready = scanned_record(&support::native(&dir.path().join("a"), 1, 4, 4, 8), 1);
    idx.upsert_instance(&ready, DuplicatePolicy::Keep)
        .await
        .unwrap();
    let hs = phantom_headers(&dir.path().join("p"), 2);
    idx.upsert_instance(&record(hs[0].clone(), 2), DuplicatePolicy::Keep)
        .await
        .unwrap();
    let mut failed = record(hs[1].clone(), 3);
    failed.scanned = true;
    failed.data.problem = Some("metadata too large".into());
    idx.upsert_instance(&failed, DuplicatePolicy::Keep)
        .await
        .unwrap();

    let mut ids = Vec::new();
    for (st, sop) in [
        (support::STUDY_UID, support::SOP_UID),
        (
            hs[0].study_instance_uid.as_str(),
            hs[0].sop_instance_uid.as_str(),
        ),
        (
            hs[1].study_instance_uid.as_str(),
            hs[1].sop_instance_uid.as_str(),
        ),
    ] {
        let found = idx.find_instances(&Scope::Study(st.into())).await.unwrap();
        ids.push(found.iter().find(|i| i.sop_uid == sop).unwrap().id);
    }
    assert_eq!(ids.len(), 3);
    let mut query = ids.clone();
    query.push(9999);
    let states = idx.metadata_batch(&query).await.unwrap();
    assert_eq!(states.len(), 3);
    assert_eq!(
        states[0],
        (
            ids[0],
            MetadataState::Ready(ready.data.metadata.clone().unwrap())
        )
    );
    assert_eq!(states[1], (ids[1], MetadataState::Unscanned));
    assert_eq!(
        states[2],
        (
            ids[2],
            MetadataState::Unavailable("metadata too large".into())
        )
    );
    assert!(idx.metadata_batch(&[]).await.unwrap().is_empty());

    // The same states without the metadata: its length instead.
    let len = ready.data.metadata.as_ref().unwrap().len() as u64;
    let kinds = idx.metadata_states(&query).await.unwrap();
    assert_eq!(
        kinds,
        [
            (ids[0], MetadataStateKind::Ready(len)),
            (ids[1], MetadataStateKind::Unscanned),
            (
                ids[2],
                MetadataStateKind::Unavailable("metadata too large".into())
            ),
        ]
    );
    // Order follows the ids asked for.
    let reversed: Vec<i64> = ids.iter().rev().copied().collect();
    let kinds = idx.metadata_states(&reversed).await.unwrap();
    assert_eq!(kinds.iter().map(|k| k.0).collect::<Vec<_>>(), reversed);
    assert!(idx.metadata_states(&[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn stored_data_for_checks_the_blob() {
    let dir = tempfile::tempdir().unwrap();
    let idx = open(dir.path()).await;
    let a = scanned_record(&support::native(&dir.path().join("a"), 2, 4, 4, 8), 1);
    let b = scanned_record(&support::native(&dir.path().join("b"), 3, 4, 4, 8), 2);
    idx.upsert_instance(&a, DuplicatePolicy::Overwrite)
        .await
        .unwrap();
    let id = idx
        .find_instances(&Scope::Study(support::STUDY_UID.into()))
        .await
        .unwrap()[0]
        .id;
    let StoredDataState::Ready(data) = idx.stored_data_for(id, &a.blob).await.unwrap() else {
        panic!("blob a is current and scanned");
    };
    assert_eq!(data.frames, a.data.frames);

    // After an overwrite, a reader still holding blob a gets nothing.
    idx.upsert_instance(&b, DuplicatePolicy::Overwrite)
        .await
        .unwrap();
    assert_eq!(
        idx.stored_data_for(id, &a.blob).await.unwrap(),
        StoredDataState::Replaced
    );
    let StoredDataState::Ready(data) = idx.stored_data_for(id, &b.blob).await.unwrap() else {
        panic!("blob b is current and scanned");
    };
    assert_eq!(data.frames, b.data.frames);

    // Unscanned, and unknown ids.
    let c = record(b.header.clone(), 3);
    idx.upsert_instance(&c, DuplicatePolicy::Overwrite)
        .await
        .unwrap();
    assert_eq!(
        idx.stored_data_for(id, &c.blob).await.unwrap(),
        StoredDataState::Unscanned
    );
    assert_eq!(
        idx.stored_data_for(id, &b.blob).await.unwrap(),
        StoredDataState::Replaced
    );
    assert_eq!(
        idx.stored_data_for(id + 100, &c.blob).await.unwrap(),
        StoredDataState::Replaced
    );
}

#[tokio::test]
async fn write_scan_skips_changed_blob() {
    let dir = tempfile::tempdir().unwrap();
    let idx = open(dir.path()).await;
    let file = support::native(&dir.path().join("a"), 2, 4, 4, 8);
    let scan = scan_instance(&file).unwrap();
    let pending = record(scan.header.clone(), 1);
    idx.upsert_instance(&pending, DuplicatePolicy::Keep)
        .await
        .unwrap();
    let (id, uid, blob) = idx.unscanned(10).await.unwrap().remove(0);
    assert_eq!((uid.as_str(), blob), (support::SOP_UID, pending.blob));

    // A scan of some other blob is dropped.
    let stale = BlobHash([9; 32]);
    assert!(!idx.write_scan(id, &stale, &scan).await.unwrap());
    assert!(!idx.write_scan_failure(id, &stale, "x").await.unwrap());
    assert_eq!(idx.unscanned(10).await.unwrap().len(), 1);
    assert!(idx.stored_data(id).await.unwrap().is_none());

    // The matching blob is written, and fills the study columns.
    assert!(idx.write_scan(id, &blob, &scan).await.unwrap());
    assert!(idx.unscanned(10).await.unwrap().is_empty());
    let stored = idx.stored_data(id).await.unwrap().unwrap();
    assert_eq!(stored.frames, scan.data.frames);
    let pool = sql_pool(dir.path()).await;
    let name: Option<String> = sqlx::query_scalar("SELECT patient_name_folded FROM studies")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(name.as_deref(), Some("scan^test"));
}

#[tokio::test]
async fn write_scan_failure_marks_scanned_without_data() {
    let dir = tempfile::tempdir().unwrap();
    let idx = open(dir.path()).await;
    let h = phantom_headers(&dir.path().join("p"), 1).remove(0);
    let r = record(h, 1);
    idx.upsert_instance(&r, DuplicatePolicy::Keep)
        .await
        .unwrap();
    let (id, _, blob) = idx.unscanned(10).await.unwrap().remove(0);
    assert!(
        idx.write_scan_failure(id, &blob, "bad pixel data")
            .await
            .unwrap()
    );
    assert!(idx.unscanned(10).await.unwrap().is_empty());
    let stored = idx.stored_data(id).await.unwrap().unwrap();
    assert_eq!(stored.frames, None);
    assert!(stored.bulk.is_empty() && !stored.offsets_valid);
    assert_eq!(stored.problem.as_deref(), Some("bad pixel data"));
}

#[tokio::test]
async fn series_slices_carry_frames() {
    let dir = tempfile::tempdir().unwrap();
    let idx = open(dir.path()).await;
    let r = scanned_record(&support::native(&dir.path().join("a"), 2, 4, 4, 8), 1);
    idx.upsert_instance(&r, DuplicatePolicy::Keep)
        .await
        .unwrap();
    let slices = idx.series_slices(support::SERIES_UID).await.unwrap();
    assert_eq!(slices.len(), 1);
    assert!(matches!(
        slices[0].frames,
        Some(FrameTable::Native { count: 2, .. })
    ));

    // Without valid offsets the table is not offered.
    let pool = sql_pool(dir.path()).await;
    sqlx::query("UPDATE instance_data SET offsets_valid = 0")
        .execute(&pool)
        .await
        .unwrap();
    let slices = idx.series_slices(support::SERIES_UID).await.unwrap();
    assert_eq!(slices[0].frames, None);

    // Unscanned instances have none either.
    let h = phantom_headers(&dir.path().join("p"), 1).remove(0);
    let series = h.series_instance_uid.clone();
    idx.upsert_instance(&record(h, 2), DuplicatePolicy::Keep)
        .await
        .unwrap();
    let slices = idx.series_slices(&series).await.unwrap();
    assert_eq!(slices[0].frames, None);
}
