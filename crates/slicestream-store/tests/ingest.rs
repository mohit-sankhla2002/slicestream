#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;
mod support;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::tags;
use dicom_dictionary_std::uids::CT_IMAGE_STORAGE;
use slicestream_store::{
    BlobKind, DuplicatePolicy, Expectations, IngestError, IngestSource, Ingestor, NiftiMeta,
    RecoveryReport, SpooledBlob, StorageConfig, is_storage_sop_class,
};
use support::{index_pool, ingest_slices};

fn config(root: &Path) -> StorageConfig {
    StorageConfig {
        root: root.to_path_buf(),
        max_instance_bytes: 2_147_483_648,
        on_duplicate: DuplicatePolicy::Keep,
    }
}

/// Write a one-slice phantom into `dir` and return its path.
fn phantom_file(dir: &Path) -> PathBuf {
    phantom::write_phantom(dir, 1, 16).unwrap();
    dir.join("IM00001.dcm")
}

fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            out.extend(files_under(&p));
        } else {
            out.push(p);
        }
    }
    out
}

async fn spool(ing: &Ingestor, path: &Path) -> SpooledBlob {
    let bytes = std::fs::read(path).unwrap();
    let mut t = ing.create_temp().unwrap();
    for chunk in bytes.chunks(1000) {
        t.write_chunk(chunk).await.unwrap();
    }
    t.finish().await.unwrap()
}

fn dimse() -> IngestSource {
    IngestSource::Dimse {
        calling_ae: "MODALITY1".into(),
        called_ae: "SLICESTREAM".into(),
    }
}

#[tokio::test]
async fn ingest_stores_and_indexes() {
    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let ing = Ingestor::open(config(&d.path().join("store")))
        .await
        .unwrap();
    let mut rx = ing.subscribe();

    let spooled = spool(&ing, &file).await;
    let stored = ing
        .ingest(spooled, dimse(), Expectations::default())
        .await
        .unwrap();
    assert!(!stored.duplicate);
    assert!(!stored.replaced);
    assert!(ing.blobs().path_of(&stored.blob, BlobKind::Dicom).is_file());

    let series = ing.index().list_series().await.unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].instance_count, 1);
    assert_eq!(series[0].series_instance_uid, stored.series_instance_uid);

    let ev = rx.recv().await.unwrap();
    assert_eq!(ev.sop_instance_uid, stored.sop_instance_uid);
    assert_eq!(ev.series_instance_uid, stored.series_instance_uid);

    let summary = ing
        .index()
        .instance(&stored.sop_instance_uid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.source, "dimse");
    assert_eq!(summary.calling_ae.as_deref(), Some("MODALITY1"));
    assert!(files_under(&d.path().join("store/tmp")).is_empty());
}

#[tokio::test]
async fn duplicate_same_bytes_is_success() {
    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let ing = Ingestor::open(config(&d.path().join("store")))
        .await
        .unwrap();
    let a = ing.import_file(&file).await.unwrap();
    assert!(!a.duplicate);
    let b = ing.import_file(&file).await.unwrap();
    assert!(b.duplicate);
    assert_eq!(a.blob, b.blob);
    assert_eq!(files_under(&d.path().join("store/objects")).len(), 1);
    let s = ing
        .index()
        .instance(&a.sop_instance_uid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s.source, "import");
}

#[tokio::test]
async fn uid_mismatch_rejected() {
    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let ing = Ingestor::open(config(&d.path().join("store")))
        .await
        .unwrap();
    let spooled = spool(&ing, &file).await;
    let err = ing
        .ingest(
            spooled,
            dimse(),
            Expectations {
                sop_instance_uid: Some("1.2.3".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err.error, IngestError::UidMismatch { .. }),
        "{err:?}"
    );
    assert_eq!(err.status(), 0xA900);
    assert!(files_under(&d.path().join("store/objects")).is_empty());
    assert!(files_under(&d.path().join("store/tmp")).is_empty());
}

#[tokio::test]
async fn study_mismatch_rejected() {
    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let ing = Ingestor::open(config(&d.path().join("store")))
        .await
        .unwrap();
    let spooled = spool(&ing, &file).await;
    let err = ing
        .ingest(
            spooled,
            IngestSource::Http { remote: None },
            Expectations {
                study_instance_uid: Some("1.2.3.4".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err.error, IngestError::StudyMismatch { .. }),
        "{err:?}"
    );
    assert_eq!(err.status(), 0xC409);
    // The header was parsed, so the failure carries the instance's UIDs.
    assert_eq!(err.sop_class_uid.as_deref(), Some(CT_IMAGE_STORAGE));
    assert!(
        err.sop_instance_uid
            .as_deref()
            .unwrap()
            .starts_with("2.25.")
    );
    assert!(files_under(&d.path().join("store/objects")).is_empty());
    assert!(files_under(&d.path().join("store/tmp")).is_empty());
}

#[tokio::test]
async fn non_storage_sop_class_rejected() {
    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let mut obj = dicom_object::open_file(&file).unwrap();
    obj.put(DataElement::new(
        tags::SOP_CLASS_UID,
        VR::UI,
        PrimitiveValue::from("1.2.840.10008.1.1"),
    ));
    obj.write_to_file(&file).unwrap();

    let ing = Ingestor::open(config(&d.path().join("store")))
        .await
        .unwrap();
    let err = ing.import_file(&file).await.unwrap_err();
    assert!(
        matches!(err.error, IngestError::UnsupportedSopClass(_)),
        "{err:?}"
    );
    assert_eq!(err.status(), 0x0122);
    assert!(files_under(&d.path().join("store/objects")).is_empty());
    assert!(files_under(&d.path().join("store/tmp")).is_empty());
}

#[test]
fn storage_sop_class_detection() {
    // CT Image Storage
    assert!(is_storage_sop_class("1.2.840.10008.5.1.4.1.1.2"));
    // Digital X-Ray Image Storage - For Presentation
    assert!(is_storage_sop_class("1.2.840.10008.5.1.4.1.1.1.1"));
    // Verification
    assert!(!is_storage_sop_class("1.2.840.10008.1.1"));
    // Storage Commitment Push Model
    assert!(!is_storage_sop_class("1.2.840.10008.1.20.1"));
    // Private / unknown
    assert!(is_storage_sop_class("1.3.6.1.4.1.99999.1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_instance_one_row() {
    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let ing = Ingestor::open(config(&d.path().join("store")))
        .await
        .unwrap();
    let mut handles = Vec::new();
    for _ in 0..8 {
        let ing = Arc::clone(&ing);
        let file = file.clone();
        handles.push(tokio::spawn(async move {
            let spooled = spool(&ing, &file).await;
            ing.ingest(spooled, dimse(), Expectations::default()).await
        }));
    }
    let mut fresh = 0;
    for h in handles {
        let s = h.await.unwrap().unwrap();
        if !s.duplicate {
            fresh += 1;
        }
    }
    assert_eq!(fresh, 1);
    let series = ing.index().list_series().await.unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].instance_count, 1);
    assert_eq!(files_under(&d.path().join("store/objects")).len(), 1);
    assert!(files_under(&d.path().join("store/tmp")).is_empty());
}

#[tokio::test]
async fn recover_removes_orphans_and_tmp() {
    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let kept = ing.import_file(&file).await.unwrap();

    // Orphan: committed but never indexed.
    let mut t = ing.create_temp().unwrap();
    t.write_chunk(b"not dicom").await.unwrap();
    let orphan = ing
        .blobs()
        .commit(t.finish().await.unwrap(), BlobKind::Dicom)
        .unwrap();
    std::fs::write(root.join("tmp/stray.part"), b"junk").unwrap();

    let report = ing.recover().await.unwrap();
    assert_eq!(
        report,
        RecoveryReport {
            tmp_removed: 1,
            orphans_removed: 1,
            uploads_aborted: 0,
        }
    );
    assert!(!ing.blobs().path_of(&orphan, BlobKind::Dicom).exists());
    assert!(ing.blobs().path_of(&kept.blob, BlobKind::Dicom).is_file());
    assert!(files_under(&root.join("tmp")).is_empty());
}

#[tokio::test]
async fn overwrite_policy_replaces_and_deletes_old_blob() {
    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let root = d.path().join("store");
    let mut cfg = config(&root);
    cfg.on_duplicate = DuplicatePolicy::Overwrite;
    let ing = Ingestor::open(cfg).await.unwrap();
    let a = ing.import_file(&file).await.unwrap();

    let mut obj = dicom_object::open_file(&file).unwrap();
    obj.put(DataElement::new(
        tags::SERIES_DESCRIPTION,
        VR::LO,
        PrimitiveValue::from("changed"),
    ));
    obj.write_to_file(&file).unwrap();
    let b = ing.import_file(&file).await.unwrap();
    assert!(b.replaced);
    assert_ne!(a.blob, b.blob);
    assert!(!ing.blobs().path_of(&a.blob, BlobKind::Dicom).exists());
    assert!(ing.blobs().path_of(&b.blob, BlobKind::Dicom).is_file());
    assert_eq!(files_under(&root.join("objects")).len(), 1);
}

#[tokio::test]
async fn keep_policy_discards_new_blob() {
    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let a = ing.import_file(&file).await.unwrap();

    let mut obj = dicom_object::open_file(&file).unwrap();
    obj.put(DataElement::new(
        tags::SERIES_DESCRIPTION,
        VR::LO,
        PrimitiveValue::from("changed"),
    ));
    obj.write_to_file(&file).unwrap();
    let b = ing.import_file(&file).await.unwrap();
    assert!(b.duplicate);
    assert!(!b.replaced);
    assert_eq!(b.blob, a.blob);
    assert_eq!(files_under(&root.join("objects")).len(), 1);
}

#[tokio::test]
async fn too_large_maps_to_a700() {
    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let root = d.path().join("store");
    let mut cfg = config(&root);
    cfg.max_instance_bytes = 100;
    let ing = Ingestor::open(cfg).await.unwrap();
    let err = ing.import_file(&file).await.unwrap_err();
    assert!(
        matches!(err.error, IngestError::TooLarge { limit: 100 }),
        "{err:?}"
    );
    assert_eq!(err.status(), 0xA700);
    assert!(files_under(&root.join("tmp")).is_empty());
}

#[tokio::test]
async fn storage_failure_maps_to_a700() {
    use std::os::unix::fs::PermissionsExt;

    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let objects = root.join("objects");
    std::fs::set_permissions(&objects, std::fs::Permissions::from_mode(0o555)).unwrap();
    // Root ignores directory permissions; nothing to test then.
    if std::fs::create_dir(objects.join("probe")).is_ok() {
        std::fs::remove_dir(objects.join("probe")).unwrap();
        std::fs::set_permissions(&objects, std::fs::Permissions::from_mode(0o755)).unwrap();
        eprintln!("skipping: running as root");
        return;
    }

    let result = ing.import_file(&file).await;
    std::fs::set_permissions(&objects, std::fs::Permissions::from_mode(0o755)).unwrap();
    let err = result.unwrap_err();
    assert!(matches!(err.error, IngestError::Storage(_)), "{err:?}");
    // A post-parse failure still reports the instance.
    assert_eq!(err.sop_class_uid.as_deref(), Some(CT_IMAGE_STORAGE));
    assert!(err.sop_instance_uid.is_some());
    assert_eq!(err.status(), 0xA700);
    assert!(ing.index().list_series().await.unwrap().is_empty());
    assert!(files_under(&root.join("tmp")).is_empty());
}

#[test]
fn status_codes() {
    use std::io;
    let cases: Vec<(IngestError, u16)> = vec![
        (IngestError::Unparseable("x".into()), 0xC000),
        (IngestError::MissingAttribute("SOPInstanceUID"), 0xC000),
        (
            IngestError::UidMismatch {
                field: "SOPInstanceUID",
                expected: "1".into(),
                actual: "2".into(),
            },
            0xA900,
        ),
        (
            IngestError::StudyMismatch {
                expected: "1".into(),
                actual: "2".into(),
            },
            0xC409,
        ),
        (IngestError::UnsupportedSopClass("1.2".into()), 0x0122),
        (IngestError::TooLarge { limit: 1 }, 0xA700),
        (IngestError::Storage(io::Error::other("x")), 0xA700),
        (IngestError::Index(sqlx::Error::PoolClosed), 0xA700),
    ];
    for (e, s) in cases {
        assert_eq!(e.status(), s, "{e:?}");
    }
}

#[test]
fn config_defaults() {
    let cfg: StorageConfig = serde_json::from_str("{}").unwrap();
    assert_eq!(cfg.root, PathBuf::from("storage"));
    assert_eq!(cfg.max_instance_bytes, 2_147_483_648);
    assert_eq!(cfg.on_duplicate, DuplicatePolicy::Keep);
}

#[test]
fn ingestor_is_send_sync() {
    fn ss<T: Send + Sync>() {}
    ss::<Arc<Ingestor>>();
}

#[tokio::test]
async fn second_open_on_same_root_fails_until_first_is_dropped() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("store");
    let first = Ingestor::open(config(&root)).await.unwrap();
    let err = Ingestor::open(config(&root)).await.unwrap_err();
    assert!(matches!(err, IngestError::Locked { .. }), "{err:?}");
    assert!(err.to_string().contains("in use"), "{err}");
    drop(first);
    let again = Ingestor::open(config(&root)).await.unwrap();
    drop(again);
}

#[tokio::test]
async fn stats_count_each_outcome_once() {
    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let file_len = std::fs::metadata(&file).unwrap().len();
    let bad = d.path().join("bad.dcm");
    std::fs::write(&bad, b"not a dicom file").unwrap();
    let ing = Ingestor::open(config(&d.path().join("store")))
        .await
        .unwrap();
    assert_eq!(ing.stats(), Default::default());

    ing.import_file(&file).await.unwrap();
    assert!(ing.import_file(&file).await.unwrap().duplicate);
    assert!(ing.import_file(&bad).await.is_err());

    let s = ing.stats();
    assert_eq!((s.stored, s.duplicate, s.failed), (1, 1, 1));
    assert_eq!(s.bytes, 2 * file_len);
    assert!(s.latency_p50_ms.is_some());
}

/// A synthetic NIfTI-1 file: 352-byte header (sizeof_hdr = 348, magic
/// "n+1") followed by a few voxel bytes. The store does not parse it.
fn nifti_bytes(seed: u8) -> Vec<u8> {
    let mut v = vec![0u8; 352];
    v[0..4].copy_from_slice(&348i32.to_le_bytes());
    v[344..348].copy_from_slice(b"n+1\0");
    v.extend([seed; 16]);
    v
}

fn nifti_meta(compressed: bool) -> NiftiMeta {
    NiftiMeta {
        version: 1,
        dims: vec![3, 2, 2, 2],
        pixdim: vec![1.0, 1.0, 1.0, 1.0],
        datatype: 2,
        descrip: String::new(),
        compressed,
        original_name: "vol.nii.gz".into(),
    }
}

fn spool_bytes(ing: &Ingestor, bytes: &[u8]) -> SpooledBlob {
    ing.spool_sync(&mut &bytes[..]).unwrap()
}

#[tokio::test]
async fn ingest_nifti_stores_and_dedups() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let up = ing
        .index()
        .create_upload("import", "x", None)
        .await
        .unwrap();
    let bytes = nifti_bytes(1);

    let a = ing
        .ingest_nifti(spool_bytes(&ing, &bytes), nifti_meta(true), up)
        .await
        .unwrap();
    assert!(!a.duplicate);
    let hex = a.blob.to_hex();
    let expected = root
        .join("objects")
        .join(&hex[0..2])
        .join(&hex[2..4])
        .join(format!("{hex}.nii.gz"));
    assert!(expected.is_file());
    assert_eq!(std::fs::read(&expected).unwrap(), bytes);

    let b = ing
        .ingest_nifti(spool_bytes(&ing, &bytes), nifti_meta(true), up)
        .await
        .unwrap();
    assert!(b.duplicate);
    assert_eq!(b.id, a.id);
    assert_eq!(b.blob, a.blob);
    assert_eq!(files_under(&root.join("objects")), vec![expected]);
    assert!(files_under(&root.join("tmp")).is_empty());

    let c = ing.index().storage_counts().await.unwrap();
    assert_eq!((c.nifti_count, c.nifti_bytes), (1, bytes.len() as i64));
    let st = ing.stats();
    assert_eq!((st.nifti_stored, st.nifti_duplicate), (1, 1));

    // Uncompressed goes to `.nii`.
    let u = ing
        .ingest_nifti(spool_bytes(&ing, &nifti_bytes(2)), nifti_meta(false), up)
        .await
        .unwrap();
    assert!(ing.blobs().path_of(&u.blob, BlobKind::Nifti).is_file());
}

#[tokio::test]
async fn ingest_nifti_unknown_upload_removes_blob() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let err = ing
        .ingest_nifti(spool_bytes(&ing, &nifti_bytes(3)), nifti_meta(false), 4242)
        .await
        .unwrap_err();
    assert!(matches!(err, IngestError::Index(_)), "{err:?}");
    assert!(files_under(&root.join("objects")).is_empty());
    assert!(files_under(&root.join("tmp")).is_empty());
    assert_eq!(ing.stats().failed, 1);
}

#[tokio::test]
async fn recover_keeps_indexed_nifti_and_removes_orphans() {
    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let dicom = ing.import_file(&file).await.unwrap();
    let up = ing
        .index()
        .create_upload("import", "x", None)
        .await
        .unwrap();
    let nii = ing
        .ingest_nifti(spool_bytes(&ing, &nifti_bytes(1)), nifti_meta(true), up)
        .await
        .unwrap();
    ing.index()
        .finish_upload(up, "done", &Default::default())
        .await
        .unwrap();

    // Hand-placed, unreferenced blobs of both kinds.
    let stray_nii = ing
        .blobs()
        .commit(spool_bytes(&ing, &nifti_bytes(9)), BlobKind::Nifti)
        .unwrap();
    let stray_dcm = ing
        .blobs()
        .commit(spool_bytes(&ing, b"orphan dicom"), BlobKind::Dicom)
        .unwrap();
    // A file whose name is not a blob name is never touched.
    let foreign = root.join("objects").join("README");
    std::fs::write(&foreign, b"keep me").unwrap();

    let report = ing.recover().await.unwrap();
    assert_eq!(
        report,
        RecoveryReport {
            tmp_removed: 0,
            orphans_removed: 2,
            uploads_aborted: 0,
        }
    );
    assert!(ing.blobs().path_of(&nii.blob, BlobKind::NiftiGz).is_file());
    assert!(ing.blobs().path_of(&dicom.blob, BlobKind::Dicom).is_file());
    assert!(!ing.blobs().path_of(&stray_nii, BlobKind::Nifti).exists());
    assert!(!ing.blobs().path_of(&stray_dcm, BlobKind::Dicom).exists());
    assert!(foreign.is_file());

    // A second pass finds nothing more.
    assert_eq!(ing.recover().await.unwrap().orphans_removed, 0);
    assert!(ing.blobs().path_of(&nii.blob, BlobKind::NiftiGz).is_file());
}

/// Corrupt one stored hash in `table` with raw SQL, then check that
/// recovery fails and deletes nothing (referenced or not).
async fn recover_fails_closed_on(table: &str) {
    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let dicom = ing.import_file(&file).await.unwrap();
    let up = ing
        .index()
        .create_upload("import", "x", None)
        .await
        .unwrap();
    let nii = ing
        .ingest_nifti(spool_bytes(&ing, &nifti_bytes(1)), nifti_meta(false), up)
        .await
        .unwrap();
    let stray = ing
        .blobs()
        .commit(spool_bytes(&ing, b"unreferenced"), BlobKind::Dicom)
        .unwrap();

    let pool =
        sqlx::SqlitePool::connect(&format!("sqlite://{}", root.join("index.sqlite").display()))
            .await
            .unwrap();
    let n = sqlx::query(&format!(
        "UPDATE {table} SET blob_sha256 = 'not-a-hash' WHERE rowid = (SELECT MIN(rowid) FROM {table})"
    ))
    .execute(&pool)
    .await
    .unwrap()
    .rows_affected();
    assert_eq!(n, 1);
    pool.close().await;

    assert!(ing.recover().await.is_err(), "{table}");
    assert!(ing.blobs().path_of(&dicom.blob, BlobKind::Dicom).is_file());
    assert!(ing.blobs().path_of(&nii.blob, BlobKind::Nifti).is_file());
    assert!(ing.blobs().path_of(&stray, BlobKind::Dicom).is_file());
}

#[tokio::test]
async fn recover_fails_closed_on_corrupt_instance_hash() {
    recover_fails_closed_on("instances").await;
}

#[tokio::test]
async fn recover_fails_closed_on_corrupt_nifti_hash() {
    recover_fails_closed_on("nifti_volumes").await;
}

#[tokio::test]
async fn recover_survives_restart_with_nifti() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("store");
    let blob = {
        let ing = Ingestor::open(config(&root)).await.unwrap();
        let up = ing
            .index()
            .create_upload("import", "x", None)
            .await
            .unwrap();
        ing.ingest_nifti(spool_bytes(&ing, &nifti_bytes(5)), nifti_meta(false), up)
            .await
            .unwrap()
            .blob
    };
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let report = ing.recover().await.unwrap();
    assert_eq!(report.orphans_removed, 0);
    assert_eq!(report.uploads_aborted, 1);
    assert!(ing.blobs().path_of(&blob, BlobKind::Nifti).is_file());
}

#[tokio::test]
async fn abort_running_uploads() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let running = ing
        .index()
        .create_upload("http_archive", "a.zip", None)
        .await
        .unwrap();
    let done = ing
        .index()
        .create_upload("import", "b", None)
        .await
        .unwrap();
    ing.index()
        .finish_upload(done, "done", &Default::default())
        .await
        .unwrap();
    let report = ing.recover().await.unwrap();
    assert_eq!(report.uploads_aborted, 1);
    let r = ing.index().upload(running).await.unwrap().unwrap();
    assert_eq!(r.status, "aborted");
    assert_eq!(
        ing.index().upload(done).await.unwrap().unwrap().status,
        "done"
    );
}

#[tokio::test]
async fn instance_links_after_ingest() {
    let d = tempfile::tempdir().unwrap();
    let file = phantom_file(&d.path().join("src"));
    let ing = Ingestor::open(config(&d.path().join("store")))
        .await
        .unwrap();
    let stored = ing.import_file(&file).await.unwrap();
    let (patient_fk, study_fk) = ing
        .index()
        .instance_links(&stored.sop_instance_uid)
        .await
        .unwrap()
        .expect("links for an indexed instance");

    let up = ing
        .index()
        .create_upload("import", "x", None)
        .await
        .unwrap();
    let n = ing
        .ingest_nifti(spool_bytes(&ing, &nifti_bytes(1)), nifti_meta(true), up)
        .await
        .unwrap();
    ing.index()
        .link_nifti(n.id, Some(patient_fk), Some(study_fk))
        .await
        .unwrap();
    let row = ing.index().nifti_volume(n.id).await.unwrap().unwrap();
    assert_eq!(
        (row.patient_fk, row.study_fk),
        (Some(patient_fk), Some(study_fk))
    );
}

#[tokio::test]
async fn spool_sync_respects_max_instance_bytes() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("store");
    let mut cfg = config(&root);
    cfg.max_instance_bytes = 100;
    let ing = Ingestor::open(cfg).await.unwrap();
    let err = ing.spool_sync(&mut &[0u8; 101][..]).unwrap_err();
    assert!(matches!(err, IngestError::TooLarge { limit: 100 }));
    assert!(files_under(&root.join("tmp")).is_empty());
    let ok = ing.spool_sync(&mut &[0u8; 100][..]).unwrap();
    assert_eq!(ok.len, 100);
}

async fn unscan_all(root: &Path) {
    let pool = index_pool(root).await;
    for q in [
        "DELETE FROM instance_data",
        "UPDATE instances SET scan_version = 0",
        "UPDATE studies SET study_time = NULL",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
}

async fn studies_with_time(root: &Path) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM studies WHERE study_time IS NOT NULL")
        .fetch_one(&index_pool(root).await)
        .await
        .unwrap()
}

#[tokio::test]
async fn ingest_stores_scan_data() {
    let d = tempfile::tempdir().unwrap();
    let ing = Ingestor::open(config(&d.path().join("store")))
        .await
        .unwrap();
    let ids = ingest_slices(&ing, &d.path().join("src"), 1).await;
    let data = ing.index().stored_data(ids[0]).await.unwrap().unwrap();
    assert!(matches!(
        data.frames,
        Some(slicestream_store::FrameTable::Native { .. })
    ));
    let md = ing.index().metadata_batch(&ids).await.unwrap();
    let slicestream_store::MetadataState::Ready(json) = &md[0].1 else {
        panic!("metadata not ready: {:?}", md[0].1)
    };
    let v: serde_json::Value = serde_json::from_slice(json).unwrap();
    assert!(v.get("00100010").is_some());
}

#[tokio::test]
async fn backfill_scans_old_rows() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let src = d.path().join("src");
    phantom::write_phantom(&src, 5, 16).unwrap();
    for i in 1..=5 {
        let f = src.join(format!("IM{i:05}.dcm"));
        let mut obj = dicom_object::open_file(&f).unwrap();
        obj.put(DataElement::new(
            tags::STUDY_TIME,
            VR::TM,
            PrimitiveValue::from("101530"),
        ));
        obj.write_to_file(&f).unwrap();
        ing.import_file(&f).await.unwrap();
    }
    let ids = sqlx::query_scalar::<_, i64>("SELECT id FROM instances ORDER BY id")
        .fetch_all(&index_pool(&root).await)
        .await
        .unwrap();
    let before = studies_with_time(&root).await;
    assert_eq!(before, 1, "the files have a study time");
    unscan_all(&root).await;
    assert_eq!(studies_with_time(&root).await, 0);
    assert!(ing.index().stored_data(ids[0]).await.unwrap().is_none());

    let mut got = Vec::new();
    for _ in 0..4 {
        got.push(ing.backfill_batch(2).await.unwrap());
    }
    assert_eq!(got, vec![2, 2, 1, 0]);
    assert!(ing.index().unscanned(10).await.unwrap().is_empty());
    for id in ids {
        assert!(ing.index().stored_data(id).await.unwrap().is_some());
    }
    assert_eq!(studies_with_time(&root).await, before);
}

#[tokio::test]
async fn backfill_skips_instance_replaced_meanwhile() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("store");
    let mut cfg = config(&root);
    cfg.on_duplicate = DuplicatePolicy::Overwrite;
    let ing = Ingestor::open(cfg).await.unwrap();
    let file = phantom_file(&d.path().join("src"));
    ing.import_file(&file).await.unwrap();
    unscan_all(&root).await;

    // The backfill read the row and scanned the old bytes...
    let (id, _, old_blob) = ing.index().unscanned(10).await.unwrap().remove(0);
    let old_scan = slicestream_store::scan_instance(&file).unwrap();
    // ...then an overwrite replaced the instance.
    let mut obj = dicom_object::open_file(&file).unwrap();
    obj.put(DataElement::new(
        tags::SERIES_DESCRIPTION,
        VR::LO,
        PrimitiveValue::from("changed"),
    ));
    obj.write_to_file(&file).unwrap();
    let b = ing.import_file(&file).await.unwrap();
    assert!(b.replaced);

    assert!(
        !ing.index()
            .write_scan(id, &old_blob, &old_scan)
            .await
            .unwrap()
    );
    let md = ing.index().metadata_batch(&[id]).await.unwrap();
    let slicestream_store::MetadataState::Ready(json) = &md[0].1 else {
        panic!("not ready")
    };
    assert!(String::from_utf8_lossy(json).contains("changed"));
}

#[tokio::test]
async fn backfill_records_unreadable_blob_problem() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let ids = ingest_slices(&ing, &d.path().join("src"), 1).await;
    unscan_all(&root).await;
    let (_, _, blob) = ing.index().unscanned(1).await.unwrap().remove(0);
    std::fs::remove_file(ing.blobs().path_of(&blob, BlobKind::Dicom)).unwrap();

    assert_eq!(ing.backfill_batch(10).await.unwrap(), 1);
    assert_eq!(ing.backfill_batch(10).await.unwrap(), 0);
    let md = ing.index().metadata_batch(&ids).await.unwrap();
    assert!(matches!(
        md[0].1,
        slicestream_store::MetadataState::Unavailable(_)
    ));
}

#[tokio::test]
async fn ensure_scanned_single() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let ids = ingest_slices(&ing, &d.path().join("src"), 2).await;
    unscan_all(&root).await;

    ing.ensure_scanned(ids[0]).await.unwrap();
    assert!(ing.index().stored_data(ids[0]).await.unwrap().is_some());
    assert!(ing.index().stored_data(ids[1]).await.unwrap().is_none());
    // Already scanned, and unknown ids, are no-ops.
    ing.ensure_scanned(ids[0]).await.unwrap();
    ing.ensure_scanned(9999).await.unwrap();
}

#[tokio::test]
async fn ingest_rejects_deflated_as_before() {
    let d = tempfile::tempdir().unwrap();
    let ing = Ingestor::open(config(&d.path().join("store")))
        .await
        .unwrap();
    let file = support::deflated(&d.path().join("src"));
    let err = ing.import_file(&file).await.unwrap_err();
    assert!(
        matches!(&err.error, IngestError::Unparseable(m)
            if m == "unsupported transfer syntax 1.2.840.10008.1.2.1.99"),
        "{err:?}"
    );
    assert_eq!(err.status(), 0xC000);
    assert!(ing.index().list_series().await.unwrap().is_empty());
    assert!(files_under(&ing.config().root.join("objects")).is_empty());
}

/// Run `sql` on the index through a second connection.
async fn exec(root: &Path, sql: &str) {
    sqlx::query(sql)
        .execute(&index_pool(root).await)
        .await
        .unwrap();
}

async fn scan_problem(root: &Path, id: i64) -> Option<String> {
    sqlx::query_scalar("SELECT scan_problem FROM instances WHERE id = ?1")
        .bind(id)
        .fetch_one(&index_pool(root).await)
        .await
        .unwrap()
}

#[tokio::test]
async fn backfill_leaves_rows_unscanned_when_the_database_fails() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let ids = ingest_slices(&ing, &d.path().join("src"), 3).await;
    unscan_all(&root).await;
    // A failure that is not caused by any row (here the table is missing;
    // a full disk or a busy database behaves the same) must not be recorded
    // as a permanent scan problem.
    exec(
        &root,
        "ALTER TABLE instance_data RENAME TO instance_data_away",
    )
    .await;

    assert!(ing.backfill_batch(10).await.is_err());
    assert_eq!(ing.index().unscanned(10).await.unwrap().len(), 3);
    for &id in &ids {
        assert_eq!(scan_problem(&root, id).await, None);
    }

    // Once the database works again, the next batch scans every row.
    exec(
        &root,
        "ALTER TABLE instance_data_away RENAME TO instance_data",
    )
    .await;
    assert_eq!(ing.backfill_batch(10).await.unwrap(), 3);
    for &id in &ids {
        let data = ing.index().stored_data(id).await.unwrap().expect("scanned");
        assert!(data.frames.is_some(), "{id}");
        assert_eq!(scan_problem(&root, id).await, None);
    }
}

#[tokio::test]
async fn backfill_continues_past_a_row_whose_write_fails() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let ids = ingest_slices(&ing, &d.path().join("src"), 3).await;
    unscan_all(&root).await;
    // Writing the scan of the first row is refused by a constraint, as a
    // row the index will never take (SQLITE_TOOBIG, a constraint) would be.
    exec(
        &root,
        &format!(
            "CREATE TRIGGER fail_first BEFORE INSERT ON instance_data
             WHEN NEW.instance_fk = {} BEGIN SELECT RAISE(ABORT, 'injected'); END",
            ids[0]
        ),
    )
    .await;

    assert_eq!(ing.backfill_batch(10).await.unwrap(), 3);
    assert!(ing.index().unscanned(10).await.unwrap().is_empty());
    // The failing row is recorded as a problem, so it does not come back...
    assert_eq!(
        scan_problem(&root, ids[0]).await.as_deref(),
        Some("the scan could not be recorded")
    );
    let md = ing.index().metadata_batch(&ids).await.unwrap();
    assert!(matches!(
        md[0].1,
        slicestream_store::MetadataState::Unavailable(_)
    ));
    // ...and the rows after it are scanned.
    for &id in &ids[1..] {
        let data = ing.index().stored_data(id).await.unwrap().expect("scanned");
        assert!(data.frames.is_some(), "{id}");
        assert_eq!(scan_problem(&root, id).await, None);
    }
    assert_eq!(ing.backfill_batch(10).await.unwrap(), 0);
}

#[tokio::test]
async fn backfill_skips_a_row_it_cannot_record_at_all() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let ids = ingest_slices(&ing, &d.path().join("src"), 3).await;
    unscan_all(&root).await;
    // Neither the scan nor the failure can be written for the first row.
    exec(
        &root,
        &format!(
            "CREATE TRIGGER lock_first BEFORE UPDATE ON instances
             WHEN OLD.id = {} BEGIN SELECT RAISE(ABORT, 'injected'); END",
            ids[0]
        ),
    )
    .await;

    // The row is skipped for this batch; the others are scanned.
    assert_eq!(ing.backfill_batch(10).await.unwrap(), 2);
    for &id in &ids[1..] {
        assert!(ing.index().stored_data(id).await.unwrap().is_some(), "{id}");
    }
    let left: Vec<i64> = ing
        .index()
        .unscanned(10)
        .await
        .unwrap()
        .iter()
        .map(|r| r.0)
        .collect();
    assert_eq!(left, [ids[0]]);
    // A batch that records nothing is an error, so the caller backs off
    // instead of spinning on the same rows.
    assert!(ing.backfill_batch(10).await.is_err());
    // Once the database accepts the write, the row is scanned.
    exec(&root, "DROP TRIGGER lock_first").await;
    assert_eq!(ing.backfill_batch(10).await.unwrap(), 1);
    assert_eq!(ing.backfill_batch(10).await.unwrap(), 0);
    assert!(ing.index().stored_data(ids[0]).await.unwrap().is_some());
}

/// Drain the events received so far, as SOP Instance UIDs.
fn sops_sent(
    rx: &mut tokio::sync::broadcast::Receiver<slicestream_store::StoredEvent>,
) -> Vec<String> {
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e.sop_instance_uid);
    }
    out.sort();
    out
}

#[tokio::test]
async fn backfill_and_ensure_scanned_send_events() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("store");
    let ing = Ingestor::open(config(&root)).await.unwrap();
    let ids = ingest_slices(&ing, &d.path().join("src"), 3).await;
    let mut rows = ing.index().unscanned(10).await.unwrap();
    assert!(rows.is_empty());
    unscan_all(&root).await;
    rows = ing.index().unscanned(10).await.unwrap();
    assert_eq!(rows[0].0, ids[0]);
    // The rows the backfill will scan, after ids[0].
    let mut rest: Vec<String> = rows[1..].iter().map(|r| r.1.clone()).collect();
    rest.sort();
    let series = ing.index().list_series().await.unwrap()[0]
        .series_instance_uid
        .clone();

    let mut rx = ing.subscribe();
    // On demand: one event, for that instance's series.
    ing.ensure_scanned(ids[0]).await.unwrap();
    let e = rx.try_recv().expect("an event after ensure_scanned");
    assert_eq!(e.series_instance_uid, series);
    assert_eq!(e.sop_instance_uid, rows[0].1);
    // Already scanned: nothing written, no event.
    ing.ensure_scanned(ids[0]).await.unwrap();
    assert!(rx.try_recv().is_err());

    // In the background: one event per row scanned.
    assert_eq!(ing.backfill_batch(10).await.unwrap(), 2);
    assert_eq!(sops_sent(&mut rx), rest);
}
