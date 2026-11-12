#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::tags;
use dicom_dictionary_std::uids::CT_IMAGE_STORAGE;
use slicestream_store::{
    DuplicatePolicy, Expectations, IngestError, IngestSource, Ingestor, RecoveryReport,
    SpooledBlob, StorageConfig, is_storage_sop_class,
};

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
    assert!(ing.blobs().path_of(&stored.blob).is_file());

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
    let orphan = ing.blobs().commit(t.finish().await.unwrap()).unwrap();
    std::fs::write(root.join("tmp/stray.part"), b"junk").unwrap();

    let report = ing.recover().await.unwrap();
    assert_eq!(
        report,
        RecoveryReport {
            tmp_removed: 1,
            orphans_removed: 1
        }
    );
    assert!(!ing.blobs().path_of(&orphan).exists());
    assert!(ing.blobs().path_of(&kept.blob).is_file());
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
    assert!(!ing.blobs().path_of(&a.blob).exists());
    assert!(ing.blobs().path_of(&b.blob).is_file());
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
