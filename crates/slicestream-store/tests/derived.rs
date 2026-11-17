#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

use std::path::Path;

use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::tags;
use slicestream_store::{
    BlobHash, BlobKind, CopyFrame, CopyFrames, CopyState, DuplicatePolicy, Index, Ingestor,
    RecoveryReport, StorageConfig,
};

fn config(root: &Path, on_duplicate: DuplicatePolicy) -> StorageConfig {
    StorageConfig {
        root: root.to_path_buf(),
        max_instance_bytes: 2_147_483_648,
        on_duplicate,
    }
}

async fn index_pool(root: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect(&format!("sqlite://{}", root.join("index.sqlite").display()))
        .await
        .unwrap()
}

/// Ingest `n` phantom slices; returns the instance ids in id order.
async fn ingest_slices(ing: &Ingestor, src: &Path, n: usize) -> Vec<i64> {
    phantom::write_phantom(src, n, 16).unwrap();
    for i in 1..=n {
        ing.import_file(&src.join(format!("IM{i:05}.dcm")))
            .await
            .unwrap();
    }
    sqlx::query_scalar::<_, i64>("SELECT id FROM instances ORDER BY id")
        .fetch_all(&index_pool(&ing.config().root).await)
        .await
        .unwrap()
}

async fn open(d: &Path, policy: DuplicatePolicy) -> (std::sync::Arc<Ingestor>, Vec<i64>, Index) {
    let ing = Ingestor::open(config(&d.join("store"), policy))
        .await
        .unwrap();
    let ids = ingest_slices(&ing, &d.join("src"), 3).await;
    let idx = ing.index().clone();
    (ing, ids, idx)
}

fn frames() -> CopyFrames {
    CopyFrames(vec![CopyFrame {
        offset: 0,
        len: 90,
        ends: vec![10, 50, 90],
    }])
}

#[test]
fn copy_frames_round_trip() {
    let f = frames();
    assert_eq!(CopyFrames::decode(&f.encode()), Some(f));
    assert_eq!(CopyFrames::decode(b"junk"), None);
}

#[tokio::test]
async fn candidates_newest_first_and_skip_unscanned() {
    let d = tempfile::tempdir().unwrap();
    let (ing, ids, idx) = open(d.path(), DuplicatePolicy::Keep).await;
    let all = idx.copy_candidates(10).await.unwrap();
    assert_eq!(
        all.iter().map(|c| c.0).collect::<Vec<_>>(),
        [ids[2], ids[1], ids[0]]
    );

    sqlx::query("UPDATE instances SET scan_version = 0 WHERE id = ?1")
        .bind(ids[1])
        .execute(&index_pool(&ing.config().root).await)
        .await
        .unwrap();
    let some = idx.copy_candidates(10).await.unwrap();
    assert_eq!(
        some.iter().map(|c| c.0).collect::<Vec<_>>(),
        [ids[2], ids[0]]
    );
    assert_eq!(idx.copy_candidates(1).await.unwrap().len(), 1);
}

#[tokio::test]
async fn write_and_read_copy() {
    let d = tempfile::tempdir().unwrap();
    let (_ing, ids, idx) = open(d.path(), DuplicatePolicy::Keep).await;
    let (id, _, src) = idx.copy_candidates(1).await.unwrap().remove(0);
    assert_eq!(id, ids[2]);
    assert_eq!(idx.copy_for(id, &src).await.unwrap(), None);
    let state = CopyState::Ready {
        blob: BlobHash([7; 32]),
        frames: frames(),
    };
    assert!(idx.write_copy(id, &src, &state).await.unwrap());
    assert_eq!(idx.copy_for(id, &src).await.unwrap(), Some(state));
    let left = idx.copy_candidates(10).await.unwrap();
    assert_eq!(left.len(), 2);
    assert!(left.iter().all(|c| c.0 != id));
}

#[tokio::test]
async fn copy_write_skipped_after_overwrite() {
    let d = tempfile::tempdir().unwrap();
    let (ing, ids, idx) = open(d.path(), DuplicatePolicy::Overwrite).await;
    let (_, _, old) = idx
        .copy_candidates(10)
        .await
        .unwrap()
        .into_iter()
        .find(|c| c.0 == ids[0])
        .unwrap();
    let file = d.path().join("src/IM00001.dcm");
    let mut obj = dicom_object::open_file(&file).unwrap();
    obj.put(DataElement::new(
        tags::SERIES_DESCRIPTION,
        VR::LO,
        PrimitiveValue::from("changed"),
    ));
    obj.write_to_file(&file).unwrap();
    assert!(ing.import_file(&file).await.unwrap().replaced);

    let state = CopyState::NotEligible("x".into());
    assert!(!idx.write_copy(ids[0], &old, &state).await.unwrap());
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM derived_htj2k")
        .fetch_one(&index_pool(&ing.config().root).await)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn stale_copy_is_ignored() {
    let d = tempfile::tempdir().unwrap();
    let (ing, ids, idx) = open(d.path(), DuplicatePolicy::Keep).await;
    let (id, _, a) = idx.copy_candidates(1).await.unwrap().remove(0);
    assert_eq!(id, ids[2]);
    let state = CopyState::Failed("boom".into());
    assert!(idx.write_copy(id, &a, &state).await.unwrap());
    assert_eq!(idx.copy_for(id, &a).await.unwrap(), Some(state));

    // The instance now holds blob B.
    let b = BlobHash([9; 32]);
    sqlx::query("UPDATE instances SET blob_sha256 = ?2 WHERE id = ?1")
        .bind(id)
        .bind(b.to_hex())
        .execute(&index_pool(&ing.config().root).await)
        .await
        .unwrap();
    assert_eq!(idx.copy_for(id, &b).await.unwrap(), None);
    assert!(idx.copy_for(id, &a).await.unwrap().is_some());
    assert!(
        idx.copy_candidates(10)
            .await
            .unwrap()
            .iter()
            .any(|c| c.0 == id)
    );

    // A copy by an older codec is ignored too.
    sqlx::query("UPDATE derived_htj2k SET source_blob = ?2, codec_version = 0")
        .bind(id)
        .bind(b.to_hex())
        .execute(&index_pool(&ing.config().root).await)
        .await
        .unwrap();
    assert_eq!(idx.copy_for(id, &b).await.unwrap(), None);
    assert!(
        idx.copy_candidates(10)
            .await
            .unwrap()
            .iter()
            .any(|c| c.0 == id)
    );
}

#[tokio::test]
async fn not_eligible_recorded_once() {
    let d = tempfile::tempdir().unwrap();
    let (_ing, _ids, idx) = open(d.path(), DuplicatePolicy::Keep).await;
    let (id, _, src) = idx.copy_candidates(1).await.unwrap().remove(0);
    let state = CopyState::NotEligible("color".into());
    assert!(idx.write_copy(id, &src, &state).await.unwrap());
    assert_eq!(idx.copy_for(id, &src).await.unwrap(), Some(state));
    assert!(
        idx.copy_candidates(10)
            .await
            .unwrap()
            .iter()
            .all(|c| c.0 != id)
    );
}

#[tokio::test]
async fn recovery_keeps_ready_copies() {
    let d = tempfile::tempdir().unwrap();
    let (ing, _ids, idx) = open(d.path(), DuplicatePolicy::Keep).await;
    let (id, _, src) = idx.copy_candidates(1).await.unwrap().remove(0);

    let commit = |bytes: &[u8]| {
        let mut r = bytes;
        let s = ing.blobs().spool_sync(&mut r, 1 << 20).unwrap();
        ing.blobs().commit(s, BlobKind::Htj2k).unwrap()
    };
    let copy = commit(b"copy bytes");
    let stray = commit(b"unreferenced copy");
    let state = CopyState::Ready {
        blob: copy,
        frames: frames(),
    };
    assert!(idx.write_copy(id, &src, &state).await.unwrap());

    let report = ing.recover().await.unwrap();
    assert_eq!(
        report,
        RecoveryReport {
            tmp_removed: 0,
            orphans_removed: 1,
            uploads_aborted: 0,
        }
    );
    assert!(ing.blobs().path_of(&copy, BlobKind::Htj2k).is_file());
    assert!(!ing.blobs().path_of(&stray, BlobKind::Htj2k).exists());
    assert!(
        ing.blobs()
            .list()
            .unwrap()
            .contains(&(copy, BlobKind::Htj2k))
    );
}

#[tokio::test]
async fn migration_0004_on_old_store() {
    use sqlx::sqlite::SqliteConnectOptions;
    let dir = tempfile::tempdir().unwrap();
    let old = dir.path().join("old-migrations");
    std::fs::create_dir(&old).unwrap();
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for name in [
        "0001_init.sql",
        "0002_uploads_nifti.sql",
        "0003_dicomweb.sql",
    ] {
        std::fs::copy(src.join(name), old.join(name)).unwrap();
    }
    let pool = sqlx::SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(dir.path().join("index.db"))
            .create_if_missing(true),
    )
    .await
    .unwrap();
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
                                transfer_syntax, blob_sha256, size_bytes, source, received_at,
                                frame_info, scan_version)
         VALUES ('2.25.3', 1, '1.2', 1, '1.2.840.10008.1.2.1', ?1, 10, 'stow', 't',
                 '{}', ?2)",
    )
    .bind(&hash)
    .bind(slicestream_store::SCAN_VERSION)
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;

    let idx = Index::open(&dir.path().join("index.db")).await.unwrap();
    let c = idx.copy_candidates(10).await.unwrap();
    assert_eq!(c.len(), 1);
    assert_eq!(c[0].1, "2.25.3");
    assert_eq!(c[0].2.to_hex(), hash);
    assert!(idx.blob_hashes().await.unwrap().len() == 1);
}
