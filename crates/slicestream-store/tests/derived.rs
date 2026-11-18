#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;
mod support;

use std::path::Path;

use support::{index_pool, ingest_slices};

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

// ---- copy maker ----

use slicestream_codec::{decode_full, samples};
use slicestream_core::frame::read_frame_at;

async fn source_of(ing: &Ingestor, id: i64) -> BlobHash {
    ing.index().instance_blob(id).await.unwrap().unwrap().1
}

/// Per frame: the samples of the original and the decoded copy.
async fn originals_and_copies(ing: &Ingestor, id: i64) -> Vec<(Vec<i32>, Vec<i32>)> {
    let source = source_of(ing, id).await;
    let (copy, path) = ing.copy_frames(id, &source).await.unwrap().expect("a copy");
    let bytes = std::fs::read(path).unwrap();
    let info = ing.index().frame_info(id).await.unwrap().unwrap();
    let table = ing
        .index()
        .stored_data(id)
        .await
        .unwrap()
        .unwrap()
        .frames
        .unwrap();
    let original = ing.blobs().path_of(&source, BlobKind::Dicom);
    copy.0
        .iter()
        .enumerate()
        .map(|(k, f)| {
            let raw = read_frame_at(&original, &table.location(k as u32).unwrap()).unwrap();
            let end = (f.offset + f.len) as usize;
            let decoded = decode_full(&bytes[f.offset as usize..end]).unwrap();
            (samples(&raw, &info).unwrap(), decoded)
        })
        .collect()
}

async fn copy_rows(ing: &Ingestor) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM derived_htj2k")
        .fetch_one(&index_pool(&ing.config().root).await)
        .await
        .unwrap()
}

fn copy_blobs(root: &Path) -> usize {
    walkdir(&root.join("objects"))
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "j2cs"))
        .count()
}

fn walkdir(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walkdir(&p));
        } else {
            out.push(p);
        }
    }
    out
}

#[tokio::test]
async fn worker_makes_copies_that_decode_to_originals() {
    let d = tempfile::tempdir().unwrap();
    let (ing, ids, idx) = open(d.path(), DuplicatePolicy::Keep).await;
    assert_eq!(ing.copy_batch(10, 2, || false).await.unwrap(), 3);
    assert!(idx.copy_candidates(10).await.unwrap().is_empty());
    for id in ids {
        let pairs = originals_and_copies(&ing, id).await;
        assert_eq!(pairs.len(), 1);
        for (original, decoded) in pairs {
            assert_eq!(original, decoded);
        }
    }
    assert_eq!(ing.copy_batch(10, 2, || false).await.unwrap(), 0);
}

#[tokio::test]
async fn copy_batch_stops_between_groups() {
    let d = tempfile::tempdir().unwrap();
    let (ing, _ids, idx) = open(d.path(), DuplicatePolicy::Keep).await;
    // Asked before each group of one instance: the first passes, the rest stop.
    let asked = std::cell::Cell::new(0);
    let stop = || {
        asked.set(asked.get() + 1);
        asked.get() > 1
    };
    assert_eq!(ing.copy_batch(10, 1, stop).await.unwrap(), 1);
    assert_eq!(idx.copy_candidates(10).await.unwrap().len(), 2);
}

#[tokio::test]
async fn ineligible_instance_recorded_with_reason() {
    let d = tempfile::tempdir().unwrap();
    let ing = Ingestor::open(config(&d.path().join("store"), DuplicatePolicy::Keep))
        .await
        .unwrap();
    ing.import_file(&support::rgb_instance(&d.path().join("src")))
        .await
        .unwrap();
    assert_eq!(ing.copy_batch(10, 1, || false).await.unwrap(), 1);
    let id = ing.index().copy_candidates(10).await.unwrap();
    assert!(id.is_empty(), "not retried");
    let pool = index_pool(&ing.config().root).await;
    let (state, problem): (String, String) =
        sqlx::query_as("SELECT state, problem FROM derived_htj2k")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, "not_eligible");
    assert!(problem.contains("samples per pixel"), "{problem}");
    assert_eq!(copy_blobs(&ing.config().root), 0);
}

#[tokio::test]
async fn transient_errors_leave_no_row() {
    let d = tempfile::tempdir().unwrap();
    let (ing, ids, idx) = open(d.path(), DuplicatePolicy::Keep).await;
    let pool = index_pool(&ing.config().root).await;
    sqlx::query("ALTER TABLE derived_htj2k RENAME TO derived_away")
        .execute(&pool)
        .await
        .unwrap();
    assert!(ing.copy_batch(10, 2, || false).await.is_err());
    assert!(ing.make_copy(ids[0]).await.is_err());
    assert_eq!(copy_blobs(&ing.config().root), 0);
    sqlx::query("ALTER TABLE derived_away RENAME TO derived_htj2k")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(copy_rows(&ing).await, 0);
    assert_eq!(ing.copy_batch(10, 2, || false).await.unwrap(), 3);
    assert_eq!(copy_rows(&ing).await, 3);
    assert!(idx.copy_candidates(10).await.unwrap().is_empty());
}

#[tokio::test]
async fn prioritized_instance_goes_first() {
    let d = tempfile::tempdir().unwrap();
    let ing = Ingestor::open(config(&d.path().join("store"), DuplicatePolicy::Keep))
        .await
        .unwrap();
    let ids = ingest_slices(&ing, &d.path().join("src"), 5).await;
    ing.prioritize_copy(ids[0]);
    ing.prioritize_copy(ids[0]);
    assert_eq!(ing.copy_batch(1, 1, || false).await.unwrap(), 1);
    let source = source_of(&ing, ids[0]).await;
    assert!(ing.copy_frames(ids[0], &source).await.unwrap().is_some());
    assert_eq!(copy_rows(&ing).await, 1);
    // Queued once only: the next batch takes the newest.
    assert_eq!(ing.copy_batch(1, 1, || false).await.unwrap(), 1);
    let source = source_of(&ing, ids[4]).await;
    assert!(ing.copy_frames(ids[4], &source).await.unwrap().is_some());
}

/// `copy_requested` copies only queued instances, never the other
/// candidates; `copy_request_waiting` wakes for a request made before it.
#[tokio::test]
async fn copy_requested_copies_only_queued_instances() {
    let d = tempfile::tempdir().unwrap();
    let (ing, ids, idx) = open(d.path(), DuplicatePolicy::Keep).await;
    assert_eq!(ing.copy_requested(10, 2, || false).await.unwrap(), 0);
    assert_eq!(copy_rows(&ing).await, 0);

    ing.prioritize_copy(ids[1]);
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ing.copy_request_waiting(),
    )
    .await
    .expect("a queued request wakes the waiter");
    assert_eq!(ing.copy_requested(10, 2, || false).await.unwrap(), 1);
    assert_eq!(copy_rows(&ing).await, 1);
    let source = source_of(&ing, ids[1]).await;
    assert!(ing.copy_frames(ids[1], &source).await.unwrap().is_some());
    assert_eq!(ing.copy_requested(10, 2, || false).await.unwrap(), 0);
    assert_eq!(idx.copy_candidates(10).await.unwrap().len(), 2);
}

#[tokio::test]
async fn transient_encode_stores_nothing() {
    let d = tempfile::tempdir().unwrap();
    let (ing, ids, _) = open(d.path(), DuplicatePolicy::Keep).await;
    let out = ing.encode_transient(ids[0], &[1]).await.unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(copy_rows(&ing).await, 0);
    assert_eq!(copy_blobs(&ing.config().root), 0);
    assert!(matches!(
        ing.encode_transient(i64::MAX, &[1]).await,
        Err(slicestream_store::IngestError::NotFound(_))
    ));
    assert!(ing.encode_transient(ids[0], &[2]).await.is_err());
    assert!(ing.encode_transient(ids[0], &[0]).await.is_err());
    // Prioritized: the first batch of one makes it, not the newest.
    assert_eq!(ing.copy_batch(1, 1, || false).await.unwrap(), 1);
    let source = source_of(&ing, ids[0]).await;
    assert!(ing.copy_frames(ids[0], &source).await.unwrap().is_some());
}

#[tokio::test]
async fn transient_encode_of_ineligible_instance_is_unparseable() {
    let d = tempfile::tempdir().unwrap();
    let ing = Ingestor::open(config(&d.path().join("store"), DuplicatePolicy::Keep))
        .await
        .unwrap();
    ing.import_file(&support::rgb_instance(&d.path().join("src")))
        .await
        .unwrap();
    let id = sqlx::query_scalar::<_, i64>("SELECT id FROM instances")
        .fetch_one(&index_pool(&ing.config().root).await)
        .await
        .unwrap();
    let err = ing.encode_transient(id, &[1]).await.unwrap_err();
    assert!(
        matches!(&err, slicestream_store::IngestError::Unparseable(r) if r.contains("samples per pixel")),
        "{err}"
    );
}

#[tokio::test]
async fn make_copy_is_not_repeated() {
    let d = tempfile::tempdir().unwrap();
    let (ing, ids, _) = open(d.path(), DuplicatePolicy::Keep).await;
    let (a, b) = tokio::join!(ing.make_copy(ids[0]), ing.make_copy(ids[0]));
    a.unwrap();
    b.unwrap();
    assert_eq!(copy_rows(&ing).await, 1);
    assert_eq!(copy_blobs(&ing.config().root), 1);
}

#[tokio::test]
async fn multi_frame_copy_offsets() {
    let d = tempfile::tempdir().unwrap();
    let ing = Ingestor::open(config(&d.path().join("store"), DuplicatePolicy::Keep))
        .await
        .unwrap();
    ing.import_file(&support::native_multiframe(&d.path().join("src"), 4))
        .await
        .unwrap();
    assert_eq!(ing.copy_batch(10, 1, || false).await.unwrap(), 1);
    let id = sqlx::query_scalar::<_, i64>("SELECT id FROM instances")
        .fetch_one(&index_pool(&ing.config().root).await)
        .await
        .unwrap();
    let source = source_of(&ing, id).await;
    let (copy, path) = ing.copy_frames(id, &source).await.unwrap().unwrap();
    assert_eq!(copy.0.len(), 4);
    let mut at = 0;
    for f in &copy.0 {
        assert_eq!(f.offset, at);
        assert!(f.len > 0 && f.ends.last().is_none_or(|e| u64::from(*e) <= f.len));
        at += f.len;
    }
    assert_eq!(at, std::fs::metadata(path).unwrap().len());
    for (k, (original, decoded)) in originals_and_copies(&ing, id).await.iter().enumerate() {
        assert_eq!(original, decoded, "frame {k}");
        let expected: Vec<i32> = (0..64 * 64)
            .map(|i| i32::from(support::multiframe_sample(k as u32, i)))
            .collect();
        assert_eq!(*original, expected);
    }
}
