//! `[htj2k] mode` against the full server: the background worker makes
//! copies of stored phantom instances, `off` makes none.

#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

mod support;

use std::time::Duration;

use slicestream_server::config::Htj2kMode;
use slicestream_store::{Ingestor, StorageConfig};
use support::*;

/// A temp dir whose store holds `n` phantom instances.
async fn populated(n: usize) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    phantom::write_phantom(&src, n, 16).unwrap();
    let ingestor = Ingestor::open(StorageConfig {
        root: dir.path().join("store"),
        ..Default::default()
    })
    .await
    .unwrap();
    for i in 1..=n {
        ingestor
            .import_file(&src.join(format!("IM{i:05}.dcm")))
            .await
            .unwrap();
    }
    drop(ingestor);
    dir
}

/// Instances of the store at `dir` that have a copy, and the total.
async fn copy_counts(dir: &std::path::Path) -> (usize, usize) {
    let ingestor = Ingestor::open(StorageConfig {
        root: dir.join("store"),
        ..Default::default()
    })
    .await
    .unwrap();
    let (mut with_copy, mut total) = (0, 0);
    for s in ingestor.index().list_series().await.unwrap() {
        for r in ingestor
            .index()
            .series_slices(&s.series_instance_uid)
            .await
            .unwrap()
        {
            total += 1;
            with_copy += usize::from(ingestor.copy_frames(r.id, &r.blob).await.unwrap().is_some());
        }
    }
    (with_copy, total)
}

/// Rows of `derived_htj2k`, read next to the running server.
async fn copy_rows(dir: &std::path::Path) -> i64 {
    let pool = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}?mode=ro",
        dir.join("store").join("index.sqlite").display()
    ))
    .await
    .unwrap();
    let n = sqlx::query_scalar("SELECT count(*) FROM derived_htj2k")
        .fetch_one(&pool)
        .await
        .unwrap_or(0);
    pool.close().await;
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_makes_copies_in_background() {
    let srv = TestServer::start_in(populated(2).await, |_| {}).await;
    let root = srv.root().parent().unwrap().to_path_buf();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while copy_rows(&root).await < 2 {
        assert!(tokio::time::Instant::now() < deadline, "no copies in 15 s");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let dir = srv.stop_keep_dir().await;
    assert_eq!(copy_counts(dir.path()).await, (2, 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn off_mode_makes_no_copies() {
    let srv = TestServer::start_in(populated(2).await, |c| c.htj2k.mode = Htj2kMode::Off).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let dir = srv.stop_keep_dir().await;
    assert_eq!(copy_counts(dir.path()).await, (0, 2));
}

/// SOP Instance UIDs with a `ready` copy, read next to the running server.
async fn copied_sops(dir: &std::path::Path) -> Vec<String> {
    let pool = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}?mode=ro",
        dir.join("store").join("index.sqlite").display()
    ))
    .await
    .unwrap();
    let sops = sqlx::query_scalar(
        "SELECT i.sop_instance_uid FROM derived_htj2k d
         JOIN instances i ON i.id = d.instance_fk WHERE d.state = 'ready'",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    pool.close().await;
    sops
}

/// `on-demand`: a request for HTJ2K frames gets a stored copy made for
/// its instance afterwards; an instance nobody asked for gets none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_demand_mode_copies_requested_instances_only() {
    let srv = TestServer::start_in(populated(2).await, |c| {
        c.htj2k.mode = Htj2kMode::OnDemand;
    })
    .await;
    let root = srv.root().parent().unwrap().to_path_buf();
    let study = "2.25.1000000000000000000000000000002";
    let series = "2.25.1000000000000000000000000000001";
    let requested = "2.25.100000000000000000000000000100000";
    let r = srv
        .get(
            &format!("/dicom-web/studies/{study}/series/{series}/instances/{requested}/frames/1"),
            &[("Accept", "multipart/related; type=\"image/jphc\"")],
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while copied_sops(&root).await.is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no copy of the requested instance in 15 s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Give a worker that would copy the rest time to do so.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let dir = srv.stop_keep_dir().await;
    assert_eq!(copied_sops(dir.path()).await, [requested]);
    assert_eq!(copy_counts(dir.path()).await, (1, 2));
}
