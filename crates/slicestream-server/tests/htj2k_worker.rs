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
