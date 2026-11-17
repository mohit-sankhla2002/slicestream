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

/// Run a server in `mode` for `run`, then count the instances with a copy.
async fn copies_after(mode: Htj2kMode, run: Duration) -> (usize, usize) {
    let dir = populated(2).await;
    let srv = TestServer::start_in(dir, |c| c.htj2k.mode = mode).await;
    tokio::time::sleep(run).await;
    let dir = srv.stop_keep_dir().await;
    let ingestor = Ingestor::open(StorageConfig {
        root: dir.path().join("store"),
        ..Default::default()
    })
    .await
    .unwrap();
    let mut with_copy = 0;
    let mut total = 0;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_makes_copies_in_background() {
    let (copies, total) = copies_after(Htj2kMode::Background, Duration::from_secs(3)).await;
    assert_eq!(total, 2);
    assert_eq!(copies, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn off_mode_makes_no_copies() {
    let (copies, total) = copies_after(Htj2kMode::Off, Duration::from_secs(2)).await;
    assert_eq!(total, 2);
    assert_eq!(copies, 0);
}
