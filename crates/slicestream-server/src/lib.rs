//! Reference HTTP + WebSocket server for slicestream: series streaming,
//! DICOMweb (STOW-RS ingest, QIDO-RS search) and single-instance ingest.
#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::handler::HandlerWithoutStateExt;
use axum::http::{HeaderName, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::routing::{get, post};
use axum::serve::ListenerExt;
use slicestream_core::SchedulerConfig;
use slicestream_dimse::StoreScp;
use slicestream_store::Ingestor;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::services::ServeDir;

pub mod archive_ingest;
pub mod archive_jobs;
pub mod catalog;
pub mod config;
pub mod dicomweb;
pub mod hosts;
pub mod linking;
pub mod metrics;
mod progressive;
pub mod routes;
pub mod stow;
pub mod stream;

pub use catalog::SeriesCatalog;
use config::{Config, DicomwebConfig, Htj2kConfig, Htj2kMode, HttpConfig};

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

pub struct AppState {
    pub catalog: Arc<SeriesCatalog>,
    pub ingestor: Arc<Ingestor>,
    pub scheduler: SchedulerConfig,
    pub http: HttpConfig,
    pub dicomweb: DicomwebConfig,
    pub htj2k: Htj2kConfig,
    /// `None` when `metrics_enabled = false`; the console routes are then
    /// not mounted.
    pub metrics: Option<Arc<metrics::Metrics>>,
    pub gauges: Arc<metrics::Gauges>,
    /// `None` when `[archives] enabled = false`; the archive routes are
    /// then not mounted.
    pub archives: Option<Arc<archive_jobs::ArchiveJobs>>,
}

/// Build the application router.
pub fn app(state: Arc<AppState>) -> Router {
    // Ingest bodies are streamed to disk; size is enforced by multer
    // (`max_request_bytes`) and the spool (`max_instance_bytes`).
    let mut ingest = Router::new()
        .route("/dicom-web/studies", post(stow::stow_all))
        .route("/dicom-web/studies/{study}", post(stow::stow_study))
        .route("/api/instances", post(stow::post_instance));
    if !state.dicomweb.enabled {
        // The DICOMweb GET routes are not mounted. Where STOW-RS shares
        // their path, GET answers 404 like any absent route (not 405).
        ingest = ingest
            .route("/dicom-web/studies", get(not_found))
            .route("/dicom-web/studies/{study}", get(not_found));
    }
    let ingest = ingest.layer(DefaultBodyLimit::disable());

    // CORS only for the read API and only for configured origins. Ingest
    // routes and static files never get CORS, so cross-origin pages cannot
    // upload or read through them. (The demo viewer is same-origin.)
    let mut read = Router::new()
        .route("/api/series", get(routes::list_series))
        .route("/api/series/{uid}", get(routes::get_series))
        .route("/api/series/{uid}/frames/{index}", get(routes::get_frame))
        .route("/api/series/{uid}/stream", get(stream::stream_series));
    if state.dicomweb.enabled {
        read = read.merge(dicomweb::routes());
    }
    if let Some(cors) = cors_layer(&state.http.cors_allowed_origins) {
        read = read.layer(cors);
    }

    // Console routes reveal process and storage details: never any CORS.
    let mut console = Router::new();
    if state.metrics.is_some() {
        console = console
            .route("/api/metrics", get(routes::metrics_latest))
            .route("/api/metrics/history", get(routes::metrics_history))
            .route("/api/console/series", get(routes::console_series));
    }

    // Archive uploads and job reports (which reveal patient names): no CORS.
    let mut archives = Router::new();
    if state.archives.is_some() {
        archives = archives
            .route("/api/archives", post(archive_jobs::post_archive))
            .route("/api/archives/{job}", get(archive_jobs::get_archive))
            .layer(DefaultBodyLimit::disable());
    }

    // Every route, including static files, checks Host against DNS rebinding.
    let allowed = Arc::new(hosts::HostAllowList::from_config(&state.http));
    let gauges = Arc::clone(&state.gauges);
    Router::new()
        // GET (QIDO-RS, read) and POST (STOW-RS, ingest) share
        // `/dicom-web/studies` and `/dicom-web/studies/{study}`. axum merges
        // them into one method router whose GET and POST keep their own
        // layers, and whose fallback (any other method, so also the CORS
        // preflight `OPTIONS`) comes from the router merged last. Merging
        // `read` last gives preflight the read CORS layer; POST never gets
        // CORS.
        .merge(ingest)
        .merge(read)
        .merge(console)
        .merge(archives)
        // Static files answer GET/HEAD only; any other method on a path no
        // route claims is 404, so an unmounted API route reads as absent.
        .fallback_service(
            ServeDir::new(&state.http.web_dir)
                .call_fallback_on_method_not_allowed(true)
                .fallback(not_found.into_service()),
        )
        .layer(axum::middleware::from_fn(move |req, next| {
            hosts::host_guard(Arc::clone(&allowed), req, next)
        }))
        // Outermost: counts every request until its response is returned.
        .layer(axum::middleware::from_fn(move |req, next: Next| {
            let gauges = Arc::clone(&gauges);
            async move {
                let _guard = metrics::GaugeGuard::enter(&gauges, metrics::Gauge::HttpInFlight);
                next.run(req).await
            }
        }))
        .with_state(state)
}

async fn not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}

/// CORS for exactly `origins`; `None` when the list is empty.
fn cors_layer(origins: &[String]) -> Option<CorsLayer> {
    let origins: Vec<HeaderValue> = origins
        .iter()
        .filter_map(|o| match HeaderValue::from_str(o) {
            Ok(v) => Some(v),
            Err(_) => {
                tracing::warn!(origin = %o, "ignoring invalid cors_allowed_origins entry");
                None
            }
        })
        .collect();
    if origins.is_empty() {
        return None;
    }
    Some(
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(origins))
            .allow_methods([Method::GET, Method::HEAD])
            // `Accept` with quoted parameters is not CORS-safelisted, so
            // DICOMweb clients send a preflight for it.
            .allow_headers([header::ACCEPT])
            // `Content-Range` gives the size of a ranged HTJ2K frame.
            .expose_headers([
                HeaderName::from_static("x-slicestream-frame-info"),
                header::WARNING,
                header::CONTENT_RANGE,
                header::ACCEPT_RANGES,
            ]),
    )
}

/// How long open HTTP connections (including WebSocket streams) may run
/// after shutdown starts before they are dropped.
const HTTP_SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

/// A started server. `task` finishes after `shutdown` is cancelled and both
/// listeners have stopped, or as soon as either fails.
pub struct Running {
    pub http_addr: SocketAddr,
    /// `None` when `[dimse] enabled = false`.
    pub dimse_addr: Option<SocketAddr>,
    pub task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

/// Open (and lock) storage, bind HTTP and (if enabled) the DIMSE SCP, run
/// recovery, and serve both until `shutdown` is cancelled.
///
/// The listeners are bound before recovery so that a port conflict fails
/// before anything is deleted; nothing is accepted until recovery is done.
pub async fn start(config: Config, shutdown: CancellationToken) -> anyhow::Result<Running> {
    config.validate()?;
    let root = config.storage.root.clone();
    let ingestor = Ingestor::open(config.storage)
        .await
        .with_context(|| format!("opening storage at {}", root.display()))?;

    let listener = tokio::net::TcpListener::bind(config.http.addr)
        .await
        .with_context(|| format!("binding HTTP on {}", config.http.addr))?;
    let http_addr = listener.local_addr()?;

    let dimse_max = config
        .dimse
        .enabled
        .then_some(config.dimse.max_associations);
    let scp = if config.dimse.enabled {
        let addr = config.dimse.addr;
        Some(
            StoreScp::bind(config.dimse, Arc::clone(&ingestor))
                .await
                .with_context(|| format!("binding DIMSE on {addr}"))?,
        )
    } else {
        None
    };
    let dimse_addr = scp.as_ref().map(StoreScp::local_addr);
    let dimse_active = scp.as_ref().map(StoreScp::active_associations);

    let report = ingestor.recover().await.context("storage recovery")?;
    tracing::info!(
        root = %root.display(),
        tmp_removed = report.tmp_removed,
        orphans_removed = report.orphans_removed,
        uploads_aborted = report.uploads_aborted,
        "storage ready"
    );

    // Instances stored before scanning existed are scanned in the
    // background; until then they are scanned when a request needs them.
    let backfill = tokio::spawn(backfill_metadata(
        Arc::clone(&ingestor),
        shutdown.clone(),
        BACKFILL_RETRY,
    ));
    let copier = (config.htj2k.mode == Htj2kMode::Background).then(|| {
        tokio::spawn(copy_worker(
            Arc::clone(&ingestor),
            shutdown.clone(),
            COPY_RETRY,
            COPY_IDLE,
            config.htj2k.worker_count(),
        ))
    });
    if config.dicomweb.enabled && !http_addr.ip().is_loopback() {
        tracing::warn!(
            "DICOMweb read routes are unauthenticated: anyone who can reach {http_addr} can read patient data (see SECURITY.md)"
        );
    }

    let catalog = SeriesCatalog::new(Arc::clone(&ingestor));
    catalog.spawn_invalidator();

    // With `addr` port 0, derive the default allowed hosts from the bound port.
    let mut http = config.http;
    http.addr = http_addr;
    let gauges = Arc::new(metrics::Gauges::default());
    let (metrics, sampler) = match http.metrics_enabled.then(|| {
        metrics::spawn_sampler(
            metrics::MetricsSources {
                ingestor: Arc::clone(&ingestor),
                gauges: Arc::clone(&gauges),
                dimse_active,
                dimse_max,
                http_addr,
                dimse_addr,
            },
            shutdown.clone(),
        )
    }) {
        Some((m, h)) => (Some(m), Some(h)),
        None => (None, None),
    };
    let archives = config.archives.enabled.then(|| {
        archive_jobs::ArchiveJobs::new(
            Arc::clone(&ingestor),
            config.archives.clone(),
            shutdown.clone(),
        )
    });
    let jobs = archives.clone();
    let state = Arc::new(AppState {
        catalog,
        ingestor,
        scheduler: SchedulerConfig::default(),
        http,
        dicomweb: config.dicomweb,
        htj2k: config.htj2k,
        metrics,
        gauges,
        archives,
    });
    let router = app(state);

    let token = shutdown.clone();
    let mut http = tokio::spawn(async move {
        axum::serve(
            no_delay(listener),
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(token.cancelled_owned())
        .await
    });
    let mut dimse = scp.map(|scp| tokio::spawn(scp.run(shutdown.clone())));

    let task = tokio::spawn(async move {
        let mut http_result = None;
        let mut dimse_result = None;
        // Run until shutdown is requested or either server stops on its own.
        tokio::select! {
            r = &mut http => http_result = Some(joined("HTTP", r)),
            r = async { dimse.as_mut().expect("guarded").await }, if dimse.is_some() => {
                dimse_result = Some(joined("DIMSE", r));
            }
            _ = shutdown.cancelled() => {}
        }
        if !shutdown.is_cancelled() {
            tracing::error!("a server stopped unexpectedly; shutting down");
        }
        shutdown.cancel();

        // HTTP shutdown is graceful, but long-lived streams get a bounded grace.
        let http_result = match http_result {
            Some(r) => r,
            None => match tokio::time::timeout(HTTP_SHUTDOWN_GRACE, &mut http).await {
                Ok(r) => joined("HTTP", r),
                Err(_) => {
                    tracing::warn!("HTTP grace period over; dropping open connections");
                    http.abort();
                    Ok(())
                }
            },
        };
        // The SCP enforces its own grace period for in-flight associations.
        let dimse_result = match (dimse_result, dimse) {
            (Some(r), _) => r,
            (None, Some(t)) => joined("DIMSE", t.await),
            (None, None) => Ok(()),
        };
        // Running archive jobs stop at their next entry; queued ones abort.
        if let Some(jobs) = jobs {
            if !jobs.wait(HTTP_SHUTDOWN_GRACE).await {
                tracing::warn!("archive jobs still running after the grace period");
            }
            drop(jobs);
        }
        // Let the sampler, the backfill and the copy worker release the
        // storage lock before reporting stopped. The workers stop after
        // their current batch or group; both are joined under one grace.
        if let Some(h) = sampler {
            let _ = h.await;
        }
        let workers = async {
            let (backfill, copier) = tokio::join!(backfill, async {
                match copier {
                    Some(c) => c.await.map(Some),
                    None => Ok(None),
                }
            });
            drop((backfill, copier));
        };
        if tokio::time::timeout(HTTP_SHUTDOWN_GRACE, workers)
            .await
            .is_err()
        {
            tracing::warn!(
                "metadata backfill or HTJ2K copy worker still running after the grace period"
            );
        }
        http_result.and(dimse_result)
    });

    Ok(Running {
        http_addr,
        dimse_addr,
        task,
    })
}

/// Unsent bytes the kernel queues per socket before a write waits
/// (`TCP_NOTSENT_LOWAT`), so a re-planned stream is not stuck behind a deep
/// socket buffer on a direct connection.
#[cfg(any(target_os = "linux", target_os = "android"))]
const NOTSENT_LOWAT: u32 = 131_072;

/// Set `TCP_NODELAY` on every accepted HTTP connection. Without it, a small
/// write that follows another one (the stream's first `series` message after
/// the 101 response) waits for the client's delayed ACK, about 40 ms on
/// Linux. Where `socket2` offers it (Linux, Android), also set
/// `TCP_NOTSENT_LOWAT`.
fn no_delay(
    listener: tokio::net::TcpListener,
) -> axum::serve::TapIo<tokio::net::TcpListener, fn(&mut tokio::net::TcpStream)> {
    listener.tap_io(|tcp| {
        if let Err(e) = tcp.set_nodelay(true) {
            tracing::debug!("cannot set TCP_NODELAY: {e}");
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Err(e) = socket2::SockRef::from(&*tcp).set_tcp_notsent_lowat(NOTSENT_LOWAT) {
            tracing::debug!("cannot set TCP_NOTSENT_LOWAT: {e}");
        }
    })
}

/// Instances scanned per backfill batch.
const BACKFILL_BATCH: i64 = 64;
/// How long the backfill waits after a failed batch before trying again.
const BACKFILL_RETRY: Duration = Duration::from_secs(5);

/// Scan instances stored before scanning existed (or at an older scan
/// version), batch by batch, until none are left or shutdown starts. The
/// next start continues where this one stopped. A failed batch (the index
/// cannot be read, or none of its rows could be recorded) is logged and
/// tried again after `retry`.
async fn backfill_metadata(ingestor: Arc<Ingestor>, shutdown: CancellationToken, retry: Duration) {
    let mut scanned = 0;
    while !shutdown.is_cancelled() {
        match ingestor.backfill_batch(BACKFILL_BATCH).await {
            Ok(0) => {
                tracing::info!(scanned, "metadata backfill done");
                return;
            }
            Ok(n) => {
                scanned += n;
                tracing::info!(scanned, "metadata backfill progress");
            }
            Err(e) => {
                tracing::warn!(
                    scanned,
                    "metadata backfill batch failed: {e}; retrying in {} s",
                    retry.as_secs_f32()
                );
                tokio::select! {
                    _ = tokio::time::sleep(retry) => {}
                    _ = shutdown.cancelled() => {}
                }
            }
        }
    }
    tracing::info!(scanned, "metadata backfill paused for shutdown");
}

/// Instances copied per worker batch.
const COPY_BATCH: i64 = 64;
/// How long the copy worker waits after a failed batch.
const COPY_RETRY: Duration = Duration::from_secs(5);
/// How long the copy worker waits when no instance needs a copy.
const COPY_IDLE: Duration = Duration::from_secs(10);

/// Make HTJ2K copies in batches until shutdown, `parallel` instances at a
/// time. A batch that wrote nothing waits `idle` (new instances may have
/// arrived); a failed batch waits `retry`. Shutdown ends a batch after the
/// group of instances in flight.
async fn copy_worker(
    ingestor: Arc<Ingestor>,
    shutdown: CancellationToken,
    retry: Duration,
    idle: Duration,
    parallel: usize,
) {
    let mut copied = 0;
    while !shutdown.is_cancelled() {
        let wait = match ingestor
            .copy_batch(COPY_BATCH, parallel, || shutdown.is_cancelled())
            .await
        {
            Ok(0) => idle,
            Ok(n) => {
                copied += n;
                tracing::info!(copied, "HTJ2K copy progress");
                continue;
            }
            Err(e) => {
                tracing::warn!(
                    copied,
                    "HTJ2K copy batch failed: {e}; retrying in {} s",
                    retry.as_secs_f32()
                );
                retry
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = shutdown.cancelled() => {}
        }
    }
    tracing::info!(copied, "HTJ2K copy worker stopped for shutdown");
}

/// Flatten a server task's join result.
fn joined<E>(name: &str, r: Result<Result<(), E>, tokio::task::JoinError>) -> anyhow::Result<()>
where
    E: std::error::Error + Send + Sync + 'static,
{
    match r {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(anyhow::Error::new(e).context(format!("{name} server failed"))),
        Err(e) => Err(anyhow::Error::new(e).context(format!("{name} server task panicked"))),
    }
}

/// Sub-directories and regular files of `dir`. Symlinks to files are
/// followed; symlinks to directories are not, so recursion cannot loop.
/// An unreadable directory or entry is logged and passed to `on_error`
/// (path, reason); the rest of the directory is still listed.
pub(crate) async fn list_dir(
    dir: &Path,
    on_error: &mut dyn FnMut(&Path, String),
) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let (mut dirs, mut files) = (Vec::new(), Vec::new());
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(e) => {
            tracing::warn!(dir = %dir.display(), "cannot read directory: {e}");
            on_error(dir, format!("cannot read directory: {e}"));
            return (dirs, files);
        }
    };
    loop {
        let e = match rd.next_entry().await {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(e) => {
                // A failed readdir step does not advance; stop rather than spin.
                tracing::warn!(dir = %dir.display(), "cannot list directory: {e}");
                on_error(dir, format!("cannot list directory: {e}"));
                break;
            }
        };
        let path = e.path();
        let ft = match e.file_type().await {
            Ok(ft) => ft,
            Err(err) => {
                tracing::warn!(path = %path.display(), "cannot stat entry: {err}");
                on_error(&path, format!("cannot stat entry: {err}"));
                continue;
            }
        };
        if ft.is_dir() {
            dirs.push(path);
        } else if ft.is_file()
            || (ft.is_symlink() && tokio::fs::metadata(&path).await.is_ok_and(|m| m.is_file()))
        {
            files.push(path);
        }
    }
    (dirs, files)
}

#[cfg(test)]
mod tests {
    use axum::serve::Listener;

    use super::*;

    #[tokio::test]
    async fn accepted_connections_have_tcp_nodelay() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut listener = no_delay(listener);
        let client = tokio::spawn(tokio::net::TcpStream::connect(addr));
        let (io, _) = listener.accept().await;
        assert!(
            io.nodelay().unwrap(),
            "TCP_NODELAY is off on an accepted connection"
        );
        #[cfg(any(target_os = "linux", target_os = "android"))]
        assert_eq!(
            socket2::SockRef::from(&io).tcp_notsent_lowat().unwrap(),
            NOTSENT_LOWAT
        );
        client.await.unwrap().unwrap();
    }

    /// Shutdown mid-batch stops the worker promptly and releases the store.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_stops_mid_batch_and_releases_store() {
        let dir = tempfile::tempdir().unwrap();
        let ingestor = unscanned_store(dir.path(), 8).await;
        ingestor.backfill_batch(10).await.unwrap();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(copy_worker(
            Arc::clone(&ingestor),
            shutdown.clone(),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            1,
        ));
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the worker stops promptly")
            .unwrap();
        drop(ingestor);
        Ingestor::open(slicestream_store::StorageConfig {
            root: dir.path().join("store"),
            ..Default::default()
        })
        .await
        .expect("the storage lock is released");
    }

    /// The worker copies every eligible phantom instance.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_makes_copies_in_background() {
        let dir = tempfile::tempdir().unwrap();
        let ingestor = unscanned_store(dir.path(), 2).await;
        // Only scanned instances are candidates.
        ingestor.backfill_batch(10).await.unwrap();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(copy_worker(
            Arc::clone(&ingestor),
            shutdown.clone(),
            Duration::from_millis(50),
            Duration::from_millis(50),
            2,
        ));
        let uid = ingestor.index().list_series().await.unwrap()[0]
            .series_instance_uid
            .clone();
        let rows = ingestor.index().series_slices(&uid).await.unwrap();
        assert_eq!(rows.len(), 2);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        for r in &rows {
            while ingestor.copy_frames(r.id, &r.blob).await.unwrap().is_none() {
                assert!(tokio::time::Instant::now() < deadline, "no copy in 10 s");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("shutdown stops the worker")
            .unwrap();
    }

    async fn unscanned_store(dir: &Path, n: usize) -> Arc<Ingestor> {
        let src = dir.join("src");
        phantom::write_phantom(&src, n, 16).unwrap();
        let root = dir.join("store");
        let ingestor = Ingestor::open(slicestream_store::StorageConfig {
            root: root.clone(),
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
        let pool = index_pool(&root).await;
        for q in [
            "DELETE FROM instance_data",
            "UPDATE instances SET scan_version = 0",
        ] {
            sqlx::query(q).execute(&pool).await.unwrap();
        }
        ingestor
    }

    async fn index_pool(root: &Path) -> sqlx::SqlitePool {
        sqlx::SqlitePool::connect(&format!("sqlite://{}", root.join("index.sqlite").display()))
            .await
            .unwrap()
    }

    /// A failing batch is retried after the delay until it succeeds; the
    /// backfill does not give up until the next start.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backfill_retries_after_failed_batch() {
        let dir = tempfile::tempdir().unwrap();
        let ingestor = unscanned_store(dir.path(), 2).await;
        let pool = index_pool(&dir.path().join("store")).await;
        // Every write to `instances` fails, as with a locked database.
        sqlx::query(
            "CREATE TRIGGER no_writes BEFORE UPDATE ON instances
             BEGIN SELECT RAISE(ABORT, 'injected'); END",
        )
        .execute(&pool)
        .await
        .unwrap();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(backfill_metadata(
            Arc::clone(&ingestor),
            shutdown.clone(),
            Duration::from_millis(50),
        ));
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!task.is_finished(), "the backfill gave up");
        assert_eq!(ingestor.index().unscanned(10).await.unwrap().len(), 2);

        sqlx::query("DROP TRIGGER no_writes")
            .execute(&pool)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the backfill finishes once writes work")
            .unwrap();
        assert!(ingestor.index().unscanned(10).await.unwrap().is_empty());
    }

    /// Shutdown stops a backfill that is waiting to retry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backfill_retry_stops_on_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let ingestor = unscanned_store(dir.path(), 1).await;
        let pool = index_pool(&dir.path().join("store")).await;
        sqlx::query(
            "CREATE TRIGGER no_writes BEFORE UPDATE ON instances
             BEGIN SELECT RAISE(ABORT, 'injected'); END",
        )
        .execute(&pool)
        .await
        .unwrap();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(backfill_metadata(
            Arc::clone(&ingestor),
            shutdown.clone(),
            Duration::from_secs(3600),
        ));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!task.is_finished(), "the backfill gave up");
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("shutdown ends the wait")
            .unwrap();
    }
}
