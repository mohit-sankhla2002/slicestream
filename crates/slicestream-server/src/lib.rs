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
pub mod routes;
pub mod stow;
pub mod stream;

pub use catalog::SeriesCatalog;
use config::{Config, DicomwebConfig, HttpConfig};

pub struct AppState {
    pub catalog: Arc<SeriesCatalog>,
    pub ingestor: Arc<Ingestor>,
    pub scheduler: SchedulerConfig,
    pub http: HttpConfig,
    pub dicomweb: DicomwebConfig,
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
            .expose_headers([
                HeaderName::from_static("x-slicestream-frame-info"),
                header::WARNING,
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
    let backfill = tokio::spawn(backfill_metadata(Arc::clone(&ingestor), shutdown.clone()));
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
        metrics,
        gauges,
        archives,
    });
    let router = app(state);

    let token = shutdown.clone();
    let mut http = tokio::spawn(async move {
        axum::serve(
            listener,
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
        // Let the sampler and the backfill release the storage lock before
        // reporting stopped. The backfill stops after its current batch.
        if let Some(h) = sampler {
            let _ = h.await;
        }
        if tokio::time::timeout(HTTP_SHUTDOWN_GRACE, backfill)
            .await
            .is_err()
        {
            tracing::warn!("metadata backfill still running after the grace period");
        }
        http_result.and(dimse_result)
    });

    Ok(Running {
        http_addr,
        dimse_addr,
        task,
    })
}

/// Instances scanned per backfill batch.
const BACKFILL_BATCH: i64 = 64;

/// Scan instances stored before scanning existed (or at an older scan
/// version), batch by batch, until none are left or shutdown starts. The
/// next start continues where this one stopped.
async fn backfill_metadata(ingestor: Arc<Ingestor>, shutdown: CancellationToken) {
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
                tracing::error!(
                    scanned,
                    "metadata backfill stopped: {e}; it resumes at the next start"
                );
                return;
            }
        }
    }
    tracing::info!(scanned, "metadata backfill paused for shutdown");
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
