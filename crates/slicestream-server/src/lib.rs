//! Reference HTTP + WebSocket server for slicestream: series streaming plus
//! STOW-RS / single-instance ingest.
#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderName, HeaderValue, Method};
use axum::routing::{get, post};
use slicestream_core::SchedulerConfig;
use slicestream_dimse::StoreScp;
use slicestream_store::Ingestor;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::services::ServeDir;

pub mod catalog;
pub mod config;
pub mod hosts;
// wired in by the console endpoints task
#[allow(dead_code)]
pub(crate) mod metrics;
pub mod routes;
pub mod stow;
pub mod stream;

pub use catalog::SeriesCatalog;
use config::{Config, HttpConfig};

pub struct AppState {
    pub catalog: Arc<SeriesCatalog>,
    pub ingestor: Arc<Ingestor>,
    pub scheduler: SchedulerConfig,
    pub http: HttpConfig,
}

/// Build the application router.
pub fn app(state: Arc<AppState>) -> Router {
    // Ingest bodies are streamed to disk; size is enforced by multer
    // (`max_request_bytes`) and the spool (`max_instance_bytes`).
    let ingest = Router::new()
        .route("/dicom-web/studies", post(stow::stow_all))
        .route("/dicom-web/studies/{study}", post(stow::stow_study))
        .route("/api/instances", post(stow::post_instance))
        .layer(DefaultBodyLimit::disable());

    // CORS only for the read API and only for configured origins. Ingest
    // routes and static files never get CORS, so cross-origin pages cannot
    // upload or read through them. (The demo viewer is same-origin.)
    let mut read = Router::new()
        .route("/api/series", get(routes::list_series))
        .route("/api/series/{uid}", get(routes::get_series))
        .route("/api/series/{uid}/frames/{index}", get(routes::get_frame))
        .route("/api/series/{uid}/stream", get(stream::stream_series));
    if let Some(cors) = cors_layer(&state.http.cors_allowed_origins) {
        read = read.layer(cors);
    }

    // Every route, including static files, checks Host against DNS rebinding.
    let allowed = Arc::new(hosts::HostAllowList::from_config(&state.http));
    Router::new()
        .merge(read)
        .merge(ingest)
        .fallback_service(ServeDir::new(&state.http.web_dir))
        .layer(axum::middleware::from_fn(move |req, next| {
            hosts::host_guard(Arc::clone(&allowed), req, next)
        }))
        .with_state(state)
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
            .expose_headers([HeaderName::from_static("x-slicestream-frame-info")]),
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
    let root = config.storage.root.clone();
    let ingestor = Ingestor::open(config.storage)
        .await
        .with_context(|| format!("opening storage at {}", root.display()))?;

    let listener = tokio::net::TcpListener::bind(config.http.addr)
        .await
        .with_context(|| format!("binding HTTP on {}", config.http.addr))?;
    let http_addr = listener.local_addr()?;

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

    let report = ingestor.recover().await.context("storage recovery")?;
    tracing::info!(
        root = %root.display(),
        tmp_removed = report.tmp_removed,
        orphans_removed = report.orphans_removed,
        "storage ready"
    );

    let catalog = SeriesCatalog::new(Arc::clone(&ingestor));
    catalog.spawn_invalidator();

    // With `addr` port 0, derive the default allowed hosts from the bound port.
    let mut http = config.http;
    http.addr = http_addr;
    let state = Arc::new(AppState {
        catalog,
        ingestor,
        scheduler: SchedulerConfig::default(),
        http,
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
        http_result.and(dimse_result)
    });

    Ok(Running {
        http_addr,
        dimse_addr,
        task,
    })
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

/// Counts from [`import_dir`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ImportReport {
    /// Newly stored (including overwrites under `on_duplicate = "overwrite"`).
    pub stored: usize,
    /// Already present.
    pub duplicate: usize,
    /// Looked like DICOM but could not be ingested.
    pub failed: usize,
    /// Not Part-10 DICOM (no `DICM` at byte 128), left alone.
    pub skipped: usize,
}

/// Ingest every Part-10 file in `dir` (and its sub-directories when
/// `recursive`) through the normal pipeline. Files are processed in path
/// order; per-file errors are logged and counted, never fatal.
pub async fn import_dir(ingestor: &Ingestor, dir: &Path, recursive: bool) -> ImportReport {
    let mut report = ImportReport::default();
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(d) = dirs.pop() {
        let (subdirs, mut files) = list_dir(&d, &mut report).await;
        if recursive {
            dirs.extend(subdirs);
        }
        files.sort();
        for path in files {
            match is_part10(&path).await {
                Ok(true) => {}
                Ok(false) => {
                    tracing::debug!(path = %path.display(), "skipping non-DICOM file");
                    report.skipped += 1;
                    continue;
                }
                Err(e) => {
                    tracing::warn!(path = %path.display(), "cannot read file: {e}");
                    report.failed += 1;
                    continue;
                }
            }
            match ingestor.import_file(&path).await {
                Ok(s) if s.duplicate => report.duplicate += 1,
                Ok(_) => report.stored += 1,
                Err(e) => {
                    tracing::warn!(path = %path.display(), "import failed: {e}");
                    report.failed += 1;
                }
            }
        }
    }
    report
}

/// Sub-directories and regular files of `dir`. Symlinks to files are
/// followed; symlinks to directories are not, so recursion cannot loop.
/// An unreadable directory or entry is logged and counted as failed; the
/// rest of the directory is still listed.
async fn list_dir(dir: &Path, report: &mut ImportReport) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let (mut dirs, mut files) = (Vec::new(), Vec::new());
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(e) => {
            tracing::warn!(dir = %dir.display(), "cannot read directory: {e}");
            report.failed += 1;
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
                report.failed += 1;
                break;
            }
        };
        let path = e.path();
        let ft = match e.file_type().await {
            Ok(ft) => ft,
            Err(err) => {
                tracing::warn!(path = %path.display(), "cannot stat entry: {err}");
                report.failed += 1;
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

/// Whether the file has a Part-10 preamble: at least 132 bytes with `DICM`
/// at offset 128.
async fn is_part10(path: &Path) -> std::io::Result<bool> {
    let mut f = tokio::fs::File::open(path).await?;
    let mut head = [0u8; 132];
    let mut n = 0;
    while n < head.len() {
        let read = f.read(&mut head[n..]).await?;
        if read == 0 {
            return Ok(false);
        }
        n += read;
    }
    Ok(&head[128..132] == b"DICM")
}
