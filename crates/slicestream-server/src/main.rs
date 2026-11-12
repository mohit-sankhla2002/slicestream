//! Reference server for slicestream.
//!
//! ```sh
//! slicestream-server <dicom-root> [--addr 127.0.0.1:8080] [--web web]
//! ```
//!
//! `<dicom-root>` and each of its immediate sub-directories are scanned for
//! DICOM series. Open http://127.0.0.1:8080 for the demo viewer.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, bail};
use axum::Router;
use axum::routing::get;
use slicestream_core::{SchedulerConfig, Series};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

mod routes;
mod stream;

pub struct AppState {
    pub series: HashMap<String, Arc<Series>>,
    pub scheduler: SchedulerConfig,
}

struct Args {
    root: PathBuf,
    addr: SocketAddr,
    web: PathBuf,
}

fn parse_args() -> anyhow::Result<Args> {
    let mut root = None;
    let mut addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let mut web = PathBuf::from("web");
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--addr" => addr = it.next().context("--addr needs a value")?.parse()?,
            "--web" => web = it.next().context("--web needs a value")?.into(),
            "-h" | "--help" => {
                println!("usage: slicestream-server <dicom-root> [--addr HOST:PORT] [--web DIR]");
                std::process::exit(0);
            }
            other if root.is_none() => root = Some(PathBuf::from(other)),
            other => bail!("unexpected argument: {other}"),
        }
    }
    Ok(Args {
        root: root.context("missing <dicom-root>; try --help")?,
        addr,
        web,
    })
}

fn load_series(root: &PathBuf) -> anyhow::Result<HashMap<String, Arc<Series>>> {
    let mut dirs = vec![root.clone()];
    for entry in std::fs::read_dir(root).with_context(|| format!("reading {}", root.display()))? {
        let path = entry?.path();
        if path.is_dir() {
            dirs.push(path);
        }
    }
    let mut map = HashMap::new();
    for dir in dirs {
        if let Ok(found) = Series::scan_dir(&dir) {
            for s in found {
                tracing::info!(uid = %s.uid, slices = s.len(), dir = %dir.display(), "indexed series");
                map.insert(s.uid.clone(), Arc::new(s));
            }
        }
    }
    if map.is_empty() {
        bail!("no DICOM series found under {}", root.display());
    }
    Ok(map)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = parse_args()?;

    let root = args.root.clone();
    let series = tokio::task::spawn_blocking(move || load_series(&root)).await??;
    let state = Arc::new(AppState {
        series,
        scheduler: SchedulerConfig::default(),
    });

    let app = Router::new()
        .route("/api/series", get(routes::list_series))
        .route("/api/series/{uid}", get(routes::get_series))
        .route("/api/series/{uid}/frames/{index}", get(routes::get_frame))
        .route("/api/series/{uid}/stream", get(stream::stream_series))
        .fallback_service(ServeDir::new(&args.web))
        .layer(CorsLayer::permissive()) // dev default; lock down in production
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(args.addr).await?;
    tracing::info!("listening on http://{}", args.addr);
    axum::serve(listener, app).await?;
    Ok(())
}
