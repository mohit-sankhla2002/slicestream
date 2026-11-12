//! Reference HTTP + WebSocket server for slicestream: series streaming plus
//! STOW-RS / single-instance ingest.
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use slicestream_core::{SchedulerConfig, Series};
use slicestream_store::Ingestor;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

pub mod config;
pub mod routes;
pub mod stow;
pub mod stream;

use config::HttpConfig;

pub struct AppState {
    pub series: HashMap<String, Arc<Series>>,
    pub scheduler: SchedulerConfig,
    pub ingestor: Arc<Ingestor>,
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

    Router::new()
        .route("/api/series", get(routes::list_series))
        .route("/api/series/{uid}", get(routes::get_series))
        .route("/api/series/{uid}/frames/{index}", get(routes::get_frame))
        .route("/api/series/{uid}/stream", get(stream::stream_series))
        .merge(ingest)
        .fallback_service(ServeDir::new(&state.http.web_dir))
        .layer(CorsLayer::permissive()) // dev default; lock down in production
        .with_state(state)
}
