//! Plain HTTP endpoints: series listing and single-frame fetch.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::AppState;

pub async fn list_series(State(state): State<Arc<AppState>>) -> Response {
    match state.catalog.list().await {
        Ok(list) => Json(list).into_response(),
        Err(e) => internal_error(&e),
    }
}

pub async fn get_series(State(state): State<Arc<AppState>>, Path(uid): Path<String>) -> Response {
    match state.catalog.get(&uid).await {
        Ok(Some(s)) => Json(s.summary()).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => internal_error(&e),
    }
}

/// Log the cause and return a bare 500 (index errors are not for clients).
pub(crate) fn internal_error(e: &anyhow::Error) -> Response {
    tracing::error!("catalog error: {e:#}");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

/// Fetch one frame over plain HTTP (fallback for clients without WebSocket,
/// and handy for curl). Frame metadata goes in the `x-slicestream-frame-info` header.
pub async fn get_frame(
    State(state): State<Arc<AppState>>,
    Path((uid, index)): Path<(String, usize)>,
) -> Response {
    let series = match state.catalog.get(&uid).await {
        Ok(Some(s)) => s,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return internal_error(&e),
    };
    let frame = match tokio::task::spawn_blocking(move || series.load_frame(index)).await {
        Ok(Ok(f)) => f,
        Ok(Err(slicestream_core::Error::SliceOutOfRange { .. })) => {
            return StatusCode::NOT_FOUND.into_response();
        }
        Ok(Err(e)) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let info = serde_json::to_string(&frame.info).unwrap_or_default();
    let mut res = frame.data.into_response();
    let headers = res.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    if let Ok(v) = HeaderValue::from_str(&info) {
        headers.insert("x-slicestream-frame-info", v);
    }
    res
}
