//! WebSocket streaming: push frames in the order the scheduler picks, and
//! re-plan instantly whenever the client reports a new cursor position.
//! With `?progressive=htj2k`, frames go out in parts paced by the client's
//! acks (see [`crate::progressive`]).

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::stream::SplitStream;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use slicestream_core::protocol::{self, ClientMessage};
use slicestream_core::{PrefetchScheduler, Series};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

use crate::AppState;
use crate::config::Htj2kMode;

#[derive(Debug, Deserialize)]
pub struct StreamParams {
    /// `htj2k` opts in to the progressive stream.
    progressive: Option<String>,
}

pub async fn stream_series(
    ws: WebSocketUpgrade,
    Path(uid): Path<String>,
    Query(params): Query<StreamParams>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if !crate::hosts::websocket_origin_allowed(&state.http, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let progressive = match params.progressive.as_deref() {
        None => false,
        Some("htj2k") if state.htj2k.mode == Htj2kMode::Off => {
            return (
                StatusCode::BAD_REQUEST,
                "progressive=htj2k is not available: [htj2k] mode = \"off\"",
            )
                .into_response();
        }
        Some("htj2k") => true,
        Some(_) => {
            return (
                StatusCode::BAD_REQUEST,
                "unknown progressive value; the only one is \"htj2k\"",
            )
                .into_response();
        }
    };
    let series = match state.catalog.get(&uid).await {
        Ok(Some(s)) => s,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return crate::routes::internal_error(&e),
    };
    let gauges = Arc::clone(&state.gauges);
    ws.on_upgrade(move |socket| async move {
        let _guard = crate::metrics::GaugeGuard::enter(&gauges, crate::metrics::Gauge::WsStreams);
        let result = if progressive {
            crate::progressive::run_progressive(socket, series, state).await
        } else {
            let scheduler = PrefetchScheduler::new(series.len(), state.scheduler);
            run(socket, series, scheduler).await
        };
        if let Err(e) = result {
            tracing::debug!("stream closed: {e}");
        }
    })
}

async fn run(
    socket: WebSocket,
    series: Arc<Series>,
    mut sched: PrefetchScheduler,
) -> anyhow::Result<()> {
    let (mut tx, rx) = socket.split();

    let hello = serde_json::json!({ "type": "series", "series": series.summary() });
    tx.send(Message::Text(hello.to_string().into())).await?;

    let (mut ctl_rx, reader) = spawn_reader(rx);

    // Give the client a moment to report where it is looking, so the first
    // frame on the wire is the one it will actually display.
    if let Ok(Some(m)) =
        tokio::time::timeout(std::time::Duration::from_millis(100), ctl_rx.recv()).await
    {
        apply(&mut sched, m);
    }

    let result = async {
        loop {
            // Apply every pending control message; the latest cursor wins.
            loop {
                match ctl_rx.try_recv() {
                    Ok(m) => apply(&mut sched, m),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return Ok(()),
                }
            }

            let Some(index) = sched.pop_next() else {
                // Everything delivered; sleep until the client says something.
                match ctl_rx.recv().await {
                    Some(m) => {
                        apply(&mut sched, m);
                        continue;
                    }
                    None => return Ok(()),
                }
            };

            let s = series.clone();
            let frame = match tokio::task::spawn_blocking(move || s.load_frame(index)).await? {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(index, "failed to load frame: {e}");
                    continue;
                }
            };
            tx.send(Message::Binary(protocol::encode_frame(&frame)))
                .await?;
        }
    }
    .await;

    reader.abort();
    result
}

/// Reader task: forward control messages so the writer never blocks on
/// input. The channel closes when the client closes the socket.
pub(crate) fn spawn_reader(
    mut rx: SplitStream<WebSocket>,
) -> (
    mpsc::UnboundedReceiver<ClientMessage>,
    tokio::task::JoinHandle<()>,
) {
    let (ctl_tx, ctl_rx) = mpsc::unbounded_channel::<ClientMessage>();
    let reader = tokio::spawn(async move {
        while let Some(Ok(msg)) = rx.next().await {
            match msg {
                Message::Text(text) => match serde_json::from_str(&text) {
                    Ok(m) => {
                        if ctl_tx.send(m).is_err() {
                            break;
                        }
                    }
                    Err(e) => tracing::debug!("ignoring bad client message: {e}"),
                },
                Message::Close(_) => break,
                _ => {}
            }
        }
    });
    (ctl_rx, reader)
}

fn apply(sched: &mut PrefetchScheduler, msg: ClientMessage) {
    match msg {
        ClientMessage::Cursor { index, velocity } => sched.update(index, velocity),
        ClientMessage::Evicted { indices } => {
            indices.into_iter().for_each(|i| sched.mark_unsent(i))
        }
        // Flow control is for the progressive stream only.
        ClientMessage::Ack { .. } => {}
    }
}
