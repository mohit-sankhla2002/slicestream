//! WebSocket streaming: push frames in the order the scheduler picks, and
//! re-plan instantly whenever the client reports a new cursor position.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use slicestream_core::protocol::{self, ClientMessage};
use slicestream_core::{PrefetchScheduler, Series};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

use crate::AppState;

pub async fn stream_series(
    ws: WebSocketUpgrade,
    Path(uid): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let series = match state.catalog.get(&uid).await {
        Ok(Some(s)) => s,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return crate::routes::internal_error(&e),
    };
    let scheduler = PrefetchScheduler::new(series.len(), state.scheduler);
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = run(socket, series, scheduler).await {
            tracing::debug!("stream closed: {e}");
        }
    })
}

async fn run(
    socket: WebSocket,
    series: Arc<Series>,
    mut sched: PrefetchScheduler,
) -> anyhow::Result<()> {
    let (mut tx, mut rx) = socket.split();

    let hello = serde_json::json!({ "type": "series", "series": series.summary() });
    tx.send(Message::Text(hello.to_string().into())).await?;

    // Reader task: forward control messages so the writer never blocks on input.
    let (ctl_tx, mut ctl_rx) = mpsc::unbounded_channel::<ClientMessage>();
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

fn apply(sched: &mut PrefetchScheduler, msg: ClientMessage) {
    match msg {
        ClientMessage::Cursor { index, velocity } => sched.update(index, velocity),
        ClientMessage::Evicted { indices } => {
            indices.into_iter().for_each(|i| sched.mark_unsent(i))
        }
    }
}
