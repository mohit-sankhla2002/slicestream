//! WADO-RS metadata (PS3.18 10.4): the stored DICOM JSON of every instance
//! of a study, series or instance, streamed as one JSON array.

use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::{Stream, StreamExt, stream};
use slicestream_dicomweb::{instance_url, negotiate_json, splice_bulk_uris};
use slicestream_store::{InstanceRef, MetadataState, Scope};

use super::{DICOM_JSON, accept_ranges, find, plain, request_base_url};
use crate::AppState;
use crate::routes::internal_error;

/// Instances whose metadata is read at once.
const CHUNK: usize = 128;

pub async fn study(
    State(state): State<Arc<AppState>>,
    Path(study): Path<String>,
    headers: HeaderMap,
) -> Response {
    respond(&state, Scope::Study(study), &headers).await
}

pub async fn series(
    State(state): State<Arc<AppState>>,
    Path((study, series)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    respond(&state, Scope::Series(study, series), &headers).await
}

pub async fn instance(
    State(state): State<Arc<AppState>>,
    Path((study, series, sop)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    respond(&state, Scope::Instance(study, series, sop), &headers).await
}

async fn respond(state: &Arc<AppState>, scope: Scope, headers: &HeaderMap) -> Response {
    let instances = match find(state, &scope).await {
        Ok(i) => i,
        Err(r) => return *r,
    };
    if let Err(e) = negotiate_json(&accept_ranges(headers)) {
        return plain(StatusCode::NOT_ACCEPTABLE, e.0);
    }
    // Scan what is not scanned yet and make sure every instance has
    // metadata, so a failure is a clean 500 rather than a broken body.
    for inst in instances.iter().filter(|i| !i.scanned) {
        if let Err(e) = state.ingestor.ensure_scanned(inst.id).await {
            tracing::warn!(instance = inst.id, "scan on demand failed: {e}");
        }
    }
    let index = state.ingestor.index();
    for chunk in instances.chunks(CHUNK) {
        let ids: Vec<i64> = chunk.iter().map(|i| i.id).collect();
        let states = match index.metadata_batch(&ids).await {
            Ok(s) => s,
            Err(e) => return internal_error(&anyhow::Error::new(e)),
        };
        for (id, s) in states {
            match s {
                MetadataState::Ready(_) => {}
                MetadataState::Unscanned => {
                    return plain(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("instance {id} could not be scanned for metadata"),
                    );
                }
                MetadataState::Unavailable(reason) => {
                    return plain(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("no metadata for an instance: {reason}"),
                    );
                }
            }
        }
    }

    let base = request_base_url(&state.http, headers);
    let body = Body::from_stream(json_stream(Arc::clone(state), instances, base));
    (
        [(header::CONTENT_TYPE, HeaderValue::from_static(DICOM_JSON))],
        body,
    )
        .into_response()
}

struct Ctx {
    state: Arc<AppState>,
    instances: Vec<InstanceRef>,
    base: String,
}

enum Phase {
    Open,
    Chunk(usize),
    Close,
    Done,
}

/// `[`, the objects of each chunk joined by `,`, `]`.
fn json_stream(
    state: Arc<AppState>,
    instances: Vec<InstanceRef>,
    base: String,
) -> impl Stream<Item = std::io::Result<Bytes>> {
    let ctx = Arc::new(Ctx {
        state,
        instances,
        base,
    });
    // Fused: the compression layer polls once more after the end.
    stream::unfold((Phase::Open, false), move |(phase, mut wrote)| {
        let ctx = Arc::clone(&ctx);
        async move {
            let Ctx {
                state,
                instances,
                base,
            } = &*ctx;
            match phase {
                Phase::Open => Some((Ok(Bytes::from_static(b"[")), (Phase::Chunk(0), wrote))),
                Phase::Close => Some((Ok(Bytes::from_static(b"]")), (Phase::Done, wrote))),
                Phase::Done => None,
                Phase::Chunk(start) => {
                    let Some(chunk) = instances.get(start..) else {
                        return Some((Ok(Bytes::new()), (Phase::Close, wrote)));
                    };
                    let chunk = &chunk[..chunk.len().min(CHUNK)];
                    if chunk.is_empty() {
                        return Some((Ok(Bytes::new()), (Phase::Close, wrote)));
                    }
                    let ids: Vec<i64> = chunk.iter().map(|i| i.id).collect();
                    let states = match state.ingestor.index().metadata_batch(&ids).await {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!("metadata read failed mid-stream: {e}");
                            return Some((Err(std::io::Error::other(e)), (Phase::Done, wrote)));
                        }
                    };
                    let mut out = Vec::new();
                    for (id, s) in states {
                        let MetadataState::Ready(json) = s else {
                            tracing::error!(instance = id, "metadata vanished mid-stream");
                            return Some((
                                Err(std::io::Error::other("metadata unavailable")),
                                (Phase::Done, wrote),
                            ));
                        };
                        let Some(inst) = chunk.iter().find(|i| i.id == id) else {
                            continue;
                        };
                        if wrote {
                            out.push(b',');
                        }
                        wrote = true;
                        let prefix = format!(
                            "{}/",
                            instance_url(base, &inst.study_uid, &inst.series_uid, &inst.sop_uid)
                        );
                        splice_bulk_uris(&json, &prefix, &mut out);
                    }
                    Some((Ok(Bytes::from(out)), (Phase::Chunk(start + CHUNK), wrote)))
                }
            }
        }
    })
    .fuse()
}
