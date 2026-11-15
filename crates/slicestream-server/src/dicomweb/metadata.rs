//! WADO-RS metadata (PS3.18 10.4): the stored DICOM JSON of every instance
//! of a study, series or instance, streamed as one JSON array, an instance
//! at a time.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::{Stream, StreamExt, stream};
use slicestream_dicomweb::{instance_url, negotiate_json, splice_bulk_uris};
use slicestream_store::{InstanceRef, MetadataState, MetadataStateKind, Scope};

use super::fetch::MetadataReader;
use super::{DICOM_JSON, accept_ranges, find, plain, request_base_url};
use crate::AppState;
use crate::routes::internal_error;

/// Instances whose metadata state is checked at once.
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
    // metadata, so a failure is a clean 500 rather than a broken body. Only
    // the state and length of each instance's metadata is read here.
    for inst in instances.iter().filter(|i| !i.scanned) {
        if let Err(e) = state.ingestor.ensure_scanned(inst.id).await {
            tracing::warn!(instance = inst.id, "scan on demand failed: {e}");
        }
    }
    let index = state.ingestor.index();
    let mut items = Vec::with_capacity(instances.len());
    for chunk in instances.chunks(CHUNK) {
        let ids: Vec<i64> = chunk.iter().map(|i| i.id).collect();
        let mut states: HashMap<i64, MetadataStateKind> = match index.metadata_states(&ids).await {
            Ok(s) => s.into_iter().collect(),
            Err(e) => return internal_error(&anyhow::Error::new(e)),
        };
        for &id in &ids {
            let len = match states.remove(&id) {
                Some(MetadataStateKind::Ready(len)) => len,
                Some(MetadataStateKind::Unscanned) => {
                    return plain(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("instance {id} could not be scanned for metadata"),
                    );
                }
                Some(MetadataStateKind::Unavailable(reason)) => {
                    return plain(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("no metadata for an instance: {reason}"),
                    );
                }
                // Deleted since it was found; the body stops there.
                None => 0,
            };
            items.push((id, len));
        }
    }

    let base = request_base_url(&state.http, headers);
    let body = Body::from_stream(json_stream(Arc::clone(state), instances, items, base));
    (
        [(header::CONTENT_TYPE, HeaderValue::from_static(DICOM_JSON))],
        body,
    )
        .into_response()
}

/// A metadata response body being written.
struct MetaStream {
    state: Arc<AppState>,
    instances: Vec<InstanceRef>,
    base: String,
    reader: MetadataReader,
    /// Instances sent so far.
    sent: usize,
    opened: bool,
    done: bool,
}

impl MetaStream {
    /// The next piece of the body: `[`, one instance, or `]`.
    async fn step(&mut self) -> Option<std::io::Result<Bytes>> {
        if self.done {
            return None;
        }
        if !self.opened {
            self.opened = true;
            return Some(Ok(Bytes::from_static(b"[")));
        }
        let next = self.reader.next(self.state.ingestor.index()).await;
        let (id, json) = match next {
            Ok(Some((id, Some(MetadataState::Ready(json))))) => (id, json),
            Ok(None) => {
                self.done = true;
                return Some(Ok(Bytes::from_static(b"]")));
            }
            // An instance deleted, or failing, after the pre-check must not
            // shorten the array silently.
            Ok(Some((id, _))) => {
                tracing::error!(instance = id, "metadata vanished mid-stream");
                return self.fail(std::io::Error::other("metadata unavailable"));
            }
            Err(e) => {
                tracing::error!("metadata read failed mid-stream: {e}");
                return self.fail(std::io::Error::other(e));
            }
        };
        let Some(inst) = self.instances.get(self.sent).filter(|i| i.id == id) else {
            tracing::error!(instance = id, "metadata read out of order");
            return self.fail(std::io::Error::other("metadata out of order"));
        };
        let prefix = format!(
            "{}/",
            instance_url(&self.base, &inst.study_uid, &inst.series_uid, &inst.sop_uid)
        );
        let mut out = Vec::with_capacity(json.len() + 1);
        if self.sent > 0 {
            out.push(b',');
        }
        splice_bulk_uris(&json, &prefix, &mut out);
        self.sent += 1;
        Some(Ok(Bytes::from(out)))
    }

    fn fail(&mut self, e: std::io::Error) -> Option<std::io::Result<Bytes>> {
        self.done = true;
        Some(Err(e))
    }
}

/// `[`, the object of each instance (separated by `,`), `]`. The stored
/// metadata is read under the [`MetadataReader`] budget and spliced and
/// sent an instance at a time.
fn json_stream(
    state: Arc<AppState>,
    instances: Vec<InstanceRef>,
    items: Vec<(i64, u64)>,
    base: String,
) -> impl Stream<Item = std::io::Result<Bytes>> {
    let s = MetaStream {
        state,
        instances,
        base,
        reader: MetadataReader::new(items),
        sent: 0,
        opened: false,
        done: false,
    };
    // Fused: the compression layer polls once more after the end.
    stream::unfold(s, |mut s| async move {
        let item = s.step().await?;
        Some((item, s))
    })
    .fuse()
}
