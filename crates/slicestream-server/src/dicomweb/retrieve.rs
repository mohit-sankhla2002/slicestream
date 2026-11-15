//! WADO-RS instance retrieve (PS3.18 10.4): the stored Part 10 files of a
//! study, series or instance, sent as stored (no transcoding). Files are
//! streamed in 1 MiB reads, so memory does not grow with the response.

use std::path::PathBuf;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use futures_util::{Stream, StreamExt, stream};
use slicestream_dicomweb::{Multipart, instance_part_type, negotiate_instances};
use slicestream_store::{BlobKind, Scope};
use tokio::fs::File;
use tokio_util::io::ReaderStream;

use super::{accept_ranges, find, plain};
use crate::AppState;

const READ_CHUNK: usize = 1 << 20;

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
    let stored: Vec<&str> = instances
        .iter()
        .map(|i| i.transfer_syntax.as_str())
        .collect();
    let plan = match negotiate_instances(&accept_ranges(headers), &stored, instances.len()) {
        Ok(p) => p,
        Err(e) => return plain(StatusCode::NOT_ACCEPTABLE, e.0),
    };
    let items: Vec<(PathBuf, String)> = instances
        .iter()
        .map(|i| {
            (
                state.ingestor.blobs().path_of(&i.blob, BlobKind::Dicom),
                instance_part_type(&i.transfer_syntax),
            )
        })
        .collect();

    if plan.single_part {
        let (path, part_type) = items.into_iter().next().expect("one instance");
        let file = match File::open(&path).await {
            Ok(f) => f,
            Err(e) => {
                tracing::error!(path = %path.display(), "blob unreadable: {e}");
                return plain(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "the stored file could not be read".into(),
                );
            }
        };
        let len = file.metadata().await.ok().map(|m| m.len());
        let mut res = Response::new(Body::from_stream(ReaderStream::with_capacity(
            file, READ_CHUNK,
        )));
        if let Ok(v) = HeaderValue::from_str(&part_type) {
            res.headers_mut().insert(header::CONTENT_TYPE, v);
        }
        if let Some(len) = len {
            res.headers_mut().insert(header::CONTENT_LENGTH, len.into());
        }
        return res;
    }

    let multipart = Arc::new(Multipart::new());
    let content_type = multipart.content_type("application/dicom");
    let body = Body::from_stream(multipart_stream(multipart, items));
    let mut res = Response::new(body);
    if let Ok(v) = HeaderValue::from_str(&content_type) {
        res.headers_mut().insert(header::CONTENT_TYPE, v);
    }
    res
}

/// Each part's header then its file; a blob that cannot be opened or read
/// ends the stream with an error (the client sees a broken body).
fn multipart_stream(
    multipart: Arc<Multipart>,
    items: Vec<(PathBuf, String)>,
) -> impl Stream<Item = std::io::Result<Bytes>> {
    let closing = Arc::clone(&multipart);
    stream::iter(items)
        .map(move |(path, part_type)| {
            let header = multipart.part_header(&part_type);
            stream::once(async move {
                match File::open(&path).await {
                    Ok(file) => stream::once(std::future::ready(Ok(header)))
                        .chain(ReaderStream::with_capacity(file, READ_CHUNK))
                        .left_stream(),
                    Err(e) => {
                        tracing::error!(path = %path.display(), "blob unreadable mid-stream: {e}");
                        stream::once(std::future::ready(Err(e))).right_stream()
                    }
                }
            })
            .flatten()
        })
        .flatten()
        .chain(stream::once(std::future::ready(Ok(closing.close()))))
}
