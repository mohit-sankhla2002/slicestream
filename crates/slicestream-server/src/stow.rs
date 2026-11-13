//! Ingest over HTTP: STOW-RS (`POST /dicom-web/studies[/{study}]`) and the
//! single-instance convenience endpoint `POST /api/instances`.
//!
//! Bodies are streamed part by part into [`TempBlob`]s and never buffered
//! whole. Status codes follow the spec's "Status mapping" table.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::body::Body;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::{Path, State};
use axum::http::{Extensions, HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use mime::Mime;
use multer::{Constraints, Field, Multipart, SizeLimit};
use serde_json::{Map, Value, json};
use slicestream_store::{Expectations, IngestError, IngestFailure, IngestSource, Stored, TempBlob};
use subtle::ConstantTimeEq;

use crate::AppState;
use crate::config::HttpConfig;

const APPLICATION_DICOM: &str = "application/dicom";
const DICOM_JSON: &str = "application/dicom+json";

/// FailureReason: cannot parse / wrong part type.
const REASON_CANNOT_UNDERSTAND: u16 = 0xC000;
/// FailureReason: out of resources (too large, storage or index failure).
const REASON_OUT_OF_RESOURCES: u16 = 0xA700;

/// Outcome of one STOW-RS part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartResult {
    Stored(Stored),
    Failed {
        sop_class_uid: Option<String>,
        sop_instance_uid: Option<String>,
        reason: u16,
    },
}

impl PartResult {
    fn failed(reason: u16) -> Self {
        PartResult::Failed {
            sop_class_uid: None,
            sop_instance_uid: None,
            reason,
        }
    }

    /// Map an ingest error, keeping whichever UIDs the error itself carries.
    fn from_error(e: &IngestError) -> Self {
        let (sop_class_uid, sop_instance_uid) = uids_in_error(e);
        PartResult::Failed {
            sop_class_uid,
            sop_instance_uid,
            reason: e.status(),
        }
    }

    /// Map an ingest failure, preferring the UIDs of the parsed header.
    fn from_failure(f: &IngestFailure) -> Self {
        let (class, instance) = uids_in_error(&f.error);
        PartResult::Failed {
            sop_class_uid: f.sop_class_uid.clone().or(class),
            sop_instance_uid: f.sop_instance_uid.clone().or(instance),
            reason: f.status(),
        }
    }
}

/// SOP Class / Instance UIDs named by the error itself, if any.
fn uids_in_error(e: &IngestError) -> (Option<String>, Option<String>) {
    match e {
        IngestError::UidMismatch { field, actual, .. } => match *field {
            "SOPClassUID" => (Some(actual.clone()), None),
            "SOPInstanceUID" => (None, Some(actual.clone())),
            _ => (None, None),
        },
        IngestError::UnsupportedSopClass(uid) => (Some(uid.clone()), None),
        _ => (None, None),
    }
}

/// `POST /dicom-web/studies`
pub async fn stow_all(
    State(state): State<Arc<AppState>>,
    ext: Extensions,
    headers: HeaderMap,
    body: Body,
) -> Response {
    stow_inner(&state, None, remote_addr(&ext), &headers, body).await
}

/// `POST /dicom-web/studies/{study}`
pub async fn stow_study(
    State(state): State<Arc<AppState>>,
    Path(study): Path<String>,
    ext: Extensions,
    headers: HeaderMap,
    body: Body,
) -> Response {
    stow_inner(&state, Some(study), remote_addr(&ext), &headers, body).await
}

pub(crate) fn remote_addr(ext: &Extensions) -> Option<SocketAddr> {
    ext.get::<ConnectInfo<SocketAddr>>().map(|c| c.0)
}

/// Shared STOW-RS implementation. `study`, if given, must match every part's
/// Study Instance UID (mismatches fail with 0xC409).
pub async fn stow_inner(
    state: &AppState,
    study: Option<String>,
    remote: Option<SocketAddr>,
    headers: &HeaderMap,
    body: Body,
) -> Response {
    if !authorized(&state.http, headers) {
        return unauthorized();
    }
    let boundary = match stow_boundary(headers) {
        Ok(b) => b,
        Err(status) => return status.into_response(),
    };
    let constraints =
        Constraints::new().size_limit(SizeLimit::new().whole_stream(state.http.max_request_bytes));
    let mut multipart = Multipart::with_constraints(body.into_data_stream(), boundary, constraints);

    let mut results = Vec::new();
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => return multipart_error(e),
        };
        match ingest_part(state, field, study.as_deref(), remote).await {
            Ok(r) => results.push(r),
            Err(e) => return multipart_error(e),
        }
    }

    // Top-level RetrieveURL: the URL study, else the single study all stored
    // instances belong to.
    let retrieve_study = study.or_else(|| single_study(&results));
    let (status, value) = stow_response(
        &base_url(&state.http, headers),
        retrieve_study.as_deref(),
        &results,
    );
    (
        status,
        [(header::CONTENT_TYPE, HeaderValue::from_static(DICOM_JSON))],
        value.to_string(),
    )
        .into_response()
}

/// Spool one part and ingest it. `Err` only for multipart framing / transport
/// errors, which abort the whole request.
async fn ingest_part(
    state: &AppState,
    mut field: Field<'_>,
    study: Option<&str>,
    remote: Option<SocketAddr>,
) -> Result<PartResult, multer::Error> {
    let is_dicom = field
        .content_type()
        .is_some_and(|m| m.essence_str() == APPLICATION_DICOM);
    if !is_dicom {
        // Dropping the field lets multer skip the rest of its data.
        return Ok(PartResult::failed(REASON_CANNOT_UNDERSTAND));
    }

    let mut temp: Option<TempBlob> = match state.ingestor.create_temp() {
        Ok(t) => Some(t),
        Err(e) => {
            tracing::error!(error = %e, "cannot create spool file");
            None
        }
    };
    let mut reason = REASON_OUT_OF_RESOURCES;
    while let Some(chunk) = field.chunk().await? {
        if let Some(t) = temp.as_mut()
            && let Err(e) = t.write_chunk(&chunk).await
        {
            let e = IngestError::from(e);
            if !matches!(e, IngestError::TooLarge { .. }) {
                tracing::error!(error = %e, "spool write failed");
            }
            reason = e.status();
            // Dropping the TempBlob removes its file; keep draining the part.
            temp = None;
        }
    }
    let Some(temp) = temp else {
        return Ok(PartResult::failed(reason));
    };
    let spooled = match temp.finish().await {
        Ok(s) => s,
        Err(e) => {
            let e = IngestError::from(e);
            tracing::error!(error = %e, "spool finish failed");
            return Ok(PartResult::from_error(&e));
        }
    };
    let expect = Expectations {
        study_instance_uid: study.map(str::to_owned),
        ..Expectations::default()
    };
    match state
        .ingestor
        .ingest(spooled, IngestSource::Http { remote }, expect)
        .await
    {
        Ok(stored) => Ok(PartResult::Stored(stored)),
        Err(e) => {
            log_ingest_error(&e.error);
            Ok(PartResult::from_failure(&e))
        }
    }
}

fn single_study(results: &[PartResult]) -> Option<String> {
    let mut studies = results.iter().filter_map(|r| match r {
        PartResult::Stored(s) => Some(s.study_instance_uid.as_str()),
        PartResult::Failed { .. } => None,
    });
    let first = studies.next()?;
    studies.all(|s| s == first).then(|| first.to_owned())
}

/// Validate the request Content-Type and return the multipart boundary.
fn stow_boundary(headers: &HeaderMap) -> Result<String, StatusCode> {
    let mime: Mime = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .ok_or(StatusCode::UNSUPPORTED_MEDIA_TYPE)?;
    if mime.type_() != mime::MULTIPART || mime.subtype() != "related" {
        return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    // Many clients omit `type`; accept that, but reject any other type.
    if let Some(ty) = mime.get_param("type") {
        let ty: Mime = ty
            .as_str()
            .parse()
            .map_err(|_| StatusCode::UNSUPPORTED_MEDIA_TYPE)?;
        if ty.essence_str() != APPLICATION_DICOM {
            return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }
    }
    mime.get_param(mime::BOUNDARY)
        .map(|b| b.as_str().to_owned())
        .filter(|b| !b.is_empty())
        .ok_or(StatusCode::BAD_REQUEST)
}

fn multipart_error(e: multer::Error) -> Response {
    match e {
        multer::Error::StreamSizeExceeded { .. } => StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        e => (
            StatusCode::BAD_REQUEST,
            format!("malformed multipart body: {e}"),
        )
            .into_response(),
    }
}

/// `scheme://host` used to build RetrieveURLs.
fn base_url(http: &HttpConfig, headers: &HeaderMap) -> String {
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .filter(|h| !h.is_empty() && h.bytes().all(|b| b.is_ascii_graphic()))
        .map(str::to_owned)
        .unwrap_or_else(|| http.addr.to_string());
    format!("http://{host}")
}

/// Whether the request carries the configured bearer token (if any).
pub(crate) fn authorized(http: &HttpConfig, headers: &HeaderMap) -> bool {
    let Some(expected) = &http.stow_token else {
        return true;
    };
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    let Some((scheme, token)) = value.split_once(' ') else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("bearer") {
        return false;
    }
    // `ct_eq` returns false for slices of different lengths.
    token.trim().as_bytes().ct_eq(expected.as_bytes()).into()
}

pub(crate) fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"))],
    )
        .into_response()
}

fn log_ingest_error(e: &IngestError) {
    match e {
        IngestError::Storage(_) | IngestError::Index(_) => {
            tracing::error!(error = %e, "ingest failed");
        }
        _ => tracing::info!(error = %e, "instance rejected"),
    }
}

fn attr(vr: &str, value: Value) -> Value {
    json!({ "vr": vr, "Value": value })
}

/// Build the STOW-RS response body (DICOM JSON) and HTTP status.
///
/// 200 if every part was stored, 202 if some were, 409 if none were.
pub fn stow_response(
    base_url: &str,
    study: Option<&str>,
    results: &[PartResult],
) -> (StatusCode, Value) {
    let mut referenced = Vec::new();
    let mut failed = Vec::new();
    for r in results {
        match r {
            PartResult::Stored(s) => {
                let url = format!(
                    "{base_url}/dicom-web/studies/{}/series/{}/instances/{}",
                    s.study_instance_uid, s.series_instance_uid, s.sop_instance_uid
                );
                referenced.push(json!({
                    "00081150": attr("UI", json!([s.sop_class_uid])),
                    "00081155": attr("UI", json!([s.sop_instance_uid])),
                    "00081190": attr("UR", json!([url])),
                }));
            }
            PartResult::Failed {
                sop_class_uid,
                sop_instance_uid,
                reason,
            } => {
                let mut item = Map::new();
                if let Some(uid) = sop_class_uid {
                    item.insert("00081150".into(), attr("UI", json!([uid])));
                }
                if let Some(uid) = sop_instance_uid {
                    item.insert("00081155".into(), attr("UI", json!([uid])));
                }
                item.insert("00081197".into(), attr("US", json!([reason])));
                failed.push(Value::Object(item));
            }
        }
    }

    let status = if referenced.is_empty() {
        StatusCode::CONFLICT
    } else if failed.is_empty() {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };

    let mut body = Map::new();
    if let Some(study) = study {
        body.insert(
            "00081190".into(),
            attr(
                "UR",
                json!([format!("{base_url}/dicom-web/studies/{study}")]),
            ),
        );
    }
    if !failed.is_empty() {
        body.insert("00081198".into(), attr("SQ", Value::Array(failed)));
    }
    if !referenced.is_empty() {
        body.insert("00081199".into(), attr("SQ", Value::Array(referenced)));
    }
    (status, Value::Object(body))
}

/// `POST /api/instances`: one Part-10 file as `application/dicom`.
pub async fn post_instance(
    State(state): State<Arc<AppState>>,
    ext: Extensions,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if !authorized(&state.http, &headers) {
        return unauthorized();
    }
    let is_dicom = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<Mime>().ok())
        .is_some_and(|m| m.essence_str() == APPLICATION_DICOM);
    if !is_dicom {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
    }

    let mut temp = match state.ingestor.create_temp() {
        Ok(t) => t,
        Err(e) => return ingest_error_response(&e),
    };
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                let body = json!({ "error": format!("reading request body: {e}"), "status": 400 });
                return (StatusCode::BAD_REQUEST, Json(body)).into_response();
            }
        };
        // On error `temp` is dropped on return, removing its file.
        if let Err(e) = temp.write_chunk(&chunk).await {
            return ingest_error_response(&IngestError::from(e));
        }
    }
    let spooled = match temp.finish().await {
        Ok(s) => s,
        Err(e) => return ingest_error_response(&IngestError::from(e)),
    };
    let source = IngestSource::Http {
        remote: remote_addr(&ext),
    };
    match state
        .ingestor
        .ingest(spooled, source, Expectations::default())
        .await
    {
        Ok(s) => {
            let status = if s.replaced {
                "replaced"
            } else if s.duplicate {
                "duplicate"
            } else {
                "stored"
            };
            Json(json!({
                "status": status,
                "sopInstanceUid": s.sop_instance_uid,
                "seriesInstanceUid": s.series_instance_uid,
                "studyInstanceUid": s.study_instance_uid,
            }))
            .into_response()
        }
        Err(e) => ingest_error_response(&e.error),
    }
}

fn ingest_error_response(e: &IngestError) -> Response {
    log_ingest_error(e);
    let (http, message) = match e {
        IngestError::TooLarge { .. } => (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()),
        // Don't leak filesystem / database details to the client.
        IngestError::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "storage error".into()),
        IngestError::Index(_) => (StatusCode::INTERNAL_SERVER_ERROR, "index error".into()),
        _ => (StatusCode::BAD_REQUEST, e.to_string()),
    };
    (
        http,
        Json(json!({ "error": message, "status": e.status() })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_sop_class_reports_class_uid() {
        let r = PartResult::from_error(&IngestError::UnsupportedSopClass(
            "1.2.840.10008.1.1".into(),
        ));
        assert_eq!(
            r,
            PartResult::Failed {
                sop_class_uid: Some("1.2.840.10008.1.1".into()),
                sop_instance_uid: None,
                reason: 0x0122,
            }
        );
    }
}
