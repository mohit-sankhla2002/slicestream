//! DICOMweb read routes (PS3.18): QIDO-RS search. Mounted on the read
//! router when `[dicomweb] enabled = true`, so they share its CORS policy.

use std::sync::Arc;

use axum::Router;
use axum::http::{Extensions, HeaderMap, HeaderValue, StatusCode, Version, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use slicestream_dicomweb::{MediaRange, parse_accept};
use tower_http::compression::predicate::{Predicate, SizeAbove};
use tower_http::compression::{CompressionLayer, CompressionLevel};

use crate::AppState;
use crate::config::HttpConfig;

pub mod qido;
mod qido_json;

pub(crate) const DICOM_JSON: &str = "application/dicom+json";

/// The DICOMweb GET routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/dicom-web/studies", get(qido::studies))
        .route("/dicom-web/series", get(qido::all_series))
        .route("/dicom-web/instances", get(qido::all_instances))
        .route("/dicom-web/studies/{study}/series", get(qido::study_series))
        .route(
            "/dicom-web/studies/{study}/instances",
            get(qido::study_instances),
        )
        .route(
            "/dicom-web/studies/{study}/series/{series}/instances",
            get(qido::series_instances),
        )
        // JSON compresses well; pixel data and multipart bodies do not.
        .layer(
            CompressionLayer::new()
                .quality(CompressionLevel::Fastest)
                .compress_when(SizeAbove::default().and(is_dicom_json)),
        )
}

fn is_dicom_json(_: StatusCode, _: Version, headers: &HeaderMap, _: &Extensions) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with(DICOM_JSON))
}

/// The request's `Accept` ranges (several `Accept` headers count as one list).
pub(crate) fn accept_ranges(headers: &HeaderMap) -> Vec<MediaRange> {
    let values: Vec<&str> = headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .collect();
    parse_accept(Some(&values.join(",")))
}

/// Base of Retrieve URLs and BulkDataURIs: `[http] public_url`, else
/// `http://<Host>`, else the bound address.
pub(crate) fn request_base_url(http: &HttpConfig, headers: &HeaderMap) -> String {
    slicestream_dicomweb::base_url(
        http.public_url.as_deref(),
        headers.get(header::HOST).and_then(|v| v.to_str().ok()),
        &http.addr.to_string(),
    )
}

/// `299 <service>: "<text>"` (PS3.18 8.3.4.2 and 8.3.4.4.1), where
/// `<service>` is the server's base URI. Characters a header value (or the
/// quoted text) cannot hold become `?`.
pub(crate) fn warning(service: &str, text: &str) -> Option<HeaderValue> {
    let clean = |s: &str, quoted: bool| -> String {
        s.chars()
            .map(|c| match c {
                '"' | '\\' if quoted => '?',
                ' '..='~' => c,
                _ => '?',
            })
            .collect()
    };
    let value = format!("299 {}: \"{}\"", clean(service, false), clean(text, true));
    HeaderValue::from_str(&value).ok()
}

/// A plain-text response (errors).
pub(crate) fn plain(status: StatusCode, text: String) -> Response {
    (
        status,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        )],
        text,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warning_text_is_header_safe() {
        let v = warning("http://h:1", "a\"b\\c\r\nd é").unwrap();
        assert_eq!(v.to_str().unwrap(), "299 http://h:1: \"a?b?c??d ?\"");
        let v = warning("https://p\u{7f}é/x", "t").unwrap();
        assert_eq!(v.to_str().unwrap(), "299 https://p??/x: \"t\"");
    }

    #[test]
    fn accept_headers_are_joined() {
        let mut h = HeaderMap::new();
        assert_eq!(accept_ranges(&h)[0].essence, "*/*");
        h.append(
            header::ACCEPT,
            HeaderValue::from_static("application/dicom+xml"),
        );
        h.append(header::ACCEPT, HeaderValue::from_static("application/json"));
        let r = accept_ranges(&h);
        assert_eq!(r.len(), 2);
        assert_eq!(r[1].essence, "application/json");
    }
}
