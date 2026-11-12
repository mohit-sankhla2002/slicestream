//! STOW-RS and `POST /api/instances` tests, driven through the router.

#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use slicestream_core::SchedulerConfig;
use slicestream_server::config::HttpConfig;
use slicestream_server::stow::{PartResult, stow_response};
use slicestream_server::{AppState, app};
use slicestream_store::{BlobHash, Ingestor, StorageConfig, Stored};
use tower::ServiceExt;

const BOUNDARY: &str = "XXslicestreamXX";
const STUDY: &str = "2.25.1000000000000000000000000000002";

struct Harness {
    router: Router,
    ingestor: Arc<Ingestor>,
    files: Vec<PathBuf>,
    _dir: tempfile::TempDir,
}

async fn harness(slices: usize, http: impl FnOnce(&mut HttpConfig)) -> Harness {
    harness_with(slices, http, |_, _| {}).await
}

async fn harness_with(
    slices: usize,
    http: impl FnOnce(&mut HttpConfig),
    storage: impl FnOnce(&mut StorageConfig, &[PathBuf]),
) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("phantom");
    phantom::write_phantom(&data, slices, 32).unwrap();
    let mut files: Vec<PathBuf> = std::fs::read_dir(&data)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    let mut scfg = StorageConfig {
        root: dir.path().join("store"),
        ..StorageConfig::default()
    };
    storage(&mut scfg, &files);
    let ingestor = Ingestor::open(scfg).await.unwrap();
    let mut cfg = HttpConfig::default();
    http(&mut cfg);
    let state = Arc::new(AppState {
        series: HashMap::new(),
        scheduler: SchedulerConfig::default(),
        ingestor: Arc::clone(&ingestor),
        http: cfg,
    });
    Harness {
        router: app(state),
        ingestor,
        files,
        _dir: dir,
    }
}

/// A part: (Content-Type, bytes).
fn multipart(parts: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut body = Vec::new();
    for (ct, bytes) in parts {
        body.extend_from_slice(format!("--{BOUNDARY}\r\nContent-Type: {ct}\r\n\r\n").as_bytes());
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    body
}

fn related_ct() -> String {
    format!("multipart/related; type=\"application/dicom\"; boundary={BOUNDARY}")
}

fn stow_request(uri: &str, content_type: &str, body: Vec<u8>) -> Request<Body> {
    Request::post(uri)
        .header(header::HOST, "pacs.example")
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .unwrap()
}

async fn send(router: &Router, req: Request<Body>) -> (StatusCode, Value) {
    let res = router.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

fn dicom_parts(files: &[PathBuf]) -> Vec<(&'static str, Vec<u8>)> {
    files
        .iter()
        .map(|f| ("application/dicom", std::fs::read(f).unwrap()))
        .collect()
}

fn items<'a>(v: &'a Value, tag: &str) -> &'a [Value] {
    v[tag]["Value"].as_array().map(Vec::as_slice).unwrap_or(&[])
}

async fn instance_total(ingestor: &Ingestor) -> i64 {
    ingestor
        .index()
        .list_series()
        .await
        .unwrap()
        .iter()
        .map(|s| s.instance_count)
        .sum()
}

#[tokio::test]
async fn stow_three_instances_200() {
    let h = harness(3, |_| {}).await;
    let body = multipart(&dicom_parts(&h.files));
    let (status, json) = send(
        &h.router,
        stow_request("/dicom-web/studies", &related_ct(), body),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(items(&json, "00081199").len(), 3);
    assert!(json.get("00081198").is_none());
    assert_eq!(
        json["00081190"]["Value"][0],
        format!("http://pacs.example/dicom-web/studies/{STUDY}")
    );
    assert_eq!(instance_total(&h.ingestor).await, 3);
}

#[tokio::test]
async fn mixed_parts_returns_202() {
    let h = harness(2, |_| {}).await;
    let mut parts = dicom_parts(&h.files);
    parts.push(("application/dicom", b"not dicom".to_vec()));
    let (status, json) = send(
        &h.router,
        stow_request("/dicom-web/studies", &related_ct(), multipart(&parts)),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{json}");
    assert_eq!(items(&json, "00081199").len(), 2);
    let failed = items(&json, "00081198");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["00081197"]["Value"], json!([49152]));
    assert_eq!(instance_total(&h.ingestor).await, 2);
}

#[tokio::test]
async fn all_failed_returns_409() {
    let h = harness(1, |_| {}).await;
    let parts = vec![
        ("application/dicom", b"not dicom".to_vec()),
        ("text/plain", b"hello".to_vec()),
    ];
    let (status, json) = send(
        &h.router,
        stow_request("/dicom-web/studies", &related_ct(), multipart(&parts)),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{json}");
    assert!(json.get("00081199").is_none());
    let failed = items(&json, "00081198");
    assert_eq!(failed.len(), 2);
    for f in failed {
        assert_eq!(f["00081197"]["Value"], json!([49152]));
    }
    assert_eq!(instance_total(&h.ingestor).await, 0);
}

#[tokio::test]
async fn study_mismatch_in_url() {
    let h = harness(1, |_| {}).await;
    let body = multipart(&dicom_parts(&h.files));
    let (status, json) = send(
        &h.router,
        stow_request("/dicom-web/studies/9.9.9", &related_ct(), body),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{json}");
    let failed = items(&json, "00081198");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["00081197"]["Value"], json!([50185]));
    assert_eq!(
        json["00081190"]["Value"][0],
        "http://pacs.example/dicom-web/studies/9.9.9"
    );
    assert_eq!(instance_total(&h.ingestor).await, 0);
}

#[tokio::test]
async fn wrong_content_type_415() {
    let h = harness(1, |_| {}).await;
    let (status, _) = send(
        &h.router,
        stow_request("/dicom-web/studies", "application/json", b"{}".to_vec()),
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);

    let ct = format!("multipart/related; type=\"application/json\"; boundary={BOUNDARY}");
    let body = multipart(&dicom_parts(&h.files));
    let (status, _) = send(&h.router, stow_request("/dicom-web/studies", &ct, body)).await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test]
async fn missing_type_param_is_accepted() {
    let h = harness(1, |_| {}).await;
    let ct = format!("multipart/related; boundary={BOUNDARY}");
    let body = multipart(&dicom_parts(&h.files));
    let (status, json) = send(&h.router, stow_request("/dicom-web/studies", &ct, body)).await;
    assert_eq!(status, StatusCode::OK, "{json}");
}

#[tokio::test]
async fn missing_boundary_400() {
    let h = harness(1, |_| {}).await;
    let body = multipart(&dicom_parts(&h.files));
    let (status, _) = send(
        &h.router,
        stow_request(
            "/dicom-web/studies",
            "multipart/related; type=\"application/dicom\"",
            body,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn malformed_body_400() {
    let h = harness(1, |_| {}).await;
    let (status, _) = send(
        &h.router,
        stow_request(
            "/dicom-web/studies",
            &related_ct(),
            b"--XXslicestreamXX\r\nContent-Type: application/dicom\r\n\r\ntruncated".to_vec(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(instance_total(&h.ingestor).await, 0);
}

#[tokio::test]
async fn request_too_large_413() {
    let h = harness(1, |c| c.max_request_bytes = 1024).await;
    let body = multipart(&dicom_parts(&h.files));
    assert!(body.len() > 1024);
    let (status, _) = send(
        &h.router,
        stow_request("/dicom-web/studies", &related_ct(), body),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(instance_total(&h.ingestor).await, 0);
}

#[tokio::test]
async fn oversized_part_fails_and_next_part_is_stored() {
    let h = harness_with(
        1,
        |_| {},
        |s, files| s.max_instance_bytes = std::fs::metadata(&files[0]).unwrap().len(),
    )
    .await;
    let limit = std::fs::metadata(&h.files[0]).unwrap().len() as usize;
    let mut parts = vec![("application/dicom", vec![0u8; limit + 4096])];
    parts.extend(dicom_parts(&h.files));
    let (status, json) = send(
        &h.router,
        stow_request("/dicom-web/studies", &related_ct(), multipart(&parts)),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{json}");
    assert_eq!(items(&json, "00081199").len(), 1);
    let failed = items(&json, "00081198");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["00081197"]["Value"], json!([0xA700]));
    assert_eq!(instance_total(&h.ingestor).await, 1);
    // The rejected spool file was removed.
    let tmp = h._dir.path().join("store").join("tmp");
    assert_eq!(std::fs::read_dir(tmp).unwrap().count(), 0);
}

#[tokio::test]
async fn token_required_401() {
    let h = harness(1, |c| c.stow_token = Some("s3cret".into())).await;
    let body = multipart(&dicom_parts(&h.files));

    let (status, _) = send(
        &h.router,
        stow_request("/dicom-web/studies", &related_ct(), body.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let mut req = stow_request("/dicom-web/studies", &related_ct(), body.clone());
    req.headers_mut()
        .insert(header::AUTHORIZATION, "Bearer wrong!".parse().unwrap());
    let (status, _) = send(&h.router, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let mut req = stow_request("/dicom-web/studies", &related_ct(), body);
    req.headers_mut()
        .insert(header::AUTHORIZATION, "Bearer s3cret".parse().unwrap());
    let (status, json) = send(&h.router, req).await;
    assert_eq!(status, StatusCode::OK, "{json}");

    // The single-instance endpoint is guarded too.
    let bytes = std::fs::read(&h.files[0]).unwrap();
    let (status, _) = send(
        &h.router,
        stow_request("/api/instances", "application/dicom", bytes),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn post_instance_single() {
    let h = harness(1, |_| {}).await;
    let bytes = std::fs::read(&h.files[0]).unwrap();

    let (status, json) = send(
        &h.router,
        stow_request("/api/instances", "application/dicom", bytes.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["status"], "stored");
    assert_eq!(json["studyInstanceUid"], STUDY);
    assert_eq!(
        json["seriesInstanceUid"],
        "2.25.1000000000000000000000000000001"
    );
    let sop = json["sopInstanceUid"].as_str().unwrap().to_owned();
    assert!(h.ingestor.index().instance(&sop).await.unwrap().is_some());

    let (status, json) = send(
        &h.router,
        stow_request("/api/instances", "application/dicom", bytes),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["status"], "duplicate");
}

#[tokio::test]
async fn post_instance_errors() {
    let h = harness(1, |_| {}).await;
    let (status, _) = send(
        &h.router,
        stow_request("/api/instances", "application/json", b"{}".to_vec()),
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);

    let (status, json) = send(
        &h.router,
        stow_request("/api/instances", "application/dicom", b"not dicom".to_vec()),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json["status"], 0xC000);
    assert!(json["error"].is_string());
}

#[tokio::test]
async fn post_instance_too_large_413() {
    let dir = tempfile::tempdir().unwrap();
    let ingestor = Ingestor::open(StorageConfig {
        root: dir.path().join("store"),
        max_instance_bytes: 16,
        ..StorageConfig::default()
    })
    .await
    .unwrap();
    let router = app(Arc::new(AppState {
        series: HashMap::new(),
        scheduler: SchedulerConfig::default(),
        ingestor,
        http: HttpConfig::default(),
    }));
    let (status, json) = send(
        &router,
        stow_request("/api/instances", "application/dicom", vec![0u8; 64]),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(json["status"], 0xA700);
}

fn preflight(uri: &str) -> Request<Body> {
    Request::options(uri)
        .header(header::ORIGIN, "https://evil.example")
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
        .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn ingest_routes_reject_cors_preflight() {
    let h = harness(1, |_| {}).await;
    for uri in [
        "/api/instances",
        "/dicom-web/studies",
        "/dicom-web/studies/1.2.3",
    ] {
        let res = h.router.clone().oneshot(preflight(uri)).await.unwrap();
        assert!(
            res.headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none(),
            "{uri} granted a cross-origin preflight"
        );
    }

    // A simple cross-origin POST gets no CORS grant either.
    let mut req = stow_request(
        "/api/instances",
        "application/dicom",
        std::fs::read(&h.files[0]).unwrap(),
    );
    req.headers_mut()
        .insert(header::ORIGIN, "https://evil.example".parse().unwrap());
    let res = h.router.clone().oneshot(req).await.unwrap();
    assert!(
        res.headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none()
    );
}

#[tokio::test]
async fn read_routes_keep_permissive_cors() {
    let h = harness(1, |_| {}).await;
    let req = Request::get("/api/series")
        .header(header::ORIGIN, "https://viewer.example")
        .body(Body::empty())
        .unwrap();
    let res = h.router.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(
        res.headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_some()
    );
}

#[test]
fn stow_response_shape() {
    let stored = Stored {
        sop_class_uid: "1.2.840.10008.5.1.4.1.1.2".into(),
        sop_instance_uid: "1.2.3.4".into(),
        study_instance_uid: "1.2.3".into(),
        series_instance_uid: "1.2.3.9".into(),
        blob: BlobHash([0; 32]),
        duplicate: false,
        replaced: false,
    };
    let results = [
        PartResult::Stored(stored),
        PartResult::Failed {
            sop_class_uid: Some("1.2.840.10008.5.1.4.1.1.2".into()),
            sop_instance_uid: Some("1.2.3.5".into()),
            reason: 0xC409,
        },
    ];
    let (status, v) = stow_response("http://h", Some("1.2.3"), &results);
    assert_eq!(status, StatusCode::ACCEPTED);
    let expected = json!({
        "00081190": { "vr": "UR", "Value": ["http://h/dicom-web/studies/1.2.3"] },
        "00081199": { "vr": "SQ", "Value": [{
            "00081150": { "vr": "UI", "Value": ["1.2.840.10008.5.1.4.1.1.2"] },
            "00081155": { "vr": "UI", "Value": ["1.2.3.4"] },
            "00081190": { "vr": "UR", "Value": [
                "http://h/dicom-web/studies/1.2.3/series/1.2.3.9/instances/1.2.3.4"
            ] }
        }] },
        "00081198": { "vr": "SQ", "Value": [{
            "00081150": { "vr": "UI", "Value": ["1.2.840.10008.5.1.4.1.1.2"] },
            "00081155": { "vr": "UI", "Value": ["1.2.3.5"] },
            "00081197": { "vr": "US", "Value": [50185] }
        }] }
    });
    assert_eq!(v, expected);

    // Unknown UIDs are omitted; no study → no top-level RetrieveURL; all failed → 409.
    let (status, v) = stow_response(
        "http://h",
        None,
        &[PartResult::Failed {
            sop_class_uid: None,
            sop_instance_uid: None,
            reason: 0xC000,
        }],
    );
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        v,
        json!({ "00081198": { "vr": "SQ", "Value": [{
            "00081197": { "vr": "US", "Value": [49152] }
        }] } })
    );
}
