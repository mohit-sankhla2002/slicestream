//! WADO-RS metadata and instance retrieve against the full server on an
//! ephemeral port. Every instance is synthesized here (no real patient data).

use std::net::SocketAddr;
use std::path::Path;

use dicom_core::VR;
use dicom_dictionary_std::tags;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod support;
use support::*;

const DICOM_JSON: &str = "application/dicom+json";
const ORIGIN: &str = "https://viewer.example";
const EXPLICIT_LE: &str = "1.2.840.10008.1.2.1";

fn study() -> String {
    study_uid(1, 1)
}
fn series() -> String {
    series_uid(1, 1, 1)
}

/// Study 1: series 1 with instances 1 and 2, series 2 with instance 1.
async fn server() -> TestServer {
    let srv = TestServer::start(|_| {}).await;
    srv.stow(&[
        dicom(1, 1, 1, 1),
        dicom(1, 1, 1, 2),
        dicom(1, 1, 2, 1),
        dicom(2, 2, 1, 1),
    ])
    .await;
    srv
}

async fn index_pool(root: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect(&format!("sqlite://{}", root.join("index.sqlite").display()))
        .await
        .unwrap()
}

fn metadata_path(study: &str, series: Option<&str>, sop: Option<&str>) -> String {
    let mut p = format!("/dicom-web/studies/{study}");
    if let Some(se) = series {
        p.push_str(&format!("/series/{se}"));
        if let Some(i) = sop {
            p.push_str(&format!("/instances/{i}"));
        }
    }
    p
}

fn sops(rows: &[Value]) -> Vec<String> {
    rows.iter()
        .map(|r| r["00080018"]["Value"][0].as_str().unwrap().to_owned())
        .collect()
}

/// Split a `multipart/related` body into (part content type, part body).
fn parts(r: &Reply) -> Vec<(String, Vec<u8>)> {
    let ct = r.header("content-type").unwrap();
    assert!(ct.starts_with("multipart/related"), "{ct}");
    let boundary = ct.split("boundary=").nth(1).unwrap().trim_matches('"');
    let delim = format!("--{boundary}");
    let body = &r.body;
    let find = |from: usize, pat: &[u8]| {
        body[from..]
            .windows(pat.len())
            .position(|w| w == pat)
            .map(|p| p + from)
    };
    let mut out = Vec::new();
    let mut pos = find(0, delim.as_bytes()).unwrap();
    loop {
        let after = pos + delim.len();
        if body[after..].starts_with(b"--") {
            break;
        }
        let head_end = find(after, b"\r\n\r\n").unwrap();
        let head = String::from_utf8_lossy(&body[after..head_end]).into_owned();
        let part_type = head
            .lines()
            .find_map(|l| l.strip_prefix("Content-Type: "))
            .unwrap()
            .to_owned();
        let next = find(head_end + 4, format!("\r\n{delim}").as_bytes()).unwrap();
        out.push((part_type, body[head_end + 4..next].to_vec()));
        pos = next + 2;
    }
    out
}

/// Sizes of the HTTP/1.1 chunks of a `Connection: close` GET response.
async fn chunk_sizes(addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> Vec<usize> {
    let mut head = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(head.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let split = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let head = String::from_utf8_lossy(&buf[..split]).to_ascii_lowercase();
    assert!(head.starts_with("http/1.1 200"), "{head}");
    assert!(head.contains("transfer-encoding: chunked"), "{head}");
    let mut sizes = Vec::new();
    let mut rest = &buf[split..];
    loop {
        let eol = rest.windows(2).position(|w| w == b"\r\n").unwrap();
        let n = usize::from_str_radix(std::str::from_utf8(&rest[..eol]).unwrap(), 16).unwrap();
        if n == 0 {
            return sizes;
        }
        sizes.push(n);
        rest = &rest[eol + 2 + n + 2..];
    }
}

#[tokio::test]
async fn series_metadata_lists_every_instance() {
    let srv = server().await;
    let host = srv.addr().to_string();
    let r = srv
        .get(
            &format!(
                "{}/metadata",
                metadata_path(&study(), Some(&series()), None)
            ),
            &[("Host", &host)],
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("content-type"), Some(DICOM_JSON));
    let rows = r.json();
    let rows = rows.as_array().unwrap();
    assert_eq!(sops(rows), [sop(1, 1, 1, 1), sop(1, 1, 1, 2)]);
    let uri = rows[0]["7FE00010"]["BulkDataURI"].as_str().unwrap();
    assert!(uri.starts_with(&format!(
        "http://{host}/dicom-web/studies/{}/series/{}/instances/{}/",
        study(),
        series(),
        sop(1, 1, 1, 1)
    )));
    assert!(uri.ends_with("7FE00010"), "{uri}");
    srv.stop().await;
}

#[tokio::test]
async fn study_and_instance_metadata() {
    let srv = server().await;
    let r = srv
        .get(&format!("/dicom-web/studies/{}/metadata", study()), &[])
        .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json().as_array().unwrap().len(), 3);
    let r = srv
        .get(
            &format!(
                "{}/metadata",
                metadata_path(&study(), Some(&series()), Some(&sop(1, 1, 1, 2)))
            ),
            &[],
        )
        .await;
    assert_eq!(r.status, 200);
    assert_eq!(sops(r.json().as_array().unwrap()), [sop(1, 1, 1, 2)]);
    srv.stop().await;
}

#[tokio::test]
async fn metadata_bulk_uri_uses_public_url() {
    let srv =
        TestServer::start(|c| c.http.public_url = Some("https://pacs.example/base".into())).await;
    srv.stow(&[dicom(1, 1, 1, 1)]).await;
    let r = srv
        .get(&format!("/dicom-web/studies/{}/metadata", study()), &[])
        .await;
    let rows = r.json();
    let uri = rows[0]["7FE00010"]["BulkDataURI"].as_str().unwrap();
    assert!(
        uri.starts_with("https://pacs.example/base/dicom-web/studies/"),
        "{uri}"
    );
    srv.stop().await;
}

#[tokio::test]
async fn instance_metadata_404_wrong_series() {
    let srv = server().await;
    let r = srv
        .get(
            &format!(
                "{}/metadata",
                metadata_path(&study(), Some(&series_uid(1, 1, 2)), Some(&sop(1, 1, 1, 1)))
            ),
            &[],
        )
        .await;
    assert_eq!(r.status, 404);
    srv.stop().await;
}

#[tokio::test]
async fn unknown_study_404() {
    let srv = server().await;
    for p in [
        "/dicom-web/studies/1.2.3/metadata",
        "/dicom-web/studies/1.2.3/series/4/metadata",
        "/dicom-web/studies/1.2.3",
        "/dicom-web/studies/1.2.3/series/4",
        "/dicom-web/studies/1.2.3/series/4/instances/5",
    ] {
        let r = srv.get(p, &[]).await;
        assert_eq!(r.status, 404, "{p}");
    }
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_scans_unscanned_instance() {
    let srv = server().await;
    let pool = index_pool(&srv.root()).await;
    for q in [
        "DELETE FROM instance_data",
        "UPDATE instances SET scan_version = 0",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    let r = srv
        .get(
            &format!(
                "{}/metadata",
                metadata_path(&study(), Some(&series()), None)
            ),
            &[],
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(sops(r.json().as_array().unwrap()).len(), 2);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_unavailable_500() {
    let srv = server().await;
    let pool = index_pool(&srv.root()).await;
    sqlx::query("UPDATE instance_data SET metadata = NULL")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE instances SET scan_problem = 'metadata too big for test'")
        .execute(&pool)
        .await
        .unwrap();
    let r = srv
        .get(&format!("/dicom-web/studies/{}/metadata", study()), &[])
        .await;
    assert_eq!(r.status, 500);
    assert!(
        r.header("content-type").unwrap().starts_with("text/plain"),
        "{:?}",
        r.headers
    );
    assert!(
        r.text().contains("metadata too big for test"),
        "{}",
        r.text()
    );
    srv.stop().await;
}

#[tokio::test]
async fn non_image_instance_in_series_metadata() {
    let srv = TestServer::start(|_| {}).await;
    let sr = dicom_with(
        1,
        1,
        1,
        2,
        &[
            (tags::SOP_CLASS_UID, VR::UI, "1.2.840.10008.5.1.4.1.1.88.11"),
            (tags::MODALITY, VR::CS, "SR"),
        ],
    );
    srv.stow(&[dicom(1, 1, 1, 1), sr]).await;
    let r = srv
        .get(
            &format!(
                "{}/metadata",
                metadata_path(&study(), Some(&series()), None)
            ),
            &[],
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(
        sops(r.json().as_array().unwrap()),
        [sop(1, 1, 1, 1), sop(1, 1, 1, 2)]
    );
    srv.stop().await;
}

#[tokio::test]
async fn metadata_not_acceptable() {
    let srv = server().await;
    let r = srv
        .get(
            &format!("/dicom-web/studies/{}/metadata", study()),
            &[("Accept", "application/dicom+xml")],
        )
        .await;
    assert_eq!(r.status, 406);
    srv.stop().await;
}

#[tokio::test]
async fn metadata_gzip_when_accepted() {
    let srv = server().await;
    let r = srv
        .get(
            &format!("/dicom-web/studies/{}/metadata", study()),
            &[("Accept-Encoding", "gzip")],
        )
        .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-encoding"), Some("gzip"));
    let mut json = Vec::new();
    std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(&r.body[..]), &mut json).unwrap();
    let rows: Value = serde_json::from_slice(&json).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 3);
    srv.stop().await;
}

#[tokio::test]
async fn study_retrieve_parts_equal_files() {
    let srv = TestServer::start(|_| {}).await;
    let files = [dicom(1, 1, 1, 1), dicom(1, 1, 1, 2), dicom(1, 1, 2, 1)];
    srv.stow(&files).await;
    let r = srv
        .get(
            &format!("/dicom-web/studies/{}", study()),
            &[("Accept-Encoding", "gzip")],
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("content-encoding"), None);
    let ct = r.header("content-type").unwrap();
    assert!(ct.contains("type=\"application/dicom\""), "{ct}");
    let got = parts(&r);
    assert_eq!(got.len(), 3);
    for (i, (pt, body)) in got.iter().enumerate() {
        assert_eq!(
            pt,
            &format!("application/dicom; transfer-syntax={EXPLICIT_LE}")
        );
        assert_eq!(body, &files[i], "part {i}");
    }
    // Series level returns just that series.
    let r = srv
        .get(
            &format!("/dicom-web/studies/{}/series/{}", study(), series()),
            &[],
        )
        .await;
    assert_eq!(parts(&r).len(), 2);
    srv.stop().await;
}

#[tokio::test]
async fn instance_single_part_bare_accept() {
    let srv = TestServer::start(|_| {}).await;
    let file = dicom(1, 1, 1, 1);
    srv.stow(std::slice::from_ref(&file)).await;
    let path = metadata_path(&study(), Some(&series()), Some(&sop(1, 1, 1, 1)));
    let r = srv.get(&path, &[("Accept", "application/dicom")]).await;
    assert_eq!(r.status, 200);
    assert!(
        r.header("content-type")
            .unwrap()
            .starts_with("application/dicom"),
        "{:?}",
        r.headers
    );
    assert!(!r.header("content-type").unwrap().contains("multipart"));
    assert_eq!(r.body, file);
    // Default (no Accept) is multipart with one part.
    let r = srv.get(&path, &[]).await;
    assert_eq!(parts(&r).len(), 1);
    srv.stop().await;
}

#[tokio::test]
async fn bare_accept_multiple_instances_406() {
    let srv = server().await;
    let r = srv
        .get(
            &format!("/dicom-web/studies/{}", study()),
            &[("Accept", "application/dicom")],
        )
        .await;
    assert_eq!(r.status, 406);
    srv.stop().await;
}

#[tokio::test]
async fn specific_ts_mismatch_406() {
    let srv = server().await;
    let r = srv
        .get(
            &format!("/dicom-web/studies/{}", study()),
            &[(
                "Accept",
                "multipart/related; type=\"application/dicom\"; transfer-syntax=1.2.840.10008.1.2.4.50",
            )],
        )
        .await;
    assert_eq!(r.status, 406);
    let r = srv
        .get(
            &format!("/dicom-web/studies/{}", study()),
            &[(
                "Accept",
                &format!(
                    "multipart/related; type=\"application/dicom\"; transfer-syntax={EXPLICIT_LE}"
                ),
            )],
        )
        .await;
    assert_eq!(r.status, 200);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retrieve_streams_large_instance() {
    let srv = TestServer::start(|c| c.storage.max_instance_bytes = 128 << 20).await;
    let file = dicom_sized(1, 1, 1, 1, 64 << 20);
    srv.stow(std::slice::from_ref(&file)).await;
    let sizes = chunk_sizes(srv.addr(), &format!("/dicom-web/studies/{}", study()), &[]).await;
    assert!(sizes.len() > 1, "{} chunks", sizes.len());
    assert!(sizes.iter().all(|&n| n <= (1 << 20) + 4096), "{sizes:?}");
    assert!(sizes.iter().sum::<usize>() > file.len());
    srv.stop().await;
}

/// GET and STOW-RS POST share `/dicom-web/studies/{study}`: GET answers with
/// CORS, preflight succeeds, POST keeps working and gets no CORS.
#[tokio::test]
async fn cors_preflight_on_study_path() {
    let srv = TestServer::start(|c| c.http.cors_allowed_origins = vec![ORIGIN.into()]).await;
    srv.stow(&[dicom(1, 1, 1, 1)]).await;
    let path = format!("/dicom-web/studies/{}", study());
    let r = http_options(
        srv.addr(),
        &path,
        &[
            ("Origin", ORIGIN),
            ("Access-Control-Request-Method", "GET"),
            ("Access-Control-Request-Headers", "accept"),
        ],
    )
    .await;
    assert_eq!(r.status, 200, "{:?}", r.headers);
    assert_eq!(r.header("access-control-allow-origin"), Some(ORIGIN));
    let r = srv.get(&path, &[("Origin", ORIGIN)]).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("access-control-allow-origin"), Some(ORIGIN));

    let r = stow(
        srv.addr(),
        &path,
        &[dicom(1, 1, 1, 2)],
        &[("Origin", ORIGIN)],
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("access-control-allow-origin"), None);
    srv.stop().await;
}
