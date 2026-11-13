//! Archive job API: `POST /api/archives` and `GET /api/archives/{job}`,
//! against the full server on an ephemeral port. Fixtures are phantoms.

#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

use std::io::{Cursor, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::Value;
use slicestream_server::config::Config;
use slicestream_server::{Running, start};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use zip::write::{SimpleFileOptions, ZipWriter};

struct Server {
    running: Running,
    shutdown: CancellationToken,
    phantom: Vec<PathBuf>,
    store: PathBuf,
    _dir: tempfile::TempDir,
}

impl Server {
    async fn start(configure: impl FnOnce(&mut Config)) -> Server {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("phantom");
        phantom::write_phantom(&data, 4, 16).unwrap();
        let mut phantom: Vec<PathBuf> = std::fs::read_dir(&data)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        phantom.sort();
        let mut config = Config::default();
        config.storage.root = dir.path().join("store");
        config.http.addr = "127.0.0.1:0".parse().unwrap();
        config.http.web_dir = dir.path().join("web");
        config.dimse.enabled = false;
        configure(&mut config);
        let store = config.storage.root.clone();
        let shutdown = CancellationToken::new();
        let running = start(config, shutdown.clone()).await.unwrap();
        Server {
            running,
            shutdown,
            phantom,
            store,
            _dir: dir,
        }
    }

    fn addr(&self) -> SocketAddr {
        self.running.http_addr
    }

    /// The phantom slices zipped.
    fn zip(&self) -> Vec<u8> {
        let mut w = ZipWriter::new(Cursor::new(Vec::new()));
        for p in &self.phantom {
            let name = p.file_name().unwrap().to_str().unwrap();
            w.start_file(format!("scan/{name}"), SimpleFileOptions::default())
                .unwrap();
            w.write_all(&std::fs::read(p).unwrap()).unwrap();
        }
        w.finish().unwrap().into_inner()
    }

    async fn stop(self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(10), self.running.task)
            .await
            .expect("server stops promptly")
            .unwrap()
            .unwrap();
    }
}

struct Reply {
    status: u16,
    head: String,
    body: Vec<u8>,
}

impl Reply {
    fn header(&self, name: &str) -> Option<String> {
        self.head.lines().skip(1).find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case(name)
                .then(|| v.trim().to_owned())
        })
    }

    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }
}

/// HTTP/1.0 request, one per connection, with extra header lines.
async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Reply {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let mut head = format!(
        "{method} {path} HTTP/1.0\r\nHost: {addr}\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    s.write_all(head.as_bytes()).await.unwrap();
    s.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response head");
    let head = String::from_utf8_lossy(&buf[..split]).into_owned();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    Reply {
        status,
        head,
        body: buf[split + 4..].to_vec(),
    }
}

async fn post(srv: &Server, headers: &[(&str, &str)], body: &[u8]) -> Reply {
    request(srv.addr(), "POST", "/api/archives", headers, body).await
}

/// POST and expect 202; returns the job id.
async fn submit(srv: &Server, headers: &[(&str, &str)], body: &[u8]) -> String {
    let r = post(srv, headers, body).await;
    assert_eq!(r.status, 202, "{}", String::from_utf8_lossy(&r.body));
    let v = r.json();
    assert!(v["upload_id"].as_i64().unwrap() > 0, "{v}");
    let job = v["job"].as_str().unwrap().to_owned();
    assert_eq!(job.len(), 32, "{job}");
    assert!(job.bytes().all(|b| b.is_ascii_hexdigit()), "{job}");
    job
}

/// Poll the job until it is no longer queued/running (≤ 10 s).
async fn finished(srv: &Server, job: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let r = request(srv.addr(), "GET", &format!("/api/archives/{job}"), &[], b"").await;
        assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
        assert_eq!(r.header("cache-control").as_deref(), Some("no-store"));
        let v = r.json();
        match v["status"].as_str().unwrap() {
            "queued" | "running" => {}
            _ => return v,
        }
        assert!(Instant::now() < deadline, "timed out; last: {v}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Files left in `tmp/` (spooled bodies must be removed).
fn tmp_files(store: &Path) -> usize {
    std::fs::read_dir(store.join("tmp")).unwrap().count()
}

/// NIfTI-1 single file: 348-byte header, extension flag, 8 data bytes.
fn nifti() -> Vec<u8> {
    let mut v = vec![0u8; 352];
    v[0..4].copy_from_slice(&348i32.to_le_bytes());
    let dims: [i16; 8] = [3, 2, 2, 2, 1, 1, 1, 1];
    for (i, d) in dims.iter().enumerate() {
        v[40 + i * 2..42 + i * 2].copy_from_slice(&d.to_le_bytes());
    }
    v[70..72].copy_from_slice(&2i16.to_le_bytes());
    v[72..74].copy_from_slice(&8i16.to_le_bytes());
    v[108..112].copy_from_slice(&352f32.to_le_bytes());
    v[344..348].copy_from_slice(b"n+1\0");
    v.extend_from_slice(&[9; 8]);
    v
}

#[tokio::test]
async fn post_zip_returns_202_and_job_completes() {
    let srv = Server::start(|_| {}).await;
    let job = submit(&srv, &[("x-filename", "scan.zip")], &srv.zip()).await;
    let v = finished(&srv, &job).await;
    assert_eq!(v["status"], "done", "{v}");
    assert_eq!(v["reason"], Value::Null);
    assert_eq!(v["name"], "scan.zip");
    assert!(v["upload_id"].as_i64().unwrap() > 0);
    assert_eq!(v["counts"]["dicom_stored"], 4, "{v}");
    assert_eq!(v["counts"]["failed"], 0, "{v}");
    assert_eq!(v["entries_seen"], 4, "{v}");
    assert!(v["bytes_expanded"].as_u64().unwrap() > 0);
    let entries = v["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 4);
    let path = entries[0]["path"].as_str().unwrap();
    assert!(path.starts_with("scan.zip!/scan/"), "{path}");
    assert_eq!(entries[0]["kind"], "dicom");
    assert_eq!(entries[0]["outcome"], "stored");
    let patients = v["patients"].as_array().unwrap();
    assert_eq!(patients.len(), 1, "{v}");
    assert_eq!(patients[0]["studies"][0]["instances"], 4);
    assert_eq!(v["nifti"]["linked"], 0);
    assert_eq!(tmp_files(&srv.store), 0);
    srv.stop().await;
}

#[tokio::test]
async fn post_bare_nifti_one_entry_job() {
    let srv = Server::start(|_| {}).await;
    let job = submit(&srv, &[("x-filename", "brain.nii")], &nifti()).await;
    let v = finished(&srv, &job).await;
    assert_eq!(v["status"], "done", "{v}");
    assert_eq!(v["counts"]["nifti_stored"], 1, "{v}");
    let entries = v["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["kind"], "nifti");
    assert_eq!(v["nifti"]["unassigned"], 1, "{v}");
    assert_eq!(tmp_files(&srv.store), 0);
    srv.stop().await;
}

#[tokio::test]
async fn post_junk_400() {
    let srv = Server::start(|_| {}).await;
    let r = post(&srv, &[], &[b'x'; 2000]).await;
    assert_eq!(r.status, 400);
    assert_eq!(r.json()["error"], "unsupported file type");
    assert_eq!(tmp_files(&srv.store), 0);
    srv.stop().await;
}

#[tokio::test]
async fn post_requires_token_when_set() {
    let srv = Server::start(|c| c.http.stow_token = Some("s3cret".into())).await;
    let body = nifti();
    assert_eq!(post(&srv, &[], &body).await.status, 401);
    let r = post(&srv, &[("Authorization", "Bearer wrong")], &body).await;
    assert_eq!(r.status, 401);
    let job = submit(&srv, &[("Authorization", "Bearer s3cret")], &body).await;
    assert_eq!(finished(&srv, &job).await["status"], "done");
    srv.stop().await;
}

#[tokio::test]
async fn post_too_large_413() {
    let srv = Server::start(|c| c.http.max_request_bytes = 100).await;
    assert_eq!(post(&srv, &[], &nifti()).await.status, 413);
    assert_eq!(tmp_files(&srv.store), 0);
    srv.stop().await;
}

#[tokio::test]
async fn post_low_disk_507() {
    let srv = Server::start(|c| c.archives.min_free_bytes = u64::MAX / 2).await;
    assert_eq!(post(&srv, &[], &nifti()).await.status, 507);
    srv.stop().await;
}

#[tokio::test]
async fn unknown_job_404() {
    let srv = Server::start(|_| {}).await;
    for job in ["0123456789abcdef0123456789abcdef", "nope"] {
        let r = request(srv.addr(), "GET", &format!("/api/archives/{job}"), &[], b"").await;
        assert_eq!(r.status, 404, "{job}");
    }
    srv.stop().await;
}

#[tokio::test]
async fn archives_disabled_404() {
    let srv = Server::start(|c| c.archives.enabled = false).await;
    assert_eq!(post(&srv, &[], &nifti()).await.status, 404);
    let r = request(
        srv.addr(),
        "GET",
        "/api/archives/0123456789abcdef0123456789abcdef",
        &[],
        b"",
    )
    .await;
    assert_eq!(r.status, 404);
    srv.stop().await;
}

#[tokio::test]
async fn no_cors_on_archive_routes() {
    let origin = "https://viewer.example.org";
    let srv = Server::start(|c| c.http.cors_allowed_origins = vec![origin.into()]).await;
    let r = post(&srv, &[("Origin", origin)], &nifti()).await;
    assert_eq!(r.status, 202);
    assert_eq!(r.header("access-control-allow-origin"), None);
    let job = r.json()["job"].as_str().unwrap().to_owned();
    let r = request(
        srv.addr(),
        "GET",
        &format!("/api/archives/{job}"),
        &[("Origin", origin)],
        b"",
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("access-control-allow-origin"), None);
    // Preflight gets no CORS approval either.
    let r = request(
        srv.addr(),
        "OPTIONS",
        "/api/archives",
        &[
            ("Origin", origin),
            ("Access-Control-Request-Method", "POST"),
        ],
        b"",
    )
    .await;
    assert_eq!(r.header("access-control-allow-origin"), None);
    srv.stop().await;
}

#[tokio::test]
async fn job_eviction_keeps_retention() {
    let srv = Server::start(|c| c.archives.job_retention = 1).await;
    let first = submit(&srv, &[], &nifti()).await;
    finished(&srv, &first).await;
    let second = submit(&srv, &[], &srv.zip()).await;
    finished(&srv, &second).await;
    let r = request(
        srv.addr(),
        "GET",
        &format!("/api/archives/{first}"),
        &[],
        b"",
    )
    .await;
    assert_eq!(r.status, 404);
    let r = request(
        srv.addr(),
        "GET",
        &format!("/api/archives/{second}"),
        &[],
        b"",
    )
    .await;
    assert_eq!(r.status, 200);
    srv.stop().await;
}

#[tokio::test]
async fn x_filename_sanitized() {
    let srv = Server::start(|_| {}).await;
    let job = submit(
        &srv,
        &[("x-filename", "C:\\Users\\x/../../etc/sc\tan.nii")],
        &nifti(),
    )
    .await;
    assert_eq!(finished(&srv, &job).await["name"], "scan.nii");

    let long = "a".repeat(300);
    let job = submit(&srv, &[("x-filename", &long)], &nifti()).await;
    assert_eq!(finished(&srv, &job).await["name"], "a".repeat(255));

    let job = submit(&srv, &[], &nifti()).await;
    assert_eq!(finished(&srv, &job).await["name"], "upload");
    let job = submit(&srv, &[("x-filename", "dir/")], &nifti()).await;
    assert_eq!(finished(&srv, &job).await["name"], "upload");
    srv.stop().await;
}
