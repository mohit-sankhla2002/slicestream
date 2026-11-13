//! Console endpoints: `/api/metrics`, `/api/metrics/history`,
//! `/api/console/series`, against the full server on an ephemeral port.

#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use serde_json::Value;
use slicestream_server::config::Config;
use slicestream_server::{Running, start};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

const SERIES: &str = "2.25.1000000000000000000000000000001";

struct Server {
    running: Running,
    shutdown: CancellationToken,
    phantom: Vec<PathBuf>,
    _dir: tempfile::TempDir,
}

impl Server {
    async fn start(configure: impl FnOnce(&mut Config)) -> Server {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("phantom");
        phantom::write_phantom(&data, 1, 32).unwrap();
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
        let shutdown = CancellationToken::new();
        let running = start(config, shutdown.clone()).await.unwrap();
        Server {
            running,
            shutdown,
            phantom,
            _dir: dir,
        }
    }

    fn addr(&self) -> SocketAddr {
        self.running.http_addr
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

/// HTTP/1.0 request, one per connection.
async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    origin: Option<&str>,
    ct: &str,
    body: &[u8],
) -> Reply {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let origin = origin
        .map(|o| format!("Origin: {o}\r\n"))
        .unwrap_or_default();
    let head = format!(
        "{method} {path} HTTP/1.0\r\nHost: {addr}\r\n{origin}Content-Type: {ct}\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
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

async fn get(addr: SocketAddr, path: &str) -> Reply {
    request(addr, "GET", path, None, "text/plain", b"").await
}

/// Poll `/api/metrics` until `ok` accepts the snapshot or `secs` pass.
async fn metrics_until(addr: SocketAddr, secs: u64, ok: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let r = get(addr, "/api/metrics").await;
        let v = r.json();
        if r.status == 200 && ok(&v) {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "timed out; last: {} {v}",
            r.status
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn upload(srv: &Server) {
    let bytes = std::fs::read(&srv.phantom[0]).unwrap();
    let r = request(
        srv.addr(),
        "POST",
        "/api/instances",
        None,
        "application/dicom",
        &bytes,
    )
    .await;
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
}

#[tokio::test]
async fn metrics_on_empty_storage() {
    let srv = Server::start(|_| {}).await;
    let v = metrics_until(srv.addr(), 2, |_| true).await;
    assert_eq!(v["storage"]["instances"], 0);
    assert_eq!(v["ingest"]["stored"], 0);
    assert!(v["ingest"]["latency_ms"]["p50"].is_null());
    assert!(v["process"]["rss_bytes"].as_u64().unwrap() > 0);
    assert_eq!(v["pid"], std::process::id());
    let r = get(srv.addr(), "/api/metrics").await;
    assert_eq!(r.header("cache-control").as_deref(), Some("no-store"));
    srv.stop().await;
}

#[tokio::test]
async fn metrics_count_stow_upload() {
    let srv = Server::start(|_| {}).await;
    upload(&srv).await;
    metrics_until(srv.addr(), 3, |v| v["ingest"]["stored"] == 1).await;
    metrics_until(srv.addr(), 6, |v| v["storage"]["instances"] == 1).await;
    srv.stop().await;
}

#[tokio::test]
async fn history_grows() {
    let srv = Server::start(|_| {}).await;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    let r = get(srv.addr(), "/api/metrics/history").await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("cache-control").as_deref(), Some("no-store"));
    let v = r.json();
    let items = v.as_array().unwrap();
    assert!(items.len() >= 2, "{v}");
    let times: Vec<u64> = items
        .iter()
        .map(|s| s["sampled_at_ms"].as_u64().unwrap())
        .collect();
    assert!(times.windows(2).all(|w| w[0] <= w[1]), "{times:?}");
    assert!(items.iter().all(|s| s.get("pid").is_none()));
    srv.stop().await;
}

#[tokio::test]
async fn console_series_lists_uploaded() {
    let srv = Server::start(|_| {}).await;
    upload(&srv).await;
    let r = get(srv.addr(), "/api/console/series").await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("cache-control").as_deref(), Some("no-store"));
    let v = r.json();
    let rows = v.as_array().unwrap();
    assert_eq!(rows.len(), 1, "{v}");
    assert_eq!(rows[0]["instance_count"], 1);
    srv.stop().await;
}

#[tokio::test]
async fn console_routes_have_no_cors() {
    let origin = "https://viewer.example.org";
    let srv = Server::start(|c| c.http.cors_allowed_origins = vec![origin.into()]).await;
    metrics_until(srv.addr(), 2, |_| true).await;
    for path in [
        "/api/metrics",
        "/api/metrics/history",
        "/api/console/series",
    ] {
        let r = request(srv.addr(), "GET", path, Some(origin), "text/plain", b"").await;
        assert_eq!(r.status, 200, "{path}");
        assert_eq!(r.header("access-control-allow-origin"), None, "{path}");
    }
    let r = request(
        srv.addr(),
        "GET",
        "/api/series",
        Some(origin),
        "text/plain",
        b"",
    )
    .await;
    assert_eq!(
        r.header("access-control-allow-origin").as_deref(),
        Some(origin)
    );
    srv.stop().await;
}

#[tokio::test]
async fn metrics_disabled_404() {
    let srv = Server::start(|c| c.http.metrics_enabled = false).await;
    for path in [
        "/api/metrics",
        "/api/metrics/history",
        "/api/console/series",
    ] {
        assert_eq!(get(srv.addr(), path).await.status, 404, "{path}");
    }
    srv.stop().await;
}

#[tokio::test]
async fn ws_stream_gauge() {
    let srv = Server::start(|_| {}).await;
    upload(&srv).await;
    let url = format!("ws://{}/api/series/{SERIES}/stream", srv.addr());
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    ws.next().await.unwrap().unwrap();
    metrics_until(srv.addr(), 3, |v| v["connections"]["ws_streams"] == 1).await;
    drop(ws);
    metrics_until(srv.addr(), 3, |v| v["connections"]["ws_streams"] == 0).await;
    srv.stop().await;
}
