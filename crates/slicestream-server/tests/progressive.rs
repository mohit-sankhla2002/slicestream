//! `GET /api/series/{uid}/stream?progressive=htj2k`: frames in parts (a
//! preview stage, then the rest), paced by the client's acknowledgements.
//! Flow assertions count bytes; the test client decides when to ack.

#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

mod support;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::serve::ListenerExt;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use slicestream_codec::{HTJ2K_RPCL, decode_full, preview_level, samples};
use slicestream_core::protocol::{FrameHeader, decode_frame, encode_frame};
use slicestream_core::{SchedulerConfig, Series};
use slicestream_server::config::{Htj2kConfig, Htj2kMode, HttpConfig};
use slicestream_server::{AppState, SeriesCatalog, app};
use slicestream_store::{Ingestor, StorageConfig};
use support::{IMAGE_SERIES, encapsulated_image, image_sop, native_image};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;

const PHANTOM_SERIES: &str = "2.25.1000000000000000000000000000001";
/// Frames of the noise instance, each 256 x 256.
const NOISE_FRAMES: u32 = 6;
const NOISE_SIZE: u16 = 256;
/// Small preview so 256 x 256 frames have a preview and a multi-part final.
const PREVIEW_SIZE: u32 = 64;
const PART_MAX: usize = 65_536;
/// Initial flow window (`slicestream_core::flow::INITIAL`).
const WINDOW: u64 = 262_144;
const RLE: &str = "1.2.840.10008.1.2.5";

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// The router of `app` on an ephemeral port, over a store of its own.
struct Harness {
    addr: SocketAddr,
    ingestor: Arc<Ingestor>,
    shutdown: CancellationToken,
    dir: tempfile::TempDir,
}

impl Harness {
    async fn start(mode: Htj2kMode) -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let ingestor = Ingestor::open(StorageConfig {
            root: dir.path().join("store"),
            ..StorageConfig::default()
        })
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let catalog = SeriesCatalog::new(Arc::clone(&ingestor));
        catalog.spawn_invalidator();
        let state = Arc::new(AppState {
            catalog,
            ingestor: Arc::clone(&ingestor),
            scheduler: SchedulerConfig::default(),
            http: HttpConfig {
                addr,
                web_dir: dir.path().join("web"),
                ..HttpConfig::default()
            },
            dicomweb: Default::default(),
            htj2k: Htj2kConfig {
                mode,
                preview_size: PREVIEW_SIZE,
                ..Htj2kConfig::default()
            },
            metrics: None,
            gauges: Default::default(),
            archives: None,
        });
        let shutdown = CancellationToken::new();
        let router = app(state);
        let listener = listener.tap_io(|tcp| {
            let _ = tcp.set_nodelay(true);
        });
        tokio::spawn(
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(shutdown.clone().cancelled_owned())
            .into_future(),
        );
        Harness {
            addr,
            ingestor,
            shutdown,
            dir,
        }
    }

    async fn import(&self, name: &str, bytes: &[u8]) {
        let path = self.dir.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        self.ingestor.import_file(&path).await.unwrap();
    }

    async fn import_phantom(&self, slices: usize) -> std::path::PathBuf {
        let src = self.dir.path().join("phantom");
        phantom::write_phantom(&src, slices, 32).unwrap();
        for i in 1..=slices {
            self.ingestor
                .import_file(&src.join(format!("IM{i:05}.dcm")))
                .await
                .unwrap();
        }
        src
    }

    /// One instance of `NOISE_FRAMES` frames of 12-bit noise, each
    /// `NOISE_SIZE` square; returns the pixel bytes of each frame.
    async fn import_noise(&self) -> Vec<Vec<u8>> {
        self.import_noise_sized(NOISE_FRAMES, NOISE_SIZE).await
    }

    async fn import_noise_sized(&self, frames: u32, size: u16) -> Vec<Vec<u8>> {
        let frame_len = size as usize * size as usize * 2;
        let pixels = noise(frames as usize * frame_len);
        let file = native_image(&image_sop(1), frames, size, size, 16, pixels.clone());
        self.import("noise.dcm", &file).await;
        pixels.chunks(frame_len).map(<[u8]>::to_vec).collect()
    }

    /// Make every copy now.
    async fn make_copies(&self) {
        self.ingestor.copy_batch(100, 2, || false).await.unwrap();
    }

    /// Codestream of the copy of frame `frame` of the only instance of
    /// `series`.
    async fn copy_codestream(&self, series: &str, frame: usize) -> Vec<u8> {
        let rows = self.ingestor.index().series_slices(series).await.unwrap();
        let (frames, path) = self
            .ingestor
            .copy_frames(rows[0].id, &rows[0].blob)
            .await
            .unwrap()
            .expect("a copy");
        let f = &frames.0[frame];
        let all = std::fs::read(path).unwrap();
        all[f.offset as usize..(f.offset + f.len) as usize].to_vec()
    }

    async fn connect(&self, series: &str, query: &str) -> (Ws, Value) {
        let url = format!("ws://{}/api/series/{series}/stream{query}", self.addr);
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let hello = match next(&mut ws).await.expect("hello") {
            Message::Text(t) => serde_json::from_str(&t).unwrap(),
            other => panic!("expected hello, got {other:?}"),
        };
        (ws, hello)
    }

    /// The HTTP status and body of a refused WebSocket handshake.
    async fn refused(&self, series: &str, query: &str) -> (u16, String) {
        let url = format!("ws://{}/api/series/{series}/stream{query}", self.addr);
        match tokio_tungstenite::connect_async(url).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(r)) => (
                r.status().as_u16(),
                String::from_utf8_lossy(r.body().as_deref().unwrap_or_default()).into_owned(),
            ),
            Err(e) => panic!("unexpected error {e}"),
            Ok(_) => panic!("handshake succeeded"),
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Deterministic 12-bit noise, little-endian 16-bit samples.
fn noise(len: usize) -> Vec<u8> {
    let mut x: u32 = 0x9E37_79B9;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        out.extend_from_slice(&((x & 0x0FFF) as u16).to_le_bytes());
    }
    out.truncate(len);
    out
}

/// Next message, skipping pings; `None` once the socket closes. Panics after
/// 30 s (a hang, not a threshold).
async fn next(ws: &mut Ws) -> Option<Message> {
    loop {
        let m = tokio::time::timeout(Duration::from_secs(30), ws.next())
            .await
            .expect("message within 30 s")?
            .ok()?;
        match m {
            Message::Ping(_) | Message::Pong(_) => continue,
            m => return Some(m),
        }
    }
}

/// Next binary message within `wait`, or `None`.
async fn binary_within(ws: &mut Ws, wait: Duration) -> Option<Vec<u8>> {
    match tokio::time::timeout(wait, next(ws)).await {
        Ok(Some(Message::Binary(b))) => Some(b.to_vec()),
        Ok(Some(other)) => panic!("unexpected message {other:?}"),
        Ok(None) | Err(_) => None,
    }
}

async fn binary(ws: &mut Ws) -> Vec<u8> {
    match next(ws).await.expect("socket open") {
        Message::Binary(b) => b.to_vec(),
        other => panic!("unexpected message {other:?}"),
    }
}

async fn send(ws: &mut Ws, json: Value) {
    ws.send(Message::Text(json.to_string().into()))
        .await
        .unwrap();
}

async fn cursor(ws: &mut Ws, index: usize) {
    send(
        ws,
        serde_json::json!({"type": "cursor", "index": index, "velocity": 0}),
    )
    .await;
}

async fn ack(ws: &mut Ws, bytes: u64) {
    send(ws, serde_json::json!({"type": "ack", "bytes": bytes})).await;
}

/// What a progressive client has received: it checks that parts arrive in
/// order and acks as the protocol asks (every 64 KiB and on every completed
/// frame) once `acking` is set.
struct Collected {
    /// Payload messages in arrival order.
    messages: Vec<(FrameHeader, Vec<u8>)>,
    /// Reassembled codestream (or whole payload) per completed index.
    frames: BTreeMap<usize, Vec<u8>>,
    partial: BTreeMap<usize, Vec<u8>>,
    received: u64,
    acked: u64,
    acking: bool,
}

impl Collected {
    fn new() -> Collected {
        Collected {
            messages: Vec::new(),
            frames: BTreeMap::new(),
            partial: BTreeMap::new(),
            received: 0,
            acked: 0,
            acking: true,
        }
    }

    /// Receive one binary message; returns its header and payload length.
    async fn receive(&mut self, ws: &mut Ws) -> (FrameHeader, u64) {
        let msg = binary(ws).await;
        let (header, payload) = decode_frame(&msg).expect("valid frame");
        self.received += payload.len() as u64;
        let complete = match header.part {
            None => {
                self.frames.insert(header.index, payload.to_vec());
                true
            }
            Some(part) => {
                let buf = self.partial.entry(header.index).or_default();
                assert_eq!(
                    part.offset,
                    buf.len() as u64,
                    "slice {}: parts arrive in order, without gaps or repeats",
                    header.index
                );
                buf.extend_from_slice(payload);
                if part.offset + payload.len() as u64 == part.total {
                    let whole = self.partial.remove(&header.index).unwrap();
                    self.frames.insert(header.index, whole);
                    true
                } else {
                    false
                }
            }
        };
        self.messages.push((header.clone(), payload.to_vec()));
        if self.acking && (complete || self.received - self.acked >= PART_MAX as u64) {
            ack(ws, self.received).await;
            self.acked = self.received;
        }
        (header, payload.len() as u64)
    }

    /// Receive until `slices` frames are complete.
    async fn until_complete(&mut self, ws: &mut Ws, slices: usize) {
        while self.frames.len() < slices {
            self.receive(ws).await;
        }
    }
}

/// Every message of a progressive stream until `slices` frames are complete.
async fn collect(ws: &mut Ws, slices: usize) -> Collected {
    let mut c = Collected::new();
    c.until_complete(ws, slices).await;
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_stream_unchanged() {
    let h = Harness::start(Htj2kMode::Background).await;
    let src = h.import_phantom(5).await;
    h.make_copies().await;
    let expected = &Series::scan_dir(&src).unwrap()[0];

    let (mut ws, hello) = h.connect(PHANTOM_SERIES, "").await;
    assert_eq!(hello["type"], "series");
    assert!(hello.get("progressive").is_none(), "{hello}");
    cursor(&mut ws, 2).await;
    let mut seen = Vec::new();
    for _ in 0..5 {
        let msg = binary(&mut ws).await;
        let (header, _) = decode_frame(&msg).unwrap();
        assert_eq!(header.part, None);
        assert!(!String::from_utf8_lossy(&msg).contains("\"part\""));
        let frame = expected.load_frame(header.index).unwrap();
        assert!(
            msg == encode_frame(&frame).to_vec(),
            "slice {}: message differs from encode_frame",
            header.index
        );
        seen.push(header.index);
    }
    assert_eq!(seen[0], 2);
    seen.sort();
    assert_eq!(seen, [0, 1, 2, 3, 4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn progressive_first_message_is_cursor_preview() {
    let h = Harness::start(Htj2kMode::Background).await;
    h.import_noise().await;
    h.make_copies().await;

    let (mut ws, hello) = h.connect(IMAGE_SERIES, "?progressive=htj2k").await;
    assert_eq!(hello["type"], "series");
    assert_eq!(hello["progressive"], "htj2k");
    cursor(&mut ws, 3).await;
    let msg = binary(&mut ws).await;
    let (header, payload) = decode_frame(&msg).unwrap();
    assert_eq!(header.index, 3);
    assert_eq!(header.info.transfer_syntax, HTJ2K_RPCL);
    let part = header.part.expect("a part");
    assert_eq!(part.offset, 0);
    let level = preview_level(NOISE_SIZE, NOISE_SIZE, PREVIEW_SIZE);
    assert_eq!(part.level as usize, level);
    assert!(
        (payload.len() as u64) < part.total,
        "the preview is a real prefix"
    );
    let copy = h.copy_codestream(IMAGE_SERIES, 3).await;
    assert_eq!(part.total, copy.len() as u64);
    assert!(
        payload == &copy[..payload.len()],
        "the preview is the copy's prefix"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn progressive_parts_reassemble_to_copy() {
    let h = Harness::start(Htj2kMode::Background).await;
    let pixels = h.import_noise().await;
    h.make_copies().await;

    let (mut ws, _) = h.connect(IMAGE_SERIES, "?progressive=htj2k").await;
    cursor(&mut ws, 0).await;
    let c = collect(&mut ws, NOISE_FRAMES as usize).await;
    let info = c.messages[0].0.info.clone();
    for (index, codestream) in &c.frames {
        assert!(
            *codestream == h.copy_codestream(IMAGE_SERIES, *index).await,
            "slice {index}: reassembled bytes differ from the copy"
        );
        assert!(
            decode_full(codestream).unwrap() == samples(&pixels[*index], &info).unwrap(),
            "slice {index}: decoded samples differ"
        );
    }
    // Levels rise within a frame and end at full resolution.
    for index in 0..NOISE_FRAMES as usize {
        let levels: Vec<u32> = c
            .messages
            .iter()
            .filter(|(h, _)| h.index == index)
            .map(|(h, _)| h.part.unwrap().level)
            .collect();
        assert!(levels.len() >= 2, "slice {index}: {levels:?}");
        assert!(levels.windows(2).all(|w| w[0] <= w[1]), "{levels:?}");
        assert_eq!(*levels.last().unwrap(), 2, "256 x 256 has 2 levels");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn progressive_parts_at_most_64k() {
    let h = Harness::start(Htj2kMode::Background).await;
    h.import_noise().await;
    h.make_copies().await;

    let (mut ws, _) = h.connect(IMAGE_SERIES, "?progressive=htj2k").await;
    cursor(&mut ws, 0).await;
    let c = collect(&mut ws, NOISE_FRAMES as usize).await;
    assert!(c.messages.iter().all(|(_, p)| p.len() <= PART_MAX));
    // The finals are long enough to need more than one part.
    let frame0 = c.messages.iter().filter(|(h, _)| h.index == 0).count();
    assert!(
        frame0 >= 3,
        "preview + at least two final parts, got {frame0}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn progressive_without_copy_uses_transient_encode() {
    let h = Harness::start(Htj2kMode::OnDemand).await;
    let pixels = h.import_noise().await;
    // A newer instance in another series: an unprioritized batch would
    // copy it first.
    h.import_phantom(1).await;

    let (mut ws, _) = h.connect(IMAGE_SERIES, "?progressive=htj2k").await;
    cursor(&mut ws, 0).await;
    let c = collect(&mut ws, NOISE_FRAMES as usize).await;
    assert!(c.messages.iter().all(|(h, _)| h.part.is_some()));
    let info = c.messages[0].0.info.clone();
    for (index, codestream) in &c.frames {
        assert!(
            decode_full(codestream).unwrap() == samples(&pixels[*index], &info).unwrap(),
            "slice {index}: decoded samples differ"
        );
    }
    drop(ws);

    let rows = h
        .ingestor
        .index()
        .series_slices(IMAGE_SERIES)
        .await
        .unwrap();
    assert!(
        h.ingestor
            .copy_frames(rows[0].id, &rows[0].blob)
            .await
            .unwrap()
            .is_none(),
        "the stream itself stores nothing"
    );
    assert_eq!(h.ingestor.copy_batch(1, 1, || false).await.unwrap(), 1);
    assert!(
        h.ingestor
            .copy_frames(rows[0].id, &rows[0].blob)
            .await
            .unwrap()
            .is_some(),
        "the streamed instance was copied first"
    );
    // The transient codestream is the one the copy holds.
    assert!(c.frames[&0] == h.copy_codestream(IMAGE_SERIES, 0).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ineligible_slices_sent_whole() {
    let h = Harness::start(Htj2kMode::Background).await;
    let frames: Vec<Vec<Vec<u8>>> = (0..3u8)
        .map(|k| vec![vec![k; 10], vec![k + 100; 6]])
        .collect();
    h.import(
        "rle.dcm",
        &encapsulated_image(&image_sop(1), &frames, true, RLE),
    )
    .await;
    h.make_copies().await;

    let (mut ws, hello) = h.connect(IMAGE_SERIES, "?progressive=htj2k").await;
    assert_eq!(hello["progressive"], "htj2k");
    cursor(&mut ws, 1).await;
    let c = collect(&mut ws, 3).await;
    assert_eq!(c.messages.len(), 3);
    assert_eq!(c.messages[0].0.index, 1);
    for (header, payload) in &c.messages {
        assert_eq!(header.part, None);
        assert_eq!(header.info.transfer_syntax, RLE);
        assert!(
            *payload == frames[header.index].concat(),
            "slice {}",
            header.index
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn progressive_stream_stops_after_window_without_acks() {
    let h = Harness::start(Htj2kMode::Background).await;
    h.import_noise().await;
    h.make_copies().await;
    let mut total = 0;
    for frame in 0..NOISE_FRAMES as usize {
        total += h.copy_codestream(IMAGE_SERIES, frame).await.len() as u64;
    }
    assert!(total > 2 * WINDOW, "the series outsizes the window");

    let (mut ws, _) = h.connect(IMAGE_SERIES, "?progressive=htj2k").await;
    cursor(&mut ws, 0).await;
    let mut received = 0u64;
    while let Some(msg) = binary_within(&mut ws, Duration::from_millis(500)).await {
        received += decode_frame(&msg).unwrap().1.len() as u64;
        assert!(received <= WINDOW, "{received} bytes unacknowledged");
    }
    assert!(
        received > WINDOW / 2,
        "only {received} bytes before the stop"
    );

    // An ack opens the window again.
    ack(&mut ws, received).await;
    assert!(
        binary_within(&mut ws, Duration::from_secs(10))
            .await
            .is_some()
    );
}

/// A slow reader jumps while slice 0's final stage (several parts at
/// 512 x 512) is under way: the jumped slice's preview comes before more
/// than one window of other bytes, and slice 0 later resumes where it
/// stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursor_jump_served_within_one_window() {
    const FRAMES: u32 = 3;
    let h = Harness::start(Htj2kMode::Background).await;
    h.import_noise_sized(FRAMES, 512).await;
    h.make_copies().await;
    let jump = FRAMES as usize - 1;

    let (mut ws, _) = h.connect(IMAGE_SERIES, "?progressive=htj2k").await;
    cursor(&mut ws, 0).await;
    // Read the first 64 KiB without acknowledging anything.
    let mut c = Collected::new();
    c.acking = false;
    while c.received < PART_MAX as u64 {
        let (header, _) = c.receive(&mut ws).await;
        assert_eq!(header.index, 0);
    }
    let before_jump = c.received;
    cursor(&mut ws, jump).await;
    ack(&mut ws, c.received).await;
    c.acked = c.received;
    c.acking = true;
    // Old priorities still on their way: at most what the window let out.
    loop {
        let (header, len) = c.receive(&mut ws).await;
        if header.index == jump {
            assert_eq!(header.part.unwrap().offset, 0, "the preview comes first");
            break;
        }
        assert!(
            c.received - len <= WINDOW,
            "{} bytes of old priorities after the jump ({before_jump} before)",
            c.received - len - before_jump
        );
    }
    assert!(
        c.partial.contains_key(&0),
        "slice 0 was interrupted mid-final"
    );
    c.until_complete(&mut ws, FRAMES as usize).await;
    for (index, codestream) in &c.frames {
        assert!(
            *codestream == h.copy_codestream(IMAGE_SERIES, *index).await,
            "slice {index}: reassembled bytes differ from the copy"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_progressive_value_is_400() {
    let h = Harness::start(Htj2kMode::Background).await;
    h.import_phantom(1).await;
    let (status, _) = h.refused(PHANTOM_SERIES, "?progressive=jpeg").await;
    assert_eq!(status, 400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn off_mode_ignores_progressive() {
    let h = Harness::start(Htj2kMode::Off).await;
    h.import_phantom(1).await;
    let (status, body) = h.refused(PHANTOM_SERIES, "?progressive=htj2k").await;
    assert_eq!(status, 400);
    assert!(body.contains("[htj2k] mode = \"off\""), "{body}");
    // Without the parameter the stream works as always.
    let (mut ws, hello) = h.connect(PHANTOM_SERIES, "").await;
    assert_eq!(hello["type"], "series");
    cursor(&mut ws, 0).await;
    assert!(
        decode_frame(&binary(&mut ws).await)
            .unwrap()
            .0
            .part
            .is_none()
    );
}
