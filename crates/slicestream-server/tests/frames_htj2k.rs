//! WADO-RS frames as HTJ2K copies, and HTTP `Range` on them, against the
//! router on an ephemeral port. Instances are synthesized noise or the
//! phantom (no real patient data).

#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

mod support;

use std::net::SocketAddr;
use std::sync::Arc;

use slicestream_codec::{HTJ2K_RPCL, decode_full, samples};
use slicestream_core::{FrameInfo, SchedulerConfig};
use slicestream_server::config::{Htj2kConfig, Htj2kMode, HttpConfig};
use slicestream_server::{AppState, SeriesCatalog, app};
use slicestream_store::{Ingestor, StorageConfig};
use support::{IMAGE_SERIES, IMAGE_STUDY, Reply, http_get, image_sop, native_image, parts};
use tokio_util::sync::CancellationToken;

const EXPLICIT_LE: &str = "1.2.840.10008.1.2.1";
const SIZE: u16 = 128;
const FRAMES: u32 = 2;
const ORIGIN: &str = "https://viewer.example";
const JPHC: &str = "multipart/related; type=\"image/jphc\"";
const OHIF: &str = "multipart/related; type=application/octet-stream; transfer-syntax=*";

struct Options {
    mode: Htj2kMode,
    prefer_for_wildcard: bool,
}

const BACKGROUND: Options = Options {
    mode: Htj2kMode::Background,
    prefer_for_wildcard: false,
};

/// The router of `app` on an ephemeral port, over a store of its own. No
/// copy worker runs: tests make copies with `make_copies`.
struct Harness {
    addr: SocketAddr,
    ingestor: Arc<Ingestor>,
    shutdown: CancellationToken,
    dir: tempfile::TempDir,
}

impl Harness {
    async fn start(opts: Options) -> Harness {
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
        let state = Arc::new(AppState {
            catalog,
            ingestor: Arc::clone(&ingestor),
            scheduler: SchedulerConfig::default(),
            http: HttpConfig {
                addr,
                web_dir: dir.path().join("web"),
                cors_allowed_origins: vec![ORIGIN.into()],
                ..HttpConfig::default()
            },
            dicomweb: Default::default(),
            htj2k: Htj2kConfig {
                mode: opts.mode,
                prefer_for_wildcard: opts.prefer_for_wildcard,
                ..Htj2kConfig::default()
            },
            metrics: None,
            gauges: Default::default(),
            archives: None,
        });
        let shutdown = CancellationToken::new();
        tokio::spawn(
            axum::serve(listener, app(state))
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

    /// One instance of `FRAMES` frames of 12-bit noise; the pixel bytes of
    /// each frame.
    async fn import_noise(&self) -> Vec<Vec<u8>> {
        let frame_len = SIZE as usize * SIZE as usize * 2;
        let pixels = noise(FRAMES as usize * frame_len);
        let file = native_image(&image_sop(1), FRAMES, SIZE, SIZE, 16, pixels.clone());
        self.import("noise.dcm", &file).await;
        pixels.chunks(frame_len).map(<[u8]>::to_vec).collect()
    }

    /// One phantom slice, in a series of its own.
    async fn import_phantom(&self) {
        let src = self.dir.path().join("phantom");
        phantom::write_phantom(&src, 1, 32).unwrap();
        self.ingestor
            .import_file(&src.join("IM00001.dcm"))
            .await
            .unwrap();
    }

    async fn make_copies(&self) {
        self.ingestor.copy_batch(100, 2, || false).await.unwrap();
    }

    /// The copy's codestream of frame `frame` (0-based) of the noise
    /// instance; `None` without a copy.
    async fn copy_codestream(&self, frame: usize) -> Option<Vec<u8>> {
        let rows = self
            .ingestor
            .index()
            .series_slices(IMAGE_SERIES)
            .await
            .unwrap();
        let (frames, path) = self
            .ingestor
            .copy_frames(rows[0].id, &rows[0].blob)
            .await
            .unwrap()?;
        let f = &frames.0[frame];
        let all = std::fs::read(path).unwrap();
        Some(all[f.offset as usize..(f.offset + f.len) as usize].to_vec())
    }

    async fn get(&self, list: &str, headers: &[(&str, &str)]) -> Reply {
        let path = format!(
            "/dicom-web/studies/{IMAGE_STUDY}/series/{IMAGE_SERIES}/instances/{}/frames/{list}",
            image_sop(1)
        );
        http_get(self.addr, &path, headers).await
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

/// The noise instance's frame attributes.
fn noise_info() -> FrameInfo {
    FrameInfo {
        rows: SIZE,
        columns: SIZE,
        bits_allocated: 16,
        bits_stored: 16,
        samples_per_pixel: 1,
        pixel_representation: 0,
        photometric_interpretation: "MONOCHROME2".into(),
        transfer_syntax: EXPLICIT_LE.into(),
        rescale_slope: 1.0,
        rescale_intercept: 0.0,
        window_center: None,
        window_width: None,
    }
}

fn jphc_type() -> String {
    format!("image/jphc; transfer-syntax={HTJ2K_RPCL}")
}

/// The one part of a multipart answer, checked to be an HTJ2K copy part.
fn one_part(r: &Reply) -> Vec<u8> {
    let mut p = parts(r);
    assert_eq!(p.len(), 1);
    let (ct, body) = p.remove(0);
    assert_eq!(ct, jphc_type());
    body
}

fn decodes_to(codestream: &[u8], pixels: &[u8]) -> bool {
    decode_full(codestream).unwrap() == samples(pixels, &noise_info()).unwrap()
}

#[tokio::test]
async fn jphc_frames_decode_to_original() {
    let h = Harness::start(BACKGROUND).await;
    let pixels = h.import_noise().await;
    h.make_copies().await;

    let r = h.get("1", &[("Accept", JPHC)]).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("accept-ranges"), Some("bytes"));
    let body = one_part(&r);
    assert!(decodes_to(&body, &pixels[0]), "decoded samples differ");
    assert!(
        body == h.copy_codestream(0).await.unwrap(),
        "served from the copy"
    );

    // Several frames, in request order.
    let r = h.get("2,1", &[("Accept", JPHC)]).await;
    assert_eq!(r.status, 200, "{}", r.text());
    let p = parts(&r);
    assert_eq!(p.len(), 2);
    assert!(p.iter().all(|(ct, _)| *ct == jphc_type()));
    assert!(decodes_to(&p[0].1, &pixels[1]));
    assert!(decodes_to(&p[1].1, &pixels[0]));

    // Bare single part, and octet-stream in the copy's syntax.
    let r = h.get("2", &[("Accept", "image/jphc")]).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("content-type"), Some(jphc_type().as_str()));
    assert!(decodes_to(&r.body, &pixels[1]));
    let octet = format!("application/octet-stream; transfer-syntax={HTJ2K_RPCL}");
    let r = h.get("1", &[("Accept", &octet)]).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("content-type"), Some(octet.as_str()));
    assert!(decodes_to(&r.body, &pixels[0]));
}

#[tokio::test]
async fn range_206_with_content_range() {
    let h = Harness::start(BACKGROUND).await;
    h.import_noise().await;
    h.make_copies().await;
    let full = h.copy_codestream(0).await.unwrap();
    let len = full.len();
    assert!(len > 1024, "codestream of {len} bytes");

    let r = h
        .get("1", &[("Accept", JPHC), ("Range", "bytes=0-1023")])
        .await;
    assert_eq!(r.status, 206, "{}", r.text());
    assert_eq!(
        r.header("content-range"),
        Some(format!("bytes 0-1023/{len}").as_str())
    );
    assert_eq!(r.header("accept-ranges"), Some("bytes"));
    assert!(one_part(&r) == full[..1024]);

    // Bare single part: `first-` and a suffix.
    let r = h
        .get("1", &[("Accept", "image/jphc"), ("Range", "bytes=100-")])
        .await;
    assert_eq!(r.status, 206, "{}", r.text());
    assert_eq!(
        r.header("content-range"),
        Some(format!("bytes 100-{}/{len}", len - 1).as_str())
    );
    assert_eq!(r.header("content-type"), Some(jphc_type().as_str()));
    assert_eq!(
        r.header("content-length"),
        Some((len - 100).to_string().as_str())
    );
    assert!(r.body == full[100..]);
    let r = h
        .get("1", &[("Accept", "image/jphc"), ("Range", "bytes=-10")])
        .await;
    assert_eq!(r.status, 206);
    assert!(r.body == full[len - 10..]);
}

#[tokio::test]
async fn range_416() {
    let h = Harness::start(BACKGROUND).await;
    h.import_noise().await;
    h.make_copies().await;
    let len = h.copy_codestream(0).await.unwrap().len();

    let range = format!("bytes={len}-");
    let r = h.get("1", &[("Accept", JPHC), ("Range", &range)]).await;
    assert_eq!(r.status, 416, "{}", r.text());
    assert_eq!(
        r.header("content-range"),
        Some(format!("bytes */{len}").as_str())
    );
}

#[tokio::test]
async fn several_ranges_ignored() {
    let h = Harness::start(BACKGROUND).await;
    h.import_noise().await;
    h.make_copies().await;
    let full = h.copy_codestream(0).await.unwrap();

    for range in ["bytes=0-1,4-5", "bytes=5-2", "items=0-1"] {
        let r = h.get("1", &[("Accept", JPHC), ("Range", range)]).await;
        assert_eq!(r.status, 200, "{range}: {}", r.text());
        assert_eq!(r.header("content-range"), None);
        assert!(one_part(&r) == full, "{range}");
    }
}

#[tokio::test]
async fn range_on_frame_list_ignored() {
    let h = Harness::start(BACKGROUND).await;
    h.import_noise().await;
    h.make_copies().await;

    let r = h
        .get("1,2", &[("Accept", JPHC), ("Range", "bytes=0-9")])
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("content-range"), None);
    let p = parts(&r);
    assert!(p[0].1 == h.copy_codestream(0).await.unwrap());
    assert!(p[1].1 == h.copy_codestream(1).await.unwrap());
}

/// Frames served as stored ignore `Range` and advertise no ranges.
#[tokio::test]
async fn range_on_stored_frames_ignored() {
    let h = Harness::start(BACKGROUND).await;
    let pixels = h.import_noise().await;
    h.make_copies().await;

    let r = h
        .get(
            "1",
            &[
                (
                    "Accept",
                    "multipart/related; type=\"application/octet-stream\"",
                ),
                ("Range", "bytes=0-9"),
            ],
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("content-range"), None);
    assert_eq!(r.header("accept-ranges"), None);
    assert!(parts(&r)[0].1 == pixels[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_copy_is_encoded_on_demand() {
    let h = Harness::start(Options {
        mode: Htj2kMode::OnDemand,
        prefer_for_wildcard: false,
    })
    .await;
    let pixels = h.import_noise().await;
    // A newer instance in another series: an unprioritized batch would
    // copy it first.
    h.import_phantom().await;

    let r = h.get("2", &[("Accept", JPHC)]).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("accept-ranges"), Some("bytes"));
    let served = one_part(&r);
    assert!(decodes_to(&served, &pixels[1]));
    let r = h
        .get("2", &[("Accept", JPHC), ("Range", "bytes=0-99")])
        .await;
    assert_eq!(r.status, 206, "{}", r.text());
    assert!(one_part(&r) == served[..100]);
    assert!(h.copy_codestream(0).await.is_none(), "nothing was stored");

    // Prioritized: a batch of one copies this instance, not the newest.
    assert_eq!(h.ingestor.copy_batch(1, 1, || false).await.unwrap(), 1);
    let copy = h
        .copy_codestream(1)
        .await
        .expect("the instance was copied first");
    assert!(served == copy, "the transient codestream is the copy's");
}

#[tokio::test]
async fn cors_exposes_content_range() {
    let h = Harness::start(BACKGROUND).await;
    h.import_noise().await;
    h.make_copies().await;

    let r = h
        .get(
            "1",
            &[
                ("Origin", ORIGIN),
                ("Accept", JPHC),
                ("Range", "bytes=0-1023"),
            ],
        )
        .await;
    assert_eq!(r.status, 206, "{}", r.text());
    assert_eq!(r.header("access-control-allow-origin"), Some(ORIGIN));
    let exposed = r
        .header("access-control-expose-headers")
        .unwrap()
        .to_ascii_lowercase();
    let exposed: Vec<&str> = exposed.split(',').map(str::trim).collect();
    assert!(exposed.contains(&"content-range"), "{exposed:?}");
    assert!(exposed.contains(&"accept-ranges"), "{exposed:?}");
}

#[tokio::test]
async fn wildcard_follows_prefer_for_wildcard() {
    let stored = format!("application/octet-stream; transfer-syntax={EXPLICIT_LE}");
    let h = Harness::start(BACKGROUND).await;
    let pixels = h.import_noise().await;
    h.make_copies().await;
    let r = h.get("1", &[("Accept", OHIF)]).await;
    let p = parts(&r);
    assert_eq!(p[0].0, stored);
    assert!(p[0].1 == pixels[0]);
    assert_eq!(r.header("accept-ranges"), None);
    let r = h.get("1", &[]).await;
    assert_eq!(parts(&r)[0].0, stored);

    let h = Harness::start(Options {
        mode: Htj2kMode::Background,
        prefer_for_wildcard: true,
    })
    .await;
    let pixels = h.import_noise().await;
    h.make_copies().await;
    let r = h.get("1", &[("Accept", OHIF)]).await;
    let p = parts(&r);
    assert_eq!(
        p[0].0,
        format!("application/octet-stream; transfer-syntax={HTJ2K_RPCL}")
    );
    assert!(decodes_to(&p[0].1, &pixels[0]));
    assert_eq!(r.header("accept-ranges"), Some("bytes"));
    let r = h.get("1", &[]).await;
    let p = parts(&r);
    assert_eq!(p[0].0, jphc_type());
    assert!(decodes_to(&p[0].1, &pixels[0]));
}

#[tokio::test]
async fn jphc_with_copies_off_is_406() {
    let h = Harness::start(Options {
        mode: Htj2kMode::Off,
        prefer_for_wildcard: true,
    })
    .await;
    let pixels = h.import_noise().await;
    let r = h.get("1", &[("Accept", JPHC)]).await;
    assert_eq!(r.status, 406, "{}", r.text());
    // Wildcards are served as stored.
    let r = h.get("1", &[("Accept", OHIF)]).await;
    assert!(parts(&r)[0].1 == pixels[0]);
}
