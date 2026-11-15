//! Fixtures shared by the integration tests: synthesized DICOM, NIfTI and
//! archives (no real patient data), a server on an ephemeral port and a
//! minimal HTTP client.
#![allow(dead_code)]

use std::io::{Cursor, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use dicom_core::value::PixelFragmentSequence;
use dicom_core::{DataElement, PrimitiveValue, Tag, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_object::{FileMetaTableBuilder, InMemDicomObject};
use flate2::Compression;
use flate2::write::GzEncoder;
use slicestream_server::config::Config;
use slicestream_server::{Running, start};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use zip::write::{SimpleFileOptions, ZipWriter};

/// Part-10 CT image of patient `patient` (`P<patient>`), study `study`,
/// series `series`, instance `inst`. UIDs are derived from those numbers.
pub fn dicom(patient: u32, study: u32, series: u32, inst: u32) -> Vec<u8> {
    dicom_sized(patient, study, series, inst, 32)
}

/// [`dicom`] with `pixel_bytes` of pixel data.
pub fn dicom_sized(
    patient: u32,
    study: u32,
    series: u32,
    inst: u32,
    pixel_bytes: usize,
) -> Vec<u8> {
    dicom_build(patient, study, series, inst, pixel_bytes, &[])
}

/// [`dicom`] with extra string attributes; an attribute the fixture already
/// has is replaced.
pub fn dicom_with(
    patient: u32,
    study: u32,
    series: u32,
    inst: u32,
    extra: &[(Tag, VR, &str)],
) -> Vec<u8> {
    dicom_build(patient, study, series, inst, 32, extra)
}

fn dicom_build(
    patient: u32,
    study: u32,
    series: u32,
    inst: u32,
    pixel_bytes: usize,
    extra: &[(Tag, VR, &str)],
) -> Vec<u8> {
    let study_uid = format!("2.25.9{patient}0{study}");
    let series_uid = format!("{study_uid}.{series}");
    let sop_uid = format!("{series_uid}.{inst}");
    let s = |v: &str| PrimitiveValue::from(v);
    let mut obj = InMemDicomObject::from_element_iter([
        DataElement::new(tags::SOP_CLASS_UID, VR::UI, s(uids::CT_IMAGE_STORAGE)),
        DataElement::new(tags::SOP_INSTANCE_UID, VR::UI, s(&sop_uid)),
        DataElement::new(tags::PATIENT_ID, VR::LO, s(&format!("P{patient}"))),
        DataElement::new(
            tags::PATIENT_NAME,
            VR::PN,
            s(&format!("TEST^PATIENT{patient}")),
        ),
        DataElement::new(tags::STUDY_INSTANCE_UID, VR::UI, s(&study_uid)),
        DataElement::new(tags::STUDY_DATE, VR::DA, s("20260101")),
        DataElement::new(
            tags::STUDY_DESCRIPTION,
            VR::LO,
            s(&format!("Study {study}")),
        ),
        DataElement::new(tags::SERIES_INSTANCE_UID, VR::UI, s(&series_uid)),
        DataElement::new(tags::MODALITY, VR::CS, s("CT")),
        DataElement::new(tags::INSTANCE_NUMBER, VR::IS, s(&inst.to_string())),
        DataElement::new(tags::SAMPLES_PER_PIXEL, VR::US, PrimitiveValue::from(1_u16)),
        DataElement::new(tags::PHOTOMETRIC_INTERPRETATION, VR::CS, s("MONOCHROME2")),
        DataElement::new(tags::ROWS, VR::US, PrimitiveValue::from(4_u16)),
        DataElement::new(tags::COLUMNS, VR::US, PrimitiveValue::from(4_u16)),
        DataElement::new(tags::BITS_ALLOCATED, VR::US, PrimitiveValue::from(16_u16)),
        DataElement::new(tags::BITS_STORED, VR::US, PrimitiveValue::from(12_u16)),
        DataElement::new(tags::HIGH_BIT, VR::US, PrimitiveValue::from(11_u16)),
        DataElement::new(
            tags::PIXEL_REPRESENTATION,
            VR::US,
            PrimitiveValue::from(0_u16),
        ),
        DataElement::new(
            tags::PIXEL_DATA,
            VR::OW,
            PrimitiveValue::from(vec![7u8; pixel_bytes]),
        ),
    ]);
    for (tag, vr, value) in extra {
        obj.put(DataElement::new(*tag, *vr, s(value)));
    }
    let file = obj
        .with_meta(
            FileMetaTableBuilder::new()
                .transfer_syntax(uids::EXPLICIT_VR_LITTLE_ENDIAN)
                .media_storage_sop_class_uid(uids::CT_IMAGE_STORAGE),
        )
        .unwrap();
    let mut out = Vec::new();
    file.write_all(&mut out).unwrap();
    out
}

pub fn sop(patient: u32, study: u32, series: u32, inst: u32) -> String {
    format!("2.25.9{patient}0{study}.{series}.{inst}")
}

/// Study Instance UID of the [`dicom`] fixtures.
pub fn study_uid(patient: u32, study: u32) -> String {
    format!("2.25.9{patient}0{study}")
}

/// Series Instance UID of the [`dicom`] fixtures.
pub fn series_uid(patient: u32, study: u32, series: u32) -> String {
    format!("2.25.9{patient}0{study}.{series}")
}

/// Study and series of the image fixtures below ([`image`]).
pub const IMAGE_STUDY: &str = "2.25.400000000000000000000000000001";
pub const IMAGE_SERIES: &str = "2.25.400000000000000000000000000002";

/// SOP Instance UID `n` of the image fixtures.
pub fn image_sop(n: u32) -> String {
    format!("2.25.40000000000000000000000000{n:04}")
}

/// Image attributes of a monochrome multi-frame instance in [`IMAGE_SERIES`],
/// without Pixel Data. `bits` is Bits Allocated.
pub fn image(sop: &str, frames: u32, rows: u16, cols: u16, bits: u16) -> InMemDicomObject {
    let us = |tag, v: u16| DataElement::new(tag, VR::US, PrimitiveValue::from(v));
    let s = |v: &str| PrimitiveValue::from(v);
    InMemDicomObject::from_element_iter([
        DataElement::new(
            tags::SOP_CLASS_UID,
            VR::UI,
            s(uids::MULTI_FRAME_GRAYSCALE_WORD_SECONDARY_CAPTURE_IMAGE_STORAGE),
        ),
        DataElement::new(tags::SOP_INSTANCE_UID, VR::UI, s(sop)),
        DataElement::new(tags::MODALITY, VR::CS, s("OT")),
        DataElement::new(tags::PATIENT_ID, VR::LO, s("FRAMES1")),
        DataElement::new(tags::STUDY_INSTANCE_UID, VR::UI, s(IMAGE_STUDY)),
        DataElement::new(tags::SERIES_INSTANCE_UID, VR::UI, s(IMAGE_SERIES)),
        DataElement::new(tags::INSTANCE_NUMBER, VR::IS, s("1")),
        us(tags::SAMPLES_PER_PIXEL, 1),
        DataElement::new(tags::PHOTOMETRIC_INTERPRETATION, VR::CS, s("MONOCHROME2")),
        DataElement::new(tags::NUMBER_OF_FRAMES, VR::IS, s(&frames.to_string())),
        us(tags::ROWS, rows),
        us(tags::COLUMNS, cols),
        us(tags::BITS_ALLOCATED, bits),
        us(tags::BITS_STORED, bits),
        us(tags::HIGH_BIT, bits - 1),
        us(tags::PIXEL_REPRESENTATION, 0),
    ])
}

/// `obj` as a Part-10 file in transfer syntax `ts`.
pub fn part10(obj: InMemDicomObject, ts: &str) -> Vec<u8> {
    // The SOP Class and Instance UIDs of the meta group come from `obj`.
    let file = obj
        .with_meta(FileMetaTableBuilder::new().transfer_syntax(ts))
        .unwrap();
    let mut out = Vec::new();
    file.write_all(&mut out).unwrap();
    out
}

/// Deterministic pixel bytes: every frame differs from the others.
pub fn pixel_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 7 + i / 13) as u8).collect()
}

/// Native Explicit VR Little Endian multi-frame image with `pixels`.
pub fn native_image(
    sop: &str,
    frames: u32,
    rows: u16,
    cols: u16,
    bits: u16,
    pixels: Vec<u8>,
) -> Vec<u8> {
    let mut obj = image(sop, frames, rows, cols, bits);
    let vr = if bits > 8 { VR::OW } else { VR::OB };
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        vr,
        PrimitiveValue::from(pixels),
    ));
    part10(obj, uids::EXPLICIT_VR_LITTLE_ENDIAN)
}

/// Encapsulated 16x16 8-bit image: `frames[k]` holds the fragments of frame
/// `k`. `bot` fills the Basic Offset Table.
pub fn encapsulated_image(sop: &str, frames: &[Vec<Vec<u8>>], bot: bool, ts: &str) -> Vec<u8> {
    let mut obj = image(sop, frames.len() as u32, 16, 16, 8);
    let mut table = Vec::new();
    let mut at = 0u32;
    for frame in frames {
        table.push(at);
        at += frame.iter().map(|f| 8 + f.len() as u32).sum::<u32>();
    }
    if !bot {
        table.clear();
    }
    let fragments: Vec<Vec<u8>> = frames.iter().flatten().cloned().collect();
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        VR::OB,
        PixelFragmentSequence::new(table, fragments),
    ));
    part10(obj, ts)
}

/// Split a `multipart/related` body into (part content type, part body).
pub fn parts(r: &Reply) -> Vec<(String, Vec<u8>)> {
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

/// The full server (`start`) on an ephemeral port with a temp storage root
/// and DIMSE off.
pub struct TestServer {
    pub running: Running,
    shutdown: CancellationToken,
    dir: tempfile::TempDir,
}

impl TestServer {
    pub async fn start(configure: impl FnOnce(&mut Config)) -> TestServer {
        TestServer::start_in(tempfile::tempdir().unwrap(), configure).await
    }

    /// Start on `dir`, keeping whatever storage is already under it.
    pub async fn start_in(
        dir: tempfile::TempDir,
        configure: impl FnOnce(&mut Config),
    ) -> TestServer {
        let mut config = Config::default();
        config.storage.root = dir.path().join("store");
        config.http.addr = "127.0.0.1:0".parse().unwrap();
        config.http.web_dir = dir.path().join("web");
        config.dimse.enabled = false;
        configure(&mut config);
        let shutdown = CancellationToken::new();
        let running = start(config, shutdown.clone()).await.unwrap();
        TestServer {
            running,
            shutdown,
            dir,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.running.http_addr
    }

    /// The storage root.
    pub fn root(&self) -> PathBuf {
        self.dir.path().join("store")
    }

    pub async fn stop(self) {
        self.stop_keep_dir().await;
    }

    /// Stop the server and hand back its temp dir (storage still on disk).
    pub async fn stop_keep_dir(self) -> tempfile::TempDir {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(10), self.running.task)
            .await
            .expect("server stops promptly")
            .unwrap()
            .unwrap();
        self.dir
    }

    pub async fn get(&self, path: &str, headers: &[(&str, &str)]) -> Reply {
        http_get(self.addr(), path, headers).await
    }

    /// STOW-RS `parts` to `/dicom-web/studies`; expects 200.
    pub async fn stow(&self, parts: &[Vec<u8>]) -> Reply {
        let r = stow(self.addr(), "/dicom-web/studies", parts, &[]).await;
        assert_eq!(r.status, 200, "{}", r.text());
        r
    }
}

/// One HTTP response.
pub struct Reply {
    pub status: u16,
    /// Header lines in order, names as sent.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Reply {
    /// The first value of header `name`.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.header_all(name).into_iter().next()
    }

    /// Every value of header `name`, in order.
    pub fn header_all(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
            .collect()
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// HTTP/1.0 request, one per connection, so the response is never chunked
/// and ends at EOF. Sends `Host: <addr>` unless `headers` has a Host.
pub async fn http_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Reply {
    let mut head = format!("{method} {path} HTTP/1.0\r\n");
    if !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("host")) {
        head.push_str(&format!("Host: {addr}\r\n"));
    }
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(head.as_bytes()).await.unwrap();
    s.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response head");
    let head = String::from_utf8_lossy(&buf[..split]).into_owned();
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|l| l.split(' ').nth(1))
        .and_then(|s| s.parse().ok())
        .expect("status line");
    let headers = lines
        .filter_map(|l| {
            let (k, v) = l.split_once(':')?;
            Some((k.trim().to_owned(), v.trim().to_owned()))
        })
        .collect();
    Reply {
        status,
        headers,
        body: buf[split + 4..].to_vec(),
    }
}

pub async fn http_get(addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> Reply {
    http_request(addr, "GET", path, headers, b"").await
}

pub async fn http_options(addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> Reply {
    http_request(addr, "OPTIONS", path, headers, b"").await
}

pub const STOW_BOUNDARY: &str = "XXsupportXX";

/// A STOW-RS body of `application/dicom` parts.
pub fn stow_body(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut body = Vec::new();
    for part in parts {
        body.extend_from_slice(
            format!("--{STOW_BOUNDARY}\r\nContent-Type: application/dicom\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(part);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{STOW_BOUNDARY}--\r\n").as_bytes());
    body
}

/// POST `parts` as STOW-RS to `path` with extra `headers`.
pub async fn stow(
    addr: SocketAddr,
    path: &str,
    parts: &[Vec<u8>],
    headers: &[(&str, &str)],
) -> Reply {
    let ct = format!("multipart/related; type=\"application/dicom\"; boundary={STOW_BOUNDARY}");
    let mut all = vec![("Content-Type", ct.as_str())];
    all.extend_from_slice(headers);
    http_request(addr, "POST", path, &all, &stow_body(parts)).await
}

/// NIfTI-1 single file: 348-byte header, 4-byte extension flag, 8 data
/// bytes; `tag` varies the data so fixtures hash differently.
pub fn nifti(tag: u8) -> Vec<u8> {
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
    v.extend_from_slice(&[tag; 8]);
    v
}

pub fn gz(bytes: &[u8]) -> Vec<u8> {
    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    e.write_all(bytes).unwrap();
    e.finish().unwrap()
}

pub fn zip_with(entries: &[(&str, &[u8])], method: zip::CompressionMethod) -> Vec<u8> {
    let mut w = ZipWriter::new(Cursor::new(Vec::new()));
    let opts = SimpleFileOptions::default().compression_method(method);
    for (name, data) in entries {
        w.start_file(*name, opts).unwrap();
        w.write_all(data).unwrap();
    }
    w.finish().unwrap().into_inner()
}

pub fn zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    zip_with(entries, zip::CompressionMethod::Deflated)
}

pub fn zip_stored(entries: &[(&str, &[u8])]) -> Vec<u8> {
    zip_with(entries, zip::CompressionMethod::Stored)
}

/// Set the encrypted flag on every local and central header.
pub fn mark_encrypted(mut z: Vec<u8>) -> Vec<u8> {
    for i in 0..z.len().saturating_sub(10) {
        if &z[i..i + 4] == b"PK\x03\x04" {
            z[i + 6] |= 1;
        } else if &z[i..i + 4] == b"PK\x01\x02" {
            z[i + 8] |= 1;
        }
    }
    z
}
