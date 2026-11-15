//! WADO-RS frames and bulk data, and the series catalog on frame tables,
//! against the full server on an ephemeral port. Instances are the phantom
//! or synthesized here (no real patient data).

#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

mod support;

use std::net::SocketAddr;
use std::path::Path;

use dicom_core::value::{DataSetSequence, PixelFragmentSequence};
use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_object::{InMemDicomObject, OpenFileOptions};
use futures_util::StreamExt;
use slicestream_core::attrs::get_i64;
use slicestream_core::frame::read_frame_bytes;
use slicestream_core::protocol::decode_frame;
use slicestream_core::{FrameInfo, Series};
use slicestream_store::DuplicatePolicy;
use support::*;
use tokio_tungstenite::tungstenite::Message;

const EXPLICIT_LE: &str = "1.2.840.10008.1.2.1";
const EXPLICIT_BE: &str = "1.2.840.10008.1.2.2";
const DEFLATED: &str = "1.2.840.10008.1.2.1.99";
const JPEG: &str = "1.2.840.10008.1.2.4.50";
const OCTET: &str = "multipart/related; type=\"application/octet-stream\"";
const OCTET_ANY: &str = "multipart/related; type=\"application/octet-stream\"; transfer-syntax=*";

const PHANTOM_STUDY: &str = "2.25.1000000000000000000000000000002";
const PHANTOM_SERIES: &str = "2.25.1000000000000000000000000000001";

fn phantom_sop(z: usize) -> String {
    format!("2.25.1000000000000000000000000001{z:05}")
}

fn instance_path(sop: &str) -> String {
    format!("/dicom-web/studies/{IMAGE_STUDY}/series/{IMAGE_SERIES}/instances/{sop}")
}

fn frames_path(sop: &str, list: &str) -> String {
    format!("{}/frames/{list}", instance_path(sop))
}

fn bulk_path(sop: &str, path: &str) -> String {
    format!("{}/bulk/{path}", instance_path(sop))
}

async fn get(srv: &TestServer, path: &str, accept: &str) -> Reply {
    srv.get(path, &[("Accept", accept)]).await
}

fn octet_type(ts: &str) -> String {
    format!("application/octet-stream; transfer-syntax={ts}")
}

/// Part bodies of a 200 multipart response.
fn bodies(r: &Reply) -> Vec<Vec<u8>> {
    assert_eq!(r.status, 200, "{}", r.text());
    parts(r).into_iter().map(|(_, b)| b).collect()
}

/// Every frame of a Part-10 file as the old path reads it: a whole-file
/// parse with `read_frame_bytes`.
fn old_frames(bytes: &[u8]) -> Vec<Vec<u8>> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f.dcm");
    std::fs::write(&path, bytes).unwrap();
    let obj = OpenFileOptions::new().open_file(&path).unwrap();
    let info = FrameInfo::from_dataset(&obj, obj.meta().transfer_syntax()).unwrap();
    let n = get_i64(&obj, tags::NUMBER_OF_FRAMES).unwrap_or(1) as u32;
    (0..n)
        .map(|f| read_frame_bytes(&path, f, &info).unwrap().to_vec())
        .collect()
}

/// Three 4x5 16-bit frames.
fn three_frames(sop: &str) -> Vec<u8> {
    native_image(sop, 3, 4, 5, 16, pixel_bytes(3 * 4 * 5 * 2))
}

/// Two JPEG Baseline frames of two fragments each, with a Basic Offset Table.
fn jpeg_frames() -> Vec<Vec<Vec<u8>>> {
    vec![
        vec![vec![0xFF, 0xD8, 1, 2], vec![3, 4, 5, 6]],
        vec![vec![0xFF, 0xD8, 7, 8, 9, 10], vec![11, 12]],
    ]
}

fn jpeg_image(sop: &str) -> Vec<u8> {
    encapsulated_image(sop, &jpeg_frames(), true, JPEG)
}

/// A private OB value over the 1,024-byte inline limit.
const PRIVATE_BULK: &str = "00091001";

fn private_ob_value() -> Vec<u8> {
    (0..3000u32).map(|i| (i % 251) as u8).collect()
}

fn with_private_ob(mut obj: InMemDicomObject) -> InMemDicomObject {
    obj.put(DataElement::new(
        dicom_core::Tag(0x0009, 0x0010),
        VR::LO,
        PrimitiveValue::from("SLICESTREAM TEST"),
    ));
    obj.put(DataElement::new(
        dicom_core::Tag(0x0009, 0x1001),
        VR::OB,
        PrimitiveValue::from(private_ob_value()),
    ));
    obj
}

/// Two 4x4 16-bit frames with the private OB value, in transfer syntax `ts`.
fn two_frames_with_bulk(sop: &str, ts: &str) -> Vec<u8> {
    let mut obj = with_private_ob(image(sop, 2, 4, 4, 16));
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        VR::OW,
        PrimitiveValue::from(pixel_bytes(2 * 4 * 4 * 2)),
    ));
    part10(obj, ts)
}

async fn index_pool(root: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect(&format!("sqlite://{}", root.join("index.sqlite").display()))
        .await
        .unwrap()
}

/// Forget every scan, as in a store from before scanning existed.
async fn unscan_all(srv: &TestServer) {
    let pool = index_pool(&srv.root()).await;
    for q in [
        "DELETE FROM instance_data",
        "UPDATE instances SET scan_version = 0, scan_problem = NULL",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frame_octet_stream_matches_old_path() {
    let dir = tempfile::tempdir().unwrap();
    phantom::write_phantom(dir.path(), 4, 32).unwrap();
    let mut files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    let phantom: Vec<Vec<u8>> = files.iter().map(|f| std::fs::read(f).unwrap()).collect();
    let srv = TestServer::start(|_| {}).await;
    srv.stow(&phantom).await;
    for z in 0..4 {
        let sop = phantom_sop(z);
        let file = phantom
            .iter()
            .find(|b| b.windows(sop.len()).any(|w| w == sop.as_bytes()))
            .unwrap();
        let path = format!(
            "/dicom-web/studies/{PHANTOM_STUDY}/series/{PHANTOM_SERIES}/instances/{sop}/frames/1"
        );
        let r = get(&srv, &path, OCTET).await;
        assert_eq!(r.status, 200, "{}", r.text());
        assert!(
            r.header("content-type")
                .unwrap()
                .starts_with("multipart/related; type=\"application/octet-stream\"; boundary="),
            "{:?}",
            r.headers
        );
        assert_eq!(
            parts(&r),
            [(octet_type(EXPLICIT_LE), old_frames(file).remove(0))]
        );
    }

    let multi = three_frames(&image_sop(1));
    srv.stow(std::slice::from_ref(&multi)).await;
    let r = get(&srv, &frames_path(&image_sop(1), "1,2,3"), OCTET).await;
    assert_eq!(bodies(&r), old_frames(&multi));
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frames_list_order_and_duplicates() {
    let srv = TestServer::start(|_| {}).await;
    let multi = three_frames(&image_sop(1));
    srv.stow(std::slice::from_ref(&multi)).await;
    let old = old_frames(&multi);
    let r = get(&srv, &frames_path(&image_sop(1), "3,1,3"), OCTET).await;
    assert_eq!(bodies(&r), [old[2].clone(), old[0].clone(), old[2].clone()]);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frames_zero_and_past_end_404() {
    let srv = TestServer::start(|_| {}).await;
    srv.stow(&[three_frames(&image_sop(1)), dicom(1, 1, 1, 1)])
        .await;
    for list in ["0", "4", "1,4", "2,0"] {
        let r = get(&srv, &frames_path(&image_sop(1), list), OCTET).await;
        assert_eq!(r.status, 404, "{list}: {}", r.text());
        assert!(r.header("content-type").unwrap().starts_with("text/plain"));
    }
    // Unknown instance, and a real instance under the wrong series.
    let r = get(&srv, &frames_path(&image_sop(9), "1"), OCTET).await;
    assert_eq!(r.status, 404);
    let wrong = format!(
        "/dicom-web/studies/{IMAGE_STUDY}/series/{}/instances/{}/frames/1",
        series_uid(1, 1, 1),
        image_sop(1)
    );
    assert_eq!(get(&srv, &wrong, OCTET).await.status, 404);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frames_non_image_and_no_pixel_data_404() {
    let srv = TestServer::start(|_| {}).await;
    // An image without Pixel Data, and an object that is not an image.
    let no_pixels = part10(image(&image_sop(1), 1, 4, 4, 16), EXPLICIT_LE);
    let mut report = InMemDicomObject::new_empty();
    for (tag, value) in [
        (tags::SOP_CLASS_UID, uids::BASIC_TEXT_SR_STORAGE.to_owned()),
        (tags::SOP_INSTANCE_UID, image_sop(2)),
        (tags::STUDY_INSTANCE_UID, IMAGE_STUDY.to_owned()),
        (tags::SERIES_INSTANCE_UID, IMAGE_SERIES.to_owned()),
    ] {
        report.put(DataElement::new(tag, VR::UI, PrimitiveValue::from(value)));
    }
    srv.stow(&[no_pixels, part10(report, EXPLICIT_LE)]).await;
    for path in [
        frames_path(&image_sop(1), "1"),
        bulk_path(&image_sop(1), "7FE00010"),
    ] {
        let r = get(&srv, &path, OCTET).await;
        assert_eq!(r.status, 404, "{path}");
        assert_eq!(r.text(), "no pixel data");
    }
    let r = get(&srv, &frames_path(&image_sop(2), "1"), OCTET).await;
    assert_eq!(r.status, 404);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frames_bad_lists_400() {
    let srv = TestServer::start(|_| {}).await;
    srv.stow(&[three_frames(&image_sop(1))]).await;
    let many = vec!["1"; 5000].join(",");
    let over = vec!["1"; 4097].join(",");
    for list in [
        "1,,2",
        "abc",
        "-1",
        "+1",
        "1,",
        ",1",
        "1%202",
        "4294967296",
        &many,
        &over,
    ] {
        let r = get(&srv, &frames_path(&image_sop(1), list), OCTET).await;
        assert_eq!(r.status, 400, "{list}");
        assert!(r.header("content-type").unwrap().starts_with("text/plain"));
        // Bad lists are rejected before the instance is looked up.
        let r = get(&srv, &frames_path(&image_sop(9), list), OCTET).await;
        assert_eq!(r.status, 400, "{list}");
    }
    // 4,096 entries is the limit, and allowed.
    let max = vec!["2"; 4096].join(",");
    let r = get(&srv, &frames_path(&image_sop(1), &max), OCTET).await;
    assert_eq!(bodies(&r).len(), 4096);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encapsulated_frames_octet_406() {
    let srv = TestServer::start(|_| {}).await;
    srv.stow(&[jpeg_image(&image_sop(1))]).await;
    for accept in [
        OCTET,
        "application/octet-stream",
        "multipart/related; type=\"application/octet-stream\"; transfer-syntax=1.2.840.10008.1.2.1",
    ] {
        let r = get(&srv, &frames_path(&image_sop(1), "1"), accept).await;
        assert_eq!(r.status, 406, "{accept}");
    }
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encapsulated_frames_star_carry_ts() {
    let srv = TestServer::start(|_| {}).await;
    srv.stow(&[jpeg_image(&image_sop(1))]).await;
    let want: Vec<Vec<u8>> = jpeg_frames().iter().map(|f| f.concat()).collect();
    let r = get(&srv, &frames_path(&image_sop(1), "2,1"), OCTET_ANY).await;
    assert_eq!(
        parts(&r),
        [
            (octet_type(JPEG), want[1].clone()),
            (octet_type(JPEG), want[0].clone())
        ]
    );
    // `*/*` (and no Accept) serve the stored codestream as image/jpeg.
    for accept in ["*/*", ""] {
        let r = get(&srv, &frames_path(&image_sop(1), "1"), accept).await;
        assert_eq!(
            parts(&r),
            [(
                format!("image/jpeg; transfer-syntax={JPEG}"),
                want[0].clone()
            )]
        );
    }
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn image_jpeg_accept() {
    let srv = TestServer::start(|_| {}).await;
    srv.stow(&[jpeg_image(&image_sop(1))]).await;
    let want: Vec<Vec<u8>> = jpeg_frames().iter().map(|f| f.concat()).collect();
    let part_type = format!("image/jpeg; transfer-syntax={JPEG}");
    for accept in [
        "multipart/related; type=\"image/jpeg\"".to_owned(),
        "multipart/related; type=image/jpeg".to_owned(),
        format!("multipart/related; type=\"image/jpeg\"; transfer-syntax={JPEG}"),
    ] {
        let r = get(&srv, &frames_path(&image_sop(1), "1,2"), &accept).await;
        assert!(
            r.header("content-type")
                .unwrap()
                .starts_with("multipart/related; type=\"image/jpeg\""),
            "{:?}",
            r.headers
        );
        assert_eq!(
            parts(&r),
            [
                (part_type.clone(), want[0].clone()),
                (part_type.clone(), want[1].clone())
            ]
        );
    }
    // A bare image/jpeg gets the frame itself when one frame is asked for.
    let r = get(&srv, &frames_path(&image_sop(1), "2"), "image/jpeg").await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-type"), Some(part_type.as_str()));
    assert_eq!(r.body, want[1]);
    for (list, accept) in [
        ("1,2", "image/jpeg"),
        ("1", "multipart/related; type=\"image/jp2\""),
        (
            "1",
            "multipart/related; type=\"image/jpeg\"; transfer-syntax=1.2.840.10008.1.2.4.70",
        ),
    ] {
        let r = get(&srv, &frames_path(&image_sop(1), list), accept).await;
        assert_eq!(r.status, 406, "{accept}");
    }
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quoted_and_unquoted_type() {
    let srv = TestServer::start(|_| {}).await;
    let multi = three_frames(&image_sop(1));
    srv.stow(std::slice::from_ref(&multi)).await;
    let old = old_frames(&multi);
    for accept in [
        "multipart/related; type=application/octet-stream; transfer-syntax=*",
        "multipart/related; type=\"application/octet-stream\"; transfer-syntax=*",
        "multipart/related; type=application/octet-stream",
    ] {
        let r = get(&srv, &frames_path(&image_sop(1), "2"), accept).await;
        assert_eq!(
            parts(&r),
            [(octet_type(EXPLICIT_LE), old[1].clone())],
            "{accept}"
        );
    }
    srv.stop().await;
}

/// The pixel values of [`big_endian_image`].
fn big_endian_pixels() -> Vec<u16> {
    (0..12u16).map(|i| 0x0102 + i * 0x0101).collect()
}

/// Two 3x2 16-bit frames, to be stored Explicit VR Big Endian.
fn big_endian_image(sop: &str) -> InMemDicomObject {
    let mut obj = image(sop, 2, 2, 3, 16);
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        VR::OW,
        PrimitiveValue::U16(big_endian_pixels().into_iter().collect()),
    ));
    obj
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn big_endian_frames_swapped() {
    let srv = TestServer::start(|_| {}).await;
    let mut obj = big_endian_image(&image_sop(1));
    // A private OW value, bulk data: sent as stored (big-endian).
    let words: Vec<u16> = (0..600u16).map(|i| i.wrapping_mul(0x0301)).collect();
    obj.put(DataElement::new(
        dicom_core::Tag(0x0009, 0x1001),
        VR::OW,
        PrimitiveValue::U16(words.iter().copied().collect()),
    ));
    srv.stow(&[part10(obj, EXPLICIT_BE)]).await;
    let px = big_endian_pixels();
    let le: Vec<u8> = px[6..].iter().flat_map(|v| v.to_le_bytes()).collect();
    let be: Vec<u8> = px[6..].iter().flat_map(|v| v.to_be_bytes()).collect();
    let words_be: Vec<u8> = words.iter().flat_map(|v| v.to_be_bytes()).collect();
    // Indexed (pread), then unscanned (whole-file parse): same answers.
    for round in ["indexed", "unscanned"] {
        let r = get(&srv, &frames_path(&image_sop(1), "2"), OCTET).await;
        assert_eq!(
            parts(&r),
            [(octet_type(EXPLICIT_LE), le.clone())],
            "{round}"
        );
        let r = get(&srv, &frames_path(&image_sop(1), "2"), OCTET_ANY).await;
        assert_eq!(
            parts(&r),
            [(octet_type(EXPLICIT_BE), be.clone())],
            "{round}"
        );
        let r = get(&srv, &bulk_path(&image_sop(1), PRIVATE_BULK), OCTET_ANY).await;
        assert_eq!(
            parts(&r),
            [(octet_type(EXPLICIT_BE), words_be.clone())],
            "{round}"
        );
        unscan_all(&srv).await;
    }
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bits_allocated_1_frame() {
    let srv = TestServer::start(|_| {}).await;
    // Three 3x3 frames, bit-packed back to back (27 bits, padded to 4 bytes).
    let packed = vec![0b1011_0110, 0b0100_1101, 0b1110_0010, 0b0000_0101];
    let mut obj = image(&image_sop(1), 3, 3, 3, 1);
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        VR::OW,
        PrimitiveValue::from(packed.clone()),
    ));
    let file = part10(obj, EXPLICIT_LE);
    srv.stow(std::slice::from_ref(&file)).await;
    let old = old_frames(&file);
    let r = get(&srv, &frames_path(&image_sop(1), "2,3"), OCTET).await;
    let got = bodies(&r);
    assert_eq!(got, old[1..]);
    assert_eq!(got[0], slicestream_core::frame::extract_bits(&packed, 9, 9));
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frames_ambiguous_500() {
    let srv = TestServer::start(|_| {}).await;
    // Three frames in two fragments, no offset table, no codestream markers.
    let mut obj = image(&image_sop(1), 3, 16, 16, 8);
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        VR::OB,
        PixelFragmentSequence::new(Vec::<u32>::new(), vec![vec![1u8, 2], vec![3, 4]]),
    ));
    srv.stow(&[part10(obj, JPEG)]).await;
    for path in [
        frames_path(&image_sop(1), "1"),
        bulk_path(&image_sop(1), "7FE00010"),
    ] {
        let r = get(&srv, &path, OCTET_ANY).await;
        assert_eq!(r.status, 500, "{path}");
        assert!(r.header("content-type").unwrap().starts_with("text/plain"));
        assert!(
            r.text().contains("frame boundaries unknown"),
            "{}",
            r.text()
        );
    }
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deflated_frames_fall_back() {
    let srv = TestServer::start(|_| {}).await;
    let file = two_frames_with_bulk(&image_sop(1), DEFLATED);
    srv.stow(std::slice::from_ref(&file)).await;
    let old = old_frames(&file);
    assert_eq!(old.concat(), pixel_bytes(2 * 4 * 4 * 2));
    // The pixels are served inflated, so they are Explicit VR Little Endian.
    for accept in [OCTET, OCTET_ANY] {
        let r = get(&srv, &frames_path(&image_sop(1), "2,1"), accept).await;
        assert_eq!(
            parts(&r),
            [
                (octet_type(EXPLICIT_LE), old[1].clone()),
                (octet_type(EXPLICIT_LE), old[0].clone())
            ]
        );
    }
    // Bulk data has no recorded offsets either: whole-file parse.
    let r = get(&srv, &bulk_path(&image_sop(1), PRIVATE_BULK), OCTET).await;
    assert_eq!(bodies(&r), [private_ob_value()]);
    let r = get(&srv, &bulk_path(&image_sop(1), "7FE00010"), OCTET).await;
    assert_eq!(bodies(&r), old);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unscanned_frames_fall_back() {
    let srv = TestServer::start(|_| {}).await;
    let file = two_frames_with_bulk(&image_sop(1), EXPLICIT_LE);
    srv.stow(std::slice::from_ref(&file)).await;
    unscan_all(&srv).await;
    let old = old_frames(&file);
    let r = get(&srv, &frames_path(&image_sop(1), "1,2"), OCTET).await;
    assert_eq!(bodies(&r), old);
    let r = get(&srv, &bulk_path(&image_sop(1), PRIVATE_BULK), OCTET).await;
    assert_eq!(bodies(&r), [private_ob_value()]);
    let r = get(&srv, &bulk_path(&image_sop(1), "7FE00010"), OCTET).await;
    assert_eq!(bodies(&r), old);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_then_frames_new_bytes() {
    let srv = TestServer::start(|c| c.storage.on_duplicate = DuplicatePolicy::Overwrite).await;
    let sop = image_sop(1);
    // The replacement has a longer private value and more frames, so its
    // values sit at other offsets than the first file's.
    let file = |value: Vec<u8>, frames: u32, pixels: Vec<u8>| {
        let mut obj = image(&sop, frames, 4, 4, 8);
        obj.put(DataElement::new(
            dicom_core::Tag(0x0009, 0x1001),
            VR::OB,
            PrimitiveValue::from(value),
        ));
        obj.put(DataElement::new(
            tags::PIXEL_DATA,
            VR::OB,
            PrimitiveValue::from(pixels),
        ));
        part10(obj, EXPLICIT_LE)
    };
    let first = file(vec![1; 2000], 2, pixel_bytes(32));
    let second = file(vec![2; 3002], 3, (0..48).rev().collect());
    for (bytes, value) in [(&first, vec![1u8; 2000]), (&second, vec![2u8; 3002])] {
        srv.stow(std::slice::from_ref(bytes)).await;
        let r = get(&srv, &frames_path(&sop, "2"), OCTET).await;
        assert_eq!(bodies(&r), [old_frames(bytes)[1].clone()]);
        let r = get(&srv, &bulk_path(&sop, "7FE00010"), OCTET).await;
        assert_eq!(bodies(&r), old_frames(bytes));
        let r = get(&srv, &bulk_path(&sop, PRIVATE_BULK), OCTET).await;
        assert_eq!(bodies(&r), [value]);
    }
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_private_ob_value() {
    let srv = TestServer::start(|_| {}).await;
    srv.stow(&[two_frames_with_bulk(&image_sop(1), EXPLICIT_LE)])
        .await;
    // The BulkDataURI from the metadata resolves to the value.
    let host = srv.addr().to_string();
    let r = srv
        .get(&format!("{}/metadata", instance_path(&image_sop(1))), &[])
        .await;
    let uri = r.json()[0][PRIVATE_BULK]["BulkDataURI"]
        .as_str()
        .unwrap()
        .to_owned();
    let path = uri.strip_prefix(&format!("http://{host}")).unwrap();
    assert_eq!(path, bulk_path(&image_sop(1), PRIVATE_BULK));
    let r = get(&srv, path, OCTET).await;
    assert!(
        r.header("content-type")
            .unwrap()
            .starts_with("multipart/related; type=\"application/octet-stream\"; boundary="),
        "{:?}",
        r.headers
    );
    assert_eq!(parts(&r), [(octet_type(EXPLICIT_LE), private_ob_value())]);
    for accept in [OCTET_ANY, "*/*"] {
        let r = get(&srv, path, accept).await;
        assert_eq!(bodies(&r), [private_ob_value()], "{accept}");
    }
    let r = get(&srv, path, "multipart/related; type=\"image/jpeg\"").await;
    assert_eq!(r.status, 406);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_nested_path() {
    let srv = TestServer::start(|_| {}).await;
    // ICC profiles in the items of the Optical Path Sequence.
    let profile = |seed: u8, len: usize| -> Vec<u8> {
        (0..len).map(|i| (i as u8).wrapping_mul(seed)).collect()
    };
    let item = |icc: Vec<u8>| {
        InMemDicomObject::from_element_iter([DataElement::new(
            tags::ICC_PROFILE,
            VR::OB,
            PrimitiveValue::from(icc),
        )])
    };
    let mut obj = image(&image_sop(1), 1, 4, 4, 16);
    obj.put(DataElement::new(
        tags::OPTICAL_PATH_SEQUENCE,
        VR::SQ,
        DataSetSequence::from(vec![item(profile(3, 1500)), item(profile(5, 2000))]),
    ));
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        VR::OW,
        PrimitiveValue::from(pixel_bytes(32)),
    ));
    let file = part10(obj, EXPLICIT_LE);
    srv.stow(std::slice::from_ref(&file)).await;
    let r = srv
        .get(&format!("{}/metadata", instance_path(&image_sop(1))), &[])
        .await;
    let uri = r.json()[0]["00480105"]["Value"][1]["00282000"]["BulkDataURI"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(uri.ends_with("/bulk/00480105/1/00282000"), "{uri}");
    for round in ["indexed", "unscanned"] {
        for (index, want) in [(0, profile(3, 1500)), (1, profile(5, 2000))] {
            let path = bulk_path(&image_sop(1), &format!("00480105/{index}/00282000"));
            let r = get(&srv, &path, OCTET).await;
            assert_eq!(bodies(&r), [want], "{round} {path}");
        }
        unscan_all(&srv).await;
    }
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_pixel_data_all_frames() {
    let srv = TestServer::start(|_| {}).await;
    let multi = three_frames(&image_sop(1));
    srv.stow(&[multi.clone(), jpeg_image(&image_sop(2))]).await;
    let r = get(&srv, &bulk_path(&image_sop(1), "7FE00010"), OCTET).await;
    assert_eq!(bodies(&r), old_frames(&multi));
    // Encapsulated: one part per frame, under the frames rules.
    let r = get(&srv, &bulk_path(&image_sop(2), "7FE00010"), OCTET_ANY).await;
    let want: Vec<(String, Vec<u8>)> = jpeg_frames()
        .iter()
        .map(|f| (octet_type(JPEG), f.concat()))
        .collect();
    assert_eq!(parts(&r), want);
    let r = get(&srv, &bulk_path(&image_sop(2), "7FE00010"), OCTET).await;
    assert_eq!(r.status, 406);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_single_part_bare_accept() {
    let srv = TestServer::start(|_| {}).await;
    let one = native_image(&image_sop(2), 1, 4, 4, 16, pixel_bytes(32));
    srv.stow(&[
        two_frames_with_bulk(&image_sop(1), EXPLICIT_LE),
        one.clone(),
    ])
    .await;
    let bare = "application/octet-stream";
    let r = get(&srv, &bulk_path(&image_sop(1), PRIVATE_BULK), bare).await;
    assert_eq!(r.status, 200);
    assert_eq!(
        r.header("content-type"),
        Some(octet_type(EXPLICIT_LE).as_str())
    );
    assert_eq!(r.header("content-length"), Some("3000"));
    assert_eq!(r.body, private_ob_value());
    // Pixel Data of one frame: that frame alone; of two frames: 406.
    let r = get(&srv, &bulk_path(&image_sop(2), "7FE00010"), bare).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body, old_frames(&one)[0]);
    let r = get(&srv, &bulk_path(&image_sop(1), "7FE00010"), bare).await;
    assert_eq!(r.status, 406);
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_unknown_path_404() {
    let srv = TestServer::start(|_| {}).await;
    srv.stow(&[two_frames_with_bulk(&image_sop(1), EXPLICIT_LE)])
        .await;
    for round in ["indexed", "unscanned"] {
        for path in [
            "00100010",
            "7fe00010",
            "00091002",
            "00091001/0/00100010",
            "0009100",
            "zz",
            "00091001/",
        ] {
            let r = get(&srv, &bulk_path(&image_sop(1), path), OCTET).await;
            assert_eq!(r.status, 404, "{round} {path}");
        }
        let r = get(&srv, &bulk_path(&image_sop(9), PRIVATE_BULK), OCTET).await;
        assert_eq!(r.status, 404, "{round}");
        unscan_all(&srv).await;
    }
    srv.stop().await;
}

/// Every frame of a series as the WebSocket stream sends it, by index.
async fn ws_frames(addr: SocketAddr, series: &str, count: usize) -> Vec<Vec<u8>> {
    let url = format!("ws://{addr}/api/series/{series}/stream");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let hello = ws.next().await.unwrap().unwrap();
    assert!(hello.is_text(), "{hello:?}");
    let mut out = vec![None; count];
    while out.iter().any(Option::is_none) {
        match ws.next().await.unwrap().unwrap() {
            Message::Binary(b) => {
                let (header, payload) = decode_frame(&b).expect("valid frame");
                out[header.index] = Some(payload.to_vec());
            }
            Message::Text(_) | Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("unexpected message {other:?}"),
        }
    }
    out.into_iter().map(Option::unwrap).collect()
}

/// `/api/series/{uid}/frames/{i}` for every slice.
async fn api_frames(srv: &TestServer, series: &str, count: usize) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for i in 0..count {
        let r = srv
            .get(&format!("/api/series/{series}/frames/{i}"), &[])
            .await;
        assert_eq!(r.status, 200, "{i}: {}", r.text());
        out.push(r.body);
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_and_api_frames_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let phantom_dir = dir.path().join("phantom");
    phantom::write_phantom(&phantom_dir, 4, 32).unwrap();
    let jpeg_dir = dir.path().join("jpeg");
    std::fs::create_dir_all(&jpeg_dir).unwrap();
    std::fs::write(jpeg_dir.join("jpeg.dcm"), jpeg_image(&image_sop(1))).unwrap();
    // Big-endian keeps the whole-file parse; it must not change either.
    let be_dir = dir.path().join("be");
    std::fs::create_dir_all(&be_dir).unwrap();
    let be_series = "2.25.400000000000000000000000000003";
    let mut be = big_endian_image(&image_sop(2));
    be.put(DataElement::new(
        tags::SERIES_INSTANCE_UID,
        VR::UI,
        PrimitiveValue::from(be_series),
    ));
    std::fs::write(be_dir.join("be.dcm"), part10(be, EXPLICIT_BE)).unwrap();

    let srv = TestServer::start(|_| {}).await;
    for d in [&phantom_dir, &jpeg_dir, &be_dir] {
        let files: Vec<Vec<u8>> = std::fs::read_dir(d)
            .unwrap()
            .map(|e| std::fs::read(e.unwrap().path()).unwrap())
            .collect();
        srv.stow(&files).await;
    }
    for (d, uid) in [
        (&phantom_dir, PHANTOM_SERIES),
        (&jpeg_dir, IMAGE_SERIES),
        (&be_dir, be_series),
    ] {
        // The old output: the series indexed from the files, no frame table.
        let old_series = Series::scan_dir(d).unwrap().remove(0);
        let old: Vec<Vec<u8>> = (0..old_series.len())
            .map(|i| old_series.load_frame(i).unwrap().data.to_vec())
            .collect();
        assert_eq!(api_frames(&srv, uid, old.len()).await, old, "{uid}");
        assert_eq!(ws_frames(srv.addr(), uid, old.len()).await, old, "{uid}");
    }
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_frames_use_frame_table() {
    // Two frames in three fragments, no offset table: only the codestream
    // markers place them, which the whole-file parse cannot do.
    let srv = TestServer::start(|_| {}).await;
    let mut obj = image(&image_sop(1), 2, 16, 16, 8);
    let fragments = vec![vec![0xFF, 0xD8, 1, 2], vec![3, 4], vec![0xFF, 0xD8, 5, 6]];
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        VR::OB,
        PixelFragmentSequence::new(Vec::<u32>::new(), fragments.clone()),
    ));
    srv.stow(&[part10(obj, JPEG)]).await;
    let want = vec![fragments[..2].concat(), fragments[2].clone()];
    assert_eq!(api_frames(&srv, IMAGE_SERIES, 2).await, want);
    assert_eq!(ws_frames(srv.addr(), IMAGE_SERIES, 2).await, want);
    let r = get(&srv, &frames_path(&image_sop(1), "1,2"), OCTET_ANY).await;
    assert_eq!(bodies(&r), want);
    srv.stop().await;
}

/// Open a GET and read a little, leaving the server blocked on a full socket.
async fn start_stalled(addr: SocketAddr, path: &str) -> (tokio::net::TcpStream, Vec<u8>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(
        format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let mut first = vec![0u8; 4096];
    let n = s.read(&mut first).await.unwrap();
    first.truncate(n);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    (s, first)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blob_vanishing_mid_stream_aborts_frames() {
    use tokio::io::AsyncReadExt;
    let srv = TestServer::start(|c| c.storage.max_instance_bytes = 128 << 20).await;
    // Two 32 MiB frames: the server is still sending the first when the
    // file goes away, so reading the second fails.
    let side = 4096u16;
    let len = side as usize * side as usize * 2;
    srv.stow(&[native_image(
        &image_sop(1),
        2,
        side,
        side,
        16,
        vec![7u8; 2 * len],
    )])
    .await;
    let (mut s, mut buf) = start_stalled(srv.addr(), &frames_path(&image_sop(1), "1,2")).await;
    assert!(buf.starts_with(b"HTTP/1.1 200"), "{:?}", &buf[..12]);
    let mut objects = vec![srv.root().join("objects")];
    while let Some(d) = objects.pop() {
        for e in std::fs::read_dir(d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                objects.push(p);
            } else {
                std::fs::remove_file(p).unwrap();
            }
        }
    }
    let _ = s.read_to_end(&mut buf).await;
    assert!(
        !buf.ends_with(b"0\r\n\r\n"),
        "a missing blob must abort the body, not end it cleanly"
    );
    srv.stop().await;
}
