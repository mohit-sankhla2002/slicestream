#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;
mod support;

use std::path::{Path, PathBuf};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use dicom_core::value::DataSetSequence;
use dicom_core::{DataElement, PrimitiveValue, Tag, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_object::InMemDicomObject;
use serde_json::{Value, json};
use slicestream_core::frame::{read_frame_at, read_frame_bytes};
use slicestream_store::{
    BULK_BINARY_MIN, BulkValue, FrameTable, InstanceHeader, InstanceScan, MAX_FRAGMENTS,
    MAX_METADATA_BYTES, SCAN_VERSION, ScanLimits, read_header, scan_instance, scan_instance_traced,
    scan_instance_with,
};
use support::{ele, ele_long, item, pixel_bytes};

fn metadata(scan: &InstanceScan) -> Value {
    let bytes = scan.data.metadata.as_ref().expect("metadata");
    serde_json::from_slice(bytes).expect("metadata is valid JSON")
}

fn file_bytes(path: &Path, offset: u64, len: u64) -> Vec<u8> {
    let all = std::fs::read(path).unwrap();
    all[offset as usize..(offset + len) as usize].to_vec()
}

/// The header without the fields only the scan reads.
fn shared(mut h: InstanceHeader) -> InstanceHeader {
    h.study_time = None;
    h.study_id = None;
    h.referring_physician_name = None;
    h.patient_birth_date = None;
    h.patient_sex = None;
    h.performed_procedure_step_start_date = None;
    h.performed_procedure_step_start_time = None;
    h
}

/// Every frame of `scan`, read with positioned reads.
fn frames_at(path: &Path, scan: &InstanceScan) -> Vec<Vec<u8>> {
    let table = scan.data.frames.as_ref().expect("frame table");
    (0..table.count())
        .map(|k| {
            read_frame_at(path, &table.location(k).unwrap())
                .unwrap()
                .to_vec()
        })
        .collect()
}

fn private(element: u16, vr: VR, value: PrimitiveValue) -> DataElement<InMemDicomObject> {
    DataElement::new(Tag(0x0009, element), vr, value)
}

fn creator() -> DataElement<InMemDicomObject> {
    private(0x0010, VR::LO, PrimitiveValue::from("SLICESTREAM TEST"))
}

#[test]
fn scan_constants() {
    assert_eq!(SCAN_VERSION, 1);
    assert_eq!(MAX_METADATA_BYTES, 67_108_864);
    assert_eq!(BULK_BINARY_MIN, 1024);
    assert_eq!(ScanLimits::default().max_metadata_bytes, MAX_METADATA_BYTES);
    assert_eq!(MAX_FRAGMENTS, 1_048_576);
    assert_eq!(ScanLimits::default().max_fragments, MAX_FRAGMENTS);
}

#[test]
fn scan_header_matches_read_header() {
    let dir = tempfile::tempdir().unwrap();
    phantom::write_phantom(dir.path(), 2, 16).unwrap();
    for name in ["IM00001.dcm", "IM00002.dcm"] {
        let p = dir.path().join(name);
        let scan = scan_instance(&p).unwrap();
        assert_eq!(scan.header, read_header(&p).unwrap());
        assert_eq!(scan.data.problem, None);
        assert!(scan.data.offsets_valid);
        let v = metadata(&scan);
        assert_eq!(
            v["00100010"],
            json!({"vr":"PN","Value":[{"Alphabetic":"PHANTOM^SYNTHETIC"}]})
        );
        assert_eq!(v["00280010"], json!({"vr":"US","Value":[16]}));
    }
}

#[test]
fn scan_native_locations_match_parse() {
    let dir = tempfile::tempdir().unwrap();
    phantom::write_phantom(&dir.path().join("phantom"), 3, 16).unwrap();
    let mut files: Vec<PathBuf> = (1..=3)
        .map(|i| dir.path().join(format!("phantom/IM{i:05}.dcm")))
        .collect();
    files.push(support::native(&dir.path().join("n16"), 4, 5, 3, 16));
    files.push(support::native(&dir.path().join("n8"), 3, 3, 3, 8));
    for p in files {
        let scan = scan_instance(&p).unwrap();
        assert_eq!(scan.data.problem, None, "{}", p.display());
        let info = scan.header.frame_info.clone().unwrap();
        let table = scan.data.frames.as_ref().expect("frame table");
        assert!(matches!(table, FrameTable::Native { .. }), "{table:?}");
        assert_eq!(table.count(), scan.header.number_of_frames);
        for (k, frame) in frames_at(&p, &scan).iter().enumerate() {
            let old = read_frame_bytes(&p, k as u32, &info).unwrap();
            assert_eq!(&frame[..], &old[..], "{} frame {k}", p.display());
        }
    }
    let p = dir.path().join("n16/native.dcm");
    match scan_instance(&p).unwrap().data.frames {
        Some(FrameTable::Native {
            frame_len, count, ..
        }) => assert_eq!((frame_len, count), (5 * 3 * 2, 4)),
        other => panic!("{other:?}"),
    }
}

/// Write `frames` encapsulated, scan, and check every frame reads back as
/// the concatenation of its fragments.
fn check_encapsulated(
    dir: &Path,
    frames: &[Vec<Vec<u8>>],
    bot: bool,
    eot: bool,
    ts: &str,
) -> (PathBuf, InstanceScan) {
    let p = support::encapsulated(dir, frames, bot, eot, ts);
    let scan = scan_instance(&p).unwrap();
    assert_eq!(scan.data.problem, None);
    let table = scan.data.frames.as_ref().expect("frame table");
    assert!(matches!(table, FrameTable::Fragments(_)), "{table:?}");
    assert_eq!(table.count() as usize, frames.len());
    let got = frames_at(&p, &scan);
    for (k, frame) in frames.iter().enumerate() {
        assert_eq!(got[k], frame.concat(), "frame {k}");
    }
    let v = metadata(&scan);
    assert_eq!(v["7FE00010"]["BulkDataURI"], "bulk/7FE00010");
    assert!(scan.data.bulk.iter().all(|b| b.path != "7FE00010"));
    (p, scan)
}

#[test]
fn scan_encapsulated_bot() {
    let dir = tempfile::tempdir().unwrap();
    let frames = vec![
        vec![vec![1; 10], vec![2; 6]],
        vec![vec![3; 4]],
        vec![vec![4; 2], vec![5; 8], vec![6; 2]],
    ];
    let (p, scan) = check_encapsulated(dir.path(), &frames, true, false, uids::JPEG_BASELINE8_BIT);
    // The whole-file parse groups the same way.
    let info = scan.header.frame_info.unwrap();
    for (k, frame) in frames.iter().enumerate() {
        let old = read_frame_bytes(&p, k as u32, &info).unwrap();
        assert_eq!(&old[..], &frame.concat()[..]);
    }
}

#[test]
fn scan_encapsulated_eot() {
    let dir = tempfile::tempdir().unwrap();
    // Two frames in three fragments, no Basic Offset Table, no markers:
    // only the Extended Offset Table can group them.
    let frames = vec![vec![vec![1; 10], vec![2; 6]], vec![vec![3; 4]]];
    let (_, scan) = check_encapsulated(dir.path(), &frames, false, true, uids::JPEG_BASELINE8_BIT);
    let v = metadata(&scan);
    // 16 bytes each: small enough to go inline.
    let offsets: Vec<u8> = [0u64, 32].iter().flat_map(|x| x.to_le_bytes()).collect();
    assert_eq!(
        v["7FE00001"],
        json!({"vr":"OV","InlineBinary":STANDARD.encode(offsets)})
    );
    assert_eq!(v["7FE00002"]["vr"], "OV");
}

#[test]
fn scan_one_fragment_per_frame() {
    let dir = tempfile::tempdir().unwrap();
    let frames = vec![vec![vec![1; 4]], vec![vec![2; 6]], vec![vec![3; 8]]];
    check_encapsulated(dir.path(), &frames, false, false, uids::JPEG_BASELINE8_BIT);
}

#[test]
fn scan_markers_group_fragments() {
    let dir = tempfile::tempdir().unwrap();
    // 3 frames in 7 fragments; each frame starts with JPEG SOI.
    let frames = vec![
        vec![vec![0xFF, 0xD8, 1, 1], vec![2, 2]],
        vec![vec![0xFF, 0xD8, 3, 3], vec![4, 4], vec![5, 5, 5, 5]],
        vec![vec![0xFF, 0xD8, 6, 6], vec![7, 7, 7, 7]],
    ];
    check_encapsulated(
        &dir.path().join("jpeg"),
        &frames,
        false,
        false,
        uids::JPEG_BASELINE8_BIT,
    );
    // Same with JPEG 2000 SOC + SIZ.
    let frames = vec![
        vec![vec![0xFF, 0x4F, 0xFF, 0x51, 1, 1], vec![2, 2]],
        vec![vec![0xFF, 0x4F, 0xFF, 0x51], vec![3, 3], vec![4, 4]],
    ];
    check_encapsulated(
        &dir.path().join("j2k"),
        &frames,
        false,
        false,
        uids::JPEG2000,
    );
}

#[test]
fn scan_single_frame_all_fragments() {
    let dir = tempfile::tempdir().unwrap();
    let frames = vec![vec![vec![1; 4], vec![2; 6], vec![3; 2]]];
    check_encapsulated(dir.path(), &frames, false, false, uids::JPEG_BASELINE8_BIT);
}

#[test]
fn scan_ambiguous_has_no_frame_table() {
    let dir = tempfile::tempdir().unwrap();
    let frames = vec![vec![vec![1; 4], vec![2; 4]], vec![vec![3; 4]]];
    let p = support::encapsulated(dir.path(), &frames, false, false, uids::JPEG_BASELINE8_BIT);
    let scan = scan_instance(&p).unwrap();
    assert_eq!(scan.data.frames, None);
    assert_eq!(
        scan.data.problem.as_deref(),
        Some("frame boundaries unknown")
    );
    assert_eq!(metadata(&scan)["7FE00010"]["BulkDataURI"], "bulk/7FE00010");
}

#[test]
fn scan_big_endian_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let p = support::big_endian(dir.path());
    let scan = scan_instance(&p).unwrap();
    assert_eq!(scan.data.problem, None);
    assert!(scan.data.offsets_valid);
    assert_eq!(scan.header.transfer_syntax, support::EXPLICIT_VR_BIG_ENDIAN);
    assert_eq!(shared(scan.header.clone()), read_header(&p).unwrap());
    // Values are decoded with the right byte order.
    let v = metadata(&scan);
    assert_eq!(v["00280010"], json!({"vr":"US","Value":[2]}));
    assert_eq!(v["00280011"], json!({"vr":"US","Value":[3]}));
    // Frames are the stored big-endian bytes.
    let px = support::big_endian_pixels();
    let frames = frames_at(&p, &scan);
    assert_eq!(frames.len(), 2);
    for (k, frame) in frames.iter().enumerate() {
        let want: Vec<u8> = px[k * 6..(k + 1) * 6]
            .iter()
            .flat_map(|x| x.to_be_bytes())
            .collect();
        assert_eq!(frame, &want, "frame {k}");
    }
}

#[test]
fn scan_deflated_has_metadata_no_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let p = support::deflated(dir.path());
    let scan = scan_instance(&p).unwrap();
    assert!(!scan.data.offsets_valid);
    assert_eq!(scan.data.frames, None);
    assert!(scan.data.bulk.is_empty());
    assert_eq!(scan.data.problem, None);
    assert_eq!(scan.header, read_header(&p).unwrap());
    let v = metadata(&scan);
    assert_eq!(v["00100010"]["Value"][0]["Alphabetic"], "SCAN^TEST");
    assert_eq!(v["00280008"], json!({"vr":"IS","Value":[2]}));
    assert_eq!(v["7FE00010"]["BulkDataURI"], "bulk/7FE00010");
}

#[test]
fn scan_bits_allocated_1() {
    let dir = tempfile::tempdir().unwrap();
    // 15 bits per frame: frames 1 and 2 do not start on a byte boundary.
    let p = support::bits1(dir.path(), 3, 3, 5);
    let scan = scan_instance(&p).unwrap();
    assert_eq!(scan.data.problem, None);
    match scan.data.frames {
        Some(FrameTable::Bits {
            frame_bits, count, ..
        }) => assert_eq!((frame_bits, count), (15, 3)),
        ref other => panic!("{other:?}"),
    }
    let info = scan.header.frame_info.clone().unwrap();
    for (k, frame) in frames_at(&p, &scan).iter().enumerate() {
        let old = read_frame_bytes(&p, k as u32, &info).unwrap();
        assert_eq!(&frame[..], &old[..], "frame {k}");
        assert_eq!(frame.len(), 2);
    }
}

#[test]
fn scan_bulk_rule() {
    let dir = tempfile::tempdir().unwrap();
    let big = pixel_bytes(1025);
    let small = vec![0xABu8; 1024];
    let text = "a".repeat(70_000);
    let p = support::with_elements(
        dir.path(),
        vec![
            creator(),
            private(0x1001, VR::OB, PrimitiveValue::from(big.clone())),
            private(0x1002, VR::OB, PrimitiveValue::from(small.clone())),
            private(0x1003, VR::UT, PrimitiveValue::from(text.clone())),
            private(0x1004, VR::LO, PrimitiveValue::from("short")),
        ],
    );
    let scan = scan_instance(&p).unwrap();
    assert_eq!(scan.data.problem, None);
    let v = metadata(&scan);

    let paths: Vec<&str> = scan.data.bulk.iter().map(|b| b.path.as_str()).collect();
    assert_eq!(paths, ["00091001", "00091003"]);

    let ob = &scan.data.bulk[0];
    // The stored value is padded to an even length.
    assert_eq!(ob.len, 1026);
    assert_eq!(file_bytes(&p, ob.offset, 1025), big);
    assert_eq!(
        v["00091001"],
        json!({"vr":"OB","BulkDataURI":"bulk/00091001"})
    );

    assert_eq!(
        v["00091002"],
        json!({"vr":"OB","InlineBinary":STANDARD.encode(&small)})
    );

    let ut = &scan.data.bulk[1];
    assert_eq!(ut.len, 70_000);
    assert_eq!(file_bytes(&p, ut.offset, ut.len), text.as_bytes());
    assert_eq!(
        v["00091003"],
        json!({"vr":"UT","BulkDataURI":"bulk/00091003"})
    );

    assert_eq!(v["00091004"], json!({"vr":"LO","Value":["short"]}));
    assert_eq!(v["7FE00010"]["BulkDataURI"], "bulk/7FE00010");
}

#[test]
fn scan_nested_bulk_path() {
    let dir = tempfile::tempdir().unwrap();
    let value = pixel_bytes(2000);
    let item0 = InMemDicomObject::from_element_iter([private(
        0x1002,
        VR::OB,
        PrimitiveValue::from(vec![1u8; 4]),
    )]);
    let item1 = InMemDicomObject::from_element_iter([private(
        0x1002,
        VR::OB,
        PrimitiveValue::from(value.clone()),
    )]);
    let p = support::with_elements(
        dir.path(),
        vec![
            creator(),
            DataElement::new(
                Tag(0x0009, 0x1001),
                VR::SQ,
                DataSetSequence::from(vec![item0, item1]),
            ),
        ],
    );
    let scan = scan_instance(&p).unwrap();
    assert_eq!(scan.data.problem, None);
    assert_eq!(scan.data.bulk.len(), 1);
    let b = &scan.data.bulk[0];
    assert_eq!(b.path, "00091001/1/00091002");
    assert_eq!(b.len, 2000);
    assert_eq!(file_bytes(&p, b.offset, b.len), value);
    let v = metadata(&scan);
    assert_eq!(
        v["00091001"]["Value"][0]["00091002"],
        json!({"vr":"OB","InlineBinary":"AQEBAQ=="})
    );
    assert_eq!(
        v["00091001"]["Value"][1]["00091002"],
        json!({"vr":"OB","BulkDataURI":"bulk/00091001/1/00091002"})
    );
}

#[test]
fn scan_truncated_pixel_data_is_problem_not_error() {
    let dir = tempfile::tempdir().unwrap();
    let p = support::native(dir.path(), 2, 4, 4, 16);
    let bytes = std::fs::read(&p).unwrap();
    std::fs::write(&p, &bytes[..bytes.len() - 10]).unwrap();
    assert!(read_header(&p).is_ok());
    let scan = scan_instance(&p).unwrap();
    assert_eq!(scan.data.problem.as_deref(), Some("pixel data truncated"));
    assert_eq!(scan.data.frames, None);
    assert_eq!(metadata(&scan)["00280008"], json!({"vr":"IS","Value":[2]}));
}

#[test]
fn scan_short_pixel_data_has_no_frame_table() {
    let dir = tempfile::tempdir().unwrap();
    // Three frames announced, pixel data for one.
    let p = support::with_elements(
        dir.path(),
        vec![DataElement::new(
            tags::NUMBER_OF_FRAMES,
            VR::IS,
            PrimitiveValue::from("3"),
        )],
    );
    let scan = scan_instance(&p).unwrap();
    assert_eq!(scan.data.frames, None);
    assert_eq!(scan.data.problem.as_deref(), Some("pixel data too short"));
    assert!(scan.data.metadata.is_some());
}

#[test]
fn scan_error_inside_sequence_keeps_valid_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let item = InMemDicomObject::from_element_iter([
        DataElement::new(
            tags::REQUESTED_PROCEDURE_DESCRIPTION,
            VR::LO,
            PrimitiveValue::from("FIRST"),
        ),
        DataElement::new(
            tags::REQUESTED_PROCEDURE_ID,
            VR::SH,
            PrimitiveValue::from("SECOND"),
        ),
    ]);
    let p = support::with_elements(
        dir.path(),
        vec![DataElement::new(
            tags::REQUEST_ATTRIBUTES_SEQUENCE,
            VR::SQ,
            DataSetSequence::from(vec![item]),
        )],
    );
    // Cut the file two bytes into the header of the item's second element.
    let bytes = std::fs::read(&p).unwrap();
    let at = bytes.windows(6).position(|w| w == b"SECOND").unwrap();
    std::fs::write(&p, &bytes[..at - 8 + 2]).unwrap();

    assert!(read_header(&p).is_ok());
    let scan = scan_instance(&p).unwrap();
    assert!(scan.data.problem.is_some());
    assert_eq!(scan.data.frames, None);
    let v = metadata(&scan);
    assert_eq!(
        v["00400275"],
        json!({"vr":"SQ","Value":[{"00321060":{"vr":"LO","Value":["FIRST"]}}]})
    );
    assert_eq!(v["00100010"]["Value"][0]["Alphabetic"], "SCAN^TEST");
}

/// A UI value padded to even length.
fn ui(tag: Tag, uid: &str) -> Vec<u8> {
    let mut v = uid.as_bytes().to_vec();
    if v.len() % 2 == 1 {
        v.push(0);
    }
    ele(tag, b"UI", &v)
}

#[test]
fn scan_problem_read_header_would_skip_is_not_an_error() {
    let dir = tempfile::tempdir().unwrap();
    // A defined-length private sequence whose only item is too short to hold
    // an element header. read_header skips the sequence whole.
    let bad_item = item(4, &[0x08, 0x00, 0x20, 0x00]);
    let mut ds = Vec::new();
    ds.extend(ui(
        tags::SOP_CLASS_UID,
        uids::SECONDARY_CAPTURE_IMAGE_STORAGE,
    ));
    ds.extend(ui(tags::SOP_INSTANCE_UID, support::SOP_UID));
    ds.extend(ele(Tag(0x0009, 0x0010), b"LO", b"SLICESTREAM TEST"));
    ds.extend(ele_long(
        Tag(0x0009, 0x1001),
        b"SQ",
        bad_item.len() as u32,
        &bad_item,
    ));
    ds.extend(ele(tags::STUDY_TIME, b"TM", b"101112"));
    ds.extend(ui(tags::STUDY_INSTANCE_UID, support::STUDY_UID));
    ds.extend(ui(tags::SERIES_INSTANCE_UID, support::SERIES_UID));
    let p = support::raw(dir.path(), &ds);

    let expected = read_header(&p).unwrap();
    let scan = scan_instance(&p).unwrap();
    assert_eq!(shared(scan.header.clone()), expected);
    assert!(scan.data.problem.is_some());
    assert_eq!(scan.data.frames, None);
    let v = metadata(&scan);
    assert_eq!(v["00080018"], json!({"vr":"UI","Value":[support::SOP_UID]}));
}

#[test]
fn scan_defined_length_sequence() {
    let dir = tempfile::tempdir().unwrap();
    // A private sequence and its items all with defined lengths, the second
    // item holding a bulk value.
    let value = pixel_bytes(1100);
    let item0 = item(12, &ele(Tag(0x0009, 0x1003), b"LO", b"ZERO"));
    let item1 = item(1112, &ele_long(Tag(0x0009, 0x1002), b"OB", 1100, &value));
    let items = [item0, item1].concat();
    let mut ds = Vec::new();
    ds.extend(ui(
        tags::SOP_CLASS_UID,
        uids::SECONDARY_CAPTURE_IMAGE_STORAGE,
    ));
    ds.extend(ui(tags::SOP_INSTANCE_UID, support::SOP_UID));
    ds.extend(ele(Tag(0x0009, 0x0010), b"LO", b"SLICESTREAM TEST"));
    ds.extend(ele_long(
        Tag(0x0009, 0x1001),
        b"SQ",
        items.len() as u32,
        &items,
    ));
    ds.extend(ui(tags::STUDY_INSTANCE_UID, support::STUDY_UID));
    ds.extend(ui(tags::SERIES_INSTANCE_UID, support::SERIES_UID));
    let p = support::raw(dir.path(), &ds);

    let scan = scan_instance(&p).unwrap();
    assert_eq!(scan.data.problem, None);
    assert_eq!(scan.header, read_header(&p).unwrap());
    let b = &scan.data.bulk[..];
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].path, "00091001/1/00091002");
    assert_eq!(file_bytes(&p, b[0].offset, b[0].len), value);
    let v = metadata(&scan);
    assert_eq!(
        v["00091001"],
        json!({"vr":"SQ","Value":[
            {"00091003":{"vr":"LO","Value":["ZERO"]}},
            {"00091002":{"vr":"OB","BulkDataURI":"bulk/00091001/1/00091002"}}
        ]})
    );
    // Elements after the sequence are still read.
    assert_eq!(v["0020000E"]["Value"][0], support::SERIES_UID);
}

#[test]
fn scan_rejects_what_read_header_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let junk = dir.path().join("junk.dcm");
    std::fs::write(&junk, b"this is definitely not a dicom file").unwrap();

    let mut ds = Vec::new();
    ds.extend(ui(
        tags::SOP_CLASS_UID,
        uids::SECONDARY_CAPTURE_IMAGE_STORAGE,
    ));
    ds.extend(ui(tags::STUDY_INSTANCE_UID, support::STUDY_UID));
    ds.extend(ui(tags::SERIES_INSTANCE_UID, support::SERIES_UID));
    let missing = support::raw(dir.path(), &ds);

    for p in [junk, missing] {
        let want = format!("{:?}", read_header(&p).unwrap_err());
        let got = format!("{:?}", scan_instance(&p).unwrap_err());
        assert_eq!(got, want);
    }
}

#[test]
fn scan_metadata_cap() {
    let dir = tempfile::tempdir().unwrap();
    let p = support::with_elements(
        dir.path(),
        vec![
            DataElement::new(
                tags::STUDY_DESCRIPTION,
                VR::LO,
                PrimitiveValue::from("x".repeat(600)),
            ),
            DataElement::new(
                tags::SERIES_DESCRIPTION,
                VR::LO,
                PrimitiveValue::from("y".repeat(600)),
            ),
        ],
    );
    let full = scan_instance(&p).unwrap();
    let size = full.data.metadata.as_ref().unwrap().len();
    assert!(size > 1024, "{size}");

    let capped = scan_instance_with(
        &p,
        &ScanLimits {
            max_metadata_bytes: 1024,
            ..ScanLimits::default()
        },
    )
    .unwrap();
    assert_eq!(capped.data.metadata, None);
    assert_eq!(
        capped.data.problem,
        Some(format!("metadata too large ({size} bytes)"))
    );
    assert!(capped.data.frames.is_some());
    assert_eq!(capped.data.frames, full.data.frames);
    assert_eq!(capped.data.bulk, full.data.bulk);
    assert_eq!(capped.header, full.header);
}

#[test]
fn scan_latin1_patient_name() {
    let dir = tempfile::tempdir().unwrap();
    let latin1 = b"M\xDCLLER^HANS".to_vec();
    let p = support::with_elements(
        dir.path(),
        vec![
            DataElement::new(
                tags::SPECIFIC_CHARACTER_SET,
                VR::CS,
                PrimitiveValue::from("ISO_IR 100"),
            ),
            DataElement::new(
                tags::PATIENT_NAME,
                VR::PN,
                PrimitiveValue::from(latin1.clone()),
            ),
        ],
    );
    let bytes = std::fs::read(&p).unwrap();
    assert!(bytes.windows(latin1.len()).any(|w| w == latin1));

    let scan = scan_instance(&p).unwrap();
    let raw = String::from_utf8(scan.data.metadata.clone().unwrap()).unwrap();
    assert!(raw.contains(r#""Alphabetic":"MÜLLER^HANS""#), "{raw}");
    assert_eq!(scan.header.patient_name.as_deref(), Some("MÜLLER^HANS"));
}

#[test]
fn scan_item_character_set_does_not_leak() {
    let dir = tempfile::tempdir().unwrap();
    // An item that declares UTF-8, before a Latin-1 Patient Name.
    let item = InMemDicomObject::from_element_iter([DataElement::new(
        tags::SPECIFIC_CHARACTER_SET,
        VR::CS,
        PrimitiveValue::from("ISO_IR 192"),
    )]);
    let p = support::with_elements(
        dir.path(),
        vec![
            DataElement::new(
                tags::SPECIFIC_CHARACTER_SET,
                VR::CS,
                PrimitiveValue::from("ISO_IR 100"),
            ),
            DataElement::new(
                tags::REFERENCED_STUDY_SEQUENCE,
                VR::SQ,
                DataSetSequence::from(vec![item]),
            ),
            DataElement::new(
                tags::PATIENT_NAME,
                VR::PN,
                PrimitiveValue::from(b"M\xDCLLER^HANS".to_vec()),
            ),
        ],
    );
    let scan = scan_instance(&p).unwrap();
    assert_eq!(scan.data.problem, None);
    assert_eq!(scan.header, read_header(&p).unwrap());
    let v = metadata(&scan);
    assert_eq!(v["00100010"]["Value"][0]["Alphabetic"], "MÜLLER^HANS");
    assert_eq!(
        v["00081110"]["Value"][0]["00080005"],
        json!({"vr":"CS","Value":["ISO_IR 192"]})
    );
}

#[test]
fn scan_excludes_group_0002() {
    let dir = tempfile::tempdir().unwrap();
    let p = support::with_elements(
        dir.path(),
        vec![DataElement::new(
            tags::SOURCE_APPLICATION_ENTITY_TITLE,
            VR::AE,
            PrimitiveValue::from("ROGUE"),
        )],
    );
    // The element really is in the data set, not only in the meta group.
    let bytes = std::fs::read(&p).unwrap();
    assert!(bytes.windows(5).any(|w| w == b"ROGUE"));

    let scan = scan_instance(&p).unwrap();
    assert_eq!(scan.data.problem, None);
    let v = metadata(&scan);
    let keys: Vec<&String> = v.as_object().unwrap().keys().collect();
    assert!(keys.iter().all(|k| !k.starts_with("0002")), "{keys:?}");
    assert!(keys.iter().any(|k| *k == "00080018"));
}

#[test]
fn scan_qido_fields() {
    let dir = tempfile::tempdir().unwrap();
    let el = |tag, vr, v: &str| DataElement::new(tag, vr, PrimitiveValue::from(v));
    let p = support::with_elements(
        dir.path(),
        vec![
            el(tags::STUDY_TIME, VR::TM, "101112"),
            el(tags::STUDY_ID, VR::SH, "S1"),
            el(tags::REFERRING_PHYSICIAN_NAME, VR::PN, "REF^DOC"),
            el(tags::PATIENT_BIRTH_DATE, VR::DA, "19700101"),
            el(tags::PATIENT_SEX, VR::CS, "F"),
            el(
                tags::PERFORMED_PROCEDURE_STEP_START_DATE,
                VR::DA,
                "20240102",
            ),
            el(tags::PERFORMED_PROCEDURE_STEP_START_TIME, VR::TM, "131415"),
        ],
    );
    let h = scan_instance(&p).unwrap().header;
    assert_eq!(h.study_time.as_deref(), Some("101112"));
    assert_eq!(h.study_id.as_deref(), Some("S1"));
    assert_eq!(h.referring_physician_name.as_deref(), Some("REF^DOC"));
    assert_eq!(h.patient_birth_date.as_deref(), Some("19700101"));
    assert_eq!(h.patient_sex.as_deref(), Some("F"));
    assert_eq!(
        h.performed_procedure_step_start_date.as_deref(),
        Some("20240102")
    );
    assert_eq!(
        h.performed_procedure_step_start_time.as_deref(),
        Some("131415")
    );
    let old = read_header(&p).unwrap();
    assert_eq!(old.study_time, None);
    assert_eq!(shared(h), old);
}

#[test]
fn bulk_value_serializes() {
    let b = BulkValue {
        path: "00091001/1/00091002".into(),
        offset: 10,
        len: 2000,
    };
    let s = serde_json::to_string(&b).unwrap();
    assert_eq!(serde_json::from_str::<BulkValue>(&s).unwrap(), b);
}

/// The UIDs `read_header` needs, then `between`, then Study and Series UID.
fn minimal_with(between: &[u8]) -> Vec<u8> {
    let mut ds = Vec::new();
    ds.extend(ui(
        tags::SOP_CLASS_UID,
        uids::SECONDARY_CAPTURE_IMAGE_STORAGE,
    ));
    ds.extend(ui(tags::SOP_INSTANCE_UID, support::SOP_UID));
    ds.extend(ele(Tag(0x0009, 0x0010), b"LO", b"SLICESTREAM TEST"));
    ds.extend_from_slice(between);
    ds.extend(ui(tags::STUDY_INSTANCE_UID, support::STUDY_UID));
    ds.extend(ui(tags::SERIES_INSTANCE_UID, support::SERIES_UID));
    ds
}

/// Sequence delimitation item.
fn sequence_end() -> Vec<u8> {
    let mut out = vec![0xFE, 0xFF, 0xDD, 0xE0];
    out.extend_from_slice(&0u32.to_le_bytes());
    out
}

#[test]
fn scan_empty_items_stay_under_metadata_cap() {
    let dir = tempfile::tempdir().unwrap();
    // One private sequence of 200,000 empty items: 1.6 MB of file, about
    // 600 KB of JSON (`{},` per item) and no element inside any item.
    let mut sq = item(0, &[]).repeat(200_000);
    sq.extend(sequence_end());
    let p = support::raw(
        dir.path(),
        &minimal_with(&ele_long(Tag(0x0009, 0x1001), b"SQ", u32::MAX, &sq)),
    );
    let limits = ScanLimits {
        max_metadata_bytes: 1024,
        ..ScanLimits::default()
    };
    let (scan, trace) = scan_instance_traced(&p, &limits).unwrap();
    assert_eq!(shared(scan.header.clone()), read_header(&p).unwrap());
    assert_eq!(scan.data.metadata, None);
    let problem = scan.data.problem.as_deref().unwrap_or_default();
    assert!(problem.starts_with("metadata too large ("), "{problem}");
    // Never more than one element or item past the cap.
    assert!(
        trace.peak_metadata_bytes <= 1024 + 64,
        "buffered {} bytes of JSON against a 1024-byte cap",
        trace.peak_metadata_bytes
    );
    // The full scan still has it all.
    let full = scan_instance(&p).unwrap();
    assert_eq!(full.data.problem, None);
    assert_eq!(
        metadata(&full)["00091001"]["Value"]
            .as_array()
            .unwrap()
            .len(),
        200_000
    );
}

#[test]
fn scan_nested_bulk_values_stay_under_metadata_cap() {
    let dir = tempfile::tempdir().unwrap();
    // 20,000 items, each with an empty Pixel Data element: every one is a
    // bulk value with a path, from 20 bytes of file.
    let px = ele_long(tags::PIXEL_DATA, b"OB", 0, &[]);
    let mut sq = item(px.len() as u32, &px).repeat(20_000);
    sq.extend(sequence_end());
    let p = support::raw(
        dir.path(),
        &minimal_with(&ele_long(Tag(0x0009, 0x1001), b"SQ", u32::MAX, &sq)),
    );
    let full = scan_instance(&p).unwrap();
    assert_eq!(full.data.bulk.len(), 20_000);
    assert_eq!(full.data.bulk[19_999].path, "00091001/19999/7FE00010");

    let limits = ScanLimits {
        max_metadata_bytes: 1024,
        ..ScanLimits::default()
    };
    let (scan, trace) = scan_instance_traced(&p, &limits).unwrap();
    assert_eq!(scan.data.metadata, None);
    // No metadata to find bulk paths in: no bulk list either.
    assert!(
        scan.data.bulk.is_empty(),
        "{} bulk values",
        scan.data.bulk.len()
    );
    assert!(trace.peak_metadata_bytes <= 1024 + 128, "{trace:?}");
}

#[test]
fn scan_too_many_fragments_has_no_frame_table() {
    let dir = tempfile::tempdir().unwrap();
    // One frame in 1,000 empty fragments: 8 KB of file, 16 KB of locations.
    let frames = vec![vec![Vec::new(); 1000]];
    let p = support::encapsulated(dir.path(), &frames, false, false, uids::JPEG_BASELINE8_BIT);
    let full = scan_instance(&p).unwrap();
    assert_eq!(full.data.problem, None);
    assert_eq!(full.data.frames.as_ref().map(FrameTable::count), Some(1));

    let limits = ScanLimits {
        max_fragments: 100,
        ..ScanLimits::default()
    };
    let (scan, trace) = scan_instance_traced(&p, &limits).unwrap();
    // Still accepted, with metadata; only the frame table is missing.
    assert_eq!(scan.header, read_header(&p).unwrap());
    assert_eq!(scan.data.frames, None);
    assert_eq!(
        scan.data.problem.as_deref(),
        Some("too many fragments (1000)")
    );
    assert_eq!(metadata(&scan)["7FE00010"]["BulkDataURI"], "bulk/7FE00010");
    assert!(trace.fragments_kept <= 100, "{trace:?}");
}

#[test]
fn scan_offset_tables_read_only_within_fragment_cap() {
    let dir = tempfile::tempdir().unwrap();
    // 150 one-fragment frames with a Basic and an Extended Offset Table.
    let frames: Vec<Vec<Vec<u8>>> = (0..150u8).map(|i| vec![vec![i; 2]]).collect();
    let p = support::encapsulated(dir.path(), &frames, true, true, uids::JPEG_BASELINE8_BIT);
    let (full, trace) = scan_instance_traced(&p, &ScanLimits::default()).unwrap();
    assert_eq!(full.data.problem, None);
    assert_eq!(trace.table_bytes, 150 * (4 + 8 + 8));

    let limits = ScanLimits {
        max_fragments: 100,
        ..ScanLimits::default()
    };
    let (scan, trace) = scan_instance_traced(&p, &limits).unwrap();
    assert_eq!(trace.table_bytes, 0, "tables for more frames than the cap");
    assert_eq!(scan.data.frames, None);
    assert_eq!(
        scan.data.problem.as_deref(),
        Some("too many fragments (150)")
    );
    // The Extended Offset Table is then an ordinary bulk value.
    let v = metadata(&scan);
    assert_eq!(v["7FE00001"]["BulkDataURI"], "bulk/7FE00001");
}

#[test]
fn scan_repeated_problems_recorded_once() {
    let dir = tempfile::tempdir().unwrap();
    let us = |tag, v: u16| ele(tag, b"US", &v.to_le_bytes());
    let mut ds = Vec::new();
    ds.extend(ui(
        tags::SOP_CLASS_UID,
        uids::SECONDARY_CAPTURE_IMAGE_STORAGE,
    ));
    ds.extend(ui(tags::SOP_INSTANCE_UID, support::SOP_UID));
    ds.extend(ui(tags::STUDY_INSTANCE_UID, support::STUDY_UID));
    ds.extend(ui(tags::SERIES_INSTANCE_UID, support::SERIES_UID));
    ds.extend(us(tags::SAMPLES_PER_PIXEL, 1));
    ds.extend(ele(
        tags::PHOTOMETRIC_INTERPRETATION,
        b"CS",
        b"MONOCHROME2 ",
    ));
    ds.extend(us(tags::ROWS, 4));
    ds.extend(us(tags::COLUMNS, 4));
    ds.extend(us(tags::BITS_ALLOCATED, 16));
    ds.extend(us(tags::BITS_STORED, 16));
    ds.extend(us(tags::PIXEL_REPRESENTATION, 0));
    // 50,000 empty Pixel Data elements, each one too short for its frame.
    ds.extend(ele_long(tags::PIXEL_DATA, b"OW", 0, &[]).repeat(50_000));
    let p = support::raw(dir.path(), &ds);
    let scan = scan_instance(&p).unwrap();
    assert_eq!(scan.header, read_header(&p).unwrap());
    assert_eq!(scan.data.problem.as_deref(), Some("pixel data too short"));
}
