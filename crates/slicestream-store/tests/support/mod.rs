//! Synthetic DICOM fixtures for the scan tests. No real patient data.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use dicom_core::value::PixelFragmentSequence;
use dicom_core::{DataElement, PrimitiveValue, Tag, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_object::{FileMetaTableBuilder, InMemDicomObject};

pub const STUDY_UID: &str = "2.25.300000000000000000000000000001";
pub const SERIES_UID: &str = "2.25.300000000000000000000000000002";
pub const SOP_UID: &str = "2.25.300000000000000000000000000003";
/// Explicit VR Big Endian (retired, so dicom-dictionary-std deprecates its constant).
pub const EXPLICIT_VR_BIG_ENDIAN: &str = "1.2.840.10008.1.2.2";

/// Image attributes of a monochrome multi-frame instance, without Pixel Data.
pub fn base(frames: u32, rows: u16, cols: u16, bits: u16) -> InMemDicomObject {
    let us = |tag, v: u16| DataElement::new(tag, VR::US, PrimitiveValue::from(v));
    InMemDicomObject::from_element_iter([
        DataElement::new(
            tags::SOP_CLASS_UID,
            VR::UI,
            PrimitiveValue::from(uids::MULTI_FRAME_GRAYSCALE_WORD_SECONDARY_CAPTURE_IMAGE_STORAGE),
        ),
        DataElement::new(
            tags::SOP_INSTANCE_UID,
            VR::UI,
            PrimitiveValue::from(SOP_UID),
        ),
        DataElement::new(tags::MODALITY, VR::CS, PrimitiveValue::from("OT")),
        DataElement::new(
            tags::PATIENT_NAME,
            VR::PN,
            PrimitiveValue::from("SCAN^TEST"),
        ),
        DataElement::new(tags::PATIENT_ID, VR::LO, PrimitiveValue::from("SCAN1")),
        DataElement::new(
            tags::STUDY_INSTANCE_UID,
            VR::UI,
            PrimitiveValue::from(STUDY_UID),
        ),
        DataElement::new(
            tags::SERIES_INSTANCE_UID,
            VR::UI,
            PrimitiveValue::from(SERIES_UID),
        ),
        DataElement::new(tags::INSTANCE_NUMBER, VR::IS, PrimitiveValue::from("1")),
        us(tags::SAMPLES_PER_PIXEL, 1),
        DataElement::new(
            tags::PHOTOMETRIC_INTERPRETATION,
            VR::CS,
            PrimitiveValue::from("MONOCHROME2"),
        ),
        DataElement::new(
            tags::NUMBER_OF_FRAMES,
            VR::IS,
            PrimitiveValue::from(frames.to_string()),
        ),
        us(tags::ROWS, rows),
        us(tags::COLUMNS, cols),
        us(tags::BITS_ALLOCATED, bits),
        us(tags::BITS_STORED, bits),
        us(tags::HIGH_BIT, bits - 1),
        us(tags::PIXEL_REPRESENTATION, 0),
    ])
}

/// Write `obj` with transfer syntax `ts` to `dir/name`.
pub fn write(dir: &Path, name: &str, obj: InMemDicomObject, ts: &str) -> PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let file = obj
        .with_meta(
            FileMetaTableBuilder::new()
                .transfer_syntax(ts)
                .media_storage_sop_class_uid(
                    uids::MULTI_FRAME_GRAYSCALE_WORD_SECONDARY_CAPTURE_IMAGE_STORAGE,
                )
                .media_storage_sop_instance_uid(SOP_UID),
        )
        .unwrap();
    let path = dir.join(name);
    file.write_to_file(&path).unwrap();
    path
}

/// Deterministic pixel bytes: every frame differs from the others.
pub fn pixel_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 7 + i / 13) as u8).collect()
}

/// Native little-endian multi-frame; `bits` is Bits Allocated (8 or 16).
pub fn native(dir: &Path, frames: u32, rows: u16, cols: u16, bits: u16) -> PathBuf {
    let len = frames as usize * rows as usize * cols as usize * (bits as usize / 8);
    let mut obj = base(frames, rows, cols, bits);
    let vr = if bits > 8 { VR::OW } else { VR::OB };
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        vr,
        PrimitiveValue::from(pixel_bytes(len)),
    ));
    write(dir, "native.dcm", obj, uids::EXPLICIT_VR_LITTLE_ENDIAN)
}

/// Encapsulated pixel data: `frames[k]` holds the fragments of frame `k`.
/// `bot` fills the Basic Offset Table, `eot` adds the Extended Offset Table
/// and its lengths.
pub fn encapsulated(
    dir: &Path,
    frames: &[Vec<Vec<u8>>],
    bot: bool,
    eot: bool,
    ts: &str,
) -> PathBuf {
    let mut obj = base(frames.len() as u32, 16, 16, 8);
    let mut offsets = Vec::new();
    let mut lengths = Vec::new();
    let mut at = 0u64;
    for frame in frames {
        offsets.push(at);
        lengths.push(frame.iter().map(|f| f.len() as u64).sum::<u64>());
        at += frame.iter().map(|f| 8 + f.len() as u64).sum::<u64>();
    }
    if eot {
        let le = |v: &[u64]| v.iter().flat_map(|x| x.to_le_bytes()).collect::<Vec<u8>>();
        obj.put(DataElement::new(
            tags::EXTENDED_OFFSET_TABLE,
            VR::OV,
            PrimitiveValue::from(le(&offsets)),
        ));
        obj.put(DataElement::new(
            tags::EXTENDED_OFFSET_TABLE_LENGTHS,
            VR::OV,
            PrimitiveValue::from(le(&lengths)),
        ));
    }
    let table: Vec<u32> = if bot {
        offsets.iter().map(|&o| o as u32).collect()
    } else {
        Vec::new()
    };
    let fragments: Vec<Vec<u8>> = frames.iter().flatten().cloned().collect();
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        VR::OB,
        PixelFragmentSequence::new(table, fragments),
    ));
    write(dir, "encapsulated.dcm", obj, ts)
}

/// Two 3x2 frames of 16-bit pixels, Explicit VR Big Endian. The pixel values
/// are [`big_endian_pixels`].
pub fn big_endian(dir: &Path) -> PathBuf {
    let mut obj = base(2, 2, 3, 16);
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        VR::OW,
        PrimitiveValue::U16(big_endian_pixels().into_iter().collect()),
    ));
    write(dir, "big_endian.dcm", obj, EXPLICIT_VR_BIG_ENDIAN)
}

pub fn big_endian_pixels() -> Vec<u16> {
    (0..12u16).map(|i| 0x0102 + i * 0x0101).collect()
}

/// Two 4x4 16-bit frames, Deflated Explicit VR Little Endian. Written by
/// hand (the store does not read or write deflated data sets): the Explicit
/// VR Little Endian data set as raw DEFLATE stored blocks.
pub fn deflated(dir: &Path) -> PathBuf {
    let mut obj = base(2, 4, 4, 16);
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        VR::OW,
        PrimitiveValue::from(pixel_bytes(2 * 4 * 4 * 2)),
    ));
    let mut dataset = Vec::new();
    obj.write_dataset_with_ts(
        &mut dataset,
        &dicom_transfer_syntax_registry::entries::EXPLICIT_VR_LITTLE_ENDIAN.erased(),
    )
    .unwrap();
    let meta = FileMetaTableBuilder::new()
        .transfer_syntax(uids::DEFLATED_EXPLICIT_VR_LITTLE_ENDIAN)
        .media_storage_sop_class_uid(
            uids::MULTI_FRAME_GRAYSCALE_WORD_SECONDARY_CAPTURE_IMAGE_STORAGE,
        )
        .media_storage_sop_instance_uid(SOP_UID)
        .build()
        .unwrap();
    let mut bytes = vec![0u8; 128];
    bytes.extend_from_slice(b"DICM");
    meta.write(&mut bytes).unwrap();
    bytes.extend(stored_deflate(&dataset));
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join("deflated.dcm");
    std::fs::write(&path, bytes).unwrap();
    path
}

/// `data` as a raw DEFLATE stream (RFC 1951) of uncompressed blocks.
fn stored_deflate(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut blocks = data.chunks(0xFFFF).peekable();
    if blocks.peek().is_none() {
        return vec![1, 0, 0, 0xFF, 0xFF];
    }
    while let Some(block) = blocks.next() {
        // BFINAL on the last block, BTYPE 00 (stored).
        out.push(u8::from(blocks.peek().is_none()));
        let len = block.len() as u16;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&(!len).to_le_bytes());
        out.extend_from_slice(block);
    }
    out
}

/// Bits Allocated 1: frames are bit-packed back to back.
pub fn bits1(dir: &Path, frames: u32, rows: u16, cols: u16) -> PathBuf {
    let bits = frames as usize * rows as usize * cols as usize;
    let mut packed = pixel_bytes(bits.div_ceil(8));
    // Pad to an even length, as a writer would.
    if packed.len() % 2 == 1 {
        packed.push(0);
    }
    let mut obj = base(frames, rows, cols, 1);
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        VR::OW,
        PrimitiveValue::from(packed),
    ));
    write(dir, "bits1.dcm", obj, uids::EXPLICIT_VR_LITTLE_ENDIAN)
}

/// A one-frame 4x4 16-bit native instance with `extra` elements added.
pub fn with_elements(dir: &Path, extra: Vec<DataElement<InMemDicomObject>>) -> PathBuf {
    let mut obj = base(1, 4, 4, 16);
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        VR::OW,
        PrimitiveValue::from(pixel_bytes(4 * 4 * 2)),
    ));
    for e in extra {
        obj.put(e);
    }
    write(dir, "extra.dcm", obj, uids::EXPLICIT_VR_LITTLE_ENDIAN)
}

/// A file whose data set is `dataset`, written by hand after a regular
/// Explicit VR Little Endian file meta group.
pub fn raw(dir: &Path, dataset: &[u8]) -> PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let meta = FileMetaTableBuilder::new()
        .transfer_syntax(uids::EXPLICIT_VR_LITTLE_ENDIAN)
        .media_storage_sop_class_uid(uids::SECONDARY_CAPTURE_IMAGE_STORAGE)
        .media_storage_sop_instance_uid(SOP_UID)
        .build()
        .unwrap();
    let mut bytes = vec![0u8; 128];
    bytes.extend_from_slice(b"DICM");
    meta.write(&mut bytes).unwrap();
    bytes.extend_from_slice(dataset);
    let path = dir.join("raw.dcm");
    std::fs::write(&path, bytes).unwrap();
    path
}

/// One Explicit VR Little Endian element with a short (2-byte) length field.
pub fn ele(tag: Tag, vr: &[u8; 2], value: &[u8]) -> Vec<u8> {
    let mut out = tag_bytes(tag);
    out.extend_from_slice(vr);
    out.extend_from_slice(&(value.len() as u16).to_le_bytes());
    out.extend_from_slice(value);
    out
}

/// One Explicit VR Little Endian element with a long (4-byte) length field
/// (OB, OW, SQ, UN, UT, ...). `len` is written as given.
pub fn ele_long(tag: Tag, vr: &[u8; 2], len: u32, value: &[u8]) -> Vec<u8> {
    let mut out = tag_bytes(tag);
    out.extend_from_slice(vr);
    out.extend_from_slice(&[0, 0]);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value);
    out
}

/// Item header with the given length, followed by `value`.
pub fn item(len: u32, value: &[u8]) -> Vec<u8> {
    let mut out = tag_bytes(Tag(0xFFFE, 0xE000));
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value);
    out
}

fn tag_bytes(tag: Tag) -> Vec<u8> {
    let mut out = tag.group().to_le_bytes().to_vec();
    out.extend_from_slice(&tag.element().to_le_bytes());
    out
}

/// Attributes of a synthesized instance for query tests. Empty strings are left out.
#[derive(Default, Clone)]
pub struct Spec<'a> {
    pub patient_name: &'a str,
    pub patient_id: &'a str,
    pub study_uid: &'a str,
    pub series_uid: &'a str,
    pub sop_uid: &'a str,
    pub modality: &'a str,
    pub study_date: &'a str,
    pub study_time: &'a str,
    pub study_description: &'a str,
    pub accession: &'a str,
    pub referring: &'a str,
    pub series_number: Option<i64>,
    pub instance_number: Option<i64>,
}

/// A one-frame instance with the attributes of `spec`, written to `dir/<sop uid>.dcm`.
pub fn instance(dir: &Path, spec: &Spec) -> PathBuf {
    let mut obj = base(1, 4, 4, 16);
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        VR::OW,
        PrimitiveValue::from(pixel_bytes(4 * 4 * 2)),
    ));
    let mut put = |tag, vr, v: &str| {
        if !v.is_empty() {
            obj.put(DataElement::new(tag, vr, PrimitiveValue::from(v)));
        }
    };
    put(tags::PATIENT_NAME, VR::PN, spec.patient_name);
    put(tags::PATIENT_ID, VR::LO, spec.patient_id);
    put(tags::STUDY_INSTANCE_UID, VR::UI, spec.study_uid);
    put(tags::SERIES_INSTANCE_UID, VR::UI, spec.series_uid);
    put(tags::SOP_INSTANCE_UID, VR::UI, spec.sop_uid);
    put(tags::MODALITY, VR::CS, spec.modality);
    put(tags::STUDY_DATE, VR::DA, spec.study_date);
    put(tags::STUDY_TIME, VR::TM, spec.study_time);
    put(tags::STUDY_DESCRIPTION, VR::LO, spec.study_description);
    put(tags::ACCESSION_NUMBER, VR::SH, spec.accession);
    put(tags::REFERRING_PHYSICIAN_NAME, VR::PN, spec.referring);
    if let Some(n) = spec.series_number {
        put(tags::SERIES_NUMBER, VR::IS, &n.to_string());
    }
    if let Some(n) = spec.instance_number {
        put(tags::INSTANCE_NUMBER, VR::IS, &n.to_string());
    }
    let file = obj
        .with_meta(
            FileMetaTableBuilder::new()
                .transfer_syntax(uids::EXPLICIT_VR_LITTLE_ENDIAN)
                .media_storage_sop_class_uid(
                    uids::MULTI_FRAME_GRAYSCALE_WORD_SECONDARY_CAPTURE_IMAGE_STORAGE,
                )
                .media_storage_sop_instance_uid(spec.sop_uid),
        )
        .unwrap();
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join(format!("{}.dcm", spec.sop_uid));
    file.write_to_file(&path).unwrap();
    path
}

pub async fn index_pool(root: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect(&format!("sqlite://{}", root.join("index.sqlite").display()))
        .await
        .unwrap()
}

/// Ingest `n` phantom slices through the pipeline; returns instance ids.
pub async fn ingest_slices(ing: &slicestream_store::Ingestor, src: &Path, n: usize) -> Vec<i64> {
    crate::phantom::write_phantom(src, n, 16).unwrap();
    for i in 1..=n {
        ing.import_file(&src.join(format!("IM{i:05}.dcm")))
            .await
            .unwrap();
    }
    let ids = sqlx::query_scalar::<_, i64>("SELECT id FROM instances ORDER BY id")
        .fetch_all(&index_pool(&ing.config().root).await)
        .await
        .unwrap();
    assert_eq!(ids.len(), n);
    ids
}
