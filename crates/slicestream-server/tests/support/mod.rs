//! Fixtures shared by the integration tests: synthesized DICOM, NIfTI and
//! archives (no real patient data).
#![allow(dead_code)]

use std::io::{Cursor, Write};

use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_object::{FileMetaTableBuilder, InMemDicomObject};
use flate2::Compression;
use flate2::write::GzEncoder;
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
    let study_uid = format!("2.25.9{patient}0{study}");
    let series_uid = format!("{study_uid}.{series}");
    let sop_uid = format!("{series_uid}.{inst}");
    let s = |v: &str| PrimitiveValue::from(v);
    let obj = InMemDicomObject::from_element_iter([
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
