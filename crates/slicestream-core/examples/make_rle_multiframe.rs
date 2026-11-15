//! Generate one synthetic multi-frame CT instance in RLE Lossless, so
//! DICOMweb clients and viewers can be tried on encapsulated data without
//! real patient data.
//!
//! ```sh
//! cargo run -p slicestream-core --example make_rle_multiframe -- data/rle 16
//! ```
//!
//! Frame `k` holds the pixels of phantom slice `k` (256 x 256). Each frame is
//! one fragment, and the Basic Offset Table points at each one. The
//! encoding follows PS3.5 Annex G: a 64-byte header, then one PackBits
//! segment for the high bytes and one for the low bytes of every pixel.
//! The instance is its own patient and study, with a Patient ID, Accession
//! Number, Study Date and Study Description that the phantom does not have.

#[allow(dead_code)]
#[path = "make_phantom.rs"]
pub mod phantom;

use std::path::{Path, PathBuf};

use dicom_core::value::PixelFragmentSequence;
use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_object::{FileMetaTableBuilder, InMemDicomObject};

pub const RLE_LOSSLESS: &str = "1.2.840.10008.1.2.5";

/// PackBits-encode one row (PS3.5 G.3.1) and append it to `out`. Runs of
/// three or more equal bytes become replicate runs; the rest is literal.
pub fn pack_bits(row: &[u8], out: &mut Vec<u8>) {
    let run_at = |i: usize| {
        let mut n = 1;
        while i + n < row.len() && n < 128 && row[i + n] == row[i] {
            n += 1;
        }
        n
    };
    let mut i = 0;
    while i < row.len() {
        let run = run_at(i);
        if run >= 3 {
            // A replicate run of n bytes has header 1 - n (-2 ..= -127).
            out.push((1 - run as i32) as i8 as u8);
            out.push(row[i]);
            i += run;
            continue;
        }
        let start = i;
        while i < row.len() && i - start < 128 && run_at(i) < 3 {
            i += 1;
        }
        // A literal run of n bytes has header n - 1 (0 ..= 127).
        out.push((i - start - 1) as u8);
        out.extend_from_slice(&row[start..i]);
    }
}

/// Encode one frame of 16-bit, one-sample, little-endian pixels.
fn rle_frame(pixels: &[u8], columns: usize) -> Vec<u8> {
    let mut segments = [Vec::new(), Vec::new()];
    // Segment 1 holds the most significant byte of each pixel.
    for (seg, byte) in segments.iter_mut().zip([1, 0]) {
        let plane: Vec<u8> = pixels.iter().skip(byte).step_by(2).copied().collect();
        // Each row is encoded on its own; no run crosses a row boundary.
        for row in plane.chunks(columns) {
            pack_bits(row, seg);
        }
        // Each segment has an even length.
        if seg.len() % 2 == 1 {
            seg.push(0);
        }
    }
    let mut header = [0_u32; 16];
    header[0] = 2;
    header[1] = 64;
    header[2] = 64 + segments[0].len() as u32;
    let mut frame: Vec<u8> = header.iter().flat_map(|w| w.to_le_bytes()).collect();
    frame.extend_from_slice(&segments[0]);
    frame.extend_from_slice(&segments[1]);
    frame
}

/// Write `<dir>/rle-multiframe.dcm` with `frames` frames of `size` x `size`.
pub fn write_rle_multiframe(
    dir: &Path,
    frames: usize,
    size: u16,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(dir)?;
    let fragments: Vec<Vec<u8>> = (0..frames)
        .map(|z| rle_frame(&phantom::phantom_pixels(z, frames, size), size as usize))
        .collect();
    // Offsets from the first byte of the first fragment item; each item has
    // an 8-byte header.
    let mut offsets = Vec::with_capacity(frames);
    let mut at = 0_u32;
    for f in &fragments {
        offsets.push(at);
        at += 8 + f.len() as u32;
    }

    let text = |tag, vr, v: &str| DataElement::new(tag, vr, PrimitiveValue::from(v));
    let us = |tag, v: u16| DataElement::new(tag, VR::US, PrimitiveValue::from(v));
    let obj = InMemDicomObject::from_element_iter([
        text(tags::SOP_CLASS_UID, VR::UI, uids::CT_IMAGE_STORAGE),
        text(
            tags::SOP_INSTANCE_UID,
            VR::UI,
            "2.25.2000000000000000000000000000003",
        ),
        text(tags::STUDY_DATE, VR::DA, "20260115"),
        text(tags::STUDY_TIME, VR::TM, "120000"),
        text(tags::ACCESSION_NUMBER, VR::SH, "SYNRLE01"),
        text(tags::MODALITY, VR::CS, "CT"),
        text(tags::STUDY_DESCRIPTION, VR::LO, "Synthetic RLE multi-frame"),
        text(tags::SERIES_DESCRIPTION, VR::LO, "RLE Lossless multi-frame"),
        text(tags::PATIENT_NAME, VR::PN, "RLE^MULTIFRAME"),
        text(tags::PATIENT_ID, VR::LO, "SYN-RLE-0001"),
        text(
            tags::STUDY_INSTANCE_UID,
            VR::UI,
            "2.25.2000000000000000000000000000002",
        ),
        text(
            tags::SERIES_INSTANCE_UID,
            VR::UI,
            "2.25.2000000000000000000000000000001",
        ),
        text(tags::STUDY_ID, VR::SH, "1"),
        text(tags::SERIES_NUMBER, VR::IS, "1"),
        text(tags::INSTANCE_NUMBER, VR::IS, "1"),
        text(tags::IMAGE_POSITION_PATIENT, VR::DS, "0\\0\\0"),
        text(tags::IMAGE_ORIENTATION_PATIENT, VR::DS, "1\\0\\0\\0\\1\\0"),
        us(tags::SAMPLES_PER_PIXEL, 1),
        text(tags::PHOTOMETRIC_INTERPRETATION, VR::CS, "MONOCHROME2"),
        text(tags::NUMBER_OF_FRAMES, VR::IS, &frames.to_string()),
        us(tags::ROWS, size),
        us(tags::COLUMNS, size),
        us(tags::BITS_ALLOCATED, 16),
        us(tags::BITS_STORED, 12),
        us(tags::HIGH_BIT, 11),
        us(tags::PIXEL_REPRESENTATION, 0),
        text(tags::WINDOW_CENTER, VR::DS, "40"),
        text(tags::WINDOW_WIDTH, VR::DS, "400"),
        text(tags::RESCALE_INTERCEPT, VR::DS, "-1024"),
        text(tags::RESCALE_SLOPE, VR::DS, "1"),
        DataElement::new(
            tags::PIXEL_DATA,
            VR::OB,
            PixelFragmentSequence::new(offsets, fragments),
        ),
    ]);
    let file = obj.with_meta(
        FileMetaTableBuilder::new()
            .transfer_syntax(RLE_LOSSLESS)
            .media_storage_sop_class_uid(uids::CT_IMAGE_STORAGE),
    )?;
    let path = dir.join("rle-multiframe.dcm");
    file.write_to_file(&path)?;
    Ok(path)
}

#[allow(dead_code)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let dir = args.next().unwrap_or_else(|| "data/rle".into());
    let frames: usize = args.next().map(|s| s.parse()).transpose()?.unwrap_or(16);
    let path = write_rle_multiframe(Path::new(&dir), frames, 256)?;
    println!("wrote {frames} frames to {}", path.display());
    Ok(())
}
