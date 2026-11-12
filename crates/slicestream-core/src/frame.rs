//! Frame metadata and raw pixel extraction.

use std::path::Path;

use bytes::Bytes;
use dicom_dictionary_std::tags;
use dicom_object::{InMemDicomObject, OpenFileOptions};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Everything a browser needs to decode and display one frame.
///
/// Sent to the client as JSON alongside each frame payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FrameInfo {
    pub rows: u16,
    pub columns: u16,
    pub bits_allocated: u16,
    pub bits_stored: u16,
    pub samples_per_pixel: u16,
    /// 0 = unsigned, 1 = two's complement signed.
    pub pixel_representation: u16,
    pub photometric_interpretation: String,
    /// Transfer syntax UID of the payload. Native syntaxes carry raw pixels;
    /// encapsulated syntaxes (JPEG, JPEG-LS, JPEG 2000, HTJ2K, ...) carry the
    /// compressed codestream untouched so the client can decode it.
    pub transfer_syntax: String,
    pub rescale_slope: f64,
    pub rescale_intercept: f64,
    pub window_center: Option<f64>,
    pub window_width: Option<f64>,
}

impl FrameInfo {
    /// Number of bytes of one uncompressed frame.
    pub fn native_frame_len(&self) -> usize {
        self.rows as usize
            * self.columns as usize
            * self.samples_per_pixel as usize
            * (self.bits_allocated as usize).div_ceil(8)
    }

    /// Whether the payload is encapsulated (compressed) pixel data.
    pub fn is_encapsulated(&self) -> bool {
        !matches!(
            self.transfer_syntax.as_str(),
            "1.2.840.10008.1.2" | "1.2.840.10008.1.2.1" | "1.2.840.10008.1.2.2"
        )
    }

    pub fn from_dataset(obj: &InMemDicomObject, transfer_syntax: &str) -> Result<Self> {
        Ok(Self {
            rows: req_u16(obj, tags::ROWS, "Rows")?,
            columns: req_u16(obj, tags::COLUMNS, "Columns")?,
            bits_allocated: req_u16(obj, tags::BITS_ALLOCATED, "BitsAllocated")?,
            bits_stored: get_i64(obj, tags::BITS_STORED)
                .map(|v| v as u16)
                .unwrap_or(0),
            samples_per_pixel: get_i64(obj, tags::SAMPLES_PER_PIXEL)
                .map(|v| v as u16)
                .unwrap_or(1),
            pixel_representation: get_i64(obj, tags::PIXEL_REPRESENTATION)
                .map(|v| v as u16)
                .unwrap_or(0),
            photometric_interpretation: get_str(obj, tags::PHOTOMETRIC_INTERPRETATION)
                .unwrap_or_else(|| "MONOCHROME2".into()),
            transfer_syntax: transfer_syntax.trim_end_matches('\0').to_string(),
            rescale_slope: get_f64s(obj, tags::RESCALE_SLOPE)
                .and_then(|v| v.first().copied())
                .unwrap_or(1.0),
            rescale_intercept: get_f64s(obj, tags::RESCALE_INTERCEPT)
                .and_then(|v| v.first().copied())
                .unwrap_or(0.0),
            window_center: get_f64s(obj, tags::WINDOW_CENTER).and_then(|v| v.first().copied()),
            window_width: get_f64s(obj, tags::WINDOW_WIDTH).and_then(|v| v.first().copied()),
        })
    }
}

/// One frame ready to put on the wire.
#[derive(Debug, Clone)]
pub struct Frame {
    /// Position of this frame in the sorted series stack.
    pub index: usize,
    pub info: FrameInfo,
    /// Raw pixels (native syntaxes) or the compressed codestream (encapsulated).
    pub data: Bytes,
}

/// Read frame `frame_no` (0-based, within the instance) from a DICOM file.
///
/// This is deliberately simple: it parses the whole file. A production path
/// should build a byte-offset index once and serve frames with `pread`/mmap
/// or HTTP range reads — see the roadmap in the README.
pub fn read_frame_bytes(path: &Path, frame_no: u32, info: &FrameInfo) -> Result<Bytes> {
    let obj = OpenFileOptions::new()
        .open_file(path)
        .map_err(|e| Error::Dicom {
            path: path.to_path_buf(),
            message: e.to_string(),
        })?;
    let n_frames = get_i64(&obj, tags::NUMBER_OF_FRAMES).unwrap_or(1).max(1) as usize;
    let frame_no = frame_no as usize;
    if frame_no >= n_frames {
        return Err(Error::SliceOutOfRange {
            index: frame_no,
            len: n_frames,
        });
    }

    let pixel = obj
        .element(tags::PIXEL_DATA)
        .map_err(|_| Error::MissingAttribute("PixelData"))?;
    let value = pixel.value();

    if let Some(fragments) = value.fragments() {
        let offsets = value.offset_table().unwrap_or(&[]);
        return encapsulated_frame(fragments, offsets, n_frames, frame_no);
    }

    let bytes = value
        .to_bytes()
        .map_err(|e| Error::Unsupported(format!("native pixel data: {e}")))?;
    let len = info.native_frame_len();
    let start = frame_no * len;
    let end = start + len;
    if end > bytes.len() {
        return Err(Error::Unsupported(format!(
            "pixel data too short: need {end} bytes, have {}",
            bytes.len()
        )));
    }
    Ok(Bytes::copy_from_slice(&bytes[start..end]))
}

/// Pick the fragments that belong to `frame_no` in an encapsulated pixel sequence.
fn encapsulated_frame(
    fragments: &[Vec<u8>],
    basic_offsets: &[u32],
    n_frames: usize,
    frame_no: usize,
) -> Result<Bytes> {
    // Single frame: all fragments form one codestream.
    if n_frames == 1 {
        return Ok(Bytes::from(fragments.concat()));
    }
    // Common case: one fragment per frame.
    if fragments.len() == n_frames {
        return Ok(Bytes::copy_from_slice(&fragments[frame_no]));
    }
    // Otherwise use the Basic Offset Table. Offsets are measured from the
    // first byte of the first fragment item, and each item has an 8-byte header.
    if basic_offsets.len() == n_frames {
        let start = basic_offsets[frame_no] as u64;
        let end = basic_offsets.get(frame_no + 1).map(|&o| o as u64);
        let mut pos = 0u64;
        let mut out = Vec::new();
        for frag in fragments {
            let in_range = pos >= start && end.is_none_or(|e| pos < e);
            if in_range {
                out.extend_from_slice(frag);
            }
            pos += 8 + frag.len() as u64;
        }
        return Ok(Bytes::from(out));
    }
    Err(Error::Unsupported(format!(
        "{n_frames} frames in {} fragments without a usable offset table",
        fragments.len()
    )))
}

// ---- small attribute helpers -------------------------------------------------

pub fn get_str(obj: &InMemDicomObject, tag: dicom_core::Tag) -> Option<String> {
    obj.element_opt(tag)
        .ok()
        .flatten()
        .and_then(|e| e.to_str().ok())
        .map(|s| {
            s.trim_matches(|c: char| c == '\0' || c.is_whitespace())
                .to_string()
        })
        .filter(|s| !s.is_empty())
}

pub fn get_i64(obj: &InMemDicomObject, tag: dicom_core::Tag) -> Option<i64> {
    obj.element_opt(tag)
        .ok()
        .flatten()
        .and_then(|e| e.to_int::<i64>().ok())
}

pub fn get_f64s(obj: &InMemDicomObject, tag: dicom_core::Tag) -> Option<Vec<f64>> {
    obj.element_opt(tag)
        .ok()
        .flatten()
        .and_then(|e| e.to_multi_float64().ok())
        .filter(|v| !v.is_empty())
}

fn req_u16(obj: &InMemDicomObject, tag: dicom_core::Tag, name: &'static str) -> Result<u16> {
    get_i64(obj, tag)
        .map(|v| v as u16)
        .ok_or(Error::MissingAttribute(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_frame_concatenates_fragments() {
        let frags = vec![vec![1, 2], vec![3]];
        assert_eq!(
            &encapsulated_frame(&frags, &[], 1, 0).unwrap()[..],
            &[1, 2, 3]
        );
    }

    #[test]
    fn one_fragment_per_frame() {
        let frags = vec![vec![1], vec![2], vec![3]];
        assert_eq!(&encapsulated_frame(&frags, &[], 3, 1).unwrap()[..], &[2]);
    }

    #[test]
    fn offset_table_groups_fragments() {
        // frame 0 = [a(2 bytes), b(2 bytes)], frame 1 = [c(1 byte)]
        let frags = vec![vec![1, 1], vec![2, 2], vec![3]];
        let offsets = [0, (8 + 2) + (8 + 2)];
        assert_eq!(
            &encapsulated_frame(&frags, &offsets, 2, 0).unwrap()[..],
            &[1, 1, 2, 2]
        );
        assert_eq!(
            &encapsulated_frame(&frags, &offsets, 2, 1).unwrap()[..],
            &[3]
        );
    }
}
