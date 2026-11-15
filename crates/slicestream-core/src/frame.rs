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

    /// Number of bits of one frame: rows x columns x samples x bits allocated.
    pub fn frame_bits(&self) -> u64 {
        self.rows as u64
            * self.columns as u64
            * self.samples_per_pixel as u64
            * self.bits_allocated as u64
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

/// Where one frame's bytes live inside a stored file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameLocation {
    /// One contiguous byte range.
    Bytes { offset: u64, len: u64 },
    /// `offset` is the file offset of the Pixel Data value; the frame is bits
    /// `[bit_start, bit_start + bit_len)` of it.
    Bits {
        offset: u64,
        bit_start: u64,
        bit_len: u64,
    },
    /// `(file offset, length)` of each fragment value, concatenated in order.
    Fragments(Vec<(u64, u64)>),
}

/// Read one frame from `path` with positioned reads. A short read is `Error::Io`.
#[cfg(unix)]
pub fn read_frame_at(path: &Path, loc: &FrameLocation) -> Result<Bytes> {
    use std::os::unix::fs::FileExt;

    let io = |source| Error::Io {
        path: path.to_path_buf(),
        source,
    };
    let file = std::fs::File::open(path).map_err(io)?;
    let read = |offset: u64, len: u64| -> Result<Vec<u8>> {
        let len = usize::try_from(len).map_err(|_| Error::Unsupported("frame too large".into()))?;
        let mut buf = vec![0u8; len];
        file.read_exact_at(&mut buf, offset).map_err(io)?;
        Ok(buf)
    };
    match loc {
        FrameLocation::Bytes { offset, len } => Ok(Bytes::from(read(*offset, *len)?)),
        FrameLocation::Bits {
            offset,
            bit_start,
            bit_len,
        } => {
            let first = bit_start / 8;
            let last = (bit_start + bit_len).div_ceil(8);
            let src = read(offset + first, last - first)?;
            Ok(Bytes::from(extract_bits(&src, bit_start % 8, *bit_len)))
        }
        FrameLocation::Fragments(frags) => {
            let total = frags.iter().try_fold(0u64, |a, (_, l)| a.checked_add(*l));
            let total = total.ok_or_else(|| Error::Unsupported("frame too large".into()))?;
            let mut out = Vec::with_capacity(usize::try_from(total).unwrap_or(0));
            for (offset, len) in frags {
                out.extend_from_slice(&read(*offset, *len)?);
            }
            Ok(Bytes::from(out))
        }
    }
}

/// Copy bits `[bit_start, bit_start + bit_len)` of `src` into a new buffer.
///
/// DICOM bit order: the first pixel is the least significant bit of the first
/// byte. The output starts at bit 0; unused high bits of the last byte are zero.
/// Bits past the end of `src` read as zero.
pub fn extract_bits(src: &[u8], bit_start: u64, bit_len: u64) -> Vec<u8> {
    let out_len = bit_len.div_ceil(8) as usize;
    let first = (bit_start / 8) as usize;
    let shift = (bit_start % 8) as u32;
    let at = |i: usize| src.get(first + i).copied().unwrap_or(0);
    let mut out: Vec<u8> = (0..out_len)
        .map(|i| {
            if shift == 0 {
                at(i)
            } else {
                (at(i) >> shift) | (at(i + 1) << (8 - shift))
            }
        })
        .collect();
    let tail = (bit_len % 8) as u32;
    if tail != 0
        && let Some(last) = out.last_mut()
    {
        *last &= (1u8 << tail) - 1;
    }
    out
}

/// Swap each word of a big-endian pixel buffer to little-endian, in place.
/// 16 bits swaps 2-byte words, 32 bits swaps 4-byte words, anything else is a no-op.
pub fn swap_to_little_endian(buf: &mut [u8], bits_allocated: u16) {
    let width = match bits_allocated {
        16 => 2,
        32 => 4,
        _ => return,
    };
    for word in buf.chunks_exact_mut(width) {
        word.reverse();
    }
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
    if info.bits_allocated == 1 {
        let n = info.rows as u64 * info.columns as u64 * info.samples_per_pixel as u64;
        let start = frame_no as u64 * n;
        if (start + n).div_ceil(8) > bytes.len() as u64 {
            return Err(Error::Unsupported(format!(
                "pixel data too short: need {} bytes, have {}",
                (start + n).div_ceil(8),
                bytes.len()
            )));
        }
        return Ok(Bytes::from(extract_bits(&bytes, start, n)));
    }
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

    #[test]
    fn extract_bits_aligned() {
        assert_eq!(extract_bits(&[0xAB, 0xCD, 0xEF], 8, 16), vec![0xCD, 0xEF]);
        // Trailing partial byte: unused high bits are zeroed.
        assert_eq!(extract_bits(&[0xFF, 0xFF], 0, 12), vec![0xFF, 0x0F]);
    }

    #[test]
    fn extract_bits_unaligned() {
        assert_eq!(
            extract_bits(&[0b1010_1100, 0b0000_0111], 3, 10),
            vec![0b1111_0101, 0b0000_0000]
        );
    }

    #[test]
    fn swap_16() {
        let mut b = [1, 2, 3, 4];
        swap_to_little_endian(&mut b, 16);
        assert_eq!(b, [2, 1, 4, 3]);
    }

    #[test]
    fn swap_32() {
        let mut b = [1, 2, 3, 4, 5, 6, 7, 8];
        swap_to_little_endian(&mut b, 32);
        assert_eq!(b, [4, 3, 2, 1, 8, 7, 6, 5]);
    }

    #[test]
    fn swap_8_is_noop() {
        let mut b = [1, 2, 3, 4];
        swap_to_little_endian(&mut b, 8);
        assert_eq!(b, [1, 2, 3, 4]);
    }

    #[test]
    fn bits_allocated_1_frames() {
        use dicom_core::{DataElement, PrimitiveValue, VR};
        use dicom_dictionary_std::uids;
        use dicom_object::FileMetaTableBuilder;

        // 2 frames of 3x3 bits = 18 bits, packed back to back.
        let packed = vec![0b1011_0110, 0b0100_1101, 0b0000_0010, 0];
        let obj = InMemDicomObject::from_element_iter([
            DataElement::new(
                tags::SOP_CLASS_UID,
                VR::UI,
                PrimitiveValue::from(uids::CT_IMAGE_STORAGE),
            ),
            DataElement::new(
                tags::SOP_INSTANCE_UID,
                VR::UI,
                PrimitiveValue::from("2.25.1"),
            ),
            DataElement::new(tags::SAMPLES_PER_PIXEL, VR::US, PrimitiveValue::from(1_u16)),
            DataElement::new(tags::NUMBER_OF_FRAMES, VR::IS, PrimitiveValue::from("2")),
            DataElement::new(tags::ROWS, VR::US, PrimitiveValue::from(3_u16)),
            DataElement::new(tags::COLUMNS, VR::US, PrimitiveValue::from(3_u16)),
            DataElement::new(tags::BITS_ALLOCATED, VR::US, PrimitiveValue::from(1_u16)),
            DataElement::new(tags::BITS_STORED, VR::US, PrimitiveValue::from(1_u16)),
            DataElement::new(tags::HIGH_BIT, VR::US, PrimitiveValue::from(0_u16)),
            DataElement::new(
                tags::PIXEL_REPRESENTATION,
                VR::US,
                PrimitiveValue::from(0_u16),
            ),
            DataElement::new(
                tags::PIXEL_DATA,
                VR::OW,
                PrimitiveValue::from(packed.clone()),
            ),
        ]);
        let file = obj
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(uids::EXPLICIT_VR_LITTLE_ENDIAN)
                    .media_storage_sop_class_uid(uids::CT_IMAGE_STORAGE),
            )
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bits.dcm");
        file.write_to_file(&path).unwrap();

        let info = FrameInfo::from_dataset(&file, uids::EXPLICIT_VR_LITTLE_ENDIAN).unwrap();
        assert_eq!(info.frame_bits(), 9);
        let f0 = read_frame_bytes(&path, 0, &info).unwrap();
        let f1 = read_frame_bytes(&path, 1, &info).unwrap();
        assert_eq!(&f0[..], &extract_bits(&packed, 0, 9)[..]);
        assert_eq!(&f1[..], &extract_bits(&packed, 9, 9)[..]);
        assert_eq!(f1.len(), 2);
    }
}
