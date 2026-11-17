use std::fmt;

use slicestream_core::FrameInfo;

use crate::CodecError;

/// Why a frame cannot get a progressive copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotEligible(pub String);

impl fmt::Display for NotEligible {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

const BIG_ENDIAN: &str = "1.2.840.10008.1.2.2";

pub fn eligibility(info: &FrameInfo) -> Result<(), NotEligible> {
    let no = |s: String| Err(NotEligible(s));
    if info.is_encapsulated() {
        return no(format!(
            "transfer syntax {} is not native",
            info.transfer_syntax
        ));
    }
    if info.samples_per_pixel != 1 {
        return no(format!("{} samples per pixel", info.samples_per_pixel));
    }
    if !matches!(
        info.photometric_interpretation.as_str(),
        "MONOCHROME1" | "MONOCHROME2"
    ) {
        return no(format!(
            "photometric interpretation {}",
            info.photometric_interpretation
        ));
    }
    if !matches!(info.bits_allocated, 8 | 16) {
        return no(format!("{} bits allocated", info.bits_allocated));
    }
    if info.bits_stored == 0 || info.bits_stored > info.bits_allocated {
        return no(format!(
            "{} bits stored with {} allocated",
            info.bits_stored, info.bits_allocated
        ));
    }
    if info.rows == 0 || info.columns == 0 {
        return no("empty frame".into());
    }
    Ok(())
}

/// Decode the stored bytes of one frame into one `i32` per pixel.
pub fn samples(frame: &[u8], info: &FrameInfo) -> Result<Vec<i32>, CodecError> {
    if !matches!(info.bits_allocated, 8 | 16) || info.bits_stored == 0 || info.bits_stored > 16 {
        return Err(CodecError::BadFrame(format!(
            "unsupported depth {}/{}",
            info.bits_allocated, info.bits_stored
        )));
    }
    let bytes = (info.bits_allocated / 8) as usize;
    let count = info.rows as usize * info.columns as usize;
    if frame.len() != count * bytes {
        return Err(CodecError::BadFrame(format!(
            "{} bytes, expected {}",
            frame.len(),
            count * bytes
        )));
    }
    let big = info.transfer_syntax == BIG_ENDIAN;
    let mask = (1u32 << info.bits_stored) - 1;
    let sign_bit = 1u32 << (info.bits_stored - 1);
    let signed = info.pixel_representation == 1;
    Ok(frame
        .chunks_exact(bytes)
        .map(|c| {
            let word = match (bytes, big) {
                (1, _) => c[0] as u32,
                (_, false) => u16::from_le_bytes([c[0], c[1]]) as u32,
                (_, true) => u16::from_be_bytes([c[0], c[1]]) as u32,
            } & mask;
            if signed && word & sign_bit != 0 {
                word as i32 - (1i32 << info.bits_stored)
            } else {
                word as i32
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    const LE: &str = "1.2.840.10008.1.2.1";

    fn info(ba: u16, bs: u16, pr: u16, ts: &str, rows: u16, cols: u16) -> FrameInfo {
        FrameInfo {
            rows,
            columns: cols,
            bits_allocated: ba,
            bits_stored: bs,
            samples_per_pixel: 1,
            pixel_representation: pr,
            photometric_interpretation: "MONOCHROME2".into(),
            transfer_syntax: ts.into(),
            rescale_slope: 1.0,
            rescale_intercept: 0.0,
            window_center: None,
            window_width: None,
        }
    }

    #[test]
    fn samples_mask_high_bits_and_sign_extend() {
        assert_eq!(
            samples(
                &[0x00, 0xF8, 0xFF, 0x07, 0x01, 0xA0],
                &info(16, 12, 1, LE, 3, 1)
            )
            .unwrap(),
            vec![-2048, 2047, 1]
        );
    }

    #[test]
    fn samples_big_endian() {
        assert_eq!(
            samples(&[0x01, 0x02], &info(16, 16, 0, BIG_ENDIAN, 1, 1)).unwrap(),
            vec![258]
        );
    }

    #[test]
    fn samples_eight_bit() {
        assert_eq!(
            samples(&[0, 255], &info(8, 8, 0, LE, 2, 1)).unwrap(),
            vec![0, 255]
        );
    }

    #[test]
    fn samples_wrong_length_is_bad_frame() {
        assert!(matches!(
            samples(&[0, 1, 2], &info(16, 16, 0, LE, 2, 1)),
            Err(CodecError::BadFrame(_))
        ));
    }

    #[test]
    fn eligibility_rules() {
        for ts in [LE, "1.2.840.10008.1.2", BIG_ENDIAN] {
            assert!(eligibility(&info(16, 12, 0, ts, 4, 4)).is_ok());
        }
        let mut mono1 = info(8, 1, 0, LE, 4, 4);
        mono1.photometric_interpretation = "MONOCHROME1".into();
        assert!(eligibility(&mono1).is_ok());

        let mut rgb = info(8, 8, 0, LE, 4, 4);
        rgb.photometric_interpretation = "RGB".into();
        assert!(eligibility(&rgb).unwrap_err().0.contains("RGB"));
        let mut spp3 = info(8, 8, 0, LE, 4, 4);
        spp3.samples_per_pixel = 3;
        assert!(eligibility(&spp3).is_err());
        assert!(eligibility(&info(1, 1, 0, LE, 4, 4)).is_err());
        assert!(eligibility(&info(32, 16, 0, LE, 4, 4)).is_err());
        assert!(eligibility(&info(16, 0, 0, LE, 4, 4)).is_err());
        assert!(eligibility(&info(8, 9, 0, LE, 4, 4)).is_err());
        assert!(eligibility(&info(16, 16, 0, "1.2.840.10008.1.2.4.50", 4, 4)).is_err());
    }
}
