use openjph_core::codestream::Codestream;
use openjph_core::file::{MemInfile, MemOutfile};
use openjph_core::types::{Point, Size};
use slicestream_core::FrameInfo;

use crate::CodecError;
use crate::levels::decomposition_levels;
use crate::samples::{eligibility, samples};

/// A finished copy and where each resolution ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Encoded {
    pub codestream: Vec<u8>,
    /// Byte length of the prefix that holds resolutions `0..=r`, one entry per
    /// resolution (lowest first). `ends.len() == levels + 1`; the last entry is
    /// `codestream.len()`.
    pub ends: Vec<u32>,
}

fn enc_err(e: impl std::fmt::Display) -> CodecError {
    CodecError::Encode(e.to_string())
}

/// Encode samples as lossless HTJ2K, RPCL, one tile, TLM marker.
pub fn encode(samples: &[i32], info: &FrameInfo) -> Result<Encoded, CodecError> {
    let (w, h) = (info.columns as u32, info.rows as u32);
    if samples.len() != w as usize * h as usize {
        return Err(CodecError::Encode(format!(
            "{} samples for {w}x{h}",
            samples.len()
        )));
    }
    let levels = decomposition_levels(info.rows, info.columns);
    let mut cs = Codestream::new();
    // openjph-core 0.1.0 does not round-trip unsigned 16-bit data through the
    // 5/3 transform (the DC shift leaves no headroom). Declaring one more bit
    // is lossless and decodes to the same sample values.
    let precision =
        info.bits_stored + u16::from(info.bits_stored == 16 && info.pixel_representation == 0);
    let siz = cs.access_siz_mut();
    siz.set_image_extent(Point::new(w, h));
    siz.set_num_components(1);
    siz.set_comp_info(
        0,
        Point::new(1, 1),
        precision as u32,
        info.pixel_representation == 1,
    );
    siz.set_tile_size(Size::new(w, h));
    let cod = cs.access_cod_mut();
    cod.set_num_decomposition(levels);
    cod.set_block_dims(64, 64);
    cod.set_reversible(true);
    cod.set_color_transform(false);
    cod.set_progression_order("RPCL").map_err(enc_err)?;
    cs.set_planar(0);
    cs.set_tilepart_divisions(0);
    cs.request_tlm_marker(true);

    let mut out = MemOutfile::new();
    cs.write_headers(&mut out, &[]).map_err(enc_err)?;
    for row in samples.chunks_exact(w as usize) {
        cs.exchange(row, 0).map_err(enc_err)?;
    }
    cs.flush(&mut out).map_err(enc_err)?;
    let codestream = out.get_data().to_vec();
    let ends = cs
        .resolution_ends()
        .ok_or_else(|| CodecError::Encode("no resolution ends reported".into()))?;
    if ends.len() != levels as usize + 1 || ends.last() != Some(&codestream.len()) {
        return Err(CodecError::Encode(format!(
            "resolution ends {ends:?} do not match {levels} levels / {} bytes",
            codestream.len()
        )));
    }
    let ends = ends
        .into_iter()
        .map(|e| u32::try_from(e).map_err(enc_err))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Encoded { codestream, ends })
}

/// Decode a codestream at full resolution, one `i32` per pixel.
pub fn decode_full(codestream: &[u8]) -> Result<Vec<i32>, CodecError> {
    let mut cs = Codestream::new();
    let mut input = MemInfile::new(codestream);
    cs.read_headers(&mut input).map_err(enc_err)?;
    let extent = cs.access_siz().get_image_extent();
    cs.create(&mut input).map_err(enc_err)?;
    let mut out = Vec::with_capacity(extent.x as usize * extent.y as usize);
    for _ in 0..extent.y {
        let line = cs
            .pull(0)
            .ok_or_else(|| CodecError::Encode("decoder ran out of lines".into()))?;
        out.extend_from_slice(&line);
    }
    Ok(out)
}

/// Encode a stored frame and prove the copy decodes to the same samples.
pub fn encode_verified(frame: &[u8], info: &FrameInfo) -> Result<Encoded, CodecError> {
    encode_verified_with(frame, info, |_| {})
}

/// [`encode_verified`] with `tamper` applied to the codestream before the
/// check. Tests only.
#[doc(hidden)]
pub fn encode_verified_with(
    frame: &[u8],
    info: &FrameInfo,
    tamper: impl FnOnce(&mut Vec<u8>),
) -> Result<Encoded, CodecError> {
    eligibility(info).map_err(|e| CodecError::NotEligible(e.0))?;
    let original = samples(frame, info)?;
    let mut encoded = encode(&original, info)?;
    tamper(&mut encoded.codestream);
    let decoded = decode_full(&encoded.codestream)?;
    if decoded.len() != original.len() {
        return Err(CodecError::Mismatch {
            at: decoded.len().min(original.len()),
        });
    }
    if let Some(at) = decoded.iter().zip(&original).position(|(a, b)| a != b) {
        return Err(CodecError::Mismatch { at });
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::check_conformance;

    fn info(rows: u16, cols: u16, signed: bool) -> FrameInfo {
        FrameInfo {
            rows,
            columns: cols,
            bits_allocated: 16,
            bits_stored: 16,
            samples_per_pixel: 1,
            pixel_representation: signed as u16,
            photometric_interpretation: "MONOCHROME2".into(),
            transfer_syntax: "1.2.840.10008.1.2.1".into(),
            rescale_slope: 1.0,
            rescale_intercept: 0.0,
            window_center: None,
            window_width: None,
        }
    }

    /// Deterministic noise-ish 16-bit frame (xorshift).
    fn noisy_frame(n: usize) -> Vec<u8> {
        let mut x = 0x9E3779B9u32;
        (0..n * 2)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                (x >> 8) as u8
            })
            .collect()
    }

    #[test]
    fn tiny_frame_has_one_resolution() {
        let i = info(32, 32, false);
        let e = encode_verified(&noisy_frame(32 * 32), &i).unwrap();
        assert_eq!(e.ends, vec![e.codestream.len() as u32]);
    }

    #[test]
    fn round_trip_signed_16() {
        let i = info(512, 512, true);
        // CT-like: a signed ramp (HU -1000..2000) with a little noise.
        let noise = noisy_frame(512 * 512);
        let frame: Vec<u8> = (0..512 * 512)
            .flat_map(|k| {
                let (x, y) = (k % 512, k / 512);
                let hu = ((x * 7 + y * 3) % 3000) as i16 - 1000 + (noise[k * 2] % 48) as i16;
                hu.to_le_bytes()
            })
            .collect();
        let e = encode_verified(&frame, &i).unwrap();
        assert_eq!(e.ends.len(), 4);
    }

    #[test]
    fn conformance_of_output() {
        let i = info(128, 200, false);
        let s = samples(&noisy_frame(128 * 200), &i).unwrap();
        assert_eq!(
            check_conformance(&encode(&s, &i).unwrap().codestream),
            Ok(())
        );
    }

    #[test]
    fn conformance_rejects_missing_tlm() {
        let i = info(128, 128, false);
        let s = samples(&noisy_frame(128 * 128), &i).unwrap();
        let cs = encode(&s, &i).unwrap().codestream;
        let mut pos = 2;
        while u16::from_be_bytes([cs[pos], cs[pos + 1]]) != 0xFF55 {
            pos += 2 + u16::from_be_bytes([cs[pos + 2], cs[pos + 3]]) as usize;
        }
        let len = 2 + u16::from_be_bytes([cs[pos + 2], cs[pos + 3]]) as usize;
        let mut stripped = cs[..pos].to_vec();
        stripped.extend_from_slice(&cs[pos + len..]);
        let err = check_conformance(&stripped).unwrap_err();
        assert!(err.contains("TLM"), "{err}");
    }

    #[test]
    fn verify_rejects_mismatch() {
        let i = info(128, 128, false);
        let r = encode_verified_with(&noisy_frame(128 * 128), &i, |cs| {
            let mid = cs.len() / 2;
            cs[mid] ^= 0xFF;
        });
        assert!(
            matches!(r, Err(CodecError::Mismatch { .. } | CodecError::Encode(_))),
            "{r:?}"
        );
    }

    #[test]
    fn ineligible_frame_is_rejected() {
        let mut i = info(8, 8, false);
        i.samples_per_pixel = 3;
        assert!(matches!(
            encode_verified(&[0; 8 * 8 * 2], &i),
            Err(CodecError::NotEligible(_))
        ));
    }
}
