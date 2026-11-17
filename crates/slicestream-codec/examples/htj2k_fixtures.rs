//! Writes HTJ2K test fixtures for the Cornerstone interop check:
//! `<name>.j2c`, `<name>.raw` (little-endian i32 samples) and `<name>.json`.
//!
//! Usage: `cargo run -p slicestream-codec --example htj2k_fixtures -- <out_dir>`

#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

use std::path::Path;

use slicestream_codec::{decomposition_levels, encode_verified, samples};
use slicestream_core::FrameInfo;

fn info(rows: u16, columns: u16, bits: u16, signed: bool) -> FrameInfo {
    FrameInfo {
        rows,
        columns,
        bits_allocated: 16,
        bits_stored: bits,
        samples_per_pixel: 1,
        pixel_representation: u16::from(signed),
        photometric_interpretation: "MONOCHROME2".into(),
        transfer_syntax: "1.2.840.10008.1.2.1".into(),
        rescale_slope: 1.0,
        rescale_intercept: 0.0,
        window_center: None,
        window_width: None,
    }
}

/// Deterministic synthetic frame: gradients plus a ring, as 16-bit words.
fn synthetic(rows: u16, columns: u16, lo: i32, hi: i32) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows as usize * columns as usize * 2);
    for y in 0..rows as i32 {
        for x in 0..columns as i32 {
            let span = hi - lo;
            let base = lo + (x * span / columns as i32 + y * span / rows as i32) / 2;
            let dx = x - columns as i32 / 2;
            let dy = y - rows as i32 / 2;
            let ring = if (dx * dx + dy * dy) % 997 < 60 {
                span / 8
            } else {
                0
            };
            let v = (base + ring).clamp(lo, hi);
            out.extend_from_slice(&(v as i16 as u16).to_le_bytes());
        }
    }
    out
}

fn write(
    dir: &Path,
    name: &str,
    frame: &[u8],
    info: &FrameInfo,
) -> Result<(), Box<dyn std::error::Error>> {
    let encoded = encode_verified(frame, info)?;
    let raw: Vec<u8> = samples(frame, info)?
        .iter()
        .flat_map(|s| s.to_le_bytes())
        .collect();
    let ends: Vec<String> = encoded.ends.iter().map(u32::to_string).collect();
    let json = format!(
        "{{\"w\":{},\"h\":{},\"signed\":{},\"bits\":{},\"ends\":[{}]}}",
        info.columns,
        info.rows,
        info.pixel_representation == 1,
        info.bits_stored,
        ends.join(",")
    );
    assert_eq!(
        encoded.ends.len() as u32,
        decomposition_levels(info.rows, info.columns) + 1
    );
    std::fs::write(dir.join(format!("{name}.j2c")), &encoded.codestream)?;
    std::fs::write(dir.join(format!("{name}.raw")), raw)?;
    std::fs::write(dir.join(format!("{name}.json")), json)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args()
        .nth(1)
        .ok_or("usage: htj2k_fixtures <out_dir>")?;
    let dir = Path::new(&dir);
    std::fs::create_dir_all(dir)?;
    for z in 0..6 {
        let frame = phantom::phantom_pixels(z, 6, 512);
        write(
            dir,
            &format!("phantom-{z}"),
            &frame,
            &info(512, 512, 12, false),
        )?;
    }
    write(
        dir,
        "signed16",
        &synthetic(256, 256, -30000, 30000),
        &info(256, 256, 16, true),
    )?;
    write(
        dir,
        "tiny",
        &synthetic(32, 32, 0, 4095),
        &info(32, 32, 12, false),
    )?;
    write(
        dir,
        "wide",
        &synthetic(512, 3328, 0, 4095),
        &info(512, 3328, 12, false),
    )?;
    Ok(())
}
