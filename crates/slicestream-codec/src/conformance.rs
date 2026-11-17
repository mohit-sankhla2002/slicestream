const SOC: u16 = 0xFF4F;
const SIZ: u16 = 0xFF51;
const COD: u16 = 0xFF52;
const TLM: u16 = 0xFF55;
const SOT: u16 = 0xFF90;

/// Check that a codestream has the shape progressive delivery relies on:
/// RPCL order, reversible 5/3, a coarsest resolution of at most 64 samples
/// per side, and a TLM marker.
pub fn check_conformance(codestream: &[u8]) -> Result<(), String> {
    let u16_at = |p: usize| -> Result<u16, String> {
        codestream
            .get(p..p + 2)
            .map(|b| u16::from_be_bytes([b[0], b[1]]))
            .ok_or_else(|| "truncated main header".to_string())
    };
    let u32_at = |p: usize| -> Result<u32, String> {
        codestream
            .get(p..p + 4)
            .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
            .ok_or_else(|| "truncated main header".to_string())
    };
    if u16_at(0)? != SOC {
        return Err("missing SOC".into());
    }
    let (mut size, mut cod, mut tlm) = (None, None, false);
    let mut pos = 2;
    loop {
        let marker = u16_at(pos)?;
        if marker == SOT {
            break;
        }
        let len = u16_at(pos + 2)? as usize;
        match marker {
            SIZ => {
                let w = u32_at(pos + 6)?.saturating_sub(u32_at(pos + 14)?);
                let h = u32_at(pos + 10)?.saturating_sub(u32_at(pos + 18)?);
                size = Some((w, h));
            }
            COD => {
                let b = codestream.get(pos + 5..pos + 14).ok_or("truncated COD")?;
                // b[0] progression, b[4] levels, b[8] transform
                cod = Some((b[0], b[4] as u32, b[8]));
            }
            TLM => tlm = true,
            _ => {}
        }
        pos += 2 + len;
    }
    let (w, h) = size.ok_or("missing SIZ")?;
    let (progression, levels, transform) = cod.ok_or("missing COD")?;
    if progression != 2 {
        return Err(format!(
            "progression order {progression}, expected RPCL (2)"
        ));
    }
    if transform != 1 {
        return Err("transform is not reversible 5/3".into());
    }
    let short = w.min(h);
    if short.div_ceil(1u32.checked_shl(levels).unwrap_or(u32::MAX)) > 64 {
        return Err(format!(
            "{levels} decomposition levels leave more than 64 samples for {w}x{h}"
        ));
    }
    if !tlm {
        return Err("missing TLM marker".into());
    }
    Ok(())
}
