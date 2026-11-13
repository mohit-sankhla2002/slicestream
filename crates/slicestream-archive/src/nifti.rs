use std::fmt;

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct NiftiHeader {
    pub version: u8,
    pub dims: Vec<i64>,
    pub pixdim: Vec<f64>,
    pub datatype: i16,
    pub descrip: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NiftiError {
    TooShort,
    BadMagic,
}

impl fmt::Display for NiftiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooShort => f.write_str("NIfTI header too short"),
            Self::BadMagic => f.write_str("not a NIfTI header"),
        }
    }
}

impl std::error::Error for NiftiError {}

const V1_LEN: usize = 348;
const V2_LEN: usize = 540;

/// Identify a NIfTI header by `sizeof_hdr` and magic. Returns (version, big_endian).
/// Needs enough bytes to see the magic; otherwise `None`.
pub(crate) fn sniff(head: &[u8]) -> Option<(u8, bool)> {
    let raw: [u8; 4] = head.get(0..4)?.try_into().ok()?;
    let le = i32::from_le_bytes(raw);
    let be = i32::from_be_bytes(raw);
    let magic = |off: usize, n: usize| head.get(off..off + n);
    if le == V1_LEN as i32 || be == V1_LEN as i32 {
        let m = magic(344, 4)?;
        if m == b"n+1\0" || m == b"ni1\0" {
            return Some((1, be == V1_LEN as i32));
        }
    } else if (le == V2_LEN as i32 || be == V2_LEN as i32)
        && magic(4, 4) == Some(b"n+2\0".as_slice())
    {
        return Some((2, be == V2_LEN as i32));
    }
    None
}

struct Reader<'a> {
    b: &'a [u8],
    big: bool,
}

impl Reader<'_> {
    fn arr<const N: usize>(&self, off: usize) -> [u8; N] {
        let mut a = [0u8; N];
        a.copy_from_slice(&self.b[off..off + N]);
        if self.big {
            a.reverse();
        }
        a
    }
    // Arrays are reversed to little-endian order when big-endian.
    fn i16(&self, off: usize) -> i16 {
        i16::from_le_bytes(self.arr(off))
    }
    fn i64(&self, off: usize) -> i64 {
        i64::from_le_bytes(self.arr(off))
    }
    fn f32(&self, off: usize) -> f32 {
        f32::from_le_bytes(self.arr(off))
    }
    fn f64(&self, off: usize) -> f64 {
        f64::from_le_bytes(self.arr(off))
    }
}

pub fn parse_nifti_header(head: &[u8]) -> Result<NiftiHeader, NiftiError> {
    if head.len() < 4 {
        return Err(NiftiError::TooShort);
    }
    let raw: [u8; 4] = [head[0], head[1], head[2], head[3]];
    let size = [i32::from_le_bytes(raw), i32::from_be_bytes(raw)];
    let need = if size.contains(&(V1_LEN as i32)) {
        V1_LEN
    } else if size.contains(&(V2_LEN as i32)) {
        V2_LEN
    } else {
        return Err(NiftiError::BadMagic);
    };
    if head.len() < need {
        return Err(NiftiError::TooShort);
    }
    let (version, big) = sniff(head).ok_or(NiftiError::BadMagic)?;
    let r = Reader { b: head, big };
    let (dim, pix, datatype, descrip_off): (Vec<i64>, Vec<f64>, i16, usize) = if version == 1 {
        (
            (0..8).map(|i| i64::from(r.i16(40 + 2 * i))).collect(),
            (0..8).map(|i| f64::from(r.f32(76 + 4 * i))).collect(),
            r.i16(70),
            148,
        )
    } else {
        (
            (0..8).map(|i| r.i64(16 + 8 * i)).collect(),
            (0..8).map(|i| r.f64(104 + 8 * i)).collect(),
            r.i16(12),
            240,
        )
    };
    let n = dim[0].clamp(1, 7) as usize;
    let raw = &head[descrip_off..descrip_off + 80];
    let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    Ok(NiftiHeader {
        version,
        dims: dim[1..=n].to_vec(),
        pixdim: pix[1..=n].to_vec(),
        datatype,
        descrip: String::from_utf8_lossy(&raw[..end]).into_owned(),
    })
}

#[cfg(test)]
pub(crate) mod tests_support {
    pub fn nifti1_head(big_endian: bool) -> Vec<u8> {
        super::tests::nifti1(&[2, 3], &[1.0, 2.0], 4, "x", big_endian)
    }
    pub fn nifti2_head() -> Vec<u8> {
        super::tests::nifti2(&[2, 3], &[1.0, 2.0], 4, "x")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn nifti1(
        dims: &[i16],
        pixdim: &[f32],
        datatype: i16,
        descrip: &str,
        big_endian: bool,
    ) -> Vec<u8> {
        let mut v = vec![0u8; 348 + 4];
        let w16 = |v: &mut Vec<u8>, off: usize, x: i16| {
            let b = if big_endian {
                x.to_be_bytes()
            } else {
                x.to_le_bytes()
            };
            v[off..off + 2].copy_from_slice(&b);
        };
        let b = if big_endian {
            348i32.to_be_bytes()
        } else {
            348i32.to_le_bytes()
        };
        v[0..4].copy_from_slice(&b);
        w16(&mut v, 40, dims.len() as i16);
        for (i, d) in dims.iter().enumerate() {
            w16(&mut v, 42 + 2 * i, *d);
        }
        w16(&mut v, 70, datatype);
        w16(&mut v, 72, 16);
        for (i, p) in pixdim.iter().enumerate() {
            let b = if big_endian {
                p.to_be_bytes()
            } else {
                p.to_le_bytes()
            };
            let off = 76 + 4 * (i + 1);
            v[off..off + 4].copy_from_slice(&b);
        }
        v[148..148 + descrip.len()].copy_from_slice(descrip.as_bytes());
        v[344..348].copy_from_slice(b"n+1\0");
        v
    }

    pub(super) fn nifti2(dims: &[i64], pixdim: &[f64], datatype: i16, descrip: &str) -> Vec<u8> {
        let mut v = vec![0u8; 544];
        v[0..4].copy_from_slice(&540i32.to_le_bytes());
        v[4..12].copy_from_slice(b"n+2\0\r\n\x1a\n");
        v[12..14].copy_from_slice(&datatype.to_le_bytes());
        v[14..16].copy_from_slice(&16i16.to_le_bytes());
        v[16..24].copy_from_slice(&(dims.len() as i64).to_le_bytes());
        for (i, d) in dims.iter().enumerate() {
            let off = 24 + 8 * i;
            v[off..off + 8].copy_from_slice(&d.to_le_bytes());
        }
        for (i, p) in pixdim.iter().enumerate() {
            let off = 104 + 8 * (i + 1);
            v[off..off + 8].copy_from_slice(&p.to_le_bytes());
        }
        v[240..240 + descrip.len()].copy_from_slice(descrip.as_bytes());
        v
    }

    #[test]
    fn parse_nifti1_le() {
        let h = parse_nifti_header(&nifti1(&[64, 64, 30], &[2.0, 2.0, 3.5], 16, "hello", false))
            .unwrap();
        assert_eq!(h.version, 1);
        assert_eq!(h.dims, vec![64, 64, 30]);
        assert_eq!(h.pixdim, vec![2.0, 2.0, 3.5]);
        assert_eq!(h.datatype, 16);
        assert_eq!(h.descrip, "hello");
    }

    #[test]
    fn parse_nifti1_be() {
        let h = parse_nifti_header(&nifti1(&[10, 20], &[1.5, 2.5], 4, "be", true)).unwrap();
        assert_eq!(h.version, 1);
        assert_eq!(h.dims, vec![10, 20]);
        assert_eq!(h.pixdim, vec![1.5, 2.5]);
        assert_eq!(h.datatype, 4);
        assert_eq!(h.descrip, "be");
    }

    #[test]
    fn parse_nifti2_le() {
        let h = parse_nifti_header(&nifti2(
            &[100, 200, 300, 4],
            &[0.5, 0.5, 1.0, 2.0],
            64,
            "two",
        ))
        .unwrap();
        assert_eq!(h.version, 2);
        assert_eq!(h.dims, vec![100, 200, 300, 4]);
        assert_eq!(h.pixdim, vec![0.5, 0.5, 1.0, 2.0]);
        assert_eq!(h.datatype, 64);
        assert_eq!(h.descrip, "two");
    }

    #[test]
    fn parse_rejects_short() {
        assert_eq!(parse_nifti_header(&[]), Err(NiftiError::TooShort));
        let v = nifti1(&[1], &[1.0], 2, "", false);
        assert_eq!(parse_nifti_header(&v[..300]), Err(NiftiError::TooShort));
        let v = nifti2(&[1], &[1.0], 2, "");
        assert_eq!(parse_nifti_header(&v[..400]), Err(NiftiError::TooShort));
        assert_eq!(parse_nifti_header(&[0u8; 400]), Err(NiftiError::BadMagic));
    }
}
