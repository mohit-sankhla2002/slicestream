//! Derived HTJ2K copies: the per-frame location table of a copy blob and the
//! state recorded for each instance.

use crate::BlobHash;
use crate::frames::ByteReader;

const ENCODING_VERSION: u8 = 1;

/// Where one frame's codestream lives in the copy blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyFrame {
    pub offset: u64,
    pub len: u64,
    /// Cumulative end offsets (relative to the frame) of its progressive
    /// prefixes, in order.
    pub ends: Vec<u32>,
}

/// Locations of every frame of a copy blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyFrames(pub Vec<CopyFrame>);

impl CopyFrames {
    /// `[version u8 = 1][count u32 LE]`, then per frame
    /// `[offset u64][len u64][n u8][ends u32 x n]`, all little-endian.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![ENCODING_VERSION];
        out.extend_from_slice(&(self.0.len() as u32).to_le_bytes());
        for f in &self.0 {
            debug_assert!(f.ends.len() <= usize::from(u8::MAX));
            out.extend_from_slice(&f.offset.to_le_bytes());
            out.extend_from_slice(&f.len.to_le_bytes());
            out.push(f.ends.len() as u8);
            for e in &f.ends {
                out.extend_from_slice(&e.to_le_bytes());
            }
        }
        out
    }

    /// Inverse of [`encode`](Self::encode). `None` for anything else,
    /// including trailing bytes.
    pub fn decode(bytes: &[u8]) -> Option<CopyFrames> {
        let mut r = ByteReader(bytes);
        if r.u8()? != ENCODING_VERSION {
            return None;
        }
        let mut frames = Vec::new();
        for _ in 0..r.u32()? {
            let offset = r.u64()?;
            let len = r.u64()?;
            let mut ends = Vec::new();
            for _ in 0..r.u8()? {
                ends.push(r.u32()?);
            }
            frames.push(CopyFrame { offset, len, ends });
        }
        r.0.is_empty().then_some(CopyFrames(frames))
    }
}

/// What is known about the HTJ2K copy of one instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyState {
    /// A verified copy blob (kind `j2cs`) with its frame locations.
    Ready { blob: BlobHash, frames: CopyFrames },
    /// The instance cannot have a copy; the reason is recorded once.
    NotEligible(String),
    /// Encoding or verification failed; the reason is recorded.
    Failed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_rejects_junk() {
        let f = CopyFrames(vec![
            CopyFrame {
                offset: 7,
                len: 100,
                ends: vec![10, 40, 100],
            },
            CopyFrame {
                offset: 107,
                len: 5,
                ends: vec![],
            },
        ]);
        assert_eq!(CopyFrames::decode(&f.encode()), Some(f.clone()));
        assert_eq!(CopyFrames::decode(b"junk"), None);
        let mut trailing = f.encode();
        trailing.push(0);
        assert_eq!(CopyFrames::decode(&trailing), None);
        let short = &f.encode()[..f.encode().len() - 1];
        assert_eq!(CopyFrames::decode(short), None);
    }
}
