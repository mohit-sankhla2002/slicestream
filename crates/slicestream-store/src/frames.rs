//! Frame tables: where each frame of an instance lives in its stored file,
//! how they are encoded for the index, and how encapsulated fragments are
//! grouped into frames.

use slicestream_core::FrameLocation;

use crate::header::InstanceHeader;

/// Where each frame of an instance lives in the stored file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameTable {
    /// Native pixel data: frame `k` is `frame_len` bytes at `offset + k * frame_len`.
    Native {
        offset: u64,
        frame_len: u64,
        count: u32,
    },
    /// Bit-packed native pixel data (Bits Allocated 1): frame `k` is bits
    /// `[k * frame_bits, (k + 1) * frame_bits)` of the value at `offset`.
    Bits {
        offset: u64,
        frame_bits: u64,
        count: u32,
    },
    /// Encapsulated pixel data: `(offset, len)` of each fragment value, per frame.
    Fragments(Vec<Vec<(u64, u64)>>),
}

const ENCODING_VERSION: u8 = 1;
const KIND_NATIVE: u8 = 0;
const KIND_BITS: u8 = 1;
const KIND_FRAGMENTS: u8 = 2;

impl FrameTable {
    pub fn count(&self) -> u32 {
        match self {
            Self::Native { count, .. } | Self::Bits { count, .. } => *count,
            Self::Fragments(frames) => u32::try_from(frames.len()).unwrap_or(u32::MAX),
        }
    }

    /// Location of frame `frame` (0-based); `None` past the last frame.
    pub fn location(&self, frame: u32) -> Option<FrameLocation> {
        if frame >= self.count() {
            return None;
        }
        match self {
            Self::Native {
                offset, frame_len, ..
            } => Some(FrameLocation::Bytes {
                offset: offset.checked_add(u64::from(frame).checked_mul(*frame_len)?)?,
                len: *frame_len,
            }),
            Self::Bits {
                offset, frame_bits, ..
            } => Some(FrameLocation::Bits {
                offset: *offset,
                bit_start: u64::from(frame).checked_mul(*frame_bits)?,
                bit_len: *frame_bits,
            }),
            Self::Fragments(frames) => Some(FrameLocation::Fragments(
                frames.get(frame as usize)?.clone(),
            )),
        }
    }

    /// Version byte, kind byte, then little-endian fields. Fragments: frame
    /// count, then per frame a fragment count and `(offset, len)` pairs.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![ENCODING_VERSION];
        match self {
            Self::Native {
                offset,
                frame_len,
                count,
            } => {
                out.push(KIND_NATIVE);
                out.extend_from_slice(&offset.to_le_bytes());
                out.extend_from_slice(&frame_len.to_le_bytes());
                out.extend_from_slice(&count.to_le_bytes());
            }
            Self::Bits {
                offset,
                frame_bits,
                count,
            } => {
                out.push(KIND_BITS);
                out.extend_from_slice(&offset.to_le_bytes());
                out.extend_from_slice(&frame_bits.to_le_bytes());
                out.extend_from_slice(&count.to_le_bytes());
            }
            Self::Fragments(frames) => {
                out.push(KIND_FRAGMENTS);
                out.extend_from_slice(&(frames.len() as u32).to_le_bytes());
                for frame in frames {
                    out.extend_from_slice(&(frame.len() as u32).to_le_bytes());
                    for (offset, len) in frame {
                        out.extend_from_slice(&offset.to_le_bytes());
                        out.extend_from_slice(&len.to_le_bytes());
                    }
                }
            }
        }
        out
    }

    /// Inverse of [`encode`](Self::encode). `None` for anything else,
    /// including trailing bytes.
    pub fn decode(bytes: &[u8]) -> Option<FrameTable> {
        let mut r = ByteReader(bytes);
        if r.u8()? != ENCODING_VERSION {
            return None;
        }
        let table = match r.u8()? {
            KIND_NATIVE => Self::Native {
                offset: r.u64()?,
                frame_len: r.u64()?,
                count: r.u32()?,
            },
            KIND_BITS => Self::Bits {
                offset: r.u64()?,
                frame_bits: r.u64()?,
                count: r.u32()?,
            },
            KIND_FRAGMENTS => {
                let mut frames = Vec::new();
                for _ in 0..r.u32()? {
                    let mut frame = Vec::new();
                    for _ in 0..r.u32()? {
                        frame.push((r.u64()?, r.u64()?));
                    }
                    frames.push(frame);
                }
                Self::Fragments(frames)
            }
            _ => return None,
        };
        r.0.is_empty().then_some(table)
    }
}

pub(crate) struct ByteReader<'a>(pub(crate) &'a [u8]);

impl ByteReader<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.0.split_first_chunk::<N>()?;
        self.0 = rest;
        Some(*head)
    }

    pub(crate) fn u8(&mut self) -> Option<u8> {
        self.take::<1>().map(|[b]| b)
    }

    pub(crate) fn u32(&mut self) -> Option<u32> {
        self.take().map(u32::from_le_bytes)
    }

    pub(crate) fn u64(&mut self) -> Option<u64> {
        self.take().map(u64::from_le_bytes)
    }
}

/// Frame table for native pixel data of `len` bytes at `offset`. `None` for
/// non-image objects; `Err` when the value is too short for every frame.
pub(crate) fn native_table(
    header: &InstanceHeader,
    offset: u64,
    len: u32,
) -> Option<Result<FrameTable, &'static str>> {
    let info = header.frame_info.as_ref()?;
    let count = header.number_of_frames;
    let pixels = u64::from(info.rows) * u64::from(info.columns) * u64::from(info.samples_per_pixel);
    let (table, needed) = if info.bits_allocated == 1 {
        let needed = pixels.checked_mul(u64::from(count)).map(|b| b.div_ceil(8));
        let table = FrameTable::Bits {
            offset,
            frame_bits: pixels,
            count,
        };
        (table, needed)
    } else {
        // Same as FrameInfo::native_frame_len: whole bytes per sample.
        let frame_len = pixels * u64::from(info.bits_allocated).div_ceil(8);
        let table = FrameTable::Native {
            offset,
            frame_len,
            count,
        };
        (table, frame_len.checked_mul(u64::from(count)))
    };
    Some(match needed {
        Some(needed) if needed <= u64::from(len) => Ok(table),
        _ => Err("pixel data too short"),
    })
}

/// Group encapsulated fragments into `frames` frames by the first rule that
/// applies. `marked[i]` says fragment `i` starts with a codestream marker.
pub(crate) fn group_fragments(
    frames: u32,
    fragments: &[(u64, u64)],
    marked: &[bool],
    basic: &[u32],
    extended: Option<(&[u64], &[u64])>,
) -> Option<Vec<Vec<(u64, u64)>>> {
    let n = frames as usize;
    if fragments.is_empty() {
        return None;
    }
    // 1. Extended Offset Table with its lengths.
    if let Some((offsets, lengths)) = extended
        && let Some(groups) = by_extended_table(n, fragments, offsets, lengths)
    {
        return Some(groups);
    }
    // 2. Basic Offset Table with one entry per frame.
    if let Some(groups) = by_basic_table(n, fragments, basic) {
        return Some(groups);
    }
    // 3. One frame: all fragments.
    if n == 1 {
        return Some(vec![fragments.to_vec()]);
    }
    // 4. One fragment per frame.
    if fragments.len() == n {
        return Some(fragments.iter().map(|&f| vec![f]).collect());
    }
    // 5. Codestream start markers, if they yield exactly `frames` frames.
    let starts: Vec<usize> = (0..fragments.len())
        .filter(|&i| marked.get(i) == Some(&true))
        .collect();
    if starts.len() == n && starts.first() == Some(&0) {
        return Some(split_at(fragments, &starts));
    }
    None
}

/// Index of the fragment whose item starts `rel` bytes after the first
/// fragment's item. Table offsets count from the first fragment's item tag,
/// and every fragment value sits 8 bytes after its own item tag.
fn fragment_at(fragments: &[(u64, u64)], rel: u64) -> Option<usize> {
    let first = fragments.first()?.0;
    fragments
        .binary_search_by_key(&rel, |&(offset, _)| offset - first)
        .ok()
}

/// Fragment index where each frame starts, from offset table entries. `None`
/// unless every entry hits a fragment start, the first is 0 and they
/// strictly increase.
fn frame_starts(
    fragments: &[(u64, u64)],
    offsets: impl Iterator<Item = u64>,
) -> Option<Vec<usize>> {
    let starts: Vec<usize> = offsets
        .map(|rel| fragment_at(fragments, rel))
        .collect::<Option<_>>()?;
    if starts.first() != Some(&0) || starts.windows(2).any(|w| w[0] >= w[1]) {
        return None;
    }
    Some(starts)
}

/// Frame `k` starts at the fragment `offsets[k]` points to and is
/// `lengths[k]` bytes long (which leaves out trailing padding). It must fit
/// before the fragment where frame `k + 1` starts.
fn by_extended_table(
    n: usize,
    fragments: &[(u64, u64)],
    offsets: &[u64],
    lengths: &[u64],
) -> Option<Vec<Vec<(u64, u64)>>> {
    if offsets.len() != n || lengths.len() != n || lengths.contains(&0) {
        return None;
    }
    let starts = frame_starts(fragments, offsets.iter().copied())?;
    let mut frames = Vec::with_capacity(n);
    for (k, (&start, &len)) in starts.iter().zip(lengths).enumerate() {
        let end = starts.get(k + 1).copied().unwrap_or(fragments.len());
        let mut left = len;
        let mut frame = Vec::new();
        for &(offset, frag_len) in &fragments[start..end] {
            if left == 0 {
                break;
            }
            let take = frag_len.min(left);
            frame.push((offset, take));
            left -= take;
        }
        if left > 0 {
            return None;
        }
        frames.push(frame);
    }
    Some(frames)
}

fn by_basic_table(
    n: usize,
    fragments: &[(u64, u64)],
    basic: &[u32],
) -> Option<Vec<Vec<(u64, u64)>>> {
    if basic.is_empty() || basic.len() != n {
        return None;
    }
    let starts = frame_starts(fragments, basic.iter().map(|&rel| u64::from(rel)))?;
    Some(split_at(fragments, &starts))
}

/// Split `fragments` into frames that begin at the indexes in `starts`.
fn split_at(fragments: &[(u64, u64)], starts: &[usize]) -> Vec<Vec<(u64, u64)>> {
    starts
        .iter()
        .enumerate()
        .map(|(k, &start)| {
            let end = starts.get(k + 1).copied().unwrap_or(fragments.len());
            fragments[start..end].to_vec()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tables() -> Vec<FrameTable> {
        vec![
            FrameTable::Native {
                offset: 1234,
                frame_len: 512 * 512 * 2,
                count: 3,
            },
            FrameTable::Bits {
                offset: u64::MAX - 1,
                frame_bits: 15,
                count: u32::MAX,
            },
            FrameTable::Fragments(vec![vec![(10, 4), (22, 6)], vec![], vec![(36, 8)]]),
            FrameTable::Fragments(Vec::new()),
        ]
    }

    #[test]
    fn frame_table_roundtrip() {
        for table in tables() {
            let bytes = table.encode();
            assert_eq!(bytes[0], 1, "version byte");
            assert_eq!(FrameTable::decode(&bytes), Some(table.clone()));
            // Truncated or extended encodings are rejected.
            assert_eq!(FrameTable::decode(&bytes[..bytes.len() - 1]), None);
            let mut longer = bytes.clone();
            longer.push(0);
            assert_eq!(FrameTable::decode(&longer), None);
            let mut other_version = bytes.clone();
            other_version[0] = 2;
            assert_eq!(FrameTable::decode(&other_version), None);
        }
        assert_eq!(FrameTable::decode(&[1, 9]), None);
        assert_eq!(FrameTable::decode(&[]), None);
        // A huge claimed frame count with no data behind it.
        assert_eq!(FrameTable::decode(&[1, 2, 0xFF, 0xFF, 0xFF, 0xFF]), None);
    }

    #[test]
    fn frame_table_encoding_layout() {
        let native = FrameTable::Native {
            offset: 0x0102,
            frame_len: 3,
            count: 2,
        };
        let mut want = vec![1, 0];
        want.extend_from_slice(&0x0102u64.to_le_bytes());
        want.extend_from_slice(&3u64.to_le_bytes());
        want.extend_from_slice(&2u32.to_le_bytes());
        assert_eq!(native.encode(), want);

        let frags = FrameTable::Fragments(vec![vec![(5, 6)]]);
        let mut want = vec![1, 2];
        want.extend_from_slice(&1u32.to_le_bytes());
        want.extend_from_slice(&1u32.to_le_bytes());
        want.extend_from_slice(&5u64.to_le_bytes());
        want.extend_from_slice(&6u64.to_le_bytes());
        assert_eq!(frags.encode(), want);
    }

    #[test]
    fn frame_table_location() {
        let native = FrameTable::Native {
            offset: 100,
            frame_len: 50,
            count: 3,
        };
        assert_eq!(native.count(), 3);
        assert_eq!(
            native.location(0),
            Some(FrameLocation::Bytes {
                offset: 100,
                len: 50
            })
        );
        assert_eq!(
            native.location(2),
            Some(FrameLocation::Bytes {
                offset: 200,
                len: 50
            })
        );
        assert_eq!(native.location(3), None);

        let bits = FrameTable::Bits {
            offset: 7,
            frame_bits: 15,
            count: 2,
        };
        assert_eq!(
            bits.location(1),
            Some(FrameLocation::Bits {
                offset: 7,
                bit_start: 15,
                bit_len: 15
            })
        );
        assert_eq!(bits.location(2), None);

        let frags = FrameTable::Fragments(vec![vec![(10, 4)], vec![(22, 6), (36, 2)]]);
        assert_eq!(frags.count(), 2);
        assert_eq!(
            frags.location(1),
            Some(FrameLocation::Fragments(vec![(22, 6), (36, 2)]))
        );
        assert_eq!(frags.location(2), None);

        // Offsets that do not fit in a u64 have no location.
        let huge = FrameTable::Native {
            offset: u64::MAX - 10,
            frame_len: 10,
            count: 3,
        };
        assert_eq!(huge.location(2), None);
    }

    // Fragment values at 8-byte item headers apart: (offset, len).
    const FRAGS: [(u64, u64); 3] = [(100, 10), (118, 6), (132, 4)];

    #[test]
    fn extended_table_lengths_trim_padding() {
        // Frame 0 is fragments 0 and 1 minus one padding byte; frame 1 is fragment 2.
        let groups = group_fragments(2, &FRAGS, &[false; 3], &[], Some((&[0, 32], &[15, 4])));
        assert_eq!(
            groups,
            Some(vec![vec![(100, 10), (118, 5)], vec![(132, 4)]])
        );
    }

    #[test]
    fn unusable_tables_fall_through() {
        // An extended offset that is not a fragment start, and a basic table
        // that does not start at 0, fall through to one fragment per frame.
        let groups = group_fragments(
            3,
            &FRAGS,
            &[false; 3],
            &[8, 18, 32],
            Some((&[0, 5, 32], &[1, 1, 1])),
        );
        assert_eq!(
            groups,
            Some(vec![vec![(100, 10)], vec![(118, 6)], vec![(132, 4)]])
        );
        // Markers must start at the first fragment.
        assert_eq!(
            group_fragments(2, &FRAGS, &[false, true, true], &[], None),
            None
        );
        assert_eq!(group_fragments(1, &[], &[], &[], None), None);
    }

    #[test]
    fn malformed_extended_table_falls_through() {
        let two = &FRAGS[..2];
        let one_each = Some(vec![vec![(100, 10)], vec![(118, 6)]]);
        // Both frames point at the first fragment.
        let eot: (&[u64], &[u64]) = (&[0, 0], &[10, 6]);
        assert_eq!(
            group_fragments(2, two, &[false; 2], &[], Some(eot)),
            one_each
        );
        // Offsets that go backwards.
        let eot: (&[u64], &[u64]) = (&[18, 0], &[6, 10]);
        assert_eq!(
            group_fragments(2, two, &[false; 2], &[], Some(eot)),
            one_each
        );
        // Empty frames.
        let eot: (&[u64], &[u64]) = (&[0, 18], &[0, 0]);
        assert_eq!(
            group_fragments(2, two, &[false; 2], &[], Some(eot)),
            one_each
        );

        // Three fragments; markers say frames are [0], [1, 2] or [0, 1], [2].
        // Not starting at the first fragment.
        let eot: (&[u64], &[u64]) = (&[18, 32], &[6, 4]);
        assert_eq!(
            group_fragments(2, &FRAGS, &[true, false, true], &[], Some(eot)),
            Some(vec![vec![(100, 10), (118, 6)], vec![(132, 4)]])
        );
        // Frame 0's length runs into fragment 1, where frame 1 starts.
        let eot: (&[u64], &[u64]) = (&[0, 18], &[16, 10]);
        assert_eq!(
            group_fragments(2, &FRAGS, &[true, true, false], &[], Some(eot)),
            Some(vec![vec![(100, 10)], vec![(118, 6), (132, 4)]])
        );
    }
}
