//! HTTP `Range` headers (RFC 9110 14.1.2), byte ranges only.

/// One byte range of a `Range: bytes=` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    /// `first-last`, both inclusive.
    FromTo(u64, u64),
    /// `first-`: from `first` to the end.
    From(u64),
    /// `-n`: the last `n` bytes.
    Suffix(u64),
}

/// The ranges of a `Range` header value, in order. `None` when the unit is
/// not `bytes` or any range is malformed (including `last < first`).
/// Empty list elements are skipped, as RFC 9110 5.6.1.2 asks.
pub fn parse_range(header: &str) -> Option<Vec<ByteRange>> {
    let (unit, set) = header.split_once('=')?;
    if !unit.trim().eq_ignore_ascii_case("bytes") {
        return None;
    }
    let mut out = Vec::new();
    for item in set.split(',').map(str::trim).filter(|i| !i.is_empty()) {
        let (first, last) = item.split_once('-')?;
        out.push(match (number(first), number(last)) {
            (Some(first), Some(last)) if first <= last => ByteRange::FromTo(first, last),
            (Some(first), None) if last.is_empty() => ByteRange::From(first),
            (None, Some(suffix)) if first.is_empty() => ByteRange::Suffix(suffix),
            _ => return None,
        });
    }
    (!out.is_empty()).then_some(out)
}

/// A non-empty run of ASCII digits that fits `u64`.
fn number(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// `[start, end)` of `r` within `len` bytes; `None` when unsatisfiable.
pub fn resolve(r: &ByteRange, len: u64) -> Option<(u64, u64)> {
    let (start, end) = match *r {
        ByteRange::FromTo(first, last) => (first, last.saturating_add(1).min(len)),
        ByteRange::From(first) => (first, len),
        ByteRange::Suffix(n) => (len.saturating_sub(n), if n == 0 { 0 } else { len }),
    };
    (start < end).then_some((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ByteRange::*;

    #[test]
    fn range_forms() {
        assert_eq!(parse_range("bytes=0-99"), Some(vec![FromTo(0, 99)]));
        assert_eq!(parse_range("bytes=100-"), Some(vec![From(100)]));
        assert_eq!(parse_range("bytes=-500"), Some(vec![Suffix(500)]));
        assert_eq!(parse_range("bytes=5-5"), Some(vec![FromTo(5, 5)]));
        assert_eq!(parse_range("bytes=5-2"), None);
        assert_eq!(parse_range("items=0-1"), None);
        assert_eq!(
            parse_range("bytes=0-1,4-5"),
            Some(vec![FromTo(0, 1), FromTo(4, 5)])
        );
        assert_eq!(
            parse_range("Bytes = 0-1 , -3"),
            Some(vec![FromTo(0, 1), Suffix(3)])
        );
        // Empty list elements are skipped.
        assert_eq!(parse_range("bytes=0-1,"), Some(vec![FromTo(0, 1)]));
        assert_eq!(
            parse_range("bytes=0-1,,2-3"),
            Some(vec![FromTo(0, 1), FromTo(2, 3)])
        );
        for bad in [
            "",
            "bytes",
            "bytes=",
            "bytes=-",
            "bytes=a-b",
            "bytes=1-2-3",
            "bytes=+1-2",
            "bytes=1 -2",
            "bytes=,",
            "bytes=18446744073709551616-",
        ] {
            assert_eq!(parse_range(bad), None, "{bad:?}");
        }
        assert_eq!(resolve(&From(1000), 500), None);
        assert_eq!(resolve(&From(499), 500), Some((499, 500)));
        assert_eq!(resolve(&Suffix(900), 500), Some((0, 500)));
        assert_eq!(resolve(&Suffix(100), 500), Some((400, 500)));
        assert_eq!(resolve(&Suffix(0), 500), None);
        assert_eq!(resolve(&FromTo(0, 10_000), 500), Some((0, 500)));
        assert_eq!(resolve(&FromTo(0, 1023), 5000), Some((0, 1024)));
        assert_eq!(resolve(&FromTo(500, 600), 500), None);
        assert_eq!(resolve(&FromTo(0, u64::MAX), 10), Some((0, 10)));
        assert_eq!(resolve(&From(0), 0), None);
        assert_eq!(resolve(&Suffix(5), 0), None);
    }
}
