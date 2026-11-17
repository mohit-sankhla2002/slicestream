//! `Accept` header parsing and content negotiation for QIDO-RS and WADO-RS.

use std::fmt;

const OCTET: &str = "application/octet-stream";
const DICOM: &str = "application/dicom";
const DICOM_JSON: &str = "application/dicom+json";
const BE_EXPLICIT: &str = "1.2.840.10008.1.2.2";
const LE_EXPLICIT: &str = "1.2.840.10008.1.2.1";
const JPHC: &str = "image/jphc";
/// HTJ2K Lossless RPCL: the transfer syntax of HTJ2K copies.
const HTJ2K_RPCL: &str = "1.2.840.10008.1.2.4.202";
/// HTJ2K Lossless Only, which an RPCL codestream also satisfies.
const HTJ2K_LOSSLESS: &str = "1.2.840.10008.1.2.4.201";

/// One entry of an `Accept` header.
#[derive(Debug, Clone, PartialEq)]
pub struct MediaRange {
    /// Lower-case `type/subtype`.
    pub essence: String,
    /// The `type=` parameter (multipart/related), quotes removed.
    pub multipart_type: Option<String>,
    pub transfer_syntax: Option<String>,
    pub q: f32,
}

/// The request cannot be served in any accepted form (HTTP 406).
#[derive(Debug, Clone, PartialEq)]
pub struct NotAcceptable(pub String);

impl fmt::Display for NotAcceptable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "not acceptable: {}", self.0)
    }
}

impl std::error::Error for NotAcceptable {}

/// Splits on `sep` outside double quotes. An unbalanced quote runs to the end.
fn split_unquoted(s: &str, sep: char) -> Vec<&str> {
    let mut out = Vec::new();
    let mut in_quotes = false;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        if c == '"' {
            in_quotes = !in_quotes;
        } else if c == sep && !in_quotes {
            out.push(&s[start..i]);
            start = i + c.len_utf8();
        }
    }
    out.push(&s[start..]);
    out
}

fn unquote(v: &str) -> String {
    v.trim().trim_matches('"').trim().to_owned()
}

/// Parses an `Accept` header. A missing or empty header gives one `*/*`.
/// Malformed entries are skipped; a bad q-value counts as 1.
pub fn parse_accept(header: Option<&str>) -> Vec<MediaRange> {
    let mut ranges = Vec::new();
    for item in split_unquoted(header.unwrap_or(""), ',') {
        let mut parts = split_unquoted(item, ';').into_iter();
        let essence = parts.next().unwrap_or("").trim().to_ascii_lowercase();
        if essence.is_empty() || essence.matches('/').count() != 1 {
            continue;
        }
        let mut range = MediaRange {
            essence,
            multipart_type: None,
            transfer_syntax: None,
            q: 1.0,
        };
        for param in parts {
            let Some((name, value)) = param.split_once('=') else {
                continue;
            };
            let value = unquote(value);
            match name.trim().to_ascii_lowercase().as_str() {
                "type" if !value.is_empty() => {
                    range.multipart_type = Some(value.to_ascii_lowercase())
                }
                "transfer-syntax" if !value.is_empty() => range.transfer_syntax = Some(value),
                "q" => {
                    if let Ok(q) = value.parse::<f32>()
                        && (0.0..=1.0).contains(&q)
                    {
                        range.q = q;
                    }
                }
                _ => {}
            }
        }
        ranges.push(range);
    }
    if ranges.is_empty() {
        ranges.push(MediaRange {
            essence: "*/*".into(),
            multipart_type: None,
            transfer_syntax: None,
            q: 1.0,
        });
    }
    ranges
}

/// Ranges with q > 0, highest q first, ties in header order.
fn by_preference(accept: &[MediaRange]) -> Vec<&MediaRange> {
    let mut v: Vec<&MediaRange> = accept.iter().filter(|r| r.q > 0.0).collect();
    v.sort_by(|a, b| b.q.total_cmp(&a.q));
    v
}

/// Whether the response is one bare part (`true`) or multipart/related
/// (`false`), and the media type asked for. `None` if the range cannot
/// describe a part (for example `multipart/related` with no `type`).
fn requested_kind(r: &MediaRange) -> Option<(bool, &str)> {
    match r.essence.as_str() {
        "*/*" | "application/*" | "multipart/*" | "image/*" => Some((false, "*/*")),
        "multipart/related" => r.multipart_type.as_deref().map(|t| (false, t)),
        other => Some((true, other)),
    }
}

/// Accepts JSON for QIDO-RS and metadata.
pub fn negotiate_json(accept: &[MediaRange]) -> Result<(), NotAcceptable> {
    for r in by_preference(accept) {
        let ok = match r.essence.as_str() {
            "*/*" | "application/*" | DICOM_JSON | "application/json" => true,
            "multipart/related" => r.multipart_type.as_deref() == Some(DICOM_JSON),
            _ => false,
        };
        if ok {
            return Ok(());
        }
    }
    Err(NotAcceptable(
        "only application/dicom+json is available here".into(),
    ))
}

#[derive(Debug, Clone, PartialEq)]
pub struct InstancePlan {
    /// A bare `application/dicom` response (no multipart wrapper).
    pub single_part: bool,
}

/// `Content-Type` of an instance part.
pub fn instance_part_type(ts: &str) -> String {
    format!("{DICOM}; transfer-syntax={ts}")
}

/// Negotiates a whole-instance retrieve. Instances are sent as stored, so a
/// specific transfer syntax must equal every stored one.
pub fn negotiate_instances(
    accept: &[MediaRange],
    stored_ts: &[&str],
    count: usize,
) -> Result<InstancePlan, NotAcceptable> {
    for r in by_preference(accept) {
        let Some((single, media)) = requested_kind(r) else {
            continue;
        };
        if media != DICOM && media != "*/*" {
            continue;
        }
        if let Some(ts) = r.transfer_syntax.as_deref()
            && ts != "*"
            && stored_ts.iter().any(|s| *s != ts)
        {
            continue;
        }
        if single && count != 1 {
            continue;
        }
        return Ok(InstancePlan {
            single_part: single,
        });
    }
    Err(NotAcceptable(
        "no acceptable media type or transfer syntax for application/dicom".into(),
    ))
}

/// The image media type for an encapsulated transfer syntax. Native: `None`.
pub fn media_type_for_ts(ts: &str) -> Option<&'static str> {
    Some(match ts {
        "1.2.840.10008.1.2.4.50"
        | "1.2.840.10008.1.2.4.51"
        | "1.2.840.10008.1.2.4.57"
        | "1.2.840.10008.1.2.4.70" => "image/jpeg",
        "1.2.840.10008.1.2.4.80" | "1.2.840.10008.1.2.4.81" => "image/jls",
        "1.2.840.10008.1.2.4.90" | "1.2.840.10008.1.2.4.91" => "image/jp2",
        "1.2.840.10008.1.2.4.201" | "1.2.840.10008.1.2.4.202" | "1.2.840.10008.1.2.4.203" => {
            "image/jphc"
        }
        "1.2.840.10008.1.2.5" => "image/dicom-rle",
        "1.2.840.10008.1.2.4.110" | "1.2.840.10008.1.2.4.111" | "1.2.840.10008.1.2.4.112" => {
            "image/jxl"
        }
        _ => return None,
    })
}

/// What is stored for the frames being served.
#[derive(Debug, Clone)]
pub struct FrameSource<'a> {
    pub transfer_syntax: &'a str,
    pub encapsulated: bool,
    pub bits_allocated: u16,
    /// An HTJ2K copy (`1.2.840.10008.1.2.4.202`) can be served: the frames
    /// are eligible and copies are not turned off.
    pub copy_possible: bool,
    /// `transfer-syntax=*`, `*/*` and no `Accept` select the copy
    /// (`[htj2k] prefer_for_wildcard`).
    pub prefer_copy_for_wildcard: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartPlan {
    pub single_part: bool,
    /// `Content-Type` of each part.
    pub part_content_type: String,
    /// Swap each 16- or 32-bit word (big-endian source, little-endian wanted).
    pub byte_swap: bool,
    /// Serve the HTJ2K copy instead of the stored frames.
    pub use_copy: bool,
}

/// Negotiates frames and Pixel Data bulk. `part_count` is the number of parts
/// the response will hold.
pub fn negotiate_frames(
    accept: &[MediaRange],
    src: &FrameSource,
    part_count: usize,
) -> Result<PartPlan, NotAcceptable> {
    let stored = src.transfer_syntax;
    let stored_media = if src.encapsulated {
        media_type_for_ts(stored)
    } else {
        None
    };
    for r in by_preference(accept) {
        let Some((single, media)) = requested_kind(r) else {
            continue;
        };
        if single && part_count != 1 {
            continue;
        }
        let ts = r.transfer_syntax.as_deref();
        if src.copy_possible
            && let Some(copy_media) = copy_media(r, media, ts, src.prefer_copy_for_wildcard)
        {
            return Ok(PartPlan {
                single_part: single,
                part_content_type: format!("{copy_media}; transfer-syntax={HTJ2K_RPCL}"),
                byte_swap: false,
                use_copy: true,
            });
        }
        let ts_ok = ts.is_none_or(|t| t == "*" || t == stored);
        let plan = |content_type: String, byte_swap: bool| PartPlan {
            single_part: single,
            part_content_type: content_type,
            byte_swap,
            use_copy: false,
        };
        let as_stored = |media: &str| plan(format!("{media}; transfer-syntax={stored}"), false);
        let found = match media {
            // A wildcard range that names an HTJ2K syntax is an explicit
            // HTJ2K request: the copy (above) or nothing, unless the data
            // is stored in that syntax.
            "*/*" if matches!(ts, Some(HTJ2K_RPCL | HTJ2K_LOSSLESS)) && ts != Some(stored) => None,
            "*/*" => Some(as_stored(stored_media.unwrap_or(OCTET))),
            OCTET => match ts {
                // Native pixels as little-endian; nothing else qualifies.
                None if !src.encapsulated => {
                    let swap = stored == BE_EXPLICIT && matches!(src.bits_allocated, 16 | 32);
                    Some(plan(
                        format!("{OCTET}; transfer-syntax={LE_EXPLICIT}"),
                        swap,
                    ))
                }
                None => None,
                Some(_) if ts_ok => Some(as_stored(OCTET)),
                Some(_) => None,
            },
            m if stored_media == Some(m) && ts_ok => Some(as_stored(m)),
            _ => None,
        };
        if let Some(p) = found {
            return Ok(p);
        }
    }
    Err(NotAcceptable(format!(
        "frames are stored as {stored} and cannot be served in an accepted form"
    )))
}

/// The part media type when range `r` (requesting `media` in transfer
/// syntax `ts`) selects the HTJ2K copy; `None` when it does not.
/// `image/jphc` with no syntax, `*`, `.201` or `.202`, and
/// `application/octet-stream` with `.202`, always select it; wildcards
/// only when `prefer_wildcard`.
fn copy_media(
    r: &MediaRange,
    media: &str,
    ts: Option<&str>,
    prefer_wildcard: bool,
) -> Option<&'static str> {
    let wildcard_ts = ts.is_none_or(|t| t == "*");
    match media {
        JPHC if matches!(ts, None | Some("*" | HTJ2K_RPCL | HTJ2K_LOSSLESS)) => Some(JPHC),
        OCTET if ts == Some(HTJ2K_RPCL) => Some(OCTET),
        OCTET if ts == Some("*") && prefer_wildcard => Some(OCTET),
        "*/*"
            if matches!(ts, Some(HTJ2K_RPCL | HTJ2K_LOSSLESS))
                || (wildcard_ts && prefer_wildcard) =>
        {
            Some(if r.essence == "application/*" {
                OCTET
            } else {
                JPHC
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LE: &str = "1.2.840.10008.1.2.1";
    const IMPLICIT: &str = "1.2.840.10008.1.2";
    const JPEG: &str = "1.2.840.10008.1.2.4.50";
    const HTJ2K: &str = "1.2.840.10008.1.2.4.201";

    fn src(ts: &str, enc: bool, bits: u16) -> FrameSource<'_> {
        FrameSource {
            transfer_syntax: ts,
            encapsulated: enc,
            bits_allocated: bits,
            copy_possible: false,
            prefer_copy_for_wildcard: false,
        }
    }

    /// Native little-endian 16-bit frames with a copy available.
    fn copyable(prefer: bool) -> FrameSource<'static> {
        FrameSource {
            copy_possible: true,
            prefer_copy_for_wildcard: prefer,
            ..src(LE, false, 16)
        }
    }

    const RPCL: &str = "1.2.840.10008.1.2.4.202";

    fn frames(h: &str, s: &FrameSource, n: usize) -> Result<PartPlan, NotAcceptable> {
        negotiate_frames(&parse_accept(Some(h)), s, n)
    }

    const OHIF: &str = "multipart/related; type=application/octet-stream; transfer-syntax=*";
    const OHIF_Q: &str = "multipart/related; type=\"application/octet-stream\"; transfer-syntax=*";
    const DWC: &str = "multipart/related; type=\"application/octet-stream\"";
    const BROWSER: &str = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";

    #[test]
    fn accept_missing_is_any() {
        let r = parse_accept(None);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].essence, "*/*");
        assert_eq!(r[0].q, 1.0);
        assert_eq!(parse_accept(Some("  ")).len(), 1);
    }

    #[test]
    fn accept_quoted_and_unquoted_type() {
        for h in [OHIF, OHIF_Q] {
            let r = parse_accept(Some(h));
            assert_eq!(r.len(), 1);
            assert_eq!(r[0].essence, "multipart/related");
            assert_eq!(r[0].multipart_type.as_deref(), Some(OCTET));
            assert_eq!(r[0].transfer_syntax.as_deref(), Some("*"));
        }
        let r = parse_accept(Some(DWC));
        assert_eq!(r[0].multipart_type.as_deref(), Some(OCTET));
        assert_eq!(r[0].transfer_syntax, None);
    }

    #[test]
    fn accept_q_values_order() {
        let r = parse_accept(Some("a/b;q=0.5, c/d, e/f;q=bogus, g/h;q=7"));
        let q: Vec<f32> = r.iter().map(|m| m.q).collect();
        assert_eq!(q, [0.5, 1.0, 1.0, 1.0]);
        let order: Vec<&str> = by_preference(&r)
            .iter()
            .map(|m| m.essence.as_str())
            .collect();
        assert_eq!(order, ["c/d", "e/f", "g/h", "a/b"]);
    }

    #[test]
    fn accept_multiple_ranges() {
        let r = parse_accept(Some(BROWSER));
        let e: Vec<&str> = r.iter().map(|m| m.essence.as_str()).collect();
        assert_eq!(
            e,
            [
                "text/html",
                "application/xhtml+xml",
                "application/xml",
                "*/*"
            ]
        );
        assert_eq!(r[3].q, 0.8);
        // A comma or semicolon inside quotes does not split.
        let r = parse_accept(Some(
            "multipart/related; type=\"a/b,c\"; transfer-syntax=1.2",
        ));
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].multipart_type.as_deref(), Some("a/b,c"));
    }

    #[test]
    fn accept_malformed_never_panics() {
        for h in [
            "\"",
            "a/b; type=\"",
            ";;;",
            ",,,",
            "a/b;=",
            "a/b; q=",
            "noslash",
            "a/b/c",
            "\u{1F600}/x;q=\u{1F600}",
            "multipart/related; type=",
        ] {
            let _ = parse_accept(Some(h));
        }
        assert_eq!(parse_accept(Some("a/b; q=")).len(), 1);
    }

    #[test]
    fn json_xml_only_406() {
        let r = parse_accept(Some("application/dicom+xml"));
        assert!(negotiate_json(&r).is_err());
        let r = parse_accept(Some("text/xml, application/xml;q=0.9"));
        assert!(negotiate_json(&r).is_err());
        let r = parse_accept(Some("application/dicom+json;q=0"));
        assert!(negotiate_json(&r).is_err());
    }

    #[test]
    fn json_star() {
        assert!(negotiate_json(&parse_accept(None)).is_ok());
        assert!(negotiate_json(&parse_accept(Some(BROWSER))).is_ok());
        for h in [
            "application/dicom+json",
            "application/json",
            "application/*",
            "multipart/related; type=\"application/dicom+json\"",
        ] {
            assert!(negotiate_json(&parse_accept(Some(h))).is_ok(), "{h}");
        }
        assert!(
            negotiate_json(&parse_accept(Some(
                "multipart/related; type=application/dicom"
            )))
            .is_err()
        );
    }

    #[test]
    fn frames_octet_native() {
        for h in [DWC, "application/octet-stream"] {
            let p = frames(h, &src(LE, false, 16), 3);
            if h == DWC {
                let p = p.unwrap();
                assert!(!p.single_part && !p.byte_swap);
                assert_eq!(
                    p.part_content_type,
                    format!("{OCTET}; transfer-syntax={LE}")
                );
            }
        }
        let p = frames(DWC, &src(IMPLICIT, false, 8), 2).unwrap();
        assert_eq!(
            p.part_content_type,
            format!("{OCTET}; transfer-syntax={LE}")
        );
        assert!(!p.byte_swap);
    }

    #[test]
    fn frames_octet_big_endian_swaps() {
        let be = "1.2.840.10008.1.2.2";
        assert!(frames(DWC, &src(be, false, 16), 1).unwrap().byte_swap);
        assert!(frames(DWC, &src(be, false, 32), 1).unwrap().byte_swap);
        assert!(!frames(DWC, &src(be, false, 8), 1).unwrap().byte_swap);
        // transfer-syntax=* keeps it as stored, so no swap.
        let p = frames(OHIF, &src(be, false, 16), 1).unwrap();
        assert!(!p.byte_swap);
        assert_eq!(
            p.part_content_type,
            format!("{OCTET}; transfer-syntax={be}")
        );
    }

    #[test]
    fn frames_octet_encapsulated_406() {
        assert!(frames(DWC, &src(JPEG, true, 8), 1).is_err());
        assert!(frames("application/octet-stream", &src(JPEG, true, 8), 1).is_err());
    }

    #[test]
    fn frames_star_as_stored() {
        for h in [OHIF, OHIF_Q] {
            let p = frames(h, &src(JPEG, true, 8), 4).unwrap();
            assert!(!p.single_part);
            assert_eq!(
                p.part_content_type,
                format!("{OCTET}; transfer-syntax={JPEG}")
            );
        }
        let p = frames(BROWSER, &src(LE, false, 16), 1).unwrap();
        assert!(!p.single_part);
    }

    #[test]
    fn frames_specific_ts_match_and_mismatch() {
        let h =
            format!("multipart/related; type=\"application/octet-stream\"; transfer-syntax={JPEG}");
        assert!(frames(&h, &src(JPEG, true, 8), 1).is_ok());
        assert!(frames(&h, &src(LE, false, 8), 1).is_err());
    }

    #[test]
    fn frames_image_jpeg() {
        let p = frames(
            "multipart/related; type=\"image/jpeg\"",
            &src(JPEG, true, 8),
            2,
        )
        .unwrap();
        assert_eq!(
            p.part_content_type,
            format!("image/jpeg; transfer-syntax={JPEG}")
        );
        let h = format!("image/jpeg; transfer-syntax={JPEG}");
        assert!(frames(&h, &src(JPEG, true, 8), 1).unwrap().single_part);
        assert!(frames("image/jpeg", &src(LE, false, 8), 1).is_err());
        assert!(frames("image/jp2", &src(JPEG, true, 8), 1).is_err());
    }

    #[test]
    fn frames_image_jphc_wrong_ts_406() {
        let h = "multipart/related; type=image/jphc; transfer-syntax=1.2.840.10008.1.2.4.202";
        assert!(frames(h, &src(HTJ2K, true, 8), 1).is_err());
        assert!(
            frames(
                "multipart/related; type=image/jphc",
                &src(HTJ2K, true, 8),
                1
            )
            .is_ok()
        );
    }

    #[test]
    fn frames_bare_single_part_only_for_one() {
        let s = src(JPEG, true, 8);
        assert!(frames("image/jpeg", &s, 1).unwrap().single_part);
        assert!(frames("image/jpeg", &s, 2).is_err());
        // A later range can still serve it.
        let p = frames(
            "image/jpeg, multipart/related; type=image/jpeg;q=0.5",
            &s,
            2,
        )
        .unwrap();
        assert!(!p.single_part);
    }

    #[test]
    fn jphc_selects_copy() {
        let want = format!("image/jphc; transfer-syntax={RPCL}");
        for h in [
            "multipart/related; type=\"image/jphc\"".to_owned(),
            format!("multipart/related; type=image/jphc; transfer-syntax={RPCL}"),
            "multipart/related; type=image/jphc; transfer-syntax=1.2.840.10008.1.2.4.201"
                .to_owned(),
            "multipart/related; type=image/jphc; transfer-syntax=*".to_owned(),
        ] {
            let p = frames(&h, &copyable(false), 3).unwrap();
            assert!(p.use_copy && !p.single_part && !p.byte_swap, "{h}");
            assert_eq!(p.part_content_type, want, "{h}");
        }
        let p = frames("image/jphc", &copyable(false), 1).unwrap();
        assert!(p.use_copy && p.single_part);
        assert_eq!(p.part_content_type, want);
        // Another HTJ2K syntax is not what the copy holds.
        let h = "multipart/related; type=image/jphc; transfer-syntax=1.2.840.10008.1.2.4.203";
        assert!(frames(h, &copyable(false), 1).is_err());
        // A preferred range that cannot be served gives way to the next.
        let h = "multipart/related; type=image/jpeg, multipart/related; type=image/jphc;q=0.5";
        assert!(frames(h, &copyable(false), 1).unwrap().use_copy);
    }

    #[test]
    fn octet_202_selects_copy() {
        let h =
            format!("multipart/related; type=\"application/octet-stream\"; transfer-syntax={RPCL}");
        let p = frames(&h, &copyable(false), 2).unwrap();
        assert!(p.use_copy && !p.single_part);
        assert_eq!(
            p.part_content_type,
            format!("{OCTET}; transfer-syntax={RPCL}")
        );
        let h = format!("application/octet-stream; transfer-syntax={RPCL}");
        let p = frames(&h, &copyable(false), 1).unwrap();
        assert!(p.use_copy && p.single_part);
        // Without a copy, as today: the stored syntax must match.
        assert!(frames(&h, &src(LE, false, 16), 1).is_err());
        // Plain octet-stream stays native little-endian.
        let p = frames(DWC, &copyable(true), 1).unwrap();
        assert!(!p.use_copy);
        assert_eq!(
            p.part_content_type,
            format!("{OCTET}; transfer-syntax={LE}")
        );
    }

    #[test]
    fn wildcard_keeps_stored_by_default() {
        for h in [OHIF, OHIF_Q, "*/*", BROWSER] {
            let p = frames(h, &copyable(false), 1).unwrap();
            assert!(!p.use_copy, "{h}");
            assert_eq!(
                p.part_content_type,
                format!("{OCTET}; transfer-syntax={LE}"),
                "{h}"
            );
        }
        let p = negotiate_frames(&parse_accept(None), &copyable(false), 1).unwrap();
        assert!(!p.use_copy);
    }

    #[test]
    fn wildcard_with_htj2k_syntax_is_explicit() {
        for r in ["*/*", "image/*", "application/*"] {
            for ts in [RPCL, HTJ2K] {
                let h = format!("{r}; transfer-syntax={ts}");
                // Copy possible: served from the copy.
                let p = frames(&h, &copyable(false), 1).unwrap();
                assert!(p.use_copy, "{h}");
                // No copy: not acceptable, not "as stored".
                assert!(frames(&h, &src(LE, false, 16), 1).is_err(), "{h}");
                // A later range still gets its turn.
                let h2 = format!("{h}, */*;q=0.5");
                let p = frames(&h2, &src(LE, false, 16), 1).unwrap();
                assert!(!p.use_copy, "{h2}");
            }
        }
        // Stored in that syntax: as stored.
        let stored = src(RPCL, true, 16);
        let h = format!("*/*; transfer-syntax={RPCL}");
        assert!(!frames(&h, &stored, 1).unwrap().use_copy);
    }

    #[test]
    fn wildcard_prefers_copy_when_configured() {
        for h in [OHIF, OHIF_Q] {
            let p = frames(h, &copyable(true), 2).unwrap();
            assert!(p.use_copy, "{h}");
            assert_eq!(
                p.part_content_type,
                format!("{OCTET}; transfer-syntax={RPCL}")
            );
        }
        for p in [
            frames("*/*", &copyable(true), 2).unwrap(),
            negotiate_frames(&parse_accept(None), &copyable(true), 2).unwrap(),
            frames("image/*", &copyable(true), 2).unwrap(),
        ] {
            assert!(p.use_copy);
            assert_eq!(
                p.part_content_type,
                format!("image/jphc; transfer-syntax={RPCL}")
            );
        }
        // Not possible: as stored.
        let p = frames(OHIF, &src(LE, false, 16), 1).unwrap();
        assert!(!p.use_copy);
        // A specific stored syntax is still served as stored.
        let h =
            format!("multipart/related; type=\"application/octet-stream\"; transfer-syntax={LE}");
        assert!(!frames(&h, &copyable(true), 1).unwrap().use_copy);
    }

    #[test]
    fn jphc_without_copy_is_406() {
        // RGB or off: no copy.
        assert!(frames("multipart/related; type=image/jphc", &src(LE, false, 8), 1).is_err());
        let h =
            format!("multipart/related; type=\"application/octet-stream\"; transfer-syntax={RPCL}");
        assert!(frames(&h, &src(LE, false, 8), 1).is_err());
    }

    #[test]
    fn stored_htj2k_as_stored() {
        let stored = src(RPCL, true, 16);
        let p = frames("multipart/related; type=image/jphc", &stored, 1).unwrap();
        assert!(!p.use_copy);
        assert_eq!(
            p.part_content_type,
            format!("image/jphc; transfer-syntax={RPCL}")
        );
    }

    #[test]
    fn instances_star_and_absent_as_stored() {
        for h in [
            None,
            Some("*/*"),
            Some("multipart/related; type=\"application/dicom\""),
            Some("multipart/related; type=application/dicom; transfer-syntax=*"),
        ] {
            let p = negotiate_instances(&parse_accept(h), &[LE, JPEG], 2).unwrap();
            assert!(!p.single_part);
        }
        assert_eq!(
            instance_part_type(LE),
            format!("application/dicom; transfer-syntax={LE}")
        );
    }

    #[test]
    fn instances_specific_ts_mismatch_406() {
        let h = format!("multipart/related; type=\"application/dicom\"; transfer-syntax={LE}");
        let a = parse_accept(Some(&h));
        assert!(negotiate_instances(&a, &[LE], 1).is_ok());
        assert!(negotiate_instances(&a, &[LE, JPEG], 2).is_err());
        assert!(
            negotiate_instances(&parse_accept(Some("application/dicom+json")), &[LE], 1).is_err()
        );
    }

    #[test]
    fn instances_bare_dicom_single() {
        let a = parse_accept(Some("application/dicom"));
        assert!(negotiate_instances(&a, &[LE], 1).unwrap().single_part);
        assert!(negotiate_instances(&a, &[LE, LE], 2).is_err());
    }

    #[test]
    fn media_type_table() {
        let cases = [
            ("1.2.840.10008.1.2.4.50", "image/jpeg"),
            ("1.2.840.10008.1.2.4.51", "image/jpeg"),
            ("1.2.840.10008.1.2.4.57", "image/jpeg"),
            ("1.2.840.10008.1.2.4.70", "image/jpeg"),
            ("1.2.840.10008.1.2.4.80", "image/jls"),
            ("1.2.840.10008.1.2.4.81", "image/jls"),
            ("1.2.840.10008.1.2.4.90", "image/jp2"),
            ("1.2.840.10008.1.2.4.91", "image/jp2"),
            ("1.2.840.10008.1.2.4.201", "image/jphc"),
            ("1.2.840.10008.1.2.4.202", "image/jphc"),
            ("1.2.840.10008.1.2.4.203", "image/jphc"),
            ("1.2.840.10008.1.2.5", "image/dicom-rle"),
            ("1.2.840.10008.1.2.4.110", "image/jxl"),
            ("1.2.840.10008.1.2.4.111", "image/jxl"),
            ("1.2.840.10008.1.2.4.112", "image/jxl"),
        ];
        for (ts, mt) in cases {
            assert_eq!(media_type_for_ts(ts), Some(mt), "{ts}");
        }
        for ts in [
            IMPLICIT,
            LE,
            "1.2.840.10008.1.2.2",
            "1.2.840.10008.1.2.1.99",
        ] {
            assert_eq!(media_type_for_ts(ts), None);
        }
    }
}
