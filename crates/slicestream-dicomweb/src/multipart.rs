//! `multipart/related` framing and BulkDataURI splicing.

use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;

/// Writes multipart/related boundaries. Part bodies are streamed by the caller.
pub struct Multipart {
    boundary: String,
    started: AtomicBool,
}

impl Default for Multipart {
    fn default() -> Self {
        Self::new()
    }
}

impl Multipart {
    /// A writer with a random 32 hex character boundary.
    pub fn new() -> Self {
        Self::with_boundary(format!("{:032x}", rand::random::<u128>()))
    }

    fn with_boundary(boundary: String) -> Self {
        Self {
            boundary,
            started: AtomicBool::new(false),
        }
    }

    /// The response `Content-Type`; `type_param` is the part media type.
    pub fn content_type(&self, type_param: &str) -> String {
        format!(
            "multipart/related; type=\"{type_param}\"; boundary={}",
            self.boundary
        )
    }

    /// The bytes before a part body. Every part but the first is preceded by
    /// the CRLF that ends the previous body.
    pub fn part_header(&self, content_type: &str) -> Bytes {
        let lead = if self.started.swap(true, Ordering::Relaxed) {
            "\r\n"
        } else {
            ""
        };
        Bytes::from(format!(
            "{lead}--{}\r\nContent-Type: {content_type}\r\n\r\n",
            self.boundary
        ))
    }

    /// The closing delimiter.
    pub fn close(&self) -> Bytes {
        Bytes::from(format!("\r\n--{}--\r\n", self.boundary))
    }
}

fn json_escape(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o
}

const KEY: &[u8] = b"\"BulkDataURI\":\"";

/// Copies `json` into `out`, inserting `prefix` right after each
/// `"BulkDataURI":"`. The prefix is JSON-escaped once, so any characters in it
/// stay inside the string. A literal key inside a string value is always
/// escaped in JSON, so it cannot match here.
pub fn splice_bulk_uris(json: &[u8], prefix: &str, out: &mut Vec<u8>) {
    let escaped = json_escape(prefix);
    let prefix = escaped.as_str();
    out.reserve(json.len());
    let mut rest = json;
    while let Some(pos) = rest.windows(KEY.len()).position(|w| w == KEY) {
        let end = pos + KEY.len();
        out.extend_from_slice(&rest[..end]);
        out.extend_from_slice(prefix.as_bytes());
        rest = &rest[end..];
    }
    out.extend_from_slice(rest);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multipart_exact_bytes() {
        let m = Multipart::with_boundary("b".into());
        assert_eq!(
            m.content_type("application/octet-stream"),
            "multipart/related; type=\"application/octet-stream\"; boundary=b"
        );
        let mut all = Vec::new();
        all.extend_from_slice(&m.part_header("application/octet-stream"));
        all.extend_from_slice(b"AAA");
        all.extend_from_slice(&m.part_header("image/jpeg; transfer-syntax=1.2"));
        all.extend_from_slice(b"BB");
        all.extend_from_slice(&m.close());
        assert_eq!(
            all,
            b"--b\r\nContent-Type: application/octet-stream\r\n\r\nAAA\
              \r\n--b\r\nContent-Type: image/jpeg; transfer-syntax=1.2\r\n\r\nBB\
              \r\n--b--\r\n"
        );
    }

    #[test]
    fn random_boundary_is_32_hex() {
        let m = Multipart::new();
        let ct = m.content_type("x");
        let b = ct.rsplit("boundary=").next().unwrap();
        assert_eq!(b.len(), 32);
        assert!(b.bytes().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn splice_inserts_prefix_only_after_keys() {
        let json = br#"{"7FE00010":{"vr":"OW","BulkDataURI":"bulk/7FE00010"},"x":"BulkDataURI","y":{"BulkDataURI":"bulk/a/0/b"}}"#;
        let mut out = Vec::new();
        splice_bulk_uris(json, "http://h/i/", &mut out);
        assert_eq!(
            String::from_utf8(out).unwrap(),
            r#"{"7FE00010":{"vr":"OW","BulkDataURI":"http://h/i/bulk/7FE00010"},"x":"BulkDataURI","y":{"BulkDataURI":"http://h/i/bulk/a/0/b"}}"#
        );
    }

    #[test]
    fn splice_escapes_prefix() {
        let json = br#"{"a":{"BulkDataURI":"bulk/x"}}"#;
        let prefix = "http://x\"y\\z\n/";
        let mut out = Vec::new();
        splice_bulk_uris(json, prefix, &mut out);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["a"]["BulkDataURI"], format!("{prefix}bulk/x"));
        assert_eq!(v.as_object().unwrap().len(), 1);
    }
}
