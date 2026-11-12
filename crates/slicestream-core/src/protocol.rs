//! Wire protocol between browser and server.
//!
//! * **Client -> server**: JSON text messages ([`ClientMessage`]).
//! * **Server -> client**: one JSON text message with the series summary, then
//!   one binary message per frame:
//!
//! ```text
//! +--------+----------------+----------------------+-------------------+
//! | "SLS1" | header_len u32 | header (JSON, UTF-8) | payload bytes ... |
//! | 4 B    | little-endian  | header_len bytes     | rest of message   |
//! +--------+----------------+----------------------+-------------------+
//! ```
//!
//! The JSON header keeps the format self-describing and easy to debug in
//! browser devtools; the payload is never base64-encoded.

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::{Frame, FrameInfo};

pub const MAGIC: &[u8; 4] = b"SLS1";

/// Messages the browser sends.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ClientMessage {
    /// The viewer moved. `velocity` is in slices per second (signed).
    Cursor { index: usize, velocity: f64 },
    /// The client dropped these slices from its cache; resend if needed.
    Evicted { indices: Vec<usize> },
}

/// Per-frame header placed in front of each binary payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FrameHeader {
    pub index: usize,
    pub info: FrameInfo,
}

/// Encode a frame into a single binary WebSocket message.
pub fn encode_frame(frame: &Frame) -> Bytes {
    let header = serde_json::to_vec(&FrameHeader {
        index: frame.index,
        info: frame.info.clone(),
    })
    .expect("FrameHeader serialization cannot fail");
    let mut buf = BytesMut::with_capacity(8 + header.len() + frame.data.len());
    buf.put_slice(MAGIC);
    buf.put_u32_le(header.len() as u32);
    buf.put_slice(&header);
    buf.put_slice(&frame.data);
    buf.freeze()
}

/// Decode a binary message produced by [`encode_frame`]. Mainly for tests and
/// Rust clients; the browser does the same in a few lines of JS.
pub fn decode_frame(msg: &[u8]) -> Option<(FrameHeader, &[u8])> {
    if msg.len() < 8 || &msg[..4] != MAGIC {
        return None;
    }
    let len = u32::from_le_bytes(msg[4..8].try_into().ok()?) as usize;
    let header_end = 8usize.checked_add(len)?;
    let header = serde_json::from_slice(msg.get(8..header_end)?).ok()?;
    Some((header, &msg[header_end..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info() -> FrameInfo {
        FrameInfo {
            rows: 2,
            columns: 2,
            bits_allocated: 16,
            bits_stored: 12,
            samples_per_pixel: 1,
            pixel_representation: 0,
            photometric_interpretation: "MONOCHROME2".into(),
            transfer_syntax: "1.2.840.10008.1.2.1".into(),
            rescale_slope: 1.0,
            rescale_intercept: -1024.0,
            window_center: Some(40.0),
            window_width: Some(400.0),
        }
    }

    #[test]
    fn frame_roundtrip() {
        let frame = Frame {
            index: 7,
            info: info(),
            data: Bytes::from_static(&[1, 2, 3, 4, 5, 6, 7, 8]),
        };
        let msg = encode_frame(&frame);
        let (header, payload) = decode_frame(&msg).unwrap();
        assert_eq!(header.index, 7);
        assert_eq!(header.info, info());
        assert_eq!(payload, &[1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn rejects_garbage() {
        assert!(decode_frame(b"nope").is_none());
        assert!(decode_frame(b"SLS1\xff\xff\xff\xff{}").is_none());
    }

    #[test]
    fn client_message_json() {
        let m: ClientMessage =
            serde_json::from_str(r#"{"type":"cursor","index":3,"velocity":-12.5}"#).unwrap();
        assert_eq!(
            m,
            ClientMessage::Cursor {
                index: 3,
                velocity: -12.5
            }
        );
    }
}
