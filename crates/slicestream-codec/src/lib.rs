//! Lossless progressive HTJ2K copies of stored grayscale frames.
//!
//! A copy is a single-tile, single-tile-part HTJ2K codestream in RPCL order
//! with a TLM marker, so each resolution is a contiguous prefix that can be
//! streamed on its own. [`encode_verified`] only returns a copy after an exact
//! sample-by-sample round trip.
#![forbid(unsafe_code)]

mod conformance;
mod encode;
mod levels;
mod samples;

pub use conformance::check_conformance;
pub use encode::{Encoded, decode_full, encode, encode_verified, encode_verified_with};
pub use levels::{decomposition_levels, preview_level};
pub use samples::{MAX_PIXELS, NotEligible, eligibility, samples};

/// Transfer syntax of the copies: HTJ2K Lossless, RPCL progression.
pub const HTJ2K_RPCL: &str = "1.2.840.10008.1.2.4.202";
/// Bumped whenever the encoder settings change, so old copies can be rebuilt.
pub const CODEC_VERSION: i64 = 1;

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("not eligible: {0}")]
    NotEligible(String),
    #[error("bad frame: {0}")]
    BadFrame(String),
    #[error("encode failed: {0}")]
    Encode(String),
    #[error("round trip mismatch at sample {at}")]
    Mismatch { at: usize },
}
