//! # slicestream-core
//!
//! Transport-agnostic building blocks for streaming DICOM slices to a browser
//! in the order a radiologist is about to look at them.
//!
//! * [`series`] — index a directory of DICOM instances into an ordered stack of
//!   slices without loading pixel data.
//! * [`frame`] — extract the raw (possibly still-compressed) bytes of one frame.
//! * [`scheduler`] — decide which slice to send next given the viewer's cursor
//!   and scroll velocity.
//! * [`staged`] — preview-then-final ordering on top of the scheduler.
//! * [`flow`] — ack-driven send window.
//! * [`protocol`] — the small binary/JSON wire format spoken over WebSocket.

pub mod error;
pub mod flow;
pub mod frame;
pub mod protocol;
pub mod scheduler;
pub mod series;
pub mod staged;

/// Header attribute helpers shared with the ingest pipeline.
pub mod attrs {
    pub use crate::frame::{get_f64s, get_i64, get_str};
}

pub use error::{Error, Result};
pub use flow::FlowWindow;
pub use frame::{Frame, FrameInfo, FrameLocation};
pub use scheduler::{PrefetchScheduler, SchedulerConfig};
pub use series::{Series, SliceRef};
pub use staged::{Stage, StagedScheduler};
