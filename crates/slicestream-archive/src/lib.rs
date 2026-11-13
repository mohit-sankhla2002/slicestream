//! Content detection, NIfTI header parsing and the archive walker for
//! archive ingest.

mod count;
mod detect;
mod nifti;
#[cfg(feature = "rar")]
mod rar;
mod sevenz;
mod walk;

pub use detect::{Kind, SNIFF_LEN, detect};
pub use nifti::{NiftiError, NiftiHeader, parse_nifti_header};
pub use walk::{Event, Leaf, Limits, Sink, WalkError, WalkStats, walk_file};
