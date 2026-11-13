//! Content detection, NIfTI header parsing and the archive walker for
//! archive ingest.

mod count;
mod detect;
mod nifti;

// The UnRAR wrapper declares its C structs for Unix (UTF-32 wchar_t,
// 64-bit layout asserts).
#[cfg(all(feature = "rar", windows))]
compile_error!("the rar feature supports Unix only");
#[cfg(feature = "rar")]
mod rar;
mod sevenz;
mod walk;

pub use detect::{Kind, SNIFF_LEN, detect};
pub use nifti::{NiftiError, NiftiHeader, parse_nifti_header};
pub use walk::{Event, Leaf, Limits, Sink, WalkError, WalkStats, walk_file};
