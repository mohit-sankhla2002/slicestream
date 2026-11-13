//! Content detection and NIfTI header parsing for archive ingest.

mod detect;
mod nifti;

pub use detect::{Kind, SNIFF_LEN, detect};
pub use nifti::{NiftiError, NiftiHeader, parse_nifti_header};
