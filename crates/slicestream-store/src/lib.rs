//! Durable spooling, content-addressed blob storage and the SQLite index.
#![forbid(unsafe_code)]

mod blob;
mod header;
mod index;
mod part10;

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

pub use blob::{BlobHash, FsBlobStore, SpoolError, SpooledBlob, TempBlob};
pub use header::{HeaderError, InstanceHeader, read_header};
pub use index::{
    DuplicatePolicy, Index, InstanceRecord, InstanceSummary, SeriesRow, SliceRow, UpsertOutcome,
};
pub use part10::{IMPLEMENTATION_CLASS_UID, IMPLEMENTATION_VERSION_NAME, write_part10_prefix};
