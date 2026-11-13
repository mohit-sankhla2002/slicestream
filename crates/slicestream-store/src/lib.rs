//! Durable spooling, content-addressed blob storage and the SQLite index.
#![forbid(unsafe_code)]

mod blob;
mod config;
mod header;
mod index;
mod ingest;
mod part10;
mod stats;

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

pub use blob::{BlobHash, FsBlobStore, SpoolError, SpooledBlob, TempBlob};
pub use config::StorageConfig;
pub use header::{HeaderError, InstanceHeader, read_header};
pub use index::{
    DuplicatePolicy, Index, InstanceRecord, InstanceSummary, SeriesRow, SliceRow, UpsertOutcome,
};
pub use ingest::{
    Expectations, IngestError, IngestFailure, IngestSource, Ingestor, RecoveryReport, Stored,
    StoredEvent, is_storage_sop_class,
};
pub use part10::{IMPLEMENTATION_CLASS_UID, IMPLEMENTATION_VERSION_NAME, write_part10_prefix};
pub use stats::IngestStats;
