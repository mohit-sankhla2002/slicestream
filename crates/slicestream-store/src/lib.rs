//! Durable spooling, content-addressed blob storage and the SQLite index.
#![forbid(unsafe_code)]

mod blob;
mod config;
mod frames;
mod header;
mod index;
mod ingest;
mod json;
mod part10;
mod scan;
mod stats;

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

pub use blob::{BlobHash, BlobKind, FsBlobStore, SpoolError, SpooledBlob, TempBlob};
pub use config::StorageConfig;
pub use frames::FrameTable;
pub use header::{HeaderError, InstanceHeader, read_header};
pub use index::{
    CONSOLE_SERIES_LIMIT, ConsoleSeriesRow, DuplicatePolicy, Index, InstanceRecord, InstanceRef,
    InstanceSummary, MetadataState, NiftiMeta, NiftiRow, Scope, SeriesRow, SliceRow, StorageCounts,
    StoredData, StudyInfo, UploadCounts, UploadRow, UpsertOutcome, fold_name,
};
pub use ingest::{
    Expectations, IngestError, IngestFailure, IngestSource, Ingestor, RecoveryReport, Stored,
    StoredEvent, StoredNifti, is_storage_sop_class,
};
pub use part10::{IMPLEMENTATION_CLASS_UID, IMPLEMENTATION_VERSION_NAME, write_part10_prefix};
pub use scan::{
    BULK_BINARY_MIN, BulkValue, InstanceData, InstanceScan, MAX_METADATA_BYTES, SCAN_VERSION,
    ScanLimits, scan_instance, scan_instance_with,
};
pub use stats::IngestStats;
