//! Durable spooling and content-addressed blob storage.
#![forbid(unsafe_code)]

mod blob;

pub use blob::{BlobHash, FsBlobStore, SpoolError, SpooledBlob, TempBlob};
