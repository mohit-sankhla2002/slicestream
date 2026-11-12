use std::path::PathBuf;

/// Errors produced by `slicestream-core`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error on {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to read DICOM file {path}: {message}")]
    Dicom { path: PathBuf, message: String },

    #[error("missing required attribute {0}")]
    MissingAttribute(&'static str),

    #[error("slice index {index} out of range (series has {len} slices)")]
    SliceOutOfRange { index: usize, len: usize },

    #[error("unsupported pixel data layout: {0}")]
    Unsupported(String),

    #[error("series contains no readable DICOM images")]
    EmptySeries,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
