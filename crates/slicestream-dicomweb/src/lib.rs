//! DICOMweb request models and parsing. Pure logic: no HTTP framework, no database.
#![forbid(unsafe_code)]

pub mod qido;

pub use qido::{Include, Key, Level, Matcher, QidoQuery, QueryError, default_fields, parse_qido};
