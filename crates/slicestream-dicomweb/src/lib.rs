//! DICOMweb request models and parsing. Pure logic: no HTTP framework, no database.
#![forbid(unsafe_code)]

pub mod accept;
pub mod multipart;
pub mod qido;
pub mod urls;

pub use accept::{
    FrameSource, InstancePlan, MediaRange, NotAcceptable, PartPlan, instance_part_type,
    media_type_for_ts, negotiate_frames, negotiate_instances, negotiate_json, parse_accept,
};
pub use multipart::{Multipart, splice_bulk_uris};
pub use qido::{Include, Key, Level, Matcher, QidoQuery, QueryError, default_fields, parse_qido};
pub use urls::{base_url, instance_url, series_url, study_url};
