//! WADO-RS frames (PS3.18 10.4): chosen frames of one instance, one part per
//! frame in request order, with no transcoding (big-endian pixels may be
//! byte-swapped). Frames in the instance's frame table are read with
//! positioned reads. Instances that are not scanned yet are parsed whole
//! instead. Each frame is read when its part is sent, so a request holds one
//! frame in memory at a time.

use std::path::PathBuf;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use futures_util::{Stream, stream};
use slicestream_core::frame::{read_frame_at, read_frame_bytes, swap_to_little_endian};
use slicestream_core::{Error as FrameError, FrameInfo};
use slicestream_dicomweb::{FrameSource, Multipart, negotiate_frames};
use slicestream_store::{BlobKind, FrameTable, InstanceRef, StoredData};

use super::{accept_ranges, find_instance_data, plain};
use crate::AppState;

/// Most frame numbers one request may list.
const MAX_LISTED: usize = 4096;

pub(crate) const EXPLICIT_LE: &str = "1.2.840.10008.1.2.1";
pub(crate) const EXPLICIT_BE: &str = "1.2.840.10008.1.2.2";
const IMPLICIT_LE: &str = "1.2.840.10008.1.2";

pub async fn frames(
    State(state): State<Arc<AppState>>,
    Path((study, series, sop, list)): Path<(String, String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let numbers = match parse_list(&list) {
        Some(n) => n,
        None => {
            return plain(
                StatusCode::BAD_REQUEST,
                format!(
                    "the frame list must be 1 to {MAX_LISTED} comma-separated decimal frame numbers"
                ),
            );
        }
    };
    let (inst, data) = match find_instance_data(&state, study, series, sop).await {
        Ok(found) => found,
        Err(r) => return *r,
    };
    respond(&state, &inst, data, Wanted::List(numbers), &headers).await
}

/// `{list}`: comma-separated decimal numbers (digits only), at most
/// [`MAX_LISTED`]. `None` when malformed. Range is checked later.
fn parse_list(list: &str) -> Option<Vec<u32>> {
    let mut out = Vec::new();
    for item in list.split(',') {
        if out.len() == MAX_LISTED || item.is_empty() || !item.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        out.push(item.parse().ok()?);
    }
    Some(out)
}

/// The frames a response holds.
pub(super) enum Wanted {
    /// 1-based frame numbers, in response order.
    List(Vec<u32>),
    /// Every frame, in order (Pixel Data bulk data).
    All,
}

/// Where the frames of an instance are read from.
enum Source {
    /// Positioned reads at the offsets the scan recorded.
    Table(FrameTable),
    /// A whole-file parse per frame.
    Parse,
}

/// Serve the `wanted` frames of `inst`, negotiated against `headers`.
/// `data` is what the scan stored for `inst.blob` (`None`: not scanned).
pub(super) async fn respond(
    state: &Arc<AppState>,
    inst: &InstanceRef,
    data: Option<StoredData>,
    wanted: Wanted,
    headers: &HeaderMap,
) -> Response {
    let Some(info) = &inst.frame_info else {
        return plain(StatusCode::NOT_FOUND, "the instance is not an image".into());
    };
    let total = inst.number_of_frames;
    let (numbers, count): (Box<dyn Iterator<Item = u32> + Send>, usize) = match wanted {
        Wanted::List(list) => {
            if let Some(n) = list.iter().find(|&&n| n == 0 || n > total) {
                return plain(
                    StatusCode::NOT_FOUND,
                    format!("there is no frame {n}: the instance has {total} frames"),
                );
            }
            let count = list.len();
            (Box::new(list.into_iter()), count)
        }
        Wanted::All => (Box::new(1..=total), total as usize),
    };
    let source = match frame_source(data) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    let (transfer_syntax, encapsulated) = served_syntax(&inst.transfer_syntax);
    let src = FrameSource {
        transfer_syntax,
        encapsulated,
        bits_allocated: info.bits_allocated,
    };
    let plan = match negotiate_frames(&accept_ranges(headers), &src, count) {
        Ok(p) => p,
        Err(e) => return plain(StatusCode::NOT_ACCEPTABLE, e.0),
    };
    let reader = Arc::new(FrameReader {
        path: state.ingestor.blobs().path_of(&inst.blob, BlobKind::Dicom),
        source,
        info: info.clone(),
        big_endian: inst.transfer_syntax == EXPLICIT_BE,
        swap: plan.byte_swap,
    });

    // Read the first frame now, so a failure is a status code rather than
    // a broken body.
    let mut numbers = numbers.peekable();
    let Some(&first_number) = numbers.peek() else {
        return plain(StatusCode::NOT_FOUND, "the instance has no frames".into());
    };
    let first = match reader.read(first_number).await {
        Ok(f) => f,
        Err(e) => return read_failed(&reader, &e),
    };

    if plan.single_part {
        let len = first.len();
        let mut res = Response::new(Body::from(first));
        if let Ok(v) = HeaderValue::from_str(&plan.part_content_type) {
            res.headers_mut().insert(header::CONTENT_TYPE, v);
        }
        res.headers_mut().insert(header::CONTENT_LENGTH, len.into());
        return res;
    }

    let multipart = Multipart::new();
    let content_type = multipart.content_type(essence(&plan.part_content_type));
    let body = Body::from_stream(part_stream(Parts {
        reader,
        multipart,
        part_type: plan.part_content_type,
        numbers: Box::new(numbers),
        first: Some(first),
        body: None,
    }));
    let mut res = Response::new(body);
    if let Ok(v) = HeaderValue::from_str(&content_type) {
        res.headers_mut().insert(header::CONTENT_TYPE, v);
    }
    res
}

/// The frame table when the scan recorded one; a whole-file parse when it
/// recorded no file offsets or has not run yet.
fn frame_source(data: Option<StoredData>) -> Result<Source, Box<Response>> {
    match data {
        Some(data) if data.offsets_valid => match (data.frames, data.problem) {
            (Some(table), _) => Ok(Source::Table(table)),
            (None, Some(problem)) => {
                Err(Box::new(plain(StatusCode::INTERNAL_SERVER_ERROR, problem)))
            }
            (None, None) => Err(Box::new(plain(
                StatusCode::NOT_FOUND,
                "no pixel data".into(),
            ))),
        },
        _ => Ok(Source::Parse),
    }
}

/// The transfer syntax frames are served in, and whether they are
/// encapsulated.
fn served_syntax(stored: &str) -> (&str, bool) {
    match stored {
        IMPLICIT_LE | EXPLICIT_LE | EXPLICIT_BE => (stored, false),
        _ => (stored, true),
    }
}

/// `type/subtype` of a media type with parameters.
fn essence(media_type: &str) -> &str {
    media_type.split(';').next().unwrap_or(media_type).trim()
}

/// The response for a frame that could not be read before the body started.
fn read_failed(reader: &FrameReader, e: &FrameError) -> Response {
    match e {
        FrameError::MissingAttribute(_) => plain(StatusCode::NOT_FOUND, "no pixel data".into()),
        FrameError::Unsupported(reason) => plain(StatusCode::INTERNAL_SERVER_ERROR, reason.clone()),
        _ => {
            // I/O and parse errors name the blob path: log them, do not send them.
            tracing::error!(path = %reader.path.display(), "frame unreadable: {e}");
            plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                "the stored file could not be read".into(),
            )
        }
    }
}

/// The error for a frame read whose task failed (panicked). The panic
/// message may name paths: it is logged, and the response says only that
/// the read failed.
fn join_failed(e: tokio::task::JoinError) -> FrameError {
    tracing::error!("frame read task failed: {e}");
    FrameError::Unsupported("frame read failed".into())
}

/// Reads the frames of one instance in the byte order the plan asks for.
struct FrameReader {
    path: PathBuf,
    source: Source,
    info: FrameInfo,
    /// Stored as Explicit VR Big Endian.
    big_endian: bool,
    /// Swap each word to little-endian (from the plan).
    swap: bool,
}

impl FrameReader {
    /// Frame `number` (1-based), on the blocking pool.
    async fn read(self: &Arc<Self>, number: u32) -> Result<Bytes, FrameError> {
        let me = Arc::clone(self);
        tokio::task::spawn_blocking(move || me.read_blocking(number))
            .await
            .unwrap_or_else(|e| Err(join_failed(e)))
    }

    fn read_blocking(&self, number: u32) -> Result<Bytes, FrameError> {
        let frame = number - 1;
        let (data, parsed) = match &self.source {
            Source::Table(table) => {
                let loc = table.location(frame).ok_or_else(|| {
                    FrameError::Unsupported(format!("frame {number} is not in the frame table"))
                })?;
                (read_frame_at(&self.path, &loc)?, false)
            }
            Source::Parse => (read_frame_bytes(&self.path, frame, &self.info)?, true),
        };
        // The parse decodes big-endian OW pixel data into native-endian
        // words. Put them back in stored order, which the plan starts from.
        let restore = parsed
            && self.big_endian
            && self.info.bits_allocated > 8
            && cfg!(target_endian = "little");
        if !restore && !self.swap {
            return Ok(data);
        }
        let mut buf = Vec::from(data);
        if restore {
            swap_to_little_endian(&mut buf, 16);
        }
        if self.swap {
            swap_to_little_endian(&mut buf, self.info.bits_allocated);
        }
        Ok(Bytes::from(buf))
    }
}

struct Parts {
    reader: Arc<FrameReader>,
    multipart: Multipart,
    part_type: String,
    numbers: Box<dyn Iterator<Item = u32> + Send>,
    /// The first frame, read before the response started.
    first: Option<Bytes>,
    /// A frame whose part header was just sent.
    body: Option<Bytes>,
}

/// For each frame: read it, then send its part header and the frame. A
/// frame that cannot be read ends the stream with an error (the client sees
/// a broken body), never with a clean close.
fn part_stream(parts: Parts) -> impl Stream<Item = std::io::Result<Bytes>> {
    stream::unfold(Some(parts), |state| async move {
        let mut p = state?;
        if let Some(body) = p.body.take() {
            return Some((Ok(body), Some(p)));
        }
        let Some(number) = p.numbers.next() else {
            return Some((Ok(p.multipart.close()), None));
        };
        let frame = match p.first.take() {
            Some(f) => Ok(f),
            None => p.reader.read(number).await,
        };
        match frame {
            Ok(f) => {
                p.body = Some(f);
                let head = p.multipart.part_header(&p.part_type);
                Some((Ok(head), Some(p)))
            }
            Err(e) => {
                tracing::error!(
                    path = %p.reader.path.display(),
                    frame = number,
                    "frame unreadable mid-stream: {e}"
                );
                Some((Err(std::io::Error::other(e)), None))
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_lists() {
        assert_eq!(parse_list("1"), Some(vec![1]));
        assert_eq!(parse_list("3,1,3"), Some(vec![3, 1, 3]));
        assert_eq!(parse_list("007"), Some(vec![7]));
        assert_eq!(parse_list("4294967295"), Some(vec![u32::MAX]));
        for bad in [
            "",
            ",",
            "1,,2",
            "1,",
            ",1",
            "abc",
            "-1",
            "+1",
            " 1",
            "1 ",
            "4294967296",
        ] {
            assert_eq!(parse_list(bad), None, "{bad:?}");
        }
        let max = vec!["1"; MAX_LISTED].join(",");
        assert_eq!(parse_list(&max).map(|v| v.len()), Some(MAX_LISTED));
        let over = vec!["1"; MAX_LISTED + 1].join(",");
        assert_eq!(parse_list(&over), None);
    }

    /// A panicking read answers 500 with a fixed text; the panic message
    /// (which may name paths) is only logged.
    #[tokio::test]
    async fn failed_read_task_text_is_generic() {
        let e = tokio::task::spawn_blocking(|| panic!("secret /srv/objects/ab/cd"))
            .await
            .unwrap_err();
        let reader = FrameReader {
            path: PathBuf::from("/srv/objects/ab/cd"),
            source: Source::Parse,
            info: FrameInfo {
                rows: 1,
                columns: 1,
                bits_allocated: 8,
                bits_stored: 8,
                samples_per_pixel: 1,
                pixel_representation: 0,
                photometric_interpretation: "MONOCHROME2".into(),
                transfer_syntax: EXPLICIT_LE.into(),
                rescale_slope: 1.0,
                rescale_intercept: 0.0,
                window_center: None,
                window_width: None,
            },
            big_endian: false,
            swap: false,
        };
        let res = read_failed(&reader, &join_failed(e));
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(res.into_body(), 1024).await.unwrap();
        assert_eq!(String::from_utf8_lossy(&body), "frame read failed");
    }

    #[test]
    fn served_syntaxes() {
        assert_eq!(served_syntax(IMPLICIT_LE), (IMPLICIT_LE, false));
        assert_eq!(served_syntax(EXPLICIT_BE), (EXPLICIT_BE, false));
        assert_eq!(
            served_syntax("1.2.840.10008.1.2.4.50"),
            ("1.2.840.10008.1.2.4.50", true)
        );
        assert_eq!(essence("image/jpeg; transfer-syntax=1"), "image/jpeg");
        assert_eq!(
            essence("application/octet-stream"),
            "application/octet-stream"
        );
    }
}
