//! WADO-RS frames (PS3.18 10.4): chosen frames of one instance, one part per
//! frame in request order, as stored (big-endian pixels may be
//! byte-swapped) or, when negotiated, as their HTJ2K copy. Frames in the
//! instance's frame table are read with positioned reads. Instances that are
//! not scanned yet are parsed whole instead. HTJ2K frames come from the copy
//! file, or are encoded now when the copy is not made yet. Each frame is
//! read when its part is sent, so a request holds one frame in memory at a
//! time. A single HTJ2K frame honours one HTTP `Range`.

use std::path::PathBuf;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use futures_util::{Stream, stream};
use slicestream_codec::eligibility;
use slicestream_core::frame::{
    FrameLocation, read_frame_at, read_frame_bytes, swap_to_little_endian,
};
use slicestream_core::{Error as FrameError, FrameInfo};
use slicestream_dicomweb::{FrameSource, Multipart, negotiate_frames, parse_range, resolve};
use slicestream_store::{
    BlobHash, BlobKind, CopyFrames, FrameTable, IngestError, Ingestor, InstanceRef, StoredData,
};

use super::{accept_ranges, find_instance_data, plain};
use crate::AppState;
use crate::config::Htj2kMode;

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
    /// Positioned reads of the HTJ2K copy file.
    Copy(CopyFrames),
    /// HTJ2K encoded now, from the instance's current blob, which must
    /// stay `blob`.
    Transient {
        ingestor: Arc<Ingestor>,
        id: i64,
        blob: BlobHash,
    },
}

/// Why a frame could not be read.
#[derive(Debug)]
enum ReadError {
    Frame(FrameError),
    /// The HTJ2K encode failed.
    Encode(IngestError),
    /// The instance's blob changed during the encode.
    Replaced,
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::Frame(e) => e.fmt(f),
            ReadError::Encode(e) => write!(f, "HTJ2K encode failed: {e}"),
            ReadError::Replaced => f.write_str("the instance was replaced during the encode"),
        }
    }
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
    let listed = matches!(wanted, Wanted::List(_));
    // A copy needs a frame table: none when the scan recorded no offsets.
    let table_possible = data.as_ref().is_none_or(|d| d.offsets_valid);
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
        // Copies are for the frames resource, not Pixel Data bulk data.
        copy_possible: listed
            && table_possible
            && state.htj2k.mode != Htj2kMode::Off
            && eligibility(info).is_ok(),
        prefer_copy_for_wildcard: state.htj2k.prefer_for_wildcard,
    };
    let plan = match negotiate_frames(&accept_ranges(headers), &src, count) {
        Ok(p) => p,
        Err(e) => return plain(StatusCode::NOT_ACCEPTABLE, e.0),
    };
    let dicom_path = state.ingestor.blobs().path_of(&inst.blob, BlobKind::Dicom);
    let reader = if plan.use_copy {
        // The copy made from the blob being served, never another one.
        let (path, source) = match state.ingestor.copy_frames(inst.id, &inst.blob).await {
            Ok(Some((frames, path))) => (path, Source::Copy(frames)),
            Ok(None) => (
                dicom_path,
                Source::Transient {
                    ingestor: Arc::clone(&state.ingestor),
                    id: inst.id,
                    blob: inst.blob,
                },
            ),
            Err(e) => return encode_failed(e),
        };
        FrameReader {
            path,
            source,
            info: info.clone(),
            big_endian: false,
            swap: false,
        }
    } else {
        FrameReader {
            path: dicom_path,
            source,
            info: info.clone(),
            big_endian: inst.transfer_syntax == EXPLICIT_BE,
            swap: plan.byte_swap,
        }
    };
    let reader = Arc::new(reader);

    // Read the first frame now, so a failure is a status code rather than
    // a broken body.
    let mut numbers = numbers.peekable();
    let Some(&first_number) = numbers.peek() else {
        return plain(StatusCode::NOT_FOUND, "the instance has no frames".into());
    };
    let mut first = match reader.read(first_number).await {
        Ok(f) => f,
        Err(ReadError::Frame(e)) => return read_failed(&reader, &e),
        Err(ReadError::Encode(e)) => return encode_failed(e),
        Err(ReadError::Replaced) => return replaced(),
    };

    // `Range` applies to a single HTJ2K frame only; elsewhere it is ignored.
    let mut content_range = None;
    if plan.use_copy && count == 1 {
        let frame_len = first.len() as u64;
        match single_range(headers).map(|r| resolve(&r, frame_len)) {
            Some(Some((start, end))) => {
                first = first.slice(start as usize..end as usize);
                content_range = Some(format!("bytes {start}-{}/{frame_len}", end - 1));
            }
            Some(None) => return unsatisfiable(frame_len),
            None => {}
        }
    }

    if plan.single_part {
        let len = first.len();
        let mut res = Response::new(Body::from(first));
        if let Ok(v) = HeaderValue::from_str(&plan.part_content_type) {
            res.headers_mut().insert(header::CONTENT_TYPE, v);
        }
        res.headers_mut().insert(header::CONTENT_LENGTH, len.into());
        return with_range_headers(res, plan.use_copy, content_range);
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
    with_range_headers(res, plan.use_copy, content_range)
}

/// The one byte range of the request's `Range`; `None` when there is no
/// `Range`, more than one, or it is malformed.
fn single_range(headers: &HeaderMap) -> Option<slicestream_dicomweb::ByteRange> {
    let mut values = headers.get_all(header::RANGE).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    match parse_range(value.to_str().ok()?)?.as_slice() {
        [r] => Some(*r),
        _ => None,
    }
}

/// `Accept-Ranges: bytes` on an HTJ2K answer, and 206 with
/// `Content-Range` when a range was served.
fn with_range_headers(
    mut res: Response,
    use_copy: bool,
    content_range: Option<String>,
) -> Response {
    if use_copy {
        res.headers_mut()
            .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    }
    if let Some(v) = content_range.and_then(|v| HeaderValue::from_str(&v).ok()) {
        *res.status_mut() = StatusCode::PARTIAL_CONTENT;
        res.headers_mut().insert(header::CONTENT_RANGE, v);
    }
    res
}

/// 416 for a range outside a frame of `len` bytes.
fn unsatisfiable(len: u64) -> Response {
    let mut res = plain(
        StatusCode::RANGE_NOT_SATISFIABLE,
        format!("the range is outside the frame's {len} bytes"),
    );
    if let Ok(v) = HeaderValue::from_str(&format!("bytes */{len}")) {
        res.headers_mut().insert(header::CONTENT_RANGE, v);
    }
    res.headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    res
}

/// The response for an HTJ2K copy that could not be looked up or encoded
/// before the body started.
fn encode_failed(e: IngestError) -> Response {
    match e {
        IngestError::NotFound(_) => plain(
            StatusCode::NOT_FOUND,
            "the instance no longer exists".into(),
        ),
        e => {
            // Messages may name paths: log them, do not send them.
            tracing::error!("HTJ2K frame unavailable: {e}");
            plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                "the frame could not be served as HTJ2K".into(),
            )
        }
    }
}

/// The response when the instance changed while a frame was encoded.
fn replaced() -> Response {
    plain(
        StatusCode::SERVICE_UNAVAILABLE,
        "the instance was replaced while it was being read; try again".into(),
    )
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
    /// Frame `number` (1-based): encoded now, or read on the blocking pool.
    async fn read(self: &Arc<Self>, number: u32) -> Result<Bytes, ReadError> {
        if let Source::Transient { ingestor, id, blob } = &self.source {
            return encode_now(ingestor, *id, blob, number).await;
        }
        let me = Arc::clone(self);
        tokio::task::spawn_blocking(move || me.read_blocking(number))
            .await
            .unwrap_or_else(|e| Err(join_failed(e)))
            .map_err(ReadError::Frame)
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
            Source::Copy(frames) => {
                let f = frames.0.get(frame as usize).ok_or_else(|| {
                    FrameError::Unsupported(format!("frame {number} is not in the HTJ2K copy"))
                })?;
                let loc = FrameLocation::Bytes {
                    offset: f.offset,
                    len: f.len,
                };
                (read_frame_at(&self.path, &loc)?, false)
            }
            Source::Transient { .. } => {
                return Err(FrameError::Unsupported(
                    "an encoded frame is not read from a file".into(),
                ));
            }
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

/// Frame `number` of instance `id` encoded as HTJ2K now, when the
/// instance still holds `blob` afterwards.
async fn encode_now(
    ingestor: &Ingestor,
    id: i64,
    blob: &BlobHash,
    number: u32,
) -> Result<Bytes, ReadError> {
    let mut encoded = ingestor
        .encode_transient(id, &[number])
        .await
        .map_err(ReadError::Encode)?;
    match ingestor.index().instance_blob(id).await {
        Ok(Some((_, current))) if current == *blob => {}
        Ok(_) => return Err(ReadError::Replaced),
        Err(e) => return Err(ReadError::Encode(IngestError::Index(e))),
    }
    match encoded.pop() {
        Some(e) if encoded.is_empty() => Ok(Bytes::from(e.codestream)),
        _ => Err(ReadError::Frame(FrameError::Unsupported(
            "the encoder returned the wrong number of frames".into(),
        ))),
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
                Some((Err(std::io::Error::other(e.to_string())), None))
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
