//! WADO-RS bulk data (PS3.18 10.4): the value a `BulkDataURI` points to.
//! Pixel Data is served as its frames, one part per frame. Other values are
//! read at the file offsets the scan recorded, in 1 MiB reads. Without
//! recorded offsets (not scanned yet) the file is parsed and the element at
//! the path is taken.

use std::os::unix::fs::FileExt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use dicom_core::value::Value;
use dicom_core::{Tag, VR};
use dicom_object::{InMemDicomObject, OpenFileOptions};
use futures_util::stream::{self, BoxStream};
use futures_util::{Stream, StreamExt};
use slicestream_dicomweb::{FrameSource, Multipart, negotiate_frames};
use slicestream_store::BlobKind;

use super::frames::{self, EXPLICIT_BE, EXPLICIT_LE, Wanted};
use super::{accept_ranges, find_instance_data, plain};
use crate::AppState;

/// Path of the top-level Pixel Data element.
const PIXEL_DATA: &str = "7FE00010";

const READ_CHUNK: u64 = 1 << 20;

pub async fn bulk(
    State(state): State<Arc<AppState>>,
    Path((study, series, sop, path)): Path<(String, String, String, String)>,
    headers: HeaderMap,
) -> Response {
    // `data` belongs to `inst.blob`, so its offsets fit the file read below.
    let (inst, data) = match find_instance_data(&state, study, series, sop).await {
        Ok(found) => found,
        Err(r) => return *r,
    };
    if path == PIXEL_DATA {
        return frames::respond(&state, &inst, data, Wanted::All, &headers).await;
    }
    let file = state.ingestor.blobs().path_of(&inst.blob, BlobKind::Dicom);
    let big_endian = inst.transfer_syntax == EXPLICIT_BE;
    let value = match data {
        Some(data) if data.offsets_valid => match data.bulk.into_iter().find(|b| b.path == path) {
            Some(b) => match open_range(file, b.offset, b.len).await {
                Ok(v) => v,
                Err(r) => return *r,
            },
            None => return no_value(),
        },
        // Not scanned yet, or scanned without file offsets: no bulk list.
        _ => {
            let Some((items, tag)) = parse_path(&path) else {
                return no_value();
            };
            let parse = file.clone();
            let read =
                tokio::task::spawn_blocking(move || parsed_value(&parse, &items, tag, big_endian))
                    .await;
            match read {
                Ok(Ok(Some(bytes))) => BulkValue::Parsed(Bytes::from(bytes)),
                Ok(Ok(None)) => return no_value(),
                Ok(Err(e)) => {
                    tracing::error!(path = %file.display(), "bulk value unreadable: {e}");
                    return unreadable();
                }
                Err(e) => {
                    tracing::error!("bulk value read failed: {e}");
                    return unreadable();
                }
            }
        }
    };

    // Values other than pixel data are sent as stored: little-endian, or
    // big-endian for an Explicit VR Big Endian instance.
    let stored = if big_endian { EXPLICIT_BE } else { EXPLICIT_LE };
    let src = FrameSource {
        transfer_syntax: stored,
        encapsulated: false,
        bits_allocated: 8,
        copy_possible: false,
        prefer_copy_for_wildcard: false,
    };
    let plan = match negotiate_frames(&accept_ranges(&headers), &src, 1) {
        Ok(p) => p,
        Err(e) => return plain(StatusCode::NOT_ACCEPTABLE, e.0),
    };
    let part_type = format!("application/octet-stream; transfer-syntax={stored}");
    let len = value.len();
    let body = value.into_stream();

    if plan.single_part {
        let mut res = Response::new(Body::from_stream(body));
        if let Ok(v) = HeaderValue::from_str(&part_type) {
            res.headers_mut().insert(header::CONTENT_TYPE, v);
        }
        res.headers_mut().insert(header::CONTENT_LENGTH, len.into());
        return res;
    }
    let multipart = Multipart::new();
    let content_type = multipart.content_type("application/octet-stream");
    let head = multipart.part_header(&part_type);
    let close = multipart.close();
    let body = stream::once(std::future::ready(Ok(head)))
        .chain(body)
        .chain(stream::once(std::future::ready(Ok(close))));
    let mut res = Response::new(Body::from_stream(body));
    if let Ok(v) = HeaderValue::from_str(&content_type) {
        res.headers_mut().insert(header::CONTENT_TYPE, v);
    }
    res
}

fn no_value() -> Response {
    plain(
        StatusCode::NOT_FOUND,
        "the instance has no bulk data at this path".into(),
    )
}

fn unreadable() -> Response {
    plain(
        StatusCode::INTERNAL_SERVER_ERROR,
        "the stored file could not be read".into(),
    )
}

enum BulkValue {
    /// `len` bytes at `offset` of an open file.
    Range {
        file: Arc<std::fs::File>,
        offset: u64,
        len: u64,
    },
    /// Taken from a whole-file parse.
    Parsed(Bytes),
}

impl BulkValue {
    fn len(&self) -> u64 {
        match self {
            Self::Range { len, .. } => *len,
            Self::Parsed(b) => b.len() as u64,
        }
    }

    fn into_stream(self) -> BoxStream<'static, std::io::Result<Bytes>> {
        match self {
            Self::Range { file, offset, len } => range_stream(file, offset, len).boxed(),
            Self::Parsed(b) => stream::once(std::future::ready(Ok(b))).boxed(),
        }
    }
}

/// Open the blob and check it holds `len` bytes at `offset`, so a missing
/// or short file is a 500 rather than a broken body.
async fn open_range(path: PathBuf, offset: u64, len: u64) -> Result<BulkValue, Box<Response>> {
    let opened = tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)?;
        let size = file.metadata()?.len();
        Ok::<_, std::io::Error>((path, file, size))
    })
    .await
    .map_err(std::io::Error::other)
    .and_then(|r| r);
    match opened {
        Ok((_, file, size)) if offset.checked_add(len).is_some_and(|end| end <= size) => {
            Ok(BulkValue::Range {
                file: Arc::new(file),
                offset,
                len,
            })
        }
        Ok((path, _, size)) => {
            tracing::error!(
                path = %path.display(),
                offset,
                len,
                size,
                "bulk value lies outside the stored file"
            );
            Err(Box::new(unreadable()))
        }
        Err(e) => {
            tracing::error!("blob unreadable: {e}");
            Err(Box::new(unreadable()))
        }
    }
}

/// `len` bytes at `offset`, in positioned reads of at most 1 MiB on the
/// blocking pool. A short read ends the stream with an error.
fn range_stream(
    file: Arc<std::fs::File>,
    offset: u64,
    len: u64,
) -> impl Stream<Item = std::io::Result<Bytes>> {
    stream::unfold(Some((file, offset, len)), |state| async move {
        let (file, at, left) = state?;
        if left == 0 {
            return None;
        }
        let n = left.min(READ_CHUNK);
        let reader = Arc::clone(&file);
        let read = tokio::task::spawn_blocking(move || {
            let mut buf = vec![0u8; n as usize];
            reader
                .read_exact_at(&mut buf, at)
                .map(|()| Bytes::from(buf))
        })
        .await
        .map_err(std::io::Error::other)
        .and_then(|r| r);
        match read {
            Ok(chunk) => Some((Ok(chunk), Some((file, at + n, left - n)))),
            Err(e) => {
                tracing::error!("bulk value unreadable mid-stream: {e}");
                Some((Err(e), None))
            }
        }
    })
}

/// A bulk path in the form the metadata uses: `GGGGEEEE` (upper-case hex),
/// or `SSSSSSSS/<item>/…/GGGGEEEE` with items counted from 0. Returns the
/// `(sequence, item)` steps and the final tag; `None` for any other form.
fn parse_path(path: &str) -> Option<(Vec<(Tag, usize)>, Tag)> {
    let tag = |s: &str| -> Option<Tag> {
        let hex = s.len() == 8 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'A'..=b'F'));
        let v = u32::from_str_radix(s, 16).ok().filter(|_| hex)?;
        Some(Tag((v >> 16) as u16, v as u16))
    };
    let item = |s: &str| -> Option<usize> {
        let canonical = !s.is_empty()
            && s.bytes().all(|b| b.is_ascii_digit())
            && (s == "0" || !s.starts_with('0'));
        s.parse().ok().filter(|_| canonical)
    };
    let segments: Vec<&str> = path.split('/').collect();
    let (last, steps) = segments.split_last()?;
    if steps.len() % 2 != 0 {
        return None;
    }
    let steps = steps
        .chunks(2)
        .map(|pair| Some((tag(pair[0])?, item(pair[1])?)))
        .collect::<Option<Vec<_>>>()?;
    Some((steps, tag(last)?))
}

/// Parse the file and take the value at `items` then `tag`. `None` when
/// there is no such element or it is not a plain value (a sequence, or
/// encapsulated pixel data).
fn parsed_value(
    file: &FsPath,
    items: &[(Tag, usize)],
    tag: Tag,
    big_endian: bool,
) -> Result<Option<Vec<u8>>, dicom_object::ReadError> {
    let obj = OpenFileOptions::new().open_file(file)?;
    let mut at: &InMemDicomObject = &obj;
    for &(seq, index) in items {
        match at
            .get(seq)
            .and_then(|e| e.items())
            .and_then(|i| i.get(index))
        {
            Some(next) => at = next,
            None => return Ok(None),
        }
    }
    let Some(element) = at.get(tag) else {
        return Ok(None);
    };
    let Value::Primitive(value) = element.value() else {
        return Ok(None);
    };
    let mut bytes = value.to_bytes().into_owned();
    // Numbers are decoded into native-endian words; send them as stored,
    // like the indexed path does.
    if big_endian && cfg!(target_endian = "little") {
        let width = match element.vr() {
            VR::OW | VR::US | VR::SS | VR::AT => 2,
            VR::OL | VR::OF | VR::UL | VR::SL | VR::FL => 4,
            VR::OD | VR::OV | VR::FD | VR::UV | VR::SV => 8,
            _ => 1,
        };
        for word in bytes.chunks_exact_mut(width) {
            word.reverse();
        }
    }
    Ok(Some(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bulk_paths() {
        assert_eq!(parse_path("00091001"), Some((vec![], Tag(0x0009, 0x1001))));
        assert_eq!(
            parse_path("00480105/12/00282000"),
            Some((vec![(Tag(0x0048, 0x0105), 12)], Tag(0x0028, 0x2000)))
        );
        for bad in [
            "",
            "7fe00010",
            "0009100",
            "000910011",
            "+0091001",
            "00480105/00282000",
            "00480105/01/00282000",
            "00480105/-1/00282000",
            "00480105//00282000",
            "00091001/",
            "/00091001",
        ] {
            assert_eq!(parse_path(bad), None, "{bad:?}");
        }
    }
}
