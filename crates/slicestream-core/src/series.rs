//! Index a folder of DICOM instances into ordered slice stacks.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use dicom_dictionary_std::tags;
use dicom_object::OpenFileOptions;
use serde::Serialize;

use crate::frame::{get_f64s, get_i64, get_str, read_frame_bytes};
use crate::{Error, Frame, FrameInfo, Result};

/// Where one slice of the stack lives on disk.
#[derive(Debug, Clone)]
pub struct SliceRef {
    pub path: PathBuf,
    /// Frame number inside a (possibly multi-frame) instance.
    pub frame: u32,
    pub info: FrameInfo,
    sort_key: (f64, i64, u32),
}

/// An ordered stack of slices belonging to one SeriesInstanceUID.
///
/// Built from headers only (reading stops before PixelData), so indexing a
/// 2,000-slice CT is cheap and pixel bytes are read lazily per request.
#[derive(Debug, Clone)]
pub struct Series {
    pub uid: String,
    pub description: Option<String>,
    pub modality: Option<String>,
    slices: Vec<SliceRef>,
}

/// Lightweight JSON summary sent to clients before streaming starts.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SeriesSummary {
    pub uid: String,
    pub description: Option<String>,
    pub modality: Option<String>,
    pub slice_count: usize,
    /// Info of the first slice; most series are homogeneous.
    pub first: FrameInfo,
}

impl Series {
    /// Scan `dir` (non-recursively) and group every readable DICOM image by
    /// SeriesInstanceUID. Non-DICOM files are skipped silently.
    pub fn scan_dir(dir: impl AsRef<Path>) -> Result<Vec<Series>> {
        let dir = dir.as_ref();
        let entries = std::fs::read_dir(dir).map_err(|source| Error::Io {
            path: dir.to_path_buf(),
            source,
        })?;

        let mut groups: BTreeMap<String, Series> = BTreeMap::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Ok(obj) = OpenFileOptions::new()
                .read_until(tags::PIXEL_DATA)
                .open_file(&path)
            else {
                continue;
            };
            let Ok(info) = FrameInfo::from_object(&obj, obj.meta().transfer_syntax()) else {
                continue; // not an image (SR, PR, ...)
            };
            let uid = get_str(&obj, tags::SERIES_INSTANCE_UID).unwrap_or_else(|| "unknown".into());
            let n_frames = get_i64(&obj, tags::NUMBER_OF_FRAMES).unwrap_or(1).max(1) as u32;
            let position = slice_position(&obj);
            let instance = get_i64(&obj, tags::INSTANCE_NUMBER).unwrap_or(0);

            let series = groups.entry(uid.clone()).or_insert_with(|| Series {
                uid,
                description: get_str(&obj, tags::SERIES_DESCRIPTION),
                modality: get_str(&obj, tags::MODALITY),
                slices: Vec::new(),
            });
            for frame in 0..n_frames {
                series.slices.push(SliceRef {
                    path: path.clone(),
                    frame,
                    info: info.clone(),
                    sort_key: (position, instance, frame),
                });
            }
        }

        let mut out: Vec<Series> = groups.into_values().collect();
        for s in &mut out {
            s.slices.sort_by(|a, b| {
                a.sort_key
                    .0
                    .total_cmp(&b.sort_key.0)
                    .then(a.sort_key.1.cmp(&b.sort_key.1))
                    .then(a.sort_key.2.cmp(&b.sort_key.2))
            });
        }
        if out.is_empty() {
            return Err(Error::EmptySeries);
        }
        Ok(out)
    }

    pub fn len(&self) -> usize {
        self.slices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slices.is_empty()
    }

    pub fn slice(&self, index: usize) -> Result<&SliceRef> {
        self.slices.get(index).ok_or(Error::SliceOutOfRange {
            index,
            len: self.slices.len(),
        })
    }

    /// Load one frame's bytes. Blocking — call from `spawn_blocking` in async code.
    pub fn load_frame(&self, index: usize) -> Result<Frame> {
        let slice = self.slice(index)?;
        let data = read_frame_bytes(&slice.path, slice.frame, &slice.info)?;
        Ok(Frame {
            index,
            info: slice.info.clone(),
            data,
        })
    }

    pub fn summary(&self) -> SeriesSummary {
        SeriesSummary {
            uid: self.uid.clone(),
            description: self.description.clone(),
            modality: self.modality.clone(),
            slice_count: self.slices.len(),
            first: self.slices[0].info.clone(),
        }
    }
}

/// Distance of the slice along its normal, so stacks sort in anatomical order
/// even when InstanceNumber is missing or wrong. Returns 0 if geometry is absent.
fn slice_position(obj: &dicom_object::InMemDicomObject) -> f64 {
    let (Some(pos), Some(ori)) = (
        get_f64s(obj, tags::IMAGE_POSITION_PATIENT),
        get_f64s(obj, tags::IMAGE_ORIENTATION_PATIENT),
    ) else {
        return 0.0;
    };
    if pos.len() < 3 || ori.len() < 6 {
        return 0.0;
    }
    let (r, c) = (&ori[0..3], &ori[3..6]);
    let normal = [
        r[1] * c[2] - r[2] * c[1],
        r[2] * c[0] - r[0] * c[2],
        r[0] * c[1] - r[1] * c[0],
    ];
    pos[0] * normal[0] + pos[1] * normal[1] + pos[2] * normal[2]
}
