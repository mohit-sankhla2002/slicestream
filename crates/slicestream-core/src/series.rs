//! Index a folder of DICOM instances into ordered slice stacks.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use dicom_dictionary_std::tags;
use dicom_object::OpenFileOptions;
use serde::Serialize;

#[cfg(unix)]
use crate::frame::read_frame_at;
use crate::frame::{FrameLocation, get_f64s, get_i64, get_str, read_frame_bytes};
use crate::{Error, Frame, FrameInfo, Result};

/// Where one slice of the stack lives on disk.
#[derive(Debug, Clone)]
pub struct SliceRef {
    pub path: PathBuf,
    /// Frame number inside a (possibly multi-frame) instance.
    pub frame: u32,
    pub info: FrameInfo,
    position: Option<f64>,
    instance_number: Option<i64>,
    location: Option<FrameLocation>,
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

impl SliceRef {
    pub fn new(
        path: PathBuf,
        frame: u32,
        info: FrameInfo,
        position: Option<f64>,
        instance_number: Option<i64>,
    ) -> SliceRef {
        SliceRef {
            path,
            frame,
            info,
            position,
            instance_number,
            location: None,
        }
    }

    /// Record where the frame's bytes live so reads skip the DICOM parse.
    pub fn with_location(mut self, loc: FrameLocation) -> SliceRef {
        self.location = Some(loc);
        self
    }

    fn sort_key(&self) -> (f64, i64, u32) {
        (
            self.position.unwrap_or(0.0),
            self.instance_number.unwrap_or(0),
            self.frame,
        )
    }
}

impl Series {
    /// Build a series from already-indexed slices, sorting them anatomically.
    pub fn from_slices(
        uid: String,
        description: Option<String>,
        modality: Option<String>,
        mut slices: Vec<SliceRef>,
    ) -> Series {
        slices.sort_by(|a, b| {
            let (ka, kb) = (a.sort_key(), b.sort_key());
            ka.0.total_cmp(&kb.0)
                .then(ka.1.cmp(&kb.1))
                .then(ka.2.cmp(&kb.2))
        });
        Series {
            uid,
            description,
            modality,
            slices,
        }
    }

    /// Scan `dir` (non-recursively) and group every readable DICOM image by
    /// SeriesInstanceUID. Non-DICOM files are skipped silently.
    pub fn scan_dir(dir: impl AsRef<Path>) -> Result<Vec<Series>> {
        let dir = dir.as_ref();
        let entries = std::fs::read_dir(dir).map_err(|source| Error::Io {
            path: dir.to_path_buf(),
            source,
        })?;

        // uid -> (description, modality, slices)
        type Group = (Option<String>, Option<String>, Vec<SliceRef>);
        let mut groups: BTreeMap<String, Group> = BTreeMap::new();
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
            let Ok(info) = FrameInfo::from_dataset(&obj, obj.meta().transfer_syntax()) else {
                continue; // not an image (SR, PR, ...)
            };
            let uid = get_str(&obj, tags::SERIES_INSTANCE_UID).unwrap_or_else(|| "unknown".into());
            let n_frames = get_i64(&obj, tags::NUMBER_OF_FRAMES).unwrap_or(1).max(1) as u32;
            let position = slice_position(&obj);
            let instance = get_i64(&obj, tags::INSTANCE_NUMBER);

            let group = groups.entry(uid).or_insert_with(|| {
                (
                    get_str(&obj, tags::SERIES_DESCRIPTION),
                    get_str(&obj, tags::MODALITY),
                    Vec::new(),
                )
            });
            for frame in 0..n_frames {
                group.2.push(SliceRef::new(
                    path.clone(),
                    frame,
                    info.clone(),
                    position,
                    instance,
                ));
            }
        }

        let out: Vec<Series> = groups
            .into_iter()
            .map(|(uid, (description, modality, slices))| {
                Series::from_slices(uid, description, modality, slices)
            })
            .collect();
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
        #[cfg(unix)]
        let data = match &slice.location {
            Some(loc) => read_frame_at(&slice.path, loc)?,
            None => read_frame_bytes(&slice.path, slice.frame, &slice.info)?,
        };
        #[cfg(not(unix))]
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
/// even when InstanceNumber is missing or wrong. Returns `None` if geometry is absent.
pub fn slice_position(obj: &dicom_object::InMemDicomObject) -> Option<f64> {
    let (Some(pos), Some(ori)) = (
        get_f64s(obj, tags::IMAGE_POSITION_PATIENT),
        get_f64s(obj, tags::IMAGE_ORIENTATION_PATIENT),
    ) else {
        return None;
    };
    if pos.len() < 3 || ori.len() < 6 {
        return None;
    }
    let (r, c) = (&ori[0..3], &ori[3..6]);
    let normal = [
        r[1] * c[2] - r[2] * c[1],
        r[2] * c[0] - r[0] * c[2],
        r[0] * c[1] - r[1] * c[0],
    ];
    Some(pos[0] * normal[0] + pos[1] * normal[1] + pos[2] * normal[2])
}
