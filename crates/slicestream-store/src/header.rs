//! Header-only parsing of a spooled Part-10 file (everything before PixelData).

use std::path::Path;

use dicom_dictionary_std::tags;
use dicom_object::OpenFileOptions;
use slicestream_core::FrameInfo;
use slicestream_core::attrs::{get_i64, get_str};
use slicestream_core::series::slice_position;

/// Attributes extracted from an instance header for validation and indexing.
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceHeader {
    pub sop_class_uid: String,
    pub sop_instance_uid: String,
    pub study_instance_uid: String,
    pub series_instance_uid: String,
    pub patient_id: Option<String>,
    pub patient_name: Option<String>,
    pub study_date: Option<String>,
    pub study_description: Option<String>,
    pub accession_number: Option<String>,
    pub modality: Option<String>,
    pub series_number: Option<i64>,
    pub series_description: Option<String>,
    pub instance_number: Option<i64>,
    pub number_of_frames: u32,
    pub transfer_syntax: String,
    pub slice_position: Option<f64>,
    /// `None` for non-image objects (SR, PR, ...).
    pub frame_info: Option<FrameInfo>,
}

#[derive(Debug, thiserror::Error)]
pub enum HeaderError {
    #[error("unparseable DICOM: {0}")]
    Unparseable(String),
    #[error("missing required attribute {0}")]
    Missing(&'static str),
}

/// Read the Part-10 file at `path` up to (excluding) PixelData. Blocking.
pub fn read_header(path: &Path) -> Result<InstanceHeader, HeaderError> {
    let obj = OpenFileOptions::new()
        .read_until(tags::PIXEL_DATA)
        .open_file(path)
        .map_err(|e| HeaderError::Unparseable(e.to_string()))?;
    let req = |tag, name: &'static str| get_str(&obj, tag).ok_or(HeaderError::Missing(name));
    let sop_class_uid = req(tags::SOP_CLASS_UID, "SOPClassUID")?;
    let sop_instance_uid = req(tags::SOP_INSTANCE_UID, "SOPInstanceUID")?;
    let study_instance_uid = req(tags::STUDY_INSTANCE_UID, "StudyInstanceUID")?;
    let series_instance_uid = req(tags::SERIES_INSTANCE_UID, "SeriesInstanceUID")?;
    let transfer_syntax = obj.meta().transfer_syntax().to_string();
    let number_of_frames = get_i64(&obj, tags::NUMBER_OF_FRAMES)
        .unwrap_or(1)
        .clamp(1, i64::from(u32::MAX)) as u32;
    Ok(InstanceHeader {
        sop_class_uid,
        sop_instance_uid,
        study_instance_uid,
        series_instance_uid,
        patient_id: get_str(&obj, tags::PATIENT_ID),
        patient_name: get_str(&obj, tags::PATIENT_NAME),
        study_date: get_str(&obj, tags::STUDY_DATE),
        study_description: get_str(&obj, tags::STUDY_DESCRIPTION),
        accession_number: get_str(&obj, tags::ACCESSION_NUMBER),
        modality: get_str(&obj, tags::MODALITY),
        series_number: get_i64(&obj, tags::SERIES_NUMBER),
        series_description: get_str(&obj, tags::SERIES_DESCRIPTION),
        instance_number: get_i64(&obj, tags::INSTANCE_NUMBER),
        number_of_frames,
        slice_position: slice_position(&obj),
        frame_info: FrameInfo::from_dataset(&obj, &transfer_syntax).ok(),
        transfer_syntax,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dicom_core::{DataElement, PrimitiveValue, VR};
    use dicom_dictionary_std::{tags, uids};
    use dicom_object::{FileMetaTableBuilder, InMemDicomObject};

    #[test]
    fn read_header_phantom() {
        let dir = tempfile::tempdir().unwrap();
        crate::phantom::write_phantom(dir.path(), 3, 32).unwrap();
        let h = read_header(&dir.path().join("IM00001.dcm")).unwrap();
        assert_eq!(h.sop_class_uid, uids::CT_IMAGE_STORAGE);
        assert!(h.sop_instance_uid.starts_with("2.25."));
        assert_eq!(
            h.series_instance_uid,
            "2.25.1000000000000000000000000000001"
        );
        assert_eq!(h.study_instance_uid, "2.25.1000000000000000000000000000002");
        assert_eq!(h.modality.as_deref(), Some("CT"));
        assert_eq!(h.transfer_syntax, uids::EXPLICIT_VR_LITTLE_ENDIAN);
        assert_eq!(h.number_of_frames, 1);
        assert_eq!(h.frame_info.unwrap().rows, 32);
        assert!(h.slice_position.is_some());
        assert!(h.instance_number.is_some());
    }

    #[test]
    fn read_header_rejects_non_dicom() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("junk.dcm");
        std::fs::write(&p, b"this is definitely not a dicom file").unwrap();
        assert!(matches!(read_header(&p), Err(HeaderError::Unparseable(_))));
    }

    fn write_obj(path: &std::path::Path, elems: Vec<DataElement<InMemDicomObject>>) {
        let obj = InMemDicomObject::from_element_iter(elems);
        let file = obj
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(uids::EXPLICIT_VR_LITTLE_ENDIAN)
                    .media_storage_sop_class_uid(uids::BASIC_TEXT_SR_STORAGE)
                    .media_storage_sop_instance_uid("2.25.42"),
            )
            .unwrap();
        file.write_to_file(path).unwrap();
    }

    fn uid(tag: dicom_core::Tag, v: &str) -> DataElement<InMemDicomObject> {
        DataElement::new(tag, VR::UI, PrimitiveValue::from(v))
    }

    #[test]
    fn read_header_missing_sop_instance_uid() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("nouid.dcm");
        write_obj(
            &p,
            vec![
                uid(tags::SOP_CLASS_UID, uids::BASIC_TEXT_SR_STORAGE),
                uid(tags::STUDY_INSTANCE_UID, "2.25.1"),
                uid(tags::SERIES_INSTANCE_UID, "2.25.2"),
            ],
        );
        assert!(matches!(
            read_header(&p),
            Err(HeaderError::Missing("SOPInstanceUID"))
        ));
    }

    #[test]
    fn read_header_non_image_has_no_frame_info() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("sr.dcm");
        write_obj(
            &p,
            vec![
                uid(tags::SOP_CLASS_UID, uids::BASIC_TEXT_SR_STORAGE),
                uid(tags::SOP_INSTANCE_UID, "2.25.42"),
                uid(tags::STUDY_INSTANCE_UID, "2.25.1"),
                uid(tags::SERIES_INSTANCE_UID, "2.25.2"),
                DataElement::new(tags::MODALITY, VR::CS, PrimitiveValue::from("SR")),
            ],
        );
        let h = read_header(&p).unwrap();
        assert!(h.frame_info.is_none());
        assert_eq!(h.modality.as_deref(), Some("SR"));
        assert_eq!(h.patient_id, None);
    }
}
