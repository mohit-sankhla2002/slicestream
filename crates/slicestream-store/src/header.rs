//! Header-only parsing of a spooled Part-10 file.
//!
//! The data set is walked token by token with a dicom-parser
//! [`StatefulDecoder`]. Only the top-level attributes that [`InstanceHeader`]
//! needs are materialized, and only when their value is at most
//! [`MAX_VALUE_BYTES`]. Every other value (large private OB/UN elements,
//! sequences and their items, waveforms, encapsulated documents, ...) is read
//! and discarded through a fixed-size buffer. Parsing stops at the first
//! top-level element after the last needed attribute, (0028,1053), which is
//! always before any (7FE0,xxxx) pixel data element. Memory use is therefore
//! bounded regardless of instance size.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::Path;

use dicom_core::header::{DataElementHeader, Header, Length, SequenceItemHeader};
use dicom_core::{DataElement, Tag, VR};
use dicom_dictionary_std::tags;
use dicom_encoding::transfer_syntax::{Codec, TransferSyntaxIndex};
use dicom_object::{FileMetaTable, InMemDicomObject};
use dicom_parser::stateful::decode::{DynStatefulDecoder, Error as DecodeError, StatefulDecode};
use dicom_transfer_syntax_registry::TransferSyntaxRegistry;
use slicestream_core::FrameInfo;
use slicestream_core::attrs::{get_i64, get_str};
use slicestream_core::series::slice_position;

/// Largest value the header parser materializes, in bytes.
pub const MAX_VALUE_BYTES: u32 = 64 * 1024;

/// Sequences nested deeper than this are rejected as unparseable.
const MAX_SEQUENCE_DEPTH: usize = 32;

/// Top-level attributes read into memory. Must cover every tag used by
/// [`read_header`], [`FrameInfo::from_dataset`] and [`slice_position`].
const NEEDED: &[Tag] = &[
    tags::SPECIFIC_CHARACTER_SET,
    tags::SOP_CLASS_UID,
    tags::SOP_INSTANCE_UID,
    tags::STUDY_DATE,
    tags::ACCESSION_NUMBER,
    tags::MODALITY,
    tags::STUDY_DESCRIPTION,
    tags::SERIES_DESCRIPTION,
    tags::PATIENT_NAME,
    tags::PATIENT_ID,
    tags::STUDY_INSTANCE_UID,
    tags::SERIES_INSTANCE_UID,
    tags::SERIES_NUMBER,
    tags::INSTANCE_NUMBER,
    tags::IMAGE_POSITION_PATIENT,
    tags::IMAGE_ORIENTATION_PATIENT,
    tags::SAMPLES_PER_PIXEL,
    tags::PHOTOMETRIC_INTERPRETATION,
    tags::NUMBER_OF_FRAMES,
    tags::ROWS,
    tags::COLUMNS,
    tags::BITS_ALLOCATED,
    tags::BITS_STORED,
    tags::PIXEL_REPRESENTATION,
    tags::WINDOW_CENTER,
    tags::WINDOW_WIDTH,
    tags::RESCALE_INTERCEPT,
    tags::RESCALE_SLOPE,
];

/// The highest tag in [`NEEDED`]; parsing stops after it.
const LAST_NEEDED: Tag = tags::RESCALE_SLOPE;

const ITEM_DELIMITER: Tag = Tag(0xFFFE, 0xE00D);

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

fn unparseable(e: impl std::fmt::Display) -> HeaderError {
    HeaderError::Unparseable(e.to_string())
}

/// What the header parser read, for tests that check memory stays bounded.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReadStats {
    /// Largest value materialized, in bytes.
    pub largest_value: u32,
    /// Bytes read and discarded without being materialized.
    pub skipped_bytes: u64,
    /// Bytes pulled from the file (including read-ahead buffering).
    pub bytes_read: u64,
}

/// Read the attributes [`InstanceHeader`] needs from the Part-10 file at
/// `path`, holding at most [`MAX_VALUE_BYTES`] per value in memory. Blocking.
pub fn read_header(path: &Path) -> Result<InstanceHeader, HeaderError> {
    read_header_with_stats(path).map(|(h, _)| h)
}

pub(crate) fn read_header_with_stats(
    path: &Path,
) -> Result<(InstanceHeader, ReadStats), HeaderError> {
    let file = File::open(path).map_err(unparseable)?;
    let counter = CountingReader {
        inner: file,
        count: Default::default(),
    };
    let bytes_read = std::rc::Rc::clone(&counter.count);
    let mut src = BufReader::new(counter);

    // Like dicom-object's `ReadPreamble::Auto`: skip a 128-byte preamble if
    // `DICM` follows it.
    let head = src.fill_buf().map_err(unparseable)?;
    if head.len() >= 132 && &head[128..132] == b"DICM" {
        src.consume(128);
    }
    let meta = FileMetaTable::from_reader(&mut src).map_err(unparseable)?;
    let transfer_syntax = meta.transfer_syntax().to_string();
    let ts = TransferSyntaxRegistry
        .get(&transfer_syntax)
        .ok_or_else(|| unparseable(format!("unknown transfer syntax {transfer_syntax}")))?;
    let reader: Box<dyn Read> = match ts.codec() {
        Codec::Dataset(Some(adapter)) => adapter.adapt_reader(Box::new(src)),
        Codec::Dataset(None) => {
            return Err(unparseable(format!(
                "unsupported transfer syntax {transfer_syntax}"
            )));
        }
        Codec::None | Codec::EncapsulatedPixelData(..) => Box::new(src),
    };
    let decoder = DynStatefulDecoder::new_with_ts(reader, ts, 0).map_err(unparseable)?;
    let mut walker = Walker {
        decoder,
        stats: ReadStats::default(),
    };
    let obj = walker.read_top_level()?;
    let mut stats = walker.stats;
    stats.bytes_read = bytes_read.get();

    let req = |tag, name: &'static str| get_str(&obj, tag).ok_or(HeaderError::Missing(name));
    let sop_class_uid = req(tags::SOP_CLASS_UID, "SOPClassUID")?;
    let sop_instance_uid = req(tags::SOP_INSTANCE_UID, "SOPInstanceUID")?;
    let study_instance_uid = req(tags::STUDY_INSTANCE_UID, "StudyInstanceUID")?;
    let series_instance_uid = req(tags::SERIES_INSTANCE_UID, "SeriesInstanceUID")?;
    let number_of_frames = get_i64(&obj, tags::NUMBER_OF_FRAMES)
        .unwrap_or(1)
        .clamp(1, i64::from(u32::MAX)) as u32;
    let header = InstanceHeader {
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
    };
    Ok((header, stats))
}

/// Counts bytes pulled from the underlying file.
struct CountingReader<R> {
    inner: R,
    count: std::rc::Rc<std::cell::Cell<u64>>,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count.set(self.count.get() + n as u64);
        Ok(n)
    }
}

struct Walker<D> {
    decoder: D,
    stats: ReadStats,
}

impl<D: StatefulDecode> Walker<D> {
    /// Collect the needed top-level attributes, skipping everything else.
    fn read_top_level(&mut self) -> Result<InMemDicomObject, HeaderError> {
        let mut obj = InMemDicomObject::new_empty();
        loop {
            let header = match self.decoder.decode_header() {
                Ok(h) => h,
                // End of data before the stop tag: fine (e.g. no pixel data).
                Err(e) if is_eof_at_tag(&e) => break,
                Err(e) => return Err(unparseable(e)),
            };
            if header.tag() > LAST_NEEDED {
                break;
            }
            if header.tag() == ITEM_DELIMITER {
                // Stray delimiter outside a sequence; dicom-rs ignores it too.
                continue;
            }
            if is_sequence(&header) {
                self.skip_sequence(header.len, 1)?;
                continue;
            }
            let len = header.len.get().unwrap_or(0);
            if NEEDED.contains(&header.tag()) && len <= MAX_VALUE_BYTES {
                let value = self
                    .decoder
                    .read_value_preserved(&header)
                    .map_err(unparseable)?;
                self.stats.largest_value = self.stats.largest_value.max(len);
                obj.put(DataElement::new(header.tag(), header.vr(), value));
            } else {
                self.skip(len)?;
            }
        }
        Ok(obj)
    }

    fn skip(&mut self, len: u32) -> Result<(), HeaderError> {
        self.decoder.skip_bytes(len).map_err(unparseable)?;
        self.stats.skipped_bytes += u64::from(len);
        Ok(())
    }

    /// Skip a sequence (or undefined-length element, which dicom-rs also
    /// reads as a sequence) whose header has just been decoded.
    fn skip_sequence(&mut self, len: Length, depth: usize) -> Result<(), HeaderError> {
        if depth > MAX_SEQUENCE_DEPTH {
            return Err(unparseable("sequences nested too deeply"));
        }
        if let Some(len) = len.get() {
            return self.skip(len);
        }
        loop {
            match self.decoder.decode_item_header().map_err(unparseable)? {
                SequenceItemHeader::Item { len } => match len.get() {
                    Some(len) => self.skip(len)?,
                    None => self.skip_item(depth)?,
                },
                SequenceItemHeader::SequenceDelimiter => return Ok(()),
                SequenceItemHeader::ItemDelimiter => {
                    return Err(unparseable("item delimiter outside an item"));
                }
            }
        }
    }

    /// Skip the elements of an undefined-length item up to its delimiter.
    fn skip_item(&mut self, depth: usize) -> Result<(), HeaderError> {
        loop {
            let header = self.decoder.decode_header().map_err(unparseable)?;
            if header.tag() == ITEM_DELIMITER {
                return Ok(());
            }
            if is_sequence(&header) {
                self.skip_sequence(header.len, depth + 1)?;
            } else {
                self.skip(header.len.get().unwrap_or(0))?;
            }
        }
    }
}

fn is_sequence(header: &DataElementHeader) -> bool {
    header.vr() == VR::SQ || header.len.is_undefined()
}

/// Whether decoding a header failed because the data ended before its tag.
fn is_eof_at_tag(e: &DecodeError) -> bool {
    matches!(
        e,
        DecodeError::DecodeElementHeader {
            source: dicom_encoding::decode::Error::ReadHeaderTag { source, .. },
            ..
        } if source.kind() == io::ErrorKind::UnexpectedEof
    )
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

    /// The old header path: a full `dicom-object` parse up to PixelData.
    fn reference_header(path: &std::path::Path) -> InstanceHeader {
        use dicom_object::OpenFileOptions;
        let obj = OpenFileOptions::new()
            .read_until(tags::PIXEL_DATA)
            .open_file(path)
            .unwrap();
        let ts = obj.meta().transfer_syntax().to_string();
        let s = |t| slicestream_core::attrs::get_str(&obj, t);
        let i = |t| slicestream_core::attrs::get_i64(&obj, t);
        InstanceHeader {
            sop_class_uid: s(tags::SOP_CLASS_UID).unwrap(),
            sop_instance_uid: s(tags::SOP_INSTANCE_UID).unwrap(),
            study_instance_uid: s(tags::STUDY_INSTANCE_UID).unwrap(),
            series_instance_uid: s(tags::SERIES_INSTANCE_UID).unwrap(),
            patient_id: s(tags::PATIENT_ID),
            patient_name: s(tags::PATIENT_NAME),
            study_date: s(tags::STUDY_DATE),
            study_description: s(tags::STUDY_DESCRIPTION),
            accession_number: s(tags::ACCESSION_NUMBER),
            modality: s(tags::MODALITY),
            series_number: i(tags::SERIES_NUMBER),
            series_description: s(tags::SERIES_DESCRIPTION),
            instance_number: i(tags::INSTANCE_NUMBER),
            number_of_frames: i(tags::NUMBER_OF_FRAMES)
                .unwrap_or(1)
                .clamp(1, i64::from(u32::MAX)) as u32,
            slice_position: slice_position(&obj),
            frame_info: FrameInfo::from_dataset(&obj, &ts).ok(),
            transfer_syntax: ts,
        }
    }

    #[test]
    fn read_header_matches_full_parse() {
        let dir = tempfile::tempdir().unwrap();
        crate::phantom::write_phantom(dir.path(), 2, 16).unwrap();
        let p = dir.path().join("IM00002.dcm");
        assert_eq!(read_header(&p).unwrap(), reference_header(&p));
    }

    /// Phantom slice with extra elements (in tag order) inserted before
    /// PixelData. Returns the original header for comparison.
    fn phantom_with(
        dir: &std::path::Path,
        extra: Vec<DataElement<InMemDicomObject>>,
    ) -> (std::path::PathBuf, InstanceHeader) {
        crate::phantom::write_phantom(dir, 1, 16).unwrap();
        let src = dir.join("IM00001.dcm");
        let expected = read_header(&src).unwrap();
        let mut obj = dicom_object::open_file(&src).unwrap();
        for e in extra {
            obj.put(e);
        }
        let out = dir.join("extra.dcm");
        obj.write_to_file(&out).unwrap();
        (out, expected)
    }

    #[test]
    fn large_private_element_is_skipped_not_held() {
        const BIG: usize = 50 * 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let (p, expected) = phantom_with(
            dir.path(),
            vec![
                DataElement::new(
                    dicom_core::Tag(0x0009, 0x0010),
                    VR::LO,
                    PrimitiveValue::from("SLICESTREAM TEST"),
                ),
                DataElement::new(
                    dicom_core::Tag(0x0009, 0x1001),
                    VR::OB,
                    PrimitiveValue::from(vec![0xABu8; BIG]),
                ),
            ],
        );
        let (h, stats) = read_header_with_stats(&p).unwrap();
        // Attributes after the big element (PatientID, Rows, ...) still parse.
        assert_eq!(h, expected);
        assert_eq!(h, reference_header(&p));
        assert!(
            stats.largest_value <= MAX_VALUE_BYTES,
            "materialized a {} byte value",
            stats.largest_value
        );
        assert!(stats.skipped_bytes >= BIG as u64, "{stats:?}");
    }

    #[test]
    fn large_values_inside_sequences_are_skipped() {
        const BIG: usize = 4 * 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let item = InMemDicomObject::from_element_iter([DataElement::new(
            dicom_core::Tag(0x0009, 0x1002),
            VR::OB,
            PrimitiveValue::from(vec![1u8; BIG]),
        )]);
        let (p, expected) = phantom_with(
            dir.path(),
            vec![
                DataElement::new(
                    dicom_core::Tag(0x0009, 0x0010),
                    VR::LO,
                    PrimitiveValue::from("SLICESTREAM TEST"),
                ),
                DataElement::new(
                    dicom_core::Tag(0x0009, 0x1001),
                    VR::SQ,
                    dicom_core::value::DataSetSequence::from(vec![item]),
                ),
            ],
        );
        let (h, stats) = read_header_with_stats(&p).unwrap();
        assert_eq!(h, expected);
        assert!(stats.largest_value <= MAX_VALUE_BYTES, "{stats:?}");
        assert!(stats.skipped_bytes >= BIG as u64, "{stats:?}");
    }

    #[test]
    fn stops_before_float_pixel_data() {
        const BIG: usize = 8 * 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("float.dcm");
        write_obj(
            &p,
            vec![
                uid(tags::SOP_CLASS_UID, uids::BASIC_TEXT_SR_STORAGE),
                uid(tags::SOP_INSTANCE_UID, "2.25.42"),
                uid(tags::STUDY_INSTANCE_UID, "2.25.1"),
                uid(tags::SERIES_INSTANCE_UID, "2.25.2"),
                DataElement::new(
                    tags::FLOAT_PIXEL_DATA,
                    VR::OF,
                    PrimitiveValue::from(vec![0u8; BIG]),
                ),
            ],
        );
        let (h, stats) = read_header_with_stats(&p).unwrap();
        assert_eq!(h.sop_instance_uid, "2.25.42");
        assert_eq!(stats.skipped_bytes, 0, "{stats:?}");
        // Reading stopped at the element header, well before its value.
        assert!(stats.bytes_read < 64 * 1024, "{stats:?}");
    }
}
