//! Ingest-time scan of a stored Part-10 file.
//!
//! One pass over the data set, after the file meta group, produces the
//! [`InstanceHeader`], the DICOM JSON metadata (PS3.18 Annex F), a frame
//! table and a bulk-value table. Bulk values are never read into memory:
//! their file offset and length are recorded and the JSON gets a
//! `BulkDataURI`. Like [`read_header`], memory stays bounded whatever the
//! instance size (the frame table grows with the number of frames).
//!
//! Acceptance does not change: when the scan fails before it has passed the
//! header attributes, [`read_header`] decides whether the instance is
//! accepted. Later failures are recorded as a problem, never as an error.

use std::path::Path;

use dicom_core::header::{DataElementHeader, Header, Length, SequenceItemHeader};
use dicom_core::{DataElement, PrimitiveValue, Tag, VR};
use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;
use dicom_parser::stateful::decode::{DynStatefulDecoder, StatefulDecode};
use serde::{Deserialize, Serialize};
use slicestream_core::attrs::get_str;

use crate::frames::{FrameTable, group_fragments, native_table};
use crate::header::{
    DataSetSource, HeaderError, ITEM_DELIMITER, InstanceHeader, LAST_NEEDED, MAX_SEQUENCE_DEPTH,
    MAX_VALUE_BYTES, NEEDED, Walker, header_from, is_eof_at_tag, is_sequence, open_data_set,
    read_header, unparseable,
};
use crate::json::DicomJsonWriter;

/// Version of the scan output. Rows scanned with an older version are
/// scanned again.
pub const SCAN_VERSION: i64 = 1;

/// Largest DICOM JSON kept for one instance, in bytes.
pub const MAX_METADATA_BYTES: usize = 67_108_864;

/// Binary values (OB, OD, OF, OL, OV, OW, UN) longer than this are bulk data.
pub const BULK_BINARY_MIN: u32 = 1024;

/// Top-level attributes read for the query columns, on top of the ones
/// `read_header` needs.
const QUERY_ATTRS: &[Tag] = &[
    tags::STUDY_TIME,
    tags::REFERRING_PHYSICIAN_NAME,
    tags::PATIENT_BIRTH_DATE,
    tags::PATIENT_SEX,
    tags::STUDY_ID,
    tags::PERFORMED_PROCEDURE_STEP_START_DATE,
    tags::PERFORMED_PROCEDURE_STEP_START_TIME,
];

/// Path of the top-level Pixel Data value.
const PIXEL_DATA_PATH: &str = "7FE00010";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanLimits {
    /// Metadata JSON above this size is dropped and recorded as a problem.
    pub max_metadata_bytes: usize,
}

impl Default for ScanLimits {
    fn default() -> Self {
        Self {
            max_metadata_bytes: MAX_METADATA_BYTES,
        }
    }
}

/// Where one bulk value lives in the stored file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulkValue {
    /// `GGGGEEEE` at the top level, `SSSSSSSS/<item>/GGGGEEEE` when nested
    /// (items count from 0).
    pub path: String,
    pub offset: u64,
    pub len: u64,
}

/// Everything the scan derives from an instance besides its header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceData {
    /// DICOM JSON of the whole data set; `None` when over the size cap.
    pub metadata: Option<Vec<u8>>,
    /// `None` when the instance has no usable pixel data.
    pub frames: Option<FrameTable>,
    pub bulk: Vec<BulkValue>,
    /// False for deflated data sets: no frame or bulk offsets are recorded.
    pub offsets_valid: bool,
    /// What went wrong, if anything. The instance is still stored.
    pub problem: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InstanceScan {
    pub header: InstanceHeader,
    pub data: InstanceData,
}

/// Scan the Part-10 file at `path` with the default limits. Blocking.
pub fn scan_instance(path: &Path) -> Result<InstanceScan, HeaderError> {
    scan_instance_with(path, &ScanLimits::default())
}

/// Scan the Part-10 file at `path`. Returns `Err` exactly when
/// [`read_header`] would; any later problem is reported in
/// [`InstanceData::problem`]. Blocking.
pub fn scan_instance_with(path: &Path, limits: &ScanLimits) -> Result<InstanceScan, HeaderError> {
    let mut scanner = Scanner::new(open_data_set(path)?, limits);
    let stop = scanner.run().err();
    let header = match scanner.header.take() {
        Some(header) => header,
        // Stopped before the header attributes were complete: accept or
        // reject exactly as read_header does.
        None => read_header(path)?,
    };
    Ok(scanner.finish(header, stop))
}

/// Why the scan stopped early.
enum Stop {
    /// The data set could not be parsed further.
    Parse(HeaderError),
    /// A problem with a fixed description.
    Problem(String),
}

impl From<HeaderError> for Stop {
    fn from(e: HeaderError) -> Self {
        Stop::Parse(e)
    }
}

impl Stop {
    fn describe(self) -> String {
        match self {
            Stop::Parse(e) => e.to_string(),
            Stop::Problem(p) => p,
        }
    }
}

fn parse_error(e: impl std::fmt::Display) -> Stop {
    Stop::Parse(unparseable(e))
}

struct Scanner {
    walker: Walker<DynStatefulDecoder<Box<dyn std::io::Read>>>,
    transfer_syntax: String,
    offsets_valid: bool,
    file_len: Option<u64>,
    /// Top-level attributes for the header and the query columns.
    attrs: InMemDicomObject,
    /// Set once the walk passes the last header attribute.
    header: Option<InstanceHeader>,
    json: DicomJsonWriter,
    max_json: usize,
    /// Set once the JSON passed `max_json`; later output is only counted.
    json_over: bool,
    json_dropped: usize,
    bulk: Vec<BulkValue>,
    extended_offsets: Option<Vec<u64>>,
    extended_lengths: Option<Vec<u64>>,
    frames: Option<FrameTable>,
    /// Problems that did not stop the scan.
    problems: Vec<String>,
}

impl Scanner {
    fn new(source: DataSetSource, limits: &ScanLimits) -> Self {
        Self {
            walker: Walker {
                decoder: source.decoder,
                stats: Default::default(),
            },
            transfer_syntax: source.transfer_syntax,
            offsets_valid: source.offsets_valid,
            file_len: source.file_len,
            attrs: InMemDicomObject::new_empty(),
            header: None,
            json: DicomJsonWriter::new(),
            max_json: limits.max_metadata_bytes,
            json_over: false,
            json_dropped: 0,
            bulk: Vec::new(),
            extended_offsets: None,
            extended_lengths: None,
            frames: None,
            problems: Vec::new(),
        }
    }

    fn position(&self) -> u64 {
        self.walker.decoder.position()
    }

    fn run(&mut self) -> Result<(), Stop> {
        loop {
            let header = match self.walker.decoder.decode_header() {
                Ok(h) => h,
                // End of data at an element boundary: the data set is complete.
                Err(e) if is_eof_at_tag(&e) => break,
                Err(e) => return Err(parse_error(e)),
            };
            if self.header.is_none() && header.tag() > LAST_NEEDED {
                self.pass()?;
            }
            if header.tag() == ITEM_DELIMITER {
                // Stray delimiter outside a sequence; read_header ignores it too.
                continue;
            }
            self.element(&header, "", 0)?;
        }
        if self.header.is_none() {
            self.pass()?;
        }
        Ok(())
    }

    /// The walk is past (0028,1053): every header attribute has been seen.
    fn pass(&mut self) -> Result<(), HeaderError> {
        self.header = Some(header_from(&self.attrs, self.transfer_syntax.clone())?);
        Ok(())
    }

    fn element(
        &mut self,
        header: &DataElementHeader,
        prefix: &str,
        depth: usize,
    ) -> Result<(), Stop> {
        self.element_value(header, prefix, depth)?;
        if !self.json_over && self.json.len() > self.max_json {
            self.json_over = true;
        }
        if self.json_over {
            self.json_dropped += self.json.discard();
        }
        Ok(())
    }

    /// One element whose header was just decoded. `depth` is 0 at the top
    /// level and the sequence depth inside items.
    fn element_value(
        &mut self,
        header: &DataElementHeader,
        prefix: &str,
        depth: usize,
    ) -> Result<(), Stop> {
        let tag = header.tag();
        let vr = header.vr();
        if tag.group() == 0x0002 {
            // File meta attributes never go into the metadata.
            if is_sequence(header) {
                self.walker.skip_sequence(header.len, depth + 1)?;
            } else {
                self.walker.skip(header.len.get().unwrap_or(0))?;
            }
            return Ok(());
        }
        let path = format!("{prefix}{:04X}{:04X}", tag.group(), tag.element());
        if is_sequence(header) {
            if vr == VR::SQ || !is_pixel_data(tag) {
                return self.sequence(header, &path, depth + 1);
            }
            if depth == 0 && tag == tags::PIXEL_DATA {
                return self.encapsulated(vr);
            }
            // Encapsulated pixel data elsewhere (an icon image, say): its
            // fragments cannot be served as one bulk value.
            self.walker.skip_sequence(header.len, depth + 1)?;
            self.json.element(tag, vr, &PrimitiveValue::Empty);
            return Ok(());
        }

        let len = header.len.get().unwrap_or(0);
        let offset = self.position();
        let bulk = is_bulk(tag, vr, len);
        if depth == 0 {
            if tag == tags::PIXEL_DATA {
                return self.native_pixel_data(vr, offset, len);
            }
            if matches!(
                tag,
                tags::EXTENDED_OFFSET_TABLE | tags::EXTENDED_OFFSET_TABLE_LENGTHS
            ) && self.offsets_valid
                && u64::from(len) == 8 * u64::from(self.number_of_frames())
            {
                return self.extended_offset_table(tag, vr, path, offset, len);
            }
            if (NEEDED.contains(&tag) || QUERY_ATTRS.contains(&tag)) && len <= MAX_VALUE_BYTES {
                // Read exactly as read_header does, so the header matches.
                let value = self
                    .walker
                    .decoder
                    .read_value_preserved(header)
                    .map_err(parse_error)?;
                if bulk {
                    self.add_bulk(tag, vr, path, offset, len);
                } else {
                    self.json.element(tag, vr, &value);
                }
                self.attrs.put(DataElement::new(tag, vr, value));
                return Ok(());
            }
        }
        if bulk {
            self.check_stored(offset, len, || format!("value {path} truncated"))?;
            self.walker.skip(len)?;
            self.add_bulk(tag, vr, path, offset, len);
        } else {
            let value = self.read_value(header, depth)?;
            self.json.element(tag, vr, &value);
        }
        Ok(())
    }

    fn read_value(
        &mut self,
        header: &DataElementHeader,
        depth: usize,
    ) -> Result<PrimitiveValue, Stop> {
        let decoder = &mut self.walker.decoder;
        if depth > 0 && header.tag() == tags::SPECIFIC_CHARACTER_SET {
            // The decoder would apply an item's character set to the rest of
            // the data set, so the top-level one is kept instead.
            let raw = decoder.read_value_bytes(header).map_err(parse_error)?;
            return Ok(PrimitiveValue::from(
                String::from_utf8_lossy(&raw.to_bytes()).into_owned(),
            ));
        }
        decoder.read_value_preserved(header).map_err(parse_error)
    }

    fn number_of_frames(&self) -> u32 {
        self.header.as_ref().map_or(1, |h| h.number_of_frames)
    }

    fn add_bulk(&mut self, tag: Tag, vr: VR, path: String, offset: u64, len: u32) {
        self.json.bulk(tag, vr, &path);
        if self.offsets_valid {
            self.bulk.push(BulkValue {
                path,
                offset,
                len: u64::from(len),
            });
        }
    }

    /// Fail when a value of `len` bytes at `offset` runs past the end of the file.
    fn check_stored(
        &self,
        offset: u64,
        len: u32,
        problem: impl FnOnce() -> String,
    ) -> Result<(), Stop> {
        match self.file_len {
            Some(file_len) if self.offsets_valid && offset + u64::from(len) > file_len => {
                Err(Stop::Problem(problem()))
            }
            _ => Ok(()),
        }
    }

    fn sequence(
        &mut self,
        header: &DataElementHeader,
        path: &str,
        depth: usize,
    ) -> Result<(), Stop> {
        if depth > MAX_SEQUENCE_DEPTH {
            return Err(parse_error("sequences nested too deeply"));
        }
        self.json.begin_sequence(header.tag());
        let end = header.len.get().map(|len| self.position() + u64::from(len));
        let mut index = 0usize;
        while end.is_none_or(|end| self.position() < end) {
            match self
                .walker
                .decoder
                .decode_item_header()
                .map_err(parse_error)?
            {
                SequenceItemHeader::Item { len } => {
                    self.item(len, &format!("{path}/{index}/"), depth)?;
                    index += 1;
                }
                SequenceItemHeader::SequenceDelimiter => break,
                SequenceItemHeader::ItemDelimiter => {
                    return Err(parse_error("item delimiter outside an item"));
                }
            }
        }
        self.check_end(end)?;
        self.json.end_sequence();
        Ok(())
    }

    fn item(&mut self, len: Length, prefix: &str, depth: usize) -> Result<(), Stop> {
        self.json.begin_item();
        let end = len.get().map(|len| self.position() + u64::from(len));
        while end.is_none_or(|end| self.position() < end) {
            let header = self.walker.decoder.decode_header().map_err(parse_error)?;
            if header.tag() == ITEM_DELIMITER {
                if end.is_none() {
                    break;
                }
                continue;
            }
            self.element(&header, prefix, depth)?;
        }
        self.check_end(end)?;
        self.json.end_item();
        Ok(())
    }

    /// A defined-length sequence or item must end exactly where it says.
    fn check_end(&self, end: Option<u64>) -> Result<(), Stop> {
        match end {
            Some(end) if self.position() != end => Err(parse_error(format!(
                "contents end at {} but the length says {end}",
                self.position()
            ))),
            _ => Ok(()),
        }
    }

    fn native_pixel_data(&mut self, vr: VR, offset: u64, len: u32) -> Result<(), Stop> {
        self.json.bulk(tags::PIXEL_DATA, vr, PIXEL_DATA_PATH);
        self.check_stored(offset, len, || "pixel data truncated".into())?;
        self.walker.skip(len)?;
        if !self.offsets_valid {
            return Ok(());
        }
        let Some(header) = &self.header else {
            return Ok(());
        };
        match native_table(header, offset, len) {
            Some(Ok(table)) => self.frames = Some(table),
            Some(Err(problem)) => self.problems.push(problem.into()),
            None => {}
        }
        Ok(())
    }

    /// Read the fragments of encapsulated top-level Pixel Data and group
    /// them into frames.
    fn encapsulated(&mut self, vr: VR) -> Result<(), Stop> {
        self.json.bulk(tags::PIXEL_DATA, vr, PIXEL_DATA_PATH);
        let frames = self.number_of_frames();
        let mut basic: Option<Vec<u32>> = None;
        let mut fragments = Vec::new();
        let mut marked = Vec::new();
        loop {
            match self
                .walker
                .decoder
                .decode_item_header()
                .map_err(parse_error)?
            {
                SequenceItemHeader::Item { len } => {
                    let len = len
                        .get()
                        .ok_or_else(|| parse_error("pixel data item of undefined length"))?;
                    let offset = self.position();
                    self.check_stored(offset, len, || "pixel data truncated".into())?;
                    if basic.is_none() {
                        basic = Some(self.basic_offset_table(len, frames)?);
                    } else {
                        marked.push(self.starts_codestream(len)?);
                        fragments.push((offset, u64::from(len)));
                    }
                }
                SequenceItemHeader::SequenceDelimiter => break,
                SequenceItemHeader::ItemDelimiter => {
                    return Err(parse_error("item delimiter in pixel data"));
                }
            }
        }
        if !self.offsets_valid {
            return Ok(());
        }
        let extended = match (&self.extended_offsets, &self.extended_lengths) {
            (Some(o), Some(l)) => Some((o.as_slice(), l.as_slice())),
            _ => None,
        };
        let basic = basic.unwrap_or_default();
        match group_fragments(frames, &fragments, &marked, &basic, extended) {
            Some(groups) => self.frames = Some(FrameTable::Fragments(groups)),
            None => {
                let sop = self.header.as_ref().map(|h| h.sop_instance_uid.as_str());
                tracing::warn!(
                    sop_instance_uid = sop.unwrap_or_default(),
                    frames,
                    fragments = fragments.len(),
                    "frame boundaries unknown"
                );
                self.problems.push("frame boundaries unknown".into());
            }
        }
        Ok(())
    }

    /// The first item of encapsulated pixel data. Read only when it has one
    /// entry per frame; any other table is unusable and skipped.
    fn basic_offset_table(&mut self, len: u32, frames: u32) -> Result<Vec<u32>, Stop> {
        if !self.offsets_valid || u64::from(len) != 4 * u64::from(frames) {
            self.walker.skip(len)?;
            return Ok(Vec::new());
        }
        let mut raw = Vec::new();
        self.walker
            .decoder
            .read_to_vec(len, &mut raw)
            .map_err(parse_error)?;
        let (entries, _) = raw.as_chunks::<4>();
        Ok(entries.iter().map(|&e| u32::from_le_bytes(e)).collect())
    }

    /// Read the first bytes of a fragment and skip the rest. True when they
    /// are a JPEG SOI or a JPEG 2000 SOC + SIZ marker.
    fn starts_codestream(&mut self, len: u32) -> Result<bool, Stop> {
        let n = len.min(4);
        let mut head = Vec::with_capacity(4);
        self.walker
            .decoder
            .read_to_vec(n, &mut head)
            .map_err(parse_error)?;
        self.walker.skip(len - n)?;
        Ok(head.starts_with(&[0xFF, 0xD8]) || head.starts_with(&[0xFF, 0x4F, 0xFF, 0x51]))
    }

    /// Extended Offset Table or its lengths, read even though binary.
    fn extended_offset_table(
        &mut self,
        tag: Tag,
        vr: VR,
        path: String,
        offset: u64,
        len: u32,
    ) -> Result<(), Stop> {
        self.check_stored(offset, len, || format!("value {path} truncated"))?;
        let mut raw = Vec::new();
        self.walker
            .decoder
            .read_to_vec(len, &mut raw)
            .map_err(parse_error)?;
        let (entries, _) = raw.as_chunks::<8>();
        let values = entries.iter().map(|&e| u64::from_le_bytes(e)).collect();
        if tag == tags::EXTENDED_OFFSET_TABLE {
            self.extended_offsets = Some(values);
        } else {
            self.extended_lengths = Some(values);
        }
        if is_bulk(tag, vr, len) {
            self.add_bulk(tag, vr, path, offset, len);
        } else {
            self.json.element(tag, vr, &PrimitiveValue::from(raw));
        }
        Ok(())
    }

    fn finish(self, mut header: InstanceHeader, stop: Option<Stop>) -> InstanceScan {
        let attr = |tag| get_str(&self.attrs, tag);
        header.study_time = attr(tags::STUDY_TIME);
        header.study_id = attr(tags::STUDY_ID);
        header.referring_physician_name = attr(tags::REFERRING_PHYSICIAN_NAME);
        header.patient_birth_date = attr(tags::PATIENT_BIRTH_DATE);
        header.patient_sex = attr(tags::PATIENT_SEX);
        header.performed_procedure_step_start_date =
            attr(tags::PERFORMED_PROCEDURE_STEP_START_DATE);
        header.performed_procedure_step_start_time =
            attr(tags::PERFORMED_PROCEDURE_STEP_START_TIME);

        let mut problems: Vec<String> = stop.map(Stop::describe).into_iter().collect();
        problems.extend(self.problems);
        let json = self.json.finish();
        let size = self.json_dropped + json.len();
        let metadata = if size > self.max_json {
            problems.push(format!("metadata too large ({size} bytes)"));
            None
        } else {
            Some(json)
        };
        InstanceScan {
            header,
            data: InstanceData {
                metadata,
                frames: self.frames,
                bulk: self.bulk,
                offsets_valid: self.offsets_valid,
                problem: (!problems.is_empty()).then(|| problems.join("; ")),
            },
        }
    }
}

fn is_pixel_data(tag: Tag) -> bool {
    matches!(
        tag,
        tags::PIXEL_DATA | tags::FLOAT_PIXEL_DATA | tags::DOUBLE_FLOAT_PIXEL_DATA
    )
}

/// The bulk rule: pixel data always; binary VRs above [`BULK_BINARY_MIN`];
/// anything above [`MAX_VALUE_BYTES`].
fn is_bulk(tag: Tag, vr: VR, len: u32) -> bool {
    is_pixel_data(tag)
        || (matches!(
            vr,
            VR::OB | VR::OD | VR::OF | VR::OL | VR::OV | VR::OW | VR::UN
        ) && len > BULK_BINARY_MIN)
        || len > MAX_VALUE_BYTES
}
