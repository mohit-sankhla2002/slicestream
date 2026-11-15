//! QIDO-RS query model and parsing (DICOM PS3.18 section 8.3.4 and 10.6).
//!
//! Unsupported query parameters and unsupported matching attributes are ignored, as
//! PS3.18 section 8.3 requires ("The origin server shall ignore any unsupported Query
//! Parameters"). They are listed in [`QidoQuery::ignored`] so the caller can say so in a
//! `Warning` header. Invalid values for supported parameters are errors (HTTP 400).
use std::fmt;

use dicom_core::Tag;
use dicom_core::dictionary::{DataDictionary, DataDictionaryEntry, TagRange};
use dicom_dictionary_std::StandardDataDictionary;
use percent_encoding::percent_decode_str;

/// Query level. Ordered: `Study < Series < Instance`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Level {
    Study,
    Series,
    Instance,
}

/// A matching attribute the server supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Key {
    StudyDate,
    StudyTime,
    AccessionNumber,
    ModalitiesInStudy,
    ReferringPhysicianName,
    PatientName,
    PatientId,
    StudyInstanceUid,
    StudyId,
    StudyDescription,
    Modality,
    SeriesInstanceUid,
    SeriesNumber,
    PpsStartDate,
    PpsStartTime,
    SeriesDescription,
    SopClassUid,
    SopInstanceUid,
    InstanceNumber,
    RequestAttributeSequence,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Uid,
    Date,
    Time,
    Text,
}

const REQUEST_ATTRIBUTE_SEQUENCE: Tag = Tag(0x0040, 0x0275);

impl Key {
    pub fn tag(self) -> Tag {
        match self {
            Key::StudyDate => Tag(0x0008, 0x0020),
            Key::StudyTime => Tag(0x0008, 0x0030),
            Key::AccessionNumber => Tag(0x0008, 0x0050),
            Key::ModalitiesInStudy => Tag(0x0008, 0x0061),
            Key::ReferringPhysicianName => Tag(0x0008, 0x0090),
            Key::PatientName => Tag(0x0010, 0x0010),
            Key::PatientId => Tag(0x0010, 0x0020),
            Key::StudyInstanceUid => Tag(0x0020, 0x000D),
            Key::StudyId => Tag(0x0020, 0x0010),
            Key::StudyDescription => Tag(0x0008, 0x1030),
            Key::Modality => Tag(0x0008, 0x0060),
            Key::SeriesInstanceUid => Tag(0x0020, 0x000E),
            Key::SeriesNumber => Tag(0x0020, 0x0011),
            Key::PpsStartDate => Tag(0x0040, 0x0244),
            Key::PpsStartTime => Tag(0x0040, 0x0245),
            Key::SeriesDescription => Tag(0x0008, 0x103E),
            Key::SopClassUid => Tag(0x0008, 0x0016),
            Key::SopInstanceUid => Tag(0x0008, 0x0018),
            Key::InstanceNumber => Tag(0x0020, 0x0013),
            Key::RequestAttributeSequence => REQUEST_ATTRIBUTE_SEQUENCE,
        }
    }

    pub fn level(self) -> Level {
        match self {
            Key::StudyDate
            | Key::StudyTime
            | Key::AccessionNumber
            | Key::ModalitiesInStudy
            | Key::ReferringPhysicianName
            | Key::PatientName
            | Key::PatientId
            | Key::StudyInstanceUid
            | Key::StudyId
            | Key::StudyDescription => Level::Study,
            Key::Modality
            | Key::SeriesInstanceUid
            | Key::SeriesNumber
            | Key::PpsStartDate
            | Key::PpsStartTime
            | Key::SeriesDescription
            | Key::RequestAttributeSequence => Level::Series,
            Key::SopClassUid | Key::SopInstanceUid | Key::InstanceNumber => Level::Instance,
        }
    }

    fn kind(self) -> Kind {
        match self {
            Key::StudyInstanceUid
            | Key::SeriesInstanceUid
            | Key::SopClassUid
            | Key::SopInstanceUid => Kind::Uid,
            Key::StudyDate | Key::PpsStartDate => Kind::Date,
            Key::StudyTime | Key::PpsStartTime => Kind::Time,
            _ => Kind::Text,
        }
    }

    const ALL: [Key; 20] = [
        Key::StudyDate,
        Key::StudyTime,
        Key::AccessionNumber,
        Key::ModalitiesInStudy,
        Key::ReferringPhysicianName,
        Key::PatientName,
        Key::PatientId,
        Key::StudyInstanceUid,
        Key::StudyId,
        Key::StudyDescription,
        Key::Modality,
        Key::SeriesInstanceUid,
        Key::SeriesNumber,
        Key::PpsStartDate,
        Key::PpsStartTime,
        Key::SeriesDescription,
        Key::SopClassUid,
        Key::SopInstanceUid,
        Key::InstanceNumber,
        Key::RequestAttributeSequence,
    ];

    /// Resolve a query parameter name: a DICOM keyword or an 8-digit hex tag. A dotted path
    /// (`00400275.00401001`, `RequestAttributesSequence.RequestedProcedureID`) is accepted
    /// when it starts at Request Attribute Sequence; every part must be a valid tag or keyword.
    pub fn from_param(name: &str) -> Option<Key> {
        if name.contains('.') {
            let mut parts = name.split('.');
            if resolve_tag(parts.next()?)? != REQUEST_ATTRIBUTE_SEQUENCE {
                return None;
            }
            for part in parts {
                resolve_tag(part)?;
            }
            return Some(Key::RequestAttributeSequence);
        }
        let tag = resolve_tag(name)?;
        Key::ALL.into_iter().find(|k| k.tag() == tag)
    }
}

/// Resolve a DICOM keyword or an 8-digit hex tag to a tag.
fn resolve_tag(name: &str) -> Option<Tag> {
    if name.len() == 8 && name.bytes().all(|b| b.is_ascii_hexdigit()) {
        let group = u16::from_str_radix(&name[..4], 16).ok()?;
        let element = u16::from_str_radix(&name[4..], 16).ok()?;
        return Some(Tag(group, element));
    }
    let entry = StandardDataDictionary.by_name(name)?;
    match entry.tag_range() {
        TagRange::Single(tag) => Some(tag),
        _ => None,
    }
}

/// How one attribute is matched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Matcher {
    Universal,
    Single(String),
    List(Vec<String>),
    Wildcard(String),
    Range {
        from: Option<String>,
        to: Option<String>,
    },
}

/// Which attributes to return besides the defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Include {
    Default,
    All,
    Tags(Vec<Tag>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QidoQuery {
    pub level: Level,
    pub study_uid: Option<String>,
    pub series_uid: Option<String>,
    pub matches: Vec<(Key, Matcher)>,
    pub include: Include,
    pub limit: Option<u32>,
    pub offset: u32,
    pub fuzzy: bool,
    /// Query parameters that were ignored (unknown, or not a supported matching attribute at
    /// this level), as the client wrote them. The handler can report them in a `Warning`.
    pub ignored: Vec<String>,
}

/// A request error that maps to HTTP 400.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryError {
    pub param: String,
    pub reason: String,
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.param, self.reason)
    }
}

impl std::error::Error for QueryError {}

fn err(param: &str, reason: &str) -> QueryError {
    QueryError {
        param: param.to_string(),
        reason: reason.to_string(),
    }
}

fn decode(param: &str, s: &str) -> Result<String, QueryError> {
    percent_decode_str(s)
        .decode_utf8()
        .map(|c| c.into_owned())
        .map_err(|_| err(param, "invalid percent-encoding"))
}

/// Parse a QIDO-RS request. `raw_query` is the query string without the leading `?`.
pub fn parse_qido(
    level: Level,
    study_uid: Option<&str>,
    series_uid: Option<&str>,
    raw_query: &str,
) -> Result<QidoQuery, QueryError> {
    let mut query = QidoQuery {
        level,
        study_uid: study_uid.map(str::to_string),
        series_uid: series_uid.map(str::to_string),
        matches: Vec::new(),
        include: Include::Default,
        limit: None,
        offset: 0,
        fuzzy: false,
        ignored: Vec::new(),
    };
    let (mut seen_limit, mut seen_offset, mut seen_fuzzy) = (false, false, false);
    let mut include_tags: Vec<Tag> = Vec::new();
    let mut include_all = false;
    let mut seen_include = false;

    for pair in raw_query.split('&').filter(|p| !p.is_empty()) {
        let (raw_name, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
        let name = decode(raw_name, raw_name)?;
        let value = decode(&name, raw_value)?;
        match name.as_str() {
            "limit" => {
                if std::mem::replace(&mut seen_limit, true) {
                    return Err(err(&name, "repeated"));
                }
                query.limit = Some(value.parse().map_err(|_| err(&name, "invalid number"))?);
            }
            "offset" => {
                if std::mem::replace(&mut seen_offset, true) {
                    return Err(err(&name, "repeated"));
                }
                query.offset = value.parse().map_err(|_| err(&name, "invalid number"))?;
            }
            "fuzzymatching" => {
                if std::mem::replace(&mut seen_fuzzy, true) {
                    return Err(err(&name, "repeated"));
                }
                query.fuzzy = match value.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(err(&name, "must be true or false")),
                };
            }
            "includefield" => {
                seen_include = true;
                for item in value.split(',').map(str::trim).filter(|i| !i.is_empty()) {
                    if item == "all" {
                        include_all = true;
                    } else {
                        let tag =
                            resolve_tag(item).ok_or_else(|| err(&name, "unknown attribute"))?;
                        if !include_tags.contains(&tag) {
                            include_tags.push(tag);
                        }
                    }
                }
            }
            _ => match Key::from_param(&name) {
                Some(key) if key.level() <= level => {
                    if query.matches.iter().any(|(k, _)| *k == key) {
                        return Err(err(&name, "repeated"));
                    }
                    let matcher = parse_matcher(key, &name, &value)?;
                    query.matches.push((key, matcher));
                }
                // Unknown parameter, unsupported attribute, or a key below the query level.
                _ => query.ignored.push(name),
            },
        }
    }
    if seen_include {
        query.include = if include_all {
            Include::All
        } else {
            Include::Tags(include_tags)
        };
    }
    Ok(query)
}

fn parse_matcher(key: Key, param: &str, value: &str) -> Result<Matcher, QueryError> {
    if value.is_empty() {
        return Ok(Matcher::Universal);
    }
    match key.kind() {
        Kind::Uid => {
            if value.contains(',') {
                Ok(Matcher::List(
                    value.split(',').map(str::to_string).collect(),
                ))
            } else {
                Ok(Matcher::Single(value.to_string()))
            }
        }
        Kind::Date | Kind::Time => {
            let (check, what): (fn(&str) -> bool, &str) = if key.kind() == Kind::Date {
                (valid_date, "invalid date")
            } else {
                (valid_time, "invalid time")
            };
            if let Some((from, to)) = value.split_once('-') {
                let end = |s: &str| -> Result<Option<String>, QueryError> {
                    if s.is_empty() {
                        Ok(None)
                    } else if check(s) {
                        Ok(Some(s.to_string()))
                    } else {
                        Err(err(param, what))
                    }
                };
                let (from, to) = (end(from)?, end(to)?);
                if from.is_none() && to.is_none() {
                    return Err(err(param, what));
                }
                Ok(Matcher::Range { from, to })
            } else if check(value) {
                Ok(Matcher::Single(value.to_string()))
            } else {
                Err(err(param, what))
            }
        }
        Kind::Text => {
            if value.contains(['*', '?']) {
                Ok(Matcher::Wildcard(value.to_string()))
            } else {
                Ok(Matcher::Single(value.to_string()))
            }
        }
    }
}

/// `YYYYMMDD` with a real calendar date.
// `is_multiple_of` needs Rust 1.90; the MSRV is 1.88.
#[allow(clippy::manual_is_multiple_of)]
fn valid_date(s: &str) -> bool {
    if s.len() != 8 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    let year: u32 = s[..4].parse().unwrap_or(0);
    let month: u32 = s[4..6].parse().unwrap_or(0);
    let day: u32 = s[6..].parse().unwrap_or(0);
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    (1..=days).contains(&day)
}

/// `HH[MM[SS[.F{1,6}]]]`.
fn valid_time(s: &str) -> bool {
    let (main, fraction) = match s.split_once('.') {
        Some((m, f)) => (m, Some(f)),
        None => (s, None),
    };
    if !matches!(main.len(), 2 | 4 | 6) || !main.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    if let Some(f) = fraction
        && (main.len() != 6
            || !(1..=6).contains(&f.len())
            || !f.bytes().all(|b| b.is_ascii_digit()))
    {
        return false;
    }
    let part = |i: usize| -> u32 { main[i..i + 2].parse().unwrap_or(99) };
    part(0) <= 23 && (main.len() < 4 || part(2) <= 59) && (main.len() < 6 || part(4) <= 60)
}

const STUDY_FIELDS: &[Tag] = &[
    Tag(0x0008, 0x0005),
    Tag(0x0008, 0x0020),
    Tag(0x0008, 0x0030),
    Tag(0x0008, 0x0050),
    Tag(0x0008, 0x0056),
    Tag(0x0008, 0x0061),
    Tag(0x0008, 0x0090),
    Tag(0x0008, 0x0201),
    Tag(0x0008, 0x1190),
    Tag(0x0010, 0x0010),
    Tag(0x0010, 0x0020),
    Tag(0x0010, 0x0030),
    Tag(0x0010, 0x0040),
    Tag(0x0020, 0x000D),
    Tag(0x0020, 0x0010),
    Tag(0x0020, 0x1206),
    Tag(0x0020, 0x1208),
];

const SERIES_FIELDS: &[Tag] = &[
    Tag(0x0008, 0x0005),
    Tag(0x0008, 0x0060),
    Tag(0x0008, 0x0201),
    Tag(0x0008, 0x103E),
    Tag(0x0008, 0x1190),
    Tag(0x0020, 0x000E),
    Tag(0x0020, 0x0011),
    Tag(0x0020, 0x1209),
    Tag(0x0040, 0x0244),
    Tag(0x0040, 0x0245),
    Tag(0x0040, 0x0275),
];

const INSTANCE_FIELDS: &[Tag] = &[
    Tag(0x0008, 0x0005),
    Tag(0x0008, 0x0016),
    Tag(0x0008, 0x0018),
    Tag(0x0008, 0x0056),
    Tag(0x0008, 0x0201),
    Tag(0x0008, 0x1190),
    Tag(0x0020, 0x0013),
    Tag(0x0028, 0x0008),
    Tag(0x0028, 0x0010),
    Tag(0x0028, 0x0011),
    Tag(0x0028, 0x0100),
];

/// PS3.18 default return attributes for each level (Tables 10.6.3-2 to 10.6.3-4 style).
pub fn default_fields(level: Level) -> &'static [Tag] {
    match level {
        Level::Study => STUDY_FIELDS,
        Level::Series => SERIES_FIELDS,
        Level::Instance => INSTANCE_FIELDS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(level: Level, raw: &str) -> Result<QidoQuery, QueryError> {
        parse_qido(level, None, None, raw)
    }

    fn matcher(query: &QidoQuery, key: Key) -> &Matcher {
        &query.matches.iter().find(|(k, _)| *k == key).unwrap().1
    }

    #[test]
    fn parse_patient_name_wildcard() {
        let r = q(Level::Study, "PatientName=*smith*").unwrap();
        assert_eq!(
            matcher(&r, Key::PatientName),
            &Matcher::Wildcard("*smith*".into())
        );
        let r = q(Level::Study, "PatientName=smith").unwrap();
        assert_eq!(
            matcher(&r, Key::PatientName),
            &Matcher::Single("smith".into())
        );
        let r = q(Level::Study, "PatientName=").unwrap();
        assert_eq!(matcher(&r, Key::PatientName), &Matcher::Universal);
    }

    #[test]
    fn parse_uid_list() {
        let r = q(Level::Instance, "SOPInstanceUID=1.2.3,1.2.4").unwrap();
        assert_eq!(
            matcher(&r, Key::SopInstanceUid),
            &Matcher::List(vec!["1.2.3".into(), "1.2.4".into()])
        );
        let r = q(Level::Instance, "SOPInstanceUID=1.2.3").unwrap();
        assert_eq!(
            matcher(&r, Key::SopInstanceUid),
            &Matcher::Single("1.2.3".into())
        );
    }

    #[test]
    fn parse_date_range_open_ends() {
        let r = q(Level::Study, "StudyDate=-20260101").unwrap();
        assert_eq!(
            matcher(&r, Key::StudyDate),
            &Matcher::Range {
                from: None,
                to: Some("20260101".into())
            }
        );
        let r = q(Level::Study, "StudyDate=20250101-").unwrap();
        assert_eq!(
            matcher(&r, Key::StudyDate),
            &Matcher::Range {
                from: Some("20250101".into()),
                to: None
            }
        );
        let r = q(Level::Study, "00080020=20200101-20201231").unwrap();
        assert_eq!(
            matcher(&r, Key::StudyDate),
            &Matcher::Range {
                from: Some("20200101".into()),
                to: Some("20201231".into())
            }
        );
    }

    #[test]
    fn parse_time_partial() {
        for t in ["10", "1030", "103045", "103045.5", "103045.123456"] {
            let r = q(Level::Study, &format!("StudyTime={t}")).unwrap();
            assert_eq!(matcher(&r, Key::StudyTime), &Matcher::Single(t.into()));
        }
        let r = q(Level::Study, "StudyTime=0800-1700").unwrap();
        assert_eq!(
            matcher(&r, Key::StudyTime),
            &Matcher::Range {
                from: Some("0800".into()),
                to: Some("1700".into())
            }
        );
        let e = q(Level::Study, "StudyTime=25").unwrap_err();
        assert_eq!(e.reason, "invalid time");
        assert!(q(Level::Study, "StudyTime=103045.1234567").is_err());
    }

    #[test]
    fn parse_invalid_date_400() {
        let e = q(Level::Study, "StudyDate=2026-01-01").unwrap_err();
        assert_eq!(e.param, "StudyDate");
        assert_eq!(e.reason, "invalid date");
        assert_eq!(e.to_string(), "StudyDate: invalid date");
        assert!(q(Level::Study, "StudyDate=20261301").is_err());
        assert!(q(Level::Study, "StudyDate=2026").is_err());
    }

    #[test]
    fn parse_includefield_keyword_tag_and_all() {
        let r = q(
            Level::Study,
            "includefield=PatientSex,00080060&includefield=StudyDescription",
        )
        .unwrap();
        assert_eq!(
            r.include,
            Include::Tags(vec![
                Tag(0x0010, 0x0040),
                Tag(0x0008, 0x0060),
                Tag(0x0008, 0x1030)
            ])
        );
        let r = q(Level::Study, "includefield=all").unwrap();
        assert_eq!(r.include, Include::All);
        assert_eq!(q(Level::Study, "").unwrap().include, Include::Default);
        let e = q(Level::Study, "includefield=NotAKeyword").unwrap_err();
        assert_eq!(e.param, "includefield");
    }

    #[test]
    fn parse_limit_offset_fuzzy() {
        let r = q(Level::Study, "limit=10&offset=20&fuzzymatching=true").unwrap();
        assert_eq!((r.limit, r.offset, r.fuzzy), (Some(10), 20, true));
        let r = q(Level::Study, "").unwrap();
        assert_eq!((r.limit, r.offset, r.fuzzy), (None, 0, false));
        assert_eq!(q(Level::Study, "limit=abc").unwrap_err().param, "limit");
        assert_eq!(q(Level::Study, "offset=-1").unwrap_err().param, "offset");
        assert_eq!(
            q(Level::Study, "fuzzymatching=maybe").unwrap_err().param,
            "fuzzymatching"
        );
    }

    #[test]
    fn parse_plus_is_literal() {
        let r = q(Level::Study, "PatientName=a+b").unwrap();
        assert_eq!(
            matcher(&r, Key::PatientName),
            &Matcher::Single("a+b".into())
        );
    }

    #[test]
    fn parse_percent_decoding() {
        let r = q(Level::Study, "PatientName=Sm%C3%BCth%2A").unwrap();
        assert_eq!(
            matcher(&r, Key::PatientName),
            &Matcher::Wildcard("Smüth*".into())
        );
        let e = q(Level::Study, "PatientName=%FF").unwrap_err();
        assert_eq!(e.param, "PatientName");
    }

    #[test]
    fn parse_unknown_param_is_ignored() {
        let r = q(Level::Study, "foo=bar&PatientID=1").unwrap();
        assert_eq!(r.ignored, vec!["foo".to_string()]);
        assert_eq!(r.matches.len(), 1);
    }

    #[test]
    fn parse_unsupported_attribute_is_ignored() {
        let r = q(Level::Study, "PatientSex=M&00100030=2000").unwrap();
        assert_eq!(
            r.ignored,
            vec!["PatientSex".to_string(), "00100030".to_string()]
        );
        assert!(r.matches.is_empty());
    }

    #[test]
    fn parse_lower_level_key_is_ignored() {
        let r = q(Level::Study, "Modality=CT").unwrap();
        assert_eq!(r.ignored, vec!["Modality".to_string()]);
        assert!(r.matches.is_empty());
    }

    #[test]
    fn parse_higher_level_key_allowed() {
        let r = q(Level::Series, "PatientID=P1").unwrap();
        assert_eq!(matcher(&r, Key::PatientId), &Matcher::Single("P1".into()));
        assert!(r.ignored.is_empty());
    }

    #[test]
    fn parse_repeated_key() {
        let e = q(Level::Study, "PatientID=1&PatientID=2").unwrap_err();
        assert_eq!(
            (e.param.as_str(), e.reason.as_str()),
            ("PatientID", "repeated")
        );
        let e = q(Level::Study, "PatientID=1&00100020=2").unwrap_err();
        assert_eq!(e.reason, "repeated");
        assert_eq!(
            q(Level::Study, "limit=1&limit=2").unwrap_err().reason,
            "repeated"
        );
    }

    #[test]
    fn parse_path_uids() {
        let r = parse_qido(Level::Instance, Some("1.2"), Some("1.2.3"), "").unwrap();
        assert_eq!(r.study_uid.as_deref(), Some("1.2"));
        assert_eq!(r.series_uid.as_deref(), Some("1.2.3"));
        assert_eq!(r.level, Level::Instance);
    }

    #[test]
    fn parse_request_attribute_sequence_key() {
        let r = q(Level::Series, "00400275.00401001=ABC").unwrap();
        assert_eq!(
            matcher(&r, Key::RequestAttributeSequence),
            &Matcher::Single("ABC".into())
        );
        assert_eq!(
            Key::from_param("RequestAttributesSequence.RequestedProcedureID"),
            Some(Key::RequestAttributeSequence)
        );
        assert_eq!(
            Key::from_param("00400275"),
            Some(Key::RequestAttributeSequence)
        );
        assert_eq!(Key::from_param("PatientName.00401001"), None);
    }

    #[test]
    fn parse_ohif_study_list_query() {
        let r = q(
            Level::Study,
            "limit=101&offset=0&fuzzymatching=false&includefield=00081030%2C00080060%2C00080090%2C00100030",
        )
        .unwrap();
        assert_eq!(r.limit, Some(101));
        assert_eq!(r.offset, 0);
        assert!(!r.fuzzy);
        assert_eq!(
            r.include,
            Include::Tags(vec![
                Tag(0x0008, 0x1030),
                Tag(0x0008, 0x0060),
                Tag(0x0008, 0x0090),
                Tag(0x0010, 0x0030)
            ])
        );
        assert!(r.matches.is_empty() && r.ignored.is_empty());
    }

    #[test]
    fn key_levels_and_tags() {
        assert!(Level::Study < Level::Series && Level::Series < Level::Instance);
        assert_eq!(Key::ModalitiesInStudy.level(), Level::Study);
        assert_eq!(Key::Modality.level(), Level::Series);
        assert_eq!(Key::InstanceNumber.level(), Level::Instance);
        assert_eq!(Key::StudyInstanceUid.tag(), Tag(0x0020, 0x000D));
        assert_eq!(Key::from_param("PatientID"), Some(Key::PatientId));
        assert_eq!(Key::from_param("0020000d"), Some(Key::StudyInstanceUid));
    }

    #[test]
    fn default_fields_study_contains_counts() {
        let f = default_fields(Level::Study);
        assert!(f.contains(&Tag(0x0020, 0x1206)));
        assert!(f.contains(&Tag(0x0020, 0x1208)));
        assert!(default_fields(Level::Series).contains(&Tag(0x0020, 0x1209)));
        assert!(default_fields(Level::Instance).contains(&Tag(0x0008, 0x0018)));
    }
}
