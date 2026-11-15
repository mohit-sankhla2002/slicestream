//! DICOM JSON (PS3.18 Annex F) for QIDO-RS result rows: attributes the
//! index holds, and attributes picked from stored metadata.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::value::RawValue;
use serde_json::{Value, json};
use slicestream_dicomweb::{Level, instance_url, series_url, study_url};
use slicestream_store::QidoRow;

const SPECIFIC_CHARACTER_SET: u32 = 0x0008_0005;
const SOP_CLASS_UID: u32 = 0x0008_0016;
const SOP_INSTANCE_UID: u32 = 0x0008_0018;
const STUDY_DATE: u32 = 0x0008_0020;
const STUDY_TIME: u32 = 0x0008_0030;
const ACCESSION_NUMBER: u32 = 0x0008_0050;
const INSTANCE_AVAILABILITY: u32 = 0x0008_0056;
const MODALITY: u32 = 0x0008_0060;
const MODALITIES_IN_STUDY: u32 = 0x0008_0061;
const REFERRING_PHYSICIAN_NAME: u32 = 0x0008_0090;
const STUDY_DESCRIPTION: u32 = 0x0008_1030;
const SERIES_DESCRIPTION: u32 = 0x0008_103E;
const RETRIEVE_URL: u32 = 0x0008_1190;
const PATIENT_NAME: u32 = 0x0010_0010;
const PATIENT_ID: u32 = 0x0010_0020;
const PATIENT_BIRTH_DATE: u32 = 0x0010_0030;
const PATIENT_SEX: u32 = 0x0010_0040;
pub(super) const STUDY_INSTANCE_UID: u32 = 0x0020_000D;
pub(super) const SERIES_INSTANCE_UID: u32 = 0x0020_000E;
const STUDY_ID: u32 = 0x0020_0010;
const SERIES_NUMBER: u32 = 0x0020_0011;
const INSTANCE_NUMBER: u32 = 0x0020_0013;
const NUMBER_OF_STUDY_RELATED_SERIES: u32 = 0x0020_1206;
const NUMBER_OF_STUDY_RELATED_INSTANCES: u32 = 0x0020_1208;
const NUMBER_OF_SERIES_RELATED_INSTANCES: u32 = 0x0020_1209;
const NUMBER_OF_FRAMES: u32 = 0x0028_0008;
const ROWS: u32 = 0x0028_0010;
const COLUMNS: u32 = 0x0028_0011;
const BITS_ALLOCATED: u32 = 0x0028_0100;
const PPS_START_DATE: u32 = 0x0040_0244;
const PPS_START_TIME: u32 = 0x0040_0245;

/// Which attributes to take from a representative instance's metadata.
pub(super) enum Wanted {
    Nothing,
    Tags(BTreeSet<u32>),
    /// Every attribute except bulk values (`includefield=all` at instance
    /// level).
    Everything,
}

impl Wanted {
    fn contains(&self, tag: u32) -> bool {
        match self {
            Wanted::Nothing => false,
            Wanted::Tags(tags) => tags.contains(&tag),
            Wanted::Everything => true,
        }
    }
}

/// Whether the index holds `tag` for rows of `level`.
pub(super) fn indexed(level: Level, tag: u32) -> bool {
    const STUDY: &[u32] = &[
        SPECIFIC_CHARACTER_SET,
        STUDY_DATE,
        STUDY_TIME,
        ACCESSION_NUMBER,
        INSTANCE_AVAILABILITY,
        MODALITIES_IN_STUDY,
        REFERRING_PHYSICIAN_NAME,
        STUDY_DESCRIPTION,
        RETRIEVE_URL,
        PATIENT_NAME,
        PATIENT_ID,
        PATIENT_BIRTH_DATE,
        PATIENT_SEX,
        STUDY_INSTANCE_UID,
        STUDY_ID,
        NUMBER_OF_STUDY_RELATED_SERIES,
        NUMBER_OF_STUDY_RELATED_INSTANCES,
    ];
    const SERIES: &[u32] = &[
        MODALITY,
        SERIES_DESCRIPTION,
        SERIES_INSTANCE_UID,
        SERIES_NUMBER,
        NUMBER_OF_SERIES_RELATED_INSTANCES,
        PPS_START_DATE,
        PPS_START_TIME,
    ];
    const INSTANCE: &[u32] = &[
        SOP_CLASS_UID,
        SOP_INSTANCE_UID,
        INSTANCE_NUMBER,
        NUMBER_OF_FRAMES,
        ROWS,
        COLUMNS,
        BITS_ALLOCATED,
    ];
    STUDY.contains(&tag)
        || (level >= Level::Series && SERIES.contains(&tag))
        || (level == Level::Instance && INSTANCE.contains(&tag))
}

/// `tag` of `row` from the index, as DICOM JSON. `None` when the index does
/// not hold it for this row. A value the index holds but that is empty gives
/// an attribute without `Value`.
pub(super) fn index_value(tag: u32, row: &QidoRow, base: &str) -> Option<Value> {
    let st = &row.study;
    let series = row.series.as_ref();
    let instance = row.instance.as_ref();
    let frames = || instance.and_then(|i| i.frame_info.as_ref());
    Some(match tag {
        SPECIFIC_CHARACTER_SET => text("CS", Some("ISO_IR 192")),
        INSTANCE_AVAILABILITY => text("CS", Some("ONLINE")),
        RETRIEVE_URL => text("UR", Some(retrieve_url(row, base).as_str())),
        STUDY_DATE => text("DA", st.study_date.as_deref()),
        STUDY_TIME => text("TM", st.study_time.as_deref()),
        ACCESSION_NUMBER => text("SH", st.accession_number.as_deref()),
        MODALITIES_IN_STUDY => {
            let values: Vec<&str> = st.modalities.iter().map(String::as_str).collect();
            values_attr("CS", json!(values), values.is_empty())
        }
        REFERRING_PHYSICIAN_NAME => person_name(st.referring_physician_name.as_deref()),
        STUDY_DESCRIPTION => text("LO", st.study_description.as_deref()),
        PATIENT_NAME => person_name(st.patient_name.as_deref()),
        PATIENT_ID => text("LO", Some(st.patient_id.as_str())),
        PATIENT_BIRTH_DATE => text("DA", st.patient_birth_date.as_deref()),
        PATIENT_SEX => text("CS", st.patient_sex.as_deref()),
        STUDY_INSTANCE_UID => text("UI", Some(st.study_uid.as_str())),
        STUDY_ID => text("SH", st.study_id.as_deref()),
        NUMBER_OF_STUDY_RELATED_SERIES => number("IS", Some(st.series_count)),
        NUMBER_OF_STUDY_RELATED_INSTANCES => number("IS", Some(st.instance_count)),
        MODALITY => text("CS", series?.modality.as_deref()),
        SERIES_DESCRIPTION => text("LO", series?.series_description.as_deref()),
        SERIES_INSTANCE_UID => text("UI", Some(series?.series_uid.as_str())),
        SERIES_NUMBER => number("IS", series?.series_number),
        NUMBER_OF_SERIES_RELATED_INSTANCES => number("IS", Some(series?.instance_count)),
        PPS_START_DATE => text("DA", series?.pps_start_date.as_deref()),
        PPS_START_TIME => text("TM", series?.pps_start_time.as_deref()),
        SOP_CLASS_UID => text("UI", Some(instance?.sop_class_uid.as_str())),
        SOP_INSTANCE_UID => text("UI", Some(instance?.sop_uid.as_str())),
        INSTANCE_NUMBER => number("IS", instance?.instance_number),
        // Image attributes exist only for images.
        NUMBER_OF_FRAMES => {
            frames()?;
            number("IS", Some(i64::from(instance?.number_of_frames)))
        }
        ROWS => number("US", Some(i64::from(frames()?.rows))),
        COLUMNS => number("US", Some(i64::from(frames()?.columns))),
        BITS_ALLOCATED => number("US", Some(i64::from(frames()?.bits_allocated))),
        _ => return None,
    })
}

fn retrieve_url(row: &QidoRow, base: &str) -> String {
    let study = &row.study.study_uid;
    match (&row.series, &row.instance) {
        (Some(s), Some(i)) => instance_url(base, study, &s.series_uid, &i.sop_uid),
        (Some(s), None) => series_url(base, study, &s.series_uid),
        _ => study_url(base, study),
    }
}

fn values_attr(vr: &str, values: Value, empty: bool) -> Value {
    if empty {
        json!({ "vr": vr })
    } else {
        json!({ "vr": vr, "Value": values })
    }
}

fn text(vr: &str, value: Option<&str>) -> Value {
    match value.filter(|v| !v.is_empty()) {
        Some(v) => values_attr(vr, json!([v]), false),
        None => values_attr(vr, Value::Null, true),
    }
}

fn number(vr: &str, value: Option<i64>) -> Value {
    match value {
        Some(n) => values_attr(vr, json!([n]), false),
        None => values_attr(vr, Value::Null, true),
    }
}

/// PN as PS3.18 F.2.2: component groups `Alphabetic=Ideographic=Phonetic`.
fn person_name(value: Option<&str>) -> Value {
    let mut groups = serde_json::Map::new();
    for (name, group) in ["Alphabetic", "Ideographic", "Phonetic"]
        .iter()
        .zip(value.unwrap_or("").split('='))
    {
        let group = group.trim_end_matches([' ', '\0']);
        if !group.is_empty() {
            groups.insert((*name).to_owned(), json!(group));
        }
    }
    let empty = groups.is_empty();
    values_attr("PN", json!([groups]), empty)
}

/// The attributes of a metadata object that `want` asks for, leaving out
/// any that hold a bulk value (also inside a sequence): QIDO-RS returns no
/// BulkDataURIs.
pub(super) fn pick(id: i64, json: &[u8], want: &Wanted) -> BTreeMap<u32, String> {
    let attrs: BTreeMap<&str, &RawValue> = match serde_json::from_slice(json) {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!(instance = id, "stored metadata is not a JSON object: {e}");
            return BTreeMap::new();
        }
    };
    attrs
        .into_iter()
        .filter_map(|(key, value)| {
            let tag = parse_tag(key)?;
            (want.contains(tag) && !value.get().contains("\"BulkDataURI\":"))
                .then(|| (tag, value.get().to_owned()))
        })
        .collect()
}

fn parse_tag(key: &str) -> Option<u32> {
    (key.len() == 8)
        .then(|| u32::from_str_radix(key, 16).ok())
        .flatten()
}

/// One DICOM JSON object, attributes in tag order.
pub(super) fn write_object(out: &mut Vec<u8>, attrs: &BTreeMap<u32, String>) {
    out.push(b'{');
    for (i, (tag, value)) in attrs.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(format!("\"{tag:08X}\":").as_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    out.push(b'}');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_drops_bulk_and_unwanted() {
        let json = br#"{"00080021":{"vr":"DA","Value":["20260106"]},
            "00091010":{"vr":"OB","BulkDataURI":"bulk/00091010"},
            "00400275":{"vr":"SQ","Value":[{"00091011":{"vr":"OB","BulkDataURI":"bulk/x"}}]},
            "00100010":{"vr":"PN","Value":[{"Alphabetic":"say \"BulkDataURI\": no"}]}}"#;
        let all = pick(1, json, &Wanted::Everything);
        assert_eq!(
            all.keys().copied().collect::<Vec<_>>(),
            [0x0008_0021, 0x0010_0010]
        );
        let some = pick(1, json, &Wanted::Tags([0x0008_0021].into()));
        assert_eq!(some[&0x0008_0021], r#"{"vr":"DA","Value":["20260106"]}"#);
        assert_eq!(some.len(), 1);
        assert!(pick(1, b"not json", &Wanted::Everything).is_empty());
    }

    #[test]
    fn person_name_groups() {
        assert_eq!(
            person_name(Some("Yamada^Tarou=山田^太郎=")),
            json!({"vr":"PN","Value":[{"Alphabetic":"Yamada^Tarou","Ideographic":"山田^太郎"}]})
        );
        assert_eq!(person_name(None), json!({"vr":"PN"}));
        assert_eq!(person_name(Some("")), json!({"vr":"PN"}));
    }
}
