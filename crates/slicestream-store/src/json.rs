//! Streaming writer for DICOM JSON (PS3.18 Annex F).
//!
//! Elements arrive in data set order. The writer appends straight to one
//! byte buffer, so a caller can watch `len()` and stop early.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use dicom_core::{PrimitiveValue, Tag, VR};

/// Largest magnitude a JSON number may carry without losing integer precision.
const MAX_SAFE_INT: i128 = 1 << 53;

// Narrow allow: the scanner in the next task is the first caller.
#[allow(dead_code)]
pub(crate) struct DicomJsonWriter {
    buf: Vec<u8>,
    /// One entry per open object or array: true while nothing was written in it.
    first: Vec<bool>,
}

#[allow(dead_code)]
impl DicomJsonWriter {
    pub(crate) fn new() -> Self {
        Self {
            buf: vec![b'{'],
            first: vec![true],
        }
    }

    /// Bytes written so far.
    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }

    /// Close the top-level object and return the JSON text.
    pub(crate) fn finish(mut self) -> Vec<u8> {
        while self.first.pop().is_some() {
            self.buf.push(b'}');
        }
        self.buf
    }

    pub(crate) fn element(&mut self, tag: Tag, vr: VR, value: &PrimitiveValue) {
        self.key(tag);
        self.buf.extend_from_slice(b"{\"vr\":\"");
        self.buf.extend_from_slice(vr.to_string().as_bytes());
        self.buf.push(b'"');
        match vr {
            VR::OB | VR::OW | VR::OD | VR::OF | VR::OL | VR::OV | VR::UN => {
                let bytes = value_bytes(value);
                if !bytes.is_empty() {
                    self.buf.extend_from_slice(b",\"InlineBinary\":\"");
                    self.buf
                        .extend_from_slice(STANDARD.encode(bytes).as_bytes());
                    self.buf.push(b'"');
                }
            }
            VR::SQ => {}
            _ => self.values(vr, value),
        }
        self.buf.push(b'}');
    }

    pub(crate) fn bulk(&mut self, tag: Tag, vr: VR, path: &str) {
        self.key(tag);
        self.buf.extend_from_slice(b"{\"vr\":\"");
        self.buf.extend_from_slice(vr.to_string().as_bytes());
        self.buf.extend_from_slice(b"\",\"BulkDataURI\":");
        self.string(&format!("bulk/{path}"));
        self.buf.push(b'}');
    }

    pub(crate) fn begin_sequence(&mut self, tag: Tag) {
        self.key(tag);
        self.buf.extend_from_slice(b"{\"vr\":\"SQ\",\"Value\":[");
        self.first.push(true);
    }

    pub(crate) fn begin_item(&mut self) {
        self.comma();
        self.buf.push(b'{');
        self.first.push(true);
    }

    pub(crate) fn end_item(&mut self) {
        self.first.pop();
        self.buf.push(b'}');
    }

    pub(crate) fn end_sequence(&mut self) {
        self.first.pop();
        self.buf.extend_from_slice(b"]}");
    }

    fn comma(&mut self) {
        if let Some(first) = self.first.last_mut() {
            if !*first {
                self.buf.push(b',');
            }
            *first = false;
        }
    }

    fn key(&mut self, tag: Tag) {
        self.comma();
        self.buf
            .extend_from_slice(format!("\"{:04X}{:04X}\":", tag.group(), tag.element()).as_bytes());
    }

    fn values(&mut self, vr: VR, value: &PrimitiveValue) {
        let items = match items(vr, value) {
            Some(items) if !items.is_empty() => items,
            _ => return,
        };
        self.buf.extend_from_slice(b",\"Value\":[");
        for (i, item) in items.iter().enumerate() {
            if i > 0 {
                self.buf.push(b',');
            }
            self.item(vr, item);
        }
        self.buf.push(b']');
    }

    fn item(&mut self, vr: VR, item: &Item) {
        match item {
            Item::Text(s) if s.is_empty() => self.buf.extend_from_slice(b"null"),
            Item::Text(s) => match vr {
                VR::PN => self.person_name(s),
                VR::IS => match s.parse::<i64>() {
                    Ok(n) => self.buf.extend_from_slice(n.to_string().as_bytes()),
                    Err(_) => self.string(s),
                },
                VR::DS => match s.parse::<f64>() {
                    Ok(f) if f.is_finite() => self.buf.extend_from_slice(f.to_string().as_bytes()),
                    _ => self.string(s),
                },
                _ => self.string(s),
            },
            Item::Int(n) => {
                if matches!(vr, VR::SV | VR::UV) && n.abs() > MAX_SAFE_INT {
                    self.string(&n.to_string());
                } else {
                    self.buf.extend_from_slice(n.to_string().as_bytes());
                }
            }
            Item::Float(f) if f.is_finite() => self.buf.extend_from_slice(f.to_string().as_bytes()),
            Item::Float(_) => self.buf.extend_from_slice(b"null"),
        }
    }

    fn person_name(&mut self, s: &str) {
        let mut parts = Vec::new();
        for (name, group) in ["Alphabetic", "Ideographic", "Phonetic"]
            .iter()
            .zip(s.split('='))
        {
            let group = group.trim_end_matches([' ', '\0']);
            if !group.is_empty() {
                parts.push((*name, group));
            }
        }
        if parts.is_empty() {
            self.buf.extend_from_slice(b"null");
            return;
        }
        self.buf.push(b'{');
        for (i, (name, group)) in parts.iter().enumerate() {
            if i > 0 {
                self.buf.push(b',');
            }
            self.string(name);
            self.buf.push(b':');
            self.string(group);
        }
        self.buf.push(b'}');
    }

    fn string(&mut self, s: &str) {
        self.buf.push(b'"');
        for c in s.chars() {
            match c {
                '"' => self.buf.extend_from_slice(b"\\\""),
                '\\' => self.buf.extend_from_slice(b"\\\\"),
                '\n' => self.buf.extend_from_slice(b"\\n"),
                '\r' => self.buf.extend_from_slice(b"\\r"),
                '\t' => self.buf.extend_from_slice(b"\\t"),
                c if (c as u32) < 0x20 => {
                    self.buf
                        .extend_from_slice(format!("\\u{:04x}", c as u32).as_bytes());
                }
                c => {
                    let mut tmp = [0u8; 4];
                    self.buf
                        .extend_from_slice(c.encode_utf8(&mut tmp).as_bytes());
                }
            }
        }
        self.buf.push(b'"');
    }
}

enum Item {
    Text(String),
    Int(i128),
    Float(f64),
}

/// Split a primitive value into JSON items. `None` means the value has no content.
fn items(vr: VR, value: &PrimitiveValue) -> Option<Vec<Item>> {
    use PrimitiveValue as P;
    let ints = |v: Vec<i128>| Some(v.into_iter().map(Item::Int).collect());
    match value {
        P::Empty => None,
        P::U8(v) => ints(v.iter().map(|&x| x.into()).collect()),
        P::I16(v) => ints(v.iter().map(|&x| x.into()).collect()),
        P::U16(v) => ints(v.iter().map(|&x| x.into()).collect()),
        P::I32(v) => ints(v.iter().map(|&x| x.into()).collect()),
        P::U32(v) => ints(v.iter().map(|&x| x.into()).collect()),
        P::I64(v) => ints(v.iter().map(|&x| x.into()).collect()),
        P::U64(v) => ints(v.iter().map(|&x| x.into()).collect()),
        P::F32(v) => Some(v.iter().map(|&x| Item::Float(x.into())).collect()),
        P::F64(v) => Some(v.iter().map(|&x| Item::Float(x)).collect()),
        P::Tags(v) => Some(
            v.iter()
                .map(|t| Item::Text(format!("{:04X}{:04X}", t.group(), t.element())))
                .collect(),
        ),
        P::Str(_) | P::Strs(_) | P::Date(_) | P::DateTime(_) | P::Time(_) => {
            let raw: Vec<String> = match value {
                P::Str(s) => vec![s.clone()],
                P::Strs(v) => v.iter().cloned().collect(),
                other => other.to_multi_str().to_vec(),
            };
            Some(text_items(vr, raw))
        }
    }
}

fn text_items(vr: VR, raw: Vec<String>) -> Vec<Item> {
    let single = matches!(vr, VR::LT | VR::ST | VR::UT | VR::UR);
    let keep_leading = matches!(vr, VR::LT | VR::ST | VR::UT);
    let mut out = Vec::new();
    for piece in raw {
        let parts: Vec<&str> = if single {
            vec![piece.as_str()]
        } else {
            piece.split('\\').collect()
        };
        for p in parts {
            let p = p.trim_end_matches([' ', '\0']);
            let p = if keep_leading { p } else { p.trim_start() };
            out.push(Item::Text(p.to_string()));
        }
    }
    // A lone empty string is an empty value.
    if out.len() == 1 && matches!(&out[0], Item::Text(s) if s.is_empty()) {
        out.clear();
    }
    out
}

/// Raw bytes of a binary value, little endian as DICOM stores them.
fn value_bytes(value: &PrimitiveValue) -> Vec<u8> {
    use PrimitiveValue as P;
    match value {
        P::Empty => Vec::new(),
        P::U8(v) => v.to_vec(),
        P::I16(v) => v.iter().flat_map(|x| x.to_le_bytes()).collect(),
        P::U16(v) => v.iter().flat_map(|x| x.to_le_bytes()).collect(),
        P::I32(v) => v.iter().flat_map(|x| x.to_le_bytes()).collect(),
        P::U32(v) => v.iter().flat_map(|x| x.to_le_bytes()).collect(),
        P::I64(v) => v.iter().flat_map(|x| x.to_le_bytes()).collect(),
        P::U64(v) => v.iter().flat_map(|x| x.to_le_bytes()).collect(),
        P::F32(v) => v.iter().flat_map(|x| x.to_le_bytes()).collect(),
        P::F64(v) => v.iter().flat_map(|x| x.to_le_bytes()).collect(),
        other => other.to_bytes().into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn one(tag: Tag, vr: VR, v: PrimitiveValue) -> Value {
        let mut w = DicomJsonWriter::new();
        w.element(tag, vr, &v);
        serde_json::from_slice(&w.finish()).expect("valid json")
    }

    const T: Tag = Tag(0x0010, 0x0010);

    #[test]
    fn json_pn_groups() {
        let v = one(
            T,
            VR::PN,
            PrimitiveValue::from("Yamada^Tarou=山田^太郎=やまだ^たろう"),
        );
        assert_eq!(
            v["00100010"],
            json!({"vr":"PN","Value":[{"Alphabetic":"Yamada^Tarou","Ideographic":"山田^太郎","Phonetic":"やまだ^たろう"}]})
        );
    }

    #[test]
    fn json_pn_alphabetic_only() {
        let v = one(T, VR::PN, PrimitiveValue::from("Doe^John "));
        assert_eq!(
            v["00100010"],
            json!({"vr":"PN","Value":[{"Alphabetic":"Doe^John"}]})
        );
    }

    #[test]
    fn json_at() {
        let v = one(
            Tag(0x0020, 0x5000),
            VR::AT,
            PrimitiveValue::Tags(
                [Tag(0x0008, 0x0018), Tag(0x7FE0, 0x0010)]
                    .into_iter()
                    .collect(),
            ),
        );
        assert_eq!(
            v["00205000"],
            json!({"vr":"AT","Value":["00080018","7FE00010"]})
        );
    }

    #[test]
    fn json_is_ds_numbers() {
        let is = one(Tag(0x0020, 0x0013), VR::IS, PrimitiveValue::from("12 "));
        assert_eq!(is["00200013"], json!({"vr":"IS","Value":[12]}));
        let ds = one(
            Tag(0x0018, 0x0050),
            VR::DS,
            PrimitiveValue::Strs(
                [" 1.5 ".to_string(), "-0.25E2".to_string()]
                    .into_iter()
                    .collect(),
            ),
        );
        assert_eq!(ds["00180050"], json!({"vr":"DS","Value":[1.5,-25]}));
    }

    #[test]
    fn json_bad_ds_is_string() {
        let v = one(Tag(0x0018, 0x0050), VR::DS, PrimitiveValue::from("abc "));
        assert_eq!(v["00180050"], json!({"vr":"DS","Value":["abc"]}));
    }

    #[test]
    fn json_multivalue_strings() {
        let v = one(
            Tag(0x0008, 0x0008),
            VR::CS,
            PrimitiveValue::from("ORIGINAL\\PRIMARY\\AXIAL "),
        );
        assert_eq!(
            v["00080008"],
            json!({"vr":"CS","Value":["ORIGINAL","PRIMARY","AXIAL"]})
        );
    }

    #[test]
    fn json_long_text_keeps_leading_space_and_backslash() {
        let v = one(
            Tag(0x0008, 0x1030),
            VR::LT,
            PrimitiveValue::from("  a\\b  "),
        );
        assert_eq!(v["00081030"], json!({"vr":"LT","Value":["  a\\b"]}));
    }

    #[test]
    fn json_empty_value() {
        let v = one(T, VR::PN, PrimitiveValue::Empty);
        assert_eq!(v["00100010"], json!({"vr":"PN"}));
        let v = one(T, VR::LO, PrimitiveValue::from("  "));
        assert_eq!(v["00100010"], json!({"vr":"LO"}));
    }

    #[test]
    fn json_numbers_and_non_finite() {
        let v = one(
            Tag(0x0028, 0x0010),
            VR::US,
            PrimitiveValue::U16([512u16].into_iter().collect()),
        );
        assert_eq!(v["00280010"], json!({"vr":"US","Value":[512]}));
        let v = one(
            Tag(0x0018, 0x9999),
            VR::FD,
            PrimitiveValue::F64([f64::NAN, 2.5].into_iter().collect()),
        );
        assert_eq!(v["00189999"], json!({"vr":"FD","Value":[null,2.5]}));
    }

    #[test]
    fn json_inline_binary_base64() {
        let v = one(
            Tag(0x0009, 0x1001),
            VR::OB,
            PrimitiveValue::from(vec![0xFBu8, 0xFF, 0x01, 0x02]),
        );
        assert_eq!(v["00091001"], json!({"vr":"OB","InlineBinary":"+/8BAg=="}));
        let v = one(
            Tag(0x0009, 0x1002),
            VR::OW,
            PrimitiveValue::U16([0x0201u16].into_iter().collect()),
        );
        assert_eq!(v["00091002"], json!({"vr":"OW","InlineBinary":"AQI="}));
    }

    #[test]
    fn json_bulk_uri_relative() {
        let mut w = DicomJsonWriter::new();
        w.bulk(Tag(0x7FE0, 0x0010), VR::OW, "7FE00010");
        let v: Value = serde_json::from_slice(&w.finish()).unwrap();
        assert_eq!(
            v["7FE00010"],
            json!({"vr":"OW","BulkDataURI":"bulk/7FE00010"})
        );
    }

    #[test]
    fn json_nested_sequence() {
        let mut w = DicomJsonWriter::new();
        w.element(Tag(0x0008, 0x0060), VR::CS, &PrimitiveValue::from("CT"));
        w.begin_sequence(Tag(0x0008, 0x1140));
        w.begin_item();
        w.element(
            Tag(0x0008, 0x1150),
            VR::UI,
            &PrimitiveValue::from("1.2.3\0"),
        );
        w.begin_sequence(Tag(0x0040, 0xA730));
        w.begin_item();
        w.end_item();
        w.end_sequence();
        w.end_item();
        w.begin_item();
        w.end_item();
        w.end_sequence();
        w.element(Tag(0x0020, 0x0013), VR::IS, &PrimitiveValue::from("3"));
        assert!(w.len() > 10);
        let v: Value = serde_json::from_slice(&w.finish()).unwrap();
        assert_eq!(
            v["00081140"],
            json!({"vr":"SQ","Value":[
                {"00081150":{"vr":"UI","Value":["1.2.3"]},
                 "0040A730":{"vr":"SQ","Value":[{}]}},
                {}
            ]})
        );
        assert_eq!(v["00200013"]["Value"], json!([3]));
    }

    #[test]
    fn json_uv_beyond_2_53_is_string() {
        let v = one(
            Tag(0x0009, 0x1003),
            VR::UV,
            PrimitiveValue::U64([(1u64 << 53) + 1, 5].into_iter().collect()),
        );
        assert_eq!(
            v["00091003"],
            json!({"vr":"UV","Value":["9007199254740993",5]})
        );
        let v = one(
            Tag(0x0009, 0x1004),
            VR::SV,
            PrimitiveValue::I64([-(1i64 << 53) - 1].into_iter().collect()),
        );
        assert_eq!(v["00091004"]["Value"], json!(["-9007199254740993"]));
    }

    #[test]
    fn json_key_sequence_only_from_keys() {
        let mut w = DicomJsonWriter::new();
        w.element(
            Tag(0x0008, 0x0070),
            VR::LO,
            &PrimitiveValue::from("x\"BulkDataURI\":\"y"),
        );
        w.bulk(Tag(0x7FE0, 0x0010), VR::OW, "7FE00010");
        let out = String::from_utf8(w.finish()).unwrap();
        serde_json::from_str::<Value>(&out).unwrap();
        assert_eq!(out.matches("\"BulkDataURI\":\"").count(), 1);
    }
}
