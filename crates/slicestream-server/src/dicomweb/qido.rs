//! QIDO-RS search (PS3.18 10.6): studies, series and instances.
//!
//! Each result row carries the level's default attributes from the index,
//! plus computed ones (counts, Modalities in Study, Retrieve URL, Instance
//! Availability, Specific Character Set). `includefield` attributes the
//! index does not hold come from the stored metadata of the row's
//! representative instance (the lowest instance id of the study or series).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use slicestream_dicomweb::{
    Include, Level, Matcher, QidoQuery, default_fields, negotiate_json, parse_qido,
};
use slicestream_store::{MetadataState, QidoRow};

use super::qido_json::{
    SERIES_INSTANCE_UID, STUDY_INSTANCE_UID, Wanted, index_value, indexed, pick, write_object,
};
use super::{DICOM_JSON, accept_ranges, plain, request_base_url, warning};
use crate::AppState;
use crate::routes::internal_error;

/// Most values in one UID list: each is one bound SQL parameter, and SQLite
/// limits those.
const MAX_LIST: usize = 1000;
/// Representatives whose metadata is read at once.
const METADATA_CHUNK: usize = 128;

const FUZZY_WARNING: &str =
    "The fuzzymatching parameter is not supported. Only literal matching has been performed.";
const MORE_WARNING: &str = "There are additional results that can be requested";
/// Longest list of ignored parameter names put in a `Warning`.
const MAX_IGNORED_TEXT: usize = 1024;

/// `includefield=all` at study level: the Patient, General Study and
/// Patient Study modules (PS3.3 C.7.1.1, C.7.2.1, C.7.2.2).
const STUDY_ALL: &[u32] = &[
    // Patient
    0x0008_1120,
    0x0010_0010,
    0x0010_0020,
    0x0010_0021,
    0x0010_0022,
    0x0010_0024,
    0x0010_0030,
    0x0010_0032,
    0x0010_0033,
    0x0010_0034,
    0x0010_0035,
    0x0010_0040,
    0x0010_0200,
    0x0010_0212,
    0x0010_0213,
    0x0010_0214,
    0x0010_0215,
    0x0010_0216,
    0x0010_0217,
    0x0010_0218,
    0x0010_0219,
    0x0010_0221,
    0x0010_1001,
    0x0010_1002,
    0x0010_1100,
    0x0010_2160,
    0x0010_2201,
    0x0010_2202,
    0x0010_2292,
    0x0010_2293,
    0x0010_2294,
    0x0010_2297,
    0x0010_2298,
    0x0010_2299,
    0x0010_4000,
    0x0012_0062,
    0x0012_0063,
    0x0012_0064,
    // General Study
    0x0008_0020,
    0x0008_0030,
    0x0008_0050,
    0x0008_0051,
    0x0008_0090,
    0x0008_0096,
    0x0008_009C,
    0x0008_009D,
    0x0008_1030,
    0x0008_1032,
    0x0008_1048,
    0x0008_1049,
    0x0008_1060,
    0x0008_1062,
    0x0008_1110,
    0x0020_000D,
    0x0020_0010,
    0x0032_1033,
    0x0032_1034,
    0x0032_1066,
    0x0032_1067,
    0x0040_1012,
    // Patient Study
    0x0008_1080,
    0x0008_1084,
    0x0010_1010,
    0x0010_1020,
    0x0010_1021,
    0x0010_1022,
    0x0010_1023,
    0x0010_1024,
    0x0010_1030,
    0x0010_2180,
    0x0010_21A0,
    0x0010_21B0,
    0x0010_21C0,
    0x0010_21D0,
    0x0010_2203,
    0x0038_0010,
    0x0038_0014,
    0x0038_0060,
    0x0038_0062,
    0x0038_0064,
    0x0038_0500,
    0x0040_3001,
];

/// `includefield=all` at series level: the General Series module (PS3.3
/// C.7.3.1).
const SERIES_ALL: &[u32] = &[
    0x0008_0021,
    0x0008_0031,
    0x0008_0060,
    0x0008_103E,
    0x0008_103F,
    0x0008_1050,
    0x0008_1052,
    0x0008_1070,
    0x0008_1072,
    0x0008_1111,
    0x0008_1250,
    0x0010_2210,
    0x0018_0015,
    0x0018_1030,
    0x0018_5100,
    0x0020_000E,
    0x0020_0011,
    0x0020_0060,
    0x0028_0108,
    0x0028_0109,
    0x0040_0244,
    0x0040_0245,
    0x0040_0250,
    0x0040_0251,
    0x0040_0253,
    0x0040_0254,
    0x0040_0260,
    0x0040_0261,
    0x0040_0275,
    0x0040_0280,
];

/// `GET /dicom-web/studies`
pub async fn studies(
    State(state): State<Arc<AppState>>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    search(&state, Level::Study, None, None, query, &headers).await
}

/// `GET /dicom-web/series`
pub async fn all_series(
    State(state): State<Arc<AppState>>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    search(&state, Level::Series, None, None, query, &headers).await
}

/// `GET /dicom-web/instances`
pub async fn all_instances(
    State(state): State<Arc<AppState>>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    search(&state, Level::Instance, None, None, query, &headers).await
}

/// `GET /dicom-web/studies/{study}/series`
pub async fn study_series(
    State(state): State<Arc<AppState>>,
    Path(study): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    search(&state, Level::Series, Some(&study), None, query, &headers).await
}

/// `GET /dicom-web/studies/{study}/instances`
pub async fn study_instances(
    State(state): State<Arc<AppState>>,
    Path(study): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    search(&state, Level::Instance, Some(&study), None, query, &headers).await
}

/// `GET /dicom-web/studies/{study}/series/{series}/instances`
pub async fn series_instances(
    State(state): State<Arc<AppState>>,
    Path((study, series)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let (study, series) = (Some(study.as_str()), Some(series.as_str()));
    search(&state, Level::Instance, study, series, query, &headers).await
}

async fn search(
    state: &AppState,
    level: Level,
    study: Option<&str>,
    series: Option<&str>,
    raw_query: Option<String>,
    headers: &HeaderMap,
) -> Response {
    if let Err(e) = negotiate_json(&accept_ranges(headers)) {
        return plain(StatusCode::NOT_ACCEPTABLE, e.to_string());
    }
    let query = match parse_qido(level, study, series, raw_query.as_deref().unwrap_or("")) {
        Ok(q) => q,
        Err(e) => return plain(StatusCode::BAD_REQUEST, e.to_string()),
    };
    if let Some(reason) = oversized_list(&query) {
        return plain(StatusCode::BAD_REQUEST, reason);
    }
    let page = match state
        .ingestor
        .index()
        .qido(&query, state.dicomweb.max_results)
        .await
    {
        Ok(p) => p,
        Err(e) => return internal_error(&anyhow::Error::new(e)),
    };

    let fields = Fields::new(&query);
    let base = request_base_url(&state.http, headers);
    let mut body = Vec::new();
    body.push(b'[');
    let mut first = true;
    for rows in page.rows.chunks(METADATA_CHUNK) {
        let mut metadata = match &fields.metadata {
            Wanted::Nothing => HashMap::new(),
            want => match load_metadata(state, rows, want).await {
                Ok(m) => m,
                Err(e) => return internal_error(&e),
            },
        };
        for row in rows {
            if !first {
                body.push(b',');
            }
            first = false;
            let mut attrs = metadata.remove(&row.representative).unwrap_or_default();
            for &tag in &fields.tags {
                if let Some(v) = index_value(tag, row, &base) {
                    attrs.insert(tag, v.to_string());
                }
            }
            write_object(&mut body, &attrs);
        }
    }
    body.push(b']');

    let mut res = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, HeaderValue::from_static(DICOM_JSON))],
        body,
    )
        .into_response();
    let mut texts = Vec::new();
    if query.fuzzy {
        texts.push(FUZZY_WARNING.to_owned());
    }
    if page.more {
        texts.push(MORE_WARNING.to_owned());
    }
    if let Some(text) = ignored_text(&query.ignored) {
        texts.push(text);
    }
    for text in texts {
        if let Some(v) = warning(&base, &text) {
            res.headers_mut().append(header::WARNING, v);
        }
    }
    res
}

/// A 400 reason if a UID list is longer than [`MAX_LIST`].
fn oversized_list(q: &QidoQuery) -> Option<String> {
    q.matches.iter().find_map(|(key, m)| match m {
        Matcher::List(values) if values.len() > MAX_LIST => Some(format!(
            "{:08X}: a list may hold at most {MAX_LIST} values, not {}",
            tag_u32(key.tag()),
            values.len()
        )),
        _ => None,
    })
}

/// "The following query parameters were ignored: a, b" (names once each,
/// cut after [`MAX_IGNORED_TEXT`] bytes).
fn ignored_text(ignored: &[String]) -> Option<String> {
    if ignored.is_empty() {
        return None;
    }
    let mut text = String::from("The following query parameters were ignored: ");
    let mut seen = HashSet::new();
    for (i, name) in ignored
        .iter()
        .filter(|n| seen.insert(n.as_str()))
        .enumerate()
    {
        if i > 0 {
            text.push_str(", ");
        }
        text.push_str(name);
        if text.len() > MAX_IGNORED_TEXT {
            let mut cut = MAX_IGNORED_TEXT;
            while !text.is_char_boundary(cut) {
                cut -= 1;
            }
            text.truncate(cut);
            text.push_str("...");
            break;
        }
    }
    Some(text)
}

fn tag_u32(tag: dicom_core::Tag) -> u32 {
    (u32::from(tag.0) << 16) | u32::from(tag.1)
}

/// What each result row carries.
struct Fields {
    /// Attributes taken from the index when the row has them there.
    tags: BTreeSet<u32>,
    /// Attributes taken from the representative instance's metadata.
    metadata: Wanted,
}

impl Fields {
    fn new(q: &QidoQuery) -> Fields {
        let defaults = |level| default_fields(level).iter().map(|t| tag_u32(*t));
        let mut tags: BTreeSet<u32> = defaults(q.level).collect();
        // PS3.18 10.6.3.3.2 and 10.6.3.3.3: the levels above the target come
        // along unless the path fixes them.
        if q.level >= Level::Series && q.study_uid.is_none() {
            tags.extend(defaults(Level::Study));
        }
        if q.level == Level::Instance && q.series_uid.is_none() {
            tags.extend(defaults(Level::Series));
        }
        if q.level >= Level::Series {
            tags.insert(STUDY_INSTANCE_UID);
        }
        if q.level == Level::Instance {
            tags.insert(SERIES_INSTANCE_UID);
        }
        // Matching keys are returned too.
        tags.extend(q.matches.iter().map(|(key, _)| tag_u32(key.tag())));

        let requested: Vec<u32> = match &q.include {
            Include::Default => Vec::new(),
            Include::Tags(list) => list.iter().map(|t| tag_u32(*t)).collect(),
            Include::All => match q.level {
                Level::Study => STUDY_ALL.to_vec(),
                Level::Series => SERIES_ALL.to_vec(),
                Level::Instance => Vec::new(),
            },
        };
        let mut from_metadata = BTreeSet::new();
        for tag in requested {
            tags.insert(tag);
            if !indexed(q.level, tag) {
                from_metadata.insert(tag);
            }
        }
        let metadata = if q.level == Level::Instance && q.include == Include::All {
            Wanted::Everything
        } else if from_metadata.is_empty() {
            Wanted::Nothing
        } else {
            Wanted::Tags(from_metadata)
        };
        Fields { tags, metadata }
    }
}

/// Picked metadata attributes of each representative in `rows`, as JSON
/// text by tag. A representative stored before scanning existed is scanned
/// first. Representatives without metadata (the scan failed or the
/// metadata was too large) are left out.
async fn load_metadata(
    state: &AppState,
    rows: &[QidoRow],
    want: &Wanted,
) -> anyhow::Result<HashMap<i64, BTreeMap<u32, String>>> {
    let index = state.ingestor.index();
    let ids: Vec<i64> = rows.iter().map(|r| r.representative).collect();
    let mut states: HashMap<i64, MetadataState> =
        index.metadata_batch(&ids).await?.into_iter().collect();
    let unscanned: Vec<i64> = states
        .iter()
        .filter(|(_, s)| matches!(s, MetadataState::Unscanned))
        .map(|(id, _)| *id)
        .collect();
    if !unscanned.is_empty() {
        for &id in &unscanned {
            if let Err(e) = state.ingestor.ensure_scanned(id).await {
                tracing::warn!(instance = id, "scan on demand failed: {e}");
            }
        }
        states.extend(index.metadata_batch(&unscanned).await?);
    }
    Ok(states
        .into_iter()
        .filter_map(|(id, s)| match s {
            MetadataState::Ready(json) => Some((id, pick(id, &json, want))),
            MetadataState::Unscanned => None,
            MetadataState::Unavailable(reason) => {
                tracing::debug!(instance = id, "no metadata for QIDO-RS: {reason}");
                None
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dicom_core::Tag;
    use dicom_core::dictionary::DataDictionary;
    use dicom_dictionary_std::StandardDataDictionary;

    #[test]
    fn module_lists_are_sorted_known_tags() {
        for list in [STUDY_ALL, SERIES_ALL] {
            for &t in list {
                let tag = Tag((t >> 16) as u16, t as u16);
                assert!(
                    StandardDataDictionary.by_tag(tag).is_some(),
                    "{t:08X} is not a standard attribute"
                );
            }
            let set: BTreeSet<u32> = list.iter().copied().collect();
            assert_eq!(set.len(), list.len(), "duplicate tag");
        }
    }

    #[test]
    fn ignored_text_dedupes_and_caps() {
        assert_eq!(ignored_text(&[]), None);
        let names = ["Foo".to_owned(), "Bar".into(), "Foo".into()];
        assert_eq!(
            ignored_text(&names).unwrap(),
            "The following query parameters were ignored: Foo, Bar"
        );
        let long = vec!["é".repeat(400), "x".repeat(400)];
        let t = ignored_text(&long).unwrap();
        assert!(t.len() <= MAX_IGNORED_TEXT + 3 && t.ends_with("..."));
    }
}
