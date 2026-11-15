//! QIDO-RS matching in SQL (DICOM PS3.18 section 10.6).
//!
//! One query joins `studies`, `series` and `instances` as far down as the query level
//! needs. Every value from the request goes in as a bound parameter; only fixed column
//! names and operators are written into the SQL text.

use slicestream_core::FrameInfo;
use slicestream_dicomweb::{Key, Level, Matcher, QidoQuery};
use sqlx::sqlite::{Sqlite, SqliteRow};
use sqlx::{QueryBuilder, Row};

use crate::{Index, fold_name};

/// Study attributes of a QIDO-RS result row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StudyFields {
    pub study_uid: String,
    pub study_date: Option<String>,
    pub study_time: Option<String>,
    pub accession_number: Option<String>,
    pub referring_physician_name: Option<String>,
    pub patient_name: Option<String>,
    /// Empty when the instances carried no Patient ID.
    pub patient_id: String,
    pub patient_birth_date: Option<String>,
    pub patient_sex: Option<String>,
    pub study_id: Option<String>,
    pub study_description: Option<String>,
    /// Distinct modalities of the study's series, sorted.
    pub modalities: Vec<String>,
    pub series_count: i64,
    pub instance_count: i64,
}

/// Series attributes of a QIDO-RS result row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesFields {
    pub series_uid: String,
    pub modality: Option<String>,
    pub series_number: Option<i64>,
    pub series_description: Option<String>,
    pub pps_start_date: Option<String>,
    pub pps_start_time: Option<String>,
    pub instance_count: i64,
}

/// Instance attributes of a QIDO-RS result row.
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceFields {
    pub sop_uid: String,
    pub sop_class_uid: String,
    pub instance_number: Option<i64>,
    pub number_of_frames: u32,
    pub frame_info: Option<FrameInfo>,
}

/// One result of a QIDO-RS query. `series` is set at series and instance level, `instance`
/// at instance level.
#[derive(Debug, Clone, PartialEq)]
pub struct QidoRow {
    pub study: StudyFields,
    pub series: Option<SeriesFields>,
    pub instance: Option<InstanceFields>,
    /// Lowest instance id of the row's study (study level), series (series level) or the
    /// instance itself. Its stored metadata supplies attributes the index does not hold.
    pub representative: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QidoPage {
    pub rows: Vec<QidoRow>,
    /// True when more matches exist beyond this page.
    pub more: bool,
}

const SELECT: &str = "SELECT st.study_instance_uid, st.study_date, st.study_time,
        st.accession_number, st.referring_physician_name, st.patient_name, p.patient_id,
        st.patient_birth_date, st.patient_sex, st.study_id, st.study_description,
        (SELECT group_concat(DISTINCT x.modality) FROM series x WHERE x.study_fk = st.id),
        (SELECT COUNT(*) FROM series x WHERE x.study_fk = st.id),
        (SELECT COUNT(*) FROM instances xi JOIN series x ON x.id = xi.series_fk
          WHERE x.study_fk = st.id)";

const SERIES_COLUMNS: &str = ", s.series_instance_uid, s.modality, s.series_number,
        s.series_description, s.performed_procedure_step_start_date,
        s.performed_procedure_step_start_time,
        (SELECT COUNT(*) FROM instances xi WHERE xi.series_fk = s.id)";
const NO_SERIES_COLUMNS: &str = ", NULL, NULL, NULL, NULL, NULL, NULL, NULL";
const INSTANCE_COLUMNS: &str =
    ", i.sop_instance_uid, i.sop_class_uid, i.instance_number, i.number_of_frames, i.frame_info";
const NO_INSTANCE_COLUMNS: &str = ", NULL, NULL, NULL, NULL, NULL";

const REPRESENTATIVE_STUDY: &str = ", COALESCE((SELECT MIN(xi.id) FROM instances xi
          JOIN series x ON x.id = xi.series_fk WHERE x.study_fk = st.id), 0)";
const REPRESENTATIVE_SERIES: &str =
    ", COALESCE((SELECT MIN(xi.id) FROM instances xi WHERE xi.series_fk = s.id), 0)";
const REPRESENTATIVE_INSTANCE: &str = ", i.id";

#[derive(Clone, Copy, PartialEq)]
enum Ty {
    Text,
    /// Integer string (IS): `Single` and `List` compare as integers, `Wildcard` as text.
    Int,
    /// Person name: compared folded against the `_folded` column.
    Name,
    Date,
    Time,
}

/// The SQL column of `key` and how its values compare.
fn column(key: Key) -> (&'static str, Ty) {
    match key {
        Key::StudyDate => ("st.study_date", Ty::Date),
        Key::StudyTime => ("st.study_time", Ty::Time),
        Key::AccessionNumber => ("st.accession_number", Ty::Text),
        Key::ReferringPhysicianName => ("st.referring_physician_name_folded", Ty::Name),
        Key::PatientName => ("st.patient_name_folded", Ty::Name),
        Key::PatientId => ("p.patient_id", Ty::Text),
        Key::StudyInstanceUid => ("st.study_instance_uid", Ty::Text),
        Key::StudyId => ("st.study_id", Ty::Text),
        Key::StudyDescription => ("st.study_description", Ty::Text),
        Key::Modality => ("s.modality", Ty::Text),
        Key::SeriesInstanceUid => ("s.series_instance_uid", Ty::Text),
        Key::SeriesNumber => ("s.series_number", Ty::Int),
        Key::PpsStartDate => ("s.performed_procedure_step_start_date", Ty::Date),
        Key::PpsStartTime => ("s.performed_procedure_step_start_time", Ty::Time),
        Key::SeriesDescription => ("s.series_description", Ty::Text),
        Key::SopClassUid => ("i.sop_class_uid", Ty::Text),
        Key::SopInstanceUid => ("i.sop_instance_uid", Ty::Text),
        Key::InstanceNumber => ("i.instance_number", Ty::Int),
        // Both handled before this lookup.
        Key::ModalitiesInStudy | Key::RequestAttributeSequence => ("", Ty::Text),
    }
}

/// Escape `[` and `]` so GLOB treats them as literal characters. `*` and `?` stay wildcards,
/// as in DICOM.
fn glob_escape(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    for c in pattern.chars() {
        match c {
            '[' => out.push_str("[[]"),
            ']' => out.push_str("[]]"),
            c => out.push(c),
        }
    }
    out
}

/// `HH[MM[SS[.F]]]` as `HHMMSS.FFFFFF`. A lower bound pads with zeros, an upper bound with
/// the largest value (`59` minutes and seconds, `9` fraction digits), so `1200` as an upper
/// bound covers all of 12:00.
fn normalize_time(t: &str, upper: bool) -> String {
    let (main, fraction) = t.split_once('.').unwrap_or((t, ""));
    let mut out = main.to_string();
    while out.len() < 6 {
        out.push_str(if upper { "59" } else { "00" });
    }
    out.push('.');
    out.push_str(fraction);
    while out.len() < 13 {
        out.push(if upper { '9' } else { '0' });
    }
    out
}

/// SQL that normalizes a stored TM column the same way as [`normalize_time`] with zero fill.
fn time_expr(col: &str) -> String {
    format!(
        "(substr({col} || '000000', 1, 6) || '.' || substr(CASE WHEN instr({col}, '.') > 0 \
         THEN substr({col}, instr({col}, '.') + 1) ELSE '' END || '000000', 1, 6))"
    )
}

type Qb<'a> = QueryBuilder<'a, Sqlite>;

/// A condition that never holds.
fn never(qb: &mut Qb<'_>) {
    qb.push(" AND 0");
}

fn push_in<'a, T>(qb: &mut Qb<'a>, col: &str, values: Vec<T>)
where
    T: 'a + sqlx::Encode<'a, Sqlite> + sqlx::Type<Sqlite> + Send,
{
    if values.is_empty() {
        return never(qb);
    }
    qb.push(" AND ").push(col).push(" IN (");
    let mut sep = qb.separated(", ");
    for v in values {
        sep.push_bind(v);
    }
    qb.push(")");
}

fn push_match(qb: &mut Qb<'_>, key: Key, matcher: &Matcher) {
    if *matcher == Matcher::Universal {
        return;
    }
    if key == Key::RequestAttributeSequence {
        // Deviation: no request attributes are indexed, so only the universal match (above)
        // can succeed. Any other matcher matches no series.
        return never(qb);
    }
    if key == Key::ModalitiesInStudy {
        let list = match matcher {
            Matcher::Single(v) => vec![v.clone()],
            Matcher::List(v) => non_empty(v),
            _ => return never(qb),
        };
        if list.is_empty() {
            return;
        }
        qb.push(" AND EXISTS (SELECT 1 FROM series x WHERE x.study_fk = st.id AND x.modality IN (");
        let mut sep = qb.separated(", ");
        for v in list {
            sep.push_bind(v);
        }
        qb.push("))");
        return;
    }
    let (col, ty) = column(key);
    let fold = |s: &str| {
        if ty == Ty::Name {
            fold_name(s)
        } else {
            s.to_string()
        }
    };
    match matcher {
        Matcher::Universal => {}
        Matcher::Single(v) => match ty {
            Ty::Int => match v.trim().parse::<i64>() {
                Ok(n) => {
                    qb.push(" AND ").push(col).push(" = ").push_bind(n);
                }
                Err(_) => never(qb),
            },
            // A short time value stands for the whole interval it names.
            Ty::Time => push_range(qb, col, ty, Some(v), Some(v)),
            _ => {
                qb.push(" AND ").push(col).push(" = ").push_bind(fold(v));
            }
        },
        Matcher::List(values) => {
            let values = non_empty(values);
            if values.is_empty() {
                return;
            }
            if ty == Ty::Int {
                push_in(
                    qb,
                    col,
                    values
                        .iter()
                        .filter_map(|v| v.trim().parse::<i64>().ok())
                        .collect(),
                );
            } else {
                push_in(qb, col, values.iter().map(|v| fold(v)).collect());
            }
        }
        Matcher::Wildcard(p) => {
            let pattern = glob_escape(&fold(p));
            if ty == Ty::Int {
                qb.push(" AND CAST(").push(col).push(" AS TEXT) GLOB ");
            } else {
                qb.push(" AND ").push(col).push(" GLOB ");
            }
            qb.push_bind(pattern);
        }
        Matcher::Range { from, to } => push_range(qb, col, ty, from.as_ref(), to.as_ref()),
    }
}

fn push_range(qb: &mut Qb<'_>, col: &str, ty: Ty, from: Option<&String>, to: Option<&String>) {
    let (lhs, from, to) = if ty == Ty::Time {
        (
            time_expr(col),
            from.map(|v| normalize_time(v, false)),
            to.map(|v| normalize_time(v, true)),
        )
    } else {
        (col.to_string(), from.cloned(), to.cloned())
    };
    qb.push(format!(" AND {col} IS NOT NULL"));
    if let Some(f) = from {
        qb.push(format!(" AND {lhs} >= ")).push_bind(f);
    }
    if let Some(t) = to {
        qb.push(format!(" AND {lhs} <= ")).push_bind(t);
    }
}

/// The non-empty entries of a comma list (`1.2,,3` has an empty middle entry).
fn non_empty(values: &[String]) -> Vec<String> {
    values.iter().filter(|v| !v.is_empty()).cloned().collect()
}

impl Index {
    /// Run a QIDO-RS query. At most `min(q.limit, max_results)` rows come back; `more` says
    /// whether further matches exist. Rows are ordered by the level's id, so paging by
    /// `offset` is stable.
    pub async fn qido(&self, q: &QidoQuery, max_results: u32) -> Result<QidoPage, sqlx::Error> {
        let limit = q.limit.unwrap_or(max_results).min(max_results);
        let mut qb: Qb<'_> = QueryBuilder::new(SELECT);
        let (from, order) = match q.level {
            Level::Study => {
                qb.push(NO_SERIES_COLUMNS)
                    .push(NO_INSTANCE_COLUMNS)
                    .push(REPRESENTATIVE_STUDY);
                (
                    " FROM studies st JOIN patients p ON p.id = st.patient_fk",
                    " ORDER BY st.id",
                )
            }
            Level::Series => {
                qb.push(SERIES_COLUMNS)
                    .push(NO_INSTANCE_COLUMNS)
                    .push(REPRESENTATIVE_SERIES);
                (
                    " FROM series s JOIN studies st ON st.id = s.study_fk \
                     JOIN patients p ON p.id = st.patient_fk",
                    " ORDER BY s.id",
                )
            }
            Level::Instance => {
                qb.push(SERIES_COLUMNS)
                    .push(INSTANCE_COLUMNS)
                    .push(REPRESENTATIVE_INSTANCE);
                (
                    " FROM instances i JOIN series s ON s.id = i.series_fk \
                     JOIN studies st ON st.id = s.study_fk \
                     JOIN patients p ON p.id = st.patient_fk",
                    " ORDER BY i.id",
                )
            }
        };
        qb.push(from).push(" WHERE 1");
        if let Some(uid) = &q.study_uid {
            qb.push(" AND st.study_instance_uid = ")
                .push_bind(uid.clone());
        }
        if let Some(uid) = &q.series_uid
            && q.level >= Level::Series
        {
            qb.push(" AND s.series_instance_uid = ")
                .push_bind(uid.clone());
        }
        for (key, matcher) in &q.matches {
            // Keys below the query level have no table to match against.
            if key.level() <= q.level {
                push_match(&mut qb, *key, matcher);
            }
        }
        qb.push(order)
            .push(" LIMIT ")
            .push_bind(i64::from(limit) + 1)
            .push(" OFFSET ")
            .push_bind(i64::from(q.offset));

        let mut rows = qb.build().fetch_all(&self.pool).await?;
        let more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let rows = rows
            .iter()
            .map(|r| decode_row(r, q.level))
            .collect::<Result<_, _>>()?;
        Ok(QidoPage { rows, more })
    }
}

fn decode_row(r: &SqliteRow, level: Level) -> Result<QidoRow, sqlx::Error> {
    let modalities: Option<String> = r.try_get(11)?;
    let mut modalities: Vec<String> = modalities
        .map(|m| m.split(',').map(str::to_string).collect())
        .unwrap_or_default();
    modalities.sort();
    let study = StudyFields {
        study_uid: r.try_get(0)?,
        study_date: r.try_get(1)?,
        study_time: r.try_get(2)?,
        accession_number: r.try_get(3)?,
        referring_physician_name: r.try_get(4)?,
        patient_name: r.try_get(5)?,
        patient_id: r.try_get(6)?,
        patient_birth_date: r.try_get(7)?,
        patient_sex: r.try_get(8)?,
        study_id: r.try_get(9)?,
        study_description: r.try_get(10)?,
        modalities,
        series_count: r.try_get(12)?,
        instance_count: r.try_get(13)?,
    };
    let series = if level >= Level::Series {
        Some(SeriesFields {
            series_uid: r.try_get(14)?,
            modality: r.try_get(15)?,
            series_number: r.try_get(16)?,
            series_description: r.try_get(17)?,
            pps_start_date: r.try_get(18)?,
            pps_start_time: r.try_get(19)?,
            instance_count: r.try_get(20)?,
        })
    } else {
        None
    };
    let instance = if level == Level::Instance {
        let frames: i64 = r.try_get(24)?;
        let info: Option<&str> = r.try_get(25)?;
        Some(InstanceFields {
            sop_uid: r.try_get(21)?,
            sop_class_uid: r.try_get(22)?,
            instance_number: r.try_get(23)?,
            number_of_frames: u32::try_from(frames).map_err(|_| {
                sqlx::Error::Decode(format!("invalid number_of_frames {frames}").into())
            })?,
            frame_info: info
                .map(serde_json::from_str)
                .transpose()
                .map_err(|e| sqlx::Error::Decode(format!("invalid frame_info JSON: {e}").into()))?,
        })
    } else {
        None
    };
    Ok(QidoRow {
        study,
        series,
        instance,
        representative: r.try_get(26)?,
    })
}
