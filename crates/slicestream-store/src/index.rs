//! SQLite index of patients, studies, series and instances.
//!
//! All writes go through [`Index::upsert_instance`], which runs one
//! `BEGIN IMMEDIATE` transaction. Taking the write lock up front (instead of
//! upgrading a deferred read transaction) means concurrent writers queue on
//! `busy_timeout` instead of failing with `SQLITE_BUSY`.

use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use slicestream_core::FrameInfo;
use sqlx::Row;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteRow,
    SqliteSynchronous,
};
use tracing::warn;

use crate::{BlobHash, InstanceHeader};

/// Everything needed to index one stored instance.
#[derive(Debug, Clone)]
pub struct InstanceRecord {
    pub header: InstanceHeader,
    pub blob: BlobHash,
    pub size_bytes: u64,
    /// Transport the instance arrived on (e.g. `"dimse"`, `"stow"`).
    pub source: String,
    pub calling_ae: Option<String>,
}

/// What to do when a SOP Instance UID is already indexed with a different blob.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DuplicatePolicy {
    #[default]
    Keep,
    Overwrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertOutcome {
    Inserted,
    /// The UID was already indexed; the index is unchanged.
    Duplicate {
        existing: BlobHash,
    },
    /// The UID now points at the new blob; `previous` may be garbage.
    Replaced {
        previous: BlobHash,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceSummary {
    pub sop_instance_uid: String,
    pub series_instance_uid: String,
    pub blob: BlobHash,
    pub source: String,
    pub calling_ae: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesRow {
    pub series_instance_uid: String,
    pub study_instance_uid: String,
    pub modality: Option<String>,
    pub series_description: Option<String>,
    pub instance_count: i64,
}

/// Aggregate row counts and distinct-blob totals for the metrics sampler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
pub struct StorageCounts {
    pub studies: i64,
    pub series: i64,
    pub instances: i64,
    /// Total bytes over distinct blobs (a shared blob counts once).
    pub objects_bytes: i64,
    /// Number of distinct blobs.
    pub objects_count: i64,
}

/// One series row for the test console.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ConsoleSeriesRow {
    pub series_uid: String,
    pub study_uid: String,
    pub patient_name: Option<String>,
    pub study_date: Option<String>,
    pub modality: Option<String>,
    pub series_description: Option<String>,
    pub instance_count: i64,
    pub size_bytes: i64,
    pub updated_at: String,
}

/// One image instance of a series (rows without `frame_info` are excluded).
#[derive(Debug, Clone, PartialEq)]
pub struct SliceRow {
    pub blob: BlobHash,
    pub number_of_frames: u32,
    pub instance_number: Option<i64>,
    pub slice_position: Option<f64>,
    pub frame_info: FrameInfo,
}

/// Handle to the SQLite index. Cheap to clone (wraps a connection pool).
#[derive(Debug, Clone)]
pub struct Index {
    pool: SqlitePool,
}

fn decode_err(msg: String) -> sqlx::Error {
    sqlx::Error::Decode(msg.into())
}

fn parse_hash(s: &str) -> Result<BlobHash, sqlx::Error> {
    BlobHash::from_hex(s).ok_or_else(|| decode_err(format!("invalid blob_sha256 {s:?}")))
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

impl Index {
    /// Open (creating if missing) the database at `db_path` and run migrations.
    pub async fn open(db_path: &Path) -> Result<Index, sqlx::Error> {
        let opts = SqliteConnectOptions::new()
            .filename(db_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new().connect_with(opts).await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Index { pool })
    }

    /// Index `rec` in one transaction, applying `policy` if its SOP Instance
    /// UID is already present. Safe to call concurrently for the same UID.
    pub async fn upsert_instance(
        &self,
        rec: &InstanceRecord,
        policy: DuplicatePolicy,
    ) -> Result<UpsertOutcome, sqlx::Error> {
        let h = &rec.header;
        let ts = now();
        let frame_info = h
            .frame_info
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
        let blob_hex = rec.blob.to_hex();
        let size = i64::try_from(rec.size_bytes)
            .map_err(|_| sqlx::Error::Encode("size_bytes exceeds i64".into()))?;

        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        let patient_id: i64 = sqlx::query_scalar(
            "INSERT INTO patients (patient_id, patient_name) VALUES (?1, ?2)
             ON CONFLICT(patient_id) DO UPDATE
               SET patient_name = COALESCE(patients.patient_name, excluded.patient_name)
             RETURNING id",
        )
        .bind(h.patient_id.as_deref().unwrap_or(""))
        .bind(&h.patient_name)
        .fetch_one(&mut *tx)
        .await?;

        let (study_id, study_patient): (i64, i64) = sqlx::query_as(
            "INSERT INTO studies (study_instance_uid, patient_fk, study_date, study_description,
                                  accession_number, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
             ON CONFLICT(study_instance_uid) DO UPDATE SET updated_at = excluded.updated_at
             RETURNING id, patient_fk",
        )
        .bind(&h.study_instance_uid)
        .bind(patient_id)
        .bind(&h.study_date)
        .bind(&h.study_description)
        .bind(&h.accession_number)
        .bind(&ts)
        .fetch_one(&mut *tx)
        .await?;
        if study_patient != patient_id {
            warn!(
                study = %h.study_instance_uid,
                sop_instance = %h.sop_instance_uid,
                "study already linked to a different PatientID; keeping existing link"
            );
        }

        let (series_id, series_study): (i64, i64) = sqlx::query_as(
            "INSERT INTO series (series_instance_uid, study_fk, modality, series_number,
                                 series_description, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
             ON CONFLICT(series_instance_uid) DO UPDATE SET updated_at = excluded.updated_at
             RETURNING id, study_fk",
        )
        .bind(&h.series_instance_uid)
        .bind(study_id)
        .bind(&h.modality)
        .bind(h.series_number)
        .bind(&h.series_description)
        .bind(&ts)
        .fetch_one(&mut *tx)
        .await?;
        if series_study != study_id {
            warn!(
                series = %h.series_instance_uid,
                sop_instance = %h.sop_instance_uid,
                "series already linked to a different study; keeping existing link"
            );
        }

        let inserted = sqlx::query(
            "INSERT INTO instances (sop_instance_uid, series_fk, sop_class_uid, instance_number,
                                    number_of_frames, transfer_syntax, slice_position, frame_info,
                                    blob_sha256, size_bytes, source, calling_ae, received_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
             ON CONFLICT(sop_instance_uid) DO NOTHING",
        )
        .bind(&h.sop_instance_uid)
        .bind(series_id)
        .bind(&h.sop_class_uid)
        .bind(h.instance_number)
        .bind(i64::from(h.number_of_frames))
        .bind(&h.transfer_syntax)
        .bind(h.slice_position)
        .bind(&frame_info)
        .bind(&blob_hex)
        .bind(size)
        .bind(&rec.source)
        .bind(&rec.calling_ae)
        .bind(&ts)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;

        let outcome = if inserted {
            UpsertOutcome::Inserted
        } else {
            let existing_hex: String =
                sqlx::query_scalar("SELECT blob_sha256 FROM instances WHERE sop_instance_uid = ?1")
                    .bind(&h.sop_instance_uid)
                    .fetch_one(&mut *tx)
                    .await?;
            let existing = parse_hash(&existing_hex)?;
            if existing == rec.blob || policy == DuplicatePolicy::Keep {
                UpsertOutcome::Duplicate { existing }
            } else {
                sqlx::query(
                    "UPDATE instances SET series_fk = ?2, sop_class_uid = ?3, instance_number = ?4,
                         number_of_frames = ?5, transfer_syntax = ?6, slice_position = ?7,
                         frame_info = ?8, blob_sha256 = ?9, size_bytes = ?10, source = ?11,
                         calling_ae = ?12, received_at = ?13
                     WHERE sop_instance_uid = ?1",
                )
                .bind(&h.sop_instance_uid)
                .bind(series_id)
                .bind(&h.sop_class_uid)
                .bind(h.instance_number)
                .bind(i64::from(h.number_of_frames))
                .bind(&h.transfer_syntax)
                .bind(h.slice_position)
                .bind(&frame_info)
                .bind(&blob_hex)
                .bind(size)
                .bind(&rec.source)
                .bind(&rec.calling_ae)
                .bind(&ts)
                .execute(&mut *tx)
                .await?;
                UpsertOutcome::Replaced { previous: existing }
            }
        };
        tx.commit().await?;
        Ok(outcome)
    }

    /// Every blob hash referenced by an instance row.
    pub async fn blob_hashes(&self) -> Result<HashSet<BlobHash>, sqlx::Error> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT DISTINCT blob_sha256 FROM instances")
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(|s| parse_hash(s)).collect()
    }

    /// Whether any instance row references `hash`.
    pub async fn blob_in_use(&self, hash: &BlobHash) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM instances WHERE blob_sha256 = ?1)")
            .bind(hash.to_hex())
            .fetch_one(&self.pool)
            .await
    }

    pub async fn storage_counts(&self) -> Result<StorageCounts, sqlx::Error> {
        let row = sqlx::query(
            "SELECT (SELECT COUNT(*) FROM studies),
                    (SELECT COUNT(*) FROM series),
                    (SELECT COUNT(*) FROM instances),
                    (SELECT COALESCE(SUM(sz), 0) FROM
                        (SELECT MAX(size_bytes) AS sz FROM instances GROUP BY blob_sha256)),
                    (SELECT COUNT(*) FROM
                        (SELECT 1 FROM instances GROUP BY blob_sha256))",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(StorageCounts {
            studies: row.try_get(0)?,
            series: row.try_get(1)?,
            instances: row.try_get(2)?,
            objects_bytes: row.try_get(3)?,
            objects_count: row.try_get(4)?,
        })
    }

    /// All series with study/patient context and instance aggregates, most
    /// recently updated first.
    pub async fn console_series(&self) -> Result<Vec<ConsoleSeriesRow>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT s.series_instance_uid, st.study_instance_uid, p.patient_name,
                    st.study_date, s.modality, s.series_description,
                    COUNT(i.id) AS instance_count,
                    COALESCE(SUM(i.size_bytes), 0) AS size_bytes,
                    s.updated_at
             FROM series s
             JOIN studies st ON st.id = s.study_fk
             JOIN patients p ON p.id = st.patient_fk
             LEFT JOIN instances i ON i.series_fk = s.id
             GROUP BY s.id
             ORDER BY s.updated_at DESC, s.id DESC",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                Ok(ConsoleSeriesRow {
                    series_uid: r.try_get(0)?,
                    study_uid: r.try_get(1)?,
                    patient_name: r.try_get(2)?,
                    study_date: r.try_get(3)?,
                    modality: r.try_get(4)?,
                    series_description: r.try_get(5)?,
                    instance_count: r.try_get(6)?,
                    size_bytes: r.try_get(7)?,
                    updated_at: r.try_get(8)?,
                })
            })
            .collect()
    }

    pub async fn list_series(&self) -> Result<Vec<SeriesRow>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT s.series_instance_uid, st.study_instance_uid, s.modality,
                    s.series_description, COUNT(i.id) AS instance_count
             FROM series s
             JOIN studies st ON st.id = s.study_fk
             LEFT JOIN instances i ON i.series_fk = s.id
             GROUP BY s.id
             ORDER BY s.id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                Ok(SeriesRow {
                    series_instance_uid: r.try_get(0)?,
                    study_instance_uid: r.try_get(1)?,
                    modality: r.try_get(2)?,
                    series_description: r.try_get(3)?,
                    instance_count: r.try_get(4)?,
                })
            })
            .collect()
    }

    /// Image instances of a series (non-NULL `frame_info` only), ordered by
    /// slice position, then instance number.
    pub async fn series_slices(&self, series_uid: &str) -> Result<Vec<SliceRow>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT i.blob_sha256, i.number_of_frames, i.instance_number, i.slice_position,
                    i.frame_info
             FROM instances i
             JOIN series s ON s.id = i.series_fk
             WHERE s.series_instance_uid = ?1 AND i.frame_info IS NOT NULL
             ORDER BY i.slice_position, i.instance_number, i.id",
        )
        .bind(series_uid)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(slice_row).collect()
    }

    pub async fn instance(
        &self,
        sop_instance_uid: &str,
    ) -> Result<Option<InstanceSummary>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT i.sop_instance_uid, s.series_instance_uid, i.blob_sha256, i.source,
                    i.calling_ae
             FROM instances i
             JOIN series s ON s.id = i.series_fk
             WHERE i.sop_instance_uid = ?1",
        )
        .bind(sop_instance_uid)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| {
            Ok(InstanceSummary {
                sop_instance_uid: r.try_get(0)?,
                series_instance_uid: r.try_get(1)?,
                blob: parse_hash(r.try_get(2)?)?,
                source: r.try_get(3)?,
                calling_ae: r.try_get(4)?,
            })
        })
        .transpose()
    }
}

fn slice_row(r: &SqliteRow) -> Result<SliceRow, sqlx::Error> {
    let frames: i64 = r.try_get(1)?;
    let info: &str = r.try_get(4)?;
    Ok(SliceRow {
        blob: parse_hash(r.try_get(0)?)?,
        number_of_frames: u32::try_from(frames)
            .map_err(|_| decode_err(format!("invalid number_of_frames {frames}")))?,
        instance_number: r.try_get(2)?,
        slice_position: r.try_get(3)?,
        frame_info: serde_json::from_str(info)
            .map_err(|e| decode_err(format!("invalid frame_info JSON: {e}")))?,
    })
}
