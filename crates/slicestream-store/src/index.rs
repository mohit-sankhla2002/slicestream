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

use slicestream_codec::CODEC_VERSION;

use crate::{
    BlobHash, BulkValue, CopyFrames, CopyState, FrameTable, InstanceData, InstanceHeader,
    InstanceScan, SCAN_VERSION,
};

/// Everything needed to index one stored instance.
#[derive(Debug, Clone)]
pub struct InstanceRecord {
    pub header: InstanceHeader,
    pub blob: BlobHash,
    pub size_bytes: u64,
    /// Transport the instance arrived on (e.g. `"dimse"`, `"stow"`).
    pub source: String,
    pub calling_ae: Option<String>,
    /// What the instance scan derived (DICOM JSON, frame table, bulk offsets).
    pub data: InstanceData,
    /// True when `data` came from a scan: the row is then marked with
    /// [`SCAN_VERSION`]. False leaves the row unscanned (`scan_version` 0) so
    /// the background scan picks it up, and `data` is ignored.
    pub scanned: bool,
}

/// Lower-case `s` for case-insensitive matching (`patient_name_folded`,
/// `referring_physician_name_folded` and the query values compared with them).
pub fn fold_name(s: &str) -> String {
    s.to_lowercase()
}

/// Which instances a DICOMweb request addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    Study(String),
    /// Study UID, series UID.
    Series(String, String),
    /// Study UID, series UID, SOP Instance UID.
    Instance(String, String, String),
}

/// One instance found by [`Index::find_instances`].
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceRef {
    pub id: i64,
    pub study_uid: String,
    pub series_uid: String,
    pub sop_uid: String,
    pub sop_class_uid: String,
    pub blob: BlobHash,
    pub transfer_syntax: String,
    pub number_of_frames: u32,
    pub frame_info: Option<FrameInfo>,
    /// Scanned at the current [`SCAN_VERSION`].
    pub scanned: bool,
}

/// DICOM JSON availability of one instance, from [`Index::metadata_batch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataState {
    Ready(Vec<u8>),
    /// Not scanned yet.
    Unscanned,
    /// Scanned, but there is no metadata (over the size cap, or the scan failed).
    Unavailable(String),
}

/// [`MetadataState`] without the metadata, from [`Index::metadata_states`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataStateKind {
    /// Metadata of this many bytes is stored.
    Ready(u64),
    /// Not scanned yet.
    Unscanned,
    /// Scanned, but there is no metadata (over the size cap, or the scan failed).
    Unavailable(String),
}

/// What the scan stored beside the metadata, from [`Index::stored_data`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredData {
    pub frames: Option<FrameTable>,
    pub bulk: Vec<BulkValue>,
    pub offsets_valid: bool,
    pub problem: Option<String>,
}

/// [`StoredData`] of an instance as of one blob, from [`Index::stored_data_for`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredDataState {
    /// Scanned: what the scan stored for that blob.
    Ready(StoredData),
    /// Still that blob, not scanned yet.
    Unscanned,
    /// The instance is gone, or an overwrite pointed it at another blob.
    Replaced,
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
    /// Number of NIfTI volumes (one blob each).
    pub nifti_count: i64,
    /// Total bytes over NIfTI volumes.
    pub nifti_bytes: i64,
}

/// Per-outcome item counts of one upload / import run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct UploadCounts {
    pub dicom_stored: u64,
    pub dicom_duplicate: u64,
    pub nifti_stored: u64,
    pub nifti_duplicate: u64,
    pub skipped: u64,
    pub failed: u64,
}

/// One `uploads` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadRow {
    pub id: i64,
    /// `"http_archive"` or `"import"`.
    pub kind: String,
    pub name: String,
    pub remote: Option<String>,
    pub started_at: String,
    pub finished_at: Option<String>,
    /// `"running"`, `"done"` or `"aborted"`.
    pub status: String,
    pub counts: UploadCounts,
}

/// Header facts of a NIfTI volume, as indexed. Store-owned; the server maps
/// its parsed header into this.
#[derive(Debug, Clone, PartialEq)]
pub struct NiftiMeta {
    /// 1 or 2.
    pub version: u8,
    pub dims: Vec<i64>,
    pub pixdim: Vec<f64>,
    pub datatype: i16,
    pub descrip: String,
    /// Stored gzip-compressed (`.nii.gz`).
    pub compressed: bool,
    pub original_name: String,
}

/// One `nifti_volumes` row.
#[derive(Debug, Clone, PartialEq)]
pub struct NiftiRow {
    pub id: i64,
    pub blob: BlobHash,
    pub size_bytes: u64,
    pub meta: NiftiMeta,
    pub patient_fk: Option<i64>,
    pub study_fk: Option<i64>,
    pub upload_fk: i64,
    pub received_at: String,
}

/// A study with its patient, as returned by [`Index::study_info`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StudyInfo {
    pub patient_fk: i64,
    /// `""` when the instances carried no PatientID.
    pub patient_id: String,
    pub patient_name: Option<String>,
    pub study_instance_uid: String,
    pub study_date: Option<String>,
    pub study_description: Option<String>,
}

/// Most series [`Index::console_series`] returns.
pub const CONSOLE_SERIES_LIMIT: usize = 500;

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
    /// The instance id.
    pub id: i64,
    pub blob: BlobHash,
    pub number_of_frames: u32,
    pub instance_number: Option<i64>,
    pub slice_position: Option<f64>,
    pub frame_info: FrameInfo,
    /// `None` unless the instance was scanned with valid offsets.
    pub frames: Option<FrameTable>,
}

/// Handle to the SQLite index. Cheap to clone (wraps a connection pool).
#[derive(Debug, Clone)]
pub struct Index {
    pub(crate) pool: SqlitePool,
}

fn decode_err(msg: String) -> sqlx::Error {
    sqlx::Error::Decode(msg.into())
}

fn parse_hash(s: &str) -> Result<BlobHash, sqlx::Error> {
    BlobHash::from_hex(s).ok_or_else(|| decode_err(format!("invalid blob_sha256 {s:?}")))
}

fn to_i64(v: u64, what: &str) -> Result<i64, sqlx::Error> {
    i64::try_from(v).map_err(|_| sqlx::Error::Encode(format!("{what} exceeds i64").into()))
}

fn to_u64(v: i64, what: &str) -> Result<u64, sqlx::Error> {
    u64::try_from(v).map_err(|_| decode_err(format!("invalid {what} {v}")))
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
                                  accession_number, created_at, updated_at, study_time, study_id,
                                  referring_physician_name, referring_physician_name_folded,
                                  patient_name, patient_name_folded, patient_birth_date,
                                  patient_sex)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
             ON CONFLICT(study_instance_uid) DO UPDATE SET
               updated_at = excluded.updated_at,
               study_time = COALESCE(studies.study_time, excluded.study_time),
               study_id = COALESCE(studies.study_id, excluded.study_id),
               referring_physician_name = COALESCE(studies.referring_physician_name,
                                                   excluded.referring_physician_name),
               referring_physician_name_folded = COALESCE(studies.referring_physician_name_folded,
                                                   excluded.referring_physician_name_folded),
               patient_name = COALESCE(studies.patient_name, excluded.patient_name),
               patient_name_folded = COALESCE(studies.patient_name_folded,
                                              excluded.patient_name_folded),
               patient_birth_date = COALESCE(studies.patient_birth_date,
                                             excluded.patient_birth_date),
               patient_sex = COALESCE(studies.patient_sex, excluded.patient_sex)
             RETURNING id, patient_fk",
        )
        .bind(&h.study_instance_uid)
        .bind(patient_id)
        .bind(&h.study_date)
        .bind(&h.study_description)
        .bind(&h.accession_number)
        .bind(&ts)
        .bind(&h.study_time)
        .bind(&h.study_id)
        .bind(&h.referring_physician_name)
        .bind(h.referring_physician_name.as_deref().map(fold_name))
        .bind(&h.patient_name)
        .bind(h.patient_name.as_deref().map(fold_name))
        .bind(&h.patient_birth_date)
        .bind(&h.patient_sex)
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
                                 series_description, created_at, updated_at,
                                 performed_procedure_step_start_date,
                                 performed_procedure_step_start_time)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6, ?7, ?8)
             ON CONFLICT(series_instance_uid) DO UPDATE SET
               updated_at = excluded.updated_at,
               performed_procedure_step_start_date =
                 COALESCE(series.performed_procedure_step_start_date,
                          excluded.performed_procedure_step_start_date),
               performed_procedure_step_start_time =
                 COALESCE(series.performed_procedure_step_start_time,
                          excluded.performed_procedure_step_start_time)
             RETURNING id, study_fk",
        )
        .bind(&h.series_instance_uid)
        .bind(study_id)
        .bind(&h.modality)
        .bind(h.series_number)
        .bind(&h.series_description)
        .bind(&ts)
        .bind(&h.performed_procedure_step_start_date)
        .bind(&h.performed_procedure_step_start_time)
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
            self.write_instance_data(&mut tx, &h.sop_instance_uid, rec)
                .await?;
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
                self.write_instance_data(&mut tx, &h.sop_instance_uid, rec)
                    .await?;
                UpsertOutcome::Replaced { previous: existing }
            }
        };
        tx.commit().await?;
        Ok(outcome)
    }

    /// Store (or clear) the scan result of the instance just inserted or
    /// replaced in `tx`.
    async fn write_instance_data(
        &self,
        tx: &mut sqlx::SqliteConnection,
        sop_uid: &str,
        rec: &InstanceRecord,
    ) -> Result<(), sqlx::Error> {
        let id: i64 = sqlx::query_scalar("SELECT id FROM instances WHERE sop_instance_uid = ?1")
            .bind(sop_uid)
            .fetch_one(&mut *tx)
            .await?;
        if rec.scanned {
            put_scan_data(tx, id, &rec.data).await?;
            sqlx::query("UPDATE instances SET scan_version = ?2, scan_problem = ?3 WHERE id = ?1")
                .bind(id)
                .bind(SCAN_VERSION)
                .bind(&rec.data.problem)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query("DELETE FROM instance_data WHERE instance_fk = ?1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE instances SET scan_version = 0, scan_problem = NULL WHERE id = ?1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        Ok(())
    }

    /// Every blob hash referenced by an `instances` or `nifti_volumes` row, or
    /// by a `ready` HTJ2K copy.
    ///
    /// Startup recovery deletes every blob not in this set, so any row whose
    /// hash cannot be parsed fails the whole call rather than being skipped.
    pub async fn blob_hashes(&self) -> Result<HashSet<BlobHash>, sqlx::Error> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT blob_sha256 FROM instances
             UNION
             SELECT blob_sha256 FROM nifti_volumes
             UNION
             SELECT blob_sha256 FROM derived_htj2k
             WHERE state = 'ready' AND blob_sha256 IS NOT NULL",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(|s| parse_hash(s)).collect()
    }

    /// Whether any `instances` or `nifti_volumes` row references `hash`.
    pub async fn blob_in_use(&self, hash: &BlobHash) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM instances WHERE blob_sha256 = ?1)
                 OR EXISTS(SELECT 1 FROM nifti_volumes WHERE blob_sha256 = ?1)",
        )
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
                        (SELECT 1 FROM instances GROUP BY blob_sha256)),
                    (SELECT COUNT(*) FROM nifti_volumes),
                    (SELECT COALESCE(SUM(size_bytes), 0) FROM nifti_volumes)",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(StorageCounts {
            studies: row.try_get(0)?,
            series: row.try_get(1)?,
            instances: row.try_get(2)?,
            objects_bytes: row.try_get(3)?,
            objects_count: row.try_get(4)?,
            nifti_count: row.try_get(5)?,
            nifti_bytes: row.try_get(6)?,
        })
    }

    /// Start an upload / import run (status `running`); returns its id.
    /// `kind` is `"http_archive"` or `"import"`.
    pub async fn create_upload(
        &self,
        kind: &str,
        name: &str,
        remote: Option<&str>,
    ) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(
            "INSERT INTO uploads (kind, name, remote, started_at, status)
             VALUES (?1, ?2, ?3, ?4, 'running')
             RETURNING id",
        )
        .bind(kind)
        .bind(name)
        .bind(remote)
        .bind(now())
        .fetch_one(&self.pool)
        .await
    }

    /// Record the end of upload `id` with its final `status` and `counts`.
    /// Fails with [`sqlx::Error::RowNotFound`] if no such upload exists.
    pub async fn finish_upload(
        &self,
        id: i64,
        status: &str,
        counts: &UploadCounts,
    ) -> Result<(), sqlx::Error> {
        let n = sqlx::query(
            "UPDATE uploads SET status = ?2, finished_at = ?3, dicom_stored = ?4,
                 dicom_duplicate = ?5, nifti_stored = ?6, nifti_duplicate = ?7, skipped = ?8,
                 failed = ?9
             WHERE id = ?1",
        )
        .bind(id)
        .bind(status)
        .bind(now())
        .bind(to_i64(counts.dicom_stored, "dicom_stored")?)
        .bind(to_i64(counts.dicom_duplicate, "dicom_duplicate")?)
        .bind(to_i64(counts.nifti_stored, "nifti_stored")?)
        .bind(to_i64(counts.nifti_duplicate, "nifti_duplicate")?)
        .bind(to_i64(counts.skipped, "skipped")?)
        .bind(to_i64(counts.failed, "failed")?)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if n == 0 {
            return Err(sqlx::Error::RowNotFound);
        }
        Ok(())
    }

    /// Mark every `running` upload `aborted` (startup: the server restarted
    /// under it). Returns how many rows changed.
    pub async fn abort_running_uploads(&self) -> Result<u64, sqlx::Error> {
        Ok(sqlx::query(
            "UPDATE uploads SET status = 'aborted', finished_at = ?1 WHERE status = 'running'",
        )
        .bind(now())
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    pub async fn upload(&self, id: i64) -> Result<Option<UploadRow>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT id, kind, name, remote, started_at, finished_at, status, dicom_stored,
                    dicom_duplicate, nifti_stored, nifti_duplicate, skipped, failed
             FROM uploads WHERE id = ?1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| {
            let count =
                |i: usize, what: &str| -> Result<u64, sqlx::Error> { to_u64(r.try_get(i)?, what) };
            Ok(UploadRow {
                id: r.try_get(0)?,
                kind: r.try_get(1)?,
                name: r.try_get(2)?,
                remote: r.try_get(3)?,
                started_at: r.try_get(4)?,
                finished_at: r.try_get(5)?,
                status: r.try_get(6)?,
                counts: UploadCounts {
                    dicom_stored: count(7, "dicom_stored")?,
                    dicom_duplicate: count(8, "dicom_duplicate")?,
                    nifti_stored: count(9, "nifti_stored")?,
                    nifti_duplicate: count(10, "nifti_duplicate")?,
                    skipped: count(11, "skipped")?,
                    failed: count(12, "failed")?,
                },
            })
        })
        .transpose()
    }

    /// Index a committed NIfTI blob in one transaction. Returns the row id
    /// and whether `blob` was already indexed (a duplicate: the existing row
    /// is left unchanged and its id returned).
    pub async fn insert_nifti(
        &self,
        blob: &BlobHash,
        size_bytes: u64,
        meta: &NiftiMeta,
        upload_id: i64,
    ) -> Result<(i64, bool), sqlx::Error> {
        let enc = |e: serde_json::Error| sqlx::Error::Encode(Box::new(e));
        let dims = serde_json::to_string(&meta.dims).map_err(enc)?;
        // Non-finite values are written as JSON `null`.
        let pixdim = serde_json::to_string(&meta.pixdim).map_err(enc)?;
        let blob_hex = blob.to_hex();
        let size = to_i64(size_bytes, "size_bytes")?;

        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let inserted: Option<i64> = sqlx::query_scalar(
            "INSERT INTO nifti_volumes (blob_sha256, size_bytes, compressed, nifti_version,
                                        original_name, dims, pixdim, datatype, descrip,
                                        upload_fk, received_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(blob_sha256) DO NOTHING
             RETURNING id",
        )
        .bind(&blob_hex)
        .bind(size)
        .bind(meta.compressed)
        .bind(i64::from(meta.version))
        .bind(&meta.original_name)
        .bind(&dims)
        .bind(&pixdim)
        .bind(meta.datatype)
        .bind(&meta.descrip)
        .bind(upload_id)
        .bind(now())
        .fetch_optional(&mut *tx)
        .await?;
        let out = match inserted {
            Some(id) => (id, false),
            None => {
                let id: i64 =
                    sqlx::query_scalar("SELECT id FROM nifti_volumes WHERE blob_sha256 = ?1")
                        .bind(&blob_hex)
                        .fetch_one(&mut *tx)
                        .await?;
                (id, true)
            }
        };
        tx.commit().await?;
        Ok(out)
    }

    pub async fn nifti_volume(&self, id: i64) -> Result<Option<NiftiRow>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT id, blob_sha256, size_bytes, compressed, nifti_version, original_name, dims,
                    pixdim, datatype, descrip, patient_fk, study_fk, upload_fk, received_at
             FROM nifti_volumes WHERE id = ?1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| {
            let dims: &str = r.try_get(6)?;
            let pixdim: &str = r.try_get(7)?;
            let version: i64 = r.try_get(4)?;
            let pixdim: Vec<Option<f64>> = serde_json::from_str(pixdim)
                .map_err(|e| decode_err(format!("invalid pixdim JSON: {e}")))?;
            Ok(NiftiRow {
                id: r.try_get(0)?,
                blob: parse_hash(r.try_get(1)?)?,
                size_bytes: to_u64(r.try_get(2)?, "size_bytes")?,
                meta: NiftiMeta {
                    version: u8::try_from(version)
                        .map_err(|_| decode_err(format!("invalid nifti_version {version}")))?,
                    dims: serde_json::from_str(dims)
                        .map_err(|e| decode_err(format!("invalid dims JSON: {e}")))?,
                    pixdim: pixdim.into_iter().map(|v| v.unwrap_or(f64::NAN)).collect(),
                    datatype: r.try_get(8)?,
                    descrip: r.try_get(9)?,
                    compressed: r.try_get(3)?,
                    original_name: r.try_get(5)?,
                },
                patient_fk: r.try_get(10)?,
                study_fk: r.try_get(11)?,
                upload_fk: r.try_get(12)?,
                received_at: r.try_get(13)?,
            })
        })
        .transpose()
    }

    /// Link NIfTI volume `nifti_id` to a patient and study (either may be
    /// `None`). Fails with [`sqlx::Error::RowNotFound`] if no such volume.
    pub async fn link_nifti(
        &self,
        nifti_id: i64,
        patient_fk: Option<i64>,
        study_fk: Option<i64>,
    ) -> Result<(), sqlx::Error> {
        let n =
            sqlx::query("UPDATE nifti_volumes SET patient_fk = ?2, study_fk = ?3 WHERE id = ?1")
                .bind(nifti_id)
                .bind(patient_fk)
                .bind(study_fk)
                .execute(&self.pool)
                .await?
                .rows_affected();
        if n == 0 {
            return Err(sqlx::Error::RowNotFound);
        }
        Ok(())
    }

    /// `(patient_fk, study_fk)` of an indexed instance, via series → study.
    pub async fn instance_links(
        &self,
        sop_instance_uid: &str,
    ) -> Result<Option<(i64, i64)>, sqlx::Error> {
        sqlx::query_as(
            "SELECT st.patient_fk, s.study_fk
             FROM instances i
             JOIN series s ON s.id = i.series_fk
             JOIN studies st ON st.id = s.study_fk
             WHERE i.sop_instance_uid = ?1",
        )
        .bind(sop_instance_uid)
        .fetch_optional(&self.pool)
        .await
    }

    /// Study `study_fk` (a `studies` row id) with its patient, or `None`.
    pub async fn study_info(&self, study_fk: i64) -> Result<Option<StudyInfo>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT st.patient_fk, p.patient_id, p.patient_name, st.study_instance_uid,
                    st.study_date, st.study_description
             FROM studies st
             JOIN patients p ON p.id = st.patient_fk
             WHERE st.id = ?1",
        )
        .bind(study_fk)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| {
            Ok(StudyInfo {
                patient_fk: r.try_get(0)?,
                patient_id: r.try_get(1)?,
                patient_name: r.try_get(2)?,
                study_instance_uid: r.try_get(3)?,
                study_date: r.try_get(4)?,
                study_description: r.try_get(5)?,
            })
        })
        .transpose()
    }

    /// The newest [`CONSOLE_SERIES_LIMIT`] series with study/patient context
    /// and instance aggregates, most recently updated first.
    pub async fn console_series(&self) -> Result<Vec<ConsoleSeriesRow>, sqlx::Error> {
        let rows = sqlx::query(&format!(
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
             ORDER BY s.updated_at DESC, s.id DESC
             LIMIT {CONSOLE_SERIES_LIMIT}"
        ))
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
                    i.frame_info, d.frames, i.id
             FROM instances i
             JOIN series s ON s.id = i.series_fk
             LEFT JOIN instance_data d ON d.instance_fk = i.id AND d.offsets_valid = 1
             WHERE s.series_instance_uid = ?1 AND i.frame_info IS NOT NULL
             ORDER BY i.slice_position, i.instance_number, i.id",
        )
        .bind(series_uid)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(slice_row).collect()
    }

    /// Instances addressed by `scope`, in series, slice position, instance
    /// number order. The UIDs must belong together; an empty list means not
    /// found.
    pub async fn find_instances(&self, scope: &Scope) -> Result<Vec<InstanceRef>, sqlx::Error> {
        let (filter, binds): (&str, Vec<&str>) = match scope {
            Scope::Study(st) => ("st.study_instance_uid = ?1", vec![st]),
            Scope::Series(st, se) => (
                "st.study_instance_uid = ?1 AND s.series_instance_uid = ?2",
                vec![st, se],
            ),
            Scope::Instance(st, se, sop) => (
                "st.study_instance_uid = ?1 AND s.series_instance_uid = ?2
                 AND i.sop_instance_uid = ?3",
                vec![st, se, sop],
            ),
        };
        let sql = format!(
            "SELECT i.id, st.study_instance_uid, s.series_instance_uid, i.sop_instance_uid,
                    i.sop_class_uid, i.blob_sha256, i.transfer_syntax, i.number_of_frames,
                    i.frame_info, i.scan_version
             FROM instances i
             JOIN series s ON s.id = i.series_fk
             JOIN studies st ON st.id = s.study_fk
             WHERE {filter}
             ORDER BY s.id, i.slice_position, i.instance_number, i.id"
        );
        let mut q = sqlx::query(&sql);
        for b in binds {
            q = q.bind(b);
        }
        let rows = q.fetch_all(&self.pool).await?;
        rows.iter()
            .map(|r| {
                let frames: i64 = r.try_get(7)?;
                let info: Option<&str> = r.try_get(8)?;
                let version: i64 = r.try_get(9)?;
                Ok(InstanceRef {
                    id: r.try_get(0)?,
                    study_uid: r.try_get(1)?,
                    series_uid: r.try_get(2)?,
                    sop_uid: r.try_get(3)?,
                    sop_class_uid: r.try_get(4)?,
                    blob: parse_hash(r.try_get(5)?)?,
                    transfer_syntax: r.try_get(6)?,
                    number_of_frames: u32::try_from(frames)
                        .map_err(|_| decode_err(format!("invalid number_of_frames {frames}")))?,
                    frame_info: info
                        .map(serde_json::from_str)
                        .transpose()
                        .map_err(|e| decode_err(format!("invalid frame_info JSON: {e}")))?,
                    scanned: version >= SCAN_VERSION,
                })
            })
            .collect()
    }

    /// DICOM JSON state of each instance in `ids` (callers chunk by 128).
    /// Unknown ids are left out; the rest keep the order of `ids`.
    pub async fn metadata_batch(
        &self,
        ids: &[i64],
    ) -> Result<Vec<(i64, MetadataState)>, sqlx::Error> {
        let list = serde_json::to_string(ids).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
        let rows = sqlx::query(
            "SELECT i.id, i.scan_version, i.scan_problem, d.metadata
             FROM instances i
             LEFT JOIN instance_data d ON d.instance_fk = i.id
             WHERE i.id IN (SELECT value FROM json_each(?1))",
        )
        .bind(list)
        .fetch_all(&self.pool)
        .await?;
        let mut found = std::collections::HashMap::new();
        for r in &rows {
            let version: i64 = r.try_get(1)?;
            let problem: Option<String> = r.try_get(2)?;
            let metadata: Option<Vec<u8>> = r.try_get(3)?;
            let state = match metadata {
                Some(m) => MetadataState::Ready(m),
                None if version < SCAN_VERSION => MetadataState::Unscanned,
                None => MetadataState::Unavailable(
                    problem.unwrap_or_else(|| "no metadata was stored".into()),
                ),
            };
            found.insert(r.try_get::<i64, _>(0)?, state);
        }
        Ok(ids
            .iter()
            .filter_map(|id| found.remove(id).map(|s| (*id, s)))
            .collect())
    }

    /// [`Index::metadata_batch`] without reading the metadata: only whether
    /// it is there, and its length. Unknown ids are left out; the rest keep
    /// the order of `ids`.
    pub async fn metadata_states(
        &self,
        ids: &[i64],
    ) -> Result<Vec<(i64, MetadataStateKind)>, sqlx::Error> {
        let list = serde_json::to_string(ids).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
        // SQLite answers length() of a blob from its header, without
        // reading the blob.
        let rows = sqlx::query(
            "SELECT i.id, i.scan_version, i.scan_problem, length(d.metadata)
             FROM instances i
             LEFT JOIN instance_data d ON d.instance_fk = i.id
             WHERE i.id IN (SELECT value FROM json_each(?1))",
        )
        .bind(list)
        .fetch_all(&self.pool)
        .await?;
        let mut found = std::collections::HashMap::new();
        for r in &rows {
            let version: i64 = r.try_get(1)?;
            let problem: Option<String> = r.try_get(2)?;
            let len: Option<i64> = r.try_get(3)?;
            let state = match len {
                Some(n) => MetadataStateKind::Ready(to_u64(n, "metadata length")?),
                None if version < SCAN_VERSION => MetadataStateKind::Unscanned,
                None => MetadataStateKind::Unavailable(
                    problem.unwrap_or_else(|| "no metadata was stored".into()),
                ),
            };
            found.insert(r.try_get::<i64, _>(0)?, state);
        }
        Ok(ids
            .iter()
            .filter_map(|id| found.remove(id).map(|s| (*id, s)))
            .collect())
    }

    /// Frame table and bulk offsets of instance `id`. `None` when the
    /// instance is unknown or not scanned yet; a failed scan gives an empty
    /// result carrying the problem.
    pub async fn stored_data(&self, id: i64) -> Result<Option<StoredData>, sqlx::Error> {
        Ok(self.stored_row(id).await?.and_then(|(_, data)| data))
    }

    /// [`Index::stored_data`] of instance `id`, but only while the instance
    /// still points at `blob`. An overwrite replaces the blob and the stored
    /// data together, so offsets read here always belong to `blob`'s file.
    pub async fn stored_data_for(
        &self,
        id: i64,
        blob: &BlobHash,
    ) -> Result<StoredDataState, sqlx::Error> {
        Ok(match self.stored_row(id).await? {
            Some((current, data)) if current == *blob => match data {
                Some(d) => StoredDataState::Ready(d),
                None => StoredDataState::Unscanned,
            },
            _ => StoredDataState::Replaced,
        })
    }

    /// The blob of instance `id` and its stored data (`None` when not
    /// scanned yet), read in one statement. `None` for an unknown id.
    async fn stored_row(
        &self,
        id: i64,
    ) -> Result<Option<(BlobHash, Option<StoredData>)>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT i.blob_sha256, i.scan_version, i.scan_problem, d.frames, d.bulk,
                    d.offsets_valid
             FROM instances i
             LEFT JOIN instance_data d ON d.instance_fk = i.id
             WHERE i.id = ?1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(r) = row else { return Ok(None) };
        let blob = parse_hash(r.try_get(0)?)?;
        let version: i64 = r.try_get(1)?;
        if version < SCAN_VERSION {
            return Ok(Some((blob, None)));
        }
        let frames = match r.try_get::<Option<&[u8]>, _>(3)? {
            Some(b) => Some(
                FrameTable::decode(b).ok_or_else(|| decode_err("invalid frame table".into()))?,
            ),
            None => None,
        };
        let bulk = match r.try_get::<Option<&str>, _>(4)? {
            Some(b) => serde_json::from_str(b)
                .map_err(|e| decode_err(format!("invalid bulk JSON: {e}")))?,
            None => Vec::new(),
        };
        let data = StoredData {
            frames,
            bulk,
            offsets_valid: r.try_get::<Option<bool>, _>(5)?.unwrap_or(false),
            problem: r.try_get(2)?,
        };
        Ok(Some((blob, Some(data))))
    }

    /// Up to `limit` instances not scanned at [`SCAN_VERSION`] yet, in id
    /// order, as `(id, SOP Instance UID, blob)`.
    pub async fn unscanned(&self, limit: i64) -> Result<Vec<(i64, String, BlobHash)>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT id, sop_instance_uid, blob_sha256 FROM instances
             WHERE scan_version < ?1 ORDER BY id LIMIT ?2",
        )
        .bind(SCAN_VERSION)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| Ok((r.try_get(0)?, r.try_get(1)?, parse_hash(r.try_get(2)?)?)))
            .collect()
    }

    /// Up to `limit` scanned image instances without a copy row for their
    /// current blob at [`CODEC_VERSION`], newest first, as
    /// `(id, SOP Instance UID, blob)`.
    pub async fn copy_candidates(
        &self,
        limit: i64,
    ) -> Result<Vec<(i64, String, BlobHash)>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT i.id, i.sop_instance_uid, i.blob_sha256 FROM instances i
             WHERE i.frame_info IS NOT NULL AND i.scan_version = ?1
               AND NOT EXISTS (
                 SELECT 1 FROM derived_htj2k c
                 WHERE c.instance_fk = i.id AND c.source_blob = i.blob_sha256
                   AND c.codec_version = ?2)
             ORDER BY i.id DESC LIMIT ?3",
        )
        .bind(SCAN_VERSION)
        .bind(CODEC_VERSION)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| Ok((r.try_get(0)?, r.try_get(1)?, parse_hash(r.try_get(2)?)?)))
            .collect()
    }

    /// Record the copy state of instance `id` made from `source`, but only if
    /// the instance still points at `source`. Returns whether it was written.
    pub async fn write_copy(
        &self,
        id: i64,
        source: &BlobHash,
        state: &CopyState,
    ) -> Result<bool, sqlx::Error> {
        let (name, problem, blob, frames) = match state {
            CopyState::Ready { blob, frames } => {
                ("ready", None, Some(blob.to_hex()), Some(frames.encode()))
            }
            CopyState::NotEligible(p) => ("not_eligible", Some(p.as_str()), None, None),
            CopyState::Failed(p) => ("failed", Some(p.as_str()), None, None),
        };
        let done = sqlx::query(
            "INSERT OR REPLACE INTO derived_htj2k
               (instance_fk, source_blob, codec_version, state, problem, blob_sha256, frames)
             SELECT id, blob_sha256, ?3, ?4, ?5, ?6, ?7 FROM instances
             WHERE id = ?1 AND blob_sha256 = ?2",
        )
        .bind(id)
        .bind(source.to_hex())
        .bind(CODEC_VERSION)
        .bind(name)
        .bind(problem)
        .bind(blob)
        .bind(frames)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected() > 0)
    }

    /// The copy state of instance `id` for `source`; `None` when missing,
    /// made from another blob, or made by an older codec.
    pub async fn copy_for(
        &self,
        id: i64,
        source: &BlobHash,
    ) -> Result<Option<CopyState>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT state, problem, blob_sha256, frames FROM derived_htj2k
             WHERE instance_fk = ?1 AND source_blob = ?2 AND codec_version >= ?3",
        )
        .bind(id)
        .bind(source.to_hex())
        .bind(CODEC_VERSION)
        .fetch_optional(&self.pool)
        .await?;
        let Some(r) = row else { return Ok(None) };
        let problem: Option<String> = r.try_get(1)?;
        let state: &str = r.try_get(0)?;
        Ok(Some(match state {
            "ready" => {
                let blob: Option<&str> = r.try_get(2)?;
                let frames: Option<&[u8]> = r.try_get(3)?;
                let blob =
                    parse_hash(blob.ok_or_else(|| decode_err("ready copy without blob".into()))?)?;
                let frames = frames
                    .and_then(CopyFrames::decode)
                    .ok_or_else(|| decode_err("invalid copy frame table".into()))?;
                CopyState::Ready { blob, frames }
            }
            "not_eligible" => CopyState::NotEligible(problem.unwrap_or_default()),
            "failed" => CopyState::Failed(problem.unwrap_or_default()),
            other => return Err(decode_err(format!("invalid copy state {other}"))),
        }))
    }

    /// SOP Instance UID and blob of instance `id`; `None` when unknown.
    pub async fn instance_blob(&self, id: i64) -> Result<Option<(String, BlobHash)>, sqlx::Error> {
        let row = sqlx::query("SELECT sop_instance_uid, blob_sha256 FROM instances WHERE id = ?1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|r| Ok((r.try_get(0)?, parse_hash(r.try_get(1)?)?)))
            .transpose()
    }

    /// Record the scan of instance `id`, but only if it still points at
    /// `blob` (an overwrite may have replaced it meanwhile). Fills empty
    /// study and series columns. Returns whether anything was written.
    pub async fn write_scan(
        &self,
        id: i64,
        blob: &BlobHash,
        scan: &InstanceScan,
    ) -> Result<bool, sqlx::Error> {
        let h = &scan.header;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let changed = sqlx::query(
            "UPDATE instances SET scan_version = ?3, scan_problem = ?4
             WHERE id = ?1 AND blob_sha256 = ?2",
        )
        .bind(id)
        .bind(blob.to_hex())
        .bind(SCAN_VERSION)
        .bind(&scan.data.problem)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if changed == 0 {
            return Ok(false);
        }
        put_scan_data(&mut tx, id, &scan.data).await?;
        sqlx::query(
            "UPDATE studies SET
               study_time = COALESCE(study_time, ?2),
               study_id = COALESCE(study_id, ?3),
               referring_physician_name = COALESCE(referring_physician_name, ?4),
               referring_physician_name_folded = COALESCE(referring_physician_name_folded, ?5),
               patient_name = COALESCE(patient_name, ?6),
               patient_name_folded = COALESCE(patient_name_folded, ?7),
               patient_birth_date = COALESCE(patient_birth_date, ?8),
               patient_sex = COALESCE(patient_sex, ?9)
             WHERE id = (SELECT s.study_fk FROM instances i JOIN series s ON s.id = i.series_fk
                         WHERE i.id = ?1)",
        )
        .bind(id)
        .bind(&h.study_time)
        .bind(&h.study_id)
        .bind(&h.referring_physician_name)
        .bind(h.referring_physician_name.as_deref().map(fold_name))
        .bind(&h.patient_name)
        .bind(h.patient_name.as_deref().map(fold_name))
        .bind(&h.patient_birth_date)
        .bind(&h.patient_sex)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE series SET
               performed_procedure_step_start_date =
                 COALESCE(performed_procedure_step_start_date, ?2),
               performed_procedure_step_start_time =
                 COALESCE(performed_procedure_step_start_time, ?3)
             WHERE id = (SELECT series_fk FROM instances WHERE id = ?1)",
        )
        .bind(id)
        .bind(&h.performed_procedure_step_start_date)
        .bind(&h.performed_procedure_step_start_time)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Record that scanning instance `id` failed, so it is not retried. Only
    /// applies if the row still points at `blob`. Returns whether it did.
    pub async fn write_scan_failure(
        &self,
        id: i64,
        blob: &BlobHash,
        problem: &str,
    ) -> Result<bool, sqlx::Error> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let changed = sqlx::query(
            "UPDATE instances SET scan_version = ?3, scan_problem = ?4
             WHERE id = ?1 AND blob_sha256 = ?2",
        )
        .bind(id)
        .bind(blob.to_hex())
        .bind(SCAN_VERSION)
        .bind(problem)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if changed == 0 {
            return Ok(false);
        }
        sqlx::query("DELETE FROM instance_data WHERE instance_fk = ?1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
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
        id: r.try_get(6)?,
        blob: parse_hash(r.try_get(0)?)?,
        number_of_frames: u32::try_from(frames)
            .map_err(|_| decode_err(format!("invalid number_of_frames {frames}")))?,
        instance_number: r.try_get(2)?,
        slice_position: r.try_get(3)?,
        frame_info: serde_json::from_str(info)
            .map_err(|e| decode_err(format!("invalid frame_info JSON: {e}")))?,
        // An unreadable table falls back to reading frames by header.
        frames: r
            .try_get::<Option<&[u8]>, _>(5)?
            .and_then(FrameTable::decode),
    })
}

/// `INSERT OR REPLACE` the `instance_data` row of instance `id`.
async fn put_scan_data(
    tx: &mut sqlx::SqliteConnection,
    id: i64,
    data: &InstanceData,
) -> Result<(), sqlx::Error> {
    let bulk = serde_json::to_string(&data.bulk).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
    sqlx::query(
        "INSERT OR REPLACE INTO instance_data (instance_fk, metadata, frames, bulk, offsets_valid)
         VALUES (?1, ?2, ?3, ?4, ?5)",
    )
    .bind(id)
    .bind(&data.metadata)
    .bind(data.frames.as_ref().map(FrameTable::encode))
    .bind(bulk)
    .bind(data.offsets_valid)
    .execute(&mut *tx)
    .await?;
    Ok(())
}
