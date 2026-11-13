# Archive and NIfTI Ingest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest ZIP / 7z / RAR / tar / tar.gz archives (nested in any combination) and NIfTI volumes through the console, a new HTTP job API and `import`, storing DICOM through the existing pipeline and NIfTI as untouched content-addressed blobs.

**Architecture:** A new `slicestream-archive` crate does pure work: content detection, NIfTI header parsing, and a synchronous recursive archive walker with safety limits that hands each leaf to a sink. The store crate learns blob kinds, a NIfTI ingest path, `uploads`/`nifti_volumes` tables and a kind-agnostic orphan rule. The server crate runs walks on blocking threads, bridges leaves into async ingest over a bounded channel, links NIfTI to patients, and exposes a job API used by the console; `import` reuses the same processor in-process.

**Tech Stack:** Rust 2024 (MSRV 1.88); `zip` 9.x, `sevenz-rust2`, `tar`, `flate2`, `unrar` (cargo feature `rar`, default on), `tempfile`; sqlx 0.8 SQLite; plain JS console.

**Spec:** `docs/superpowers/specs/2026-11-13-archive-ingest-design.md`

## Global Constraints

- Rust MSRV 1.88, edition 2024; `cargo +1.88 check --workspace --all-targets` passes (also with `--no-default-features` on crates that have the `rar` feature). Every new dependency builds on 1.88; pin versions that do.
- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, `node --test 'web/tests/*.test.js'` clean after every task.
- Detection is by content only (magic bytes per the spec's table), never by file name or extension.
- Entry names inside archives are never used as filesystem paths. Symlinks, hard links, devices and directories are ignored. Nothing is written outside `<root>/objects/` and `<root>/tmp/`.
- Never load a whole archive or entry into memory; stream through bounded buffers.
- DICOM goes only through `Ingestor::ingest`. NIfTI blobs are stored exactly as received (`.nii` or `.nii.gz`).
- Orphan rule: a blob in `objects/` (any extension) referenced by no `instances` or `nifti_volumes` row is deleted at startup recovery.
- Limits and defaults exactly as the spec: `max_depth = 4`, `max_entries = 200000`, `max_expanded_bytes = 68719476736`, `min_free_bytes = 2147483648`, `max_concurrent_jobs = 2`, `job_retention = 50`, queue cap `4 × max_concurrent_jobs`, in-memory entries cap 10 000 per job.
- Entry path format: archive names joined with `!/`, e.g. `outer.zip!/p1.zip!/dir/IM1`.
- Security as today: Host allow-list on every route; ingest routes and `/api/archives*` never get CORS; optional bearer token on `POST /api/archives`.
- Test data only from the phantom generator or synthesized in tests. Never real patient data.

## Review Focus

1. A ZIP of per-patient ZIPs where each inner ZIP holds DICOM + a NIfTI: each NIfTI links to its own patient, not "unassigned". Pinned in Task 5 (`link_nested_per_patient`, `two_patient_nested_zip_links_each_nifti`).
2. A crash between NIfTI blob rename and index commit: next startup deletes the orphan; an indexed NIfTI blob is never deleted. Pinned in Task 2 (`recover_keeps_indexed_nifti_and_removes_orphans`).
3. Zip bomb (one small entry inflating past `max_expanded_bytes`) and nesting deeper than `max_depth`: upload aborts / nested archive fails with the documented reason; `tmp/` is empty afterwards. Pinned in Task 3 (`expanded_limit_aborts`, `too_deep_fails_that_archive`) and Task 5 (`tmp_empty_after_job`).
4. Messy real-world exports: files without extension, `DICOMDIR`, `__MACOSX/._*`, `.exe`, encrypted or truncated inner ZIP — DICOM still found, junk skipped, broken inner archive fails alone. Pinned in Task 3 (`messy_export`) and Task 5 (`encrypted_and_truncated_fail_alone`).
5. Server restarted mid-job: `uploads` row becomes `aborted` ("server restarted"); console job URL returns 404, not a hang. Pinned in Task 2 (`abort_running_uploads`) and Task 6 (`unknown_job_404`).

---

### Task 1: `slicestream-archive` crate — detection and NIfTI header

**Files:**
- Create: `crates/slicestream-archive/Cargo.toml`, `src/lib.rs`, `src/detect.rs`, `src/nifti.rs`
- Modify: workspace `Cargo.toml` (member)
- Test: unit tests in `detect.rs`, `nifti.rs`

**Interfaces:**
- Produces:
  - `pub enum Kind { Zip, SevenZ, Rar, Gzip, Tar, Dicom, Nifti, Other }` (`Debug, Clone, Copy, PartialEq, Eq`).
  - `pub const SNIFF_LEN: usize = 1024;`
  - `pub fn detect(head: &[u8]) -> Kind` — `head` is the first `min(SNIFF_LEN, len)` bytes. Order: Zip (`PK\x03\x04`, `PK\x05\x06`), SevenZ, Rar (v4 `Rar!\x1A\x07\x00`, v5 `Rar!\x1A\x07\x01\x00`), Gzip (`\x1F\x8B`), Tar (`ustar` at 257), Dicom (`DICM` at 128), Nifti (per spec: NIfTI-1 `sizeof_hdr` 348 at 0 little- or big-endian AND magic `n+1\0` or `ni1\0` at 344; NIfTI-2 540 at 0 AND `n+2\0` at 4), else Other.
  - `pub struct NiftiHeader { pub version: u8, pub dims: Vec<i64>, pub pixdim: Vec<f64>, pub datatype: i16, pub descrip: String }` (`Debug, Clone, PartialEq, serde::Serialize`).
  - `pub fn parse_nifti_header(head: &[u8]) -> Result<NiftiHeader, NiftiError>` — handles both endiannesses (detect by `sizeof_hdr`), NIfTI-1 (`dim` i16[8] at 40, `datatype` i16 at 70, `pixdim` f32[8] at 76, `descrip` 80 bytes at 148) and NIfTI-2 (`datatype` i16 at 12, `dim` i64[8] at 16, `pixdim` f64[8] at 104, `descrip` 80 bytes at 240). `dims` = `dim[1..=dim[0]]` (`dim[0]` clamped to 1..=7), `pixdim` = `pixdim[1..=dim[0]]`; `descrip` trimmed at first NUL, lossy UTF-8. `pub enum NiftiError { TooShort, BadMagic }` with `Display`.

- [ ] **Step 1: Failing tests** — `detect_each_magic` (one buffer per row of the spec table → expected Kind, incl. both RAR versions and ZIP empty-archive magic), `detect_short_and_junk_is_other` (empty, 3 bytes, random text, PDF `%PDF`), `detect_nifti_requires_both_size_and_magic`, `parse_nifti1_le`, `parse_nifti1_be`, `parse_nifti2_le` (headers synthesized in the test with a helper `fn nifti1(dims: &[i16], pixdim: &[f32], datatype: i16, descrip: &str, big_endian: bool) -> Vec<u8>` and a NIfTI-2 equivalent; assert all fields), `parse_rejects_short`.
- [ ] **Step 2: Run** `cargo test -p slicestream-archive` — fails to compile.
- [ ] **Step 3: Implement.** Crate metadata uses `workspace = true` fields like the other crates.
- [ ] **Step 4: Run** tests, fmt, clippy, `cargo +1.88 check -p slicestream-archive` — pass.
- [ ] **Step 5: Commit** `feat(archive): content detection and NIfTI header parsing`

---

### Task 2: Store — blob kinds, migration 0002, recovery rule, NIfTI ingest

**Files:**
- Create: `crates/slicestream-store/migrations/0002_uploads_nifti.sql`
- Modify: `crates/slicestream-store/src/blob.rs`, `index.rs`, `ingest.rs`, `stats.rs`, `lib.rs`
- Test: `crates/slicestream-store/tests/ingest.rs`, `tests/index.rs`, unit tests in `blob.rs`

**Interfaces:**
- Produces (store crate):
  - `pub enum BlobKind { Dicom, Nifti, NiftiGz }` with `pub fn ext(self) -> &'static str` (`"dcm"`, `"nii"`, `"nii.gz"`).
  - `FsBlobStore::commit(&self, spooled: SpooledBlob, kind: BlobKind) -> io::Result<BlobHash>`, `path_of(&self, &BlobHash, BlobKind) -> PathBuf`, `delete(&self, &BlobHash, BlobKind)`, `list(&self) -> io::Result<Vec<(BlobHash, BlobKind)>>` (files whose names are not `<64 hex>.<known ext>` are ignored, as today). Update every existing caller to pass `BlobKind::Dicom`.
  - `FsBlobStore::spool_sync(&self, reader: &mut dyn std::io::Read, limit: u64) -> Result<SpooledBlob, SpoolError>` — blocking twin of `TempBlob` (hash while copying in 64 KiB chunks, fsync, `TooLarge` past `limit`, temp file removed on error/drop). `Ingestor::spool_sync(&self, reader) -> Result<SpooledBlob, IngestError>` uses `max_instance_bytes`.
  - Migration `0002_uploads_nifti.sql` with tables `uploads` and `nifti_volumes` exactly as the spec's column lists (`nifti_volumes.blob_sha256` UNIQUE; `patient_fk`/`study_fk` nullable FKs; `upload_fk` FK; indexes on `nifti_volumes(upload_fk)`, `nifti_volumes(patient_fk)`).
  - `pub struct UploadCounts { pub dicom_stored: u64, pub dicom_duplicate: u64, pub nifti_stored: u64, pub nifti_duplicate: u64, pub skipped: u64, pub failed: u64 }` (`Default, Clone, Copy, Debug, PartialEq, Eq, Serialize`).
  - `Index::create_upload(&self, kind: &str, name: &str, remote: Option<&str>) -> Result<i64, sqlx::Error>` (status `running`); `Index::finish_upload(&self, id: i64, status: &str, counts: &UploadCounts) -> Result<(), sqlx::Error>`; `Index::abort_running_uploads(&self) -> Result<u64, sqlx::Error>` (sets `aborted`, returns rows changed); `Index::link_nifti(&self, nifti_id: i64, patient_fk: Option<i64>, study_fk: Option<i64>) -> Result<(), sqlx::Error>`; `Index::instance_links(&self, sop_instance_uid: &str) -> Result<Option<(i64, i64)>, sqlx::Error>` (patient_fk, study_fk via series→study); `Index::blob_hashes()` now returns hashes from both `instances` and `nifti_volumes`; `StorageCounts` gains `nifti_count: i64` and `nifti_bytes: i64`.
  - `pub struct NiftiMeta { pub version: u8, pub dims: Vec<i64>, pub pixdim: Vec<f64>, pub datatype: i16, pub descrip: String, pub compressed: bool, pub original_name: String }` (store-owned; the server maps from `NiftiHeader`).
  - `pub struct StoredNifti { pub id: i64, pub duplicate: bool, pub blob: BlobHash }`.
  - `Ingestor::ingest_nifti(&self, spooled: SpooledBlob, meta: NiftiMeta, upload_id: i64) -> Result<StoredNifti, IngestError>` — same order as DICOM: commit blob (kind `NiftiGz` if `compressed` else `Nifti`) with fsyncs, then one transaction inserting the row (`ON CONFLICT(blob_sha256) DO NOTHING`; duplicate → return existing id, `duplicate: true`). Cancellation-safe like `ingest` (critical section in a spawned task). On index error the new blob is removed if unreferenced.
  - `Ingestor::recover()` additionally calls `abort_running_uploads()`; `RecoveryReport` gains `uploads_aborted: u64`.
  - `IngestStats` gains `nifti_stored: u64`, `nifti_duplicate: u64`.

- [ ] **Step 1: Failing tests**
  - blob unit: `nifti_kinds_have_own_extension` (`path_of(h, NiftiGz)` ends with `.nii.gz`), `list_returns_kinds`, `spool_sync_hashes_and_limits` (hash equals `sha256(bytes)`; `limit` exceeded → `TooLarge` and no file left in `tmp/`).
  - index: `upload_lifecycle` (create → running, finish → done with counts, `abort_running_uploads` changes only running rows), `blob_hashes_include_nifti`, `storage_counts_include_nifti`.
  - ingest: `ingest_nifti_stores_and_dedups` (stored once; second identical → duplicate, same id; file at `objects/aa/bb/<hash>.nii.gz`), `recover_keeps_indexed_nifti_and_removes_orphans` (indexed `.nii.gz` survives; a hand-placed unreferenced `.nii` and `.dcm` are removed; counts in report), `abort_running_uploads` via `recover()` (`uploads_aborted == 1`), `instance_links_after_ingest`.
- [ ] **Step 2: Run** `cargo test -p slicestream-store` — fails.
- [ ] **Step 3: Implement**; update the server and dimse crates' call sites if they use changed signatures (compile the workspace).
- [ ] **Step 4: Run** `cargo test --workspace`, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(store): blob kinds, uploads and NIfTI volumes, kind-agnostic recovery`

---

### Task 3: Archive walker — ZIP, tar, gzip, nesting and limits

**Files:**
- Create: `crates/slicestream-archive/src/walk.rs`, `src/count.rs` (counting/limiting reader), `crates/slicestream-archive/tests/walk.rs`, `tests/support/mod.rs` (fixture builders)
- Modify: `crates/slicestream-archive/Cargo.toml` (`zip` with deflate only, `tar`, `flate2`, `tempfile`, `fs4`), `src/lib.rs`
- Dev-deps: `slicestream-core` (phantom generator functions if exposed; otherwise synthesize minimal Part-10 bytes in `support`: `DICM` preamble + a few elements is enough because the walker only classifies).

**Interfaces:**
- Produces:
  - `pub struct Limits { pub max_depth: u32, pub max_entries: u64, pub max_expanded_bytes: u64, pub min_free_bytes: u64 }` with `Default` = spec values.
  - `pub struct Leaf<'a> { pub path: &'a str, pub kind: Kind /* Dicom | Nifti | Other */, pub compressed: bool /* NIfTI stored as .nii.gz */, pub head: &'a [u8] /* first ≤ SNIFF_LEN decompressed bytes */, pub reader: &'a mut dyn std::io::Read /* raw bytes to store: for a gz NIfTI the gz stream, else the plain content */ }`.
  - `pub enum Event<'a> { ContainerFailed { path: &'a str, reason: String }, Skipped { path: &'a str } }`.
  - `pub trait Sink { fn leaf(&mut self, leaf: Leaf<'_>) -> std::io::Result<()>; fn event(&mut self, ev: Event<'_>); }` — `leaf` is called only for Dicom and Nifti; Other → `Event::Skipped` (the walker drains nothing it doesn't need).
  - `pub enum WalkError { Aborted(String), Io(std::io::Error) }` with `Display`. Abort reasons, exact text: `"too many entries (limit N)"`, `"expanded size limit reached (N bytes)"`, `"disk almost full"`.
  - `pub fn walk_file(path: &std::path::Path, display_name: &str, tmp_dir: &std::path::Path, limits: &Limits, sink: &mut dyn Sink) -> Result<WalkStats, WalkError>` — `display_name` is the top-level path prefix (e.g. `outer.zip`); the top-level file itself may be any Kind (a bare DICOM/NIfTI becomes one leaf with path `display_name`). `pub struct WalkStats { pub entries_seen: u64, pub bytes_expanded: u64 }`.
- Rules: each container entry is classified from its first bytes; nested containers are spooled to a `tempfile::NamedTempFile` in `tmp_dir` (deleted on drop) then walked with depth+1; `depth > max_depth` → `ContainerFailed(path, "too deeply nested (limit N)")` and continue; gzip: spool raw, peek decompressed head, classify inner: Tar → walk the tar from a gz decoder; Nifti → leaf with `compressed = true`, `reader` = raw gz file; Dicom → leaf with decompressed reader; Other → skipped. A broken or encrypted container → `ContainerFailed` with reason `"encrypted archive"` / `"cannot read archive: <err>"` and the walk continues with its siblings. `bytes_expanded` counts every byte read out of a decoder or entry stream (leaf readers are wrapped so the sink's reads count) plus nested spool bytes; crossing the limit → `Aborted`. `entries_seen` counts every file entry (not directories); crossing `max_entries` → `Aborted`. Every 64 MiB counted, check `fs4::available_space(tmp_dir) < min_free_bytes` → `Aborted("disk almost full")`. tar symlink/hardlink/device entries and ZIP directory entries are ignored (not counted as skipped). Entry paths: `<parent>!/<entry name>`.

- [ ] **Step 1: Failing tests** in `tests/walk.rs` with a recording sink (stores `(path, kind, compressed, sha256 of bytes read)` and events): `single_zip_finds_dicom_without_extension`, `zip_of_zips_paths` (expect `outer.zip!/p1.zip!/IM1` style paths), `tar_gz_and_plain_tar`, `gz_nifti_is_compressed_leaf` (sink reads raw gz bytes; their sha equals the gz bytes' sha), `plain_nifti_leaf`, `bare_dicom_top_level`, `messy_export` (DICOMDIR, `__MACOSX/._IM1`, `viewer.exe` bytes, PDF → skipped events; DICOM found), `too_deep_fails_that_archive` (depth `max_depth+1` nested zip → `ContainerFailed` with "too deeply nested", siblings still walked), `expanded_limit_aborts` (zip bomb: 10 MiB of zeros deflated, `max_expanded_bytes = 1 MiB` → `Aborted("expanded size limit reached (1048576 bytes)")`), `entry_limit_aborts`, `truncated_zip_fails_alone` (inner truncated zip → `ContainerFailed`, outer siblings walked), `tmp_dir_empty_after_walk` (also after abort).
- [ ] **Step 2: Run** `cargo test -p slicestream-archive` — fails.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** tests, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(archive): recursive ZIP/tar/gzip walker with safety limits`

---

### Task 4: Archive walker — 7z and RAR

**Files:**
- Modify: `crates/slicestream-archive/Cargo.toml` (`sevenz-rust2` with the smallest feature set that reads LZMA/LZMA2/deflate/bzip2 and detects AES; `unrar` optional behind feature `rar`, `default = ["rar"]`), `src/walk.rs`
- Create: `crates/slicestream-archive/tests/fixtures/` (RAR fixture only, if committed), `tests/walk_7z_rar.rs`

**Interfaces:**
- Consumes/extends Task 3's walker: Kind::SevenZ and Kind::Rar become containers with the same rules (nesting, limits, failures, path format). RAR goes through the `unrar` crate on the spooled file; 7z through `sevenz-rust2` reading entries as streams. Encrypted 7z/RAR → `ContainerFailed("encrypted archive")`. With feature `rar` off: `ContainerFailed("RAR support not built in")`.
- RAR fixture: Rust has no RAR writer and only `unrar` (reader) is installed. Either commit a tiny RAR made from synthetic data (built by the implementer by writing a minimal RAR 5 *stored* archive in a small throwaway generator — RAR 5 stored format: signature, main header, file header with CRC32, data, end header) and verify it with `/usr/bin/unrar t` before committing, or generate it in test code with the same writer. The fixture must contain a synthetic DICOM (DICM preamble) and a nested ZIP.

- [ ] **Step 1: Failing tests** — `seven_z_with_nested_tar_gz` (7z built in-test with `sevenz-rust2`'s writer containing a tar.gz containing a DICOM → leaf path `x.7z!/inner.tar.gz!/IM1`), `encrypted_7z_fails_alone`, `rar_with_dicom_and_nested_zip` (`#[cfg(feature = "rar")]`), `rar_without_feature_reports_not_built_in` (`#[cfg(not(feature = "rar"))]`, run with `--no-default-features`), `zip_inside_rar_inside_7z` if cheap (else document why not).
- [ ] **Step 2: Run** `cargo test -p slicestream-archive` and `cargo test -p slicestream-archive --no-default-features` — fail.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** both test commands, fmt, clippy (both feature sets), `cargo +1.88 check -p slicestream-archive` (both) — pass. Add the `--no-default-features` test run as a step in `.github/workflows/ci.yml` `test` job; the `interop`/`msrv` jobs stay as they are (the unrar crate builds C++: ensure the ubuntu runner has a C++ compiler — it does by default).
- [ ] **Step 5: Commit** `feat(archive): 7z and RAR (feature "rar") support`

---

### Task 5: Upload processor — walk → ingest bridge, results, linking (server crate)

**Files:**
- Create: `crates/slicestream-server/src/archive_ingest.rs`, `src/linking.rs`
- Modify: `crates/slicestream-server/Cargo.toml` (`slicestream-archive` dependency; feature `rar` forwarding, default on), `src/lib.rs`
- Test: unit tests in `linking.rs`; `crates/slicestream-server/tests/archives.rs`

**Interfaces:**
- Consumes: Task 1 `detect`, `parse_nifti_header`; Task 2 `Ingestor::{spool_sync, ingest, ingest_nifti, index().create_upload, finish_upload, link_nifti, instance_links}`, `NiftiMeta`, `UploadCounts`; Task 3/4 `walk_file`, `Sink`, `Leaf`, `Event`, `Limits`, `WalkError`.
- Produces:
  - `pub enum EntryOutcome { Stored, Duplicate, Replaced, Skipped, Failed }` and `pub struct EntryResult { pub path: String, pub kind: &'static str /* "dicom"|"nifti"|"archive"|"other" */, pub outcome: EntryOutcome, pub code: Option<String> /* DICOM status hex like "C000" */, pub reason: Option<String>, pub sop_instance_uid: Option<String>, pub size: Option<u64> }` (serde: outcome lowercase).
  - `pub struct PatientSummary { pub patient_id: String, pub patient_name: Option<String>, pub studies: Vec<StudySummary> }`, `pub struct StudySummary { pub study_uid: String, pub study_date: Option<String>, pub description: Option<String>, pub series: u64, pub instances: u64 }`, `pub struct NiftiSummary { pub linked: u64, pub unassigned: u64 }`.
  - `pub trait Progress: Send + Sync { fn entry(&self, r: &EntryResult); fn stats(&self, entries_seen: u64, bytes_expanded: u64); }` (job state and CLI implement it).
  - `pub struct UploadOutcome { pub upload_id: i64, pub status: &'static str /* "done"|"aborted" */, pub reason: Option<String>, pub counts: UploadCounts, pub patients: Vec<PatientSummary>, pub nifti: NiftiSummary, pub entries_seen: u64, pub bytes_expanded: u64 }`.
  - `pub async fn process_upload(ingestor: Arc<Ingestor>, file: PathBuf, display_name: String, upload_id: i64, source: UploadSource, limits: Limits, progress: Arc<dyn Progress>) -> UploadOutcome` where `pub enum UploadSource { Http { remote: Option<SocketAddr> }, Import { root: PathBuf } }`. It runs `walk_file` in `spawn_blocking`; the sink spools each leaf with `Ingestor::spool_sync` and sends `(path, LeafJob)` over a `tokio::sync::mpsc` channel of capacity 8 with `blocking_send`; an async consumer calls `ingest` (DICOM, `IngestSource::Http`/`Import` as appropriate) or `ingest_nifti` (NiftiMeta from `parse_nifti_header(head)`, `original_name` = last path segment), records `EntryResult`s, updates counts, collects `(path, patient_fk, study_fk)` for stored/duplicate DICOM via `instance_links`, and `(nifti_id, path)` for NIfTI. Skipped events → Skipped; `ContainerFailed` → Failed with kind `"archive"`. NIfTI header parse error → Failed ("invalid NIfTI header"). After the walk: run linking, call `link_nifti`, `finish_upload`, build the patient summary (from the stored/duplicate DICOM's index rows). `WalkError::Aborted(r)` → status `aborted`, reason `r`; already-stored items stay.
  - `pub async fn process_dir(ingestor, dir: &Path, recursive: bool, upload_id, limits, progress) -> UploadOutcome` — walks the folder (sorted, as `import_dir` does today), each file classified by content: DICOM/NIfTI are leaves with path relative to `dir`; archives are walked with `display_name` = relative path; others skipped. One upload row, one linking pass at the end.
  - `linking.rs`: `pub fn link(nifti: &[(i64, String)], dicom: &[(String, i64, i64)]) -> Vec<(i64, Option<i64>, Option<i64>)>` — for each NIfTI path, candidate scopes are its ancestor prefixes, nearest first (cut after each `/` and each `!/`, then the empty prefix = whole upload). The first scope containing any DICOM path decides: exactly one patient → `(Some(p), study)` where study is `Some(s)` only if exactly one study in that scope; more than one patient → `(None, None)`; no scope with DICOM → `(None, None)`.

- [ ] **Step 1: Failing tests**
  - linking unit: `link_same_dir`, `link_nested_per_patient` (`all.zip!/a.zip!/x.nii.gz` with DICOM under `all.zip!/a.zip!/…` for patient 1 and `all.zip!/b.zip!/…` for patient 2 → patient 1), `link_two_patients_in_scope_unassigned`, `link_no_dicom_unassigned`, `link_study_only_when_unique`, `link_folder_scopes` (import paths `sub/x.nii` with DICOM `sub/IM1`).
  - integration (`tests/archives.rs`, building ZIPs in-test with the `zip` crate and synthetic phantom Part-10 bytes via the core crate's phantom functions or the existing test helpers): `zip_single_patient_counts` (stored = n, uploads row `done` with counts), `two_patient_nested_zip_links_each_nifti`, `duplicates_count_for_linking` (upload DICOM first via `/api/instances` style ingest, then the archive with the NIfTI → linked), `encrypted_and_truncated_fail_alone`, `aborted_upload_keeps_stored_items` (small `max_expanded_bytes`), `tmp_empty_after_job`, `process_dir_with_archive_and_loose_nifti`.
- [ ] **Step 2: Run** `cargo test -p slicestream-server --test archives` and unit tests — fail.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `cargo test --workspace`, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(server): archive upload processor with NIfTI linking`

---

### Task 6: Job API, config and startup wiring (server crate)

**Files:**
- Create: `crates/slicestream-server/src/archive_jobs.rs`
- Modify: `src/config.rs` (`[archives]` section `ArchivesConfig` with the spec keys and defaults, `deny_unknown_fields`, part of `Config`), `src/lib.rs` (routes, AppState field `archives: Option<Arc<ArchiveJobs>>`, wiring in `start()`), `src/routes.rs` or new handlers in `archive_jobs.rs`, `slicestream.example.toml`, `crates/slicestream-server/tests/stow.rs` (AppState literals)
- Test: `crates/slicestream-server/tests/archives_http.rs`

**Interfaces:**
- Consumes: Task 5 `process_upload`, `Progress`, `EntryResult`, `UploadOutcome`; Task 2 `create_upload`.
- Produces HTTP (all behind Host guard, no CORS, token check like STOW):
  - `POST /api/archives` — reject with **507** if `Content-Length` present and `available_space(root) < content_length + min_free_bytes`; spool body to `tmp/` (limit `[http] max_request_bytes` → 413); detect the first bytes: `Other` → 400 `{"error":"unsupported file type"}` and delete; queue full (`4 × max_concurrent_jobs` jobs queued or running) → 503; else `create_upload("http_archive", name, remote)` where `name` comes from the `x-filename` request header (sanitized: last path segment, max 255 chars, control chars removed) or `"upload"`; spawn job; **202** `{"job": "<32 hex random>", "upload_id": n}`.
  - `GET /api/archives/{job}` — 200 job JSON per spec (`status`, `reason`, `counts`, `bytes_expanded`, `entries_seen`, `entries` (last ≤ 10 000), `patients`, `nifti`, `upload_id`, `name`); unknown → 404. `Cache-Control: no-store`.
  - `[archives] enabled = false` → routes not mounted (404) and no job manager.
- `ArchiveJobs`: `tokio::sync::Semaphore(max_concurrent_jobs)` for running; jobs map with finished-job eviction beyond `job_retention` (oldest finished first); the spooled upload file is deleted when the job ends (any outcome). Shutdown: running jobs are cancelled via the server's `CancellationToken` (walk aborts at next entry; already-stored items stay; upload row `aborted`, reason "server shutting down").
- `start()`: `recover()` already aborts running uploads (Task 2); log `uploads_aborted`.

- [ ] **Step 1: Failing tests** (server started as in existing e2e tests): `post_zip_returns_202_and_job_completes` (poll ≤ 10 s until `done`; counts and `patients` populated), `post_bare_nifti_one_entry_job`, `post_junk_400`, `post_requires_token_when_set` (401), `unknown_job_404`, `archives_disabled_404`, `no_cors_on_archive_routes`, `job_eviction_keeps_retention`, `x_filename_sanitized`.
- [ ] **Step 2: Run** — fail.
- [ ] **Step 3: Implement.** Example TOML gets the `[archives]` block with comments, as the spec.
- [ ] **Step 4: Run** `cargo test --workspace`, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(server): archive job API and [archives] config`

---

### Task 7: `import` accepts archives, NIfTI and archive files

**Files:**
- Modify: `crates/slicestream-server/src/main.rs` (argument renamed from `dir` to `path`, help text), `src/lib.rs` (`import_dir` replaced by or delegating to `process_dir` / `process_upload`; keep `ImportReport` only if still used), existing tests referencing `import_dir`
- Test: `crates/slicestream-server/tests/e2e.rs` or `tests/import.rs`

**Interfaces:**
- Consumes: Task 5 `process_dir`, `process_upload`, `UploadSource::Import`.
- Behavior: `import <PATH> [--recursive]` — `PATH` a folder (as today, now also opening archives and storing NIfTI inside it) or a single file (archive, NIfTI or DICOM). Creates an `uploads` row (`kind = "import"`, `remote` NULL). Prints one line per patient (`<patient_id> <name>: <studies> studies, <instances> images`), then `stored N · duplicate N · nifti N (linked N, unassigned N) · skipped N · failed N`, and `aborted: <reason>` if aborted. Exit code non-zero if any failed or aborted (same rule as today's `failed > 0`).
- Limits come from `[archives]` in the config.

- [ ] **Step 1: Failing tests**: `import_zip_file`, `import_folder_with_nested_zip_and_loose_nifti`, `import_prints_summary` (run the binary via `assert_cmd`-like `std::process::Command` on `env!("CARGO_BIN_EXE_slicestream-server")` with a temp config; check stdout contains the totals line), existing import tests still pass.
- [ ] **Step 2: Run** — fail.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `cargo test --workspace`, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(cli): import archives, NIfTI and archive files`

---

### Task 8: Console — archive uploads, job progress, NIfTI metrics

**Files:**
- Create: `web/lib/archive-core.js` (pure: detect kind from first bytes, job-JSON → view model), `web/archive-upload.js` (XHR upload with progress, polling, row rendering), `web/tests/archive.test.js`
- Modify: `web/upload.js` (route by detected kind), `web/index.html`, `web/console.css`, `web/metrics.js` (facts grid "NIfTI volumes"; storage objects segment includes `nifti_bytes`), `crates/slicestream-server/src/metrics.rs` (expose `storage.nifti_count`, `storage.nifti_bytes` from `StorageCounts`; spec JSON gains these two fields)

**Interfaces:**
- Consumes: Task 6 HTTP API; Task 2 `StorageCounts.nifti_*`.
- `web/lib/archive-core.js`: `export function sniff(bytes /* Uint8Array, ≤ 1024 */)` → `"dicom" | "archive" | "nifti" | "other"` using the same magic table as the spec (gzip counts as `"archive"`; the server classifies its inner content); `export function jobView(job)` → `{ status, label /* e.g. "812 / ? entries · 1.2 GB expanded" */, counts, patients, rows /* EntryResult list newest first */ , done: boolean }`.
- Behavior: reading the first 1024 bytes of each file (via `file.slice(0,1024).arrayBuffer()`) decides the route: dicom → existing STOW/single path; archive/nifti → `POST /api/archives` with `x-filename` and token, upload progress shown on the archive's row; then poll `GET /api/archives/{job}` every 500 ms until `done`/`aborted` (stop polling on 404 with a "job no longer available" row state). An archive row shows name, phase (uploading % → processing entries → done), totals chip (stored/dup/nifti/skipped/failed), expandable to its entry rows (same columns and tags as file rows; NIFTI tag for NIfTI, SKIPPED tag in neutral grey), and a patient/study summary block when done. Archive uploads count against the 4-in-flight limit only during the HTTP upload phase. "other" files picked directly → failed row "unsupported file type" without sending. Dispatch `slicestream:stored` when a job finishes with any stored item.
- Visual: match the existing console language; no new colors except reusing tokens.

- [ ] **Step 1: Failing tests** `web/tests/archive.test.js`: `sniff` for each magic + short/junk, `jobView` for running (unknown total), done with patients, aborted with reason; Rust: metrics `snapshot_json_shape` updated for `storage.nifti_count`/`nifti_bytes`.
- [ ] **Step 2: Run** `node --test 'web/tests/*.test.js'` and `cargo test -p slicestream-server metrics` — fail.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** tests; manual Playwright pass in the scratch dir (temp TOML, never `./storage`, stop server by PID): upload a ZIP of two per-patient ZIPs each with DICOM + `.nii.gz`, a 7z, a junk file; check rows, summary, NIfTI fact, no console errors; screenshot to the scratch dir.
- [ ] **Step 5: Commit** `feat(web): archive and NIfTI uploads with job progress`

---

### Task 9: Docs, conformance and end-to-end check

**Files:**
- Modify: `README.md` (what works; HTTP API rows for `/api/archives`; import usage; RAR feature + licence note), `docs/ARCHITECTURE.md` (crate layout gains `slicestream-archive`; roadmap M2 notes archive/NIfTI ingest), `docs/CONFORMANCE.md` (archive ingest section: formats, limits, linking), `SECURITY.md` (archive limits; job API exposes patient names), `LICENSE-NOTICES` or a README section naming UnRAR's licence for builds with feature `rar`.

- [ ] **Step 1:** Fresh scratch storage; build fixtures from phantom data (two patients, nested ZIP with NIfTI, 7z, tar.gz); run `import` on each and via the console; verify counts, linking, `tmp/` empty, restart mid-job marks the upload aborted.
- [ ] **Step 2:** Run `cargo test --workspace`, `cargo test -p slicestream-archive --no-default-features`, fmt, clippy, `cargo +1.88 check --workspace --all-targets`, node tests, `scripts/interop-dcmtk.sh` — all pass.
- [ ] **Step 3: Commit** `docs: archive and NIfTI ingest`
