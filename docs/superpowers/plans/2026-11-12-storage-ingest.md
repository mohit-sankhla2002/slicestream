# Storage Ingest (C-STORE SCP + STOW-RS) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store DICOM instances received over DIMSE C-STORE and HTTP STOW-RS through one crash-safe, streaming ingest pipeline into content-addressed files plus a SQLite index, and serve them from the existing streaming viewer.

**Architecture:** A new crate `slicestream-store` owns spooling (`TempBlob`), the content-addressed `FsBlobStore`, header parsing, the sqlx `Index` and the `Ingestor` that orders them: spool → validate → commit blob → index → ack. A new crate `slicestream-dimse` turns dicom-ul associations into `Ingestor` calls. `slicestream-server` adds STOW-RS, TOML config, an `import` subcommand, and an index-backed series catalog.

**Tech Stack:** Rust 2024, tokio 1.54, axum 0.8, dicom-* 0.10.1 (`dicom-ul` with `async`), sqlx 0.9 (`runtime-tokio`, `sqlite`, `migrate`), multer 3.1, sha2 0.11, clap 4.6, toml 1.2, tokio-util 0.7 (`CancellationToken`).

**Spec:** `docs/superpowers/specs/2026-11-12-storage-ingest-design.md`

## Global Constraints

- Edition 2024, `rust-version = "1.88"`, dual license `MIT OR Apache-2.0` via `[workspace.package]`.
- `#![forbid(unsafe_code)]` in every new crate.
- `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --all` pass after every task.
- Never buffer a whole instance in memory; only headers (read with `read_until(tags::PIXEL_DATA)`) and DIMSE command sets.
- No attacker-controlled string becomes a filesystem path; blobs are named by SHA-256 hex only.
- An instance is acknowledged (C-STORE 0x0000 / STOW ReferencedSOPSequence) only after blob rename + directory fsync + index commit.
- Status codes exactly as in the spec's "Status mapping" table.
- Implementation Class UID `2.25.74886492046038186129868855052748391807`, Implementation Version Name `SLICESTREAM_010`.
- Defaults: AE `SLICESTREAM`, DIMSE `0.0.0.0:11112`, `max_associations = 64`, `max_pdu_length = 116794`, timeouts 60 s, `max_instance_bytes = 2147483648`, `max_request_bytes = 17179869184`, `on_duplicate = "keep"`.
- Test data only from `examples/make_phantom.rs`; no real patient data.

## Review Focus

1. Peer aborts or disconnects mid-instance: no file left in `tmp/` or `objects/`, no index row (Task 6, `abort_mid_instance_leaves_nothing`).
2. Same SOP Instance UID stored concurrently from two callers: exactly one row, both get success (Task 4, `concurrent_same_instance_one_row`).
3. Instance over `max_instance_bytes` on DIMSE: SCP drains the rest of the data PDVs, answers 0xA700, and the next C-STORE on the same association succeeds (Task 6, `oversize_then_normal_on_same_association`).
4. STOW body with one non-DICOM part among valid parts: HTTP 202, valid ones stored, bad one in FailedSOPSequence with 0xC000 (Task 7, `mixed_parts_returns_202`).
5. Crash between blob commit and index commit, or mid-spool: `Ingestor::recover` deletes orphans and tmp files (Task 4, `recover_removes_orphans_and_tmp`).

---

### Task 0: Baseline commit and branch

- [ ] **Step 1:** `git add -A && git commit -m "chore: initial slicestream scaffold"` (repo has no commits yet).
- [ ] **Step 2:** `git switch -c feat/storage-ingest`.

---

### Task 1: Core refactor — buildable `Series` and public header helpers

**Files:**
- Modify: `crates/slicestream-core/src/series.rs`, `crates/slicestream-core/src/frame.rs`
- Test: `crates/slicestream-core/tests/series.rs`

**Interfaces:**
- Produces:
  - `pub fn FrameInfo::from_dataset(obj: &InMemDicomObject, transfer_syntax: &str) -> Result<FrameInfo>` (rename of `from_object`, now `pub`).
  - `pub fn slice_position(obj: &InMemDicomObject) -> Option<f64>` in `series.rs` (now returns `None` when geometry is absent).
  - `pub fn SliceRef::new(path: PathBuf, frame: u32, info: FrameInfo, position: Option<f64>, instance_number: Option<i64>) -> SliceRef`
  - `pub fn Series::from_slices(uid: String, description: Option<String>, modality: Option<String>, slices: Vec<SliceRef>) -> Series` — sorts by `(position.unwrap_or(0.0), instance_number.unwrap_or(0), frame)`.
  - `pub fn get_str/get_i64/get_f64s` in `frame.rs` become `pub` (re-exported as `slicestream_core::attrs::{get_str, get_i64, get_f64s}` via `pub mod attrs` re-export).

- [ ] **Step 1: Failing test** `from_slices_sorts_by_position_then_instance` — three `SliceRef`s with positions `Some(2.0), None, Some(1.0)` and instance numbers `1, 5, 9`; assert sorted paths are `[None-one, pos1, pos2]` (None sorts as 0.0).
- [ ] **Step 2:** `cargo test -p slicestream-core from_slices` → FAIL (no `from_slices`).
- [ ] **Step 3:** Implement; rewrite `scan_dir` to build `SliceRef::new(..)` and call `Series::from_slices`.
- [ ] **Step 4:** `cargo test -p slicestream-core` → all pass (existing phantom tests unchanged).
- [ ] **Step 5:** Commit `refactor(core): constructible Series and public attribute helpers`.

---

### Task 2: `slicestream-store` — spooling and content-addressed blobs

**Files:**
- Create: `crates/slicestream-store/{Cargo.toml,src/lib.rs,src/blob.rs}`; add to workspace `members`.
- Test: in-module `#[cfg(test)]` in `blob.rs` (use `tempfile`).

**Interfaces:**
- Produces:
  - `pub struct BlobHash(pub [u8; 32])` with `to_hex(&self) -> String`, `from_hex(&str) -> Option<BlobHash>`, derives `Clone, Copy, Eq, Hash, Debug`.
  - `pub enum SpoolError { TooLarge { limit: u64 }, Io(std::io::Error) }` (thiserror).
  - `pub struct TempBlob` — `pub async fn write_chunk(&mut self, buf: &[u8]) -> Result<(), SpoolError>`; `pub fn len(&self) -> u64`; `pub async fn finish(self) -> Result<SpooledBlob, SpoolError>` (flush + `sync_all`). `Drop` removes the file unless finished.
  - `pub struct SpooledBlob { pub hash: BlobHash, pub len: u64 }` + `pub fn path(&self) -> &Path`. `Drop` removes the file unless committed.
  - `pub struct FsBlobStore` — `pub fn open(root: &Path) -> io::Result<Self>` (creates `tmp/`, `objects/`); `pub fn create_temp(&self, limit: u64) -> io::Result<TempBlob>` (name `tmp/<random u128 hex>.part`, opened `create_new`); `pub fn commit(&self, spooled: SpooledBlob) -> io::Result<BlobHash>` (blocking: rename to `objects/aa/bb/<hex>.dcm`, `create_dir_all`, fsync parent dir; if target exists, drop temp and return hash); `pub fn path_of(&self, hash: &BlobHash) -> PathBuf`; `pub fn delete(&self, hash: &BlobHash) -> io::Result<()>` (NotFound is Ok); `pub fn sweep_tmp(&self) -> io::Result<usize>`; `pub fn list(&self) -> io::Result<Vec<BlobHash>>`.
- Deps: `tokio` (fs, io-util), `sha2`, `thiserror`, `rand` 0.9 (temp names).

- [ ] **Step 1: Failing tests:**
  - `spool_hash_matches_sha256` — write `b"hello"` in two chunks; `finish()`; hex equals `2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824`; `len == 5`.
  - `limit_enforced` — limit 4, writing 5 bytes returns `SpoolError::TooLarge { limit: 4 }`.
  - `drop_removes_temp` — create, write, drop without finish; `tmp/` is empty. Same for a dropped `SpooledBlob`.
  - `commit_is_idempotent` — commit same content twice; one file under `objects/`, second commit returns same hash, `tmp/` empty.
  - `sweep_and_list` — leave a stray `tmp/x.part`; `sweep_tmp() == 1`; `list()` returns the committed hash.
- [ ] **Step 2:** `cargo test -p slicestream-store` → FAIL (crate empty).
- [ ] **Step 3:** Implement as specified. Hash incrementally with `sha2::Sha256` inside `write_chunk`.
- [ ] **Step 4:** `cargo test -p slicestream-store` → PASS.
- [ ] **Step 5:** Commit `feat(store): streaming temp blobs and content-addressed blob store`.

---

### Task 3: Header parsing, Part-10 prefix, SQLite index

**Files:**
- Create: `crates/slicestream-store/src/header.rs`, `src/part10.rs`, `src/index.rs`, `migrations/0001_init.sql`
- Test: in-module tests; `tests/index.rs`

**Interfaces:**
- Consumes: Task 1 `FrameInfo::from_dataset`, `slice_position`, attribute helpers; Task 2 `TempBlob`, `BlobHash`.
- Produces:
  - `pub struct InstanceHeader { pub sop_class_uid: String, pub sop_instance_uid: String, pub study_instance_uid: String, pub series_instance_uid: String, pub patient_id: Option<String>, pub patient_name: Option<String>, pub study_date: Option<String>, pub study_description: Option<String>, pub accession_number: Option<String>, pub modality: Option<String>, pub series_number: Option<i64>, pub series_description: Option<String>, pub instance_number: Option<i64>, pub number_of_frames: u32, pub transfer_syntax: String, pub slice_position: Option<f64>, pub frame_info: Option<FrameInfo> }`
  - `pub fn read_header(path: &Path) -> Result<InstanceHeader, HeaderError>`; `pub enum HeaderError { Unparseable(String), Missing(&'static str) }`. Missing any of the four UIDs → `Missing("SOPInstanceUID")` etc. `frame_info` is `None` for non-image objects (SR, PR).
  - `pub async fn write_part10_prefix(temp: &mut TempBlob, sop_class_uid: &str, sop_instance_uid: &str, transfer_syntax: &str, source_ae: Option<&str>) -> Result<(), SpoolError>` — 128 zero bytes, `b"DICM"`, then `FileMetaTableBuilder` (implementation UID/version from Global Constraints, `source_application_entity_title` when given) `.build()?.write(&mut Vec)`. (`FileMetaTable::write` does **not** emit preamble or `DICM`.)
  - `pub struct InstanceRecord { pub header: InstanceHeader, pub blob: BlobHash, pub size_bytes: u64, pub source: String, pub calling_ae: Option<String> }`
  - `pub enum DuplicatePolicy { Keep, Overwrite }` (serde `lowercase`).
  - `pub enum UpsertOutcome { Inserted, Duplicate { existing: BlobHash }, Replaced { previous: BlobHash } }`
  - `pub struct Index` — `pub async fn open(db_path: &Path) -> Result<Index, sqlx::Error>` (`create_if_missing`, `journal_mode=WAL`, `synchronous=FULL`, `foreign_keys=ON`, `busy_timeout=5s`, `sqlx::migrate!("./migrations")`); `pub async fn upsert_instance(&self, rec: &InstanceRecord, policy: DuplicatePolicy) -> Result<UpsertOutcome, sqlx::Error>`; `pub async fn blob_hashes(&self) -> Result<HashSet<BlobHash>, sqlx::Error>`; `pub async fn blob_in_use(&self, hash: &BlobHash) -> Result<bool, sqlx::Error>`; `pub async fn list_series(&self) -> Result<Vec<SeriesRow>, sqlx::Error>`; `pub async fn series_slices(&self, series_uid: &str) -> Result<Vec<SliceRow>, sqlx::Error>`.
  - `pub async fn instance(&self, sop_instance_uid: &str) -> Result<Option<InstanceSummary>, sqlx::Error>`; `pub struct InstanceSummary { pub sop_instance_uid: String, pub series_instance_uid: String, pub blob: BlobHash, pub source: String, pub calling_ae: Option<String> }` (used by Task 6/7 tests).
  - `pub struct SeriesRow { pub series_instance_uid: String, pub study_instance_uid: String, pub modality: Option<String>, pub series_description: Option<String>, pub instance_count: i64 }`
  - `pub struct SliceRow { pub blob: BlobHash, pub number_of_frames: u32, pub instance_number: Option<i64>, pub slice_position: Option<f64>, pub frame_info: FrameInfo }` — only rows with non-NULL `frame_info`.
- Schema (`0001_init.sql`): `patients(id INTEGER PRIMARY KEY, patient_id TEXT NOT NULL UNIQUE, patient_name TEXT)` (missing PatientID stored as `''`); `studies(id, study_instance_uid TEXT NOT NULL UNIQUE, patient_fk INTEGER NOT NULL REFERENCES patients(id), study_date, study_description, accession_number, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`; `series(id, series_instance_uid TEXT NOT NULL UNIQUE, study_fk REFERENCES studies(id), modality, series_number INTEGER, series_description, created_at, updated_at)`; `instances(id, sop_instance_uid TEXT NOT NULL UNIQUE, series_fk REFERENCES series(id), sop_class_uid TEXT NOT NULL, instance_number INTEGER, number_of_frames INTEGER NOT NULL, transfer_syntax TEXT NOT NULL, slice_position REAL, frame_info TEXT, blob_sha256 TEXT NOT NULL, size_bytes INTEGER NOT NULL, source TEXT NOT NULL, calling_ae TEXT, received_at TEXT NOT NULL)`; indexes on `instances(series_fk)`, `instances(blob_sha256)`, `series(study_fk)`. Timestamps RFC 3339 UTC.
- Upsert: one transaction; `INSERT … ON CONFLICT(uid) DO UPDATE SET updated_at=excluded.updated_at RETURNING id` for patient/study/series (study keeps its original `patient_fk`; log `warn!` if it differs); instance `INSERT … ON CONFLICT(sop_instance_uid) DO NOTHING`, then if no row inserted read existing `blob_sha256` and either return `Duplicate` or (`Overwrite`) `UPDATE` and return `Replaced`.

- [ ] **Step 1: Failing tests:**
  - `read_header_phantom` — phantom slice: UIDs set, `modality == Some("CT")`, `frame_info.unwrap().rows == 32`, `slice_position.is_some()`.
  - `read_header_rejects_non_dicom` → `HeaderError::Unparseable(_)`; file missing SOPInstanceUID → `Missing("SOPInstanceUID")`.
  - `part10_prefix_roundtrip` — prefix + phantom dataset bytes (via `write_dataset`) into a `TempBlob`, finish, `read_header` succeeds and transfer syntax equals the one passed.
  - `upsert_insert_then_duplicate` — insert → `Inserted`; same UID same blob → `Duplicate { existing }` with that hash; `Keep` with different blob → `Duplicate`, row unchanged; `Overwrite` → `Replaced { previous }`, row points to new blob.
  - `series_slices_returns_frame_info` — 3 inserted phantom headers → `series_slices` len 3, `list_series[0].instance_count == 3`.
- [ ] **Step 2:** `cargo test -p slicestream-store` → FAIL.
- [ ] **Step 3:** Implement. sqlx with runtime queries (`sqlx::query`/`query_as`), not compile-time macros (no `DATABASE_URL` needed). `cargo add sqlx --no-default-features -F runtime-tokio,sqlite,migrate -p slicestream-store`.
- [ ] **Step 4:** `cargo test -p slicestream-store` → PASS.
- [ ] **Step 5:** Commit `feat(store): header parsing, Part-10 prefix and SQLite index`.

---

### Task 4: `Ingestor` — the pipeline

**Files:**
- Create: `crates/slicestream-store/src/ingest.rs`, `src/config.rs`
- Test: `crates/slicestream-store/tests/ingest.rs`

**Interfaces:**
- Consumes: Tasks 2–3.
- Produces:
  - `pub struct StorageConfig { pub root: PathBuf, pub max_instance_bytes: u64, pub on_duplicate: DuplicatePolicy }` (serde, defaults from Global Constraints).
  - `pub enum IngestSource { Dimse { calling_ae: String, called_ae: String }, Http { remote: Option<SocketAddr> }, Import { path: PathBuf } }` — `source` column values `"dimse"`, `"http"`, `"import"`.
  - `#[derive(Default)] pub struct Expectations { pub sop_class_uid: Option<String>, pub sop_instance_uid: Option<String>, pub study_instance_uid: Option<String> }`
  - `pub struct Stored { pub sop_class_uid: String, pub sop_instance_uid: String, pub study_instance_uid: String, pub series_instance_uid: String, pub blob: BlobHash, pub duplicate: bool, pub replaced: bool }`
  - `pub enum IngestError { Unparseable(String), MissingAttribute(&'static str), UidMismatch { field: &'static str, expected: String, actual: String }, StudyMismatch { expected: String, actual: String }, UnsupportedSopClass(String), TooLarge { limit: u64 }, Storage(io::Error), Index(sqlx::Error) }` with `pub fn status(&self) -> u16` = `0xC000, 0xC000, 0xA900, 0xC409, 0x0122, 0xA700, 0xA700, 0xA700`. `From<SpoolError>` and `From<HeaderError>`.
  - `#[derive(Clone, Debug)] pub struct StoredEvent { pub series_instance_uid: String, pub sop_instance_uid: String }`
  - `pub struct Ingestor` — `pub async fn open(cfg: StorageConfig) -> Result<Arc<Ingestor>, IngestError>` (opens `FsBlobStore` at `root`, `Index` at `root/index.sqlite`, broadcast capacity 1024); `pub fn create_temp(&self) -> Result<TempBlob, IngestError>`; `pub async fn ingest(&self, spooled: SpooledBlob, source: IngestSource, expect: Expectations) -> Result<Stored, IngestError>`; `pub fn subscribe(&self) -> broadcast::Receiver<StoredEvent>`; `pub fn index(&self) -> &Index`; `pub fn blobs(&self) -> &FsBlobStore`; `pub async fn recover(&self) -> Result<RecoveryReport, IngestError>`; `pub async fn import_file(&self, path: &Path) -> Result<Stored, IngestError>` (copies file through `TempBlob`).
  - `pub struct RecoveryReport { pub tmp_removed: usize, pub orphans_removed: usize }`
  - `pub fn is_storage_sop_class(uid: &str) -> bool` — via `dicom_dictionary_std::sop_class::registry()` (`UidDictionary::by_uid`): known UID → `true` iff entry type is `SopClass` and name ends with `"Storage"`; unknown UID (private/vendor) → `true`.
- `ingest` order: `read_header` in `spawn_blocking` → validate (`Expectations`: sop class/instance → `UidMismatch`, study → `StudyMismatch`; then `is_storage_sop_class`) → `blobs.commit` in `spawn_blocking` → `index.upsert_instance` → on `Duplicate { existing }` with `existing != new` delete new blob unless `blob_in_use`; on `Replaced { previous }` delete previous unless `blob_in_use` → send event (ignore no-receiver error) → `Ok(Stored)`. If upsert fails after commit, delete the new blob unless `blob_in_use`, return `Index` error.

- [ ] **Step 1: Failing tests** (all against a `tempdir` root, phantom files fed via `import_file` or via a `TempBlob`):
  - `ingest_stores_and_indexes` — blob exists at `path_of`, `list_series` has 1 series, event received.
  - `duplicate_same_bytes_is_success` — second ingest `duplicate == true`, still one file in `objects/`.
  - `uid_mismatch_rejected` — `Expectations { sop_instance_uid: Some("1.2.3") }` → `UidMismatch`, `status() == 0xA900`, `objects/` empty, `tmp/` empty.
  - `study_mismatch_rejected` → `status() == 0xC409`.
  - `non_storage_sop_class_rejected` — header with SOP class `1.2.840.10008.1.1` (Verification) → `0x0122`.
  - `concurrent_same_instance_one_row` — 8 `tokio::spawn` ingests of identical bytes; all `Ok`; exactly one instance row; one file.
  - `recover_removes_orphans_and_tmp` — commit a blob without indexing, leave a stray `tmp/*.part`; `recover()` → `{ tmp_removed: 1, orphans_removed: 1 }`; indexed blobs untouched.
  - `storage_failure_maps_to_a700` — make `objects/` read-only (`std::fs::set_permissions`, skip test if running as root), ingest → `Storage(_)`, `status() == 0xA700`, no index row.
- [ ] **Step 2:** `cargo test -p slicestream-store --test ingest` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `cargo test -p slicestream-store` → PASS.
- [ ] **Step 5:** Commit `feat(store): crash-safe ingest pipeline with duplicate policy and recovery`.

---

### Task 5: `slicestream-dimse` — PDV assembly and DIMSE commands (pure)

**Files:**
- Create: `crates/slicestream-dimse/{Cargo.toml,src/lib.rs,src/assembler.rs,src/command.rs}`
- Test: in-module tests.

**Interfaces:**
- Produces:
  - `pub enum PdvEvent { Command { pc_id: u8, bytes: Vec<u8> }, Data { pc_id: u8, chunk: Vec<u8>, last: bool } }`
  - `pub struct PdvAssembler` — `pub fn new(max_command_bytes: usize) -> Self` (use 64 KiB); `pub fn push(&mut self, pdv: dicom_ul::pdu::PDataValue) -> Result<Option<PdvEvent>, AssemblerError>`; command fragments accumulate until `is_last`; data passes through untouched. `AssemblerError::{CommandTooLarge, InterleavedCommand}` (command fragment for a different `pc_id` while one is pending).
  - `pub enum DimseRequest { Echo { message_id: u16, sop_class_uid: String }, Store { message_id: u16, sop_class_uid: String, sop_instance_uid: String, has_dataset: bool }, Other { command_field: u16, message_id: u16, sop_class_uid: Option<String> } }`
  - `pub fn parse_command(bytes: &[u8]) -> Result<DimseRequest, CommandError>` — Implicit VR LE; `CommandDataSetType != 0x0101` means dataset present.
  - `pub fn echo_rsp(message_id: u16, sop_class_uid: &str) -> Vec<u8>`; `pub fn store_rsp(message_id: u16, sop_class_uid: &str, sop_instance_uid: &str, status: u16) -> Vec<u8>`; `pub fn failure_rsp(command_field: u16, message_id: u16, sop_class_uid: Option<&str>, status: u16) -> Vec<u8>` (response field = `command_field | 0x8000`). All built with `InMemDicomObject::command_from_element_iter` (computes CommandGroupLength) and `CommandDataSetType = 0x0101`.

- [ ] **Step 1: Failing tests:**
  - `command_split_across_three_pdvs` — serialize a C-STORE-RQ, split in 3 PDVs (last `is_last`); first two `push` → `None`, third → `Command` with full bytes; `parse_command` yields `Store { message_id: 7, has_dataset: true, .. }`.
  - `data_passes_through` — data PDVs yield `Data { last }` matching `is_last`.
  - `command_too_large` — 70 KiB of non-last command fragments → `CommandTooLarge`.
  - `echo_rsp_parses_back` — `parse` the response object: CommandField `0x8030`, Status `0`, MessageIDBeingRespondedTo `5`.
  - `failure_rsp_sets_response_bit` — `failure_rsp(0x0020, 3, None, 0x0211)` → CommandField `0x8020`, Status `0x0211`.
- [ ] **Step 2:** `cargo test -p slicestream-dimse` → FAIL.
- [ ] **Step 3:** Implement. Deps: `dicom-ul = { version = "0.10.1", features = ["async"] }`, `dicom-object`, `dicom-core`, `dicom-dictionary-std`, `dicom-transfer-syntax-registry`, `thiserror`.
- [ ] **Step 4:** `cargo test -p slicestream-dimse` → PASS.
- [ ] **Step 5:** Commit `feat(dimse): PDV assembler and DIMSE command codec`.

---

### Task 6: C-STORE SCP

**Files:**
- Create: `crates/slicestream-dimse/src/scp.rs`, `src/config.rs`
- Test: `crates/slicestream-dimse/tests/scp.rs` (uses dicom-ul async **client** as SCU and the phantom generator via `#[path]`).

**Interfaces:**
- Consumes: Task 4 `Ingestor` (`create_temp`, `ingest`, `blobs`, `index`), `write_part10_prefix`; Task 5 everything.
- Produces:
  - `pub struct ScpConfig { pub enabled: bool, pub ae_title: String, pub addr: SocketAddr, pub require_called_ae_title: bool, pub allowed_peers: Vec<String>, pub max_associations: usize, pub max_pdu_length: u32, pub read_timeout_secs: u64, pub write_timeout_secs: u64 }` (serde, defaults from Global Constraints).
  - `pub struct StoreScp` — `pub async fn bind(cfg: ScpConfig, ingestor: Arc<Ingestor>) -> io::Result<StoreScp>`; `pub fn local_addr(&self) -> SocketAddr`; `pub async fn run(self, shutdown: CancellationToken) -> io::Result<()>`.
- Behavior: one task per association, holding a permit from `Semaphore(max_associations)` taken with `try_acquire_owned`. If no permit is free, drop the TCP stream at once and `warn!` (dicom-ul offers no hook for a transient A-ASSOCIATE-RJ here).
  - Options: `ServerAssociationOptions::new()`, `.ae_title`, `.ae_access_control(PeerAllowList)` (implements `AccessControl`: called AE check if `require_called_ae_title`, calling AE in `allowed_peers` if non-empty; rejection reasons `CalledAeTitleNotRecognized` / `CallingAeTitleNotRecognized`), `.promiscuous(true)`, every `TransferSyntaxRegistry` entry `.with_transfer_syntax`, `.with_abstract_syntax(VERIFICATION)`, `.max_pdu_length`, `.read_timeout`, `.write_timeout`, `.establish_async`.
  - State machine: `Idle` → on `Store` command: `create_temp`, `write_part10_prefix` (TS = negotiated TS of that `pc_id`, source AE = calling AE) → `Receiving { temp, request }`; data chunks → `write_chunk`; `TooLarge` → `Discarding { request, status: 0xA700 }` (drop temp, keep reading until `last`); on `last`: `finish` → `ingest(…, Expectations { sop_class_uid, sop_instance_uid, .. })` → `store_rsp` with `0x0000` or `err.status()` → `Idle`.
  - `Echo` → `echo_rsp`. `Other` → `failure_rsp(.., 0x0211)` (if it announces a dataset, discard it first).
  - Data PDV while `Idle`, or a command while `Receiving` → send A-ABORT, end association (temp dropped).
  - `ReleaseRQ` → `ReleaseRP`; `AbortRQ`/IO error/timeout → end, temp dropped.
  - Shutdown: stop `accept`; wait up to 30 s for in-flight tasks (`JoinSet`), then abort them.
  - One `tracing` span per association with `peer`, `calling_ae`, `called_ae`.

- [ ] **Step 1: Failing tests** (SCP bound on `127.0.0.1:0`; SCU via `ClientAssociationOptions::new().calling_ae_title("TESTSCU").called_ae_title("SLICESTREAM").with_presentation_context(CT_IMAGE_STORAGE, vec![EXPLICIT_VR_LITTLE_ENDIAN])…establish_async`):
  - `c_echo_succeeds` — echo status 0x0000.
  - `c_store_three_slices` — 3 phantom instances, each status 0x0000; `index.list_series()` has 3 instances; `source == "dimse"`, `calling_ae == "TESTSCU"`.
  - `wrong_called_ae_rejected` — called AE `"NOPE"` → `establish_async` returns `Err`.
  - `peer_not_allowed_rejected` — `allowed_peers = ["CT01"]`, calling `TESTSCU` → rejected.
  - `uid_mismatch_returns_a900` — command Affected SOP Instance UID differs from dataset → status 0xA900, nothing stored.
  - `oversize_then_normal_on_same_association` — `max_instance_bytes = 1024`, send 32×32 phantom (≈2 KiB+) → 0xA700; then send an 8×8 phantom on the same association → 0x0000. The limit counts spooled bytes including the Part-10 prefix; pick any limit (e.g. 1024–1600) where the first instance exceeds it and the second fits, and assert that in the test.
  - `abort_mid_instance_leaves_nothing` — send command + first data PDV (not last), then `abort()`; after 200 ms `tmp/` and `objects/` are empty and no index rows.
  - `max_associations_enforced` — `max_associations = 1`; hold one association open; second `establish_async` fails.
- [ ] **Step 2:** `cargo test -p slicestream-dimse --test scp` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `cargo test -p slicestream-dimse` → PASS.
- [ ] **Step 5:** Commit `feat(dimse): C-STORE/C-ECHO SCP with access control, limits and streaming spool`.

---

### Task 7: STOW-RS and `POST /api/instances`

**Files:**
- Create: `crates/slicestream-server/src/stow.rs`
- Modify: `crates/slicestream-server/src/main.rs` (routes), `Cargo.toml` (`multer`, `mime`, `slicestream-store`)
- Create: `crates/slicestream-server/src/config.rs` with `pub struct HttpConfig { pub addr: SocketAddr, pub max_request_bytes: u64, pub stow_token: Option<String>, pub web_dir: PathBuf }` (serde, defaults `127.0.0.1:8080`, 17179869184, None, `web`). `AppState` gains `pub ingestor: Arc<Ingestor>` and `pub http: HttpConfig`; the existing `series` map and folder scan in `main.rs` stay until Task 8 (main opens an `Ingestor` at `storage/` with `StorageConfig::default()`).
- Test: `crates/slicestream-server/tests/stow.rs` (router via `tower::ServiceExt::oneshot`; expose `pub fn app(state) -> Router` from a `lib.rs` so tests can build it — move `routes`, `stream`, `stow` modules under `src/lib.rs`, keep `main.rs` thin).

**Interfaces:**
- Consumes: Task 4 `Ingestor`, `Expectations`, `IngestSource::Http`, `IngestError::status`.
- Produces:
  - Handlers `stow_all(State, HeaderMap, Body)` at `POST /dicom-web/studies` and `stow_study(State, Path<String>, HeaderMap, Body)` at `POST /dicom-web/studies/{study}`, both calling `async fn stow_inner(state: &AppState, study: Option<String>, headers: &HeaderMap, body: Body) -> Response`. Both routes use `DefaultBodyLimit::disable()`; size is enforced by multer.
  - `pub async fn post_instance(…, Body) -> Response` at `POST /api/instances` (`Content-Type: application/dicom` only, else 415). Response JSON `{ "status": "stored"|"duplicate"|"replaced", "sopInstanceUid", "seriesInstanceUid", "studyInstanceUid" }` (200) or `{ "error", "status": <u16> }` with HTTP 400 for 0xC000/0xA900/0xC409/0x0122, 413 for TooLarge, 500 for 0xA700 storage/index.
  - `pub enum PartResult { Stored(Stored), Failed { sop_class_uid: Option<String>, sop_instance_uid: Option<String>, reason: u16 } }`
  - `pub fn stow_response(base_url: &str, study: Option<&str>, results: &[PartResult]) -> (StatusCode, serde_json::Value)` — pure; DICOM JSON per spec (tags as 8-hex uppercase keys, `{"vr": "...", "Value": [...]}`): `00081190` (UR) RetrieveURL when `study` known; `00081199` (SQ) items with `00081150` (UI), `00081155` (UI), `00081190` (UR); `00081198` (SQ) items with `00081150`, `00081155` (when known), `00081197` (US). Status 200/202/409 per spec.
- Parsing: `mime::Mime` from Content-Type; require `multipart/related` and `type` param `application/dicom` (missing `type` → accept, many clients omit it); boundary param required (else 400). `multer::Multipart::with_constraints(body.into_data_stream(), boundary, Constraints::new().size_limit(SizeLimit::new().whole_stream(max_request_bytes)))`; per part: part Content-Type must be `application/dicom` (else `Failed { reason: 0xC000 }`), stream chunks into `ingestor.create_temp()`; `TooLarge` → keep draining part, record `Failed { reason: 0xA700 }`. multer `StreamSizeExceeded` → 413; other multer errors → 400.
- Auth: if `http.stow_token` set, require `Authorization: Bearer <token>` (constant-time compare) on both endpoints → 401.

- [ ] **Step 1: Failing tests:**
  - `stow_three_instances_200` — multipart body of 3 phantom files → 200, `00081199` has 3 items, `list_series` count 3.
  - `mixed_parts_returns_202` — 2 phantom + 1 `b"not dicom"` part → 202, 2 referenced, 1 failed with `00081197 == [49152]` (0xC000).
  - `all_failed_returns_409`.
  - `study_mismatch_in_url` — `POST /dicom-web/studies/9.9.9` with phantom → 409, reason `[50185]` (0xC409).
  - `wrong_content_type_415` — `application/json` → 415.
  - `missing_boundary_400`.
  - `request_too_large_413` — `max_request_bytes = 1024`.
  - `token_required_401` — token configured, none sent → 401; correct token → 200.
  - `post_instance_single` — `application/dicom` body → 200 `"stored"`; repeat → `"duplicate"`.
  - `stow_response_shape` — unit test of `stow_response` with one stored + one failed: keys and VRs exactly as above.
- [ ] **Step 2:** `cargo test -p slicestream-server --test stow` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `cargo test -p slicestream-server` → PASS.
- [ ] **Step 5:** Commit `feat(server): STOW-RS and single-instance upload`.

---

### Task 8: Server wiring — config, `serve`/`import`, index-backed catalog, shutdown

**Files:**
- Create: `crates/slicestream-server/src/config.rs`, `src/catalog.rs`, `slicestream.example.toml`
- Modify: `crates/slicestream-server/src/{lib.rs,main.rs,routes.rs,stream.rs}`, `README.md`, `docs/ARCHITECTURE.md`
- Test: `crates/slicestream-server/tests/e2e.rs`

**Interfaces:**
- Consumes: Tasks 1, 4, 6, 7.
- Produces:
  - `pub struct Config { pub storage: StorageConfig, pub http: HttpConfig, pub dimse: ScpConfig }`, `pub struct HttpConfig { pub addr: SocketAddr, pub max_request_bytes: u64, pub stow_token: Option<String>, pub web_dir: PathBuf }`; `pub fn Config::load(path: Option<&Path>) -> anyhow::Result<Config>` (defaults when `None`; unknown keys are an error via `#[serde(deny_unknown_fields)]`).
  - CLI (clap derive): `slicestream-server serve [--config PATH]` (default subcommand) and `slicestream-server import <DIR> [--config PATH] [--recursive]` (prints `stored / duplicate / failed` counts; non-DICOM files counted as skipped, not failed).
  - `pub struct SeriesCatalog` — `pub fn new(ingestor: Arc<Ingestor>) -> Arc<Self>`; `pub async fn list(&self) -> anyhow::Result<Vec<SeriesSummary>>`; `pub async fn get(&self, series_uid: &str) -> anyhow::Result<Option<Arc<Series>>>` (built from `index.series_slices` → `SliceRef::new(blobs.path_of(&row.blob), frame, row.frame_info, row.slice_position, row.instance_number)` for each frame → `Series::from_slices`; cached in `RwLock<HashMap>`); `pub fn spawn_invalidator(self: &Arc<Self>)` (removes cache entry on each `StoredEvent`; on `RecvError::Lagged` clears the whole cache).
  - `pub async fn start(config: Config, shutdown: CancellationToken) -> anyhow::Result<Running>`; `pub struct Running { pub http_addr: SocketAddr, pub dimse_addr: Option<SocketAddr>, pub task: tokio::task::JoinHandle<anyhow::Result<()>> }` — `serve` and `e2e.rs` both use it (ports `:0` in tests).
  - `pub async fn import_dir(ingestor: &Ingestor, dir: &Path, recursive: bool) -> ImportReport`; `pub struct ImportReport { pub stored: usize, pub duplicate: usize, pub failed: usize, pub skipped: usize }`. A file is *skipped* (not failed) when it is shorter than 132 bytes or bytes 128..132 are not `DICM`.
  - `Config` moves `HttpConfig` (from Task 7) under `Config.http`.
  - `AppState` becomes `{ catalog: Arc<SeriesCatalog>, ingestor: Arc<Ingestor>, scheduler: SchedulerConfig, http: HttpConfig }`; existing `/api/series*` and WS routes read from `catalog`. `SeriesSummary` for series with no image frames is omitted from `list`.
  - `serve`: `Ingestor::open` → `recover()` (log report) → bind HTTP and (if `dimse.enabled`) `StoreScp` → run both under one `CancellationToken` cancelled on Ctrl-C/SIGTERM; HTTP uses `axum::serve(..).with_graceful_shutdown`.
- Remove the old positional `<dicom-root>` argument and `Series::scan_dir` usage from the server (keep `scan_dir` in core for library users).

- [ ] **Step 1: Failing tests (`e2e.rs`):**
  - `stow_then_stream` — start full app on ephemeral ports with a tempdir root; STOW 5 phantom slices; `GET /api/series` lists 1 series with `sliceCount == 5`; open the WS stream (use `tokio-tungstenite` dev-dep), send cursor 2, first binary frame decodes (`protocol::decode_frame`) with `index == 2`.
  - `c_store_then_listed` — C-STORE 3 slices to the SCP; `GET /api/series` shows them without restart (catalog invalidation).
  - `import_command_counts` — run `import` logic on a dir with 4 phantom files + 1 txt → `stored 4, skipped 1`.
  - `config_rejects_unknown_key` — TOML with `[storage] rot = "x"` → error.
- [ ] **Step 2:** `cargo test -p slicestream-server --test e2e` → FAIL.
- [ ] **Step 3:** Implement; write `slicestream.example.toml` (exactly the spec's config block); update README quick start to `import` + `serve`, list the new endpoints and DIMSE port; mark M2 storage and the C-STORE part of M3 as done in `docs/ARCHITECTURE.md`.
- [ ] **Step 4:** `cargo test --all` → PASS; `cargo clippy --all-targets -- -D warnings` clean.
- [ ] **Step 5:** Commit `feat(server): config, import command and index-backed series catalog`.

---

### Task 9: Interoperability with DCMTK + conformance statement

**Files:**
- Create: `scripts/interop-dcmtk.sh`, `docs/CONFORMANCE.md`
- Modify: `.github/workflows/ci.yml` (new job `interop`: `sudo apt-get install -y dcmtk`, `bash scripts/interop-dcmtk.sh`)

**Interfaces:**
- Consumes: built `slicestream-server` binary and the phantom example.

- [ ] **Step 1:** Write `scripts/interop-dcmtk.sh` (`set -euo pipefail`; temp root; trap kills server):
  1. Generate 20 phantom slices.
  2. Start `serve` with a temp config (HTTP `127.0.0.1:18080`, DIMSE `127.0.0.1:11112`); poll `/api/series` until 200 (max 10 s).
  3. `echoscu -aec SLICESTREAM 127.0.0.1 11112` → exit 0.
  4. `storescu -aec SLICESTREAM 127.0.0.1 11112 <first 10 files>` → exit 0.
  5. `storescu -aec SLICESTREAM -xi 127.0.0.1 11112 <next 10 files>` (proposes Implicit VR LE only, so the Part-10 prefix path runs with a second transfer syntax) → exit 0.
  6. Re-send file 1 via `curl --data-binary @file -H 'Content-Type: application/dicom' /api/instances` → JSON `"status":"duplicate"`.
  7. `curl /api/series` → `sliceCount` is 20 (`jq` or `grep -q '"sliceCount":20'`).
  8. `storescu -aec WRONG …` → non-zero exit (association rejected).
- [ ] **Step 2:** Run `bash scripts/interop-dcmtk.sh` locally (install `dcmtk` if missing) → prints `interop OK`.
- [ ] **Step 3:** Write `docs/CONFORMANCE.md`: implementation UID/version; SCP roles (Verification, Storage SCP for any storage SOP class, promiscuous negotiation note); accepted transfer syntaxes = those in `dicom-transfer-syntax-registry` 0.10.1; status codes table copied from the spec; STOW-RS media types and HTTP status codes; limits and defaults.
- [ ] **Step 4:** No git remote exists: do not push. Validate the workflow YAML parses (`python3 -c 'import yaml,sys;yaml.safe_load(open(".github/workflows/ci.yml"))'`) and rely on Step 2's local run.
- [ ] **Step 5:** Commit `test: DCMTK interop script, CI job and conformance statement`.
