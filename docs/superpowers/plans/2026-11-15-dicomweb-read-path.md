# DICOMweb Read Path (M1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve stored DICOM through standard QIDO-RS and WADO-RS (metadata, frames, bulk data, instances) so OHIF and any DICOMweb client work with no custom code, with frames and bulk values read by byte offset recorded at ingest.

**Architecture:** One bounded-memory scan per instance at ingest (extending the header walker) produces DICOM JSON metadata, a frame table, a bulk-value table and extra QIDO columns, written in the same SQLite transaction as the instance row; old stores are backfilled in the background. A new pure crate `slicestream-dicomweb` parses QIDO queries, negotiates `Accept`, writes `multipart/related` and splices BulkDataURIs. The server mounts `/dicom-web` GET handlers that stream from SQLite and `pread`; the catalog (WebSocket stream, `/api/series/.../frames`) uses the same frame tables.

**Tech Stack:** Rust 2024 (MSRV 1.88); dicom-rs 0.10.1 (`dicom-parser` stateful decoder); sqlx 0.8 SQLite; axum 0.8; tower-http 0.7 (`cors`, `compression-gzip`, `compression-br`); `base64`, `percent-encoding`, `rand`; Python `dicomweb-client` (interop only); OHIF v3.13.x Docker image (acceptance only).

**Spec:** `docs/superpowers/specs/2026-11-15-dicomweb-read-path-design.md`

## Global Constraints

- Rust MSRV 1.88, edition 2024; `cargo +1.88 check --workspace --all-targets` passes. Every new dependency builds on 1.88; pin versions that do.
- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, `node --test 'web/tests/*.test.js'` clean after every task.
- `slicestream-store`, `slicestream-server` and `slicestream-dicomweb` keep `#![forbid(unsafe_code)]`. Positioned reads use `std::os::unix::fs::FileExt::read_exact_at`; no mmap. In `slicestream-core`, file-reading functions are `#[cfg(unix)]` so the crate still builds for `wasm32-unknown-unknown` (a later viewer project compiles it to WASM).
- Stored blobs stay byte-for-byte as received. Storage layout and crash-safety guarantees unchanged.
- Acceptance rules unchanged: anything today's `read_header` accepts is still stored; deeper scan problems are recorded in `instances.scan_problem`, never rejected.
- Memory per request does not grow with response size: stream part by part, file reads in chunks of at most 1 MiB.
- Bulk rule, exactly: Pixel Data (7FE0,0010), Float Pixel Data (7FE0,0008), Double Float Pixel Data (7FE0,0009) always bulk; VR OB, OD, OF, OL, OV, OW, UN longer than 1,024 bytes bulk; any other value longer than 64 KiB (`MAX_VALUE_BYTES`) bulk. Smaller binary values inline as base64 `InlineBinary`. Group 0002 never in metadata.
- Metadata JSON cap per instance: 64 MiB (`MAX_METADATA_BYTES = 67_108_864`).
- Config, exactly: `[http] public_url` (optional), `[dicomweb] enabled = true`, `[dicomweb] max_results = 1000`.
- Security as today: Host allow-list on every route; new GET routes join the read router (CORS only for `cors_allowed_origins`, empty by default); ingest routes never get CORS. No read auth (M6).
- Test data only from the phantom generator or synthesized in tests. Never real patient data.
- Docs in the existing style: plain English, short sentences.

## Review Focus

1. Non-ASCII patient names (ISO_IR 100 Latin-1 and ISO_IR 192 UTF-8): metadata JSON is correct UTF-8 and QIDO name search ignores case (`müller*` finds `MÜLLER^HANS`). Pinned in Task 3 (`scan_latin1_patient_name`) and Task 7 (`pn_match_folds_non_ascii`).
2. An old store opened by the new binary while OHIF is already reading: unscanned instances still answer metadata, frames and bulk correctly during backfill. Pinned in Task 5 (`backfill_scans_old_rows`), Task 10 (`metadata_scans_unscanned_instance`) and Task 11 (`unscanned_frames_fall_back`).
3. `on_duplicate = "overwrite"` replacing an instance: metadata, frames and the WebSocket stream serve the new bytes, never old offsets applied to the new file. Pinned in Task 4 (`overwrite_replaces_instance_data`) and Task 11 (`overwrite_then_frames_new_bytes`).
4. Malformed paths: unknown or mismatched UIDs, `frames/1,,2`, `frames/0`, `frames/abc`, 5,000-entry lists, `bulk/zzzz` give 400 or 404, never 500 or a panic. Pinned in Task 11 (`frames_bad_lists_400`, `frames_zero_and_past_end_404`, `bulk_unknown_path_404`) and Task 10 (`instance_metadata_404_wrong_series`).
5. Browser `Accept` headers: missing, `*/*`, several ranges with q-values, quoted and unquoted `type`, `transfer-syntax=*`. Pinned in Task 8 (`accept_*` tests) and Task 11 (`quoted_and_unquoted_type`).

---

### Task 1: Core — frame locations, positioned reads, bit-packed and big-endian helpers

**Files:**
- Modify: `crates/slicestream-core/src/frame.rs`, `src/series.rs`, `src/lib.rs`
- Test: unit tests in `frame.rs`; `crates/slicestream-core/tests/frame_at.rs`

**Interfaces:**
- Produces (core):
  - `pub enum FrameLocation { Bytes { offset: u64, len: u64 }, Bits { offset: u64, bit_start: u64, bit_len: u64 }, Fragments(Vec<(u64, u64)>) }` (`Debug, Clone, PartialEq, Eq`). `Bytes`: one contiguous range. `Bits`: `offset` is the file offset of the Pixel Data value; the frame is bits `[bit_start, bit_start + bit_len)` of it. `Fragments`: `(file offset, length)` of each fragment value, concatenated in order.
  - `#[cfg(unix)] pub fn read_frame_at(path: &Path, loc: &FrameLocation) -> Result<Bytes>` — `read_exact_at`; a short read is `Error::Io`.
  - `pub fn extract_bits(src: &[u8], bit_start: u64, bit_len: u64) -> Vec<u8>` — DICOM bit order (first pixel in the least significant bit of the first byte); output starts at bit 0, unused high bits of the last byte zero.
  - `pub fn swap_to_little_endian(buf: &mut [u8], bits_allocated: u16)` — swaps 2-byte words for 16, 4-byte words for 32, no-op otherwise.
  - `FrameInfo::frame_bits(&self) -> u64` = rows × columns × samples_per_pixel × bits_allocated.
  - `read_frame_bytes` handles Bits Allocated 1 with `extract_bits` (frame *k* = bits `[k·n, (k+1)·n)`, `n` = rows × columns × samples_per_pixel).
  - `SliceRef::with_location(self, loc: FrameLocation) -> SliceRef`; `Series::load_frame` uses `read_frame_at` when a location is set (on unix), else `read_frame_bytes`.

- [ ] **Step 1: Failing tests** — units: `extract_bits_aligned`, `extract_bits_unaligned` (bits 3..13 of `[0b1010_1100, 0b0000_0111]` → `[0b1111_0101, 0b0000_0000]`), `swap_16`, `swap_32`, `swap_8_is_noop`, `bits_allocated_1_frames` (2 frames of 3×3 at BA 1 written by dicom-object; frame 1 equals `extract_bits(all, 9, 9)`). `tests/frame_at.rs`: `read_frame_at_matches_parse_native` (phantom; Pixel Data is the last element with defined length, so `offset = file_len - value_len`), `read_frame_at_fragments_concatenates`, `read_frame_at_short_file_is_io_error`, `load_frame_uses_location` (a `SliceRef` whose location points at a known byte range of a scratch file returns exactly those bytes).
- [ ] **Step 2: Run** `cargo test -p slicestream-core` — fails to compile.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** tests, fmt, clippy, `cargo +1.88 check -p slicestream-core`, and `cargo check -p slicestream-core --target wasm32-unknown-unknown` if the target is installed (else note it) — pass.
- [ ] **Step 5: Commit** `feat(core): frame locations, positioned reads and bit-packed frames`

---

### Task 2: Store — DICOM JSON writer

**Files:**
- Create: `crates/slicestream-store/src/json.rs`
- Modify: `crates/slicestream-store/Cargo.toml` (`base64`), `src/lib.rs`
- Test: unit tests in `json.rs`

**Interfaces:**
- Produces (crate-private, used by Task 3):
  - `pub(crate) struct DicomJsonWriter` — streaming PS3.18 Annex F writer; elements arrive in data set order.
  - `fn new() -> Self`; `fn element(&mut self, tag: Tag, vr: VR, value: &PrimitiveValue)`; `fn bulk(&mut self, tag: Tag, vr: VR, path: &str)` writes `{"vr":"XX","BulkDataURI":"bulk/<path>"}`; `fn begin_sequence(&mut self, tag: Tag)`; `fn begin_item(&mut self)`; `fn end_item(&mut self)`; `fn end_sequence(&mut self)`; `fn len(&self) -> usize`; `fn finish(self) -> Vec<u8>` (one JSON object).
- Encoding rules: keys `"GGGGEEEE"` upper-case hex; empty value → `{"vr":"XX"}`; multi-valued strings split on `\`, trailing spaces/NULs trimmed (leading spaces kept for LT, ST, UT); PN → `{"Alphabetic":…,"Ideographic":…,"Phonetic":…}` from the `=`-separated groups, empty groups left out; AT → `"GGGGEEEE"` strings; IS, DS, US, SS, UL, SL, FL, FD → JSON numbers; SV, UV → numbers within ±2^53, strings outside; unparseable IS/DS text → the trimmed text as a JSON string; non-finite floats → `null`; OB/OW/OD/OF/OL/OV/UN inline → `"InlineBinary"` base64 (standard alphabet, padded). Strings are JSON-escaped, so the byte sequence `"BulkDataURI":"` can only occur as a real key.

- [ ] **Step 1: Failing tests** — `json_pn_groups` (`Yamada^Tarou=山田^太郎=やまだ^たろう` → three groups), `json_pn_alphabetic_only`, `json_at`, `json_is_ds_numbers` (`"12"`, `" 1.5 "`, `"-0.25E2"`), `json_bad_ds_is_string`, `json_multivalue_strings` (`ORIGINAL\PRIMARY\AXIAL`), `json_empty_value`, `json_inline_binary_base64`, `json_bulk_uri_relative`, `json_nested_sequence` (two items, one empty), `json_uv_beyond_2_53_is_string`, `json_key_sequence_only_from_keys` (a LO value `x"BulkDataURI":"y` occurs once escaped; searching the output for `"BulkDataURI":"` finds only the real bulk key). Every output parses with `serde_json`.
- [ ] **Step 2: Run** `cargo test -p slicestream-store json` — fails.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** tests, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(store): streaming DICOM JSON writer`

---

### Task 3: Store — instance scan (metadata, frame table, bulk table)

**Files:**
- Create: `crates/slicestream-store/src/scan.rs`, `crates/slicestream-store/tests/scan.rs`, `crates/slicestream-store/tests/support/mod.rs` (fixture builders)
- Modify: `crates/slicestream-store/src/header.rs` (`InstanceHeader` fields; share walker helpers), `src/lib.rs`

**Interfaces:**
- Produces (store, all `pub`, re-exported from `lib.rs`):
  - `InstanceHeader` gains `study_time, study_id, referring_physician_name, patient_birth_date, patient_sex, performed_procedure_step_start_date, performed_procedure_step_start_time: Option<String>` (`read_header` leaves them `None`; it is unchanged otherwise).
  - `pub const SCAN_VERSION: i64 = 1; pub const MAX_METADATA_BYTES: usize = 67_108_864; pub const BULK_BINARY_MIN: u32 = 1024;`
  - `pub struct ScanLimits { pub max_metadata_bytes: usize }` (`Default` = `MAX_METADATA_BYTES`).
  - `pub struct BulkValue { pub path: String, pub offset: u64, pub len: u64 }` (`Debug, Clone, PartialEq, Eq, Serialize, Deserialize`). `path` = `GGGGEEEE` for top level, `SSSSSSSS/<item>/GGGGEEEE…` nested (item 0-based). Pixel Data (7FE0,0010) is never in this list.
  - `pub enum FrameTable { Native { offset: u64, frame_len: u64, count: u32 }, Bits { offset: u64, frame_bits: u64, count: u32 }, Fragments(Vec<Vec<(u64, u64)>>) }` with `fn count(&self) -> u32`, `fn location(&self, frame: u32) -> Option<FrameLocation>` (0-based), `fn encode(&self) -> Vec<u8>`, `fn decode(bytes: &[u8]) -> Option<FrameTable>`. Encoding: version byte `1`, kind byte, then little-endian u64/u32 fields; fragments as frame count, then per frame a fragment count and `(offset, len)` pairs.
  - `pub struct InstanceData { pub metadata: Option<Vec<u8>>, pub frames: Option<FrameTable>, pub bulk: Vec<BulkValue>, pub offsets_valid: bool, pub problem: Option<String> }`.
  - `pub struct InstanceScan { pub header: InstanceHeader, pub data: InstanceData }`.
  - `pub fn scan_instance(path: &Path) -> Result<InstanceScan, HeaderError>` = `scan_instance_with(path, &ScanLimits::default())`; `pub fn scan_instance_with(path: &Path, limits: &ScanLimits) -> Result<InstanceScan, HeaderError>`. Blocking.
- Rules:
  - File offsets: after the preamble and `FileMetaTable::from_reader`, `dataset_start = bytes pulled from the file − bytes still buffered in the BufReader`; create the decoder with `DynStatefulDecoder::new_with_ts(reader, ts, dataset_start)` so `position()` is a file offset. Deflated transfer syntax → `offsets_valid = false`, no frame table, empty bulk list, metadata still produced.
  - Errors before the walk passes tag (0028,1053) return `Err` exactly as `read_header` would; any later error stops the scan and sets `problem` (what parsed so far is kept; no frame table if Pixel Data was not fully seen).
  - Native Pixel Data: `Native` (BA ≥ 8, `frame_len = rows·cols·spp·BA/8`) or `Bits` (BA 1, `frame_bits = rows·cols·spp`); `count = number_of_frames`. Value end beyond file size → no table, `problem = "pixel data truncated"`.
  - Encapsulated grouping, first rule that applies: (1) Extended Offset Table (7FE0,0001) + Lengths (7FE0,0002) each exactly 8 × frames bytes (read even though binary); (2) non-empty Basic Offset Table with exactly `frames` entries; (3) one frame → all fragments; (4) fragments == frames → one each; (5) a fragment starting `FF D8` or `FF 4F FF 51` starts a frame, used only if it yields exactly `frames` frames (read the first 4 bytes of each fragment, skip the rest); (6) else no table, `problem = "frame boundaries unknown"`, `warn!` logged.
  - Metadata over `max_metadata_bytes` → `metadata = None`, `problem = "metadata too large (N bytes)"`, scan continues for frames and bulk.
  - Character set: rely on the stateful decoder's handling of (0008,0005); JSON strings are UTF-8.
- Fixture builders in `tests/support/mod.rs` (dicom-object writes; build bytes by hand where it cannot): `native(dir, frames, rows, cols, bits) -> PathBuf`, `encapsulated(dir, frames: &[Vec<Vec<u8>>], bot: bool, eot: bool, ts: &str) -> PathBuf` (arbitrary fragment bytes; offsets do not depend on the codec), `big_endian(dir) -> PathBuf`, `deflated(dir) -> PathBuf` (if dicom-object cannot write it, raw-deflate the data set bytes with `flate2` after a meta group naming `1.2.840.10008.1.2.1.99`), `bits1(dir, frames, rows, cols) -> PathBuf`, `with_elements(dir, extra: Vec<DataElement<InMemDicomObject>>) -> PathBuf`.

- [ ] **Step 1: Failing tests** in `tests/scan.rs`: `scan_header_matches_read_header` (shared fields equal on the phantom), `scan_native_locations_match_parse` (every frame's `read_frame_at` equals `read_frame_bytes`), `scan_encapsulated_bot`, `scan_encapsulated_eot`, `scan_one_fragment_per_frame`, `scan_markers_group_fragments` (3 frames in 7 fragments, frame starts marked `FF D8`), `scan_single_frame_all_fragments`, `scan_ambiguous_has_no_frame_table` (problem `"frame boundaries unknown"`), `scan_big_endian_offsets`, `scan_deflated_has_metadata_no_offsets`, `scan_bits_allocated_1`, `scan_bulk_rule` (OB 1,025 bytes → bulk with exact offset/len; OB 1,024 → `InlineBinary`; UT 70,000 bytes → bulk; LO short → inline), `scan_nested_bulk_path` (private OB 2,000 bytes inside item 1 of a private SQ → path `00091001/1/00091002`), `scan_truncated_pixel_data_is_problem_not_error`, `scan_metadata_cap` (`max_metadata_bytes = 1024` → `metadata == None`, problem names the size, frames still present), `scan_latin1_patient_name` (`ISO_IR 100`, `MÜLLER^HANS` → JSON `"Alphabetic":"MÜLLER^HANS"`), `scan_excludes_group_0002`, `scan_qido_fields` (StudyTime, StudyID, ReferringPhysicianName, PatientBirthDate, PatientSex, PPS start date/time read into `InstanceHeader`). Unit tests in `scan.rs`: `frame_table_roundtrip` (each kind), `frame_table_location`.
- [ ] **Step 2: Run** `cargo test -p slicestream-store --test scan` — fails.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `cargo test -p slicestream-store`, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(store): instance scan with DICOM JSON, frame and bulk tables`

---

### Task 4: Store — migration 0003 and index writes/reads

**Files:**
- Create: `crates/slicestream-store/migrations/0003_dicomweb.sql`
- Modify: `crates/slicestream-store/src/index.rs`, `src/lib.rs`
- Test: `crates/slicestream-store/tests/index.rs`

**Interfaces:**
- Consumes: Task 3 types.
- Produces:
  - Migration: `studies` + `study_time, study_id, referring_physician_name, referring_physician_name_folded, patient_name, patient_name_folded, patient_birth_date, patient_sex` (TEXT); `series` + `performed_procedure_step_start_date, performed_procedure_step_start_time` (TEXT); `instances` + `scan_version INTEGER NOT NULL DEFAULT 0`, `scan_problem TEXT`; table `instance_data (instance_fk INTEGER PRIMARY KEY REFERENCES instances(id), metadata BLOB, frames BLOB, bulk TEXT NOT NULL, offsets_valid INTEGER NOT NULL)`; indexes `instances(scan_version)`, `studies(study_date)`, `studies(patient_name_folded)`, `studies(accession_number)`.
  - `pub fn fold_name(s: &str) -> String` — Unicode lower-casing (`str::to_lowercase`) used for `patient_name_folded`, `referring_physician_name_folded` and query values.
  - `InstanceRecord` gains `pub data: InstanceData`. `upsert_instance` writes new study/series columns with `COALESCE(existing, excluded)` (first non-null wins), `patient_name` on the study from the instance, and on `Inserted`/`Replaced` writes `instance_data` (`INSERT OR REPLACE`), `scan_version = SCAN_VERSION`, `scan_problem`; on `Duplicate` touches neither.
  - `pub enum Scope { Study(String), Series(String, String), Instance(String, String, String) }`.
  - `pub struct InstanceRef { pub id: i64, pub study_uid: String, pub series_uid: String, pub sop_uid: String, pub sop_class_uid: String, pub blob: BlobHash, pub transfer_syntax: String, pub number_of_frames: u32, pub frame_info: Option<FrameInfo>, pub scanned: bool }`.
  - `Index::find_instances(&self, scope: &Scope) -> Result<Vec<InstanceRef>, sqlx::Error>` — UIDs must belong together (joins); order by series id, `slice_position`, `instance_number`, id. Empty vec = not found.
  - `pub enum MetadataState { Ready(Vec<u8>), Unscanned, Unavailable(String) }`; `Index::metadata_batch(&self, ids: &[i64]) -> Result<Vec<(i64, MetadataState)>, sqlx::Error>` (caller chunks by 128).
  - `pub struct StoredData { pub frames: Option<FrameTable>, pub bulk: Vec<BulkValue>, pub offsets_valid: bool, pub problem: Option<String> }`; `Index::stored_data(&self, id: i64) -> Result<Option<StoredData>, sqlx::Error>` (`None` = unscanned).
  - `Index::unscanned(&self, limit: i64) -> Result<Vec<(i64, String, BlobHash)>, sqlx::Error>` (id, SOP UID, blob; `scan_version < SCAN_VERSION`, id order).
  - `Index::write_scan(&self, id: i64, blob: &BlobHash, scan: &InstanceScan) -> Result<bool, sqlx::Error>` — one transaction; only if the row still has `blob`; fills study/series columns with COALESCE; returns whether written.
  - `Index::write_scan_failure(&self, id: i64, blob: &BlobHash, problem: &str) -> Result<bool, sqlx::Error>` — sets `scan_version`, `scan_problem`, no `instance_data`.
  - `SliceRow` gains `pub frames: Option<FrameTable>` (LEFT JOIN; `None` unless scanned with `offsets_valid`).

- [ ] **Step 1: Failing tests** — `upsert_writes_instance_data`, `duplicate_keeps_instance_data`, `overwrite_replaces_instance_data` (frames/metadata now from the second blob), `study_patient_attrs_per_study_without_patient_id` (two studies, no PatientID, names `A^ONE` / `B^TWO` → each study row keeps its own name; `patients` has one row), `opens_store_created_before_0003` (run a `sqlx::migrate::Migrator` over a temp dir holding copies of 0001 and 0002, insert a patient/study/series/instance by SQL, then `Index::open` applies 0003; the instance has `scan_version = 0` and appears in `unscanned`), `find_instances_scopes_and_mismatch` (wrong series for an instance → empty), `metadata_batch_states`, `write_scan_skips_changed_blob`, `series_slices_carry_frames`.
- [ ] **Step 2: Run** `cargo test -p slicestream-store --test index` — fails.
- [ ] **Step 3: Implement**; update all `InstanceRecord` constructions in the workspace (tests included).
- [ ] **Step 4: Run** `cargo test --workspace`, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(store): migration 0003, instance data and DICOMweb lookups`

---

### Task 5: Store — scan at ingest, background backfill, scan on demand

**Files:**
- Modify: `crates/slicestream-store/src/ingest.rs`, `src/lib.rs`
- Test: `crates/slicestream-store/tests/ingest.rs`

**Interfaces:**
- Consumes: Tasks 3–4.
- Produces:
  - `Ingestor::ingest` calls `scan_instance` (instead of `read_header`) on the blocking pool and stores `InstanceData` with the row. DIMSE, STOW, archives and `import` all inherit this.
  - `Ingestor::backfill_batch(&self, limit: i64) -> Result<usize, IngestError>` — takes `unscanned(limit)`, scans at most 2 at a time on blocking threads, each under the per-SOP-UID lock, writes with `write_scan` (or `write_scan_failure` when the blob is missing or unparseable); returns how many rows it processed (0 = done).
  - `Ingestor::ensure_scanned(&self, id: i64) -> Result<(), IngestError>` — same path for one instance; no-op when already scanned.

- [ ] **Step 1: Failing tests** — `ingest_stores_scan_data` (phantom → `stored_data` has a `Native` table, metadata parses as JSON with (0010,0010)), `backfill_scans_old_rows` (ingest 5, then through a second `SqlitePool` on `index.sqlite`: `DELETE FROM instance_data; UPDATE instances SET scan_version = 0; UPDATE studies SET study_time = NULL` → `backfill_batch(2)` returns 2, 2, 1, 0; afterwards all scanned and `study_time` restored), `backfill_skips_instance_replaced_meanwhile` (`on_duplicate = overwrite`; replace after `unscanned` read — `write_scan` returns false, the new blob's data stays), `backfill_records_unreadable_blob_problem` (delete a blob file → row gets `scan_problem`, not retried), `ensure_scanned_single`.
- [ ] **Step 2: Run** `cargo test -p slicestream-store --test ingest` — fails.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `cargo test --workspace`, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(store): scan at ingest, background backfill and scan on demand`

---

### Task 6: `slicestream-dicomweb` crate — QIDO query model and parsing

**Files:**
- Create: `crates/slicestream-dicomweb/Cargo.toml` (workspace metadata; deps `dicom-core`, `dicom-dictionary-std`, `percent-encoding`), `src/lib.rs`, `src/qido.rs`
- Modify: workspace `Cargo.toml` (member)
- Test: unit tests in `qido.rs`

**Interfaces:**
- Produces:
  - `pub enum Level { Study, Series, Instance }` (ordered: Study < Series < Instance).
  - `pub enum Key { StudyDate, StudyTime, AccessionNumber, ModalitiesInStudy, ReferringPhysicianName, PatientName, PatientId, StudyInstanceUid, StudyId, StudyDescription, Modality, SeriesInstanceUid, SeriesNumber, PpsStartDate, PpsStartTime, SeriesDescription, SopClassUid, SopInstanceUid, InstanceNumber, RequestAttributeSequence }` with `fn tag(self) -> Tag`, `fn level(self) -> Level`, `fn from_param(name: &str) -> Option<Key>` (keyword or 8-hex tag, also dotted `0040A370.00401001` style for sequence keys → `RequestAttributeSequence`).
  - `pub enum Matcher { Universal, Single(String), List(Vec<String>), Wildcard(String), Range { from: Option<String>, to: Option<String> } }`.
  - `pub enum Include { Default, All, Tags(Vec<Tag>) }`.
  - `pub struct QidoQuery { pub level: Level, pub study_uid: Option<String>, pub series_uid: Option<String>, pub matches: Vec<(Key, Matcher)>, pub include: Include, pub limit: Option<u32>, pub offset: u32, pub fuzzy: bool }`.
  - `pub struct QueryError { pub param: String, pub reason: String }` (`Display`: `"<param>: <reason>"`).
  - `pub fn parse_qido(level: Level, study_uid: Option<&str>, series_uid: Option<&str>, raw_query: &str) -> Result<QidoQuery, QueryError>`.
  - `pub fn default_fields(level: Level) -> &'static [Tag]` — PS3.18 default return attributes per level (study, series, instance tables).
- Rules: split on `&` then first `=`; percent-decode; `+` stays literal. Params `limit`, `offset` (u32), `fuzzymatching` (`true`/`false`), `includefield` (repeatable, comma lists; keyword, 8-hex tag or `all`). A key whose level is below the query level → 400 (`Modality` at study level). A DICOM keyword or tag that is not a `Key` → reason `"matching on this attribute is not supported"`; anything else → `"unknown query parameter"`; repeated key → `"repeated"`. Matcher by key VR: UI → `List` on commas (`Single` for one); DA/TM/DT → `Range` when the value contains `-`, else `Single`; other strings → `Wildcard` when containing `*` or `?`; empty → `Universal`. Validate DA `YYYYMMDD`, TM `HH[MM[SS[.F{1,6}]]]`; invalid → 400 reason `"invalid date"` / `"invalid time"`. Path UIDs set `study_uid` / `series_uid`.
- Before implementing, read PS3.18 §8.3.4 (current edition) for unsupported matching keys and §8.3.4.4/§8.3.3 for the empty-result status. If the standard requires ignoring unsupported keys with a warning, implement that instead of 400 and note the clause for Task 9's CONFORMANCE text.

- [ ] **Step 1: Failing tests** — `parse_patient_name_wildcard`, `parse_uid_list`, `parse_date_range_open_ends` (`-20260101`, `20250101-`), `parse_time_partial`, `parse_invalid_date_400`, `parse_includefield_keyword_tag_and_all`, `parse_limit_offset_fuzzy`, `parse_plus_is_literal`, `parse_percent_decoding`, `parse_unknown_param`, `parse_unsupported_attribute`, `parse_lower_level_key_rejected`, `parse_higher_level_key_allowed` (`PatientID` at series level), `parse_repeated_key`, `parse_path_uids`, `parse_request_attribute_sequence_key`, `default_fields_study_contains_counts`.
- [ ] **Step 2: Run** `cargo test -p slicestream-dicomweb` — fails.
- [ ] **Step 3: Implement.** `#![forbid(unsafe_code)]`.
- [ ] **Step 4: Run** tests, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(dicomweb): QIDO-RS query parsing`

---

### Task 7: Store — QIDO-RS SQL

**Files:**
- Create: `crates/slicestream-store/src/qido.rs`
- Modify: `crates/slicestream-store/Cargo.toml` (`slicestream-dicomweb` path dependency), `src/lib.rs`
- Test: `crates/slicestream-store/tests/qido.rs`

**Interfaces:**
- Consumes: `QidoQuery`, `Key`, `Matcher`, `Level` (Task 6); columns from Task 4.
- Produces:
  - `pub struct StudyFields { pub study_uid: String, pub study_date: Option<String>, pub study_time: Option<String>, pub accession_number: Option<String>, pub referring_physician_name: Option<String>, pub patient_name: Option<String>, pub patient_id: String, pub patient_birth_date: Option<String>, pub patient_sex: Option<String>, pub study_id: Option<String>, pub study_description: Option<String>, pub modalities: Vec<String>, pub series_count: i64, pub instance_count: i64 }`.
  - `pub struct SeriesFields { pub series_uid: String, pub modality: Option<String>, pub series_number: Option<i64>, pub series_description: Option<String>, pub pps_start_date: Option<String>, pub pps_start_time: Option<String>, pub instance_count: i64 }`.
  - `pub struct InstanceFields { pub sop_uid: String, pub sop_class_uid: String, pub instance_number: Option<i64>, pub number_of_frames: u32, pub frame_info: Option<FrameInfo> }`.
  - `pub struct QidoRow { pub study: StudyFields, pub series: Option<SeriesFields>, pub instance: Option<InstanceFields>, pub representative: i64 }` — `representative` = lowest instance id in the row's study (study level), series (series level) or the instance itself.
  - `pub struct QidoPage { pub rows: Vec<QidoRow>, pub more: bool }`.
  - `Index::qido(&self, q: &QidoQuery, max_results: u32) -> Result<QidoPage, sqlx::Error>` — `limit = min(q.limit.unwrap_or(max), max)`; fetch `limit + 1` to set `more`; order by the level's id ascending.
- Matching: `Single` exact (the two PN keys compare `fold_name(value)` against `patient_name_folded` / `referring_physician_name_folded`); `Wildcard` → SQLite `GLOB` with `[` and `]` escaped as `[[]` / `[]]` (PN keys: folded pattern against the folded column); `List` → `IN`; `Range` on DA as string compare; TM bounds normalized — lower padded with `0`s, upper with `59`/`59.999999` to `HHMMSS.FFFFFF` — compared against stored values padded the same way in SQL; `ModalitiesInStudy` → `EXISTS` a series of the study with modality in the list; `RequestAttributeSequence` keys match only the universal case (deviation).

- [ ] **Step 1: Failing tests** (ingest synthesized instances: 3 patients, 4 studies, mixed modalities and dates) — `studies_all_with_counts`, `pn_wildcard_case_insensitive`, `pn_match_folds_non_ascii`, `patient_id_exact`, `study_date_range_and_open_ends`, `study_time_range_padding` (`1000-1200` includes `120030`), `modalities_in_study_list`, `uid_list`, `series_with_higher_level_key`, `instances_under_series`, `limit_offset_more`, `glob_metacharacters_are_literal` (`[A]*` matches the literal name `[A]X`), `representative_is_lowest_id`.
- [ ] **Step 2: Run** `cargo test -p slicestream-store --test qido` — fails.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** tests, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(store): QIDO-RS matching in SQL`

---

### Task 8: `slicestream-dicomweb` — Accept negotiation, multipart, BulkDataURI splice, base URL

**Files:**
- Create: `crates/slicestream-dicomweb/src/accept.rs`, `src/multipart.rs`, `src/urls.rs`
- Modify: `crates/slicestream-dicomweb/Cargo.toml` (`bytes`, `rand`), `src/lib.rs`
- Test: unit tests in each file

**Interfaces:**
- Produces:
  - `pub struct MediaRange { pub essence: String /* lower-case type/subtype */, pub multipart_type: Option<String> /* from type=, quotes removed */, pub transfer_syntax: Option<String>, pub q: f32 }`; `pub fn parse_accept(header: Option<&str>) -> Vec<MediaRange>` (missing header → one `*/*`).
  - `pub struct NotAcceptable(pub String)`.
  - `pub fn negotiate_json(accept: &[MediaRange]) -> Result<(), NotAcceptable>` — OK for `application/dicom+json`, `application/json`, `*/*`, `application/*`, or a `multipart/related` with `type=application/dicom+json`; XML-only → error.
  - `pub fn negotiate_instances(accept: &[MediaRange], stored_ts: &[&str], count: usize) -> Result<InstancePlan, NotAcceptable>`; `pub struct InstancePlan { pub single_part: bool }`. Each part's type comes from `instance_part_type(ts)`.
  - `pub struct FrameSource<'a> { pub transfer_syntax: &'a str, pub encapsulated: bool, pub bits_allocated: u16 }`; `pub fn negotiate_frames(accept: &[MediaRange], src: &FrameSource, part_count: usize) -> Result<PartPlan, NotAcceptable>`.
  - `pub struct PartPlan { pub single_part: bool, pub part_content_type: String, pub byte_swap: bool }` — `part_content_type` examples: `application/octet-stream; transfer-syntax=1.2.840.10008.1.2.1`, `image/jpeg; transfer-syntax=1.2.840.10008.1.2.4.50`.
  - `pub fn instance_part_type(ts: &str) -> String` (`application/dicom; transfer-syntax=<ts>`); `pub fn media_type_for_ts(ts: &str) -> Option<&'static str>` — JPEG `.50 .51 .57 .70` → `image/jpeg`; JPEG-LS `.80 .81` → `image/jls`; JPEG 2000 `.90 .91` → `image/jp2`; HTJ2K `.201 .202 .203` → `image/jphc`; RLE `1.2.840.10008.1.2.5` → `image/dicom-rle`; JPEG XL `.110 .111 .112` → `image/jxl`; native → `None`.
  - Frame rules exactly as the spec table: bare `application/octet-stream` (no TS) → native little-endian as stored, big-endian with `byte_swap = true` (BA 16/32), encapsulated → `NotAcceptable`; `transfer-syntax=*` → as stored; a specific TS → as stored if equal; image media types → as stored when `media_type_for_ts(stored)` matches (and TS matches if given). Ranges tried in q order, ties in header order. A bare (non-multipart) media type → `single_part = true`, allowed only when `part_count == 1`.
  - `pub struct Multipart { boundary: String }`: `fn new() -> Self` (random 32 hex chars), `fn content_type(&self, type_param: &str) -> String` (`multipart/related; type="<type>"; boundary=<b>`), `fn part_header(&self, content_type: &str) -> Bytes` (`--<b>\r\nContent-Type: <ct>\r\n\r\n`, preceded by `\r\n` for every part but the first), `fn close(&self) -> Bytes` (`\r\n--<b>--\r\n`).
  - `pub fn splice_bulk_uris(json: &[u8], prefix: &str, out: &mut Vec<u8>)` — copies `json`, inserting `prefix` (ends with `/`) right after each `"BulkDataURI":"`.
  - `pub fn base_url(public_url: Option<&str>, host: Option<&str>, fallback_host: &str) -> String` — `public_url` with trailing `/` removed, else `http://<host>` (host filtered as STOW does today), else `http://<fallback_host>`.
  - `pub fn instance_url(base: &str, study: &str, series: &str, sop: &str) -> String` (`{base}/dicom-web/studies/{study}/series/{series}/instances/{sop}`), `study_url`, `series_url`.

- [ ] **Step 1: Failing tests** — `accept_missing_is_any`, `accept_quoted_and_unquoted_type`, `accept_q_values_order`, `accept_multiple_ranges`, `json_xml_only_406`, `json_star`, `frames_octet_native`, `frames_octet_big_endian_swaps`, `frames_octet_encapsulated_406`, `frames_star_as_stored`, `frames_specific_ts_match_and_mismatch`, `frames_image_jpeg`, `frames_image_jphc_wrong_ts_406`, `frames_bare_single_part_only_for_one`, `instances_star_and_absent_as_stored`, `instances_specific_ts_mismatch_406`, `instances_bare_dicom_single`, `media_type_table`, `multipart_exact_bytes` (two parts, boundary `b`), `splice_inserts_prefix_only_after_keys`, `base_url_public_trailing_slash`, `base_url_from_host`.
- [ ] **Step 2: Run** `cargo test -p slicestream-dicomweb` — fails.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** tests, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(dicomweb): Accept negotiation, multipart writer and URL helpers`

---

### Task 9: Server — config, routing, backfill task, QIDO-RS handlers

**Files:**
- Create: `crates/slicestream-server/src/dicomweb/mod.rs`, `src/dicomweb/qido.rs`, `crates/slicestream-server/tests/qido.rs`
- Modify: `crates/slicestream-server/Cargo.toml` (`slicestream-dicomweb`; tower-http features `compression-gzip`, `compression-br`), `src/config.rs`, `src/lib.rs`, `src/stow.rs` (`base_url` → `slicestream_dicomweb::base_url` with `public_url`), `crates/slicestream-server/tests/support/mod.rs` (HTTP GET helper returning status, headers, body), `slicestream.example.toml`, `docs/CONFORMANCE.md`

**Interfaces:**
- Consumes: Tasks 5–8.
- Produces:
  - `HttpConfig.public_url: Option<String>` (must start with `http://` or `https://`, else config error); `pub struct DicomwebConfig { pub enabled: bool, pub max_results: u32 }` (`Default` true / 1000; `max_results = 0` is a config error); `Config.dicomweb`.
  - `pub fn dicomweb::routes() -> Router<Arc<AppState>>` with the six QIDO routes (Tasks 10–11 add theirs); merged into the `read` router when `enabled`; a `CompressionLayer` at fastest level applied only to `application/dicom+json` responses.
  - CORS layer: `allow_headers([ACCEPT])`, expose `x-slicestream-frame-info` and `warning`.
  - Startup: after recovery spawn a task looping `ingestor.backfill_batch(64)` until it returns 0 or shutdown; `info!` progress every batch and `info!(scanned, "metadata backfill done")`. When `enabled` and `!http_addr.ip().is_loopback()`: `warn!("DICOMweb read routes are unauthenticated: anyone who can reach {addr} can read patient data (see SECURITY.md)")`.
  - QIDO response: one JSON object per row with the level's `default_fields` (computed: (0020,1206) series count, (0020,1208) / (0020,1209) instance counts, (0008,0061) Modalities in Study, (0008,1190) Retrieve URL, (0008,0056) `ONLINE`, (0008,0005) `ISO_IR 192`), plus `includefield` tags taken from the representative instance's metadata (`Index::metadata_batch`; parse with `serde_json`); `all` adds Patient, General Study and Patient Study module tags (study level) or General Series module tags (series level) from a fixed list in `qido.rs`, all non-bulk attributes at instance level. Warnings as `Warning: 299 <host>: "<text>"` with the PS3.18 texts for fuzzy matching and for more results. Errors: `QueryError` → 400 `text/plain` with its `Display`; `NotAcceptable` → 406.
  - Example TOML gains the spec's `public_url` comment line and `[dicomweb]` block verbatim.

- [ ] **Step 1: Failing tests** in `tests/qido.rs` (server on port 0, temp root, STOW of synthesized instances) — `studies_lists_with_counts_and_retrieve_url`, `patient_name_wildcard_case_insensitive`, `study_date_range`, `modalities_in_study`, `series_under_study`, `series_at_root_with_patient_id`, `instances_under_series`, `includefield_from_metadata` (SeriesDate set only in the file), `includefield_all_study_level`, `fuzzymatching_warning_header`, `limit_more_warning_header`, `unsupported_key_400` (or warning, per Task 6's reading), `xml_only_406`, `empty_result_200_array`, `dicomweb_disabled_404`, `cors_preflight_allows_accept` (configured origin), `no_cors_without_origin_config`, `json_gzip_when_accepted`, `stow_retrieve_url_uses_public_url`, `config_rejects_bad_public_url_and_zero_max_results`.
- [ ] **Step 2: Run** `cargo test -p slicestream-server --test qido` — fails.
- [ ] **Step 3: Implement.** Add CONFORMANCE.md section "DICOMweb read → QIDO-RS": resources, matching keys and match types, includefield, limits, warnings, status codes, deviations (no fuzzy matching, PN case-insensitive, Request Attribute Sequence universal only), with the PS3.18 clauses Task 6 checked.
- [ ] **Step 4: Run** `cargo test --workspace`, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(server): QIDO-RS, [dicomweb] config and metadata backfill`

---

### Task 10: Server — WADO-RS metadata and instance retrieve

**Files:**
- Create: `crates/slicestream-server/src/dicomweb/metadata.rs`, `src/dicomweb/retrieve.rs`, `crates/slicestream-server/tests/wado.rs`
- Modify: `src/dicomweb/mod.rs`, `docs/CONFORMANCE.md`

**Interfaces:**
- Consumes: `Index::find_instances`, `metadata_batch`, `Ingestor::ensure_scanned`, `negotiate_json`, `negotiate_instances`, `Multipart`, `splice_bulk_uris`, `instance_url`.
- Produces: routes `GET /dicom-web/studies/{study}/metadata`, `/studies/{study}/series/{series}/metadata`, `/studies/{study}/series/{series}/instances/{instance}/metadata`, `/studies/{study}`, `/studies/{study}/series/{series}`, `/studies/{study}/series/{series}/instances/{instance}`.
- Behaviour: `find_instances` empty → 404. Metadata: before the body starts, `ensure_scanned` every unscanned instance and check states; any `Unavailable(reason)` → 500 `text/plain` reason. Body is a stream: `[`, then per chunk of 128 ids the spliced objects joined by `,`, then `]`; prefix = `instance_url(...) + "/"`. Retrieve: negotiate on the stored transfer syntaxes; body streams each blob through a 1 MiB-chunk reader, each part with `instance_part_type(ts)`; single part for a bare `application/dicom` and exactly one instance. A blob that disappears mid-stream ends the stream with an error (logged).

- [ ] **Step 1: Failing tests** — `series_metadata_lists_every_instance` (SOP UIDs, (7FE0,0010) has an absolute `BulkDataURI` built from `Host`), `metadata_bulk_uri_uses_public_url`, `instance_metadata_404_wrong_series`, `unknown_study_404`, `metadata_scans_unscanned_instance` (reset scan rows through a second pool, then GET), `metadata_unavailable_500` (set `instance_data.metadata = NULL` and a problem through the pool), `non_image_instance_in_series_metadata` (an SR in the series is included), `study_retrieve_parts_equal_files` (parse multipart; each part equals the stored blob; part `Content-Type` carries the stored TS), `instance_single_part_bare_accept`, `bare_accept_multiple_instances_406`, `specific_ts_mismatch_406`, `retrieve_streams_large_instance` (64 MiB pixel data instance; the body arrives in more than one chunk), `metadata_gzip_when_accepted`.
- [ ] **Step 2: Run** `cargo test -p slicestream-server --test wado` — fails.
- [ ] **Step 3: Implement.** CONFORMANCE.md: metadata and instance retrieve, media types, the as-stored transfer syntax deviation (shared with Orthanc), status codes. Remove the note that RetrieveURLs return 404.
- [ ] **Step 4: Run** `cargo test --workspace`, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(server): WADO-RS metadata and instance retrieve`

---

### Task 11: Server — frames, bulk data, and the catalog on frame tables

**Files:**
- Create: `crates/slicestream-server/src/dicomweb/frames.rs`, `crates/slicestream-server/tests/frames.rs`
- Modify: `src/dicomweb/mod.rs`, `src/catalog.rs`, `docs/CONFORMANCE.md`

**Interfaces:**
- Consumes: `Index::stored_data`, `FrameTable::location`, `read_frame_at`, `read_frame_bytes`, `swap_to_little_endian`, `negotiate_frames`, `Multipart`, `SliceRow.frames`, `SliceRef::with_location`.
- Produces: routes `GET …/instances/{instance}/frames/{list}` and `GET …/instances/{instance}/bulk/{*path}`.
- Frames: `{list}` = comma-separated decimal u32, no empty items, at most 4,096 entries — else 400 `text/plain`; non-image instance (no `frame_info`) → 404; any number 0 or > Number of Frames → 404. Data source per instance: scanned with `offsets_valid` and a table → `read_frame_at`; scanned with no table and a problem → 500 with the problem; unscanned or `offsets_valid = false` → `read_frame_bytes` on the blocking pool. Parts in request order (duplicates repeated); byte swap when the plan says so.
- Bulk: `path` must be in the bulk list, else 404. `7FE00010` (Pixel Data) is the exception: it is served exactly like the frames resource with every frame requested (same `negotiate_frames` call, one part per frame). Without valid offsets, parse the file with dicom-object on the blocking pool and take the element at that path. One part (`application/octet-stream`), or single part for a bare media type.
- Catalog: `build` adds `with_location(table.location(frame))` for rows with frames, so `/api/series/{uid}/frames/{i}` and the WebSocket stream use `pread`. Bytes sent are unchanged.

- [ ] **Step 1: Failing tests** — `frame_octet_stream_matches_old_path` (every phantom frame equals `read_frame_bytes`), `frames_list_order_and_duplicates` (`3,1,3`), `frames_zero_and_past_end_404`, `frames_bad_lists_400` (`1,,2`, `abc`, `-1`, 5,000 entries), `encapsulated_frames_octet_406`, `encapsulated_frames_star_carry_ts`, `image_jpeg_accept`, `quoted_and_unquoted_type`, `big_endian_frames_swapped`, `bits_allocated_1_frame`, `frames_ambiguous_500`, `deflated_frames_fall_back`, `unscanned_frames_fall_back`, `overwrite_then_frames_new_bytes`, `bulk_private_ob_value`, `bulk_nested_path`, `bulk_pixel_data_all_frames`, `bulk_single_part_bare_accept`, `bulk_unknown_path_404`, `stream_and_api_frames_unchanged` (WebSocket and `/api/series/{uid}/frames/0` bytes equal the pre-change output for phantom and encapsulated fixtures).
- [ ] **Step 2: Run** `cargo test -p slicestream-server --test frames` — fails.
- [ ] **Step 3: Implement.** CONFORMANCE.md: frames and bulk data media types (the spec's table), single-part rules, status codes.
- [ ] **Step 4: Run** `cargo test --workspace`, fmt, clippy, +1.88 check — pass.
- [ ] **Step 5: Commit** `feat(server): WADO-RS frames and bulk data; pread in the series catalog`

---

### Task 12: Interop, docs, OHIF acceptance and performance

**Files:**
- Create: `scripts/interop-dicomweb.sh`, `scripts/bench-dicomweb.sh`, `crates/slicestream-core/examples/make_rle_multiframe.rs`, `docs/images/ohif.png`
- Modify: `.github/workflows/ci.yml` (interop job: `actions/setup-python@v5` + the new script), `README.md`, `docs/ARCHITECTURE.md`, `SECURITY.md`, `docs/CONFORMANCE.md`

**Interfaces:**
- Consumes: the whole server.
- `make_rle_multiframe <dir> <frames>`: one valid RLE Lossless (`1.2.840.10008.1.2.5`) multi-frame CT instance built from phantom pixels with a Basic Offset Table (RLE PackBits per PS3.5 Annex G), so OHIF can decode it.
- `interop-dicomweb.sh`: `set -euo pipefail`; temp root and config; start the server like `interop-dcmtk.sh`; create a venv and `pip install` a pinned `dicomweb-client` (newest release that supports the runner's Python); STOW the phantom and the RLE instance; with the client: search studies, series, instances (with a PatientName wildcard), retrieve series metadata, frames 1 and N of a phantom instance (compare to the file's pixel bytes with `pydicom`, pinned), the RLE instance's frames with `transfer-syntax=*`, a bulk data URI, and the instance; print `dicomweb interop OK`.
- `bench-dicomweb.sh`: 1,000-slice 512×512 phantom imported in release mode; reports series metadata time to last byte with and without `Accept-Encoding: gzip`, the median of 200 single-frame requests, and peak RSS while retrieving a 1 GB study (`/proc/<pid>/status` VmHWM before/after).
- README: API table rows for every new route; a "Use with OHIF" section with the spec's data source entry verbatim, the `cors_allowed_origins` line OHIF needs, and the screenshot; `public_url` and `[dicomweb]` in config notes. ARCHITECTURE.md: M1 done; known limits updated (no whole-file parse for indexed frames; no read auth until M6); crate table (`slicestream-dicomweb` exists; `slicestream-dimse` exists with C-ECHO and C-STORE). SECURITY.md: read routes are unauthenticated until M6, bind to loopback or put the server behind an authenticating proxy.

- [ ] **Step 1:** Write `make_rle_multiframe` with a unit test that decodes its own frames back to the phantom pixels; write both scripts.
- [ ] **Step 2:** Run `bash scripts/interop-dicomweb.sh` → prints `dicomweb interop OK`; `bash scripts/interop-dcmtk.sh` still prints `interop OK`.
- [ ] **Step 3: OHIF acceptance** (scratch dir, temp TOML, never `./storage`; stop the server by PID): run the newest `ohif/app:v3.13.x` image on port 3000 with the README entry mounted as `app-config.js`, server on 8080 with `cors_allowed_origins = ["http://localhost:3000"]`, phantom + RLE instance stored. With Playwright: study list shows both studies; each filter (patient name, MRN, accession, date range, modality, description) narrows the list; open the study; thumbnails render; scroll every series to both ends; zero console errors and zero failed requests. Repeat with `omitQuotationForMultipartRequest: false`, and with `singlepart: 'bulkdata,video'`. Save the screenshot to `docs/images/ohif.png`.
- [ ] **Step 4:** Run `bash scripts/bench-dicomweb.sh` and record the numbers in the final report against the spec targets (metadata < 150 ms uncompressed, < 250 ms gzip; frame median < 2 ms; RSS within a few MiB of idle). A missed target is reported with the measured number, not hidden.
- [ ] **Step 5:** Run `cargo test --workspace`, `cargo test -p slicestream-archive --no-default-features`, fmt, clippy, `cargo +1.88 check --workspace --all-targets`, `node --test 'web/tests/*.test.js'` — all pass.
- [ ] **Step 6: Commit** `docs: DICOMweb read path, OHIF setup, interop and benchmark scripts`
