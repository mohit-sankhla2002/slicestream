# Test Console Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the bare demo viewer with a polished one-page test console (upload, ingest results, live slicestream process metrics, storage usage, series table, viewer), backed by new metrics endpoints.

**Architecture:** The store crate gains ingest counters/latency and two index queries; the dimse crate exposes a live association count; the server crate gains a 1 s background sampler with a 300-sample ring buffer and three no-CORS endpoints (`/api/metrics`, `/api/metrics/history`, `/api/console/series`). The browser side is plain ES modules in `web/`, with pure helpers tested by `node --test`.

**Tech Stack:** Rust 2024 (MSRV 1.88), axum 0.8, sqlx 0.8 (SQLite), `sysinfo` 0.38 (`default-features = false, features = ["system"]`), `fs4` 1.1; plain HTML/CSS/JS, inline SVG, Node's built-in test runner for pure JS helpers.

**Spec:** `docs/superpowers/specs/2026-11-13-test-console-design.md`

## Global Constraints

- Rust MSRV 1.88, edition 2024; `cargo +1.88 check --workspace --all-targets` must pass. Every new crate dependency must build on 1.88 (`sysinfo` 0.38.x declares rust-version 1.88).
- `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` clean after every task.
- Unix only (Linux, macOS). Linux-only facts (`/proc`) are `#[cfg(target_os = "linux")]` and yield `None` elsewhere.
- Web: plain HTML, CSS, JavaScript (ES modules). No build step, no npm dependencies, no CDN, no external network requests (system font stacks; inline SVG).
- Host allow-list stays on every route. Ingest routes never get CORS. The new console/metrics routes never get CORS.
- Metrics measure the slicestream process only; the only machine-level number is `volume_free_bytes`.
- The metrics endpoints must never walk `objects/`.
- JSON field names exactly as in the spec's `/api/metrics` example.
- Never commit real patient data. Test data comes from `examples/make_phantom.rs` or is built in tests.

## Review Focus

1. A folder upload containing non-DICOM files (`.DS_Store`, `DICOMDIR`, `.txt`): each gets a failed row; the upload continues; the tab stays responsive. Pinned in Task 7 (`classifyStow` / `classifyInstance` tests on 400/415 bodies) and Task 9 manual pass.
2. Server restarted while the console is open: the live pill goes red ("offline"), comes back by itself, and the charts do not draw a line through the gap. Pinned in Task 6 (`pushSample` gap test).
3. `stow_token` set and the token field empty or wrong: every row says 401, with no silent success. Pinned in Task 7 (`classifyStow` 401 test).
4. Ingest future cancelled mid-flight (client disconnects during a STOW): counters stay consistent (no negative or double counting). Pinned in Task 1 (`stats_count_each_outcome_once`).
5. Empty storage (fresh root): `/api/metrics` returns zeros and `null` latency, not an error; the console shows empty states. Pinned in Task 2 (`storage_counts_empty`) and Task 5 (`metrics_on_empty_storage`).

---

### Task 1: Ingest counters and latency window (store crate)

**Files:**
- Create: `crates/slicestream-store/src/stats.rs`
- Modify: `crates/slicestream-store/src/ingest.rs` (record outcome in `Ingestor::ingest`), `crates/slicestream-store/src/lib.rs` (export)
- Test: unit tests in `stats.rs`; `crates/slicestream-store/tests/ingest.rs`

**Interfaces:**
- Produces:
  - `pub struct IngestStats { pub stored: u64, pub duplicate: u64, pub failed: u64, pub bytes: u64, pub latency_p50_ms: Option<f64>, pub latency_p95_ms: Option<f64> }` (derive `Debug, Clone, Copy, PartialEq, Default, serde::Serialize`), re-exported from the crate root.
  - `pub fn Ingestor::stats(&self) -> IngestStats`.
  - Internal `LatencyWindow` with `fn record(&mut self, at: Instant, ms: f64)` and `fn percentiles(&mut self, now: Instant) -> (Option<f64>, Option<f64>)`.

Rules: `stored` counts stored and replaced; `duplicate` counts duplicates; `failed` counts every `Err(IngestFailure)` returned by `ingest()`. `bytes` adds the spooled length of every successful ingest (stored, replaced, duplicate). Latency = ms from `ingest()` entry to its return, recorded for successes only. The window keeps 60 s and at most 10 000 entries (drop oldest). Percentile = nearest rank on sorted values (`ceil(p/100 * n)`-th value). Counters are `AtomicU64` (Relaxed); the window is a `std::sync::Mutex<VecDeque<(Instant, f64)>>`. If the `ingest()` future is dropped before it returns, nothing is recorded (the detached store task still completes; acceptable undercount).

- [ ] **Step 1: Write failing unit tests in `stats.rs`**

```rust
#[test]
fn percentiles_nearest_rank() {
    let mut w = LatencyWindow::default();
    let t = Instant::now();
    for ms in 1..=100 { w.record(t, ms as f64); }
    assert_eq!(w.percentiles(t), (Some(50.0), Some(95.0)));
}
#[test]
fn window_drops_entries_older_than_60s() {
    let mut w = LatencyWindow::default();
    let t0 = Instant::now();
    w.record(t0, 500.0);
    w.record(t0 + Duration::from_secs(61), 10.0);
    assert_eq!(w.percentiles(t0 + Duration::from_secs(61)), (Some(10.0), Some(10.0)));
}
#[test]
fn window_empty_is_none() {
    assert_eq!(LatencyWindow::default().percentiles(Instant::now()), (None, None));
}
#[test]
fn window_caps_at_10000() { /* record 10_050 values 0..10_050 at one instant; p50 == 5_049.0 (values 50..10_049 kept) */ }
```

- [ ] **Step 2: Write failing integration test `stats_count_each_outcome_once` in `tests/ingest.rs`**

Using the existing test helpers in that file: ingest one valid instance (stored), the same file again (duplicate), and an invalid file (failed). Assert `stats()` == `stored 1, duplicate 1, failed 1`, `bytes == 2 * file_len`, `latency_p50_ms.is_some()`.

- [ ] **Step 3: Run** `cargo test -p slicestream-store stats` — expect compile failure (types missing).
- [ ] **Step 4: Implement** `stats.rs`, hold a `Stats` in `Ingestor` (in its shared state), record in `ingest()`, add `stats()`.
- [ ] **Step 5: Run** `cargo test -p slicestream-store` — all pass. Run fmt and clippy.
- [ ] **Step 6: Commit** `feat(store): ingest counters and latency window`

---

### Task 2: Index queries for storage counts and console series (store crate)

**Files:**
- Modify: `crates/slicestream-store/src/index.rs`, `crates/slicestream-store/src/lib.rs` (export)
- Test: `crates/slicestream-store/tests/index.rs`

**Interfaces:**
- Produces:
  - `pub struct StorageCounts { pub studies: i64, pub series: i64, pub instances: i64, pub objects_bytes: i64, pub objects_count: i64 }` (derive `Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize`).
  - `pub async fn Index::storage_counts(&self) -> Result<StorageCounts, sqlx::Error>`; `objects_*` are computed over distinct `blob_sha256` (`GROUP BY blob_sha256`, `MAX(size_bytes)` per blob), so a blob shared by rows counts once.
  - `pub struct ConsoleSeriesRow { pub series_uid: String, pub study_uid: String, pub patient_name: Option<String>, pub study_date: Option<String>, pub modality: Option<String>, pub series_description: Option<String>, pub instance_count: i64, pub size_bytes: i64, pub updated_at: String }` (derive `Debug, Clone, PartialEq, Eq, serde::Serialize`).
  - `pub async fn Index::console_series(&self) -> Result<Vec<ConsoleSeriesRow>, sqlx::Error>`: one query joining series → studies → patients with instance aggregates, `ORDER BY series.updated_at DESC, series.id DESC`.

- [ ] **Step 1: Failing tests**
  - `storage_counts_empty`: fresh index → `StorageCounts::default()`.
  - `storage_counts_after_inserts`: upsert 3 instances (2 in series A, 1 in series B, same study; sizes 100, 200, 300) → studies 1, series 2, instances 3, objects_count 3, objects_bytes 600.
  - `console_series_rows`: same data → 2 rows; the row for A has `instance_count 2`, `size_bytes 300`, `patient_name` and `study_date` equal to the inserted header's values; the series updated last comes first.
- [ ] **Step 2: Run** `cargo test -p slicestream-store --test index` — fails to compile.
- [ ] **Step 3: Implement** both queries.
- [ ] **Step 4: Run** tests, fmt, clippy — pass.
- [ ] **Step 5: Commit** `feat(store): storage counts and console series queries`

---

### Task 3: Live DIMSE association count (dimse crate)

**Files:**
- Modify: `crates/slicestream-dimse/src/scp.rs`, `crates/slicestream-dimse/src/lib.rs` if exports change
- Test: `crates/slicestream-dimse/tests/scp.rs`

**Interfaces:**
- Produces: `pub fn StoreScp::active_associations(&self) -> Arc<AtomicUsize>` — a handle cloned before `run(self, …)` consumes the SCP. The count goes up when an association task starts (permit acquired) and down when it ends, via a drop guard inside the task (so aborts and panics also decrement).

- [ ] **Step 1: Failing test `active_associations_tracks_open_association`**: bind SCP, take the handle, run it; open an association with `dicom-ul` client (as other tests do); poll until handle == 1 (timeout 5 s); release the association; poll until 0.
- [ ] **Step 2: Run** `cargo test -p slicestream-dimse --test scp active_associations` — fails.
- [ ] **Step 3: Implement** (field `active: Arc<AtomicUsize>` on `StoreScp`; guard struct stored next to the semaphore permit in the association task).
- [ ] **Step 4: Run** dimse tests, fmt, clippy — pass.
- [ ] **Step 5: Commit** `feat(dimse): expose active association count`

---

### Task 4: Metrics sampler and ring buffer (server crate)

**Files:**
- Create: `crates/slicestream-server/src/metrics.rs`
- Modify: `crates/slicestream-server/Cargo.toml` (add `sysinfo = { version = "0.38", default-features = false, features = ["system"] }`, `fs4 = "1.1.0"`), `crates/slicestream-server/src/lib.rs` (`mod metrics;`)
- Test: unit tests in `metrics.rs`

**Interfaces:**
- Consumes: `Ingestor::stats()` (Task 1), `Index::storage_counts()` (Task 2), `StoreScp::active_associations()` (Task 3).
- Produces (all in `metrics.rs`, `pub(crate)` unless used by tests outside):
  - `#[derive(Default)] pub struct Gauges { pub ws_streams: AtomicUsize, pub http_in_flight: AtomicUsize }`.
  - `pub struct GaugeGuard` — `Gauges`-counter guard: `GaugeGuard::enter(counter: &Arc<Gauges>, which: Gauge) -> GaugeGuard` increments, `Drop` decrements; `pub enum Gauge { WsStreams, HttpInFlight }`.
  - Serde structs matching the spec JSON exactly: `Sample { sampled_at_ms: u64, uptime_secs: u64, process: ProcessSample, storage: StorageSample, ingest: IngestSample, connections: ConnectionsSample }` and `Snapshot { pid: u32, version: &'static str, http_addr: String, dimse_addr: Option<String>, #[serde(flatten)] sample: Sample }`. `StorageSample` includes `root: Option<String>` — `Some` only in `Snapshot` (serialize with `skip_serializing_if = "Option::is_none"`; history samples carry `None`). Optional numbers serialize as `null`. `ingest.latency_ms` is `{ "p50": f64|null, "p95": f64|null }`.
  - `pub struct MetricsSources { pub ingestor: Arc<Ingestor>, pub gauges: Arc<Gauges>, pub dimse_active: Option<Arc<AtomicUsize>>, pub dimse_max: Option<usize>, pub http_addr: SocketAddr, pub dimse_addr: Option<SocketAddr> }`.
  - `pub struct Metrics` with `pub fn latest(&self) -> Option<Snapshot>` and `pub fn history(&self) -> Vec<Sample>` (oldest first, at most 300).
  - `pub fn spawn_sampler(sources: MetricsSources, shutdown: CancellationToken) -> Arc<Metrics>` — samples immediately, then every 1 s (`tokio::time::interval`, `MissedTickBehavior::Skip`) until `shutdown`.
  - `pub(crate) struct Ring<T>` with `new(cap)`, `push`, `last`, `to_vec`.

Sampling rules (from the spec): process CPU/RSS/virtual/disk via `sysinfo`, refreshing only the own pid; `cpu_percent` where 100 = one core; `cores` = `std::thread::available_parallelism()`; disk rates = delta bytes / elapsed secs between samples; disk fields `None` if sysinfo reports no disk usage support (on Linux they are always present). Threads = entries of `/proc/self/task`, open FDs = entries of `/proc/self/fd` (Linux only, else `None`). Storage block (`storage_counts`, sizes of `index.sqlite`, `index.sqlite-wal`, `index.sqlite-shm`, flat sum of `<root>/tmp/*` file sizes, `fs4::available_space(root)`) refreshed when ≥ 5 s old, otherwise previous values reused; any source error makes that field `None` and logs at `debug`. All blocking file/procfs work goes through `spawn_blocking`. Ingest: counters from `stats()`, `per_sec` = Δ(stored+duplicate+failed) / elapsed, `bytes_per_sec` = Δbytes / elapsed (0 on the first sample).

- [ ] **Step 1: Failing unit tests**
  - `ring_keeps_last_cap_items`: `Ring::new(3)`, push 1..=5 → `to_vec() == [3,4,5]`, `last() == Some(&5)`.
  - `gauge_guard_decrements_on_drop`: enter twice, drop one → counter 1.
  - `snapshot_json_shape`: build a `Snapshot` with fixed values, serialize, assert the key set of top level is exactly `{sampled_at_ms, pid, version, uptime_secs, http_addr, dimse_addr, process, storage, ingest, connections}`, `process.threads` is `null` when `None`, `ingest.latency_ms.p50` present, and a `Sample` (history) has no `pid` and no `storage.root`.
  - `rate_from_deltas`: helper `fn rate(prev: u64, now: u64, secs: f64) -> f64` → `rate(100, 1100, 2.0) == 500.0`, `rate(5, 3, 1.0) == 0.0` (counter reset never negative).
- [ ] **Step 2: Run** `cargo test -p slicestream-server metrics` — fails.
- [ ] **Step 3: Implement** `metrics.rs`.
- [ ] **Step 4: Run** tests, fmt, clippy, `cargo +1.88 check -p slicestream-server` — pass.
- [ ] **Step 5: Commit** `feat(server): process and storage metrics sampler`

---

### Task 5: Console endpoints, gauges and config switch (server crate)

**Files:**
- Modify: `crates/slicestream-server/src/lib.rs` (`AppState` gains `metrics: Option<Arc<Metrics>>` and `gauges: Arc<Gauges>`; console router; in-flight middleware; `start()` wiring), `routes.rs` (handlers), `stream.rs` (ws gauge guard), `config.rs` (`metrics_enabled: bool`, default `true`), `slicestream.example.toml`, `SECURITY.md` (deployment note), `README.md` (HTTP API table rows)
- Test: `crates/slicestream-server/tests/e2e.rs` (or a new `tests/console.rs`)

**Interfaces:**
- Consumes: Task 4's `spawn_sampler`, `Metrics::{latest, history}`, `Gauges`, `GaugeGuard`; Task 2's `Index::console_series()`.
- Produces HTTP:
  - `GET /api/metrics` → 200 `Snapshot` JSON, or 503 if no sample yet; `Cache-Control: no-store`.
  - `GET /api/metrics/history` → 200 JSON array of `Sample`; `Cache-Control: no-store`.
  - `GET /api/console/series` → 200 JSON array of `ConsoleSeriesRow`; `Cache-Control: no-store`.
  - All three on a router with **no CORS layer**, merged before the Host guard; when `metrics_enabled = false` they are not mounted (404) and no sampler runs.
- In-flight middleware wraps every route (including static files) using `GaugeGuard` with `Gauge::HttpInFlight`; it decrements when the response is returned (so a WebSocket counts only until upgrade). The WebSocket session holds a `Gauge::WsStreams` guard for its whole life.

- [ ] **Step 1: Failing tests** (start the server the way existing e2e tests do, on port 0 with a temp root):
  - `metrics_on_empty_storage`: GET `/api/metrics` (retry up to 2 s while 503) → 200; `storage.instances == 0`, `ingest.stored == 0`, `ingest.latency_ms.p50` is null, `process.rss_bytes > 0`, `pid == std::process::id()`, header `cache-control: no-store`.
  - `metrics_count_stow_upload`: POST one phantom instance via `/api/instances`; within 3 s `/api/metrics` shows `ingest.stored == 1` and (within 6 s) `storage.instances == 1`.
  - `history_grows`: after ≥ 2 s, `/api/metrics/history` has ≥ 2 entries, oldest first (`sampled_at_ms` ascending), none with `pid`.
  - `console_series_lists_uploaded`: after the upload, `/api/console/series` has one row with `instance_count 1`.
  - `console_routes_have_no_cors`: with `cors_allowed_origins = ["https://viewer.example.org"]` and `Origin: https://viewer.example.org`, responses of the three routes have no `access-control-allow-origin`, while `/api/series` does.
  - `metrics_disabled_404`: `metrics_enabled = false` → all three return 404.
  - `ws_stream_gauge`: open a stream WebSocket to the uploaded series; within 3 s `connections.ws_streams == 1`; close it; within 3 s back to 0.
- [ ] **Step 2: Run** `cargo test -p slicestream-server` — new tests fail.
- [ ] **Step 3: Implement.** Docs: example TOML gets `metrics_enabled = true  # GET /api/metrics, /api/metrics/history, /api/console/series`; SECURITY.md notes these endpoints reveal pid, bind addresses, the storage path and load, and to set `metrics_enabled = false` or keep the server on localhost; README API table gains the three rows.
- [ ] **Step 4: Run** full `cargo test --workspace`, fmt, clippy, `cargo +1.88 check --workspace --all-targets` — pass.
- [ ] **Step 5: Commit** `feat(server): console metrics and series endpoints`

---

### Task 6: Console shell, styles and metrics panel (web)

**Files:**
- Create: `web/console.css`, `web/console.js` (entry, wires panels), `web/lib/format.js`, `web/lib/series.js` (sample ring + chart path helpers), `web/metrics.js` (polling + DOM), `web/tests/format.test.js`, `web/tests/series.test.js`
- Modify: `web/index.html` (replace with the console layout), `.github/workflows/ci.yml` (add step `node --test web/tests/` to the `test` job)

**Interfaces:**
- Produces:
  - `web/lib/format.js`: `export function formatBytes(n)` → binary units, 3 significant digits, `null`/`undefined` → `"—"` (e.g. `formatBytes(88184832) === "84.1 MB"`, `formatBytes(0) === "0 B"`, `formatBytes(1023) === "1023 B"`, `formatBytes(1536) === "1.50 KB"`); `export function formatRate(bytesPerSec)` → `formatBytes(x) + "/s"`; `export function formatDuration(secs)` → `"HH:MM:SS"` (`formatDuration(761) === "00:12:41"`, ≥ 100 h keeps all hour digits); `export function formatNumber(n, digits = 1)` → fixed digits or `"—"`.
  - `web/lib/series.js`: `export class SampleRing { constructor(cap = 300); push(sample); load(samples); values(selector) }` where `values(fn)` returns an array of numbers or `null` gaps; `push` inserts a `null` gap entry first if `sample.sampled_at_ms - last.sampled_at_ms > 3000`. `export function linePath(values, width, height, max)` → SVG path `d` string; `null` values break the line (new `M`); `max` defaults to max of values or 1.
  - `web/metrics.js`: `export function startMetrics({ onSample, onStatus })` — loads `/api/metrics/history` once, then polls `/api/metrics` every 1000 ms; `onStatus("live" | "offline")`.

Visual: reproduce the approved mockup (`.superpowers/brainstorm/*/content/dashboard-v2.html` — copy its CSS tokens and structure): header (logo, live pill, pid · version, uptime, http/dimse addrs), right-hand process card (KPI row: CPU, RAM RSS + peak since page load, storage total, ingest/s + p95; four charts: CPU %, RSS MB, disk write/read MB/s with legend, ingest latency p50/p95 lines; storage bar objects/index/tmp + free space; facts grid: threads, open FDs, virtual mem, DIMSE `active / max` or `off`, WS streams, HTTP in-flight, studies, series, instances). Left column and bottom row contain placeholder cards with the right headings, filled in Tasks 7–8. No external fonts. Below 1000 px wide the columns stack. Charts show y-axis max label and "5 min" window label. Null → "—".

- [ ] **Step 1: Failing tests** `web/tests/format.test.js` and `web/tests/series.test.js` (`node:test` + `node:assert/strict`) covering every example value above, plus `SampleRing` gap insertion, cap of 300, and `linePath([1,null,3], 100, 10, 3)` containing two `M` commands.
- [ ] **Step 2: Run** `node --test web/tests/` — fails (modules missing).
- [ ] **Step 3: Implement** the files.
- [ ] **Step 4: Run** `node --test web/tests/` — pass. Run the server on the phantom data (`cargo run -p slicestream-server -- serve` after `import`), load `http://127.0.0.1:8080/`, confirm metrics populate with no console errors.
- [ ] **Step 5: Commit** `feat(web): test console shell and live metrics panel`

---

### Task 7: Upload panel and ingest results (web)

**Files:**
- Create: `web/lib/upload-core.js` (pure: multipart body building, response classification, queue math), `web/upload.js` (DOM, file walking, concurrency), `web/tests/upload.test.js`
- Modify: `web/index.html`, `web/console.css`, `web/console.js`

**Interfaces:**
- Consumes: `formatBytes`, `formatRate` from Task 6.
- Produces (`web/lib/upload-core.js`):
  - `export function stowBody(fileBytes /* Uint8Array */, boundary)` → `Blob` of `--boundary\r\nContent-Type: application/dicom\r\n\r\n<bytes>\r\n--boundary--\r\n`; request header `Content-Type: multipart/related; type="application/dicom"; boundary=<boundary>`.
  - `export function classifyStow(httpStatus, json)` → `{ status: "STORED" | "FAILED", code: string|null, uid: string|null, reason: string|null }`. 200/202/409 parse DICOM JSON: an item of `00081199` → STORED with uid from `00081155`; an item of `00081198` → FAILED, `code` = `00081197` value as 4-digit upper hex (`"C000"`), `reason` from `FAILURE_TEXT`. Other statuses → FAILED with `code = "HTTP " + status` and reason from the body's `error` field if present.
  - `export function classifyInstance(httpStatus, json)` → same shape; 200 → `status` mapped from `json.status` (`stored` → STORED, `duplicate` → DUPLICATE, `replaced` → REPLACED), uid `json.sopInstanceUid`; error → FAILED, `code` from `json.status` as hex if numeric (`0xC000` → `"C000"`) else `"HTTP " + httpStatus`, reason `json.error`.
  - `export const FAILURE_TEXT = { C000: "cannot parse dataset / missing UID", A900: "UID mismatch", C409: "study UID does not match URL", "0122": "SOP class not supported", A700: "out of resources / too large" }` (check wording against `docs/CONFORMANCE.md`; keep the same codes).
  - `export function progress({ done, total, bytesDone, bytesTotal, elapsedMs })` → `{ filesPerSec, bytesPerSec, etaSecs }` (0 elapsed → zeros, ETA `null` when rate is 0).
- Before writing `classifyInstance`, read `ingest_error_response` in `crates/slicestream-server/src/stow.rs` for the exact error JSON shape and test against it.

Behavior (from the spec): pick files, pick folder (`webkitdirectory`), drag and drop (walk directories via `webkitGetAsEntry`, recursively); mode switch STOW-RS (`POST /dicom-web/studies`) / Single file (`POST /api/instances`, `Content-Type: application/dicom`); 4 uploads in flight; one file per request; file bytes read only when its slot starts; Clear aborts in-flight (`AbortController`) and drops pending; token input stored in `sessionStorage` (wrapped in try/catch) and sent as `Authorization: Bearer` only on uploads; rows newest first, DOM capped at 500 rows, totals count all; row columns file / status tag / SOP (shortened to `…` + last 12 chars, full in `title`) or reason / size / client round-trip ms; progress bar with done/total, bytes, files/s, MB/s, ETA; after each successful row, dispatch `window` event `slicestream:stored` (Task 8 listens).

- [ ] **Step 1: Failing tests** in `web/tests/upload.test.js`: `stowBody` exact bytes for a 3-byte payload with boundary `b`; `classifyStow` for (a) 200 with one ReferencedSOP item, (b) 409 with FailedSOP `00081197` = 49152 → code `"C000"`, (c) 401 empty body → `"HTTP 401"`, (d) 415 → `"HTTP 415"`; `classifyInstance` for `stored`, `duplicate`, `replaced`, and the error JSON from `ingest_error_response`; `progress` math including zero elapsed.
- [ ] **Step 2: Run** `node --test web/tests/` — fails.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** tests — pass. Manual: upload the 120 phantom files in each mode, plus one `.txt` file; rows and totals correct; metrics move.
- [ ] **Step 5: Commit** `feat(web): upload panel with per-file ingest results`

---

### Task 8: Series table and viewer (web)

**Files:**
- Create: `web/viewer.js` (WebSocket stream + canvas, ported from `web/client.js`), `web/seriestable.js`
- Delete: `web/client.js`
- Modify: `web/index.html`, `web/console.css`, `web/console.js`

**Interfaces:**
- Consumes: `/api/console/series` (Task 5), `/api/series/{uid}` and `/api/series/{uid}/stream` (existing; protocol in `docs/PROTOCOL.md`), `formatBytes`, `slicestream:stored` event (Task 7).
- Produces: `export function startSeriesTable({ onSelect })` (refresh every 5 s and on `slicestream:stored`, throttled to once per second; rows: patient, study date, series description (or modality + UID tail), slices, size; selected row highlighted; empty state "No series stored yet — upload some files"); `export function openViewer(seriesUid)` (closes any previous socket; header shows series name, first-frame ms, cached n/N; canvas with slice n/N and W/L overlays; wheel and ↑/↓ scroll; scrub bar showing cached frames and cursor; native grayscale only, other frames show "unsupported transfer syntax" overlay).

Keep every behavior of today's `client.js` (cursor messages, frame decode, window/level from frame info). Read `web/client.js` and `docs/PROTOCOL.md` first.

- [ ] **Step 1: Manual test script** (no pure logic added beyond `client.js`'s): with phantom data imported, open console, click the series row, scroll with wheel and arrows to both ends, switch series twice, upload more files and see the table update.
- [ ] **Step 2: Implement.**
- [ ] **Step 3: Run** the manual script; `node --test web/tests/` still passes; no console errors.
- [ ] **Step 4: Commit** `feat(web): series table and embedded viewer; remove demo client`

---

### Task 9: End-to-end check, docs and screenshot

**Files:**
- Modify: `README.md` (replace "demo viewer" wording with the console; quick start step 4 mentions the console panels), `docs/ARCHITECTURE.md` (Known limits: viewer still native grayscale only)
- Create: `docs/images/console.png` (screenshot with phantom data only)

- [ ] **Step 1:** Fresh storage root; `make_phantom` 120 slices; `serve`; with Playwright load the console, upload all 120 via STOW-RS, re-upload 10 via Single file (expect DUPLICATE), upload one non-DICOM file (expect failed row), open the series, scroll. Verify the Review Focus items 1–3 by hand (folder with junk files; restart server while open; set `stow_token` and upload without it).
- [ ] **Step 2:** Save the screenshot to `docs/images/console.png`, reference it in README.
- [ ] **Step 3:** Run `cargo test --workspace`, `node --test web/tests/`, fmt, clippy, `cargo +1.88 check --workspace --all-targets`, `scripts/interop-dcmtk.sh` — all pass.
- [ ] **Step 4: Commit** `docs: test console in README with screenshot`
