# Test console design: upload, storage and process metrics in one page

Date: 2026-11-13 · Status: proposed · Approved mockup: `.superpowers/brainstorm/*/content/dashboard-v2.html` (layout reproduced below)

## Intent

The user wants to test slicestream by hand, end to end, from a browser:

1. upload DICOM files through the GUI,
2. see how each file was stored (stored, duplicate, failed, and why),
3. watch how the server reacts while that happens: RAM, disk, CPU and more.

Today the only GUI is a bare demo viewer (`web/index.html`, `web/client.js`),
which the user called bad. The console replaces it. It must **look good**:
a polished dark operations console, not a raw debug page.

## Goals

- One dashboard page (layout A), served by `serve` at `/`.
- Upload Part-10 files (multi-file pick, folder pick, drag and drop) over
  STOW-RS or `POST /api/instances`, with a per-file result row.
- Live metrics about **the slicestream process only** (not the whole machine),
  refreshed every second, with 5-minute charts.
- Storage view: bytes on disk split by objects / index / tmp, plus counts.
- Stored series table that updates as instances land; click a series to
  scroll it in the embedded viewer (the existing WebSocket stream).

## Non-goals

- Whole-machine monitoring. The only machine-level number is free space on
  the storage volume, as context for the storage bar.
- C-STORE upload from the browser (DIMSE is TCP; test it with DCMTK as today).
- Auth beyond the existing `stow_token`. Prometheus export (M6).
- Any build step, framework, npm package or CDN dependency.

## Anti-goals (failure even if it works)

- Looks like a default unstyled HTML page.
- Metrics that are wrong or that measure the browser instead of the server.
- The metrics endpoint itself costs noticeable CPU or disk I/O (for example,
  walking `objects/` every second).
- Uploading a 1000-file folder freezes or crashes the tab.

## Constraints

- Plain HTML, CSS and JavaScript (ES modules allowed), no build, no external
  network requests (system font stacks, inline SVG; no Google Fonts).
- Served from `web/` by the existing `ServeDir` fallback, same origin, so no
  CORS change.
- Rust: workspace MSRV 1.88, edition 2024, Unix (Linux and macOS). Every new
  dependency must build on 1.88.
- Existing security rules stay: Host allow-list on every route, ingest routes
  never get CORS.

## Layout (approved)

```
┌ header: logo · live pill · pid · version · uptime · http addr · dimse addr ┐
├──────────── left column ────────────┬──────── slicestream process ─────────┤
│ Upload: drop zone, mode switch      │ KPI row: CPU %, RSS, storage, ingest/s│
│   (STOW-RS | Single file), token    │ charts: CPU %, RSS MB,                │
│   field, Upload/Clear, progress     │   disk read/write MB/s, ingest latency│
│   bar with files/s, MB/s, ETA       │ storage bar: objects | index | tmp    │
│ Ingest results: totals + rows       │   + free space on volume              │
│   (file, status tag, SOP or reason, │ facts grid: threads, open FDs, virtual│
│   size, time)                       │   mem, DIMSE assoc n/max, WS streams, │
│                                     │   HTTP in-flight, studies, series,    │
│                                     │   instances                           │
├──────────── stored series ──────────┴──────── viewer ──────────────────────┤
│ patient, study date, series, slices,│ canvas, slice n/N, W/L, scrub bar     │
│ size; click selects                 │ showing cached frames + cursor        │
└─────────────────────────────────────┴──────────────────────────────────────┘
```

Visual language from the mockup: near-black background (`#0b0d10`), panels
`#12151a`, 1 px borders `#232830`, teal accent `#4fd1c5` (CPU), purple
`#9f7aea` (RAM), amber `#f6ad55` (disk I/O), green/grey/red status tags,
monospace for every number, small uppercase section labels. Below about
1000 px wide the two columns stack.

## Server side

### `GET /api/metrics`

Returns JSON, `Cache-Control: no-store`. *Builder's call:* it goes on its
own router with **no CORS** (only the same-origin console reads it), behind
the Host allow-list like every route. A new `[http] metrics_enabled = true`
switch turns both metrics endpoints off (404). No token: the server binds
`127.0.0.1` by default, and the exposure (pid, addresses, storage path, load)
is documented in SECURITY.md's deployment notes for anyone binding wider.

```json
{
  "sampled_at_ms": 1794551794123,
  "pid": 48211,
  "version": "0.1.0",
  "uptime_secs": 761,
  "http_addr": "127.0.0.1:8080",
  "dimse_addr": "0.0.0.0:11112",           // null when DIMSE disabled
  "process": {
    "cpu_percent": 37.2,                    // 100 = one full core
    "cores": 8,
    "rss_bytes": 88184832,
    "virtual_bytes": 1181116006,
    "threads": 18,                          // null if unavailable
    "open_fds": 52,                         // null if unavailable
    "disk_read_bytes_per_sec": 2097152,
    "disk_write_bytes_per_sec": 18454937,
    "disk_read_total_bytes": 104857600,
    "disk_write_total_bytes": 1310720000
  },
  "storage": {
    "root": "storage",
    "objects_bytes": 1267015680,
    "objects_count": 2304,
    "index_bytes": 22020096,                // index.sqlite + -wal + -shm
    "tmp_bytes": 8388608,
    "volume_free_bytes": 442381631488,
    "studies": 4, "series": 9, "instances": 2304
  },
  "ingest": {
    "stored": 71, "duplicate": 2, "failed": 1,      // since process start
    "per_sec": 34.0,                                 // over the last sample
    "latency_ms": { "p50": 17.0, "p95": 24.0 },      // last 60 s, null if none
    "bytes_per_sec": 18454937
  },
  "connections": {
    "dimse_active": 0, "dimse_max": 64,
    "ws_streams": 1,
    "http_in_flight": 3
  }
}
```

### `GET /api/metrics/history`

The last 300 samples (5 minutes at 1 s), oldest first, each the same shape
as `/api/metrics` minus the static fields (`pid`, `version`, addresses,
`storage.root`). The console calls it once on load so charts survive a page
reload, then polls `/api/metrics` every second.

### Sampler

A background task samples once per second into a 300-entry ring buffer
behind a lock; both endpoints only read the buffer. It stops on the
shutdown token. It always runs, so history exists when the console opens.
*Builder's call:* process numbers are read every second (cheap); the index
queries and file sizes (`storage.*`) are refreshed every **5 s** and the
cached values are reused in between, so an idle server does ~4 small
queries per 5 s. The sampler lives in the server crate (`metrics.rs`);
`sysinfo` is a server-crate dependency and must build on Rust 1.88 (pin a
version that does). Rules for each source:

- **CPU, RSS, virtual, disk I/O:** the `sysinfo` crate, refreshing only the
  own process. CPU % and I/O rates are deltas between consecutive samples.
  If the platform gives no per-process disk I/O, the four disk fields are
  `null` (not 0).
- **Threads, open FDs:** on Linux, count entries in `/proc/self/task` and
  `/proc/self/fd`. Elsewhere `null`. The console shows `—` for null.
- **objects_bytes / objects_count:** from the index
  (`SUM(size_bytes)` and `COUNT(DISTINCT blob_sha256)` over `instances`),
  never by walking `objects/`. (Instance rows sharing a blob count once.)
- **index_bytes:** file sizes of `index.sqlite`, `-wal`, `-shm`.
- **tmp_bytes:** sum of file sizes directly in `<root>/tmp/` (flat, small).
- **volume_free_bytes:** `fs4::available_space(root)`.
- **studies/series/instances:** `COUNT(*)` per table.
- **ingest counters and latency:** the `Ingestor` (store crate) keeps
  atomics for stored/duplicate/failed and total bytes, and a latency record
  (ms from `ingest()` start to ack) for the last 60 s, exposed as a new
  public `Ingestor::stats() -> IngestStats` snapshot. `stored` includes
  `replaced`. `failed` counts every `IngestFailure` returned by `ingest()`
  (one per STOW part, per `/api/instances` call, per C-STORE). Requests
  rejected before reaching `ingest()` (401, 413, 415, malformed multipart,
  spool errors) are **not** counted; they show only as failed rows in the
  console. Counted for every transport. `import` does not run the server.
- **dimse_active:** associations currently held (max minus free semaphore
  permits). The dimse crate exposes this as a cheap getter on the running
  SCP handle (for example an `Arc<AtomicUsize>` or the semaphore);
  `dimse_max` from config. Both `null` when DIMSE is disabled.
- **ws_streams:** a gauge incremented and decremented around each WebSocket
  stream session (decrement via drop guard).
- **http_in_flight:** a middleware gauge around every request, excluding the
  long-lived WebSocket after upgrade.

If one source fails (for example, an index query), that field is `null`
and the sampler logs at `debug`; the sample still goes out.

### `GET /api/console/series`

*Builder's call:* `/api/series` stays unchanged, because it builds a full
`Series` per row through the catalog and would be rebuilt over and over
during a big upload. The console instead reads a new **index-only**
endpoint (one SQL query with joins, no catalog), same router as the metrics
(no CORS, same switch). Each row: `series_uid`, `study_uid`,
`patient_name`, `study_date`, `modality`, `series_description`,
`instance_count`, `size_bytes` (sum of instance sizes), `updated_at`.
Ordered by `updated_at` descending. The viewer still uses `/api/series/{uid}`
and the stream for the series it opens.

## Client side (`web/`)

Files: `index.html` (layout), `console.css`, `console.js` (wiring),
`upload.js`, `metrics.js` (polling + SVG charts), `viewer.js` (WebSocket
stream and canvas, ported from today's `client.js`). `client.js` is removed.

### Upload

- Pick files, pick a folder (`webkitdirectory`), or drop files/folders.
  Folders are walked recursively. Every file is tried; the server decides
  what is valid (non-DICOM files come back as failed rows).
- Mode switch: **STOW-RS** sends each file as its own
  `multipart/related; type="application/dicom"` request to
  `/dicom-web/studies`; **Single file** sends it to `/api/instances`.
  One file per request gives every row an exact result. (The mockup said
  "batch of 25"; dropped for that reason.)
- Up to 4 uploads in flight at once. Clear cancels pending uploads.
- Optional bearer token field, kept in `sessionStorage` only, sent only on
  the two upload routes (read routes need none).
- Each row: file name, status tag (STORED / DUPLICATE / REPLACED / failure
  code), SOP Instance UID (shortened, full on hover) or reason text, size,
  and client-measured round-trip time. Where results come from:
  - Single file: the JSON body (`status`, `sopInstanceUid`; on failure the
    error JSON's status and message).
  - STOW-RS: the DICOM JSON body. A ReferencedSOPSequence (00081199) item
    means stored (the server does not say duplicate over STOW, so the tag
    is STORED); a FailedSOPSequence (00081198) item gives the code from
    FailureReason (00081197) as hex, with the text from the table in
    `docs/CONFORMANCE.md`.
  - HTTP-level errors (401, 413, 415, 400, network) show as failed rows
    with the HTTP status.
- Progress bar with done/total files, bytes, files/s, MB/s, ETA.
- The results table keeps the newest 500 rows in the DOM; totals count all.
  1000+ file folders must not freeze the tab (files are read lazily, one
  per upload slot).

### Metrics

- Poll `/api/metrics` every 1 s; if a poll fails, the live pill turns red
  ("offline") and polling continues.
- Charts are inline SVG, redrawn from the in-memory ring (300 points).
  Charts: CPU %, RSS MB, disk read and write MB/s (two lines), ingest
  latency p50/p95.
- Number formatting: bytes in binary units (KB, MB, GB) with 3 significant
  digits; `—` for null.

### Series table and viewer

- Refresh `/api/console/series` after successful uploads (debounced to at
  most once per second) and every 5 s.
- Click a row: the viewer opens the WebSocket stream for that series and
  renders native grayscale frames on a canvas, scroll by wheel or arrow
  keys, with slice n/N, window/level, and the scrub bar showing cached
  frames and the cursor. Same protocol as today
  ([PROTOCOL.md](../../PROTOCOL.md)).

## Testing

- Rust: unit tests for the latency window percentiles and the ring buffer;
  integration test that `/api/metrics` returns the documented shape and that
  the ingest counters move after a STOW upload; `/api/series` new fields.
- Browser: manual pass with Playwright against a running server using the
  phantom series: upload 120 files in both modes, upload a non-DICOM file,
  upload duplicates, open the series, scroll. Screenshot for the PR.

## Calls made without the user

Marked *Builder's call* above: metrics on a no-CORS router with an on/off
switch and no token; storage numbers refreshed every 5 s; null disk I/O
where unsupported; what counts as failed; index-only console series
endpoint; token only on upload routes; STOW rows say STORED even for
duplicates.

## Left to the builder

Exact module names in Rust, chart axis details, and the empty states
(no series yet, no uploads yet) as long as they match the visual language.
