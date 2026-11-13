# Archive and NIfTI ingest design

Date: 2026-11-13 · Status: proposed · Builds on: [storage ingest](2026-11-12-storage-ingest-design.md), [test console](2026-11-13-test-console-design.md)

## Intent

Imaging reaches the user as downloads: email attachments, WhatsApp files,
exports from other PACS and viewers. These arrive as archives (ZIP, RAR, 7z,
tar.gz), often messy, sometimes one archive per patient packed inside another
archive, and sometimes with NIfTI volumes next to the DICOM. Someone downloads
the file and uploads it to slicestream by hand (console, HTTP API from any
language, or the `import` command). slicestream must open it, find every
DICOM image and NIfTI volume at any nesting level, store them durably the way
it already stores DICOM, and say exactly what happened. Fast and secure.

slicestream never fetches from email, WhatsApp or other systems itself.

## Goals

- Accept these containers, nested in any combination: **ZIP**, **7z**,
  **RAR**, **tar**, **tar.gz / .tgz**, and a plain **gzip** of any of these.
- Accept **NIfTI** volumes: `.nii` and `.nii.gz` (NIfTI-1 and NIfTI-2),
  loose or inside archives.
- Every DICOM Part-10 file found goes through the existing `Ingestor::ingest`
  pipeline (same validation, de-duplication, crash safety, metrics).
- Every NIfTI file is stored untouched as a content-addressed blob with an
  index row, linked to a patient/study when that can be inferred.
- Entry points: console upload, a new HTTP endpoint, and `import <path>`
  (folder or archive file).
- Per-entry results and a summary grouped by patient and study.

## Non-goals

- Fetching from email, WhatsApp, PACS or URLs.
- Converting NIfTI to DICOM, viewing NIfTI, or serving NIfTI (later
  "distribution" work).
- Keeping the original archive file (only its contents are stored).
- Password-protected archives (reported as failed, "encrypted").
- Creating archives (export) of any kind.

## Anti-goals

- A crafted archive (zip bomb, deep nesting, millions of tiny entries,
  `../` names, symlinks) fills the disk, exhausts memory, hangs the server,
  or writes outside the storage root.
- A 2 GB archive is loaded into memory.
- An acknowledged instance or volume is lost on crash.
- Real DICOM silently skipped because of its file name or extension.

## Constraints

- Rust MSRV 1.88, edition 2024; every new crate must build on 1.88.
  Candidates: `zip` 9.x (rust-version 1.88), `sevenz-rust2`, `tar`,
  `flate2`, `unrar` (wraps the UnRAR C++ library).
- Unix only, as today. Same security rules as today (Host allow-list on
  every route; ingest routes never get CORS; optional bearer token).
- Existing storage layout and guarantees unchanged.

## Storage ("the Orthanc way")

As today and as Orthanc does: files are stored untouched in the storage area,
spread over two levels of short directories, with a database index pointing at
them. Not byte-compatible with Orthanc's own folder (nothing but Orthanc could
read that).

- DICOM: unchanged, `objects/aa/bb/<sha256>.dcm` + `instances` row.
- NIfTI: `objects/aa/bb/<sha256>.nii` or `<sha256>.nii.gz` (stored exactly as
  received, compressed stays compressed). The blob store gains a per-kind
  extension; names still come only from the hash.
- Startup recovery (orphan GC) uses one rule for both kinds: a blob in
  `objects/` (any extension) that no `instances` or `nifti_volumes` row
  references is an orphan and is deleted. A bug here deletes user data, so
  it gets its own tests.
- Startup also marks `uploads` rows still `running` as `aborted`
  ("server restarted"). Jobs are not resumed; `tmp/` is swept as today.
  Items already stored by that job stay stored.

### New index tables (migration `0002`)

`uploads`: one row per upload/import run. `id`, `kind` (`http_archive`,
`import`), `name` (top-level file or folder name), `remote` (peer address, if
HTTP), `started_at`, `finished_at`, counts (`dicom_stored`, `dicom_duplicate`,
`nifti_stored`, `nifti_duplicate`, `skipped`, `failed`), `status`
(`running`, `done`, `aborted`).

`nifti_volumes`: `id`, `blob_sha256` UNIQUE, `size_bytes`, `compressed`
(bool), `nifti_version` (1|2), `original_name`, `dims` (JSON array),
`pixdim` (JSON array), `datatype` (int), `descrip` (text from header),
`patient_fk` NULL, `study_fk` NULL, `upload_fk`, `received_at`.
Duplicate (same hash) = success, flagged duplicate, row unchanged.

## Detection (by content, never by name)

Read the first bytes of each entry:

| Magic | Treat as |
|---|---|
| `PK\x03\x04` or `PK\x05\x06` | ZIP |
| `7z\xBC\xAF\x27\x1C` | 7z |
| `Rar!\x1A\x07\x00` / `Rar!\x1A\x07\x01\x00` | RAR 4 / RAR 5 |
| `\x1F\x8B` | gzip: decompress, then classify the inner stream (tar, NIfTI, DICOM, other) |
| `ustar` at offset 257 | tar |
| `DICM` at offset 128 | DICOM Part-10 |
| NIfTI-1: `sizeof_hdr` = 348 at offset 0 and magic `n+1\0` or `ni1\0` at 344; NIfTI-2: 540 at 0 and `n+2\0` at 4 | NIfTI |
| anything else | skipped |

`.nii` pairs (`.hdr`/`.img`) are skipped. DICOM without the Part-10 preamble
is skipped (same as `import` today). A `DICOMDIR` file is skipped.

## Processing

- The HTTP body is first spooled to a temp file under `<root>/tmp/` (ZIP and
  7z need random access), within `[http] max_request_bytes`.
- Extraction runs on blocking threads. Entries stream straight into
  `TempBlob`s (hash computed while writing), so nothing is held in memory.
  Nested archives are spooled to their own temp file, then opened.
- DICOM entries go to `Ingestor::ingest` through a bounded channel, so
  extraction and committing overlap. NIfTI entries go to a new
  `Ingestor::ingest_nifti` with the same commit order (fsync blob, rename,
  fsync dir, then index transaction).
- Entry names are only reported, never used as paths. Symlinks, hard links,
  devices and directories are ignored.

### Safety limits (`[archives]` config, every key has a default)

```toml
[archives]
enabled = true
max_depth = 4                       # nesting levels below the uploaded file
max_entries = 200000                # files seen across the whole upload
max_expanded_bytes = 68719476736    # 64 GiB of decompressed data per upload
min_free_bytes = 2147483648         # keep 2 GiB free on the volume
max_concurrent_jobs = 2             # further uploads wait in a queue
job_retention = 50                  # finished jobs kept in memory for the API
```

*Builder's call:* `max_expanded_bytes` counts every byte extraction writes
into `tmp/` (leaf files and spooled nested archives alike), so a ZIP of ZIPs
counts the inner ZIP once as a file and again as its contents. Simple and
conservative. The HTTP upload itself is capped by `[http]
max_request_bytes`. A new `min_free_bytes = 2147483648` (2 GiB) guard
refuses an upload (507) when the volume has less free space than its
`Content-Length` plus this margin, and aborts a running job ("disk almost
full") when free space drops below the margin (checked every 64 MiB written).

Exceeding `max_depth` marks that nested archive failed ("too deeply nested")
and continues. Exceeding `max_entries` or `max_expanded_bytes` aborts the
upload (already-stored items stay stored; status `aborted` with the reason).
Each entry is still bound by `[storage] max_instance_bytes`. A broken or
encrypted archive fails that archive only, not its parent.

### NIfTI linking

After an upload finishes, each NIfTI stored in it is linked using scopes
from the inside out: (1) DICOM in the same directory of the same archive,
(2) DICOM anywhere in the same archive, (3) the enclosing archive, and so on
up to the whole upload. The first scope that contains DICOM decides: exactly
one patient → link `patient_fk` (and `study_fk` if exactly one study);
more than one patient → stop, unassigned. No DICOM in any scope →
unassigned. *Builder's call:* DICOM counts when it was stored **or was a
duplicate** in this upload (the patient is known either way); failed and
skipped entries do not. "Patient" and "study" mean the index rows
(`patients.patient_id`, `studies.study_instance_uid`) the stored instance
points at. A NIfTI uploaded alone stays unassigned even if its patient is
already in the database (as agreed with the user). Loose files in an `import` folder: the folder is a scope, the
import root is the outermost. (Linking only uses DICOM from the same upload.)

## Interfaces

### HTTP

- `POST /api/archives` — body is the archive or NIfTI file (any
  Content-Type; detection by content). Same token and CORS rules as other
  ingest routes. Returns **202** `{ "job": "<id>", "upload_id": n }`
  immediately after the body is spooled; 413 if too large; 400 if the body
  is not a supported container/NIfTI/DICOM; 503 if the job queue is full
  (queue cap = 4 × `max_concurrent_jobs`). With `[archives] enabled = false`
  both archive routes are not mounted (404).
- `GET /api/archives/{job}` — progress and results:
  `status` (`queued|running|done|aborted`), `reason`, counts (as in
  `uploads`), `bytes_expanded`, `entries_seen`, `entries` (array, newest
  last, capped at 10 000 in memory: `path` inside the archive including
  nested archive names joined with `!/`, `kind` (`dicom|nifti|archive|other`),
  `outcome` (`stored|duplicate|replaced|skipped|failed`), `code`, `reason`,
  `sop_instance_uid`, `size`), and `patients` summary
  (`[{patient_id, patient_name, studies:[{study_uid, study_date,
  description, series, instances}]}]` + `nifti: {linked, unassigned}`).
  Jobs live in memory (`job_retention`); the `uploads` row persists.
  Unknown job → 404. Same router class as other read routes but **no CORS**
  (it reveals patient names).
- A bare DICOM or NIfTI file sent to `/api/archives` is accepted and runs as
  a one-entry job with its own `uploads` row.
- Existing STOW and `/api/instances` unchanged.

### CLI

`slicestream-server import <PATH>` accepts a folder (as today, `--recursive`
for subfolders) **or** a single archive / NIfTI file. Inside folders,
archives and NIfTI are now opened/stored too. Prints per-patient summary and
totals: `stored / duplicate / nifti / skipped / failed`. It runs in-process
(the server must be stopped, as today); it writes an `uploads` row of kind
`import` with `remote` NULL; queue and `job_retention` do not apply.

### Console

- The upload panel sends each picked or dropped file by type, detected from
  its first bytes in the browser: DICOM → STOW-RS or single file as today;
  archive or NIfTI → `POST /api/archives` (upload progress from
  `XMLHttpRequest.upload`), then polls `GET /api/archives/{job}` every
  500 ms while running.
- An archive shows as one row with a nested progress line
  (bytes uploaded → entries processed), expandable to its entry rows, and a
  patient/study summary when done.
- Metrics panel: facts grid gains "NIfTI volumes"; storage bar's objects
  segment includes NIfTI bytes (counts from the index).

### RAR licensing (builder's call, user to confirm on review)

RAR reading needs the UnRAR library, whose licence is freeware but not
OSI-approved. RAR support is a cargo feature `rar`, **on by default** in the
server binary so the user gets RAR out of the box; packagers who need a
purely OSI build turn it off. Documented in README and LICENSE notes. With
the feature off, RAR entries fail with "RAR support not built in".

## Testing

- Fixtures are generated in tests from the phantom generator (never real
  patient data): two phantom patients, archived as ZIP, 7z, tar.gz, a ZIP of
  per-patient ZIPs, a tar.gz inside a 7z, junk files, `__MACOSX/`, a DICOMDIR,
  an encrypted ZIP, a truncated archive, a zip bomb (highly compressible
  entry over the limit), nesting deeper than `max_depth`, and a tiny
  synthetic NIfTI-1 (`.nii` and `.nii.gz`) with one patient's DICOM.
- RAR: Rust has no RAR writer. Use a small committed fixture made from
  phantom data (a few KB), or write the minimal RAR 5 "stored" archive bytes
  in test code; builder decides.
- Assertions: counts, linking (linked vs unassigned), no files outside
  `objects/` and `tmp/`, `tmp/` empty after each job, recovery keeps NIfTI
  blobs, limits abort with the right reason, memory stays bounded (no
  whole-file reads).
- Console: manual Playwright pass uploading a nested multi-patient ZIP.

## Calls made without the user

Server-side extraction for all entry points; free-space guard and how
expanded bytes are counted; restart marks running uploads aborted; duplicate
DICOM counts for linking; one orphan rule for both blob kinds; content-based detection;
archives not kept; async job API (202 + polling) instead of a long request;
the limits above; RAR as a default-on cargo feature; linking rule details;
DICOM without preamble and `.hdr/.img` pairs skipped.

## Left to the builder

Module layout, crate choice per format within the constraints, channel
sizes, exact progress-line wording, and the console's expand/collapse
design within the existing visual language.
