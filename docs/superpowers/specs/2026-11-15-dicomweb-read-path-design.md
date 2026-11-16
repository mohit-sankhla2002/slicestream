# DICOMweb read path design (M1)

Date: 2026-11-15 · Status: proposed · Builds on: [storage ingest](2026-11-12-storage-ingest-design.md), [archive ingest](2026-11-13-archive-ingest-design.md)

## Intent

Design principle 1 in [ARCHITECTURE.md](../../ARCHITECTURE.md) says every
feature must be reachable through standard DICOMweb and DIMSE, so any viewer
or backend works with no custom code. Today slicestream accepts DICOM over
STOW-RS, C-STORE, single-file upload and archives, but data only comes back
out through the custom JSON API and the WebSocket stream. A team with OHIF or
Cornerstone3D has to write code to use it.

M1 makes slicestream a standard DICOMweb origin server for reading. Point
OHIF's stock DICOMweb data source at `/dicom-web` and it lists studies,
searches, opens a study and scrolls, with no slicestream-specific settings.
The same holds for any DICOMweb client in any language (dicomweb-client for
Python or JavaScript, Weasis, scripts using plain HTTP).

M1 also fixes the main known limit of the scaffold: every frame request
parses the whole file. At ingest, slicestream records where each frame and
each large value sits inside the stored file. Frames and bulk data are then
served with positioned reads (`pread`). The WebSocket stream and
`GET /api/series/{uid}/frames/{index}` use the same index, so they get faster
too.

The user handed scope and approach to the designer. Every decision below is
the designer's call unless it is quoted from ARCHITECTURE.md.

## Goals

1. **QIDO-RS** at study, series and instance level, on all six resource paths.
2. **WADO-RS**: metadata (study, series, instance), frames, bulk data, and
   whole instances (study, series, instance) as `multipart/related;
   type="application/dicom"`.
3. **OHIF acceptance.** OHIF has no single stock configuration: every
   DICOMweb server publishes its own data source entry (OHIF ships ones for
   Orthanc and dcm4chee). slicestream publishes one in the README. It uses
   OHIF's built-in `dicomweb` data source and only standard OHIF options:

   ```js
   {
     namespace: '@ohif/extension-default.dataSourcesModule.dicomweb',
     sourceName: 'slicestream',
     configuration: {
       friendlyName: 'slicestream',
       name: 'slicestream',
       qidoRoot: 'http://localhost:8080/dicom-web',
       wadoRoot: 'http://localhost:8080/dicom-web',
       wadoUriRoot: 'http://localhost:8080/dicom-web',
       imageRendering: 'wadors',
       thumbnailRendering: 'wadors',
       enableStudyLazyLoad: true,
       supportsFuzzyMatching: false,
       supportsWildcard: true,
       qidoSupportsIncludeField: true,
       staticWado: false,
       bulkDataURI: { enabled: true },
       omitQuotationForMultipartRequest: true,
     },
   }
   ```

   With that entry, in OHIF v3.13 (the plan pins the newest 3.13.x image):
   the study list loads, the patient name, MRN, accession, date range,
   modality and description filters work, a study opens, thumbnails show,
   every series scrolls end to end, and the browser console shows no
   errors. The same holds with `omitQuotationForMultipartRequest: false`
   and with `singlepart: 'bulkdata,video'` added, because other deployments
   copy those settings from OHIF's own `default.js`.
4. **Correct for what it implements.** Behaviour follows PS3.18. Every
   deviation is listed in [CONFORMANCE.md](../../CONFORMANCE.md).
5. **Byte-offset index at ingest.** Frames and bulk values are served with
   `pread`. Indexed frames are never served by parsing the whole file.
6. **In-place upgrade.** Stores created before M1 keep working and are
   indexed in the background. No re-import.
7. **Speed.** The targets in [Performance](#performance) are met and reported.

## Non-goals

- `rendered` and `thumbnail` resources. They need image encoding, which
  belongs to M4. With `thumbnailRendering: 'wadors'`, OHIF draws thumbnails
  from frames.
- WADO-URI. OHIF uses it only when `imageRendering` or `thumbnailRendering`
  is unset or set to `wadouri`.
- Transcoding between codecs or between compressed and uncompressed data
  (M4). Only the byte-order conversion described under Frames is done.
- XML responses (`application/dicom+xml`) and `application/zip` downloads.
- HTTP `Range` requests and progressive delivery (M4).
- Read authentication and authorization (M6, OIDC/JWT).
- Delete and reject, UPS-RS, the capabilities resource, and C-FIND, C-MOVE
  and C-GET (M3).
- A frame cache (M6).
- Serving NIfTI volumes.
- CORS on ingest routes. Uploading from OHIF on another origin stays
  unsupported.
- Object storage and PostgreSQL.

## Anti-goals

- OHIF needs a plugin, a patch, custom code, or any option outside its
  documented `dicomweb` data source settings to work.
- One request loads a whole study, series or large instance into memory.
- An indexed frame is served by parsing the whole file.
- An upgrade needs a re-import, or the server refuses to start on an older
  store.
- Patient data becomes readable from another origin without the operator
  opting in.
- Existing endpoints, the WebSocket protocol or the console change
  behaviour, other than getting faster.
- Ingest memory becomes unbounded because of metadata.
- Instances that are accepted today get rejected after M1.

## Constraints

- Rust MSRV 1.88, edition 2024. Every new dependency builds on 1.88.
- The server crate keeps `#![forbid(unsafe_code)]`. Positioned reads use
  `std::os::unix::fs::FileExt::read_at`, not mmap.
- Unix only, as today.
- Same security rules as today: the `Host` allow-list on every route, and
  CORS only for the origins in `[http] cors_allowed_origins`.
- Stored blobs stay byte-for-byte as received. The storage layout and its
  crash-safety guarantees stay the same.
- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D
  warnings`, `cargo test --workspace`, `node --test 'web/tests/*.test.js'`
  and `cargo +1.88 check --workspace --all-targets` pass after every task.
- Documentation uses the existing style: plain English, short sentences.

## Approaches considered

**A. Index everything at ingest, store it in SQLite (chosen).** One scan of
each instance at ingest produces its full DICOM JSON metadata, its frame
table, its bulk-value table and the extra QIDO attributes. They are written
in the same transaction as the instance row. Reads become a database lookup
plus `pread`.
Trade-offs: ingest does slightly more work (it already reads the header; the
scan reads to the end of the data set, skipping bulk values). The SQLite
file grows by roughly 5 to 50 KB per instance, and more for enhanced
multi-frame objects. Old stores need a backfill.

**B. Parse on request, cache in memory.** No migration and no backfill, but
the first time OHIF opens a 2,000-slice series the server parses 2,000 files
before answering. Cache size, eviction and restarts make latency hard to
predict. Frame offsets would need the same cache. This defeats the purpose
of M1, so it is rejected.

**C. Sidecar files next to blobs.** Store metadata and offset tables as
derived files in `objects/`. This keeps SQLite small and would move to
object storage with the blobs. But the sidecars are not covered by the index
transaction, so recovery and orphan rules get more complex. Rejected for M1.
If the SQLite file becomes a problem at scale, metadata can move to the blob
store later without changing the HTTP surface.

## Design

### Where the code goes

- **`slicestream-store`**: the ingest-time scan (it extends the bounded
  header walker in `header.rs`), migration `0003`, the new index columns and
  the `instance_data` table, QIDO queries, lookups, and the backfill.
- **`slicestream-dicomweb`** (new crate, named in the ARCHITECTURE.md crate
  table): parts that need no HTTP framework and no database. These are QIDO
  query parsing and validation into a typed query, `Accept` negotiation,
  a streaming `multipart/related` writer, and URL building. Unit tested in
  isolation.
- **`slicestream-server`**: axum handlers under `/dicom-web`, config, and
  the catalog change.
- **`slicestream-core`**: `SliceRef` gains an optional frame location.
  `Series::load_frame` uses `pread` when the location is set and falls back
  to today's whole-file parse when it is not.

STOW-RS stays where it is.

### Ingest-time scan

One pass over the spooled file, after the file meta group, to the end of the
data set. Memory stays bounded, like `read_header` today.

**Metadata.** Every data set element goes into DICOM JSON (PS3.18 Annex F),
including sequences up to the existing depth limit of 32. Group `0002` is
left out, as PS3.18 requires. Bulk values are not read into memory. Instead
their offset and length in the file are recorded, keyed by attribute path,
and the JSON gets a `BulkDataURI` for them. A value is bulk when one of
these is true:

- it is Pixel Data (7FE0,0010), Float Pixel Data (7FE0,0008) or Double Float
  Pixel Data (7FE0,0009);
- its VR is OB, OD, OF, OL, OV, OW or UN and it is longer than 1,024 bytes;
- it is any other value longer than 64 KiB (`MAX_VALUE_BYTES`).

Smaller binary values go inline as `InlineBinary` (base64).

The stored JSON holds relative bulk paths. They are made absolute when served
(see [Base URL](#base-url)).

**Pixel data, native.** The scan records the value's offset and length.
Frame *k* starts at `offset + k × frame_len`, where `frame_len` comes from
Rows, Columns, Samples per Pixel and Bits Allocated. With Bits Allocated = 1,
frames are bit-packed and may not start on a byte boundary. Frame *k* is
bits `[k·n, (k+1)·n)` with `n = Rows × Columns × Samples per Pixel`. The
server reads the bytes that cover those bits and repacks them to start at
bit 0, padded to whole bytes. (Today's `native_frame_len` is wrong for
Bits Allocated = 1; this fixes it.)

**Pixel data, encapsulated.** The scan records each fragment's offset and
length (after its 8-byte item header). Fragments are grouped into frames by
the first rule that applies:

1. Extended Offset Table (7FE0,0001) with Extended Offset Table Lengths
   (7FE0,0002), if present. These values are read even though they are
   binary; their size is 8 bytes per frame.
2. A non-empty Basic Offset Table with one entry per frame.
3. One frame: all fragments together.
4. As many fragments as frames: one fragment per frame.
5. Codestream start markers. A fragment that starts with JPEG SOI (`FF D8`)
   or JPEG 2000 SOC + SIZ (`FF 4F FF 51`) starts a new frame. This rule is
   used only if it yields exactly Number of Frames frames.
6. Otherwise there is no frame table. The instance is stored and fully
   served (metadata, whole instance, bulk data), but its frames resource
   answers 500 with the reason "frame boundaries unknown". A warning is
   logged at ingest.

**Deflated transfer syntax** (`1.2.840.10008.1.2.1.99`).

> **Changed during implementation:** deflated support was turned on and then
> reverted after the final review, because unbounded decompression on open
> read routes was the bigger risk. Deflated data sets are rejected at ingest
> exactly as before M1, and `offsets_valid` is true for every stored
> instance. The rest of this paragraph describes the design that was not
> shipped.

File offsets do not
map to data set bytes. The scan still produces metadata and QIDO attributes
through the inflating reader, but it records no frame or bulk offsets. Frames
and bulk values for these instances use the whole-file parse path.

**Big-endian** (`1.2.840.10008.1.2.2`). Offsets are recorded as usual. Byte
order is handled when serving (see Frames).

**Acceptance rules do not change.** An instance that today's `read_header`
accepts is still stored. Problems the deeper scan finds (truncated pixel
data, a malformed element after the header attributes) are recorded on the
instance, not rejected. The affected parts are left out and a warning is
logged. Frames that fall outside the stored bytes are never served.

**Metadata size cap.** The JSON for one instance is capped at 64 MiB. Real
enhanced multi-frame objects stay well below that. Above the cap the
instance is still stored, its metadata resource answers 500 with the reason,
and a warning is logged.

### Index changes (migration 0003)

New columns, filled by the scan:

| Table | New columns |
|---|---|
| `studies` | `study_time`, `study_id`, `referring_physician_name`, `patient_name`, `patient_name_folded`, `patient_birth_date`, `patient_sex` |
| `series` | `performed_procedure_step_start_date`, `performed_procedure_step_start_time` |
| `instances` | `scan_version INTEGER NOT NULL DEFAULT 0`, `scan_problem TEXT` |

New table `instance_data`, one row per instance, keyed by the instance id:
the metadata JSON, the frame table, the bulk-value table, and whether file
offsets are valid (false for deflated; see the note above, deflated data
sets are not stored). It is written in the same
transaction as the instance row, and replaced when `on_duplicate =
"overwrite"` replaces the instance.

**Patient attributes live on the study row.** The `patients` table is keyed
by Patient ID, and every instance without a Patient ID shares one row with
an empty key (`upsert_instance` binds `""`). Patient columns there would make
every such study show the first one's name, sex and birth date, and match or
miss name searches together. So QIDO's patient-level attributes (Patient's
Name, Birth Date, Sex) are stored on each study, taken from that study's
first instance, and QIDO matches and returns them from there. Patient ID
still comes from the `patients` link; an empty Patient ID is returned as an
empty value. The `patients` table does not change, and the console keeps
using it.

`patient_name_folded` is the patient name lower-cased in Rust with full
Unicode case folding, so name matching ignores case for non-ASCII names too.

`scan_version` is 0 for rows from older stores. The current scan version is
a constant in the store crate. Raising it later re-scans every instance in
the background.

The hierarchy rules in CONFORMANCE.md stay as they are: QIDO reports the
patient, study and series links the index has.

### QIDO-RS

Resources:

```
GET /dicom-web/studies
GET /dicom-web/series
GET /dicom-web/instances
GET /dicom-web/studies/{study}/series
GET /dicom-web/studies/{study}/instances
GET /dicom-web/studies/{study}/series/{series}/instances
```

Query parameters:

- **Matching keys**, by keyword (`PatientName=`) or by tag (`00100010=`).
  Supported keys are the ones PS3.18 requires for each level, plus Study
  Description and Series Description. Keys of a higher level work at lower
  levels (for example `/dicom-web/series?PatientID=X`).
  - Study: Study Date, Study Time, Accession Number, Modalities in Study,
    Referring Physician's Name, Patient's Name, Patient ID, Study Instance
    UID, Study ID, Study Description.
  - Series: Modality, Series Instance UID, Series Number, Performed
    Procedure Step Start Date and Time, Series Description.
  - Instance: SOP Class UID, SOP Instance UID, Instance Number.
  - Request Attribute Sequence keys (required at series level) are accepted
    and match only when the series has no such data. Recorded as a
    deviation.
- **Match types**: single value; list of UIDs (comma-separated); universal
  (empty value); wildcard `*` and `?` on string VRs other than UI, DA, TM
  and DT; range on DA, TM and DT, with open ends. Patient's Name and
  Referring Physician's Name match without regard to case. Every other
  string matches exactly.
- **`includefield`**: repeatable, comma-separated, by keyword or tag, or
  `all`.
- **`limit`** and **`offset`**.
- **`fuzzymatching`**: `true` is answered with literal matching and the
  PS3.18 warning `Warning: 299 {service}: "The fuzzymatching parameter is
  not supported. Only literal matching has been performed."`.

An unknown parameter, a matching key slicestream does not support, or a
malformed value gives 400 with a plain-text reason that names the
parameter. (The builder checks this against the current PS3.18 text; if the
standard requires ignoring such keys with a warning instead, follow the
standard and record the clause in CONFORMANCE.md.)

Results:

- `200` with an `application/dicom+json` array, also when it is empty. (The
  builder confirms the empty-result status against the current PS3.18.)
- Default attributes per level are the ones PS3.18 lists. Computed ones are
  Number of Study Related Series and Instances, Number of Series Related
  Instances, Modalities in Study, Retrieve URL (0008,1190), Instance
  Availability (`ONLINE`) and Specific Character Set (`ISO_IR 192`).
- Attributes that are not index columns (from `includefield`) come from the
  stored metadata of a representative instance: the instance with the
  lowest id in that study or series. `includefield=all` at study and series
  level adds the representative instance's attributes from the Patient,
  General Study and Patient Study modules (study level) or the General
  Series module (series level). At instance level it adds all of the
  instance's attributes except bulk values.
- Order is by internal id, ascending, so paging with `offset` is stable.
- `limit` defaults to, and is capped at, `[dicomweb] max_results`. If more
  matches exist beyond the returned page, the response carries the PS3.18
  warning that more results can be requested.
- `Accept`: `application/dicom+json`, `application/json`, `*/*` or none
  gives JSON. A request that only accepts XML gets 406.

### WADO-RS

**Instances.** Each of these returns `multipart/related;
type="application/dicom"`, one part per instance, streamed from the stored
files one at a time:

```
GET /dicom-web/studies/{study}
GET /dicom-web/studies/{study}/series/{series}
GET /dicom-web/studies/{study}/series/{series}/instances/{instance}
```

A bare `application/dicom` (not wrapped in `multipart/related`) gives a
single-part response when the response holds exactly one instance, and 406
otherwise. OHIF loads multi-frame segmentations this way.

Transfer syntax: with `transfer-syntax=*`, or with no transfer syntax
given, instances are sent as stored. Each part's `Content-Type` names the
stored transfer syntax. Sending stored data when none was asked for is a
deviation (PS3.18's default is Explicit VR Little Endian), shared with
Orthanc, and is recorded in CONFORMANCE.md. A specific transfer syntax that
differs from what is stored for any instance in the response gives 406.

**Metadata.** Each returns an `application/dicom+json` array, written one
instance at a time:

```
GET /dicom-web/studies/{study}/metadata
GET /dicom-web/studies/{study}/series/{series}/metadata
GET /dicom-web/studies/{study}/series/{series}/instances/{instance}/metadata
```

An instance from an older store that has not been scanned yet is scanned on
this request and the result is stored.

Serving metadata must not parse the stored JSON. It is stored so that the
response can be produced by copying bytes and inserting the absolute URL
prefix at known places (for example, positions recorded at ingest, or a
fixed key sequence that the serializer guarantees cannot occur inside a
value). The builder picks the mechanism.

**Frames.**

```
GET /dicom-web/studies/{study}/series/{series}/instances/{instance}/frames/{list}
```

`{list}` holds 1-based frame numbers, comma-separated, in any order. The
response is `multipart/related`, one part per requested frame, in request
order. A frame number of 0 or above Number of Frames gives 404. The
supported media types are:

| Requested | Served |
|---|---|
| `application/octet-stream` (no transfer syntax) | Uncompressed little-endian pixels. Native little-endian data as stored. Big-endian data byte-swapped (Bits Allocated 16 or 32). Encapsulated data gives 406. |
| `application/octet-stream; transfer-syntax=*` | As stored. Each part's `Content-Type` carries `transfer-syntax=<stored UID>`. |
| `application/octet-stream; transfer-syntax=<UID>` | As stored if the UID matches, else 406. |
| `image/jpeg`, `image/jls`, `image/jp2`, `image/jphc`, `image/dicom-rle` (with or without a transfer syntax) | As stored if the stored transfer syntax belongs to that media type (and matches the requested one, if given), else 406. |

`Accept` parsing handles `type` with and without quotes (OHIF's
`omitQuotationForMultipartRequest` can be either), several comma-separated
choices, q-values and `*/*`. A bare media type (not wrapped in
`multipart/related`) gives a single-part response when exactly one frame is
requested, and 406 otherwise.

**Bulk data.** A `BulkDataURI` has the form
`{base}/dicom-web/studies/{study}/series/{series}/instances/{instance}/bulk/{path}`.
`{path}` is the tag for a top-level element (`7FE00010`), or the tag path
through sequence items for a nested one (`00540016/0/00181074`). The
response is `multipart/related; type="application/octet-stream"` with one
part holding the value, or a single part when the request asks for a bare
media type. Pixel Data's bulk resource returns every frame, one part per
frame, under the same rules as the frames resource with all frames
requested.

**Errors.** An unknown study, series or instance gives 404. So does a path
whose UIDs exist but do not belong together (an instance under the wrong
series). An `Accept` that cannot be met gives 406.

**Streaming.** Large responses are written part by part, reading files in
chunks of at most 1 MiB. Memory per request does not grow with the size of
the response.

**Compression.** `application/dicom+json` responses (QIDO and metadata) are
compressed with gzip or Brotli at the fastest level when the client sends
`Accept-Encoding`. A higher level costs more time than it saves on a fast
network. Pixel data and instances are not compressed.

### Frames for the stream and `/api/series`

`SliceRef` carries the frame location from `instance_data`.
`Series::load_frame` reads frames with `pread`. The catalog loads the frame
tables along with the slice rows. The bytes returned are the same as today
(tests compare the old and new paths on the phantom and on synthetic
encapsulated data). Instances without a frame table use today's whole-file
parse.

### Base URL

Retrieve URLs and `BulkDataURI`s use `[http] public_url` when it is set (for
example `https://pacs.example.org` or `https://example.org/pacs` behind a
reverse proxy). Otherwise they use `http://<Host header>`, as STOW-RS does
today. STOW-RS switches to the same helper, so its Retrieve URLs follow
`public_url` too.

### CORS and patient data

The new GET routes join the existing read router, so they send CORS headers
only for the origins in `[http] cors_allowed_origins`, which is empty by
default. The CORS layer adds `Accept` to the allowed request headers (an
`Accept` value with quoted parameters is not CORS-safelisted, so browsers
send a preflight) and exposes `Warning`.

M1 has no read authentication; that is M6. Anyone who can reach the HTTP
port can read patient data through these routes. The default bind address
stays `127.0.0.1`. When `[http] addr` is not a loopback address, `serve`
logs a warning at startup that the DICOMweb read routes are unauthenticated.
README and SECURITY.md say this plainly. `[dicomweb] enabled = false`
unmounts the new routes (they then return 404); STOW-RS is not affected.

### Upgrading existing stores

- Migration `0003` adds the columns and the table. Existing instances get
  `scan_version = 0`.
- After recovery, `serve` starts a background task that scans instances with
  `scan_version` below the current version, in id order and in batches, on
  at most two blocking threads. It logs progress and a final count. Shutdown
  stops it, and the next start continues where it stopped.
- Until an instance is scanned: QIDO matches it on the columns that exist
  today (new columns are empty, so keys on them do not match it), metadata
  is scanned on request, and frames use the whole-file parse.
- `import` scans the instances it stores. It does not run the backfill.

### Configuration

```toml
[http]
# public_url = "https://pacs.example.org"  # base of Retrieve URLs and BulkDataURIs; default http://<Host>

[dicomweb]
enabled = true        # false: the /dicom-web GET routes are not mounted (STOW-RS stays)
max_results = 1000    # QIDO limit: default and maximum
```

### Documentation

- CONFORMANCE.md: a "DICOMweb read" section covering resources, media
  types, matching keys and match types, status codes and every deviation.
  Remove the note that Retrieve URLs return 404.
- README: API table rows, and a "Use with OHIF" section with a working data
  source config.
- ARCHITECTURE.md: M1 done; known limits updated; the crate table updated
  (`slicestream-dicomweb` exists, `slicestream-dimse` exists with C-ECHO and
  C-STORE).
- SECURITY.md: the read routes are unauthenticated until M6.

## Testing

- **Unit** (dicomweb crate and store scan): query parsing and every match
  type; `Accept` negotiation for every row of the frames table, quoted and
  unquoted; exact bytes from the multipart writer; each of the six frame
  grouping rules; bit-packed frame extraction; byte swapping; the bulk rule;
  DICOM JSON for each VR (PN, AT, IS and DS as numbers, empty values,
  InlineBinary, nested sequences).
- **Store integration**: scanning the phantom, synthetic encapsulated
  multi-frame data (Basic Offset Table, Extended Offset Table, one fragment
  per frame, markers, ambiguous), big-endian, deflated (rejected at ingest), Bits Allocated = 1,
  and nested sequences. A store created with migrations 0001 and 0002 only
  is opened and backfilled. Encapsulated fixtures can hold arbitrary bytes
  under a JPEG transfer syntax UID, because offsets do not depend on the
  codec.
- **Server integration**: every resource and every status code above; frame
  bytes equal to the old path; the WebSocket stream unchanged; CORS headers
  and preflight; `public_url`; `[dicomweb] enabled = false`; a large
  instance retrieved without the response being buffered.
- **Interop**: `scripts/interop-dicomweb.sh` runs a pinned Python
  `dicomweb-client` in a virtual environment against the server. It searches
  at each level and retrieves metadata, frames, bulk data and instances. It
  runs in the CI `interop` job next to the DCMTK script.
- **OHIF acceptance** (local, not in CI): a pinned OHIF v3.13.x Docker image
  (`ohif/app`) on a different port, so requests are cross-origin, with the
  README's data source entry. Playwright checks Goal 3 with that entry, with
  `omitQuotationForMultipartRequest: false`, and with
  `singlepart: 'bulkdata,video'`. It uses phantom data plus a synthetic
  encapsulated multi-frame instance, and saves a screenshot.

## Performance

Measured on a development machine and reported in the final task. These are
not CI gates.

- Series metadata for a 1,000-instance CT series, measured to the last byte
  over loopback, warm: under 150 ms without compression, and under 250 ms
  with gzip (what OHIF gets).
- One 512×512 native frame through WADO-RS: under 2 ms server time at the
  median, warm.
- Peak memory while streaming a 1 GB study retrieve stays within a few MiB
  of idle.

## Left to the builder

- How DICOM JSON is written (the `dicom-json` crate or a serializer in the
  store crate), as long as the output is PS3.18 Annex F.
- How frame and bulk tables are encoded in SQLite.
- SQL, module layout inside each crate, and helper types.
- Batch size and pacing of the backfill.
- The exact PS3.18 wording and status codes for the points marked "the
  builder checks" above. Cite the clause in CONFORMANCE.md.
