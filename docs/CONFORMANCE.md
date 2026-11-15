# DICOM conformance statement (storage ingest and DICOMweb read)

This statement covers the storage ingest features of slicestream (a DIMSE
Storage SCP and an HTTP STOW-RS endpoint) and its DICOMweb read routes. It is
not a full DICOM Conformance Statement per PS3.2. It lists what the server
accepts and returns.

## Implementation identity

| Item | Value |
|---|---|
| Implementation Class UID | `2.25.74886492046038186129868855052748391807` |
| Implementation Version Name | `SLICESTREAM_010` |
| Default AE title | `SLICESTREAM` |
| Default DIMSE port | 11112 |

## DIMSE roles

slicestream acts only as an SCP. It never initiates associations.

| SOP class | Role |
|---|---|
| Verification (1.2.840.10008.1.1) | SCP |
| Any Storage SOP class | SCP |

Negotiation is promiscuous. The SCP accepts any proposed abstract syntax at
association time. A C-STORE whose SOP class is not a storage class gets status
0x0122. Other DIMSE commands get status 0x0211 (unrecognized operation).

If `require_called_ae_title` is true (the default), the called AE title must
match the configured one, or the association is rejected. If `allowed_peers` is
not empty, only the listed calling AE titles are accepted.

AE titles are not authentication: any host that can reach the port can claim
any calling AE title. Set `allowed_peers` and also restrict the DIMSE port to
known modality and PACS addresses with a firewall.

## Association limits and timeouts

At most `max_associations` associations (default 64) run at once. A TCP
connection beyond the limit is closed at once, without an A-ASSOCIATE-RJ.
A connection must complete association negotiation within
`handshake_timeout_secs` (default 30 s), or it is closed and its slot freed.
An established association that receives no DIMSE command for
`idle_timeout_secs` (default 300 s) while no operation is in progress is
aborted (A-ABORT). While a data set is being received, each PDU must arrive
within `read_timeout_secs`.

## Accepted transfer syntaxes

The SCP accepts every transfer syntax in `dicom-transfer-syntax-registry`
0.10.1, including encapsulated (compressed) ones. Pixel data is never decoded
or transcoded on ingest. Instances are stored exactly as received, wrapped in
a Part-10 file.

## Status codes

| Condition | C-STORE status | STOW-RS FailureReason (0008,1197) |
|---|---|---|
| stored / duplicate | 0x0000 | — (in ReferencedSOPSequence) |
| cannot parse dataset | 0xC000 | 0xC000 |
| missing SOP/Study/Series UID | 0xC000 | 0xC000 |
| command vs dataset UID mismatch | 0xA900 | n/a |
| URL Study UID ≠ dataset Study UID | n/a | 0xC409 (dcm4chee convention) |
| SOP class not a storage class | 0x0122 | 0x0122 |
| exceeds `max_instance_bytes` | 0xA700 | 0xA700 |
| disk/index I/O failure | 0xA700 | 0xA700 |
| unknown DIMSE command | 0x0211 | n/a |

## HTTP surface

| Method and path | Purpose |
|---|---|
| `POST /dicom-web/studies` | STOW-RS |
| `POST /dicom-web/studies/{study}` | STOW-RS, restricted to one study |
| `POST /api/instances` | One Part-10 file, `Content-Type: application/dicom`. Returns JSON with `status` (`stored`, `duplicate`, ...). |
| `POST /api/archives` | An archive, NIfTI or DICOM file as the raw body. Returns 202 with a job id. See [Archive ingest](#archive-ingest). |
| `GET /api/archives/{job}` | Progress and results of an archive job. |

STOW-RS requests use `Content-Type: multipart/related; type="application/dicom"`.
Responses are `application/dicom+json` and hold (0008,1190) RetrieveURL,
(0008,1198) FailedSOPSequence and (0008,1199) ReferencedSOPSequence. A
FailedSOPSequence item carries (0008,1150) / (0008,1155) whenever the part's
header could be parsed.

The RetrieveURLs follow the WADO-RS layout
(`/dicom-web/studies/{study}/series/{series}/instances/{instance}`) and can be
fetched with [WADO-RS](#wado-rs).

| HTTP status | Meaning |
|---|---|
| 200 | All parts stored |
| 202 | Some parts stored, some failed |
| 409 | No part stored |
| 415 | Content-Type is not `multipart/related; type="application/dicom"` |
| 413 | Request exceeds `max_request_bytes` |
| 400 | Malformed multipart body |
| 401 | A `stow_token` is configured and the bearer token is missing or wrong |
| 421 | The `Host` header is not in `allowed_hosts` (any route) |

RetrieveURLs start with `[http] public_url` when it is set (for example
`https://example.org/pacs` behind a reverse proxy). Otherwise they start with
`http://<Host header>`.

Ingest routes never send CORS headers. Read routes send them only for the
origins in `[http] cors_allowed_origins` (default: none). Every route checks
the `Host` header against `[http] allowed_hosts` (default: `localhost`,
`127.0.0.1`, `[::1]` and the host of `[http] addr`).

## DICOMweb read

The DICOMweb read routes answer `GET` (and `HEAD`). `[dicomweb] enabled =
false` unmounts them: they then return 404. STOW-RS is not affected.

They have no authentication. Anyone who can reach the HTTP port can read
patient data through them. The default `[http] addr` is `127.0.0.1`; on any
other address the server logs a warning at startup.

They are read routes, so they send CORS headers only for the origins in
`[http] cors_allowed_origins`. A preflight may ask for the `Accept` request
header. Responses expose `Warning` to scripts.

### QIDO-RS

| Method and path | Returns |
|---|---|
| `GET /dicom-web/studies` | Studies |
| `GET /dicom-web/series` | Series of all studies |
| `GET /dicom-web/instances` | Instances of all studies |
| `GET /dicom-web/studies/{study}/series` | Series of one study |
| `GET /dicom-web/studies/{study}/instances` | Instances of one study |
| `GET /dicom-web/studies/{study}/series/{series}/instances` | Instances of one series |

**Media type.** Results are `application/dicom+json`. An `Accept` of
`application/dicom+json`, `application/json`, `*/*`, or no `Accept` at all
gives JSON. A request that accepts only other types (for example
`application/dicom+xml`) gets 406. Results are gzip or Brotli compressed when
`Accept-Encoding` allows it.

**Matching keys.** By keyword (`PatientName=`) or by tag (`00100010=`). Keys
of a higher level also work at lower levels (`/dicom-web/series?PatientID=X`).

| Level | Keys |
|---|---|
| Study | Study Date, Study Time, Accession Number, Modalities in Study, Referring Physician's Name, Patient's Name, Patient ID, Study Instance UID, Study ID, Study Description |
| Series | Modality, Series Instance UID, Series Number, Performed Procedure Step Start Date, Performed Procedure Step Start Time, Series Description, Request Attributes Sequence |
| Instance | SOP Class UID, SOP Instance UID, Instance Number |

**Match types.**

- Single value. Patient's Name and Referring Physician's Name ignore case
  (also for non-ASCII letters). Every other value matches exactly. A single
  time (`12`) matches the whole period it names.
- List of UIDs, comma-separated, on the UID keys. At most 1000 values; a
  longer list gets 400.
- Universal (empty value): matches everything.
- Wildcard `*` and `?` on text keys (not on UIDs, dates or times). Only the
  two name keys ignore case.
- Range on dates and times (`20260101-20260131`), with either end open.

**Other parameters.**

- `includefield`: repeatable and comma-separated, by keyword or tag, or `all`.
- `limit`: default and maximum are `[dicomweb] max_results` (1000).
- `offset`: matches to skip. Results are in storage order, so paging is
  stable.
- `fuzzymatching`: `true` is answered with literal matching and a warning.

**Returned attributes.** Each result has the attributes of PS3.18 Tables
10.6.3-3 (study), 10.6.3-4 (series) or 10.6.3-5 (instance) that the
instance has, plus the attributes used as matching keys. Series of all
studies also carry the study attributes; instances carry the series
attributes unless the path names the series, and the study attributes unless
it names the study (PS3.18 10.6.3.3). Series and instance results always
carry Study Instance UID, and instance results Series Instance UID.

The server computes Number of Study Related Series and Instances, Number of
Series Related Instances, Modalities in Study, Retrieve URL (0008,1190),
Instance Availability (`ONLINE`) and Specific Character Set (`ISO_IR 192`).
Other attributes come from the index. An attribute the index holds but that
is empty is returned without a value.

`includefield` attributes the index does not hold come from the stored
metadata of a representative instance: the instance with the lowest internal
id in the study or series (at instance level, the instance itself).
`includefield=all` adds the Patient, General Study and Patient Study modules
at study level, the General Series module at series level, and every
attribute of the instance at instance level. An attribute that holds a bulk
value (also inside a sequence) is never returned: QIDO-RS gives no
BulkDataURIs.

**Warnings.** Each is one `Warning` header of the form
`299 <service>: "<text>"`, where `<service>` is the server's base URI (PS3.18
8.3.4.2):

| When | Text |
|---|---|
| `fuzzymatching=true` | `The fuzzymatching parameter is not supported. Only literal matching has been performed.` |
| More matches exist after this page | `There are additional results that can be requested` |
| Some query parameters were ignored | `The following query parameters were ignored: <names>` |

**Status codes.**

| HTTP status | Meaning |
|---|---|
| 200 | Search done. No match gives an empty array `[]` (PS3.18 8.3.4.4.1, Table 10.6.3-1). |
| 400 | Invalid value for a supported parameter (bad date, time or number, unknown `includefield` attribute, a repeated key, bad percent-encoding) or a UID list over 1000 values. The plain-text body names the parameter. |
| 404 | `[dicomweb] enabled = false` |
| 406 | The request accepts no JSON type |
| 421 | The `Host` header is not in `allowed_hosts` |

**Deviations and choices.**

- Unknown query parameters, attributes that are not matching keys, and keys
  of a lower level (Modality in a study search) are ignored, with a
  warning. PS3.18 8.3: "The origin server shall ignore any unsupported
  Query Parameters."
- No fuzzy matching; Patient's Name matching ignores case instead.
- Request Attributes Sequence keys: an empty value matches every series;
  any other value matches none. Request attributes are not indexed.
- `includefield` may name attributes of any level. At study level a series
  or instance attribute (for example Modality, which OHIF asks for) is taken
  from the representative instance. PS3.18 10.6.3.3.1 says such attributes
  are not returned at study level.
- Timezone Offset From UTC (0008,0201) and Request Attributes Sequence
  (0040,0275) are not indexed. They are returned only when `includefield`
  names them.
- The "more results" warning does not say how many: the server stops
  counting after the page. PS3.18 8.3.4.4.1 words it as `There are
  <remaining> additional results that can be requested`.
- Modalities in Study matches one modality per query. A study matches when
  any of its series has it. A wildcard on this key matches nothing.

### WADO-RS

| Method and path | Returns |
|---|---|
| `GET /dicom-web/studies/{study}` | Every instance of the study |
| `GET /dicom-web/studies/{study}/series/{series}` | Every instance of the series |
| `GET /dicom-web/studies/{study}/series/{series}/instances/{instance}` | One instance |
| `GET /dicom-web/studies/{study}/metadata` | DICOM JSON of every instance of the study |
| `GET /dicom-web/studies/{study}/series/{series}/metadata` | DICOM JSON of every instance of the series |
| `GET /dicom-web/studies/{study}/series/{series}/instances/{instance}/metadata` | DICOM JSON of one instance |
| `GET /dicom-web/studies/{study}/series/{series}/instances/{instance}/frames/{list}` | Frames of one instance |
| `GET /dicom-web/studies/{study}/series/{series}/instances/{instance}/bulk/{path}` | One bulk data value of one instance |

**Instances.** The response is `multipart/related; type="application/dicom"`,
one part per instance, each part the stored file byte for byte. A part's
`Content-Type` is `application/dicom; transfer-syntax=<stored syntax>`. An
`Accept` of exactly `application/dicom` (no `multipart/related`) gets the file
itself, and is only possible when the request names one instance; for more it
gets 406. Bodies are streamed in 1 MiB reads and are never compressed.

**Transfer syntax.** The server does not transcode: it sends every instance
in the transfer syntax it was stored with. This is the same as Orthanc. A
request for a specific `transfer-syntax` is answered only when every instance
in the response is stored in it; otherwise 406. `transfer-syntax=*` and no
`transfer-syntax` accept whatever is stored.

**Metadata.** The response is `application/dicom+json`: a JSON array with one
object per instance, in storage order. It is streamed, and gzip or Brotli
compressed when `Accept-Encoding` allows it. Group 0002 is not included.
Pixel Data, Float Pixel Data, Double Float Pixel Data, binary values of VR
OB, OD, OF, OL, OV, OW or UN longer than 1,024 bytes, and any other value
longer than 64 KiB carry a `BulkDataURI` instead of a value. Smaller binary
values are inline as base64 `InlineBinary`. The `BulkDataURI` is an absolute
URL that starts with `[http] public_url` when it is set, otherwise with
`http://<Host header>`. An instance whose metadata
would exceed 64 MiB has none, and gets 500. Its bulk values other than
Pixel Data are not recorded either, so their paths get 404.

Instances stored before metadata scanning existed are scanned on the first
request, and in the background after startup.

**Frames.** `{list}` holds 1-based frame numbers, comma-separated, for
example `3,1,3`. Each is digits only; at most 4,096 numbers. The response is
`multipart/related`, one part per listed frame, in the order listed: a number
listed twice gives two parts. The response's `type` parameter is the media
type of the parts.

| `Accept` asks for | Served |
|---|---|
| `application/octet-stream` (no transfer syntax) | Uncompressed little-endian pixels. Native little-endian data as stored. Big-endian data byte-swapped (Bits Allocated 16 or 32). Encapsulated data gives 406. Parts are `application/octet-stream; transfer-syntax=1.2.840.10008.1.2.1`. |
| `application/octet-stream; transfer-syntax=*` | As stored. Each part's `Content-Type` carries `transfer-syntax=<stored UID>`. |
| `application/octet-stream; transfer-syntax=<UID>` | As stored if the UID matches, else 406. |
| `image/jpeg`, `image/jls`, `image/jp2`, `image/jphc`, `image/jxl`, `image/dicom-rle` (with or without a transfer syntax) | As stored if the stored transfer syntax belongs to that media type (and matches the requested one, if given), else 406. |
| `*/*`, or no `Accept` | As stored: the image media type of the stored transfer syntax, or `application/octet-stream` for native data. |

`Accept` may list several choices, with q-values. The `type` parameter works
with and without quotes. A bare media type (not wrapped in
`multipart/related`) gets the frame itself as the body, and only when the
request names one frame; for more it gets 406.

Frames are read at the file offsets recorded when the instance was stored.
Two kinds of instance have none and are read by parsing the whole file,
which is slower: instances stored before scanning existed (until the
background scan reaches them) and deflated instances. A deflated instance's
frames come out inflated, so they are served as Explicit VR Little Endian
(`transfer-syntax=1.2.840.10008.1.2.1`), also for `transfer-syntax=*`.

**Bulk data.** `{path}` is the end of a `BulkDataURI` from the metadata: a
tag (`00091001`), or a path through sequence items (`00480105/0/00282000`,
items counted from 0). The response is `multipart/related;
type="application/octet-stream"` with one part holding the value. An
`Accept` of a bare `application/octet-stream` gets the value itself; `*/*`
or no `Accept` gets the multipart response. Values are sent as stored, read
in 1 MiB pieces: little-endian, or big-endian for an Explicit VR Big Endian
instance. The part's `Content-Type` names that byte order
(`transfer-syntax=1.2.840.10008.1.2.1` or `1.2.840.10008.1.2.2`).

Pixel Data (`7FE00010`) is served like the frames resource with every frame
listed: one part per frame, with the same media types and rules.

A path that is not a bulk value of the instance gets 404. For an instance
with no recorded offsets (deflated, or not scanned yet), the file is parsed
and the value of the element at that path is sent.

| HTTP status | Meaning |
|---|---|
| 200 | Instances, metadata, frames or bulk data found |
| 400 | A malformed frame list: not digits, an empty item, or more than 4,096 numbers |
| 404 | Unknown study, series or instance, or a series that is not in that study. Also: a frame number of 0 or above Number of Frames; frames of an object that is not an image; frames or Pixel Data of an image without Pixel Data (`no pixel data`); a bulk data path the instance does not have. |
| 406 | No acceptable media type or transfer syntax (also a bare media type for several instances or frames) |
| 500 | An instance has no metadata (for example its object is over 64 MiB). Frames that cannot be located: encapsulated pixel data with no usable offset table (`frame boundaries unknown`), with more than 1,048,576 fragments (`too many fragments (N)`, N the number of fragments), or pixel data shorter than its frames (`pixel data truncated`, `pixel data too short`). A stored file that cannot be read. The plain-text body says why. |
| 503 | Frames or bulk data of an instance that was overwritten twice while the request read it. Try again. |

A stored file that cannot be read after the response has started ends the
body early; the client sees a broken stream.

**Deviations.**

- Bulk data values other than Pixel Data are not converted: a big-endian
  instance's values are sent big-endian, even when no transfer syntax was
  asked for. PS3.18's default for bulk data is Explicit VR Little Endian.
- Frames of a deflated instance are labelled Explicit VR Little Endian, not
  the stored Deflated Explicit VR Little Endian: the data set is inflated to
  read them.

## Limits and defaults

| Setting | Default |
|---|---|
| `[storage] max_instance_bytes` | 2147483648 (2 GiB) |
| `[storage] on_duplicate` | `keep` (or `overwrite`) |
| `[http] max_request_bytes` | 17179869184 (16 GiB) |
| `[http] cors_allowed_origins` | empty (no CORS) |
| `[http] allowed_hosts` | empty (loopback names and the host of `addr`) |
| `[http] public_url` | unset (`http://<Host>`) |
| `[dicomweb] enabled` | `true` |
| `[dicomweb] max_results` | 1000 (QIDO-RS `limit` default and maximum; at least 1) |
| `[dimse] max_associations` | 64 |
| `[dimse] max_pdu_length` | 116794 |
| `[dimse] read_timeout_secs` / `write_timeout_secs` | 60 / 60 |
| `[dimse] handshake_timeout_secs` | 30 |
| `[dimse] idle_timeout_secs` | 300 |
| Shutdown grace for in-flight associations | 30 s |

Duplicates (same SOP Instance UID) return success. With `keep`, the first copy
stays. With `overwrite`, the index points to the new copy.

Byte-identical copies are always duplicates. The same instance received over
DIMSE and over STOW-RS is not byte-identical: the SCP wraps the DIMSE data set
in a Part-10 header whose Source Application Entity Title (0002,0016) is the
calling AE title (and whose transfer syntax is the negotiated one). With
`overwrite`, such a resend therefore replaces the stored copy.

## Archive ingest

`POST /api/archives`, the console and `import <PATH>` open archives and store
what is inside.

**Formats.** ZIP, 7z, RAR, tar, tar.gz / .tgz and plain gzip, nested in any
combination, plus NIfTI-1 and NIfTI-2 (`.nii`, `.nii.gz`). The type is found
from the first bytes of the content, never from the file name. RAR needs the
`rar` cargo feature (on by default). Encrypted archives (password-protected
ZIP, 7z or RAR) fail; the rest of the upload goes on.

**Paths.** Entries inside nested archives are reported as
`a.zip!/b.zip!/IM1`. Entry names are only reported, never used as file
paths. Symlinks, hard links, devices and directories are ignored.

**DICOM.** Every Part-10 file goes through the same ingest as STOW-RS
(validation, de-duplication, crash safety).

**NIfTI.** Stored unchanged as `objects/aa/bb/<sha256>.nii` or
`.nii.gz`, with a row in `nifti_volumes`. After the upload, each volume is
linked using scopes from the inside out: DICOM in the same folder of the same
archive, then anywhere in that archive, then the enclosing archive, up to the
whole upload. The first scope with DICOM decides. One patient there: the
volume is linked to that patient, and to the study if there is exactly one.
More than one patient, or no DICOM at all: it stays unassigned. Stored and
duplicate DICOM counts; failed and skipped files do not. A NIfTI uploaded
alone is never linked, even if its patient is already stored.

**Skipped.** Files that are not DICOM or NIfTI, `DICOMDIR`, `.hdr`/`.img`
pairs, and DICOM files without the `DICM` preamble.

**Limits** (`[archives]`):

| Setting | Default |
|---|---|
| `enabled` | `true` (`false`: both routes return 404) |
| `max_depth` (nesting below the uploaded file) | 4 |
| `max_entries` (files seen in one upload) | 200000 |
| `max_expanded_bytes` (bytes written out of archives) | 68719476736 (64 GiB) |
| `min_free_bytes` (free disk to keep) | 2147483648 (2 GiB) |
| `max_concurrent_jobs` (more wait; queue cap is 4 times this) | 2 |
| `job_retention` (finished jobs kept in memory, at least 1) | 50 |
| `body_idle_timeout_secs` (body silent this long: 408) | 60 |

Too deep: that nested archive fails and the rest goes on. Too many entries or
too many expanded bytes: the upload is aborted (what was stored stays
stored). Each entry is also bound by `[storage] max_instance_bytes`. The body
is bound by `[http] max_request_bytes`. `POST` returns 507 if free space is
below `Content-Length` plus `min_free_bytes`, or (also without
`Content-Length`) if free space falls below `min_free_bytes` while the body is
received (checked every 64 MiB); a running job aborts if free space drops
below `min_free_bytes`. A body that sends nothing for `body_idle_timeout_secs`
gets 408. Reported entry names are cut to 1024 bytes and paths to 4096 bytes
(ending in `…`). A job that is still running when the
server stops is marked `aborted`.

Responses: 202 job accepted, 400 not a supported file, 401 bad token, 408
body stalled, 413 too large, 503 job queue full, 507 not enough free space, 404 unknown job.

## Hierarchy conflicts

The index links each study to one patient and each series to one study, using
the first instance received. If a later instance places a known study under a
different PatientID, or a known series under a different study, the instance
is still stored and indexed (as part of the known series), the existing links
are kept, and a warning is logged.

## Storage ownership

One process owns a storage root. `serve` and `import` take an exclusive
advisory lock on `<root>/LOCK` and fail at startup if another process holds
it. `import` is for a stopped server; while the server runs, send instances
through STOW-RS, `POST /api/instances` or C-STORE.

## Interoperability testing

`scripts/interop-dcmtk.sh` runs DCMTK `echoscu` and `storescu` against the
server (default and Implicit VR Little Endian transfer syntaxes, duplicate
detection, and rejection of a wrong called AE title). CI runs it as the
`interop` job.

`scripts/interop-dicomweb.sh` runs the Python `dicomweb-client` 0.61.2 (with
`pydicom` 3.0.2) against the server. It stores the phantom and a synthetic
RLE Lossless multi-frame instance over STOW-RS. It searches for studies,
series and instances, also with a Patient's Name wildcard. It retrieves
series metadata, frames (native, with `transfer-syntax=*` and as
`image/dicom-rle`), Pixel Data through its `BulkDataURI`, one instance and a
whole series, and compares the bytes with the files. CI runs it in the
`interop` job too. dicomweb-client sends the `Host` header without the port,
so the script sets `[http] public_url`; otherwise its `BulkDataURI`s would
point at port 80.

OHIF v3.13 (the `ohif/app:v3.13.14` Docker image) was checked by hand with
Playwright, on another origin, with the data source entry from the README,
with `omitQuotationForMultipartRequest: false`, and with
`singlepart: 'bulkdata,video'`. In each case the study list loaded, the
patient name, MRN, accession, date range, modality and description filters
narrowed it, studies opened, thumbnails showed, every series scrolled end to
end, and there were no console errors and no failed requests. This is not
run in CI.
