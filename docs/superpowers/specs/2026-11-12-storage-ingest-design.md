# Storage ingest design: DIMSE C-STORE SCP and HTTP STOW-RS

Date: 2026-11-12 · Status: proposed · Milestones: M2 (storage) and the C-STORE part of M3 (DIMSE)

## Goal

Accept DICOM instances over two transports, the DIMSE C-STORE SCP and HTTP
STOW-RS. Both feed **one ingest pipeline**. That pipeline stores each instance
durably and exactly once, indexes it, and makes it streamable. The pipeline
must never lose data it acknowledged. It must never acknowledge data it did not
persist. It must never hold a whole instance in memory.

## Non-goals (this spec)

C-FIND, C-MOVE, C-GET, QIDO-RS, WADO-RS, transcoding, object-store backends,
PostgreSQL, OIDC, and TLS. The design leaves seams for each: `FsBlobStore` is the
blob boundary (a trait is extracted when a second backend lands), `Index` wraps sqlx,
and dicom-ul already has TLS options.

## Architecture

```
 modality/PACS ──DIMSE──▶ slicestream-dimse::StoreScp ──┐
                                                          ├─▶ slicestream-store::Ingestor ─▶ FsBlobStore
 app/viewer   ──HTTP───▶ slicestream-server::stow     ──┘            │                     ─▶ Index (SQLite)
                                                                       └─▶ broadcast<StoredEvent> ─▶ series cache
```

### Write path (identical for both transports)

1. **Spool.** The transport streams bytes into a `TempBlob` under `<root>/tmp/`.
   SHA-256 and the byte count are computed while streaming. DIMSE writes the
   Part-10 preamble, `DICM` and the file meta group first. The meta group is
   built from the C-STORE command and the negotiated transfer syntax. STOW parts
   already arrive as Part-10 files.
2. **Size guard.** Spooling aborts as soon as `max_instance_bytes` is exceeded.
3. **Parse header.** Read up to (and excluding) PixelData from the temp file.
   Extract the UIDs, the patient, study and series attributes, and the `FrameInfo`.
4. **Validate.** Check required UIDs. Check that UIDs in the command or URL match
   the dataset. Check that the SOP class is a storage class. Check that the
   transfer syntax is known.
5. **Commit blob.** `fsync` the file, rename it atomically to
   `<root>/objects/<h[0..2]>/<h[2..4]>/<sha256>.dcm`, then `fsync` the directory.
   Files are content-addressed, so no attacker-controlled string ever reaches a path.
6. **Index.** Run one SQLite transaction that upserts patient, study, series and
   instance.
7. **Acknowledge.** The pipeline returns success to the transport. It then
   broadcasts a `StoredEvent`.

Crash safety comes from this order. A blob can exist without an index row (an
orphan); startup GC deletes orphans. An index row never exists without its blob.
Startup also deletes leftover `tmp/` files.

### Duplicates

Policy `on_duplicate`: `keep` (the default) or `overwrite`.
- Same SOP Instance UID and same hash: success, flagged `duplicate`. Nothing is written.
- Same UID, different hash, `keep`: success, flagged `duplicate`. The new blob is discarded.
- Same UID, different hash, `overwrite`: the index row points to the new blob. The old blob is deleted after commit.

Concurrent stores of the same UID from any transport are serialized by the
`UNIQUE(sop_instance_uid)` constraint and `INSERT … ON CONFLICT`. Both callers
get success.

### Hierarchy conflicts

A study that already exists under a different PatientID is accepted and logged
as a warning. The existing patient link is kept. This matches Orthanc's default.

### Status mapping

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

STOW-RS HTTP status: 200 if all parts were stored. 202 if some parts were stored
and some failed. 409 if none were stored. 415 if the Content-Type is not
`multipart/related; type="application/dicom"`. 413 if the request exceeds
`max_request_bytes`. 400 if the multipart body is malformed. 401 if a token is
configured and missing or wrong. The body is `application/dicom+json` and holds
(0008,1190) RetrieveURL, (0008,1198) FailedSOPSequence and (0008,1199)
ReferencedSOPSequence.

### DIMSE SCP behavior

- Built on `dicom-ul` 0.10 `ServerAssociationOptions` and `AsyncServerAssociation`,
  on the tokio runtime that HTTP also uses.
- AE title (default `SLICESTREAM`) and port (default 11112).
  `require_called_ae_title` defaults to true.
- `allowed_peers`: if empty, any calling AE is accepted. Otherwise only the listed
  calling AE titles are accepted. Implemented with dicom-ul `AccessControl`.
- Accepts any abstract syntax (`promiscuous`). Accepts every transfer syntax in
  `TransferSyntaxRegistry`, including encapsulated ones, because pixels are never
  decoded on ingest. A non-storage SOP class in a C-STORE gets 0x0122.
- Command PDVs are assembled across fragments. The reference storescp handles
  only single-fragment commands. Data PDVs stream straight to the `TempBlob`.
- Handles C-ECHO and C-STORE. Any other command gets a response with status
  0x0211 and the same command field | 0x8000.
- Limits: `max_associations` (default 64) through a semaphore. `max_pdu_length`
  (default 116794). Read and write timeouts (default 60 s).
- An abort, a peer disconnect or a timeout mid-instance drops the `TempBlob`,
  which deletes its file. No response is sent.
- Graceful shutdown: stop accepting, let in-flight associations finish
  (30 s grace), then abort them.

### HTTP surface

- `POST /dicom-web/studies` and `POST /dicom-web/studies/{study}`: STOW-RS.
  Each part is streamed to a `TempBlob`; the body is never buffered.
- `POST /api/instances` with `Content-Type: application/dicom`: one Part-10
  file. Returns the per-instance result as JSON. This convenience endpoint
  mirrors Orthanc's.
- Optional `stow_token` (a static bearer token). Real auth (OIDC) comes in M6.

### Read-path integration

The series catalog is built from the index, not from folder scans.
`slicestream-server import <dir>` ingests existing files through the same
pipeline. `StoredEvent` invalidates the affected `Series` cache entry, so new
slices become streamable at once.

### Configuration

The configuration is in a TOML file (`--config slicestream.toml`). Each key has
a default:

```toml
[storage]
root = "storage"                 # tmp/, objects/, index.sqlite live here
max_instance_bytes = 2147483648  # 2 GiB
on_duplicate = "keep"            # keep | overwrite

[http]
addr = "127.0.0.1:8080"
max_request_bytes = 17179869184  # 16 GiB
# stow_token = "..."

[dimse]
enabled = true
ae_title = "SLICESTREAM"
addr = "0.0.0.0:11112"
require_called_ae_title = true
allowed_peers = []               # e.g. ["CT01", "MR02"]
max_associations = 64
max_pdu_length = 116794
read_timeout_secs = 60
write_timeout_secs = 60
```
