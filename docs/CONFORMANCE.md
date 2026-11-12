# DICOM conformance statement (storage ingest)

This statement covers the storage ingest features of slicestream: a DIMSE
Storage SCP and an HTTP STOW-RS endpoint. It is not a full DICOM Conformance
Statement per PS3.2. It lists what the server accepts and returns.

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

STOW-RS requests use `Content-Type: multipart/related; type="application/dicom"`.
Responses are `application/dicom+json` and hold (0008,1190) RetrieveURL,
(0008,1198) FailedSOPSequence and (0008,1199) ReferencedSOPSequence. A
FailedSOPSequence item carries (0008,1150) / (0008,1155) whenever the part's
header could be parsed.

The RetrieveURLs follow the WADO-RS layout
(`/dicom-web/studies/{study}/series/{series}/instances/{instance}`), but WADO-RS
is not served yet, so they return 404 for now.

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

Ingest routes never send CORS headers. Read routes send them only for the
origins in `[http] cors_allowed_origins` (default: none). Every route checks
the `Host` header against `[http] allowed_hosts` (default: `localhost`,
`127.0.0.1`, `[::1]` and the host of `[http] addr`).

## Limits and defaults

| Setting | Default |
|---|---|
| `[storage] max_instance_bytes` | 2147483648 (2 GiB) |
| `[storage] on_duplicate` | `keep` (or `overwrite`) |
| `[http] max_request_bytes` | 17179869184 (16 GiB) |
| `[http] cors_allowed_origins` | empty (no CORS) |
| `[http] allowed_hosts` | empty (loopback names and the host of `addr`) |
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
