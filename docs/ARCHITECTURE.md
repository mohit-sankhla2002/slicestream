# Architecture and roadmap

Goal: a PACS-grade DICOM server in the spirit of Orthanc, with streaming
performance as the main feature. It must work with Cornerstone3D/OHIF in React
without custom code, and with backends written in any language.

## Design principles

1. **Standards first, fast path second.** Every feature is reachable through
   standard DICOMweb (QIDO-RS, WADO-RS, STOW-RS) and DIMSE. Any viewer or
   backend therefore works with no custom code. The scroll-aware WebSocket
   stream is an *optional* speed-up on top.
2. **Language-neutral contracts.** The HTTP API has an OpenAPI spec. The wire
   protocol has a written spec ([PROTOCOL.md](PROTOCOL.md)). The core can be
   embedded through a C ABI. No consumer has to use Rust.
3. **Pluggable everything.** Storage, index database, codecs and auth sit behind
   traits, like Orthanc plugins, but with compile-time safety.
4. **Bytes never copied twice.** Index byte offsets at ingest time. Then serve
   frames with `pread`, mmap or object-store range reads.

## Comparison with Orthanc

| Area | Orthanc | slicestream target |
|---|---|---|
| Language / safety | C++ | Rust (memory-safe parser and network code) |
| Concurrency | Thread pool | Async (tokio); 10k+ concurrent streams |
| DICOMweb | Plugin | Built in, including frame-level WADO-RS and byte ranges |
| Progressive HTJ2K | No | Yes: transcode at ingest, stream RPCL progressively |
| Scroll-aware prefetch | No | Yes: server-side scheduler driven by the viewer cursor |
| Storage | Filesystem, DB plugins | Filesystem, S3/GCS/Azure (`object_store`), tiered cache |
| Index | SQLite / PostgreSQL | SQLite (embedded) / PostgreSQL |
| Extensibility | C plugins, Lua, Python | Rust traits, WASM plugins, webhooks |
| Bindings | REST only | REST, C ABI, Python, Node |

## Target crate layout

```
slicestream-core       index, frame extraction, scheduler, protocol      (exists)
slicestream-server     axum binary: wires everything together             (exists)
slicestream-archive    ZIP/7z/RAR/tar/gzip walker, NIfTI header            (exists)
slicestream-store      ingest, filesystem blobs, SQLite index, scan        (exists; object_store planned)
slicestream-index      Index trait: SQLite (sqlx), PostgreSQL              planned
slicestream-dicomweb   QIDO-RS parsing, Accept negotiation, multipart      (exists)
slicestream-dimse      C-ECHO/C-STORE SCP                                  (exists; C-FIND/C-MOVE/C-GET planned)
slicestream-codec      transcode to HTJ2K (OpenJPH), JPEG-LS, JPEG, RLE    planned
slicestream-wasm       browser build of core (scheduler, HTJ2K decode)     planned
slicestream-ffi        C ABI + cbindgen header                             planned
bindings/python        PyO3 package                                        planned
packages/cornerstone   npm: Cornerstone3D image loader for the WS stream   planned
```

## Integration with Cornerstone3D in React

Two levels, so teams can adopt step by step:

1. **No custom code.** Point `@cornerstonejs/dicom-image-loader` (or OHIF's
   DICOMweb data source) at `/dicom-web`. Standard `wadors:` image IDs work.
   The README has a tested OHIF data source entry.
   Once the codec crate exists, the server will also serve HTJ2K with
   `transfer-syntax=*` and HTTP byte ranges. That turns on Cornerstone's built-in
   progressive loading.
2. **Fast path.** Install `@slicestream/cornerstone-loader`. It registers a
   `slicestream:` image loader scheme. It opens one WebSocket per series and
   reports the viewport's current image index (from the `STACK_NEW_IMAGE`
   event) as the cursor. It resolves Cornerstone image-load promises from the
   frame cache. A `useSlicestream(seriesUid)` React hook will wrap this.

## Integration with backends in other languages

- **As a service:** run the binary or the Docker image, then call DICOMweb or
  REST from any language. We will publish the OpenAPI spec and generated
  clients (TypeScript, Python, Go, Java).
- **As a sidecar:** your app server keeps auth and business logic. slicestream
  handles DICOM. A JWT/OIDC verifier plus a webhook for authorization decisions
  keep the two in sync.
- **Embedded:** load `libslicestream` through the C ABI, or use the Python and
  Node packages, to index or extract frames in-process.
- **Events:** webhooks and an optional NATS/Kafka sink for "instance stored",
  "study stable" and other events.

## Roadmap

**M0 — Streaming core (done).** Folder index, frame extraction, scheduler,
WebSocket stream, console viewer, CI.

**M1 — Standard DICOMweb read path (done).** QIDO-RS (studies, series,
instances), WADO-RS instances, metadata, frames and bulk data, as
`multipart/related` or single part. OHIF v3.13 works with its stock DICOMweb
data source. A byte-offset index is built at ingest, so frames and bulk data
are served with `pread`; the WebSocket stream and `/api/series` frames use it
too. Stores from before M1 are indexed in the background.

**M2 — Storage and ingest (done for the filesystem).** STOW-RS, SQLite index,
filesystem storage, de-duplication and an `import` command are done.
Archive and NIfTI ingest is done too: ZIP, 7z, RAR, tar and gzip files, nested,
through `POST /api/archives`, the console and `import`. S3
storage and study "stable" detection are still open.

**M3 — DIMSE (C-ECHO and C-STORE SCP done).** C-ECHO and the C-STORE SCP are
done, so modalities and other PACS can push to the server. C-FIND, C-MOVE and
C-GET through `dicom-ul`, so they can also query it, are still open.

**M4 — Progressive codecs.** Transcode to HTJ2K RPCL at ingest or on demand.
Stream byte ranges progressively. Add a WASM decoder for the console viewer.

**M5 — Integrations.** `@slicestream/cornerstone-loader` with a React hook,
OpenAPI and generated clients, C ABI, Python bindings, webhooks.

**M6 — Production.** OIDC/JWT auth, audit log (ATNA), Prometheus metrics,
credit-based flow control on the stream, an LRU frame cache, Helm chart, and
benchmarks against Orthanc and dcm4chee.

## Known limits of the current scaffold

- Frames are read at offsets recorded at ingest. Instances stored before
  M1 have none until the background scan reaches them, and are read by
  parsing the whole file.
- Deflated data sets (Deflated Explicit VR Little Endian) are not accepted
  for storage.
- The DICOMweb read routes have no authentication or authorization until M6.
  Keep the server on loopback or behind an authenticating proxy.
- No transcoding: instances and frames are sent in the transfer syntax they
  were stored in. No `rendered` or `thumbnail` resources (M4), no HTTP
  `Range` requests, no XML responses.
- Frames go out as fast as TCP accepts them. There is no client credit or
  acknowledgement, so priority changes take effect only after the socket
  buffer drains.
- The console viewer renders only native (uncompressed) grayscale.
- Storage is the local filesystem only, and the series list is rebuilt from
  the index on demand.
- One process owns a storage root. `serve` and `import` take an exclusive
  lock on `<root>/LOCK`; a second process on the same root fails at startup.
  Use `import` only while the server is stopped; while it runs, upload through
  STOW-RS, `POST /api/instances` or C-STORE.
