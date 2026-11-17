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
| Progressive HTJ2K | No | Yes: lossless RPCL copies of stored instances, streamed progressively |
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
slicestream-codec      lossless HTJ2K copies (vendored openjph-core)       (exists; JPEG-LS, JPEG, RLE transcoding planned)
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
   For slow links, the server serves lossless HTJ2K copies of eligible
   frames (`image/jphc`, `transfer-syntax=1.2.840.10008.1.2.4.202`) with HTTP
   byte ranges. That turns on Cornerstone's built-in progressive loading;
   see the README for the OHIF "slow links" entry.
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

**M4 — Progressive codecs.** Part 1 is done: lossless progressive HTJ2K
copies (RPCL, verified sample by sample, made in the background or on
demand, never at ingest), a progressive WebSocket stream with an
acknowledgement window (`?progressive=htj2k`, see PROTOCOL.md), and DICOMweb
frames as HTJ2K with HTTP `Range`. Still open: lossy and other codecs
(JPEG-LS, JPEG, RLE transcoding), `rendered` and `thumbnail` resources, and
a WASM decoder for the console viewer.

The encoder is `openjph-core` 0.1.0, a pure Rust port of OpenJPH. It is
vendored in `vendor/openjph-core` and wired in with `[patch.crates-io]`. Two
patches (resolution boundaries, and a packet-header termination fix) are
described in `vendor/openjph-core/PATCHES.md` and are to be proposed
upstream. The build needs no C or C++ toolchain.

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
- Instances are sent as stored. Frames are sent as stored too, except that
  eligible frames (native monochrome, 8 or 16 bit) can be sent as lossless
  HTJ2K copies. No other transcoding. No `rendered` or `thumbnail`
  resources (M4), no XML responses. HTTP `Range` is supported only for
  single-frame HTJ2K answers.
- Each HTJ2K copy is a second file next to its original, so eligible
  instances take extra disk: about a third more for uncompressed CT in
  testing (3:1 lossless), up to about the size of the originals for noisy
  images. Set `[htj2k] mode = "off"` to make no copies.
- The plain stream (no `?progressive=htj2k`) sends frames as fast as TCP
  accepts them, with no client credit, so priority changes take effect only
  after the socket buffer drains. The progressive stream has an
  acknowledgement window instead (see PROTOCOL.md), but the window ends at
  the server's socket, so a proxy with a large buffer in front of it is
  still queued behind.
- The console viewer renders only native (uncompressed) grayscale.
- Storage is the local filesystem only, and the series list is rebuilt from
  the index on demand.
- One process owns a storage root. `serve` and `import` take an exclusive
  lock on `<root>/LOCK`; a second process on the same root fails at startup.
  Use `import` only while the server is stopped; while it runs, upload through
  STOW-RS, `POST /api/instances` or C-STORE.
