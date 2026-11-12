# slicestream

**A fast, memory-safe DICOM server in Rust that streams slices to the browser in the order the user scrolls through them.**

> Status: early scaffold (v0.1). The streaming core works end to end with a demo
> viewer. Storage, DIMSE and full DICOMweb are on the [roadmap](docs/ARCHITECTURE.md#roadmap).
> Not for clinical use.

## Why this project

Web viewers such as [OHIF](https://ohif.org) and [Cornerstone3D](https://cornerstonejs.org)
are mature. The bottleneck is usually the server:

- **Scroll lag.** Viewers fetch frames one request at a time and prefetch in a fixed
  order. OHIF maintainers note that scrolling turns laggy once one image takes more
  than about 250 ms to arrive.
- **Progressive codecs need server support.** Cornerstone3D can draw a usable
  image from the first bytes of an HTJ2K (RPCL) stream. In their 4G test, first
  render took 66 ms with streamed HTJ2K and about 4.6 s with JPEG-LS. Their docs
  also say most DICOMweb servers do not support this yet.
- **Rust options are thin.** [dicom-rs](https://github.com/Enet4/dicom-rs) covers
  parsing well. [DICOM-RST](https://github.com/UMEssen/DICOM-RST) is a DICOMweb
  gateway, but it does not serve pixel data or bulk data from S3. No Rust server
  does scroll-aware or progressive streaming.

Verdict: worth building. There is a real, measurable gap. The best way in is to
make **any standard DICOMweb client faster**, without replacing viewers. A
custom protocol only helps if clients can adopt it in a few lines of code.

## Why Rust

| Need | How Rust helps |
|---|---|
| Parse untrusted DICOM from the network | Memory safety rules out a whole class of parser CVEs that C/C++ PACS code has had. |
| Thousands of concurrent streams | `tokio` async I/O, no GC pauses, so tail latency stays predictable while users scroll. |
| Zero-copy frame serving | `bytes::Bytes` and (planned) mmap/`pread` send frame bytes straight from disk to socket. |
| CPU-heavy transcoding (JPEG → HTJ2K) | Native speed and safe parallelism (`rayon`), with no GIL. |
| Same code in the browser | The core crate can compile to **WASM** for a client-side scheduler and decoders. |
| Use from other languages | One core, many bindings: C ABI, Python (PyO3), Node (napi-rs). |
| Simple deployment | One static binary, small Docker image, low memory use. |

Trade-offs: the Rust medical-imaging ecosystem is younger than DCMTK or pydicom.
Some codecs (JPEG 2000, HTJ2K) still mean wrapping C libraries (OpenJPEG, OpenJPH)
for now.

## What works today

- `slicestream-core`: indexes a folder of DICOM files into ordered stacks.
  It sorts by `ImagePositionPatient` along the slice normal, not just
  `InstanceNumber`. It extracts native or encapsulated frames, including
  multi-frame files with a Basic Offset Table. It contains the scroll-aware
  `PrefetchScheduler` and the wire protocol.
- `slicestream-server`: an axum server with a JSON API, single-frame HTTP fetch,
  and a WebSocket stream that changes its send order on every scroll event.
- `web/`: a dependency-free demo viewer (mouse wheel or arrow keys).

On a local synthetic series of 120 slices at 256×256, the first frame arrives in
about 26 ms. Frames then come in this order: the cursor slice, the slices ahead
in scroll direction, then the slices behind.

## Quick start

```sh
# 1. Generate a synthetic CT series (no patient data needed)
cargo run -p slicestream-core --example make_phantom -- data/phantom 120

# 2. Serve it
cargo run -p slicestream-server -- data

# 3. Open http://127.0.0.1:8080 and scroll
```

To serve real data, point the server at a folder that holds DICOM files.
The server scans that folder and its direct subfolders.

## HTTP API (current)

| Method | Path | Returns |
|---|---|---|
| GET | `/api/series` | JSON list of series summaries |
| GET | `/api/series/{uid}` | One series summary |
| GET | `/api/series/{uid}/frames/{index}` | Raw frame bytes. Metadata is in the `x-slicestream-frame-info` header. |
| GET (WS) | `/api/series/{uid}/stream` | Scroll-aware frame stream. See [docs/PROTOCOL.md](docs/PROTOCOL.md). |

## Project layout

```
crates/slicestream-core     transport-agnostic library (index, frames, scheduler, protocol)
crates/slicestream-server   reference HTTP + WebSocket server (axum/tokio)
web/                        demo viewer
docs/ARCHITECTURE.md        target architecture and roadmap
docs/PROTOCOL.md            language-neutral wire protocol spec
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). **Never attach real patient data
(PHI) to issues or pull requests.** Use the phantom generator or public
anonymized datasets such as [TCIA](https://www.cancerimagingarchive.net/).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
