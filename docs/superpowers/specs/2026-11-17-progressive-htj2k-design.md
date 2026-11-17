# Progressive HTJ2K delivery design (M4, part 1)

Date: 2026-11-17 · Status: proposed · Builds on: [DICOMweb read path](2026-11-15-dicomweb-read-path-design.md)

## Intent

The user wants a viewer that loads, caches and scrolls images faster than
OHIF. Their first priority is **remote reading over a slow link**: a reader
far from the server opens a study and should see an image as soon as
possible. Large CT stacks (smooth scrolling, a memory budget) come second.

On a slow link the time goes into moving bytes, not decoding or drawing.
Today slicestream sends every frame exactly as stored. A typical CT slice is
stored uncompressed: 512 × 512 × 16 bits, about 0.5 MB, so about 0.4 s per
slice at 10 Mbit/s. The fix is to send fewer bytes before the first image.

The user accepted that a lower-quality image may appear first, as long as it
sharpens to the exact original pixels. That is what progressive HTJ2K gives:
a frame encoded resolution-first (RPCL), where the first few percent of the
bytes already decode to a whole low-resolution image, and the full
codestream decodes losslessly.

The work is split into two projects, in this order (agreed with the user):

1. **This document:** the server creates progressive HTJ2K copies of stored
   frames and delivers them progressively, over the WebSocket stream and over
   standard DICOMweb. It is useful on its own: Cornerstone3D and OHIF can
   already load progressive HTJ2K over HTTP byte ranges.
2. **Later, its own design:** the WASM viewer engine and React component
   (the user's option A), which consumes this delivery.

Decided by the user: slow-link reading first; progressive display is
acceptable if it ends lossless; the server part comes before the viewer; the
encoder is a pure-Rust HTJ2K library (approach A below), validated by a spike
first. Everything else here is the designer's call and is marked as such
where it matters.

## Goals

1. **Progressive copies.** For every frame of a supported stored instance,
   the server can produce an HTJ2K Lossless RPCL codestream (transfer syntax
   `1.2.840.10008.1.2.4.202`) and keep it next to the original.
2. **Lossless, always.** A copy is only kept after it has been decoded and
   compared, sample by sample, with the original pixels. A copy that fails
   the check is never served.
3. **Made in the background, or on demand.** Copies are made by a background
   worker after ingest (and for stores that existed before this change). A
   request for a copy that does not exist yet makes it right then.
4. **Resolution boundaries known.** For each frame, the byte offset where
   each resolution level becomes complete is recorded, so the server can send
   "exactly the 256 × 256 preview" rather than a guessed fraction.
5. **Progressive WebSocket stream.** A client that opts in receives frames in
   parts: a preview part for nearby slices first, then the rest. Clients that
   do not opt in see no change at all.
6. **Progressive DICOMweb.** Frames can be retrieved as HTJ2K
   (`image/jphc` or `transfer-syntax=1.2.840.10008.1.2.4.202`), with HTTP
   `Range` on single-frame requests, so Cornerstone3D's byte-range and
   streaming retrieve work. OHIF, configured for progressive loading, opens a
   CT from slicestream progressively.
7. **Short send queue.** On a slow link, a scroll change takes effect within
   about one round trip plus one part, not after seconds of queued data, also
   when a reverse proxy sits between the server and the reader (the
   deployment SECURITY.md recommends).
8. **Measured.** A slow-link benchmark (10 Mbit/s, 80 ms round trip,
   simulated locally) compares time to first image and time to all-lossless,
   before and after, over the stream and through OHIF.

## Non-goals

- The viewer itself (project 2).
- Lossy encoding of any kind, including lossy HTJ2K (`.203`).
- Converting instances already stored compressed (JPEG, JPEG-LS, JPEG 2000,
  RLE, HTJ2K). They are served as stored, as today. RLE may be added later.
- Color, palette, more than 16 bits stored, and float pixel data. These are
  served as stored. (Most CT and MR is 16-bit grayscale.)
- Whole-instance retrieve in HTJ2K (`application/dicom` with
  `transfer-syntax=.202`). It still answers 406 unless stored that way. OHIF
  and Cornerstone retrieve frames, not instances.
- `rendered` and `thumbnail` resources.
- Deleting copies when instances are deleted (there is no delete yet).

## Anti-goals

The design has failed, even if it works, if any of these happens:

- **The original file changes.** Originals stay byte-for-byte as received.
- **Anything lossy is served as lossless.** Every served copy passed the
  round-trip check; a preview part is only ever labelled as a part.
- **Ingest gets slower.** Making copies never delays a C-STORE response,
  a STOW-RS response or an upload; it happens after the instance is stored.
- **The server decodes outside JPEG 2000 data with the new library.**
  `openjph-core` is young. The server only uses it to encode pixels it
  decoded itself, and to decode codestreams it just produced.
- **Existing clients break.** The test console, OHIF with the README entry,
  and every DICOMweb client keep exactly today's behaviour unless they ask
  for HTJ2K or opt in to progressive parts.

## Constraints

- MSRV stays 1.88 (workspace `rust-version`). This ruled out the `j2k` crate,
  which needs 1.99.
- Pure Rust, no C or C++ toolchain in the build.
- Encoder: `openjph-core = "=0.1.0"` (BSD-2-Clause, MSRV 1.78), pinned
  exactly because it is young.
- The disk is a real cost: copies of uncompressed CT add about a third of the
  original size (3:1 lossless compression in the spike).
- Patient data rules from CONTRIBUTING and SECURITY apply to fixtures,
  benchmarks and screenshots: phantom or public anonymized data only, and the
  TCIA series is never committed.

## Approaches considered

**A. Pure-Rust HTJ2K encoder (`openjph-core`) — chosen by the user.** No C++
in the build, no unsafe code at our boundary, and the same library can be
compiled to WASM later. Risk: a 0.1.0 crate with a single release. The spike
(below) showed it works for our case.

**B. OpenJPH (C++ reference) through our own bindings.** The most proven
encoder, and the decoder Cornerstone ships. No Rust binding exists, so it
needs a C wrapper, a C++ compiler in every build, CI work on every platform,
and unsafe code at the boundary. This is the fallback if A fails.

**C. An external converter program** (`ojph_compress`). No bindings, but a
second program to ship and a process plus temporary file per frame. Dropped.

### Spike results (throwaway, 2026-11-17)

Encoder settings: reversible 5/3 wavelet, 5 decomposition levels, 64 × 64
code blocks, one tile, one layer, RPCL. A second run added a TLM marker
(`request_tlm_marker(true)`, which `openjph-core` does write); results were
the same and Cornerstone's decoder read all 201 frames exactly. Data: 20 phantom slices (512²,
12-bit unsigned) and a public TCIA CT series (181 slices, 512², 16-bit
signed).

| | Phantom | TCIA CT |
|---|---|---|
| Lossless round trip (`openjph-core` decoder) | 20/20 exact | 181/181 exact |
| Decoded by Cornerstone's OpenJPH WASM (`@cornerstonejs/codec-openjph` 2.4.11) | 20/20 exact | 181/181 exact |
| Compression vs uncompressed | 26× | 3.0× (173 KB vs 524 KB per slice) |
| Encode time per slice, one core | 2.3 ms | 4.5–5.2 ms |

Bytes of a TCIA slice needed before each resolution is complete (decoded by
Cornerstone's decoder, identical to the same level decoded from the whole
codestream): 64² at 2.3 %, 128² at 7.9 %, 256² at 28 %, 512² at 100 %. At
10 Mbit/s a 256² preview is about 40 ms, the lossless slice about 140 ms,
and today's uncompressed slice about 420 ms.

Findings that shape the design:

- `openjph-core` 0.1.0 ignores the tile-part division setting (one tile-part
  is always written), and its reduced-resolution decode returns full-size
  output. Its encoder builds the packets one resolution at a time
  (`Tile::build_all_packets`), so reporting each resolution's end offset is a
  small change (see "Resolution boundaries").
- The encoder always writes packets in LRCP order whatever the progression
  order says. With one layer, one component and one precinct per resolution,
  LRCP and RPCL produce the same byte order, which is why the spike's output
  is correctly progressive. The design therefore fixes those three settings.

## Design

### Where the code goes

| Piece | Location |
|---|---|
| Pixel extraction, encoder settings, encode + verify, boundaries | new crate `crates/slicestream-codec` (no I/O; depends on `slicestream-core` and `openjph-core`) |
| Copy storage, index tables, background worker, on-demand creation | `slicestream-store` (new module `derived.rs`, migration `0004`) |
| Two-stage scheduling | `slicestream-core/src/scheduler.rs` (pure logic, as today) |
| Progressive stream parts | `slicestream-core/src/protocol.rs`, `slicestream-server/src/stream.rs` |
| HTJ2K and `Range` on frames | `slicestream-dicomweb` (negotiation), `slicestream-server/src/dicomweb/frames.rs` |
| Send-queue control | `slicestream-server/src/lib.rs` (listener) |
| Patched encoder | `vendor/openjph-core` via `[patch.crates-io]` until upstream releases the change |

`slicestream-codec` is a separate crate so the viewer project can compile the
same pixel and boundary logic to WASM later.

### Which instances get copies

An instance gets a copy when all of these hold (designer's call):

- its transfer syntax is native: Implicit VR LE, Explicit VR LE or Explicit
  VR BE;
- Samples per Pixel is 1 and Photometric Interpretation is `MONOCHROME1` or
  `MONOCHROME2`;
- Bits Allocated is 8 or 16, Bits Stored is 1–16 (Bits Allocated 1 excluded);
- it has Pixel Data and a frame table from the M1 scan (`offsets_valid`).

Anything else is recorded as "not eligible" with the reason, once, and never
retried. It is served as stored.

**Sample values.** Each sample is taken as the low Bits Stored bits (high
bits ignored, as PS3.5 requires), sign-extended when Pixel Representation is
1. The codestream precision is Bits Stored and its signedness follows Pixel
Representation. "Lossless" means these sample values are identical.

### Encoder settings

Fixed for every copy (designer's call; the reasons are in the spike
findings):

- reversible 5/3 wavelet, no color transform;
- decomposition levels: the smallest `L ≥ 0` with
  `ceil(min(rows, columns) / 2^L) ≤ 64`, as PS3.5 §10.18.1 requires for
  HTJ2K Lossless RPCL (512² gives 3 levels, a 3328 × 4096 mammogram 6,
  a 64² frame 0);
- 64 × 64 code blocks, one tile covering the frame, one quality layer, one
  precinct per resolution (the default maximum precinct size);
- progression order RPCL in the COD marker;
- a TLM marker in the main header (PS3.5 §10.18.1 requires it for `.202`),
  one tile-part.

These settings are what makes the label `1.2.840.10008.1.2.4.202` true. The
codec crate has a test that parses the produced main header and checks each
rule (COD progression RPCL, TLM present, reversible, base resolution at most
64 on its short side).

### Encode and verify

For each frame: extract samples (using the M1 frame table and
`read_frame_at`), encode, decode the result with `openjph-core`, and compare
every sample. Only an exact match is kept. On a mismatch or an encoder error,
the instance is recorded as failed with the reason and served as stored; the
error is logged once. A panic in the encoder is caught (`spawn_blocking`
join error) and treated the same way.

### Resolution boundaries

For each frame the copy records `ends[0..=levels]`: the byte length of the
codestream prefix (main header included) after which resolution `r` is
complete. `ends[levels]` is the codestream length.

`openjph-core` 0.1.0 does not expose these. The patch: make
`build_all_packets` record the packet-data length after each resolution, and
make the encoder report them (offset by the main header and the SOT/SOD
markers). It is about 20 lines. Send it upstream as a pull request, and
meanwhile use it from `vendor/openjph-core` through `[patch.crates-io]`, with
the patch kept as a separate, documented commit. Drop the vendored copy once
a release contains it. (The workspace crates are not published, so a path
patch costs nothing today.)

A unit test checks the boundaries against an oracle: the prefix up to
`ends[r]` decodes, at level `r`, identically to the whole codestream at
level `r`. In CI the oracle is Cornerstone's decoder (see Testing), because
`openjph-core`'s reduced-resolution decode is not usable.

### Storage

- One copy file per instance, holding the codestreams of all its frames back
  to back, in the blob store as a new kind `BlobKind::Htj2k` (extension
  `j2cs`), content-addressed like other blobs.
- Migration `0004` adds table `derived_htj2k`:

| column | meaning |
|---|---|
| `instance_fk` | primary key, references `instances(id)` |
| `source_blob` | hash of the original the copy was made from |
| `state` | `ready`, `not_eligible`, `failed` |
| `problem` | reason, for `not_eligible` and `failed` |
| `blob_sha256` | the copy file, for `ready` |
| `frames` | per frame: offset and length in the copy file, and `ends` (compact binary, like the M1 frame table) |
| `codec_version` | settings version, starts at 1; a higher version makes old rows stale |

- A row whose `source_blob` differs from the instance's current blob (the
  instance was overwritten) is stale: it is ignored and replaced.
- Startup recovery removes copy files no `ready` row references, like
  orphan originals today.

### Background worker and on-demand creation

- A worker task, like the M1 backfill, picks instances with no current row,
  newest ingest first, so freshly arrived studies get copies first.
  Parallelism: `[htj2k] workers`, default half the CPU cores (minimum 1),
  each frame encoded in `spawn_blocking`.
- Creation of one instance's copy is serialized by the existing per-UID lock
  stripes, so the worker and an on-demand request never encode it twice.
- **On demand:** a request that needs frames whose copy does not exist yet
  encodes and verifies only those frames, inside the request (about 5 ms per
  512² frame), serves them, and does not store them. It also moves the
  instance to the front of the worker's queue, so the stored copy follows
  soon. This keeps large multi-frame instances (hundreds of frames) from
  blocking a request on a whole-instance encode. If a frame cannot be
  encoded or the instance is not eligible, the stream sends the stored frame
  as today, and DICOMweb applies the normal 406 rule.
- Database errors are handled the way M1 now handles scan writes: errors
  caused by the row are recorded, transient database errors leave the row
  for a later try.
- `[htj2k] mode = "background"` (default), `"on-demand"` (no worker), or
  `"off"` (no copies, no progressive delivery; requests for HTJ2K get 406 as
  today).

### Progressive WebSocket stream

- **Opt-in:** the client connects with
  `GET /api/series/{uid}/stream?progressive=htj2k`. Without the parameter the
  stream is exactly protocol version 1 as today.
- **Frame parts.** In progressive mode a binary message may carry part of a
  frame. The JSON header gains:

  ```json
  { "index": 12, "info": { ... , "transferSyntax": "1.2.840.10008.1.2.4.202" },
    "part": { "offset": 0, "total": 172845, "level": 3 } }
  ```

  The payload holds codestream bytes `[offset, offset + payload length)`.
  `level` is the highest resolution complete once this part is appended
  (0 = smallest, `levels` = full). The frame is complete when
  `offset + length == total`. Parts of one frame arrive in order. Frames
  that have no copy are sent whole, as today, with no `part` field.
- **Two stages.** Each frame has a preview stage (bytes up to the preview
  level) and a final stage (the rest). The preview level is the smallest `r`
  whose resolution is at least 256 pixels on its long side, or full
  resolution when the frame is smaller (designer's call; configurable as
  `[htj2k] preview_size`).
- **Order** after every `cursor` message:
  1. the cursor slice, preview then final, back to back;
  2. previews of the slices ahead, in today's scheduler order, up to
     `preview_ahead` slices (default 32);
  3. then, in scheduler order, whatever stage each slice still needs
     (preview before final for any slice).

  `evicted` resets both stages of the listed slices. The scheduler change
  stays pure logic in `slicestream-core` and is unit tested there.
- A client in progressive mode must send `ack` messages (see "Short send
  queue"); without them the server stops after the first window.
- **Part size.** A final stage is cut into parts of at most 64 KiB, so a
  cursor change can take effect between parts of a large frame.
- `docs/PROTOCOL.md` documents the parameter and the `part` field. The magic
  stays `SLS1`: the change is opt-in and only adds a JSON field.

### Short send queue

Remote readers normally reach the server through a reverse proxy (SECURITY.md
asks for one until M6 adds authentication). The slow link then ends at the
proxy, whose send buffer can hold megabytes, so a socket option on the
server's own socket cannot keep the queue short. Flow control therefore
lives in the protocol, in progressive mode only:

- The client acknowledges what it has received:
  `{"type":"ack","bytes":<total binary payload bytes received so far>}`,
  sent at least every 64 KiB received and on every completed frame.
- The server keeps the unacknowledged bytes at or below a window. The window
  starts at 256 KiB and adapts to the measured delivery rate: it is set to
  twice the bytes acknowledged over the last round trip (measured from ack
  timing), never below 128 KiB and never above 16 MiB, so fast links are not
  throttled.
- Parts are chosen only when the window has room, so after a cursor change
  at most one window of old priorities is still on its way.

This brings the M6 roadmap item "credit-based flow control" forward, for the
progressive mode. The non-progressive stream is unchanged.

As a bonus for direct connections, the server also sets
`TCP_NOTSENT_LOWAT` (128 KiB) on accepted sockets where `socket2` supports it
(Linux and Android in the locked `socket2` 0.6.5; elsewhere nothing is set).
The server crate forbids `unsafe`, so only `socket2`'s safe API is used.

### DICOMweb frames in HTJ2K

- **Negotiation** in `slicestream-dicomweb`: on the frames resource, an
  acceptable type of `image/jphc` (with or without
  `transfer-syntax=1.2.840.10008.1.2.4.202`, or `.201`, which a `.202`
  codestream also satisfies) or `application/octet-stream;
  transfer-syntax=1.2.840.10008.1.2.4.202` selects the copy, created on
  demand if missing. Part `Content-Type` carries `transfer-syntax=...202`.
- `transfer-syntax=*`, `*/*` and no `Accept` keep today's answer (as
  stored), unless `[htj2k] prefer_for_wildcard = true` (default false;
  designer's call, so no existing client is surprised by HTJ2K).
- **Range.** A single-frame request with one `Range: bytes=a-b` (or `a-`)
  over HTJ2K answers 206 with `Content-Range: bytes a-b/<frame length>`
  and only those codestream bytes, in the same body format as the full
  answer. More than one range, or a range on several frames, ignores `Range`
  and answers 200 with the whole body (allowed by RFC 9110). An
  unsatisfiable range answers 416. `Accept-Ranges: bytes` is sent on HTJ2K
  frame answers.
- **CORS:** `Access-Control-Expose-Headers` on read routes adds
  `Content-Range`, which Cornerstone reads to learn the total size.
- The README gets a second OHIF data source entry, "slow links", that
  requests HTJ2K with Cornerstone's progressive retrieve configuration.
  CONFORMANCE.md replaces "the server does not transcode" with the exact
  rules above.

### Configuration

```toml
[htj2k]
mode = "background"          # "background" | "on-demand" | "off"
workers = 0                  # 0 = half the CPU cores, at least 1
preview_size = 256           # long side of the preview stage, in pixels
preview_ahead = 32           # slices whose preview goes before any final stage
prefer_for_wildcard = false  # serve HTJ2K copies for transfer-syntax=* on frames
```

## Testing

- **Codec crate:** sample extraction for 8/16-bit, signed/unsigned, Bits
  Stored < Bits Allocated with junk high bits, big-endian; level rule;
  round trip on the phantom; the verify step rejects a corrupted codestream;
  the boundary oracle with `openjph-core` at full level.
- **Store:** a copy is created by the worker and on demand; not eligible and
  failed rows are recorded once; overwrite makes the row stale and a new copy
  is made; transient database errors leave the row unrecorded; recovery
  removes orphan copies; migration `0004` on a store created before it.
- **Scheduler:** order of stages after cursor moves, `preview_ahead`,
  eviction resetting both stages, idle expansion.
- **Flow control:** the window limits unacknowledged bytes, grows with fast
  acks, shrinks to the floor with slow ones, and a cursor change after a full
  window is served within one window.
- **Server:**
  - the stream without `progressive` is byte-identical to today;
  - with it, parts reassemble to the stored copy, and the first message
    after hello is the cursor slice's preview part;
  - frames with `Accept: image/jphc` decode back to the original samples;
  - `Range` gives 206 with the right `Content-Range`, 416, and the
    several-ranges fallback;
  - wildcard behaviour with and without `prefer_for_wildcard`.
- **Interop, in CI:** `scripts/interop-htj2k.sh` installs
  `@cornerstonejs/codec-openjph@2.4.11` in a temporary directory and checks,
  with Node, that every copy of the phantom decodes losslessly and that each
  `ends[r]` prefix decodes identically to the whole codestream at level `r`.
- **OHIF acceptance (manual, Playwright, as in M1):** OHIF with the
  "slow links" entry opens the phantom and the TCIA series, shows a preview
  first and ends lossless (pixel check against the original through the
  browser console), with no console errors.
- **Slow-link benchmark:** `scripts/bench-slowlink.sh` runs a local
  throttling proxy (no root needed) at 10 Mbit/s and 80 ms round trip, with
  a send buffer of at least 4 MiB like a real reverse proxy, so the
  benchmark exercises the ack window rather than the server's socket. It
  measures, for a 181-slice CT: time to first displayable image and time to
  all slices lossless, for (1) stream as stored, (2) stream progressive,
  (3) OHIF as stored, (4) OHIF with the "slow links" entry.

## Performance targets

- Encode plus verify: at most 15 ms per 512² 16-bit frame on one core.
- On-demand creation for one single-frame instance: at most 50 ms before
  the first byte is sent.
- At 10 Mbit/s and 80 ms round trip, progressive stream: first image
  (preview) of a CT series in under 1 s from opening the socket; every
  slice lossless in under 40 % of the time the as-stored stream needs.
- A cursor jump to a far slice shows its preview within 300 ms on that link.
- OHIF with the "slow links" entry shows its first image sooner than OHIF
  with the M1 entry on the same link.

## Left to the builder

- The exact OHIF and Cornerstone configuration for the "slow links" entry
  (retrieve stages, Accept header), and whether Cornerstone's range path
  wants the partial bytes inside a multipart body or single-part. The
  acceptance run decides; the server rules above already allow both.
- The throttling proxy's implementation.
- How the encoder patch exposes boundaries (return value, accessor), as long
  as the oracle test passes.
- Worker scheduling details beyond "newest first", and metric names for copy
  counts in the console.
