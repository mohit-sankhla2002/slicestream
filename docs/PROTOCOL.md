# slicestream wire protocol, version 1

This spec is language-neutral. A client in any language can follow it to
consume the stream. The Rust reference implementation is in
`crates/slicestream-core/src/protocol.rs`. The JavaScript reference is in
`web/viewer.js` (frame decoding in `web/lib/viewer-core.js`).

## Connection

```
GET /api/series/{seriesInstanceUid}/stream   (WebSocket upgrade)
```

## Server → client

**1. Hello (text message, sent once, first).**

```json
{ "type": "series", "series": {
    "uid": "1.2.3", "description": "CT CHEST", "modality": "CT",
    "sliceCount": 412, "first": { /* FrameInfo, see below */ } } }
```

**2. Frames (binary messages, any number).**

```
offset  size          field
0       4             magic, ASCII "SLS1"
4       4             header length N, unsigned 32-bit little-endian
8       N             header, UTF-8 JSON: { "index": <int>, "info": FrameInfo }
8+N     rest          payload
```

`index` is the 0-based position in the stack, sorted along the slice normal.

When `info.transferSyntax` is Explicit VR Little Endian (`1.2.840.10008.1.2.1`)
or Implicit VR Little Endian (`1.2.840.10008.1.2`), the payload is raw pixels.
For any other transfer syntax, the payload is the compressed codestream exactly
as stored. For example, `1.2.840.10008.1.2.4.201` is HTJ2K.

`FrameInfo`:

| field | type | notes |
|---|---|---|
| rows, columns | u16 | |
| bitsAllocated, bitsStored | u16 | |
| samplesPerPixel | u16 | 1 = grayscale, 3 = color |
| pixelRepresentation | u16 | 0 = unsigned, 1 = signed |
| photometricInterpretation | string | for example `MONOCHROME2` |
| transferSyntax | string | UID |
| rescaleSlope, rescaleIntercept | f64 | modality LUT |
| windowCenter, windowWidth | f64 or null | default VOI |

## Client → server (text messages, JSON)

| message | meaning |
|---|---|
| `{"type":"cursor","index":120,"velocity":-14.2}` | The viewer now shows slice `index`. `velocity` is in slices per second, signed (0 = idle). Send one on every slice change. |
| `{"type":"evicted","indices":[3,4,5]}` | The client dropped these slices from its cache. The server sends them again when they become relevant. |

Unknown messages are ignored, so clients may send extensions.

## Progressive mode (`?progressive=htj2k`)

```
GET /api/series/{seriesInstanceUid}/stream?progressive=htj2k   (WebSocket upgrade)
```

Progressive mode is opt-in. Without the parameter the stream is exactly as
described above and nothing in this section applies. Any value other than
`htj2k` gets 400. When the server's `[htj2k] mode` is `"off"`, `htj2k` gets
400 too, and the message names `[htj2k] mode = "off"`. The magic stays `SLS1`:
progressive mode only adds JSON fields and one client message.

**Hello.** The hello message gains a top-level key `"progressive": "htj2k"`, next to `type` and `series`.

**Parts.** A slice that has an HTJ2K copy (lossless, transfer syntax
`1.2.840.10008.1.2.4.202`) is sent as several binary messages, each carrying a
slice of one codestream. The frame header gains a `part` object, and
`info.transferSyntax` is `1.2.840.10008.1.2.4.202`:

```json
{ "index": 12, "info": { "transferSyntax": "1.2.840.10008.1.2.4.202" },
  "part": { "offset": 65536, "total": 91834, "level": 1 } }
```

| field | meaning |
|---|---|
| `offset` | Byte offset of this payload within the whole codestream. |
| `total` | Length of the whole codestream in bytes. |
| `level` | The highest resolution level that is complete once this part is appended. 0 is the smallest resolution; the largest value is the full image. It is 0 both when only resolution 0 is complete and when not even that is, so `level` 0 alone does not mean the prefix can be decoded. |

- The frame is complete when `offset + payload length == total`. The client
  may decode the prefix it holds. The server does not send where each
  resolution ends, only `level` on each part: a client that wants to decode
  at each resolution tries at part boundaries, and treats a new, higher
  `level` as the sign that a new resolution has become complete.
- **Assembling parts.** The client keeps one buffer per `index`. It appends a
  part only when the part's `offset` equals the number of bytes it holds for
  that index; otherwise it drops the part. A part with `offset` 0 starts a new
  buffer and replaces any buffer held. A whole frame (a message with no
  `part`) replaces any partial buffer for that index. This is needed because
  after the client sends `evicted`, parts of the evicted slice that were
  already in flight still arrive, and the server then starts the slice again
  from offset 0.
- The server sends the parts of one frame in order. Parts of different slices
  may be interleaved.
- Every part is at most 65,536 bytes of payload, the first (preview) part
  included. A preview larger than that arrives as several parts.
- The first bytes of a slice reach the preview resolution (the smallest one
  that is at least 256 pixels on the long side by default,
  `[htj2k] preview_size`, or the full image when it is smaller) before the rest of the
  slice. For the slice at the cursor, the preview and the rest are sent back
  to back. For slices ahead, previews come first, then the remainder, so a
  scroll lands on something sharp.
- A slice without a copy (not eligible, or no copy could be made) is sent
  whole, in its stored transfer syntax, in one message with no `part`, as in
  the plain stream. So is a slice whose instance changed after the connection
  opened. A whole frame may therefore follow parts of the same `index` (for
  example after a read failure on the server). The client then replaces the
  parts it holds with the whole frame.
- If the server cannot read a slice at all after it has sent parts, it stops
  sending parts for that slice and does not tell the client. It sends the slice
  again only after the client sends `evicted` for it. Clients should evict
  partial slices when the stream goes idle, so such a slice is retried.

**Acknowledgements.** The client sends, after every binary message it
receives:

```json
{ "type": "ack", "bytes": 262144 }
```

`bytes` is the cumulative number of payload bytes received so far, counted
over all binary messages of the connection. It counts payload only, not the
`SLS1` header and its JSON. A client may coalesce the acks of messages it
processes in the same event-loop turn, sending one ack with the latest total.

The server keeps the unacknowledged bytes within a window, so that a scroll
change takes effect after at most one window of data:

- The window starts at 256 KiB (262,144 bytes).
- It follows about twice the bytes acknowledged over the shortest recent round
  trip, never below 128 KiB (131,072) and never above 16 MiB (16,777,216).
  Roughly every 10 seconds the server briefly holds the queue short to
  measure the round trip again.
- When nothing is unacknowledged, the server always sends one message, even if
  it is larger than the window. This guarantees progress.
- Without acks the stream stops after about one window. A client in
  progressive mode must therefore ack. The plain stream needs no acks.

**Cursor and eviction** work as above. Control messages are applied between
parts, so a `cursor` message changes the order from the next part on.
`evicted` makes the server send the listed slices again from the start.

## Ordering guarantees

- The server waits up to 100 ms for the first `cursor` message. The first frame
  it sends is then the one the client displays.
- After every `cursor` message, the next frame sent is the cursor slice (if not
  yet sent). Then come the slices ahead in scroll direction, by distance. Slices
  behind are penalized 4×. When idle (`velocity` = 0), the order expands evenly
  in both directions.
- Each slice is sent at most once per connection, unless it is `evicted`.

## Versioning

The magic `SLS1` carries the major version (progressive mode included). Breaking changes will use `SLS2`.
New JSON fields may be added at any time. Clients must ignore fields they do
not know.
