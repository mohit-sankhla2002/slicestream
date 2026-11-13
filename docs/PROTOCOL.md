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

## Ordering guarantees

- The server waits up to 100 ms for the first `cursor` message. The first frame
  it sends is then the one the client displays.
- After every `cursor` message, the next frame sent is the cursor slice (if not
  yet sent). Then come the slices ahead in scroll direction, by distance. Slices
  behind are penalized 4×. When idle (`velocity` = 0), the order expands evenly
  in both directions.
- Each slice is sent at most once per connection, unless it is `evicted`.

## Versioning

The magic `SLS1` carries the major version. Breaking changes will use `SLS2`.
New JSON fields may be added at any time. Clients must ignore fields they do
not know.
