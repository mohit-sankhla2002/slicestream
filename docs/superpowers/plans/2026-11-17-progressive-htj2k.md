# Progressive HTJ2K Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve verified lossless progressive HTJ2K copies of stored grayscale frames over the WebSocket stream (opt-in, with acknowledgement-based flow control) and over DICOMweb (HTJ2K media types and HTTP `Range`), so a reader on a slow link sees a preview first and ends on the exact original pixels.

**Architecture:** A new pure crate `slicestream-codec` turns native frame bytes into samples, encodes them with a vendored, patched `openjph-core` (RPCL, TLM, one tile, one layer), verifies the round trip and reports where each resolution ends. `slicestream-store` keeps one copy blob per instance plus a `derived_htj2k` row, made by a background worker or, for single requests, encoded transiently. `slicestream-core` gains a two-stage scheduler and a flow-control window; the server uses them for `?progressive=htj2k` streams and uses the copies for HTJ2K frames over DICOMweb.

**Tech Stack:** Rust 1.88 (edition 2024), `openjph-core` 0.1.0 (vendored with one patch), sqlx 0.8 SQLite, axum 0.8.9, `socket2` 0.6.5, Node `@cornerstonejs/codec-openjph` 2.4.11 for interop checks, OHIF v3.13.14 for acceptance.

**Spec:** `docs/superpowers/specs/2026-11-17-progressive-htj2k-design.md`

## Global Constraints

- MSRV stays `rust-version = "1.88"`; `cargo +1.88 check --all-targets --locked` must pass (CI `msrv` job).
- No C or C++ toolchain in the build. Pure Rust only.
- `openjph-core = "=0.1.0"`, resolved to `vendor/openjph-core` through `[patch.crates-io]`; BSD-2-Clause notice kept in `vendor/openjph-core/LICENSE`.
- `#![forbid(unsafe_code)]` stays in `slicestream-store`, `slicestream-server`, `slicestream-dicomweb`; `slicestream-codec` also gets it.
- Copy transfer syntax: `1.2.840.10008.1.2.4.202` (HTJ2K Lossless RPCL). Media type `image/jphc`.
- Encoder settings, fixed: reversible 5/3, no color transform, levels = smallest `L ≥ 0` with `ceil(min(rows, columns) / 2^L) ≤ 64`, 64 × 64 code blocks, one tile, one layer, default (maximum) precincts, progression `RPCL`, TLM marker, one tile-part.
- Eligible: native transfer syntax (`1.2.840.10008.1.2`, `1.2.840.10008.1.2.1`, `1.2.840.10008.1.2.2`), Samples per Pixel 1, `MONOCHROME1`/`MONOCHROME2`, Bits Allocated 8 or 16, Bits Stored 1–16 and ≤ Bits Allocated, a frame table from the M1 scan.
- Originals are never modified. A copy is kept and served only after an exact sample-by-sample round trip.
- Copy creation never runs inside ingest (C-STORE, STOW-RS, upload, archive): only the worker and on-demand paths encode.
- The stream without `?progressive=htj2k` is byte-identical to today. DICOMweb answers for `transfer-syntax=*`, `*/*` and no `Accept` are unchanged unless `[htj2k] prefer_for_wildcard = true`.
- Config defaults: `mode = "background"`, `workers = 0` (half the cores, at least 1), `preview_size = 256`, `preview_ahead = 32`, `prefer_for_wildcard = false`.
- Stream parts at most 64 KiB (65 536 bytes of payload). Flow window starts at 256 KiB, floor 128 KiB, ceiling 16 MiB. `TCP_NOTSENT_LOWAT` 128 KiB where `socket2` offers it.
- Fixtures, benchmarks and screenshots: phantom or public anonymized data only. The TCIA series is never committed.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Bits Stored below Bits Allocated with junk high bits** (overlay bits, padding): samples must be masked to Bits Stored and sign-extended, and the round trip compares masked values. Test: `samples_mask_high_bits_and_sign_extend` (Task 2).
2. **Frames smaller than 64 on the short side** (32 × 32, 1 × 512 scouts): levels 0, `ends` has one entry equal to the codestream length, preview is the whole frame. Tests: `levels_rule`, `tiny_frame_has_one_resolution` (Task 2), `preview_level_for_tiny_frame` (Task 2).
3. **Instance overwritten while its copy is being made:** the write must not land, and a stale row is never served. Tests: `copy_write_skipped_after_overwrite`, `stale_copy_is_ignored` (Task 4).
4. **A progressive client that never acks, or acks more bytes than were sent:** the server stops at one window and does not panic; an over-ack is clamped to bytes sent. Tests: `window_blocks_without_acks`, `over_ack_is_clamped` (Task 6), `progressive_stream_stops_after_window_without_acks` (Task 8).
5. **Unusual `Range` forms:** suffix `bytes=-500`, start beyond the end, `a > b`, several ranges, `Range` on a multi-frame list, `Range` on a non-HTJ2K answer. Each has a defined answer. Test: `range_forms` (Task 9).

---

## File structure

| File | Responsibility |
|---|---|
| `vendor/openjph-core/**` | Copy of `openjph-core` 0.1.0 plus the boundary patch (Task 1) |
| `Cargo.toml` | `[patch.crates-io]`, new member `crates/slicestream-codec` |
| `crates/slicestream-codec/src/lib.rs` | Re-exports, constants `HTJ2K_RPCL`, `CODEC_VERSION` |
| `crates/slicestream-codec/src/samples.rs` | `eligibility`, `samples` |
| `crates/slicestream-codec/src/encode.rs` | `decomposition_levels`, `encode`, `encode_verified`, `Encoded` |
| `crates/slicestream-codec/src/levels.rs` | `preview_level`, `level_size` |
| `crates/slicestream-codec/src/conformance.rs` | `check_conformance` (main-header parser, used by tests and the interop script's Rust side) |
| `crates/slicestream-store/migrations/0004_htj2k.sql` | `derived_htj2k` table |
| `crates/slicestream-store/src/derived.rs` | `CopyFrames` encoding, copy maker, worker batch, transient encode |
| `crates/slicestream-store/src/index.rs` | Copy queries, `SliceRow.id`, `blob_hashes` includes copies |
| `crates/slicestream-store/src/blob.rs` | `BlobKind::Htj2k` (`j2cs`) |
| `crates/slicestream-core/src/staged.rs` | `StagedScheduler`, `Stage` |
| `crates/slicestream-core/src/flow.rs` | `FlowWindow` |
| `crates/slicestream-core/src/protocol.rs` | `ClientMessage::Ack`, `FramePart`, `encode_part` |
| `crates/slicestream-core/src/series.rs` | `SliceRef::with_instance`, `instance()` |
| `crates/slicestream-server/src/config.rs` | `Htj2kConfig` |
| `crates/slicestream-server/src/lib.rs` | worker task, `TCP_NOTSENT_LOWAT`, `AppState.htj2k` |
| `crates/slicestream-server/src/progressive.rs` | progressive stream loop |
| `crates/slicestream-server/src/stream.rs` | dispatch on `?progressive=htj2k` |
| `crates/slicestream-dicomweb/src/accept.rs` | HTJ2K negotiation (`FrameSource.copy_available`) |
| `crates/slicestream-dicomweb/src/range.rs` | `parse_range` |
| `crates/slicestream-server/src/dicomweb/frames.rs` | serve copies, `Range`, `Accept-Ranges` |
| `scripts/interop-htj2k.sh`, `scripts/htj2k-oracle.cjs` | Cornerstone decoder interop in CI |
| `scripts/bench-slowlink.sh`, `scripts/slowlink-proxy.mjs`, `scripts/slowlink-client.mjs` | Slow-link benchmark |
| Docs | `docs/PROTOCOL.md`, `docs/CONFORMANCE.md`, `docs/ARCHITECTURE.md`, `README.md`, `slicestream.example.toml` |

---

### Task 1: Vendor `openjph-core` and report resolution boundaries

**Files:**
- Create: `vendor/openjph-core/` (copy of the crates.io 0.1.0 package, then patched), `vendor/openjph-core/LICENSE`, `vendor/openjph-core/PATCHES.md`
- Modify: `Cargo.toml` (workspace root), `vendor/openjph-core/src/codestream/tile.rs`, `vendor/openjph-core/src/codestream/local.rs`, `vendor/openjph-core/src/codestream/mod.rs`
- Test: `vendor/openjph-core/tests/resolution_ends.rs`

**Interfaces:**
- Produces: `openjph_core::codestream::Codestream::resolution_ends(&self) -> Option<Vec<usize>>`. After `flush` of a single-tile, single-tile-part codestream: one entry per resolution (lowest first), each the byte length of the codestream prefix (from SOC, main header and SOT/SOD included) after which that resolution's packets are complete. The last entry equals the full codestream length (including EOC). `None` before `flush`, or for more than one tile or tile-part.

- [ ] **Step 1: Copy the package**

Copy `~/.cargo/registry/src/index.crates.io-*/openjph-core-0.1.0/` to `vendor/openjph-core/` (drop `.cargo-ok` and `.cargo_vcs_info.json`). Add `LICENSE` with the BSD-2-Clause text and copyright line from the upstream repository named in the package's `Cargo.toml.orig` (fetch it; the published package has no license file). Add to the root `Cargo.toml`:

```toml
[patch.crates-io]
openjph-core = { path = "vendor/openjph-core" }
```

Do not add `vendor/openjph-core` to `members`. Commit this unmodified copy on its own (`chore: vendor openjph-core 0.1.0`), so the patch is a separate, reviewable commit.

- [ ] **Step 2: Write the failing test** `vendor/openjph-core/tests/resolution_ends.rs`

```rust
// encode a 512x512 12-bit gradient with 3 decomposition levels, RPCL, TLM
#[test]
fn ends_cover_every_resolution() {
    let (bytes, ends) = encode_gradient(512, 512, 3); // helper in the test file
    assert_eq!(ends.len(), 4);
    assert!(ends.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(*ends.last().unwrap(), bytes.len());
    // the first resolution ends after the main header, SOT and SOD
    assert!(ends[0] > main_header_len(&bytes) + 12 + 2);
}

#[test]
fn ends_none_before_flush() { /* Codestream::new().resolution_ends() == None */ }

#[test]
fn zero_levels_has_one_end() { /* 32x32, 0 levels: ends == [bytes.len()] */ }
```

- [ ] **Step 3: Run it to verify it fails**

Run: `cargo test --manifest-path vendor/openjph-core/Cargo.toml --test resolution_ends`
Expected: FAIL, `no method named resolution_ends`.

- [ ] **Step 4: Implement the patch**

In `Tile::build_all_packets`, record `data.len()` after each resolution's packets (layer 0) into a `Vec<usize>` kept on the tile. In `encode_and_write`, convert to absolute offsets: the file position before SOT plus 12 (SOT) plus 2 (SOD) plus each packet-data length. In `CodestreamLocal::flush`, after EOC is written, store the tile's ends with the last entry replaced by the total length (so EOC is included). Expose through `Codestream::resolution_ends`. Keep the change minimal; describe it in `PATCHES.md` (what, why, upstream PR status: "to be proposed").

- [ ] **Step 5: Run the tests**

Run: `cargo test --manifest-path vendor/openjph-core/Cargo.toml` and `cargo build --workspace`
Expected: PASS; the workspace builds with the patch (nothing depends on the crate yet).

- [ ] **Step 6: Commit**

```bash
git add vendor/openjph-core Cargo.toml
git commit -m "feat(vendor): openjph-core reports where each resolution ends"
```

---

### Task 2: `slicestream-codec`: samples, encode, verify, levels

**Files:**
- Create: `crates/slicestream-codec/Cargo.toml`, `src/lib.rs`, `src/samples.rs`, `src/encode.rs`, `src/levels.rs`, `src/conformance.rs`
- Modify: `Cargo.toml` (members), `Cargo.lock`
- Test: unit tests in each module; `crates/slicestream-codec/tests/phantom.rs` (uses `#[path = "../../slicestream-core/examples/make_phantom.rs"]` like the store crate)

**Interfaces:**
- Consumes: `slicestream_core::FrameInfo`; `openjph_core::codestream::Codestream::resolution_ends` (Task 1).
- Produces:
  - `pub const HTJ2K_RPCL: &str = "1.2.840.10008.1.2.4.202";`
  - `pub const CODEC_VERSION: i64 = 1;`
  - `pub struct NotEligible(pub String);` (`Display`)
  - `pub fn eligibility(info: &FrameInfo) -> Result<(), NotEligible>`
  - `pub fn samples(frame: &[u8], info: &FrameInfo) -> Result<Vec<i32>, CodecError>` — `frame` is the stored bytes of one frame as `read_frame_at` returns them (big-endian words for `1.2.840.10008.1.2.2`).
  - `pub fn decomposition_levels(rows: u16, columns: u16) -> u32`
  - `pub struct Encoded { pub codestream: Vec<u8>, pub ends: Vec<u32> }` (`ends.len() == levels + 1`, last == `codestream.len()`)
  - `pub fn encode(samples: &[i32], info: &FrameInfo) -> Result<Encoded, CodecError>`
  - `pub fn encode_verified(frame: &[u8], info: &FrameInfo) -> Result<Encoded, CodecError>` — `eligibility`, `samples`, `encode`, `decode_full`, compare every sample.
  - `#[doc(hidden)] pub fn encode_verified_with(frame: &[u8], info: &FrameInfo, tamper: impl FnOnce(&mut Vec<u8>)) -> Result<Encoded, CodecError>` — same, with `tamper` applied to the codestream before the check (tests only).
  - `pub fn decode_full(codestream: &[u8]) -> Result<Vec<i32>, CodecError>` — full-resolution decode with `openjph-core`; also used by store and server tests to check copies.
  - `pub fn preview_level(rows: u16, columns: u16, preview_size: u32) -> usize` — index into `ends`.
  - `pub fn check_conformance(codestream: &[u8]) -> Result<(), String>`
  - `#[derive(Debug, thiserror::Error)] pub enum CodecError { NotEligible(String), BadFrame(String), Encode(String), Mismatch { at: usize } }`

- [ ] **Step 1: Write the failing tests**

```rust
// samples.rs
#[test] fn samples_mask_high_bits_and_sign_extend() {
    // BA 16, BS 12, PR 1, LE: word 0xF800 -> high 4 bits junk, low 12 = 0x800 -> -2048
    // word 0x07FF -> 2047; word 0xA001 -> 1
    assert_eq!(samples(&[0x00,0xF8, 0xFF,0x07, 0x01,0xA0], &info(16,12,1,LE,3,1)).unwrap(), vec![-2048, 2047, 1]);
}
#[test] fn samples_big_endian() { /* [0x01,0x02] with BE, BA16 BS16 PR0 -> [258] */ }
#[test] fn samples_eight_bit() { /* BA 8, BS 8, PR 0: [0, 255] -> [0, 255] */ }
#[test] fn samples_wrong_length_is_bad_frame() { /* 3 bytes for 2x1 16-bit -> CodecError::BadFrame */ }
#[test] fn eligibility_rules() {
    // ok: LE/implicit/BE, MONOCHROME1/2, BA 8/16, BS 1..=BA, SPP 1
    // not eligible, with a reason: RGB, SPP 3, BA 1, BA 32, BS 0, BS > BA, JPEG transfer syntax
}
// levels.rs / encode.rs
#[test] fn levels_rule() {
    assert_eq!(decomposition_levels(512, 512), 3);
    assert_eq!(decomposition_levels(3328, 4096), 6);
    assert_eq!(decomposition_levels(64, 64), 0);
    assert_eq!(decomposition_levels(65, 2000), 1);
    assert_eq!(decomposition_levels(1, 512), 0);
}
#[test] fn preview_level_for_tiny_frame() { assert_eq!(preview_level(32, 32, 256), 0); }
#[test] fn preview_level_picks_256() {
    // 512x512, 3 levels: sizes 64,128,256,512 -> index 2
    assert_eq!(preview_level(512, 512, 256), 2);
    // 3328x4096, 6 levels: long side 4096/2^(6-r) >= 256 -> r = 2
    assert_eq!(preview_level(3328, 4096, 256), 2);
}
#[test] fn tiny_frame_has_one_resolution() { /* 32x32 encode: ends == [len] */ }
#[test] fn round_trip_signed_16() { /* random i16 samples 512x512 -> encode_verified ok */ }
#[test] fn conformance_of_output() { /* check_conformance(&encode(..).codestream) == Ok(()) */ }
#[test] fn conformance_rejects_missing_tlm() { /* strip the TLM segment from a valid codestream -> Err mentions "TLM" */ }
#[test] fn verify_rejects_mismatch() {
    // a test-only hook `encode_verified_with(frame, info, |cs| corrupt(cs))` flips a byte
    // in the codestream before the check -> Err(CodecError::Mismatch{..}) or Err(Encode)
}
// tests/phantom.rs
#[test] fn phantom_series_round_trips() { /* make 4 phantom slices 256x256, encode_verified each, ends.len()==3 */ }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p slicestream-codec`
Expected: FAIL (crate items missing).

- [ ] **Step 3: Implement**

`samples`: per sample, read the word (LE, or BE for `1.2.840.10008.1.2.2`), mask with `(1 << bits_stored) - 1`, sign-extend from bit `bits_stored - 1` when `pixel_representation == 1`. Length must equal `rows * columns * bits_allocated/8`, else `BadFrame`.

`encode`: settings from Global Constraints (`set_comp_info(0, (1,1), bits_stored, signed)`, `set_block_dims(64, 64)`, `set_progression_order("RPCL")`, `request_tlm_marker(true)`, `set_tilepart_divisions(0)`), lines through `exchange`, `flush`, then `resolution_ends()`; a `None` or a length mismatch is `CodecError::Encode`.

`preview_level(rows, columns, p)`: with `L = decomposition_levels(rows, columns)`, the smallest `r` in `0..=L` with `ceil(max(rows, columns) / 2^(L - r)) >= p`, else `L`.

`check_conformance`: walk main-header markers from SOC to the first SOT; require SIZ, COD with progression order byte `2` (RPCL), transform byte `1` (reversible 5/3), `levels` decomposition such that `ceil(min(w,h) / 2^levels) <= 64`, and a TLM segment. Errors name the failing rule.

Add `#![forbid(unsafe_code)]`. `Cargo.toml`: `openjph-core = "=0.1.0"`, `slicestream-core = { path = "../slicestream-core", version = "0.1.0" }`, `thiserror`; `rand` as dev-dependency only if needed.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p slicestream-codec && cargo clippy -p slicestream-codec --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit** — `feat(codec): verified HTJ2K RPCL encoding of native grayscale frames`

---

### Task 3: Interop with Cornerstone's decoder in CI

**Files:**
- Create: `crates/slicestream-codec/examples/htj2k_fixtures.rs`, `scripts/htj2k-oracle.cjs`, `scripts/interop-htj2k.sh`
- Modify: `.github/workflows/ci.yml` (interop job: one more step)

**Interfaces:**
- Consumes: `encode_verified`, `preview_level` (Task 2).
- Produces: `scripts/interop-htj2k.sh` (exit 0 on success), reused by Task 10's docs.

- [ ] **Step 1: Fixture writer**

`cargo run -p slicestream-codec --example htj2k_fixtures -- <out_dir>` writes, for 6 phantom slices at 512² (12-bit unsigned), one 256² 16-bit signed synthetic frame, one 32 × 32 frame and one 3328 × 512 frame: `<name>.j2c`, `<name>.raw` (samples as little-endian i32) and `<name>.json` (`{"w","h","signed","bits","ends":[...]}`).

- [ ] **Step 2: Oracle** `scripts/htj2k-oracle.cjs <node_modules_dir> <fixture_dir>`

For each fixture: full decode with `HTJ2KDecoder` (`getEncodedBuffer`, `readHeader`, `decode`, `getDecodedBuffer`) must equal `.raw`; for each `r`, the prefix `ends[r]` decoded with `decodeSubResolution(L - r, 0)` must equal the whole codestream decoded at the same level (size from `calculateSizeAtDecompositionLevel`). Silence codec logging with `{ print() {}, printErr() {} }`. Exit 1 with the fixture name and level on any mismatch.

- [ ] **Step 3: Script** `scripts/interop-htj2k.sh`

Pattern of `scripts/interop-dicomweb.sh`: `set -euo pipefail`, require `node` and `npm`, temp dir, `npm install --ignore-scripts --no-audit --no-fund @cornerstonejs/codec-openjph@2.4.11` into it, run the example into the temp dir, run the oracle. Print `htj2k interop: N fixtures ok`.

- [ ] **Step 4: Run it**

Run: `bash scripts/interop-htj2k.sh`
Expected: `htj2k interop: 9 fixtures ok`, exit 0.

- [ ] **Step 5: CI** — add `- run: bash scripts/interop-htj2k.sh` to the interop job after the dicomweb step (Node is already set up there for the web tests; add `actions/setup-node` if that job lacks it).

- [ ] **Step 6: Commit** — `test(codec): decode copies with Cornerstone's OpenJPH in CI`

---

### Task 4: Store: copy table, blob kind, queries, recovery

**Files:**
- Create: `crates/slicestream-store/migrations/0004_htj2k.sql`, `crates/slicestream-store/src/derived.rs` (types + `CopyFrames` codec only in this task)
- Modify: `crates/slicestream-store/src/blob.rs`, `src/index.rs`, `src/lib.rs`, `src/ingest.rs` (recovery comment only if needed)
- Test: `crates/slicestream-store/tests/derived.rs`, unit tests in `derived.rs`

**Interfaces:**
- Produces:
  - `BlobKind::Htj2k` with `ext() == "j2cs"`, included in `BlobKind::ALL` (so `list()` sees copies).
  - `pub struct CopyFrame { pub offset: u64, pub len: u64, pub ends: Vec<u32> }`
  - `pub struct CopyFrames(pub Vec<CopyFrame>)` with `encode(&self) -> Vec<u8>` and `decode(&[u8]) -> Option<CopyFrames>` (versioned compact binary, like `FrameTable`).
  - `pub enum CopyState { Ready { blob: BlobHash, frames: CopyFrames }, NotEligible(String), Failed(String) }`
  - `Index::copy_candidates(&self, limit: i64) -> Result<Vec<(i64, String, BlobHash)>, sqlx::Error>` — instances with `frame_info` not null and `scan_version = SCAN_VERSION`, with no `derived_htj2k` row whose `source_blob` equals the instance blob and `codec_version = CODEC_VERSION`; newest (`id DESC`) first.
  - `Index::write_copy(&self, id: i64, source: &BlobHash, state: &CopyState) -> Result<bool, sqlx::Error>` — `INSERT OR REPLACE`, only when `instances.blob_sha256 = source`; `false` when the instance moved on.
  - `Index::copy_for(&self, id: i64, source: &BlobHash) -> Result<Option<CopyState>, sqlx::Error>` — `None` when missing, stale (`source_blob` differs) or older `codec_version`.
  - `Index::blob_hashes` also returns `blob_sha256` of `ready` copy rows.
  - `SliceRow.id: i64` (the instance id; `series_slices` selects `i.id`).

Migration:

```sql
CREATE TABLE derived_htj2k (
  instance_fk   INTEGER PRIMARY KEY REFERENCES instances(id) ON DELETE CASCADE,
  source_blob   TEXT NOT NULL,
  codec_version INTEGER NOT NULL,
  state         TEXT NOT NULL CHECK (state IN ('ready','not_eligible','failed')),
  problem       TEXT,
  blob_sha256   TEXT,
  frames        BLOB
);
CREATE INDEX derived_htj2k_blob ON derived_htj2k(blob_sha256);
```

- [ ] **Step 1: Write the failing tests** (`tests/derived.rs`, using the existing `support` helpers)

```rust
#[tokio::test] async fn copy_frames_round_trip() { /* encode/decode equal; decode(b"junk") == None */ }
#[tokio::test] async fn candidates_newest_first_and_skip_unscanned() { /* 3 instances, 1 unscanned -> 2 ids, descending */ }
#[tokio::test] async fn write_and_read_copy() { /* Ready row -> copy_for returns it; candidates no longer list it */ }
#[tokio::test] async fn copy_write_skipped_after_overwrite() { /* write_copy with old blob after on_duplicate=overwrite -> Ok(false), no row */ }
#[tokio::test] async fn stale_copy_is_ignored() { /* row for blob A, instance now blob B -> copy_for(id, &B) == None, candidate again */ }
#[tokio::test] async fn not_eligible_recorded_once() { /* NotEligible row -> not a candidate any more */ }
#[tokio::test] async fn recovery_keeps_ready_copies() { /* commit a j2cs blob, Ready row -> recover() keeps it; an unreferenced j2cs is removed */ }
#[tokio::test] async fn migration_0004_on_old_store() { /* store created with 0001-0003 opens; candidates lists its instances once scanned */ }
```

- [ ] **Step 2: Run them to verify they fail** — `cargo test -p slicestream-store --test derived` → FAIL.

- [ ] **Step 3: Implement** the migration, `BlobKind::Htj2k`, the queries and `CopyFrames` (`[version u8 = 1][count u32 LE] then per frame [offset u64][len u64][n u8][ends u32 × n]`). `slicestream-store` depends on `slicestream-codec` for `CODEC_VERSION`.

- [ ] **Step 4: Run** `cargo test -p slicestream-store` → PASS (all existing tests too).

- [ ] **Step 5: Commit** — `feat(store): HTJ2K copy table, blob kind and queries`

---

### Task 5: Store: copy maker, worker batch, transient encode

**Files:**
- Modify: `crates/slicestream-store/src/derived.rs`, `src/ingest.rs` (make `rejects_row` `pub(crate)`; expose lock stripes to `derived.rs`), `crates/slicestream-store/tests/support/mod.rs`
- Test: `crates/slicestream-store/tests/derived.rs`

Test helpers added to `tests/support/mod.rs` (built with `dicom-object`, Explicit VR LE, CT Image Storage, fresh UIDs): `native_multiframe(dir: &Path, frames: u32) -> PathBuf` (64 × 64, 16-bit unsigned, Bits Stored 12, distinct pixels per frame) and `rgb_instance(dir: &Path) -> PathBuf` (16 × 16, Samples per Pixel 3, 8-bit, `RGB`).

**Interfaces:**
- Consumes: Task 2 (`encode_verified`, `eligibility`), Task 4 (queries, `CopyState`, `BlobKind::Htj2k`), `FrameTable::location`, `read_frame_at`.
- Produces (on `Ingestor`):
  - `pub async fn make_copy(&self, id: i64) -> Result<(), IngestError>` — under the instance's lock stripe; skips when a current row exists; reads each frame with `read_frame_at`, `encode_verified` in `spawn_blocking`, writes the concatenated codestreams with `spool_sync` + `commit(.., BlobKind::Htj2k)`, then `write_copy`. `NotEligible` and codec errors record `NotEligible`/`Failed` with the reason; a panic in `spawn_blocking` records `Failed("the encoder failed")`. Database errors follow `rejects_row`: row errors are recorded, others return `Err` and leave no row. If `write_copy` returns `false`, delete the just-committed copy blob unless `blob_in_use`.
  - `pub async fn copy_batch(&self, limit: i64, parallel: usize) -> Result<usize, IngestError>` — first the ids in the priority queue, then `copy_candidates`; same `Err`-only-if-nothing-recorded rule as `backfill_batch`.
  - `pub fn prioritize_copy(&self, id: i64)` — pushes onto an in-memory bounded (1024) deduplicated queue read by `copy_batch`.
  - `pub async fn copy_frames(&self, id: i64, blob: &BlobHash) -> Result<Option<(CopyFrames, std::path::PathBuf)>, IngestError>` — a `Ready` current copy and its file path, else `None`.
  - `pub async fn encode_transient(&self, id: i64, numbers: &[u32]) -> Result<Vec<Encoded>, IngestError>` — encodes only those 1-based frames, stores nothing, calls `prioritize_copy(id)`; `IngestError::Unparseable(reason)` when not eligible.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn worker_makes_copies_that_decode_to_originals() {
    // ingest 3 phantom slices; copy_batch(10, 2) == 3; for each: copy_frames Some; read the
    // copy bytes [offset, offset+len) and decode with slicestream_codec test helper -> equals samples(original)
}
#[tokio::test] async fn ineligible_instance_recorded_with_reason() { /* support::rgb_instance(dir) -> NotEligible containing "Samples per Pixel", not retried */ }
#[tokio::test] async fn transient_errors_leave_no_row() { /* rename derived_htj2k away like the M1 test -> copy_batch Err, no row; rename back -> copies made */ }
#[tokio::test] async fn encoder_panic_recorded_as_failed() { /* test hook (cfg(test) TestHooks like scan) panics for one id -> Failed("the encoder failed"), others ok */ }
#[tokio::test] async fn prioritized_instance_goes_first() { /* 5 instances; prioritize oldest; copy_batch(1, 1) makes that one */ }
#[tokio::test] async fn transient_encode_stores_nothing() { /* encode_transient(id, &[1]) -> 1 Encoded; no row; prioritized */ }
#[tokio::test] async fn make_copy_is_not_repeated() { /* two concurrent make_copy(id) -> one blob, one row */ }
#[tokio::test] async fn multi_frame_copy_offsets() { /* support::native_multiframe(dir, 4) -> 4 CopyFrame, contiguous, non-overlapping, each decodes to its frame */ }
```

- [ ] **Step 2: Run them to verify they fail** — `cargo test -p slicestream-store --test derived` → FAIL.

- [ ] **Step 3: Implement** as specified in Interfaces. Frames encode one `spawn_blocking` per frame; `parallel` bounds concurrent instances (chunks like `backfill_batch`).

- [ ] **Step 4: Run** `cargo test -p slicestream-store` → PASS.

- [ ] **Step 5: Commit** — `feat(store): make, verify and store HTJ2K copies; transient encode`

---

### Task 6: Core: staged scheduler, flow window, protocol parts

**Files:**
- Create: `crates/slicestream-core/src/staged.rs`, `crates/slicestream-core/src/flow.rs`
- Modify: `crates/slicestream-core/src/protocol.rs`, `src/series.rs`, `src/lib.rs`
- Test: unit tests in each file

**Interfaces:**
- Consumes: `PrefetchScheduler` (unchanged).
- Produces:
  - `pub enum Stage { Preview, Final }`
  - `pub struct StagedScheduler` with `new(len: usize, config: SchedulerConfig, preview_ahead: usize)`, `set_single_stage(&mut self, index: usize)` (slice without a copy: only `Final`, sent whole), `update(&mut self, cursor: usize, velocity: f64)`, `mark_unsent(&mut self, index: usize)` (resets both stages), `requeue_final(&mut self, index: usize)` (the slice's `Final` was interrupted; hand it out again later, preview untouched), `pop_next(&mut self) -> Option<(usize, Stage)>`.
  - `pub struct FlowWindow` with `new() -> FlowWindow`, `on_sent(&mut self, bytes: u64, now: std::time::Duration)`, `on_ack(&mut self, total_acked: u64, now: Duration)` (`now` = time since the connection started, supplied by the caller, so core reads no clock and stays WASM-checkable), `room(&self) -> u64` (window − unacked, saturating), `window(&self) -> u64`, `unacked(&self) -> u64`. Constants `INITIAL = 262_144`, `FLOOR = 131_072`, `CEILING = 16_777_216`.
  - `ClientMessage::Ack { bytes: u64 }` (JSON `{"type":"ack","bytes":N}`).
  - `pub struct FramePart { pub offset: u64, pub total: u64, pub level: u32 }`; `FrameHeader.part: Option<FramePart>` with `#[serde(skip_serializing_if = "Option::is_none", default)]`.
  - `pub fn encode_part(index: usize, info: &FrameInfo, part: FramePart, payload: &[u8]) -> Bytes`.
  - `SliceRef::with_instance(self, id: i64) -> SliceRef`, `SliceRef::instance(&self) -> Option<i64>`, `Series::slices(&self) -> &[SliceRef]` if not already present.

`StagedScheduler::pop_next` order after each `update` (spec "Order"):

```text
1. cursor slice: its Preview if unsent, else its Final if unsent
2. Preview of slices in PrefetchScheduler order, while within `preview_ahead`
   slices handed out since the last update (count previews only)
3. otherwise the next slice in PrefetchScheduler order that still needs a stage:
   its Preview if unsent, else its Final
```

Implement with two `PrefetchScheduler`s (one tracking previews, one finals; a single-stage slice is pre-marked sent in the preview scheduler) plus a counter of previews since the last `update`.

`FlowWindow` adaptation: keep `(time, total_acked)` samples; on each ack, the rate over the last round trip (sample age ≥ the smoothed RTT, measured as ack time minus send time of the byte acked) sets `window = clamp(2 × bytes acked in that RTT, FLOOR, CEILING)`; until two samples exist, `window = INITIAL`. An ack larger than bytes sent is clamped to bytes sent; an ack lower than the previous one is ignored.

- [ ] **Step 1: Write the failing tests**

```rust
// staged.rs
#[test] fn cursor_slice_preview_then_final() { /* len 10, update(5, 0) -> (5,Preview),(5,Final) */ }
#[test] fn previews_ahead_before_finals() { /* preview_ahead 3, velocity +10 -> after cursor: (6,P),(7,P),(8,P) then (6,F) */ }
#[test] fn single_stage_slice_sent_once_as_final() { /* set_single_stage(5); update(5,0) -> (5,Final) only, never (5,Preview) */ }
#[test] fn evicted_resets_both_stages() { /* drain all; mark_unsent(2); update(2,0) -> (2,P),(2,F) */ }
#[test] fn every_stage_handed_out_exactly_once() { /* random cursor moves; collect all pops; each (i,P) and (i,F) once */ }
// flow.rs
#[test] fn window_blocks_without_acks() { /* on_sent(262_144) -> room()==0 */ }
#[test] fn over_ack_is_clamped() { /* sent 1000, on_ack(5000) -> unacked()==0, no panic */ }
#[test] fn window_grows_with_fast_acks() { /* simulated 100 Mbit/s, 80 ms -> window >= 1_000_000 */ }
#[test] fn window_floor_on_slow_acks() { /* 10 kbit/s -> window == 131_072 */ }
#[test] fn stale_ack_ignored() { /* on_ack(500) after on_ack(800) keeps 800 */ }
// protocol.rs
#[test] fn part_round_trip() { /* encode_part -> decode_frame gives header.part == Some(..), payload equal */ }
#[test] fn whole_frame_header_has_no_part_field() { /* encode_frame JSON lacks "part" (byte-identical to before) */ }
#[test] fn ack_message_parses() { /* {"type":"ack","bytes":42} -> ClientMessage::Ack{bytes:42} */ }
```

- [ ] **Step 2: Run them to verify they fail** — `cargo test -p slicestream-core` → FAIL.
- [ ] **Step 3: Implement** as above. Core stays I/O-free and WASM-checkable: `cargo check -p slicestream-core --target wasm32-unknown-unknown` must still pass.
- [ ] **Step 4: Run** `cargo test -p slicestream-core && cargo check -p slicestream-core --target wasm32-unknown-unknown` → PASS.
- [ ] **Step 5: Commit** — `feat(core): staged scheduler, flow window and frame parts`

---

### Task 7: Server: `[htj2k]` config, worker task, slice instance ids

**Files:**
- Modify: `crates/slicestream-server/src/config.rs`, `src/lib.rs`, `src/catalog.rs`, `crates/slicestream-store/src/index.rs` (if `SliceRow.id` not done in Task 4), `slicestream.example.toml`
- Test: unit tests in `config.rs`; `crates/slicestream-server/tests/htj2k_worker.rs`

**Interfaces:**
- Consumes: `Ingestor::copy_batch` (Task 5), `SliceRef::with_instance` (Task 6).
- Produces:
  - `pub enum Htj2kMode { Background, OnDemand, Off }` (`serde(rename_all = "kebab-case")`: `"background"`, `"on-demand"`, `"off"`)
  - `pub struct Htj2kConfig { pub mode: Htj2kMode, pub workers: usize, pub preview_size: u32, pub preview_ahead: usize, pub prefer_for_wildcard: bool }` with `serde(default, deny_unknown_fields)` and `fn worker_count(&self) -> usize` (0 → half of `std::thread::available_parallelism`, at least 1).
  - `Config.htj2k: Htj2kConfig`; `AppState.htj2k: Htj2kConfig`.
  - Worker task `copy_worker(ingestor, shutdown, retry, parallel)` in `lib.rs`, modelled on `backfill_metadata`: batches of 64, sleep 5 s after a failed batch, idle wait 10 s when nothing to do, stops at shutdown within the existing grace period. Spawned only in `Background` mode.
  - Catalog: every `SliceRef` built with `.with_instance(r.id)`.

- [ ] **Step 1: Failing tests**

```rust
#[test] fn htj2k_defaults() { /* empty [htj2k] -> Background, 0, 256, 32, false */ }
#[test] fn htj2k_mode_values() { /* "on-demand" parses; "sometimes" is an error naming the field */ }
#[test] fn worker_count_zero_means_half_cores() { /* >= 1 */ }
#[test] fn validate_rejects_zero_preview_size() { /* preview_size = 0 -> Config::validate Err */ }
#[tokio::test] async fn worker_makes_copies_in_background() { /* start server with phantom; poll copy_frames until Some within 10 s */ }
#[tokio::test] async fn off_mode_makes_no_copies() { /* mode = off; after a batch interval, copy_frames None */ }
```

- [ ] **Step 2: Run** `cargo test -p slicestream-server htj2k` → FAIL.
- [ ] **Step 3: Implement** as specified. Document every key in `slicestream.example.toml` with the defaults from Global Constraints.
- [ ] **Step 4: Run** `cargo test -p slicestream-server` → PASS.
- [ ] **Step 5: Commit** — `feat(server): [htj2k] config and background copy worker`

---

### Task 8: Server: progressive stream and send-queue control

**Files:**
- Create: `crates/slicestream-server/src/progressive.rs`
- Modify: `crates/slicestream-server/src/stream.rs`, `src/lib.rs` (`no_delay` also sets `TCP_NOTSENT_LOWAT`), `crates/slicestream-server/Cargo.toml` (`socket2` direct dependency, version already in the lockfile)
- Test: `crates/slicestream-server/tests/progressive.rs`

**Interfaces:**
- Consumes: `StagedScheduler`, `FlowWindow`, `encode_part`, `ClientMessage::Ack` (Task 6); `Ingestor::copy_frames`, `encode_transient`, `prioritize_copy` (Task 5); `preview_level` (Task 2); `AppState.htj2k` (Task 7).
- Produces: `GET /api/series/{uid}/stream?progressive=htj2k` behaviour; `pub(crate) async fn run_progressive(socket, series, state)`.

Behaviour:
- Query parsed with axum `Query<StreamParams { progressive: Option<String> }>`. `Some("htj2k")` and `mode != Off` → progressive. Any other value → 400 before upgrade. Absent → today's `run`, unchanged.
- Hello: same JSON as today plus `"progressive": "htj2k"`.
- Per slice, on first need: `copy_frames(id, blob)`; if `None` and the slice is eligible, `encode_transient(id, &[frame + 1])` (this also prioritizes the copy); if neither, `set_single_stage(index)` and send the stored frame whole as today (no `part`). Cache per-connection: the copy's `CopyFrame` (offset, len, ends) and path, or the transient codestream (dropped once the slice's final stage is sent).
- `Preview` stage: bytes `[0, ends[preview_level])`, one part, `level = preview_level`. `Final` stage: bytes `[ends[preview_level], total)` cut into parts of at most 65 536 bytes; each part's `level` is the highest `r` with `ends[r] <= offset + len` of that part. Read copy bytes with positioned reads (`read_frame_at` with `FrameLocation::Bytes`). `info.transferSyntax` in part headers is `1.2.840.10008.1.2.4.202`.
- A `Final` stage is sent part by part. The loop applies pending control messages between parts. When a `cursor` message arrives in the middle of a slice's `Final`, the server keeps that slice's `next_offset`, calls `requeue_final(index)`, and goes back to `pop_next`; when the slice's `Final` comes up again, sending resumes at `next_offset`. An `evicted` slice drops its `next_offset` (starts over).
- Flow: before each part, while `window.room() < part_len.min(window.window())`, await the next control message (ack or cursor). `on_sent` after each binary message (payload bytes only, not header). Acks: `on_ack`.
- `no_delay` in `lib.rs`: after `set_nodelay`, on `cfg(any(target_os = "linux", target_os = "android"))` call `socket2::SockRef::from(&tcp).set_tcp_notsent_lowat(131_072)`; log at debug on error.

- [ ] **Step 1: Failing tests** (`tests/progressive.rs`, WebSocket client as in existing `tests/e2e.rs`)

```rust
#[tokio::test] async fn plain_stream_unchanged() { /* without ?progressive: every binary message equals protocol::encode_frame of load_frame; no "part" */ }
#[tokio::test] async fn progressive_first_message_is_cursor_preview() {
    // copies made first (copy_batch); cursor {index: 3}; first binary after hello: index 3, part.offset 0, part.level == preview_level
}
#[tokio::test] async fn progressive_parts_reassemble_to_copy() { /* collect all parts per index, ack as we go; bytes == copy codestream; decodes to original samples */ }
#[tokio::test] async fn progressive_parts_at_most_64k() { /* every payload <= 65_536 */ }
#[tokio::test] async fn progressive_without_copy_uses_transient_encode() { /* no worker run; still parts; afterwards copy row exists after one copy_batch thanks to prioritize */ }
#[tokio::test] async fn ineligible_slices_sent_whole() { /* RGB or RLE series: messages have no part, payload as stored */ }
#[tokio::test] async fn progressive_stream_stops_after_window_without_acks() { /* never ack: total payload received <= 262_144 + 65_536, then silence for 500 ms */ }
#[tokio::test] async fn cursor_jump_served_within_one_window() { /* ack slowly; jump cursor; the jumped slice's preview arrives before more than one window of other bytes */ }
#[tokio::test] async fn unknown_progressive_value_is_400() { /* ?progressive=jpeg -> 400 */ }
#[tokio::test] async fn off_mode_ignores_progressive() { /* mode off: ?progressive=htj2k -> 400 naming the mode */ }
```

- [ ] **Step 2: Run** `cargo test -p slicestream-server --test progressive` → FAIL.
- [ ] **Step 3: Implement** as specified.
- [ ] **Step 4: Run** `cargo test -p slicestream-server` → PASS; `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [ ] **Step 5: Commit** — `feat(server): progressive HTJ2K stream with acknowledgement window`

---

### Task 9: DICOMweb: HTJ2K frames and `Range`

**Files:**
- Create: `crates/slicestream-dicomweb/src/range.rs`
- Modify: `crates/slicestream-dicomweb/src/accept.rs`, `src/lib.rs`, `crates/slicestream-server/src/dicomweb/frames.rs`, `crates/slicestream-server/src/lib.rs` (CORS `expose_headers` adds `Content-Range`, `Accept-Ranges`)
- Test: unit tests in `accept.rs` and `range.rs`; `crates/slicestream-server/tests/frames_htj2k.rs`

**Interfaces:**
- Consumes: `copy_frames`, `encode_transient` (Task 5), `HTJ2K_RPCL`, `eligibility` (Task 2), `AppState.htj2k` (Task 7).
- Produces:
  - `FrameSource` gains `pub copy_possible: bool` (eligible and `mode != Off`) and `pub prefer_copy_for_wildcard: bool`.
  - `PartPlan` gains `pub use_copy: bool`.
  - Negotiation rules (in preference order, per media range): `image/jphc` with no `transfer-syntax`, or with `.201` or `.202` → copy; `application/octet-stream; transfer-syntax=1.2.840.10008.1.2.4.202` → copy; `transfer-syntax=*` / `*/*` / no `Accept` → copy only when `prefer_copy_for_wildcard`; everything else exactly as today. Copy part `Content-Type`: `image/jphc; transfer-syntax=1.2.840.10008.1.2.4.202` or `application/octet-stream; transfer-syntax=1.2.840.10008.1.2.4.202` (matching the requested media type). Stored-as-HTJ2K instances keep today's as-stored rules.
  - `pub enum ByteRange { FromTo(u64, u64), From(u64), Suffix(u64) }`, `pub fn parse_range(header: &str) -> Option<Vec<ByteRange>>` (`None` when malformed or not `bytes=`), `pub fn resolve(r: &ByteRange, len: u64) -> Option<(u64, u64)>` (inclusive start..exclusive end; `None` = unsatisfiable).
- Frames handler: when `plan.use_copy`, frames come from the copy (`copy_frames`) or, if missing, `encode_transient` for exactly the requested numbers. Single frame + exactly one satisfiable range → 206, `Content-Range: bytes a-b/<frame length>`, body = that slice of the codestream in the negotiated form (single-part body, or one multipart part). Single frame + one unsatisfiable range → 416 with `Content-Range: bytes */<frame length>`. Several ranges, a malformed `Range`, or several frames → `Range` ignored, 200. `Range` on a non-copy answer → ignored (today's behaviour). `Accept-Ranges: bytes` on every copy answer.

- [ ] **Step 1: Failing tests**

```rust
// accept.rs
#[test] fn jphc_selects_copy() { /* "multipart/related; type=\"image/jphc\"" on native LE with copy_possible -> use_copy, part type image/jphc; transfer-syntax=1.2.840.10008.1.2.4.202 */ }
#[test] fn octet_202_selects_copy() {}
#[test] fn wildcard_keeps_stored_by_default() { /* OHIF header -> use_copy false, as today */ }
#[test] fn wildcard_prefers_copy_when_configured() {}
#[test] fn jphc_without_copy_is_406() { /* copy_possible false (RGB) -> NotAcceptable */ }
// range.rs
#[test] fn range_forms() {
    // "bytes=0-99" -> FromTo(0,99); "bytes=100-" -> From(100); "bytes=-500" -> Suffix(500)
    // "bytes=5-2" -> None; "items=0-1" -> None; "bytes=0-1,4-5" -> two ranges
    // resolve(From(1000), 500) -> None; resolve(Suffix(900), 500) -> (0,500); resolve(FromTo(0,10_000), 500) -> (0,500)
}
// tests/frames_htj2k.rs
#[tokio::test] async fn jphc_frames_decode_to_original() { /* GET frames/1 Accept image/jphc multipart -> part decodes (codec helper) == samples of stored frame */ }
#[tokio::test] async fn range_206_with_content_range() { /* Range bytes=0-1023 single frame -> 206, Content-Range bytes 0-1023/<len>, 1024 codestream bytes inside the part */ }
#[tokio::test] async fn range_416() { /* bytes=<len>- -> 416, Content-Range bytes * /<len> */ }
#[tokio::test] async fn several_ranges_ignored() { /* 200, whole frame */ }
#[tokio::test] async fn range_on_frame_list_ignored() { /* frames/1,2 + Range -> 200 */ }
#[tokio::test] async fn missing_copy_is_encoded_on_demand() { /* no worker; jphc request works; no row written; prioritized */ }
#[tokio::test] async fn cors_exposes_content_range() { /* Origin allowed -> Access-Control-Expose-Headers contains Content-Range */ }
// plus: tests/frames.rs and tests/wado.rs from M1 pass with no edits (default config)
```

- [ ] **Step 2: Run** `cargo test -p slicestream-dicomweb && cargo test -p slicestream-server --test frames_htj2k` → FAIL.
- [ ] **Step 3: Implement** as specified.
- [ ] **Step 4: Run** `cargo test --workspace` → PASS.
- [ ] **Step 5: Commit** — `feat(dicomweb): HTJ2K frames from copies, with HTTP Range`

---

### Task 10: Documentation

**Files:**
- Modify: `docs/PROTOCOL.md`, `docs/CONFORMANCE.md`, `docs/ARCHITECTURE.md`, `README.md`, `slicestream.example.toml` (if Task 7 left gaps), `SECURITY.md` (one line: copies hold the same patient data as originals)

- [ ] **Step 1: PROTOCOL.md** — new section "Progressive mode (`?progressive=htj2k`)": the parameter, hello field, `part` object (offset, total, level; parts in order; completion rule), `ack` message (semantics, cadence, the window and its bounds, what happens without acks), slices without a copy sent whole. Magic stays `SLS1`.
- [ ] **Step 2: CONFORMANCE.md** — replace "The server does not transcode" with the copy rules: eligibility list, transfer syntax `.202`, lossless verification, Accept rules table rows for `image/jphc` and `octet-stream; transfer-syntax=.202`, `prefer_for_wildcard`, `Range` behaviour (206, 416, ignored cases), `Accept-Ranges`.
- [ ] **Step 3: ARCHITECTURE.md** — M4 entry: "part 1 done: lossless progressive HTJ2K copies, progressive stream, DICOMweb Range"; Known limits: remove "no HTTP Range requests" and "Frames go out as fast as TCP accepts them" for progressive mode; add the copy disk cost and the `openjph-core` vendoring note.
- [ ] **Step 4: README.md** — `[htj2k]` config summary; second OHIF data source entry "slow links" (exact keys decided in Task 11 Step 3; leave a clearly marked placeholder line only if Task 11 has not run yet, and fill it there).
- [ ] **Step 5: Commit** — `docs: progressive HTJ2K delivery`

---

### Task 11: Slow-link benchmark and OHIF acceptance

**Files:**
- Create: `scripts/slowlink-proxy.mjs`, `scripts/slowlink-client.mjs`, `scripts/bench-slowlink.sh`
- Modify: `README.md` (OHIF "slow links" entry), `docs/ARCHITECTURE.md` (benchmark results line)

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Proxy** `scripts/slowlink-proxy.mjs --listen <port> --target <host:port> --mbit 10 --rtt-ms 80 --buffer 4194304`: plain Node TCP proxy (`net`), both directions; each direction delays chunks by `rtt/2` and releases bytes at the given rate; reads from the upstream side keep going while its in-memory queue is below `--buffer` (so it behaves like a reverse proxy with a large send buffer). No dependencies.
- [ ] **Step 2: Client** `scripts/slowlink-client.mjs <ws-url> [--progressive] [--jump <index>@<ms>]`: opens the stream through the proxy, sends `cursor` for the middle slice, acks per the protocol in progressive mode, prints JSON `{ "firstImageMs", "allLosslessMs", "jumpPreviewMs"? }`. First image = first complete preview part (progressive) or first whole frame (plain).
- [ ] **Step 3: OHIF** — run OHIF v3.13.14 in Docker (as in M1 Task 12, port 3000, slicestream on 18080 through the proxy on 18081) with a "slow links" data source: Accept for frames requesting `image/jphc` (or `transfer-syntax=1.2.840.10008.1.2.4.202`) and Cornerstone progressive retrieve (byte range or streaming stages). Find the exact OHIF keys from OHIF/Cornerstone source for v3.13.14; record them in the README entry. Accept when, with Playwright, the phantom and the TCIA series open with no console errors, a preview shows first, and the final pixels equal the originals (compare a decoded frame's pixel array from `cornerstone.cache` with the server's stored samples). Screenshots of phantom only may go to `docs/images/`; TCIA screenshots are never committed.
- [ ] **Step 4: Benchmark** `scripts/bench-slowlink.sh <series-dir>`: starts the server, ingests, runs `copy_batch` to completion (waits for the worker), starts the proxy, runs the client plain and progressive, and prints a table. Run it on the phantom (committed numbers) and on the TCIA series (numbers only, in the final report). Targets from the spec: progressive first image < 1 s; all lossless < 40 % of plain; jump preview < 300 ms; OHIF "slow links" first image sooner than OHIF M1 entry (timed with Playwright on the same proxy).
- [ ] **Step 5: Record and commit** — results line in ARCHITECTURE.md (phantom numbers, machine description, link settings). Commit — `test: slow-link benchmark and OHIF progressive acceptance`.
