# Local patches to openjph-core 0.1.0

Vendored from crates.io (`openjph-core` 0.1.0, BSD-2-Clause) and wired in
through `[patch.crates-io]` in the workspace `Cargo.toml`. The published
package ships no LICENSE file and crates.io lists no repository for the port,
so `LICENSE` is the upstream OpenJPH one (Aous Naman; Kakadu Software Pty Ltd;
The University of New South Wales).

## 1. `Codestream::resolution_ends()`

What: after `flush`, `Codestream::resolution_ends() -> Option<Vec<usize>>`
returns, per resolution (lowest first), the byte length of the codestream
prefix (SOC, main header, SOT and SOD included) after which that resolution's
packets are complete. The last entry is the whole codestream, EOC included.
`None` before `flush`, and for more than one tile or tile-part.

Why: slicestream sends a low-resolution preview as an exact byte prefix of an
HTJ2K codestream and needs to know where each resolution ends.

How: `Tile::build_all_packets` records the buffer length after each
resolution's packets (layer 0); `Tile::encode_and_write` turns those into
absolute offsets using the output position before SOT plus 12 (SOT) plus 2
(SOD); `CodestreamLocal::flush` keeps them and adds 2 for EOC on the last
entry. Meaningful for one layer; with several layers, only layer 0's
resolution boundaries are reported.

Also: `tests/resolution_ends.rs` and its `[[test]]` entry (the manifest sets
`autotests = false`).

Upstream: to be proposed.

## 2. Packet header padding (encoder bug fix)

What: `BitBufferWrite::finalize` (src/codestream/bitbuffer_write.rs) now
emits the fewest bytes that hold the packet header's bits, like OpenJPH's
`bb_terminate`: the remaining whole bytes, then one zero-padded byte for any
leftover bits, then a 0x00 only if the last byte is 0xFF.

Why: the encoder wrote some packet headers one byte too long, and every
decoder (this crate's and OpenJPH's C++ one) then read the packet body one
byte off, so that packet's code-blocks, and the packets after it, decoded
to wrong samples. `write` only
flushes once 32 bits are pending, so `finalize` could start with up to 31
bits; it rounded them up to a multiple of 8 before flushing, but a 0xFF
emitted during that flush makes the next byte carry 7 bits, so the rounding
left a few bits over, which went into an extra byte. Whether it happens
depends on where 0xFF falls in the header, so only some images were hit.
The code-block bytes themselves were already identical to OpenJPH's.

Also: `tests/encoder_round_trip.rs` and its `[[test]]` entry (reproductions
plus a sweep over sizes, precisions, signedness, levels, block sizes and
content), and the unit test `finalize_emits_no_extra_byte_after_stuffing`.

Upstream: to be proposed (to the openjph-core port; OpenJPH's C++ is not
affected).
