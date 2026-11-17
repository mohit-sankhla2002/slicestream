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
