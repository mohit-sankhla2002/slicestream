#!/usr/bin/env bash
# Slow-link benchmark of the WebSocket stream, plain against progressive
# HTJ2K, against the targets in
# docs/superpowers/specs/2026-11-17-progressive-htj2k-design.md (Performance).
#
#   scripts/bench-slowlink.sh <series-dir>
#
# <series-dir> holds one series of DICOM files (for example the phantom:
# `cargo run -p slicestream-core --example make_phantom -- <dir> 181 512`).
# The script builds the release server, imports the folder into a new
# storage root, starts the server, waits until the copy worker has made
# every HTJ2K copy, then streams the series through a throttling proxy
# (scripts/slowlink-proxy.mjs) with scripts/slowlink-client.mjs, once as
# stored and once progressive, and prints a table.
#
# Settings (environment): HTTP_PORT (18080), PROXY_PORT (18081), MBIT (10),
# RTT_MS (80), BUFFER (4194304, the proxy's queue per direction), JUMP
# (0@1000: cursor to slice 0 at 1,000 ms). Needs curl and Node 22 or later.
set -euo pipefail

HTTP_PORT="${HTTP_PORT:-18080}"
PROXY_PORT="${PROXY_PORT:-18081}"
MBIT="${MBIT:-10}"
RTT_MS="${RTT_MS:-80}"
BUFFER="${BUFFER:-4194304}"
JUMP="${JUMP:-0@1000}"

[ $# -eq 1 ] && [ -d "$1" ] || { echo "usage: $0 <series-dir>" >&2; exit 2; }
SERIES_DIR="$(cd "$1" && pwd)"
for tool in curl node; do
  command -v "$tool" >/dev/null 2>&1 || { echo "error: required tool '$tool' not found" >&2; exit 2; }
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER="$ROOT_DIR/target/release/slicestream-server"
BASE="http://127.0.0.1:$HTTP_PORT"
cd "$ROOT_DIR"
cargo build -q --release -p slicestream-server

TMP="$(mktemp -d)"
SERVER_PID=""
PROXY_PID=""
cleanup() {
  for pid in "$PROXY_PID" "$SERVER_PID"; do
    if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  done
  rm -rf "$TMP"
}
trap cleanup EXIT

cat > "$TMP/slicestream.toml" <<CFG
[storage]
root = "$TMP/storage"

[http]
addr = "127.0.0.1:$HTTP_PORT"
# Requests arrive through the proxy's port too.
allowed_hosts = ["localhost", "127.0.0.1"]

[dimse]
enabled = false

[htj2k]
mode = "background"
CFG

echo "== import $SERIES_DIR"
"$SERVER" --config "$TMP/slicestream.toml" import "$SERIES_DIR" 2>"$TMP/import.log" | tee "$TMP/import.out" | sed 's/^/   /'
STORED="$(awk '$1 == "stored" { print $2 }' "$TMP/import.out")"
[ -n "$STORED" ] && [ "$STORED" -gt 0 ] || { echo "error: nothing was stored" >&2; exit 1; }

NO_COLOR=1 "$SERVER" --config "$TMP/slicestream.toml" serve >"$TMP/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 100); do
  if curl -sf "$BASE/api/series" >/dev/null 2>&1; then break; fi
  kill -0 "$SERVER_PID" 2>/dev/null || { cat "$TMP/server.log" >&2; exit 1; }
  sleep 0.1
done

# The copy worker logs its running total after every batch that wrote rows
# (one row per instance, also for one that cannot have a copy).
echo "== waiting for the copy worker ($STORED instances)"
t0=$SECONDS
copied=0
while [ "$copied" -lt "$STORED" ]; do
  [ $((SECONDS - t0)) -lt 1800 ] || { echo "error: copies not done after 30 min" >&2; exit 1; }
  kill -0 "$SERVER_PID" 2>/dev/null || { cat "$TMP/server.log" >&2; exit 1; }
  sleep 0.5
  copied="$(sed -n 's/.*HTJ2K copy progress copied=\([0-9]*\).*/\1/p' "$TMP/server.log" | tail -1)"
  copied="${copied:-0}"
done
echo "   $copied copies in $((SECONDS - t0)) s"

SERIES_UID="$(curl -sf "$BASE/api/series" | node -e '
  let s = ""; process.stdin.on("data", (d) => (s += d)).on("end", () => {
    const list = JSON.parse(s);
    if (list.length !== 1) { console.error(`expected one series, found ${list.length}`); process.exit(1); }
    console.log(list[0].uid);
  });')"

node scripts/slowlink-proxy.mjs --listen "$PROXY_PORT" --target "127.0.0.1:$HTTP_PORT" \
  --mbit "$MBIT" --rtt-ms "$RTT_MS" --buffer "$BUFFER" 2>"$TMP/proxy.log" &
PROXY_PID=$!
for _ in $(seq 1 50); do
  if curl -sf "http://127.0.0.1:$PROXY_PORT/api/series" >/dev/null 2>&1; then break; fi
  sleep 0.1
done

URL="ws://127.0.0.1:$PROXY_PORT/api/series/$SERIES_UID/stream"
echo "== streaming $STORED slices at $MBIT Mbit/s, $RTT_MS ms RTT, proxy buffer $BUFFER B; jump $JUMP"
PLAIN="$(node scripts/slowlink-client.mjs "$URL" --jump "$JUMP")"
PROG="$(node scripts/slowlink-client.mjs "$URL" --progressive --jump "$JUMP")"

node -e '
  const plain = JSON.parse(process.argv[1]);
  const prog = JSON.parse(process.argv[2]);
  const ms = (v) => (v === undefined ? "-" : `${Math.round(v)} ms`);
  const row = (name, a, b, target) =>
    console.log(`   ${name.padEnd(22)} ${a.padStart(10)} ${b.padStart(12)}   ${target}`);
  row("", "as stored", "progressive", "target (progressive)");
  row("first image", ms(plain.firstImageMs), ms(prog.firstImageMs), "< 1000 ms");
  row("all slices lossless", ms(plain.allLosslessMs), ms(prog.allLosslessMs),
    `< 40 % of as stored: ${((100 * prog.allLosslessMs) / plain.allLosslessMs).toFixed(1)} %`);
  row("jump preview", ms(plain.jumpPreviewMs), ms(prog.jumpPreviewMs), "< 300 ms");
' "$PLAIN" "$PROG"
