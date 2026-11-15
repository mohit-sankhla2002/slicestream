#!/usr/bin/env bash
# Benchmark the DICOMweb read path with a release build, against the targets
# in docs/superpowers/specs/2026-11-15-dicomweb-read-path-design.md
# (Performance). Numbers are over loopback with a warm page cache.
#
# Linux only: peak memory comes from /proc/<pid>/status. Needs about 2.5 GB
# of free disk under $TMPDIR (default /tmp).
set -euo pipefail

HTTP_PORT="${HTTP_PORT:-18080}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER="$ROOT_DIR/target/release/slicestream-server"
BASE="http://127.0.0.1:$HTTP_PORT"
# Fixed UIDs written by the make_phantom example.
STUDY="2.25.1000000000000000000000000000002"
SERIES="2.25.1000000000000000000000000000001"
sop() { printf '2.25.1000000000000000000000000001%05d' "$1"; }

for tool in curl awk; do
  command -v "$tool" >/dev/null 2>&1 || { echo "error: required tool '$tool' not found" >&2; exit 2; }
done
[ -r /proc/self/status ] || { echo "error: needs Linux /proc" >&2; exit 2; }

cd "$ROOT_DIR"
cargo build -q --release -p slicestream-server
cargo build -q --release -p slicestream-core --example make_phantom
PHANTOM="$ROOT_DIR/target/release/examples/make_phantom"

TMP="$(mktemp -d)"
SERVER_PID=""
stop_server() {
  if [ -n "$SERVER_PID" ]; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  SERVER_PID=""
}
cleanup() { stop_server; rm -rf "$TMP"; }
trap cleanup EXIT

# Generate `slices` 512x512 phantom slices, import them into a new storage
# root `name`, and start the server on it.
serve_phantom() {
  local name="$1" slices="$2"
  cat > "$TMP/$name.toml" <<CFG
[storage]
root = "$TMP/$name"

[http]
addr = "127.0.0.1:$HTTP_PORT"

[dimse]
enabled = false
CFG
  "$PHANTOM" "$TMP/phantom" "$slices" 512 >/dev/null
  local t0=$SECONDS
  "$SERVER" --config "$TMP/$name.toml" import "$TMP/phantom" 2>"$TMP/$name-import.log" | tail -1 | sed 's/^/   /'
  echo "   imported $slices slices in $((SECONDS - t0)) s"
  rm -rf "$TMP/phantom"
  "$SERVER" --config "$TMP/$name.toml" serve >"$TMP/$name-server.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 100); do
    if curl -sf "$BASE/api/series" >/dev/null 2>&1; then return 0; fi
    kill -0 "$SERVER_PID" 2>/dev/null || { cat "$TMP/$name-server.log" >&2; exit 1; }
    sleep 0.1
  done
  echo "server did not become ready in 10 s" >&2
  exit 1
}

# Median and 90th percentile, in ms, of seconds read one per line.
stats_ms() { sort -n | awk '{ v[NR] = $1 * 1000 } END { printf "median %.2f ms, p90 %.2f ms (n=%d)", v[int((NR + 1) / 2)], v[int(NR * 0.9 + 0.5)], NR }'; }

kib() { awk -v k="$1" 'BEGIN { printf "%.1f MiB", k / 1024 }'; }
status_kb() { awk -v f="$2:" '$1 == f { print $2 }' "/proc/$1/status"; }

echo "== 1. series metadata and frames: 1,000 slices of 512x512"
serve_phantom small 1000
META="$BASE/dicom-web/studies/$STUDY/series/$SERIES/metadata"
for enc in identity gzip; do
  for _ in 1 2; do curl -sf -o /dev/null -H "Accept-Encoding: $enc" "$META"; done
  bytes="$(curl -sf -o /dev/null -w '%{size_download}' -H "Accept-Encoding: $enc" "$META")"
  times="$(for _ in $(seq 1 20); do curl -sf -o /dev/null -w '%{time_total}\n' -H "Accept-Encoding: $enc" "$META"; done | stats_ms)"
  target=$([ "$enc" = gzip ] && echo 250 || echo 150)
  echo "   metadata, 1,000 instances, $enc: $times; $bytes bytes [target < $target ms]"
done

# 200 different instances, frame 1, on one keep-alive connection (as a
# viewer does). The first pass warms the page cache; the second is timed.
ACCEPT='Accept: multipart/related; type=application/octet-stream; transfer-syntax=*'
args=()
for z in $(seq 0 199); do
  args+=(-o /dev/null "$BASE/dicom-web/studies/$STUDY/series/$SERIES/instances/$(sop "$z")/frames/1")
done
curl -sf -H "$ACCEPT" "${args[@]}" >/dev/null
sizes="$(curl -sf -H "$ACCEPT" -w '%{size_download}\n' "${args[@]}" | sort -u | tr '\n' ' ')"
times="$(curl -sf -H "$ACCEPT" -w '%{time_total}\n' "${args[@]}" | stats_ms)"
echo "   frame 1 of 200 instances, keep-alive: $times; response bytes $sizes[target: median < 2 ms]"
stop_server
rm -rf "$TMP/small"

echo "== 2. peak memory while retrieving a 1 GB study: 2,000 slices of 512x512"
serve_phantom large 2000
sleep 1
# The first retrieve also warms SQLite's page cache and the allocator; the
# second shows what streaming itself costs. Writing 5 to clear_refs resets
# VmHWM (peak RSS) to the current RSS before each one.
for run in cold warm; do
  echo 5 > "/proc/$SERVER_PID/clear_refs"
  rss0="$(status_kb "$SERVER_PID" VmRSS)"
  out="$(curl -sf -o /dev/null -w '%{size_download} %{time_total}' \
    -H 'Accept: multipart/related; type="application/dicom"' "$BASE/dicom-web/studies/$STUDY")"
  hwm1="$(status_kb "$SERVER_PID" VmHWM)"
  read -r bytes secs <<<"$out"
  echo "   $run: $bytes bytes in $secs s; RSS before $(kib "$rss0"), peak $(kib "$hwm1") (+$(kib $((hwm1 - rss0)))) [target: within a few MiB]"
done
stop_server

echo "bench done"
