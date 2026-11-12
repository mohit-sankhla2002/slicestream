#!/usr/bin/env bash
# Interoperability check: DCMTK storescu/echoscu against slicestream-server.
set -euo pipefail

HTTP_PORT="${HTTP_PORT:-18080}"
DIMSE_PORT="${DIMSE_PORT:-11112}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER="$ROOT_DIR/target/debug/slicestream-server"

for tool in storescu echoscu curl; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "error: required tool '$tool' not found (install dcmtk and curl)" >&2
    exit 2
  }
done

cd "$ROOT_DIR"
cargo build -q -p slicestream-server

TMP="$(mktemp -d)"
SERVER_PID=""
cleanup() {
  if [ -n "$SERVER_PID" ]; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  rm -rf "$TMP"
}
trap cleanup EXIT

echo "== 1. generate 20 phantom slices"
mkdir -p "$TMP/phantom"
cargo run -q -p slicestream-core --example make_phantom -- "$TMP/phantom" 20
mapfile -t FILES < <(ls "$TMP"/phantom/*.dcm | sort)
[ "${#FILES[@]}" -eq 20 ] || { echo "expected 20 files, got ${#FILES[@]}" >&2; exit 1; }

echo "== 2. start server"
cat > "$TMP/config.toml" <<CFG
[storage]
root = "$TMP/storage"

[http]
addr = "127.0.0.1:$HTTP_PORT"

[dimse]
addr = "127.0.0.1:$DIMSE_PORT"
ae_title = "SLICESTREAM"
CFG
"$SERVER" --config "$TMP/config.toml" serve >"$TMP/server.log" 2>&1 &
SERVER_PID=$!
up=0
for _ in $(seq 1 100); do
  if curl -sf "http://127.0.0.1:$HTTP_PORT/api/series" >/dev/null 2>&1; then up=1; break; fi
  kill -0 "$SERVER_PID" 2>/dev/null || { echo "server exited early" >&2; cat "$TMP/server.log" >&2; exit 1; }
  sleep 0.1
done
[ "$up" -eq 1 ] || { echo "server did not become ready in 10 s" >&2; cat "$TMP/server.log" >&2; exit 1; }

echo "== 3. C-ECHO"
echoscu -aec SLICESTREAM 127.0.0.1 "$DIMSE_PORT"

echo "== 4. C-STORE first 10 (default transfer syntaxes)"
storescu -aec SLICESTREAM 127.0.0.1 "$DIMSE_PORT" "${FILES[@]:0:10}"

echo "== 5. C-STORE next 10 (-xi, implicit VR LE only)"
storescu -aec SLICESTREAM -xi 127.0.0.1 "$DIMSE_PORT" "${FILES[@]:10:10}"

echo "== 6. duplicate via HTTP"
resp="$(curl -sf --data-binary @"${FILES[0]}" -H 'Content-Type: application/dicom' "http://127.0.0.1:$HTTP_PORT/api/instances")"
echo "$resp"
echo "$resp" | grep -q '"status":"duplicate"' || { echo "expected duplicate status" >&2; exit 1; }

echo "== 7. series slice count"
series="$(curl -sf "http://127.0.0.1:$HTTP_PORT/api/series")"
echo "$series"
echo "$series" | tr -d ' ' | grep -q '"sliceCount":20' || { echo "expected sliceCount 20" >&2; exit 1; }

echo "== 8. wrong called AE title is rejected"
set +e
storescu -aec WRONG 127.0.0.1 "$DIMSE_PORT" "${FILES[0]}" >"$TMP/wrong.log" 2>&1
rc=$?
set -e
if [ "$rc" -eq 0 ]; then echo "expected non-zero exit for wrong AE title" >&2; exit 1; fi
echo "storescu exited $rc as expected"

echo "interop OK"
