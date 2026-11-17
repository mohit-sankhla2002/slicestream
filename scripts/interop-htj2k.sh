#!/usr/bin/env bash
# Interoperability check: Cornerstone's OpenJPH WASM decoder against the
# HTJ2K copies slicestream encodes (full decode and every resolution prefix).
set -euo pipefail

OPENJPH_VERSION="2.4.11"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

for tool in node npm; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "error: required tool '$tool' not found (install Node.js)" >&2
    exit 2
  }
done

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cd "$ROOT_DIR"
(cd "$TMP" && npm install --ignore-scripts --no-audit --no-fund "@cornerstonejs/codec-openjph@$OPENJPH_VERSION" >/dev/null)
mkdir "$TMP/fixtures"
cargo run -q -p slicestream-codec --example htj2k_fixtures -- "$TMP/fixtures"
node scripts/htj2k-oracle.cjs "$TMP/node_modules" "$TMP/fixtures"
