#!/usr/bin/env bash
# Interoperability check: the Python dicomweb-client against slicestream's
# DICOMweb routes (STOW-RS, QIDO-RS and WADO-RS).
set -euo pipefail

HTTP_PORT="${HTTP_PORT:-18080}"
PYTHON="${PYTHON:-python3}"
DICOMWEB_CLIENT_VERSION="0.61.2"
PYDICOM_VERSION="3.0.2"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER="$ROOT_DIR/target/debug/slicestream-server"

for tool in "$PYTHON" curl; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "error: required tool '$tool' not found (install python3 and curl)" >&2
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

echo "== 1. generate 12 phantom slices and an 8-frame RLE instance"
cargo run -q -p slicestream-core --example make_phantom -- "$TMP/phantom" 12
cargo run -q -p slicestream-core --example make_rle_multiframe -- "$TMP/rle" 8

echo "== 2. Python environment (dicomweb-client $DICOMWEB_CLIENT_VERSION, pydicom $PYDICOM_VERSION)"
"$PYTHON" -m venv "$TMP/venv"
"$TMP/venv/bin/pip" install -q --disable-pip-version-check \
  "dicomweb-client==$DICOMWEB_CLIENT_VERSION" "pydicom==$PYDICOM_VERSION"

echo "== 3. start server"
cat > "$TMP/config.toml" <<CFG
[storage]
root = "$TMP/storage"

[http]
addr = "127.0.0.1:$HTTP_PORT"
# dicomweb-client sends the Host header without the port, so BulkDataURIs
# built from Host would point at port 80.
public_url = "http://127.0.0.1:$HTTP_PORT"

[dimse]
enabled = false
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

echo "== 4. store, search and retrieve with dicomweb-client"
"$TMP/venv/bin/python" -I - "http://127.0.0.1:$HTTP_PORT" "$TMP/phantom" "$TMP/rle/rle-multiframe.dcm" <<'PY'
import glob
import sys

import pydicom
from dicomweb_client.api import DICOMwebClient
from pydicom.encaps import generate_frames

base, phantom_dir, rle_path = sys.argv[1:4]
client = DICOMwebClient(url=f"{base}/dicom-web")


def check(ok, what):
    if not ok:
        sys.exit(f"FAILED: {what}")
    print(f"ok: {what}")


def uid(result, tag):
    return result[tag]["Value"][0]


phantom = sorted(
    (pydicom.dcmread(p) for p in glob.glob(f"{phantom_dir}/*.dcm")),
    key=lambda ds: int(ds.InstanceNumber),
)
rle = pydicom.dcmread(rle_path)
p_study, p_series = phantom[0].StudyInstanceUID, phantom[0].SeriesInstanceUID
r_study, r_series = rle.StudyInstanceUID, rle.SeriesInstanceUID
n_rle = int(rle.NumberOfFrames)
rle_frames = list(generate_frames(rle.PixelData, number_of_frames=n_rle))

# STOW-RS
resp = client.store_instances(phantom + [rle])
check(
    len(resp.ReferencedSOPSequence) == len(phantom) + 1
    and "FailedSOPSequence" not in resp,
    f"STOW-RS stored {len(phantom) + 1} instances",
)

# QIDO-RS
studies = client.search_for_studies()
check(
    {uid(s, "0020000D") for s in studies} == {p_study, r_study},
    "search studies: both studies",
)
studies = client.search_for_studies(search_filters={"PatientName": "rle*"})
check(
    [uid(s, "0020000D") for s in studies] == [r_study],
    "search studies by PatientName wildcard",
)
series = client.search_for_series(search_filters={"Modality": "CT"})
check(
    {uid(s, "0020000E") for s in series} == {p_series, r_series},
    "search series by Modality",
)
series = client.search_for_series(p_study)
check([uid(s, "0020000E") for s in series] == [p_series], "search series of one study")
instances = client.search_for_instances(search_filters={"PatientName": "PHANTOM^*"})
check(
    {uid(i, "00080018") for i in instances} == {ds.SOPInstanceUID for ds in phantom},
    f"search instances by PatientName wildcard: {len(instances)}",
)
instances = client.search_for_instances(r_study, r_series)
check(
    [uid(i, "00080018") for i in instances] == [rle.SOPInstanceUID],
    "search instances of one series",
)

# WADO-RS metadata
meta = client.retrieve_series_metadata(p_study, p_series)
check(
    {uid(m, "00080018") for m in meta} == {ds.SOPInstanceUID for ds in phantom},
    f"series metadata: {len(meta)} instances",
)
first = next(m for m in meta if uid(m, "00080018") == phantom[0].SOPInstanceUID)
pixel_uri = first["7FE00010"]["BulkDataURI"]
check(pixel_uri.startswith(f"{base}/dicom-web/"), "Pixel Data has an absolute BulkDataURI")
parsed = pydicom.Dataset.from_json(first, bulk_data_uri_handler=lambda *_: b"")
check(parsed.Rows == phantom[0].Rows and parsed.PatientName == phantom[0].PatientName,
      "metadata parses with pydicom")

# WADO-RS frames
for ds in (phantom[0], phantom[-1]):
    frames = client.retrieve_instance_frames(p_study, p_series, ds.SOPInstanceUID, [1])
    check(frames == [ds.PixelData], f"frame 1 of phantom instance {ds.InstanceNumber}")
frames = client.retrieve_instance_frames(
    r_study, r_series, rle.SOPInstanceUID, [1, n_rle],
    media_types=(("application/octet-stream", "*"),),
)
check(frames == [rle_frames[0], rle_frames[-1]], f"RLE frames 1 and {n_rle}, transfer-syntax=*")
frames = client.retrieve_instance_frames(
    r_study, r_series, rle.SOPInstanceUID, list(range(1, n_rle + 1)),
    media_types=(("image/dicom-rle", "1.2.840.10008.1.2.5"),),
)
check(frames == rle_frames, f"RLE frames 1-{n_rle} as image/dicom-rle")

# WADO-RS bulk data
check(client.retrieve_bulkdata(pixel_uri) == [phantom[0].PixelData], "Pixel Data bulk data URI")

# WADO-RS instance
ds = client.retrieve_instance(r_study, r_series, rle.SOPInstanceUID)
check(
    ds.SOPInstanceUID == rle.SOPInstanceUID
    and ds.file_meta.TransferSyntaxUID == rle.file_meta.TransferSyntaxUID
    and ds.PixelData == rle.PixelData,
    "RLE instance retrieved as stored",
)
retrieved = client.retrieve_series(p_study, p_series)
check(
    sorted(d.SOPInstanceUID for d in retrieved) == sorted(d.SOPInstanceUID for d in phantom),
    f"series retrieve: {len(retrieved)} instances",
)
PY

echo "dicomweb interop OK"
