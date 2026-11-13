import { test } from "node:test";
import assert from "node:assert/strict";
import {
  FAILURE_TEXT,
  HTTP_HINT,
  classifyInstance,
  classifyStow,
  progress,
  shortUid,
  stowBody,
  stowContentType,
} from "../lib/upload-core.js";

const SOP = "1.2.826.0.1.3680043.8.498.10000000000000000000000000041";

test("stowBody wraps the bytes in one multipart/related part", async () => {
  const blob = stowBody(new Uint8Array([1, 2, 3]), "b");
  assert.ok(blob instanceof Blob);
  const bytes = new Uint8Array(await blob.arrayBuffer());
  const enc = new TextEncoder();
  const expected = new Uint8Array([
    ...enc.encode("--b\r\nContent-Type: application/dicom\r\n\r\n"),
    1, 2, 3,
    ...enc.encode("\r\n--b--\r\n"),
  ]);
  assert.deepEqual(bytes, expected);
});

test("stowContentType names the part type and boundary", () => {
  assert.equal(stowContentType("b"), 'multipart/related; type="application/dicom"; boundary=b');
});

test("classifyStow: 200 with one ReferencedSOP item is STORED", () => {
  const json = {
    "00081199": {
      vr: "SQ",
      Value: [
        {
          "00081150": { vr: "UI", Value: ["1.2.840.10008.5.1.4.1.1.2"] },
          "00081155": { vr: "UI", Value: [SOP] },
          "00081190": { vr: "UR", Value: ["http://x/dicom-web/studies/1/series/2/instances/3"] },
        },
      ],
    },
  };
  assert.deepEqual(classifyStow(200, json), { status: "STORED", code: null, uid: SOP, reason: null });
});

test("classifyStow: 409 with FailedSOP 0xC000 is FAILED C000", () => {
  const json = { "00081198": { vr: "SQ", Value: [{ "00081197": { vr: "US", Value: [49152] } }] } };
  assert.deepEqual(classifyStow(409, json), {
    status: "FAILED",
    code: "C000",
    uid: null,
    reason: FAILURE_TEXT.C000,
  });
});

test("classifyStow: FailedSOP keeps the SOP UID and pads small codes", () => {
  const json = {
    "00081198": {
      vr: "SQ",
      Value: [{ "00081155": { vr: "UI", Value: [SOP] }, "00081197": { vr: "US", Value: [0x0122] } }],
    },
  };
  assert.deepEqual(classifyStow(409, json), {
    status: "FAILED",
    code: "0122",
    uid: SOP,
    reason: FAILURE_TEXT["0122"],
  });
});

test("classifyStow: unknown failure code still reports the code", () => {
  const json = { "00081198": { vr: "SQ", Value: [{ "00081197": { vr: "US", Value: [0xb000] } }] } };
  const r = classifyStow(409, json);
  assert.equal(r.code, "B000");
  assert.equal(r.status, "FAILED");
  assert.equal(typeof r.reason, "string");
});

test("classifyStow: 401 with empty body is HTTP 401", () => {
  assert.deepEqual(classifyStow(401, null), { status: "FAILED", code: "HTTP 401", uid: null, reason: null });
});

test("classifyStow: 415 is HTTP 415", () => {
  assert.deepEqual(classifyStow(415, null), { status: "FAILED", code: "HTTP 415", uid: null, reason: null });
});

test("classifyStow: other status takes the reason from an error field", () => {
  const r = classifyStow(400, { error: "malformed multipart body" });
  assert.deepEqual(r, { status: "FAILED", code: "HTTP 400", uid: null, reason: "malformed multipart body" });
});

test("classifyStow: 200 without either sequence is a failure, not a crash", () => {
  const r = classifyStow(200, {});
  assert.equal(r.status, "FAILED");
  assert.equal(r.code, "HTTP 200");
});

const ok = (status) => ({
  status,
  sopInstanceUid: SOP,
  seriesInstanceUid: "1.2.3",
  studyInstanceUid: "1.2",
});

test("classifyInstance maps stored, duplicate and replaced", () => {
  assert.deepEqual(classifyInstance(200, ok("stored")), { status: "STORED", code: null, uid: SOP, reason: null });
  assert.deepEqual(classifyInstance(200, ok("duplicate")), { status: "DUPLICATE", code: null, uid: SOP, reason: null });
  assert.deepEqual(classifyInstance(200, ok("replaced")), { status: "REPLACED", code: null, uid: SOP, reason: null });
});

test("classifyInstance: ingest_error_response JSON gives a hex code and the message", () => {
  // stow.rs ingest_error_response: { "error": e.to_string(), "status": e.status() } with HTTP 400.
  const json = { error: "cannot parse DICOM: missing SOPInstanceUID", status: 49152 };
  assert.deepEqual(classifyInstance(400, json), {
    status: "FAILED",
    code: "C000",
    uid: null,
    reason: "cannot parse DICOM: missing SOPInstanceUID",
  });
  assert.equal(classifyInstance(413, { error: "too large", status: 0xa700 }).code, "A700");
  assert.equal(classifyInstance(500, { error: "storage error", status: 0xa700 }).code, "A700");
});

test("classifyInstance: a status equal to the HTTP status is not a DICOM code", () => {
  // post_instance body-read error: { error, status: 400 } with HTTP 400.
  const r = classifyInstance(400, { error: "reading request body: x", status: 400 });
  assert.equal(r.code, "HTTP 400");
  assert.equal(r.reason, "reading request body: x");
});

test("classifyInstance: empty error body is HTTP <status>", () => {
  assert.deepEqual(classifyInstance(415, null), { status: "FAILED", code: "HTTP 415", uid: null, reason: null });
  assert.deepEqual(classifyInstance(401, null), { status: "FAILED", code: "HTTP 401", uid: null, reason: null });
});

test("progress: rates and ETA", () => {
  const p = progress({ done: 30, total: 120, bytesDone: 15 * 1048576, bytesTotal: 60 * 1048576, elapsedMs: 2000 });
  assert.equal(p.filesPerSec, 15);
  assert.equal(p.bytesPerSec, 7.5 * 1048576);
  assert.equal(p.etaSecs, 6);
});

test("progress: zero elapsed gives zeros and no ETA", () => {
  assert.deepEqual(progress({ done: 0, total: 10, bytesDone: 0, bytesTotal: 100, elapsedMs: 0 }), {
    filesPerSec: 0,
    bytesPerSec: 0,
    etaSecs: null,
  });
});

test("progress: zero rate gives no ETA; finished gives ETA 0", () => {
  assert.equal(progress({ done: 0, total: 10, bytesDone: 0, bytesTotal: 100, elapsedMs: 500 }).etaSecs, null);
  assert.equal(progress({ done: 10, total: 10, bytesDone: 100, bytesTotal: 100, elapsedMs: 500 }).etaSecs, 0);
});

test("shortUid keeps the last 12 characters", () => {
  assert.equal(shortUid(SOP), "…000000000041");
  assert.equal(shortUid("1.2.3"), "1.2.3");
  assert.equal(shortUid(null), "");
});

test("FAILURE_TEXT covers the documented codes", () => {
  for (const code of ["C000", "A900", "C409", "0122", "A700"]) {
    assert.equal(typeof FAILURE_TEXT[code], "string");
  }
});

test("HTTP_HINT explains the HTTP-level failures the routes return", () => {
  for (const code of ["HTTP 400", "HTTP 401", "HTTP 413", "HTTP 415", "NETWORK"]) {
    assert.equal(typeof HTTP_HINT[code], "string");
  }
});
