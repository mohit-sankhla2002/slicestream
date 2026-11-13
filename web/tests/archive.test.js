import { test } from "node:test";
import assert from "node:assert/strict";
import {
  entryRow,
  freshEntries,
  jobView,
  postError,
  processedCount,
  sniff,
} from "../lib/archive-core.js";

const at = (off, bytes, len) => {
  const v = new Uint8Array(len);
  v.set(bytes, off);
  return v;
};
const ascii = (s) => Array.from(s, (c) => c.charCodeAt(0));

function nifti1(bigEndian, magic = "n+1\0") {
  const v = new Uint8Array(352);
  new DataView(v.buffer).setInt32(0, 348, !bigEndian);
  v.set(ascii(magic), 344);
  return v;
}

test("sniff: archive magics", () => {
  assert.equal(sniff(new Uint8Array([0x50, 0x4b, 3, 4, 9])), "archive");
  assert.equal(sniff(new Uint8Array([0x50, 0x4b, 5, 6, 0, 0])), "archive");
  assert.equal(sniff(new Uint8Array([0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c, 0, 4])), "archive");
  assert.equal(sniff(new Uint8Array([...ascii("Rar!"), 0x1a, 7, 0, 1])), "archive");
  assert.equal(sniff(new Uint8Array([...ascii("Rar!"), 0x1a, 7, 1, 0, 1])), "archive");
  assert.equal(sniff(new Uint8Array([0x1f, 0x8b, 8, 0])), "archive"); // gzip, even .nii.gz
  assert.equal(sniff(at(257, ascii("ustar"), 512)), "archive");
});

test("sniff: DICOM and NIfTI", () => {
  assert.equal(sniff(at(128, ascii("DICM"), 256)), "dicom");
  assert.equal(sniff(nifti1(false)), "nifti");
  assert.equal(sniff(nifti1(true)), "nifti");
  assert.equal(sniff(nifti1(false, "ni1\0")), "nifti");
  const n2 = new Uint8Array(540);
  new DataView(n2.buffer).setInt32(0, 540, true);
  n2.set(ascii("n+2\0"), 4);
  assert.equal(sniff(n2), "nifti");
});

test("sniff: short, junk and half-magic is other", () => {
  assert.equal(sniff(new Uint8Array()), "other");
  assert.equal(sniff(new Uint8Array([0x50, 0x4b])), "other");
  assert.equal(sniff(new Uint8Array(ascii("hello world, plain text"))), "other");
  assert.equal(sniff(new Uint8Array(ascii("%PDF-1.7\n"))), "other");
  assert.equal(sniff(at(128, ascii("DIC"), 131)), "other");
  // NIfTI needs both sizeof_hdr and magic
  const sizeOnly = new Uint8Array(400);
  new DataView(sizeOnly.buffer).setInt32(0, 348, true);
  assert.equal(sniff(sizeOnly), "other");
  assert.equal(sniff(at(344, ascii("n+1\0"), 400)), "other");
  const n2 = new Uint8Array(600);
  new DataView(n2.buffer).setInt32(0, 540, true);
  assert.equal(sniff(n2), "other");
});

const counts = (o = {}) => ({
  dicom_stored: 0,
  dicom_duplicate: 0,
  nifti_stored: 0,
  nifti_duplicate: 0,
  skipped: 0,
  failed: 0,
  ...o,
});

const entry = (path, kind, outcome, extra = {}) => ({
  path,
  kind,
  outcome,
  code: null,
  reason: null,
  sop_instance_uid: null,
  size: 10,
  ...extra,
});

const job = (o = {}) => ({
  job: "ab",
  upload_id: 3,
  name: "scan.zip",
  status: "running",
  reason: null,
  counts: counts(),
  bytes_expanded: 0,
  entries_seen: 0,
  entries: [],
  patients: [],
  nifti: { linked: 0, unassigned: 0 },
  ...o,
});

test("processedCount sums every outcome", () => {
  assert.equal(processedCount(counts({ dicom_stored: 3, nifti_duplicate: 1, skipped: 2, failed: 1 })), 7);
  assert.equal(processedCount(undefined), 0);
});

test("jobView: running with unknown total", () => {
  const v = jobView(
    job({
      counts: counts({ dicom_stored: 800, skipped: 12 }),
      bytes_expanded: 1.2 * 1024 ** 3,
      entries_seen: 900,
      entries: [entry("a.dcm", "dicom", "stored"), entry("b.txt", "other", "skipped")],
    }),
  );
  assert.equal(v.status, "running");
  assert.equal(v.done, false);
  assert.equal(v.label, "812 / ? entries · 1.20 GB expanded");
  assert.deepEqual(v.totals, { stored: 800, dup: 0, nifti: 0, skipped: 12, failed: 0 });
  assert.deepEqual(
    v.rows.map((r) => r.path),
    ["b.txt", "a.dcm"],
  );
  assert.equal(v.stored, true);
});

test("jobView: queued", () => {
  const v = jobView(job({ status: "queued" }));
  assert.equal(v.done, false);
  assert.equal(v.label, "queued");
  assert.equal(v.stored, false);
});

test("jobView: done with patients and NIfTI", () => {
  const patients = [
    {
      patient_id: "P1",
      patient_name: "DOE^JANE",
      studies: [{ study_uid: "1.2", study_date: "20240102", description: "CT", series: 2, instances: 40 }],
    },
  ];
  const v = jobView(
    job({
      status: "done",
      counts: counts({ dicom_stored: 40, dicom_duplicate: 2, nifti_stored: 1, nifti_duplicate: 1 }),
      bytes_expanded: 2048,
      patients,
      nifti: { linked: 1, unassigned: 1 },
    }),
  );
  assert.equal(v.done, true);
  assert.equal(v.label, "44 entries · 2.00 KB expanded");
  assert.deepEqual(v.totals, { stored: 40, dup: 3, nifti: 1, skipped: 0, failed: 0 });
  assert.equal(v.patients, patients);
  assert.deepEqual(v.nifti, { linked: 1, unassigned: 1 });
  assert.equal(v.stored, true);
});

test("jobView: aborted keeps the reason", () => {
  const v = jobView(
    job({ status: "aborted", reason: "too many entries (max_entries)", counts: counts({ failed: 1 }) }),
  );
  assert.equal(v.done, true);
  assert.equal(v.status, "aborted");
  assert.equal(v.reason, "too many entries (max_entries)");
  assert.equal(v.label, "1 entry · 0 B expanded");
  assert.equal(v.stored, false);
});

test("jobView tolerates missing fields", () => {
  const v = jobView({ status: "running" });
  assert.equal(v.label, "0 / ? entries · 0 B expanded");
  assert.deepEqual(v.rows, []);
  assert.deepEqual(v.patients, []);
});

test("freshEntries returns only entries added since the last poll, newest first", () => {
  const e = (i) => entry(`f${i}`, "dicom", "stored");
  const j1 = job({ counts: counts({ dicom_stored: 2 }), entries: [e(1), e(2)] });
  const a = freshEntries(j1, 0);
  assert.deepEqual(a.rows.map((r) => r.path), ["f2", "f1"]);
  assert.equal(a.processed, 2);

  const j2 = job({ counts: counts({ dicom_stored: 5 }), entries: [e(1), e(2), e(3), e(4), e(5)] });
  const b = freshEntries(j2, a.processed);
  assert.deepEqual(b.rows.map((r) => r.path), ["f5", "f4", "f3"]);
  assert.equal(b.processed, 5);

  assert.deepEqual(freshEntries(j2, 5).rows, []);
});

test("freshEntries: more new entries than the server keeps", () => {
  // The server keeps the newest 3 of 10.
  const j = job({ counts: counts({ skipped: 10 }), entries: [8, 9, 10].map((i) => entry(`f${i}`, "other", "skipped")) });
  const r = freshEntries(j, 2);
  assert.deepEqual(r.rows.map((x) => x.path), ["f10", "f9", "f8"]);
  assert.equal(r.processed, 10);
});

test("freshEntries: limit keeps the newest", () => {
  const j = job({ counts: counts({ skipped: 4 }), entries: [1, 2, 3, 4].map((i) => entry(`f${i}`, "other", "skipped")) });
  assert.deepEqual(freshEntries(j, 0, 2).rows.map((x) => x.path), ["f4", "f3"]);
});

test("entryRow: tags and detail per outcome", () => {
  const sop = "1.2.826.0.1.3680043.8.498.10000000000000000000000000041";
  const stored = entryRow(entry("p/a.dcm", "dicom", "stored", { sop_instance_uid: sop, size: 512 }));
  assert.deepEqual(
    { tag: stored.tag, cls: stored.cls, detail: stored.detail, title: stored.title, why: stored.why, size: stored.size },
    { tag: "STORED", cls: "t-ok", detail: "…000000000041", title: sop, why: false, size: 512 },
  );
  assert.equal(entryRow(entry("a", "dicom", "duplicate")).tag, "DUPLICATE");
  assert.equal(entryRow(entry("a", "dicom", "duplicate")).cls, "t-dup");
  assert.equal(entryRow(entry("a", "dicom", "replaced")).cls, "t-rep");

  const nii = entryRow(entry("p/brain.nii.gz", "nifti", "stored"));
  assert.equal(nii.tag, "NIFTI");
  assert.equal(nii.cls, "t-nii");
  assert.equal(nii.detail, "NIfTI volume");
  const niiDup = entryRow(entry("p/brain.nii.gz", "nifti", "duplicate"));
  assert.equal(niiDup.tag, "DUPLICATE");
  assert.equal(niiDup.detail, "NIfTI volume");

  const skip = entryRow(entry("p/readme.txt", "other", "skipped", { reason: "not DICOM or NIfTI" }));
  assert.equal(skip.tag, "SKIPPED");
  assert.equal(skip.cls, "t-skip");
  assert.equal(skip.detail, "not DICOM or NIfTI");
  assert.equal(skip.why, false);

  const bad = entryRow(entry("p/x.dcm", "dicom", "failed", { code: "C000", reason: "cannot parse" }));
  assert.equal(bad.tag, "C000");
  assert.equal(bad.cls, "t-bad");
  assert.equal(bad.detail, "cannot parse");
  assert.equal(bad.why, true);
  const bad2 = entryRow(entry("p/x.zip", "archive", "failed", { reason: null }));
  assert.equal(bad2.tag, "FAILED");
  assert.equal(bad2.detail, "failed");
  assert.equal(entryRow(entry("a", "dicom", "skipped", { size: null })).size, null);
});

test("postError: reason text per HTTP status", () => {
  assert.deepEqual(postError(400, { error: "unsupported file type" }), {
    code: "HTTP 400",
    reason: "unsupported file type",
  });
  assert.equal(postError(401, null).reason, "unauthorized: bearer token missing or wrong");
  assert.equal(postError(404, null).reason, "archive uploads are disabled on this server");
  assert.equal(postError(413, null).reason, "file larger than the server's request limit");
  assert.equal(postError(503, { error: "too many archive jobs" }).reason, "too many archive jobs");
  assert.equal(postError(507, null).reason, "not enough free space on the server");
  assert.deepEqual(postError(500, { error: "storage error" }), { code: "HTTP 500", reason: "storage error" });
  assert.deepEqual(postError(418, null), { code: "HTTP 418", reason: "upload failed" });
});
