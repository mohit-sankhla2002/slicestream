// Archive upload logic without the DOM: file-type sniffing, job JSON to a
// view model, incremental entry rows. Used by archive-upload.js and the
// Node tests.
import { formatBytes } from "./format.js";
import { HTTP_HINT, shortUid } from "./upload-core.js";

// Bytes read from the start of each file to pick its route.
export const SNIFF_LEN = 1024;

const starts = (b, sig, off = 0) => b.length >= off + sig.length && sig.every((x, i) => b[off + i] === x);
const ascii = (s) => Array.from(s, (c) => c.charCodeAt(0));

const ARCHIVE_MAGICS = [
  [0x50, 0x4b, 0x03, 0x04], // ZIP
  [0x50, 0x4b, 0x05, 0x06], // empty ZIP
  [0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c], // 7z
  [...ascii("Rar!"), 0x1a, 0x07, 0x00], // RAR 4
  [...ascii("Rar!"), 0x1a, 0x07, 0x01, 0x00], // RAR 5
  [0x1f, 0x8b], // gzip: the server classifies the inner stream
];

function isNifti(b) {
  if (b.length < 4) return false;
  const dv = new DataView(b.buffer, b.byteOffset, b.byteLength);
  const le = dv.getInt32(0, true);
  const be = dv.getInt32(0, false);
  if (le === 348 || be === 348) return starts(b, ascii("n+1\0"), 344) || starts(b, ascii("ni1\0"), 344);
  if (le === 540 || be === 540) return starts(b, ascii("n+2\0"), 4);
  return false;
}

// "dicom" | "archive" | "nifti" | "other", from the first bytes of a file
// (same magic table as the server's detector).
export function sniff(bytes) {
  const b = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes ?? []);
  if (ARCHIVE_MAGICS.some((m) => starts(b, m))) return "archive";
  if (starts(b, ascii("ustar"), 257)) return "archive";
  if (starts(b, ascii("DICM"), 128)) return "dicom";
  if (isNifti(b)) return "nifti";
  return "other";
}

const n = (v) => (Number.isFinite(v) ? v : 0);

// Entries the job has finished (every entry adds to exactly one count).
export function processedCount(c) {
  if (!c) return 0;
  return (
    n(c.dicom_stored) + n(c.dicom_duplicate) + n(c.nifti_stored) + n(c.nifti_duplicate) + n(c.skipped) + n(c.failed)
  );
}

const entriesText = (k) => `${k.toLocaleString("en-US")} ${k === 1 ? "entry" : "entries"}`;

// GET /api/archives/{job} body -> what the archive row shows.
export function jobView(job) {
  const c = job?.counts ?? {};
  const status = job?.status ?? "queued";
  const done = status === "done" || status === "aborted";
  const processed = processedCount(c);
  const expanded = `${formatBytes(n(job?.bytes_expanded))} expanded`;
  let label;
  if (status === "queued") label = "queued";
  else if (done) label = `${entriesText(processed)} · ${expanded}`;
  else label = `${processed.toLocaleString("en-US")} / ? entries · ${expanded}`;
  const entries = Array.isArray(job?.entries) ? job.entries : [];
  return {
    status,
    done,
    reason: job?.reason ?? null,
    label,
    processed,
    counts: c,
    totals: {
      stored: n(c.dicom_stored),
      dup: n(c.dicom_duplicate) + n(c.nifti_duplicate),
      nifti: n(c.nifti_stored),
      skipped: n(c.skipped),
      failed: n(c.failed),
    },
    stored: n(c.dicom_stored) + n(c.nifti_stored) > 0,
    patients: Array.isArray(job?.patients) ? job.patients : [],
    nifti: job?.nifti ?? { linked: 0, unassigned: 0 },
    rows: entries.slice().reverse(),
  };
}

// Entries finished since `shown` were processed (at most `limit`, newest
// first), and the new processed count to pass next time. The server keeps
// only its newest entries, so older new ones may be gone.
export function freshEntries(job, shown, limit = Infinity) {
  const processed = processedCount(job?.counts);
  const entries = Array.isArray(job?.entries) ? job.entries : [];
  const k = Math.min(Math.max(processed - shown, 0), entries.length, limit);
  const rows = k > 0 ? entries.slice(entries.length - k).reverse() : [];
  return { rows, processed: Math.max(processed, shown) };
}

const OK_TAG = { stored: ["STORED", "t-ok"], replaced: ["REPLACED", "t-rep"], duplicate: ["DUPLICATE", "t-dup"] };

// One entry -> the cells of its row (same columns as a file row).
export function entryRow(e) {
  const nifti = e.kind === "nifti";
  let tag;
  let cls;
  let detail;
  let title = "";
  let why = false;
  if (e.outcome === "failed") {
    tag = e.code || "FAILED";
    cls = "t-bad";
    detail = e.reason || "failed";
    why = true;
  } else if (e.outcome === "skipped") {
    tag = "SKIPPED";
    cls = "t-skip";
    detail = e.reason || "skipped";
  } else {
    [tag, cls] = OK_TAG[e.outcome] ?? ["STORED", "t-ok"];
    if (nifti && e.outcome !== "duplicate") [tag, cls] = ["NIFTI", "t-nii"];
    if (nifti) detail = "NIfTI volume";
    else {
      detail = shortUid(e.sop_instance_uid) || "";
      title = e.sop_instance_uid ?? "";
    }
  }
  return { path: e.path ?? "", tag, cls, detail, title: title || detail, why, size: e.size ?? null };
}

const POST_HINT = {
  404: "archive uploads are disabled on this server",
  413: "file larger than the server's request limit",
  503: "server busy: too many archive jobs",
  507: "not enough free space on the server",
};

// A failed POST /api/archives -> { code, reason } for the row.
export function postError(status, json) {
  const code = `HTTP ${status}`;
  const err = typeof json?.error === "string" && json.error ? json.error : null;
  const reason = (status === 401 ? HTTP_HINT[code] : null) ?? err ?? POST_HINT[status] ?? "upload failed";
  return { code, reason };
}
