// Upload logic without the DOM: request bodies, response classification,
// progress math. Used by upload.js and the Node tests.

// FailureReason (0008,1197) codes, wording from docs/CONFORMANCE.md.
export const FAILURE_TEXT = {
  C000: "cannot parse dataset / missing UID",
  A900: "UID mismatch",
  C409: "study UID does not match URL",
  "0122": "SOP class not a storage class",
  A700: "out of resources / too large",
};

// Reason text for failures the server reports only as an HTTP status.
export const HTTP_HINT = {
  "HTTP 400": "bad request",
  "HTTP 401": "unauthorized: bearer token missing or wrong",
  "HTTP 404": "upload route not found",
  "HTTP 413": "request too large",
  "HTTP 415": "unsupported media type",
  "HTTP 500": "server error",
  NETWORK: "network error",
};

const enc = new TextEncoder();

// One Part-10 file as a single-part multipart/related body.
export function stowBody(fileBytes, boundary) {
  return new Blob([
    enc.encode(`--${boundary}\r\nContent-Type: application/dicom\r\n\r\n`),
    fileBytes,
    enc.encode(`\r\n--${boundary}--\r\n`),
  ]);
}

export function stowContentType(boundary) {
  return `multipart/related; type="application/dicom"; boundary=${boundary}`;
}

const hex4 = (n) => n.toString(16).toUpperCase().padStart(4, "0");

const result = (status, code = null, uid = null, reason = null) => ({ status, code, uid, reason });

// First Value of a DICOM JSON attribute, or null.
function first(item, tag) {
  const v = item?.[tag]?.Value;
  return Array.isArray(v) && v.length > 0 ? v[0] : null;
}

const errorText = (json) => (typeof json?.error === "string" && json.error ? json.error : null);

// STOW-RS response (one part per request) -> row result.
export function classifyStow(httpStatus, json) {
  if (httpStatus === 200 || httpStatus === 202 || httpStatus === 409) {
    const failed = first(json, "00081198");
    if (failed) {
      const n = first(failed, "00081197");
      const code = Number.isInteger(n) ? hex4(n) : null;
      const reason = (code && FAILURE_TEXT[code]) || "rejected by server";
      return result("FAILED", code ?? `HTTP ${httpStatus}`, first(failed, "00081155"), reason);
    }
    const stored = first(json, "00081199");
    if (stored) return result("STORED", null, first(stored, "00081155"));
  }
  return result("FAILED", `HTTP ${httpStatus}`, null, errorText(json));
}

const INSTANCE_STATUS = { stored: "STORED", duplicate: "DUPLICATE", replaced: "REPLACED" };

// POST /api/instances response -> row result.
export function classifyInstance(httpStatus, json) {
  if (httpStatus === 200 && INSTANCE_STATUS[json?.status]) {
    return result(INSTANCE_STATUS[json.status], null, json.sopInstanceUid ?? null);
  }
  const s = json?.status;
  // ingest_error_response sends the DICOM status; a body-read error sends the HTTP status.
  const code = Number.isInteger(s) && s !== httpStatus ? hex4(s) : `HTTP ${httpStatus}`;
  return result("FAILED", code, null, errorText(json));
}

// Throughput and ETA for the progress line.
export function progress({ done, total, bytesDone, bytesTotal, elapsedMs }) {
  if (!(elapsedMs > 0)) return { filesPerSec: 0, bytesPerSec: 0, etaSecs: null };
  const secs = elapsedMs / 1000;
  const filesPerSec = done / secs;
  const bytesPerSec = bytesDone / secs;
  let etaSecs = null;
  if (done >= total) etaSecs = 0;
  else if (bytesPerSec > 0 && bytesTotal > 0) etaSecs = (bytesTotal - bytesDone) / bytesPerSec;
  else if (filesPerSec > 0) etaSecs = (total - done) / filesPerSec;
  return { filesPerSec, bytesPerSec, etaSecs };
}

// "…" + last 12 characters of a UID; short UIDs are returned whole.
export function shortUid(uid) {
  if (!uid) return "";
  return uid.length > 12 ? `…${uid.slice(-12)}` : uid;
}
