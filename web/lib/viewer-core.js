// SLS1 frame decoding and grayscale rendering for the viewer (protocol in
// docs/PROTOCOL.md), plus series-row labels. Pure functions, no DOM.

import { DASH } from "./format.js";

export const NATIVE_TS = new Set(["1.2.840.10008.1.2.1", "1.2.840.10008.1.2"]);

// Default VOI when the frame carries none (same as the original demo client).
const DEFAULT_WL = { center: 40, width: 400 };

const MAGIC = [0x53, 0x4c, 0x53, 0x31]; // "SLS1"

// Binary message -> { index, info, data: ArrayBuffer } or null if not SLS1.
export function parseFrame(buf) {
  if (!(buf instanceof ArrayBuffer) || buf.byteLength < 8) return null;
  const head = new Uint8Array(buf, 0, 4);
  if (MAGIC.some((b, i) => head[i] !== b)) return null;
  const hlen = new DataView(buf).getUint32(4, true);
  if (8 + hlen > buf.byteLength) return null;
  let header;
  try {
    header = JSON.parse(new TextDecoder().decode(new Uint8Array(buf, 8, hlen)));
  } catch {
    return null;
  }
  if (!header || !Number.isInteger(header.index) || !header.info) return null;
  return { index: header.index, info: header.info, data: buf.slice(8 + hlen) };
}

// null if the viewer can draw this frame, else a short reason.
export function frameSupport(info) {
  if (!NATIVE_TS.has(info.transferSyntax)) return `unsupported transfer syntax ${info.transferSyntax ?? "?"}`;
  if ((info.samplesPerPixel ?? 1) !== 1) return "unsupported color frame (grayscale only)";
  if (info.bitsAllocated !== 8 && info.bitsAllocated !== 16) return `unsupported ${info.bitsAllocated}-bit pixels`;
  return null;
}

const usable = (v) => typeof v === "number" && Number.isFinite(v);

export function windowLevel(info) {
  const ok = usable(info.windowCenter) && usable(info.windowWidth) && info.windowWidth > 0;
  return ok ? { center: info.windowCenter, width: info.windowWidth } : { ...DEFAULT_WL };
}

// Modality LUT (slope/intercept) then a linear window into RGBA `out`
// (length rows * columns * 4). Pixels missing from a short payload are black.
export function renderGray(info, data, out) {
  const n = info.rows * info.columns;
  const bytes = info.bitsAllocated === 16 ? 2 : 1;
  const have = Math.min(n, Math.floor(data.byteLength / bytes));
  const px =
    bytes === 2
      ? info.pixelRepresentation ? new Int16Array(data, 0, have) : new Uint16Array(data, 0, have)
      : new Uint8Array(data, 0, have);
  const { center, width } = windowLevel(info);
  const slope = usable(info.rescaleSlope) ? info.rescaleSlope : 1;
  const icpt = usable(info.rescaleIntercept) ? info.rescaleIntercept : 0;
  // v = (px * slope + icpt - lo) / width * 255, folded into one multiply-add.
  const k = (slope * 255) / width;
  const b = ((icpt - (center - width / 2)) * 255) / width;
  const invert = info.photometricInterpretation === "MONOCHROME1";
  for (let i = 0; i < n; i++) {
    let v = i < have ? px[i] * k + b : invert ? 255 : 0;
    v = v < 0 ? 0 : v > 255 ? 255 : v;
    if (invert) v = 255 - v;
    const j = i * 4;
    out[j] = out[j + 1] = out[j + 2] = v;
    out[j + 3] = 255;
  }
}

// Window values for the overlay: integers as-is, else one decimal.
export function formatWL(v) {
  return Number.isInteger(v) ? String(v) : v.toFixed(1);
}

export function seriesLabel(row) {
  const desc = (row.series_description ?? "").trim();
  if (desc) return desc;
  const uid = row.series_uid ?? "";
  const tail = uid.length > 8 ? `…${uid.slice(-8)}` : uid;
  return `${row.modality || "?"} · ${tail}`;
}

// DICOM DA (YYYYMMDD) -> YYYY-MM-DD; other strings pass through.
export function formatStudyDate(s) {
  if (!s) return DASH;
  return /^\d{8}$/.test(s) ? `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6)}` : s;
}

export function clampIndex(i, total) {
  return Math.max(0, Math.min(total - 1, i));
}
