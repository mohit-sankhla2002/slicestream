// Number formatting for the console. Pure functions, no DOM.

export const DASH = "—";

const UNITS = ["B", "KB", "MB", "GB", "TB", "PB"];

const missing = (n) => n === null || n === undefined || !Number.isFinite(n);

// Binary units. Below 1 KB: integer bytes. From KB up: 3 significant digits.
export function formatBytes(n) {
  if (missing(n)) return DASH;
  if (Math.abs(n) < 1024) return `${Math.round(n)} B`;
  let v = n;
  let u = 0;
  while (Math.abs(v) >= 1024 && u < UNITS.length - 1) {
    v /= 1024;
    u += 1;
  }
  let s = toSig3(v);
  // Rounding can carry over the unit boundary (1023.9 KB -> "1024 KB").
  if (Math.abs(Number(s)) >= 1024 && u < UNITS.length - 1) {
    s = toSig3(v / 1024);
    u += 1;
  }
  return `${s} ${UNITS[u]}`;
}

function toSig3(v) {
  const a = Math.abs(v);
  let digits = a >= 100 ? 0 : a >= 10 ? 1 : 2;
  let s = v.toFixed(digits);
  // 99.96 -> "100.0": drop the extra digit after rounding up a magnitude.
  if (digits > 0 && Math.abs(Number(s)) >= 10 ** (3 - digits)) {
    digits -= 1;
    s = v.toFixed(digits);
  }
  return s;
}

export function formatRate(bytesPerSec) {
  if (missing(bytesPerSec)) return DASH;
  return `${formatBytes(bytesPerSec)}/s`;
}

export function formatDuration(secs) {
  if (missing(secs)) return DASH;
  const t = Math.max(0, Math.floor(secs));
  const h = Math.floor(t / 3600);
  const m = Math.floor((t % 3600) / 60);
  const s = t % 60;
  const pad = (x) => String(x).padStart(2, "0");
  return `${pad(h)}:${pad(m)}:${pad(s)}`;
}

export function formatNumber(n, digits = 1) {
  if (missing(n)) return DASH;
  return n.toFixed(digits);
}
