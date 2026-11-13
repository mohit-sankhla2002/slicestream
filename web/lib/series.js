// Sample ring and SVG path helpers for the metrics charts. Pure, no DOM.

const GAP_MS = 3000;

// Holds the last `cap` entries: samples, or null where polling had a gap.
export class SampleRing {
  constructor(cap = 300) {
    this.cap = cap;
    this.entries = [];
    this.last = null;
  }

  push(sample) {
    if (!sample) return;
    if (this.last) {
      const dt = sample.sampled_at_ms - this.last.sampled_at_ms;
      if (dt <= 0) return; // same or older sample polled twice
      if (dt > GAP_MS) this.entries.push(null);
    }
    this.entries.push(sample);
    this.last = sample;
    while (this.entries.length > this.cap) this.entries.shift();
    while (this.entries.length && this.entries[0] === null) this.entries.shift();
  }

  load(samples) {
    this.entries = [];
    this.last = null;
    for (const s of samples || []) this.push(s);
  }

  values(selector) {
    return this.entries.map((e) => {
      if (e === null) return null;
      const v = selector(e);
      return v === null || v === undefined || !Number.isFinite(v) ? null : v;
    });
  }
}

function seriesMax(values) {
  let m = 0;
  for (const v of values) if (v !== null && v > m) m = v;
  return m > 0 ? m : 1;
}

const r2 = (x) => String(Math.round(x * 100) / 100);

function points(values, width, height, max) {
  const top = max === undefined || max <= 0 ? seriesMax(values) : max;
  const step = values.length > 1 ? width / (values.length - 1) : 0;
  return values.map((v, i) => {
    if (v === null) return null;
    const y = height - (Math.min(Math.max(v, 0), top) / top) * height;
    return [r2(i * step), r2(y)];
  });
}

// SVG path `d` for a polyline; null values break the line (new `M`).
export function linePath(values, width, height, max) {
  let d = "";
  let pen = false;
  for (const p of points(values, width, height, max)) {
    if (p === null) {
      pen = false;
      continue;
    }
    d += `${d ? " " : ""}${pen ? "L" : "M"}${p[0]} ${p[1]}`;
    pen = true;
  }
  return d;
}

// Closed area under the line, one closed shape per unbroken run.
export function areaPath(values, width, height, max) {
  const runs = [];
  let run = [];
  for (const p of points(values, width, height, max)) {
    if (p === null) {
      if (run.length) runs.push(run);
      run = [];
    } else run.push(p);
  }
  if (run.length) runs.push(run);
  const h = r2(height);
  return runs
    .map((r) => `M${r[0][0]} ${h} ${r.map((p) => `L${p[0]} ${p[1]}`).join(" ")} L${r[r.length - 1][0]} ${h} Z`)
    .join(" ");
}

// Round up to 1, 2 or 5 times a power of ten (axis maximum).
export function niceMax(v) {
  if (!Number.isFinite(v) || v <= 0) return 1;
  const p = 10 ** Math.floor(Math.log10(v));
  for (const m of [1, 2, 5, 10]) if (v <= m * p * (1 + 1e-9)) return Number((m * p).toPrecision(2));
  return 10 * p;
}
