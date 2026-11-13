// Metrics panel: polls the server and renders KPIs, charts, storage and facts.
import { DASH, formatBytes, formatNumber, formatRate } from "./lib/format.js";
import { SampleRing, areaPath, linePath, niceMax } from "./lib/series.js";

const POLL_MS = 1000;
const W = 300;
const H = 70;
const MB = 1024 * 1024;
const WINDOW = 300;

async function getJson(url) {
  const res = await fetch(url, { cache: "no-store", signal: AbortSignal.timeout(3000) });
  if (!res.ok) throw new Error(`${url}: HTTP ${res.status}`);
  return res.json();
}

// Loads history once, then polls /api/metrics every second.
// onHistory(samples) gets the history array; onSample(snapshot) every poll;
// onStatus("live" | "offline") on each poll result.
export function startMetrics({ onSample, onStatus, onHistory }) {
  let stopped = false;
  let timer = 0;

  async function poll() {
    try {
      const snap = await getJson("/api/metrics");
      if (stopped) return;
      onStatus?.("live");
      onSample?.(snap);
    } catch {
      if (!stopped) onStatus?.("offline");
    }
    if (!stopped) timer = setTimeout(poll, POLL_MS);
  }

  (async () => {
    try {
      const hist = await getJson("/api/metrics/history");
      if (!stopped && Array.isArray(hist)) onHistory?.(hist);
    } catch {
      // No history (server starting or metrics off): live polling still fills the charts.
    }
    if (!stopped) poll();
  })();

  return () => {
    stopped = true;
    clearTimeout(timer);
  };
}

const $ = (id) => document.getElementById(id);

function setText(el, text) {
  if (el && el.textContent !== text) el.textContent = text;
}

// "84.1 MB" -> value with a small unit, as in the mockup KPI.
function setBytesKpi(el, n) {
  const [num, unit] = formatBytes(n).split(" ");
  el.replaceChildren(num ?? DASH);
  if (unit) {
    const s = document.createElement("small");
    s.textContent = unit;
    el.append(s);
  }
}

const count = (n) => (n === null || n === undefined ? DASH : n.toLocaleString("en-US").replace(/,/g, " "));
const toMB = (v) => (v === null || v === undefined ? null : v / MB);
const mbLabel = (v) => (v >= 10 ? formatNumber(v, 0) : v >= 1 ? formatNumber(v, 1) : formatNumber(v, 2));

function chart(id) {
  const root = $(id);
  return {
    cur: root.querySelector(".cur"),
    ymax: root.querySelector(".ymax"),
    area: root.querySelector(".area"),
    lines: [...root.querySelectorAll(".line")],
  };
}

// Left-pad with nulls so the x axis is always the full 5-minute window.
function windowed(values) {
  return values.length >= WINDOW ? values : Array(WINDOW - values.length).fill(null).concat(values);
}

function seriesTop(...arrays) {
  let m = 0;
  for (const a of arrays) for (const v of a) if (v !== null && v > m) m = v;
  return niceMax(m);
}

export function createMetricsPanel() {
  const ring = new SampleRing(WINDOW);
  const charts = { cpu: chart("c-cpu"), rss: chart("c-rss"), disk: chart("c-disk"), lat: chart("c-lat") };
  let peakRss = null;
  let firstStorage = null;
  let latest = null;

  function drawLine(c, values, top, unitLabel) {
    c.lines[0].setAttribute("d", linePath(values, W, H, top));
    c.area?.setAttribute("d", areaPath(values, W, H, top));
    setText(c.ymax, unitLabel(top));
  }

  function drawCharts() {
    const series = (fn) => windowed(ring.values(fn));
    const cpu = series((s) => s.process?.cpu_percent);
    drawLine(charts.cpu, cpu, seriesTop(cpu), (t) => `${t}%`);

    const rss = series((s) => toMB(s.process?.rss_bytes));
    drawLine(charts.rss, rss, seriesTop(rss), (t) => `${t} MB`);

    const wr = series((s) => toMB(s.process?.disk_write_bytes_per_sec));
    const rd = series((s) => toMB(s.process?.disk_read_bytes_per_sec));
    const dTop = seriesTop(wr, rd);
    const [rdLine, wrLine] = charts.disk.lines; // read drawn first (under write)
    wrLine.setAttribute("d", linePath(wr, W, H, dTop));
    rdLine.setAttribute("d", linePath(rd, W, H, dTop));
    setText(charts.disk.ymax, `${dTop} MB/s`);

    const p50 = series((s) => s.ingest?.latency_ms?.p50);
    const p95 = series((s) => s.ingest?.latency_ms?.p95);
    const lTop = seriesTop(p50, p95);
    charts.lat.lines[0].setAttribute("d", linePath(p50, W, H, lTop));
    charts.lat.lines[1].setAttribute("d", linePath(p95, W, H, lTop));
    setText(charts.lat.ymax, `${lTop} ms`);
  }

  function renderNumbers(s) {
    const p = s.process ?? {};
    const st = s.storage ?? {};
    const ing = s.ingest ?? {};
    const con = s.connections ?? {};
    const lat = ing.latency_ms ?? {};

    // KPIs
    setText($("k-cpu"), formatNumber(p.cpu_percent));
    setText($("k-cpu-s"), p.cores ? `of 1 core · ${p.cores} cores` : "of 1 core");
    setBytesKpi($("k-rss"), p.rss_bytes);
    setText($("k-rss-s"), `peak ${formatBytes(peakRss)}`);
    const total = storageTotal(st);
    setBytesKpi($("k-sto"), total);
    if (total !== null && firstStorage !== null) {
      const d = total - firstStorage;
      setText($("k-sto-s"), `${d < 0 ? "−" : "+"}${formatBytes(Math.abs(d))} this session`);
    } else setText($("k-sto-s"), DASH);
    setText($("k-ing"), ing.per_sec === null || ing.per_sec === undefined ? DASH : formatNumber(ing.per_sec, ing.per_sec >= 10 ? 0 : 1));
    setText($("k-ing-s"), `p95 ${lat.p95 == null ? DASH : `${formatNumber(lat.p95, 0)} ms`} · ${formatRate(ing.bytes_per_sec)}`);

    // chart headers
    setText(charts.cpu.cur, formatNumber(p.cpu_percent));
    const rssMb = toMB(p.rss_bytes);
    setText(charts.rss.cur, rssMb === null ? DASH : formatNumber(rssMb));
    const w = toMB(p.disk_write_bytes_per_sec);
    const r = toMB(p.disk_read_bytes_per_sec);
    setText(charts.disk.cur, `${w === null ? DASH : mbLabel(w)} / ${r === null ? DASH : mbLabel(r)}`);
    setText(charts.lat.cur, `p50 ${formatNumber(lat.p50, 0)} · p95 ${formatNumber(lat.p95, 0)}`);

    // storage bar
    if (st.root) setText($("d-root"), `Storage root · ${st.root}`);
    if (st.root) $("d-root").title = st.root;
    setText($("d-free"), `${formatBytes(total)} used · ${formatBytes(st.volume_free_bytes)} free on volume`);
    const parts = [st.objects_bytes, st.index_bytes, st.tmp_bytes];
    const bars = $("d-bar").children;
    parts.forEach((b, i) => {
      bars[i].style.width = total ? `${(Math.max(b ?? 0, 0) / total) * 100}%` : "0";
    });
    setText($("d-obj"), `objects ${formatBytes(st.objects_bytes)} · ${count(st.objects_count)} blobs`);
    setText($("d-idx"), `index.sqlite + WAL ${formatBytes(st.index_bytes)}`);
    setText($("d-tmp"), `tmp ${formatBytes(st.tmp_bytes)}`);

    // facts
    setText($("f-threads"), count(p.threads));
    setText($("f-fds"), count(p.open_fds));
    setText($("f-virt"), formatBytes(p.virtual_bytes));
    const dimseOff = con.dimse_active == null || con.dimse_max == null;
    setText($("f-dimse"), dimseOff ? "off" : `${con.dimse_active} / ${con.dimse_max}`);
    $("f-dimse").classList.toggle("off", dimseOff);
    setText($("f-ws"), count(con.ws_streams));
    setText($("f-http"), count(con.http_in_flight));
    setText($("f-studies"), count(st.studies));
    setText($("f-series"), count(st.series));
    setText($("f-inst"), count(st.instances));
  }

  return {
    load(samples) {
      ring.load(samples);
      drawCharts();
      const last = samples[samples.length - 1];
      if (last && !latest) renderNumbers(last);
    },
    update(snap) {
      latest = snap;
      const rss = snap.process?.rss_bytes;
      if (rss != null) peakRss = Math.max(peakRss ?? 0, rss);
      const total = storageTotal(snap.storage ?? {});
      if (firstStorage === null && total !== null) firstStorage = total;
      ring.push(snap);
      drawCharts();
      renderNumbers(snap);
    },
  };
}

function storageTotal(st) {
  const parts = [st.objects_bytes, st.index_bytes, st.tmp_bytes];
  if (parts.every((v) => v === null || v === undefined)) return null;
  return parts.reduce((a, v) => a + (v ?? 0), 0);
}
