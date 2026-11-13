// Console entry: wires the header and panels together.
import { formatDuration } from "./lib/format.js";
import { createMetricsPanel, startMetrics } from "./metrics.js";
import { startSeriesTable } from "./seriestable.js";
import { createUploadPanel } from "./upload.js";
import { openViewer } from "./viewer.js";

const $ = (id) => document.getElementById(id);

function setStatus(status) {
  const pill = $("h-live");
  const label = { live: "live · 1 s", stale: "stale", off: "metrics off" }[status] ?? "offline";
  pill.className = `pill ${status in { live: 1, stale: 1, off: 1 } ? status : "offline"}`;
  pill.textContent = label;
}

function renderHeader(s) {
  $("h-proc").textContent = `pid ${s.pid ?? "—"} · v${s.version ?? "—"}`;
  $("h-uptime").textContent = `uptime ${formatDuration(s.uptime_secs)}`;
  $("h-addr").textContent = `http ${s.http_addr ?? "—"} · dimse ${s.dimse_addr ?? "off"}`;
}

createUploadPanel();
startSeriesTable({ onSelect: openViewer });

const metrics = createMetricsPanel();
startMetrics({
  onHistory: (samples) => metrics.load(samples),
  onSample: (snap) => {
    renderHeader(snap);
    metrics.update(snap);
  },
  onStatus: setStatus,
});
