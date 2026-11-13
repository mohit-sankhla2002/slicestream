// Console entry: wires the header and panels together.
import { formatDuration } from "./lib/format.js";
import { createMetricsPanel, startMetrics } from "./metrics.js";

const $ = (id) => document.getElementById(id);

function setStatus(status) {
  const pill = $("h-live");
  const live = status === "live";
  pill.className = `pill ${live ? "live" : "offline"}`;
  pill.textContent = live ? "live · 1 s" : "offline";
}

function renderHeader(s) {
  $("h-proc").textContent = `pid ${s.pid ?? "—"} · v${s.version ?? "—"}`;
  $("h-uptime").textContent = `uptime ${formatDuration(s.uptime_secs)}`;
  $("h-addr").textContent = `http ${s.http_addr ?? "—"} · dimse ${s.dimse_addr ?? "off"}`;
}

const metrics = createMetricsPanel();
startMetrics({
  onHistory: (samples) => metrics.load(samples),
  onSample: (snap) => {
    renderHeader(snap);
    metrics.update(snap);
  },
  onStatus: setStatus,
});
