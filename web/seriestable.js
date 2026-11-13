// Stored series table: polls /api/console/series every 5 s and after each
// stored upload (at most once per second). Clicking a row selects it.
import { DASH, formatBytes } from "./lib/format.js";
import { formatStudyDate, seriesLabel } from "./lib/viewer-core.js";
import { createThrottle } from "./lib/throttle.js";

const POLL_MS = 5000;
const THROTTLE_MS = 1000;

const $ = (id) => document.getElementById(id);

export function startSeriesTable({ onSelect }) {
  const el = {
    note: $("s-note"),
    empty: $("s-empty"),
    emptyTitle: $("s-empty-title"),
    emptyHint: $("s-empty-hint"),
    wrap: $("s-wrap"),
    body: $("s-body"),
  };
  let selected = null;
  let lastJson = null;
  let known = null; // series UIDs from the previous render; null before the first
  let inFlight = false;
  let again = false;

  function cell(tr, text, cls) {
    const td = document.createElement("td");
    td.textContent = text;
    if (cls) td.className = cls;
    tr.append(td);
    return td;
  }

  function select(uid) {
    selected = uid;
    for (const tr of el.body.children) {
      const on = tr.dataset.uid === uid;
      tr.classList.toggle("sel", on);
      tr.setAttribute("aria-selected", String(on));
    }
    onSelect(uid);
  }

  function render(rows) {
    if (!rows.length) {
      el.wrap.hidden = true;
      el.empty.hidden = false;
      el.emptyTitle.textContent = "No series stored yet — upload some files";
      el.emptyHint.textContent = "Stored series appear here; click one to open it in the viewer";
      el.note.textContent = "updates on ingest";
      known = new Set();
      return;
    }
    const frag = document.createDocumentFragment();
    for (const r of rows) {
      const tr = document.createElement("tr");
      tr.dataset.uid = r.series_uid;
      tr.tabIndex = 0;
      tr.title = `series ${r.series_uid}`;
      const on = r.series_uid === selected;
      if (on) tr.classList.add("sel");
      tr.setAttribute("aria-selected", String(on));
      if (known && !known.has(r.series_uid)) tr.classList.add("fresh");
      cell(tr, r.patient_name || DASH);
      cell(tr, formatStudyDate(r.study_date));
      cell(tr, seriesLabel(r));
      cell(tr, (r.instance_count ?? 0).toLocaleString(), "n");
      cell(tr, formatBytes(r.size_bytes), "n");
      frag.append(tr);
    }
    // Keep keyboard focus on the same row across re-renders.
    const focused = el.body.contains(document.activeElement) ? document.activeElement.dataset.uid : null;
    el.body.replaceChildren(frag);
    if (focused) el.body.querySelector(`tr[data-uid="${CSS.escape(focused)}"]`)?.focus({ preventScroll: true });
    el.empty.hidden = true;
    el.wrap.hidden = false;
    el.note.textContent = `${rows.length.toLocaleString()} series · updates on ingest`;
    known = new Set(rows.map((r) => r.series_uid));
  }

  async function refresh() {
    if (inFlight) {
      again = true;
      return;
    }
    inFlight = true;
    try {
      const res = await fetch("/api/console/series", { cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const text = await res.text();
      if (text !== lastJson) {
        const rows = JSON.parse(text);
        render(Array.isArray(rows) ? rows : []);
        lastJson = text;
      }
    } catch (err) {
      // Keep the last good table; just say the list is stale.
      if (lastJson === null) {
        el.emptyTitle.textContent = "Could not load series";
        el.emptyHint.textContent = `${err.message || err}; retrying every 5 s`;
      }
      el.note.textContent = "offline · retrying";
      lastJson = null; // re-render (and restore the note) once it answers again
    } finally {
      inFlight = false;
      if (again) {
        again = false;
        throttle.call();
      }
    }
  }

  const throttle = createThrottle(refresh, THROTTLE_MS);

  el.body.addEventListener("click", (e) => {
    const tr = e.target.closest("tr[data-uid]");
    if (tr) select(tr.dataset.uid);
  });
  el.body.addEventListener("keydown", (e) => {
    const tr = e.target.closest("tr[data-uid]");
    if (!tr) return;
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      select(tr.dataset.uid);
    } else if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      (e.key === "ArrowDown" ? tr.nextElementSibling : tr.previousElementSibling)?.focus();
    }
  });

  addEventListener("slicestream:stored", () => throttle.call());
  setInterval(() => throttle.call(), POLL_MS);
  throttle.call();
}
