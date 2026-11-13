// Archive and NIfTI uploads: one POST /api/archives per file with upload
// progress, then the job is polled every 500 ms until it ends. Each upload
// is one results row: name, phase, totals, an expandable list of its
// entries and a patient/study summary when done.
import { DASH, formatBytes } from "./lib/format.js";
import { entryRow, freshEntries, jobView, postError } from "./lib/archive-core.js";
import { formatStudyDate } from "./lib/viewer-core.js";

const POLL_MS = 500;
const RETRY_MS = 2000;
// Entry rows kept per archive (newest), in memory and in the DOM.
const ENTRY_ROWS = 500;
// Patients listed in the summary before "+ N more".
const SUMMARY_PATIENTS = 20;

const PHASE_TAG = {
  uploading: ["UPLOADING", "t-run"],
  queued: ["QUEUED", "t-run"],
  running: ["PROCESSING", "t-run"],
  done: ["DONE", "t-ok"],
  aborted: ["ABORTED", "t-rep"],
  gone: ["GONE", "t-skip"],
};

function readJson(text) {
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return { error: text.slice(0, 200) };
  }
}

// POST the file; resolves to { status, json }, rejects on network error or abort.
function post(file, token, signal, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const abort = () => xhr.abort();
    xhr.open("POST", "/api/archives");
    xhr.setRequestHeader("Content-Type", "application/octet-stream");
    xhr.setRequestHeader("x-filename", encodeURIComponent(file.name));
    if (token) xhr.setRequestHeader("Authorization", `Bearer ${token}`);
    xhr.upload.onprogress = (e) => onProgress(e.loaded, e.lengthComputable ? e.total : file.size);
    xhr.onload = () => {
      signal.removeEventListener("abort", abort);
      resolve({ status: xhr.status, json: readJson(xhr.responseText) });
    };
    xhr.onerror = () => {
      signal.removeEventListener("abort", abort);
      reject(new TypeError("network error"));
    };
    xhr.onabort = () => reject(new DOMException("upload cancelled", "AbortError"));
    if (signal.aborted) {
      reject(new DOMException("upload cancelled", "AbortError"));
      return;
    }
    signal.addEventListener("abort", abort, { once: true });
    xhr.send(file);
  });
}

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}

function cell(tr, text, cls, title) {
  const td = el("td", cls, text);
  if (title) td.title = title;
  tr.append(td);
  return td;
}

const seconds = (ms) => (ms < 1000 ? `${Math.round(ms)} ms` : ms < 60000 ? `${(ms / 1000).toFixed(1)} s` : `${Math.round(ms / 1000)} s`);

// Same column layout as the outer results table, so nested rows line up.
function colgroup() {
  const g = el("colgroup");
  for (const c of ["w-file", "w-tag", "", "w-n w-size", "w-n"]) {
    const col = el("col");
    if (c) col.className = c;
    g.append(col);
  }
  return g;
}

// `prefix` ("<archive name>!/") is left out of the shown path; the tooltip has it all.
function entryTr(e, prefix) {
  const r = entryRow(e);
  const tr = el("tr", "arc-ent");
  cell(tr, r.path.startsWith(prefix) ? r.path.slice(prefix.length) : r.path, "", r.path);
  const tagCell = cell(tr, "");
  tagCell.append(el("span", `tag ${r.cls}`, r.tag));
  cell(tr, r.detail || DASH, r.why ? "why" : r.cls === "t-skip" ? "skip" : "", r.title);
  cell(tr, r.size === null ? DASH : formatBytes(r.size), "n");
  cell(tr, "", "n");
  return tr;
}

// Upload `item` ({ file, path }) as an archive job.
// ctx: { token, signal, mount(tr), onTotals({ STORED, DUPLICATE, FAILED }) }.
// Resolves when the HTTP upload phase ends: true (job accepted), false
// (failed; the row says why) or null (cancelled; the row is removed).
// Polling then goes on by itself.
export function uploadArchive(item, ctx) {
  const { file, path } = item;
  const t0 = performance.now();
  const prefix = `${file.name}!/`;

  // ---- row ----
  const outer = el("tr", "arc fresh");
  const holder = el("td", "arc-cell");
  holder.colSpan = 5;
  outer.append(holder);
  const table = el("table", "arc-t");
  const head = el("tbody");
  const ents = el("tbody", "arc-ents");
  const foot = el("tbody");
  ents.hidden = true;
  foot.hidden = true;
  table.append(colgroup(), head, ents, foot);
  holder.append(table);

  const top = el("tr", "arc-head");
  top.tabIndex = 0;
  top.setAttribute("aria-expanded", "false");
  const nameCell = cell(top, "", "", path);
  const caret = el("span", "caret");
  caret.setAttribute("aria-hidden", "true");
  nameCell.append(caret, el("span", "", path));
  const tag = el("span", "tag");
  cell(top, "").append(tag);
  const label = cell(top, "");
  cell(top, formatBytes(file.size), "n");
  const time = cell(top, DASH, "n");
  head.append(top);

  const progTr = el("tr", "arc-prog");
  const progTd = el("td");
  progTd.colSpan = 5;
  const bar = el("div", "progress thin");
  const fill = el("i");
  bar.append(fill);
  const meta = el("div", "arc-meta");
  const phase = el("span", "", "");
  const chips = el("span", "counts small");
  meta.append(phase, chips);
  progTd.append(bar, meta);
  progTr.append(progTd);
  head.append(progTr);

  const sumTr = el("tr", "arc-sum");
  sumTr.hidden = true;
  const sumTd = el("td");
  sumTd.colSpan = 5;
  sumTr.append(sumTd);
  head.append(sumTr);

  const noteTr = el("tr", "arc-note");
  const noteTd = el("td");
  noteTd.colSpan = 5;
  noteTr.append(noteTd);
  foot.append(noteTr);

  // ---- state ----
  let state = "uploading";
  let expanded = false;
  let recent = []; // newest first, at most ENTRY_ROWS
  let shown = 0; // entries processed as of the last poll
  let last = { STORED: 0, DUPLICATE: 0, FAILED: 0 };
  let dispatched = false;
  let endedAt = 0;

  function setTag(key, override) {
    const [text, cls] = override ?? PHASE_TAG[key];
    tag.className = `tag ${cls}`;
    tag.textContent = text;
  }

  function setChips(t) {
    const parts = [
      ["c-ok", t.stored, "stored"],
      ["c-dup", t.dup, "dup"],
      ["c-nii", t.nifti, "nifti"],
      ["c-skip", t.skipped, "skipped"],
      ["c-bad", t.failed, "failed"],
    ];
    if (chips.childElementCount === 0) {
      for (const [cls, , word] of parts) {
        const s = el("span", cls);
        s.append(el("b", "", "0"), ` ${word}`);
        chips.append(s);
      }
    }
    parts.forEach(([, v], i) => {
      const b = chips.children[i].firstChild;
      const text = v.toLocaleString();
      if (b.textContent !== text) b.textContent = text;
    });
  }

  function setNote(processed) {
    if (!expanded) return;
    const rows = ents.childElementCount;
    if (rows === 0) {
      noteTd.textContent = state === "uploading" || state === "queued" ? "no entries yet" : "no entries";
      foot.hidden = false;
    } else if (processed > rows) {
      noteTd.textContent = `showing newest ${rows.toLocaleString()} of ${processed.toLocaleString()} entries`;
      foot.hidden = false;
    } else foot.hidden = true;
  }

  function toggle() {
    expanded = !expanded;
    top.setAttribute("aria-expanded", String(expanded));
    outer.classList.toggle("open", expanded);
    ents.hidden = !expanded;
    if (expanded) {
      const frag = document.createDocumentFragment();
      for (const e of recent) frag.append(entryTr(e, prefix));
      ents.replaceChildren(frag);
      setNote(shown);
    } else {
      ents.replaceChildren();
      foot.hidden = true;
    }
  }

  top.addEventListener("click", toggle);
  top.addEventListener("keydown", (e) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      toggle();
    }
  });

  function summary(view) {
    sumTd.replaceChildren();
    const box = el("div", "arc-summary");
    const pts = view.patients;
    for (const p of pts.slice(0, SUMMARY_PATIENTS)) {
      const pt = el("div", "pt");
      const who = el("div", "who");
      who.append(el("b", "", p.patient_name || DASH), el("span", "id", p.patient_id || DASH));
      pt.append(who);
      for (const s of p.studies ?? []) {
        const st = el("div", "st");
        st.title = `study ${s.study_uid}`;
        st.append(
          el("span", "date", formatStudyDate(s.study_date)),
          el("span", "desc", s.description || DASH),
          el("span", "n", `${(s.series ?? 0).toLocaleString()} series · ${(s.instances ?? 0).toLocaleString()} inst`),
        );
        pt.append(st);
      }
      box.append(pt);
    }
    if (pts.length > SUMMARY_PATIENTS) box.append(el("div", "more", `+ ${(pts.length - SUMMARY_PATIENTS).toLocaleString()} more patients`));
    const { linked = 0, unassigned = 0 } = view.nifti;
    if (linked || unassigned) {
      box.append(el("div", "nii", `NIfTI volumes · ${linked.toLocaleString()} linked · ${unassigned.toLocaleString()} unassigned`));
    }
    if (!pts.length && !linked && !unassigned) box.append(el("div", "more", "no DICOM or NIfTI stored"));
    sumTd.append(box);
    sumTr.hidden = false;
  }

  function tick() {
    const end = endedAt || performance.now();
    time.textContent = seconds(end - t0);
  }

  // ---- job polling ----

  function apply(job) {
    const view = jobView(job);
    state = view.status;
    setTag(state);
    label.className = "";
    if (state === "aborted") {
      label.textContent = view.reason ? `aborted: ${view.reason}` : "aborted";
      label.className = "why";
      label.title = label.textContent;
    } else label.textContent = view.label.split(" · ")[0]; // bytes are on the progress line
    phase.textContent = state === "queued" ? "waiting for a job slot" : view.label;
    bar.classList.toggle("busy", state === "running" || state === "queued");
    bar.classList.toggle("bad", state === "aborted" || view.totals.failed > 0);
    fill.style.width = "100%";
    setChips(view.totals);

    const fresh = freshEntries(job, shown, ENTRY_ROWS);
    shown = fresh.processed;
    if (fresh.rows.length) {
      recent = fresh.rows.concat(recent);
      if (recent.length > ENTRY_ROWS) recent.length = ENTRY_ROWS;
      if (expanded) {
        const frag = document.createDocumentFragment();
        for (const e of fresh.rows) frag.append(entryTr(e, prefix));
        ents.prepend(frag);
        while (ents.childElementCount > ENTRY_ROWS) ents.lastElementChild.remove();
      }
    }
    setNote(shown);

    const now = {
      STORED: view.totals.stored + view.totals.nifti,
      DUPLICATE: view.totals.dup,
      FAILED: view.totals.failed,
    };
    const delta = { STORED: now.STORED - last.STORED, DUPLICATE: now.DUPLICATE - last.DUPLICATE, FAILED: now.FAILED - last.FAILED };
    last = now;
    if (delta.STORED || delta.DUPLICATE || delta.FAILED) ctx.onTotals(delta);

    if (view.done) {
      endedAt = performance.now();
      bar.classList.remove("busy");
      summary(view);
      if (view.stored && !dispatched) {
        dispatched = true;
        window.dispatchEvent(
          new CustomEvent("slicestream:stored", { detail: { status: "STORED", job: job.job, uploadId: job.upload_id } }),
        );
      }
    }
    tick();
    return view.done;
  }

  function poll(id, token) {
    const headers = token ? { Authorization: `Bearer ${token}` } : {};
    let failures = 0;
    const step = async () => {
      let res;
      try {
        res = await fetch(`/api/archives/${encodeURIComponent(id)}`, { headers, cache: "no-store" });
        if (res.status === 404) {
          state = "gone";
          endedAt = performance.now();
          setTag("gone");
          label.textContent = "job no longer available";
          label.className = "skip";
          phase.textContent = "the server no longer has this job (restarted or evicted)";
          bar.classList.remove("busy");
          tick();
          return;
        }
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const job = await res.json();
        failures = 0;
        if (apply(job)) return;
        setTimeout(step, POLL_MS);
      } catch (err) {
        failures += 1;
        phase.textContent = `cannot read job status (${err?.message || "network error"}), retrying`;
        tick();
        setTimeout(step, failures > 3 ? RETRY_MS : POLL_MS);
      }
    };
    step();
  }

  // ---- upload phase ----

  setTag("uploading");
  label.textContent = "uploading 0%";
  phase.textContent = `0 B of ${formatBytes(file.size)}`;
  ctx.mount(outer);
  const ticker = setInterval(() => {
    if (state === "uploading") tick();
    else clearInterval(ticker);
  }, 500);

  let lastPaint = 0;
  const onProgress = (loaded, total) => {
    const t = performance.now();
    if (t - lastPaint < 100 && loaded < total) return;
    lastPaint = t;
    const pct = total ? Math.min(100, (loaded / total) * 100) : 0;
    fill.style.width = `${pct}%`;
    label.textContent = `uploading ${Math.floor(pct)}%`;
    phase.textContent = `${formatBytes(loaded)} of ${formatBytes(total)}`;
  };

  const fail = (code, reason) => {
    state = "failed";
    endedAt = performance.now();
    clearInterval(ticker);
    setTag(null, [code, "t-bad"]);
    label.textContent = reason;
    label.className = "why";
    label.title = reason;
    phase.textContent = "not sent to the archive processor";
    bar.classList.add("bad");
    tick();
    ctx.onTotals({ STORED: 0, DUPLICATE: 0, FAILED: 1 });
    return false;
  };

  return post(file, ctx.token, ctx.signal, onProgress).then(
    ({ status, json }) => {
      if (status !== 202 || typeof json?.job !== "string") {
        const e = postError(status, json);
        return fail(e.code, e.reason);
      }
      state = "queued";
      fill.style.width = "100%";
      setTag("queued");
      label.textContent = "queued";
      phase.textContent = "waiting for a job slot";
      bar.classList.add("busy");
      outer.dataset.job = json.job;
      poll(json.job, ctx.token);
      return true;
    },
    (err) => {
      clearInterval(ticker);
      if (err?.name === "AbortError") {
        outer.remove();
        return null;
      }
      return fail("NETWORK", err?.message || "network error");
    },
  );
}
