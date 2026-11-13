// Upload panel and ingest results: file picking, drag and drop, the upload
// queue (4 in flight, one file per request) and the results table. Each
// file's first bytes pick its route: DICOM goes to STOW-RS or
// /api/instances, archives and NIfTI to /api/archives (archive-upload.js).
import { uploadArchive } from "./archive-upload.js";
import { SNIFF_LEN, sniff } from "./lib/archive-core.js";
import { DASH, formatBytes, formatDuration, formatRate } from "./lib/format.js";
import {
  HTTP_HINT,
  classifyInstance,
  classifyStow,
  progress,
  shortUid,
  stowBody,
  stowContentType,
} from "./lib/upload-core.js";

const IN_FLIGHT = 4;
const MAX_ROWS = 500;
const TOKEN_KEY = "slicestream.uploadToken";
const MODE_KEY = "slicestream.uploadMode";
const DROP_TITLE = "Drop DICOM, archives or a folder";

const $ = (id) => document.getElementById(id);

// sessionStorage can throw (privacy modes, sandboxed frames); never let it break the page.
const store = {
  get(key) {
    try {
      return sessionStorage.getItem(key);
    } catch {
      return null;
    }
  },
  set(key, value) {
    try {
      if (value) sessionStorage.setItem(key, value);
      else sessionStorage.removeItem(key);
    } catch {
      // Not persisted; the in-page value still works.
    }
  },
};

const plural = (n, word) => `${n.toLocaleString()} ${word}${n === 1 ? "" : "s"}`;

// ---- file collection -------------------------------------------------------

const fromFileList = (list) =>
  Array.from(list, (file) => ({ file, path: file.webkitRelativePath || file.name }));

function entryFile(entry) {
  return new Promise((resolve, reject) => entry.file(resolve, reject));
}

function readBatch(reader) {
  return new Promise((resolve, reject) => reader.readEntries(resolve, reject));
}

// Walk a FileSystemEntry tree; directories are read until readEntries returns [].
async function walk(entry, out) {
  if (entry.isFile) {
    try {
      out.push({ file: await entryFile(entry), path: entry.fullPath.replace(/^\//, "") });
    } catch (err) {
      console.warn("cannot read dropped file", entry.fullPath, err);
    }
  } else if (entry.isDirectory) {
    const reader = entry.createReader();
    for (;;) {
      let batch;
      try {
        batch = await readBatch(reader);
      } catch (err) {
        console.warn("cannot read dropped folder", entry.fullPath, err);
        return;
      }
      if (batch.length === 0) return;
      for (const child of batch) await walk(child, out);
    }
  }
}

async function fromDataTransfer(dt) {
  // Entries must be taken synchronously, before the first await.
  const entries = [];
  const loose = [];
  for (const item of Array.from(dt.items ?? [])) {
    if (item.kind !== "file") continue;
    const entry = item.webkitGetAsEntry?.();
    if (entry) entries.push(entry);
    else {
      const file = item.getAsFile();
      if (file) loose.push({ file, path: file.name });
    }
  }
  if (entries.length === 0 && loose.length === 0) return fromFileList(dt.files ?? []);
  const out = [];
  for (const entry of entries) await walk(entry, out);
  return out.concat(loose);
}

// ---- requests --------------------------------------------------------------

async function readJson(res) {
  const text = await res.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return { error: text.slice(0, 200) };
  }
}

function boundary() {
  const rand = new Uint8Array(12);
  crypto.getRandomValues(rand);
  return `slicestream-${Array.from(rand, (b) => b.toString(16).padStart(2, "0")).join("")}`;
}

// Route of a file from its first bytes: "dicom" | "archive" | "nifti" | "other".
async function kindOf(file) {
  return sniff(new Uint8Array(await file.slice(0, SNIFF_LEN).arrayBuffer()));
}

// Upload one DICOM file; resolves to a row result, or null if aborted.
async function send({ file }, mode, token, signal) {
  const headers = {};
  if (token) headers.Authorization = `Bearer ${token}`;
  let body;
  let url;
  if (mode === "stow") {
    const b = boundary();
    // Bytes are read here, when this file's slot starts.
    body = stowBody(new Uint8Array(await file.arrayBuffer()), b);
    headers["Content-Type"] = stowContentType(b);
    url = "/dicom-web/studies";
  } else {
    body = file;
    headers["Content-Type"] = "application/dicom";
    url = "/api/instances";
  }
  const res = await fetch(url, { method: "POST", headers, body, signal, cache: "no-store" });
  const json = await readJson(res);
  return mode === "stow" ? classifyStow(res.status, json) : classifyInstance(res.status, json);
}

// ---- panel -----------------------------------------------------------------

export function createUploadPanel() {
  const el = {
    seg: document.querySelectorAll("#upload-card .seg button"),
    drop: $("u-drop"),
    dropTitle: $("u-drop-title"),
    files: $("u-files"),
    dir: $("u-dir"),
    pickDir: $("u-pick-dir"),
    start: $("u-start"),
    clear: $("u-clear"),
    token: $("u-token"),
    progress: $("u-progress"),
    bar: $("u-bar"),
    count: $("u-count"),
    rate: $("u-rate"),
    empty: $("r-empty"),
    wrap: $("r-wrap"),
    body: $("r-body"),
    foot: $("r-foot"),
    totals: { STORED: $("r-stored"), DUPLICATE: $("r-dup"), REPLACED: $("r-rep"), FAILED: $("r-failed") },
    repWrap: $("r-rep-wrap"),
  };

  let mode = store.get(MODE_KEY) === "single" ? "single" : "stow";
  el.token.value = store.get(TOKEN_KEY) ?? "";

  // Current batch. A new generation after Clear makes stale workers stop.
  let gen = 0;
  let queue = [];
  let next = 0;
  let running = 0;
  let controller = null;
  let batch = null; // { done, failed, total, bytesDone, bytesTotal, startedAt, endedAt }
  const totals = { STORED: 0, DUPLICATE: 0, REPLACED: 0, FAILED: 0 };
  let drawQueued = false;

  function setMode(m) {
    mode = m;
    store.set(MODE_KEY, m === "single" ? m : "");
    for (const b of el.seg) {
      const on = b.dataset.mode === m;
      b.classList.toggle("on", on);
      b.setAttribute("aria-checked", String(on));
    }
  }

  function add(items) {
    if (items.length === 0) return;
    if (!batch || (running === 0 && next >= queue.length && batch.startedAt)) {
      // Previous batch finished: start a fresh one.
      queue = [];
      next = 0;
      batch = { done: 0, failed: 0, total: 0, bytesDone: 0, bytesTotal: 0, startedAt: 0, endedAt: 0 };
    }
    for (const it of items) {
      queue.push(it);
      batch.total += 1;
      batch.bytesTotal += it.file.size;
    }
    if (batch.startedAt) pump(); // already uploading: new files join the run
    draw();
  }

  function start() {
    if (!batch || next >= queue.length || batch.startedAt) return;
    batch.startedAt = performance.now();
    controller = new AbortController();
    pump();
    draw();
  }

  function pump() {
    while (running < IN_FLIGHT && next < queue.length) {
      const item = queue[next];
      queue[next] = null; // let the File go once it has been sent
      next += 1;
      running += 1;
      run(item, gen);
    }
    if (running === 0 && next >= queue.length && batch?.startedAt && !batch.endedAt) {
      batch.endedAt = performance.now();
      draw();
    }
  }

  async function run(item, myGen) {
    const t0 = performance.now();
    let r;
    try {
      const kind = await kindOf(item.file);
      if (myGen !== gen) return;
      if (kind === "archive" || kind === "nifti") {
        // Holds this slot only while the body uploads; the job is then polled on its own.
        const ok = await uploadArchive(item, {
          token: el.token.value.trim(),
          signal: controller.signal,
          mount: addRow,
          onTotals: addTotals,
        });
        r = ok === null ? null : { status: ok ? "ARCHIVE" : "FAILED", archive: true };
      } else if (kind === "other") {
        r = { status: "FAILED", code: "BAD TYPE", uid: null, reason: "unsupported file type" };
      } else {
        r = await send(item, mode, el.token.value.trim(), controller.signal);
      }
    } catch (err) {
      if (err?.name === "AbortError" || myGen !== gen) r = null;
      else if (err instanceof TypeError) r = { status: "FAILED", code: "NETWORK", uid: null, reason: err.message || "network error" };
      else r = { status: "FAILED", code: "READ", uid: null, reason: err?.message || "cannot read file" };
    }
    if (myGen !== gen) return; // cleared while in flight
    running -= 1;
    if (r) {
      const ms = performance.now() - t0;
      batch.done += 1;
      batch.bytesDone += item.file.size;
      if (r.status === "FAILED") batch.failed += 1;
      if (!r.archive) record(item, r, ms); // archive rows render themselves
    }
    pump();
    draw();
  }

  function clear() {
    gen += 1;
    controller?.abort();
    controller = null;
    queue = [];
    next = 0;
    running = 0;
    batch = null;
    el.files.value = "";
    el.dir.value = "";
    draw();
  }

  // ---- results ----

  let rows = 0; // rows ever added (file rows and archive rows)

  function bump(status, k) {
    if (!k) return;
    totals[status] += k;
    const b = el.totals[status];
    b.textContent = totals[status].toLocaleString();
    b.classList.remove("bump");
    void b.offsetWidth; // restart the animation
    b.classList.add("bump");
    if (status === "REPLACED") el.repWrap.hidden = false;
  }

  // Archive entries count towards the header totals as they are processed.
  function addTotals(delta) {
    for (const [status, k] of Object.entries(delta)) bump(status, k);
  }

  function addRow(tr) {
    rows += 1;
    el.body.prepend(tr);
    while (el.body.childElementCount > MAX_ROWS) el.body.lastElementChild.remove();
    el.empty.hidden = true;
    el.wrap.hidden = false;
    el.foot.hidden = rows <= MAX_ROWS;
    if (!el.foot.hidden) el.foot.textContent = `showing newest ${MAX_ROWS} of ${rows.toLocaleString()} rows`;
  }

  function record(item, r, ms) {
    bump(r.status, 1);

    const tr = document.createElement("tr");
    tr.className = "fresh";
    const td = (text, cls, title) => {
      const c = document.createElement("td");
      if (cls) c.className = cls;
      c.textContent = text;
      if (title) c.title = title;
      tr.append(c);
      return c;
    };
    td(item.path, "", item.path);

    const tagCell = td("", "");
    const tag = document.createElement("span");
    const ok = r.status !== "FAILED";
    tag.className = `tag ${{ STORED: "t-ok", DUPLICATE: "t-dup", REPLACED: "t-rep" }[r.status] ?? "t-bad"}`;
    tag.textContent = ok ? r.status : r.code;
    tagCell.append(tag);

    if (ok) {
      td(shortUid(r.uid) || DASH, "", r.uid ?? "");
    } else {
      const why = r.reason || HTTP_HINT[r.code] || "failed";
      td(why, "why", r.uid ? `${why}\nSOP ${r.uid}` : why);
    }
    td(formatBytes(item.file.size), "n");
    td(`${Math.round(ms)} ms`, "n");

    addRow(tr);

    if (ok) {
      window.dispatchEvent(
        new CustomEvent("slicestream:stored", { detail: { status: r.status, sopInstanceUid: r.uid } }),
      );
    }
  }

  // ---- progress (coalesced to one paint per frame) ----

  function draw() {
    if (drawQueued) return;
    drawQueued = true;
    requestAnimationFrame(() => {
      drawQueued = false;
      paint();
    });
  }

  function paint() {
    const pending = batch ? queue.length - next : 0;
    const active = Boolean(batch?.startedAt && !batch.endedAt);
    el.start.disabled = !batch || batch.startedAt !== 0 || pending === 0;
    if (active) el.start.textContent = "Uploading…";
    else if (batch && !batch.startedAt) el.start.textContent = `Upload ${plural(batch.total, "file")}`;
    else el.start.textContent = "Upload";
    el.clear.disabled = !batch;

    if (!batch) {
      el.bar.style.width = "0";
      el.progress.setAttribute("aria-valuenow", "0");
      el.progress.classList.remove("bad");
      el.count.textContent = "no files selected";
      el.rate.textContent = DASH;
      return;
    }
    const pct = batch.total ? (batch.done / batch.total) * 100 : 0;
    el.bar.style.width = `${pct}%`;
    el.progress.setAttribute("aria-valuenow", String(Math.round(pct)));
    el.progress.classList.toggle("bad", batch.failed > 0);

    if (!batch.startedAt) {
      el.count.textContent = `${plural(batch.total, "file")} · ${formatBytes(batch.bytesTotal)} ready`;
      el.rate.textContent = mode === "stow" ? "STOW-RS · 4 in flight" : "single file · 4 in flight";
      return;
    }
    const end = batch.endedAt || performance.now();
    const p = progress({ ...batch, elapsedMs: end - batch.startedAt });
    const failed = batch.failed ? ` · ${batch.failed.toLocaleString()} failed` : "";
    el.count.textContent = `${batch.done.toLocaleString()} / ${plural(batch.total, "file")} · ${formatBytes(batch.bytesDone)}${failed}`;
    const took = end - batch.startedAt;
    const tail = batch.endedAt
      ? `done in ${took < 1000 ? `${Math.round(took)} ms` : `${(took / 1000).toFixed(1)} s`}`
      : `eta ${p.etaSecs === null ? DASH : p.etaSecs < 60 ? `${p.etaSecs.toFixed(1)} s` : formatDuration(p.etaSecs)}`;
    el.rate.textContent = `${p.filesPerSec.toFixed(1)} files/s · ${formatRate(p.bytesPerSec)} · ${tail}`;
  }

  // Keep rates and ETA moving between completions.
  setInterval(() => {
    if (batch?.startedAt && !batch.endedAt) draw();
  }, 500);

  // ---- events ----

  for (const b of el.seg) b.addEventListener("click", () => setMode(b.dataset.mode));
  setMode(mode);

  el.token.addEventListener("input", () => store.set(TOKEN_KEY, el.token.value.trim()));
  $("u-token-form").addEventListener("submit", (e) => e.preventDefault());

  const pickFiles = () => el.files.click();
  el.drop.addEventListener("click", (e) => {
    if (e.target === el.pickDir || e.target.tagName === "INPUT") return;
    pickFiles();
  });
  el.drop.addEventListener("keydown", (e) => {
    if (e.target !== el.drop) return;
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      pickFiles();
    }
  });
  el.pickDir.addEventListener("click", (e) => {
    e.stopPropagation();
    el.dir.click();
  });
  for (const input of [el.files, el.dir]) {
    input.addEventListener("change", () => {
      add(fromFileList(input.files));
      input.value = "";
    });
  }

  // A drop outside the zone must not navigate away from the console.
  window.addEventListener("dragover", (e) => e.preventDefault());
  window.addEventListener("drop", (e) => e.preventDefault());
  let depth = 0;
  el.drop.addEventListener("dragenter", (e) => {
    e.preventDefault();
    depth += 1;
    el.drop.classList.add("over");
    el.dropTitle.textContent = "Release to add files";
  });
  el.drop.addEventListener("dragover", (e) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
  });
  const leave = () => {
    depth = 0;
    el.drop.classList.remove("over");
    el.dropTitle.textContent = DROP_TITLE;
  };
  el.drop.addEventListener("dragleave", () => {
    depth -= 1;
    if (depth <= 0) leave();
  });
  el.drop.addEventListener("drop", async (e) => {
    e.preventDefault();
    leave();
    const dt = e.dataTransfer;
    el.dropTitle.textContent = "Reading folder…";
    try {
      add(await fromDataTransfer(dt));
    } finally {
      el.dropTitle.textContent = DROP_TITLE;
    }
  });

  el.start.addEventListener("click", start);
  el.clear.addEventListener("click", clear);

  paint();
}
