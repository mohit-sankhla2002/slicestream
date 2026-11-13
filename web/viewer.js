// Embedded viewer: streams a series over the SLS1 WebSocket protocol
// (docs/PROTOCOL.md) and draws native grayscale frames on a canvas. Scroll
// with the wheel, arrow keys or the scrub bar; every slice change sends a
// cursor message so the server streams what is needed next.
import {
  clampIndex,
  formatWL,
  frameSupport,
  parseFrame,
  renderGray,
  seriesLabel,
  windowLevel,
} from "./lib/viewer-core.js";

const $ = (id) => document.getElementById(id);
const TS_PREFIX = "unsupported transfer syntax";

let el = null;
let s = null; // current session; replaced on every openViewer

function elements() {
  if (el) return el;
  el = {
    title: $("v-title"),
    note: $("v-note"),
    empty: $("v-empty"),
    wrap: $("v-wrap"),
    img: $("v-img"),
    canvas: $("v-canvas"),
    slice: $("v-slice"),
    wl: $("v-wl"),
    msg: $("v-msg"),
    scrub: $("v-scrub"),
    cache: $("v-cache"),
    cur: $("v-cur"),
  };
  el.ctx = el.canvas.getContext("2d");
  el.cacheCtx = el.cache.getContext("2d");
  bindInput();
  // Velocity decays when the user stops scrolling (as in the original client).
  setInterval(() => {
    if (s) s.velocity *= 0.5;
  }, 250);
  return el;
}

export function openViewer(seriesUid) {
  elements();
  if (s && s.uid === seriesUid && s.ws.readyState <= WebSocket.OPEN) {
    el.img.focus({ preventScroll: true });
    return;
  }
  closeSession();

  const ws = new WebSocket(
    `${location.origin.replace(/^http/, "ws")}/api/series/${encodeURIComponent(seriesUid)}/stream`,
  );
  ws.binaryType = "arraybuffer";
  s = {
    uid: seriesUid,
    ws,
    frames: new Map(), // index -> { info, data }
    total: 0,
    cursor: 0,
    velocity: 0,
    lastMove: { t: performance.now(), i: 0 },
    tOpen: performance.now(),
    tFirst: 0,
    first: null, // FrameInfo from the hello message
    label: "",
    closed: false,
    image: null,
  };
  const mine = s;
  ws.onmessage = (ev) => {
    if (s !== mine) return;
    if (typeof ev.data === "string") onText(ev.data);
    else onFrame(ev.data);
  };
  ws.onclose = () => {
    if (s !== mine) return;
    mine.closed = true;
    if (!mine.total) showMsg("Stream closed before the series loaded", "the series may have been removed");
    status();
  };

  el.empty.hidden = true;
  el.wrap.hidden = false;
  el.title.textContent = "Viewer";
  el.slice.textContent = "slice — / —";
  el.wl.textContent = "W — L —";
  el.canvas.width = el.canvas.height = 1; // clears the previous series' image
  el.canvas.classList.add("stale");
  hideMsg();
  el.cache.height = 1;
  el.cacheCtx.clearRect(0, 0, 1, 1);
  el.cur.hidden = true;
  status();
  el.img.focus({ preventScroll: true });
}

function closeSession() {
  if (!s) return;
  const { ws } = s;
  ws.onmessage = ws.onclose = null;
  ws.close();
  s = null;
}

// ---- protocol ----------------------------------------------------------------

function onText(text) {
  let msg;
  try {
    msg = JSON.parse(text);
  } catch {
    return;
  }
  if (msg.type !== "series" || !msg.series) return;
  const series = msg.series;
  s.total = series.sliceCount ?? 0;
  s.first = series.first ?? null;
  s.cursor = Math.floor(s.total / 2);
  s.lastMove = { t: performance.now(), i: s.cursor };
  s.label = seriesLabel({ series_description: series.description, modality: series.modality, series_uid: series.uid });
  el.title.textContent = `Viewer · ${s.label}`;
  if (s.first) sizeCanvas(s.first.columns, s.first.rows);
  el.cache.width = 1;
  el.cache.height = Math.max(1, s.total);
  el.cacheCtx.fillStyle = "#2c6e69";
  el.cur.hidden = s.total === 0;
  if (!s.total) showMsg("This series has no slices", "");
  sendCursor();
  draw();
}

function onFrame(buf) {
  const f = parseFrame(buf);
  if (!f || f.index < 0 || f.index >= s.total) return;
  if (!s.frames.has(f.index)) el.cacheCtx.fillRect(0, f.index, 1, 1);
  s.frames.set(f.index, { info: f.info, data: f.data });
  if (!s.tFirst) s.tFirst = performance.now();
  if (f.index === s.cursor) draw();
  status();
}

function sendCursor() {
  if (s?.ws.readyState === WebSocket.OPEN) {
    s.ws.send(JSON.stringify({ type: "cursor", index: s.cursor, velocity: s.velocity }));
  }
}

// ---- navigation -------------------------------------------------------------

function goTo(index) {
  if (!s || !s.total) return;
  const next = clampIndex(index, s.total);
  if (next === s.cursor) return;
  const now = performance.now();
  const dt = Math.max(1, now - s.lastMove.t) / 1000;
  s.velocity = 0.6 * s.velocity + 0.4 * ((next - s.lastMove.i) / dt); // smoothed slices/s
  s.lastMove = { t: now, i: next };
  s.cursor = next;
  sendCursor();
  draw();
}

const move = (delta) => s && goTo(s.cursor + delta);

function bindInput() {
  const wheel = (e) => {
    if (!s || !s.total || !e.deltaY) return;
    e.preventDefault();
    move(Math.sign(e.deltaY));
  };
  el.img.addEventListener("wheel", wheel, { passive: false });
  el.scrub.addEventListener("wheel", wheel, { passive: false });

  el.img.addEventListener("keydown", (e) => {
    if (!s || !s.total || e.altKey || e.ctrlKey || e.metaKey) return;
    const page = Math.max(1, Math.round(s.total / 10));
    const step = {
      ArrowDown: 1, ArrowRight: 1, ArrowUp: -1, ArrowLeft: -1,
      PageDown: page, PageUp: -page,
    }[e.key];
    if (step) move(step);
    else if (e.key === "Home") goTo(0);
    else if (e.key === "End") goTo(s.total - 1);
    else return;
    e.preventDefault();
  });
  el.img.addEventListener("pointerdown", () => el.img.focus({ preventScroll: true }));

  const scrubTo = (e) => {
    if (!s || !s.total) return;
    const r = el.scrub.getBoundingClientRect();
    goTo(Math.floor(((e.clientY - r.top) / r.height) * s.total));
  };
  el.scrub.addEventListener("pointerdown", (e) => {
    if (e.button !== 0) return;
    el.scrub.setPointerCapture(e.pointerId);
    el.scrub.classList.add("drag");
    el.img.focus({ preventScroll: true });
    scrubTo(e);
  });
  el.scrub.addEventListener("pointermove", (e) => {
    if (el.scrub.hasPointerCapture(e.pointerId)) scrubTo(e);
  });
  const end = () => el.scrub.classList.remove("drag");
  el.scrub.addEventListener("pointerup", end);
  el.scrub.addEventListener("pointercancel", end);
}

// ---- drawing ----------------------------------------------------------------

function sizeCanvas(columns, rows) {
  if (el.canvas.width !== columns || el.canvas.height !== rows) {
    el.canvas.width = columns;
    el.canvas.height = rows;
  }
  if (!s.image || s.image.width !== columns || s.image.height !== rows) {
    s.image = el.ctx.createImageData(columns, rows);
  }
}

function draw() {
  if (!s) return;
  el.slice.textContent = s.total ? `slice ${s.cursor + 1} / ${s.total}` : "slice — / —";
  el.cur.style.top = `calc(${((s.cursor + 0.5) / Math.max(1, s.total)) * 100}% - 2px)`;
  const f = s.frames.get(s.cursor);
  const info = f?.info ?? s.first;
  if (info) {
    const { center, width } = windowLevel(info);
    el.wl.textContent = `W ${formatWL(width)} L ${formatWL(center)}`;
  }
  if (!f) {
    // Keep the previous image, dimmed, until this slice arrives.
    el.canvas.classList.add("stale");
    return;
  }
  const why = frameSupport(f.info);
  if (why) {
    sizeCanvas(f.info.columns || 1, f.info.rows || 1);
    el.ctx.fillStyle = "#000";
    el.ctx.fillRect(0, 0, el.canvas.width, el.canvas.height);
    el.canvas.classList.remove("stale");
    const isTs = why.startsWith(TS_PREFIX);
    showMsg(isTs ? TS_PREFIX : why, isTs ? f.info.transferSyntax : "");
    return;
  }
  hideMsg();
  sizeCanvas(f.info.columns, f.info.rows);
  renderGray(f.info, f.data, s.image.data);
  el.ctx.putImageData(s.image, 0, 0);
  el.canvas.classList.remove("stale");
}

function showMsg(title, detail) {
  el.msg.replaceChildren();
  const b = document.createElement("b");
  b.textContent = title;
  el.msg.append(b);
  if (detail) {
    const small = document.createElement("small");
    small.textContent = detail;
    el.msg.append(small);
  }
  el.msg.hidden = false;
}

function hideMsg() {
  el.msg.hidden = true;
}

function status() {
  if (!s) return;
  const parts = [];
  if (s.tFirst) parts.push(`first frame ${(s.tFirst - s.tOpen).toFixed(0)} ms`);
  else if (!s.closed) parts.push(s.total ? "waiting for frames" : "connecting");
  if (s.total) parts.push(`${s.frames.size.toLocaleString()}/${s.total.toLocaleString()} cached`);
  if (s.closed) parts.push("disconnected");
  el.note.textContent = parts.join(" · ");
}
