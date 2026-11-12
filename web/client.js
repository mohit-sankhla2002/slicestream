// Minimal slicestream browser client: scroll with mouse wheel or arrow keys.
// Native (uncompressed) grayscale is rendered directly; compressed transfer
// syntaxes arrive intact and need a WASM decoder (see README roadmap).
"use strict";

const $ = (id) => document.getElementById(id);
const canvas = $("view");
const ctx = canvas.getContext("2d");
const frames = new Map(); // index -> { info, data: ArrayBuffer }
let ws, total = 0, cursor = 0, lastMove = { t: performance.now(), i: 0 }, velocity = 0;
const t0 = { open: 0, first: 0 };

async function init() {
  const list = await (await fetch("/api/series")).json();
  const sel = $("series");
  for (const s of list) {
    const o = document.createElement("option");
    o.value = s.uid;
    o.textContent = `${s.modality ?? "?"} · ${s.description ?? s.uid} (${s.sliceCount})`;
    sel.append(o);
  }
  sel.onchange = () => open(sel.value);
  if (list.length) open(list[0].uid);
}

function open(uid) {
  ws?.close();
  frames.clear();
  t0.open = performance.now(); t0.first = 0;
  ws = new WebSocket(`${location.origin.replace(/^http/, "ws")}/api/series/${encodeURIComponent(uid)}/stream`);
  ws.binaryType = "arraybuffer";
  ws.onmessage = (ev) => (typeof ev.data === "string" ? onText(JSON.parse(ev.data)) : onFrame(ev.data));
  ws.onclose = () => status("disconnected");
}

function onText(msg) {
  if (msg.type !== "series") return;
  total = msg.series.sliceCount;
  cursor = Math.floor(total / 2);
  const s = msg.series.first;
  canvas.width = s.columns; canvas.height = s.rows;
  $("bar").innerHTML = "<span></span>".repeat(total);
  sendCursor();
}

function onFrame(buf) {
  const view = new DataView(buf);
  const magic = String.fromCharCode(...new Uint8Array(buf, 0, 4));
  if (magic !== "SLS1") return;
  const hlen = view.getUint32(4, true);
  const header = JSON.parse(new TextDecoder().decode(new Uint8Array(buf, 8, hlen)));
  frames.set(header.index, { info: header.info, data: buf.slice(8 + hlen) });
  $("bar").children[header.index]?.classList.add("got");
  if (!t0.first) t0.first = performance.now();
  if (header.index === cursor) draw();
  status();
}

function sendCursor() {
  if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: "cursor", index: cursor, velocity }));
}

function move(delta) {
  if (!total) return;
  const next = Math.max(0, Math.min(total - 1, cursor + delta));
  if (next === cursor) return;
  const now = performance.now();
  const dt = Math.max(1, now - lastMove.t) / 1000;
  velocity = 0.6 * velocity + 0.4 * ((next - lastMove.i) / dt); // smoothed slices/s
  lastMove = { t: now, i: next };
  $("bar").children[cursor]?.classList.remove("cur");
  cursor = next;
  sendCursor();
  draw();
}

function draw() {
  $("bar").children[cursor]?.classList.add("cur");
  const f = frames.get(cursor);
  if (!f) { status(); return; }
  const { info } = f;
  if (info.transferSyntax !== "1.2.840.10008.1.2.1" && info.transferSyntax !== "1.2.840.10008.1.2") {
    status(`slice ${cursor}: compressed (${info.transferSyntax}) — decoder not wired yet`);
    return;
  }
  const n = info.rows * info.columns;
  const px = info.bitsAllocated === 16
    ? (info.pixelRepresentation ? new Int16Array(f.data, 0, n) : new Uint16Array(f.data, 0, n))
    : new Uint8Array(f.data, 0, n);
  const wc = info.windowCenter ?? 40, ww = info.windowWidth ?? 400;
  const lo = wc - ww / 2, slope = info.rescaleSlope, icpt = info.rescaleIntercept;
  const img = ctx.createImageData(info.columns, info.rows);
  for (let i = 0; i < n; i++) {
    let v = ((px[i] * slope + icpt - lo) / ww) * 255;
    v = v < 0 ? 0 : v > 255 ? 255 : v;
    const j = i * 4;
    img.data[j] = img.data[j + 1] = img.data[j + 2] = v;
    img.data[j + 3] = 255;
  }
  ctx.putImageData(img, 0, 0);
  status();
}

function status(text) {
  const ttf = t0.first ? `${(t0.first - t0.open).toFixed(0)} ms to first frame` : "";
  $("status").textContent = text ?? `slice ${cursor + 1}/${total} · ${frames.size} cached · ${ttf}`;
}

addEventListener("wheel", (e) => { e.preventDefault(); move(Math.sign(e.deltaY)); }, { passive: false });
addEventListener("keydown", (e) => {
  if (e.key === "ArrowDown" || e.key === "ArrowRight") move(1);
  if (e.key === "ArrowUp" || e.key === "ArrowLeft") move(-1);
});
setInterval(() => { velocity *= 0.5; }, 250); // decay when user stops scrolling

init().catch((e) => status(`error: ${e}`));
