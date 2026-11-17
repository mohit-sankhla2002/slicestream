#!/usr/bin/env node
// Timing client for the slicestream WebSocket stream (docs/PROTOCOL.md).
// Node 22 or later (built-in WebSocket); no dependencies.
//
//   node scripts/slowlink-client.mjs ws://127.0.0.1:18081/api/series/<uid>/stream \
//     [--progressive] [--jump <index>@<ms>] [--preview-size 256] [--timeout-ms 600000]
//
// It opens the stream, sends `cursor` for the middle slice once the hello
// arrives, and in progressive mode acks every binary message (coalesced per
// event-loop turn). It prints one JSON line:
//
//   { "firstImageMs": ..., "allLosslessMs": ..., "jumpPreviewMs": ... }
//
// Times are from opening the socket. The first image is the first slice with
// its preview complete (progressive: a part whose `level` reaches the preview
// resolution) or the first whole frame. `--jump i@t` sends `cursor` for slice
// i at t ms; `jumpPreviewMs` is the time from that message until slice i has
// a preview (or is whole). All lossless means every slice is complete.

function usage(msg) {
  console.error(`error: ${msg}`);
  console.error('usage: slowlink-client.mjs <ws-url> [--progressive] [--jump <index>@<ms>] [--preview-size 256] [--timeout-ms 600000]');
  process.exit(2);
}

function parseArgs(argv) {
  const opts = { progressive: false, previewSize: 256, timeoutMs: 600_000 };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--progressive') opts.progressive = true;
    else if (a === '--jump') {
      const m = /^(\d+)@(\d+)$/.exec(argv[++i] ?? '');
      if (!m) usage('--jump takes <index>@<ms>');
      opts.jump = { index: Number(m[1]), atMs: Number(m[2]) };
    } else if (a === '--preview-size') opts.previewSize = Number(argv[++i]);
    else if (a === '--timeout-ms') opts.timeoutMs = Number(argv[++i]);
    else if (!a.startsWith('--') && !opts.url) opts.url = a;
    else usage(`unknown argument ${a}`);
  }
  if (!opts.url) usage('a ws:// URL is required');
  return opts;
}

// Resolution levels of the HTJ2K copy (the server's encoder rule): the
// smallest L with ceil(min(rows, columns) / 2^L) <= 64.
function decompositionLevels(rows, columns) {
  const short = Math.min(rows, columns);
  let levels = 0;
  while (Math.ceil(short / 2 ** levels) > 64) levels++;
  return levels;
}

// The preview resolution: the smallest one at least `size` on the long side.
function previewLevel(rows, columns, size) {
  const levels = decompositionLevels(rows, columns);
  const long = Math.max(rows, columns);
  for (let r = 0; r <= levels; r++) {
    if (Math.ceil(long / 2 ** (levels - r)) >= size) return r;
  }
  return levels;
}

const opts = parseArgs(process.argv.slice(2));
let url = opts.url;
if (opts.progressive) url += (url.includes('?') ? '&' : '?') + 'progressive=htj2k';

const t0 = performance.now();
const elapsed = () => Math.round((performance.now() - t0) * 10) / 10;
const result = {};
let sliceCount = null;
let complete = 0;
let payloadBytes = 0;
let jumpSentAt = null;
let ackScheduled = false;
let finished = false;
const slices = new Map(); // index -> { held, previewed, done }

const ws = new WebSocket(url);
ws.binaryType = 'arraybuffer';

const timeout = setTimeout(() => {
  console.error(`error: timed out after ${opts.timeoutMs} ms (${complete}/${sliceCount} slices complete)`);
  process.exit(1);
}, opts.timeoutMs);

function send(msg) {
  ws.send(JSON.stringify(msg));
}

function finishIfDone() {
  if (sliceCount === null || complete < sliceCount) return;
  if (opts.jump && result.jumpPreviewMs === undefined) return;
  finished = true;
  clearTimeout(timeout);
  console.log(JSON.stringify(result));
  ws.close();
  setTimeout(() => process.exit(0), 2000).unref();
}

function slice(index) {
  let s = slices.get(index);
  if (!s) {
    s = { held: 0, previewed: false, done: false };
    slices.set(index, s);
  }
  return s;
}

function markPreview(index, s) {
  if (s.previewed) return;
  s.previewed = true;
  if (result.firstImageMs === undefined) result.firstImageMs = elapsed();
  if (opts.jump && index === opts.jump.index && jumpSentAt !== null && result.jumpPreviewMs === undefined) {
    result.jumpPreviewMs = Math.round((performance.now() - jumpSentAt) * 10) / 10;
  }
}

function markDone(index, s) {
  markPreview(index, s);
  if (s.done) return;
  s.done = true;
  complete++;
  if (complete === sliceCount) result.allLosslessMs = elapsed();
}

function onFrame(buf) {
  const view = new DataView(buf);
  const magic = String.fromCharCode(...new Uint8Array(buf, 0, 4));
  if (magic !== 'SLS1') throw new Error(`bad magic ${magic}`);
  const headerLen = view.getUint32(4, true);
  const header = JSON.parse(new TextDecoder().decode(new Uint8Array(buf, 8, headerLen)));
  const len = buf.byteLength - 8 - headerLen;
  payloadBytes += len;
  const s = slice(header.index);
  const part = header.part;
  if (!part) {
    // A whole frame replaces any parts held.
    s.held = len;
    markDone(header.index, s);
    return;
  }
  if (part.offset === 0) s.held = 0;
  if (part.offset !== s.held) return; // a stale part after `evicted`: dropped
  s.held += len;
  const { rows, columns } = header.info;
  if (part.level >= previewLevel(rows, columns, opts.previewSize)) markPreview(header.index, s);
  if (s.held === part.total) markDone(header.index, s);
}

ws.addEventListener('message', (event) => {
  if (typeof event.data === 'string') {
    const msg = JSON.parse(event.data);
    if (msg.type === 'series') {
      if (opts.progressive && msg.progressive !== 'htj2k') {
        console.error('error: the server did not confirm progressive mode');
        process.exit(1);
      }
      sliceCount = msg.series.sliceCount;
      send({ type: 'cursor', index: Math.floor(sliceCount / 2), velocity: 0 });
      if (opts.jump) {
        if (opts.jump.index >= sliceCount) usage(`--jump index ${opts.jump.index} is beyond ${sliceCount} slices`);
        setTimeout(() => {
          jumpSentAt = performance.now();
          send({ type: 'cursor', index: opts.jump.index, velocity: 0 });
          const s = slices.get(opts.jump.index);
          if (s?.previewed) {
            console.error(`warning: slice ${opts.jump.index} already had a preview at the jump`);
            result.jumpPreviewMs = 0;
          }
          finishIfDone();
        }, Math.max(0, opts.jump.atMs - (performance.now() - t0)));
      }
    }
    return;
  }
  onFrame(event.data);
  if (opts.progressive && !ackScheduled) {
    // One ack for all the messages handled in this event-loop turn.
    ackScheduled = true;
    setImmediate(() => {
      ackScheduled = false;
      if (ws.readyState === WebSocket.OPEN) send({ type: 'ack', bytes: payloadBytes });
    });
  }
  finishIfDone();
});

ws.addEventListener('error', (event) => {
  if (finished) return;
  console.error(`error: ${event.message ?? 'WebSocket error'}`);
  process.exit(1);
});
ws.addEventListener('close', () => {
  if (finished) process.exit(0);
  if (sliceCount === null || complete < sliceCount) {
    console.error(`error: stream closed with ${complete}/${sliceCount} slices complete`);
    process.exit(1);
  }
});
