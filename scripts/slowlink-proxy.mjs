#!/usr/bin/env node
// Throttling TCP proxy for slow-link tests. No dependencies, no root.
//
//   node scripts/slowlink-proxy.mjs --listen 18081 --target 127.0.0.1:18080 \
//     --mbit 10 --rtt-ms 80 --buffer 4194304
//
// Each direction of every connection is its own link: bytes leave at `--mbit`
// megabits per second, one after another, and arrive `--rtt-ms / 2` after
// they left. The proxy keeps reading from the sending side while fewer than
// `--buffer` bytes are queued in that direction, so it behaves like a reverse
// proxy with a large send buffer: the sender sees its data accepted long
// before the reader gets it.

import net from 'node:net';

function parseArgs(argv) {
  const opts = { mbit: 10, rttMs: 80, buffer: 4 * 1024 * 1024 };
  for (let i = 0; i < argv.length; i++) {
    const value = argv[i + 1];
    switch (argv[i]) {
      case '--listen': opts.listen = Number(value); i++; break;
      case '--target': opts.target = value; i++; break;
      case '--mbit': opts.mbit = Number(value); i++; break;
      case '--rtt-ms': opts.rttMs = Number(value); i++; break;
      case '--buffer': opts.buffer = Number(value); i++; break;
      default: usage(`unknown argument ${argv[i]}`);
    }
  }
  if (!Number.isInteger(opts.listen) || !opts.target) usage('--listen and --target are required');
  if (!(opts.mbit > 0) || !(opts.rttMs >= 0) || !(opts.buffer > 0)) usage('bad --mbit, --rtt-ms or --buffer');
  const colon = opts.target.lastIndexOf(':');
  opts.targetHost = opts.target.slice(0, colon) || '127.0.0.1';
  opts.targetPort = Number(opts.target.slice(colon + 1));
  return opts;
}

function usage(msg) {
  console.error(`error: ${msg}`);
  console.error('usage: slowlink-proxy.mjs --listen <port> --target <host:port> [--mbit 10] [--rtt-ms 80] [--buffer 4194304]');
  process.exit(2);
}

// Pieces this small keep the release times smooth (4 KiB is 3.3 ms at 10 Mbit/s).
const PIECE = 4096;

// One direction of the link: bytes read from `from` reach `to` after
// serialization at `bytesPerMs` and a one-way delay of `delayMs`.
function pipeSlow(from, to, bytesPerMs, delayMs, buffer) {
  const queue = []; // { data, at } in delivery order
  let queued = 0;
  let linkFree = 0; // time at which the link has sent everything accepted so far
  let timer = null;
  let ended = false;

  const deliver = () => {
    timer = null;
    const now = performance.now();
    while (queue.length && queue[0].at <= now) {
      const { data } = queue.shift();
      queued -= data.length;
      if (!to.destroyed) to.write(data);
    }
    if (queued < buffer && from.isPaused()) from.resume();
    if (queue.length) {
      timer = setTimeout(deliver, Math.max(0, queue[0].at - now));
    } else if (ended && !to.destroyed) {
      to.end();
    }
  };

  from.on('data', (chunk) => {
    const now = performance.now();
    for (let off = 0; off < chunk.length; off += PIECE) {
      const data = chunk.subarray(off, Math.min(off + PIECE, chunk.length));
      linkFree = Math.max(linkFree, now) + data.length / bytesPerMs;
      queue.push({ data, at: linkFree + delayMs });
      queued += data.length;
    }
    if (queued >= buffer) from.pause();
    if (!timer) timer = setTimeout(deliver, Math.max(0, queue[0].at - now));
  });
  from.on('end', () => {
    ended = true;
    if (!queue.length && !to.destroyed) to.end();
  });
  return () => {
    if (timer) clearTimeout(timer);
    queue.length = 0;
  };
}

const opts = parseArgs(process.argv.slice(2));
const bytesPerMs = (opts.mbit * 1e6) / 8 / 1000;
const oneWay = opts.rttMs / 2;

// Half-open sockets: a FIN travels through the link like the data before it.
const server = net.createServer({ allowHalfOpen: true }, (client) => {
  client.setNoDelay(true);
  const upstream = net.connect({ host: opts.targetHost, port: opts.targetPort, allowHalfOpen: true });
  upstream.setNoDelay(true);
  const stops = [
    pipeSlow(client, upstream, bytesPerMs, oneWay, opts.buffer),
    pipeSlow(upstream, client, bytesPerMs, oneWay, opts.buffer),
  ];
  const abort = () => {
    stops.forEach((stop) => stop());
    client.destroy();
    upstream.destroy();
  };
  client.on('error', abort);
  upstream.on('error', abort);
  // Each side closes once both directions have ended (see pipeSlow).
  let open = 2;
  const closed = () => { if (--open === 0) stops.forEach((stop) => stop()); };
  client.on('close', closed);
  upstream.on('close', closed);
});

server.listen(opts.listen, '127.0.0.1', () => {
  console.error(
    `slowlink-proxy: 127.0.0.1:${opts.listen} -> ${opts.targetHost}:${opts.targetPort}, ` +
      `${opts.mbit} Mbit/s, ${opts.rttMs} ms RTT, ${opts.buffer} B buffer`,
  );
});

const shutdown = () => server.close(() => process.exit(0));
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
