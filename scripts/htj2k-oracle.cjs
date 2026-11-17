#!/usr/bin/env node
// Decodes slicestream HTJ2K fixtures with Cornerstone's OpenJPH WASM decoder.
// Usage: node htj2k-oracle.cjs <node_modules_dir> <fixture_dir>
'use strict';
const fs = require('fs');
const path = require('path');

const [nodeModules, fixtureDir] = process.argv.slice(2);
if (!nodeModules || !fixtureDir) {
  console.error('usage: htj2k-oracle.cjs <node_modules_dir> <fixture_dir>');
  process.exit(2);
}
const factory = require(path.join(nodeModules, '@cornerstonejs/codec-openjph/dist/openjphjs.js'));

function decode(M, bytes, level, signed) {
  const d = new M.HTJ2KDecoder();
  try {
    d.getEncodedBuffer(bytes.length).set(bytes);
    d.readHeader();
    if (level === 0) d.decode();
    else d.decodeSubResolution(level, 0);
    const out = d.getDecodedBuffer();
    const copy = new Uint8Array(out.length);
    copy.set(out);
    const buf = copy.buffer;
    return signed ? new Int16Array(buf) : new Uint16Array(buf);
  } finally {
    d.delete();
  }
}

function sameSamples(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

factory({ print() {}, printErr() {} }).then((M) => {
  const names = fs.readdirSync(fixtureDir).filter((f) => f.endsWith('.json')).map((f) => f.slice(0, -5)).sort();
  let failed = false;
  for (const name of names) {
    const meta = JSON.parse(fs.readFileSync(path.join(fixtureDir, `${name}.json`), 'utf8'));
    const j2c = new Uint8Array(fs.readFileSync(path.join(fixtureDir, `${name}.j2c`)));
    const rawBuf = fs.readFileSync(path.join(fixtureDir, `${name}.raw`));
    const raw = new Int32Array(rawBuf.buffer, rawBuf.byteOffset, rawBuf.length / 4);
    const L = meta.ends.length - 1;
    const fail = (what) => {
      console.error(`FAIL ${name}: ${what}`);
      failed = true;
    };

    const full = decode(M, j2c, 0, meta.signed);
    if (!sameSamples(full, raw)) {
      fail('full decode differs from raw');
      continue;
    }
    for (let r = 0; r <= L; r++) {
      const level = L - r;
      const whole = decode(M, j2c, level, meta.signed);
      const prefix = decode(M, j2c.subarray(0, meta.ends[r]), level, meta.signed);
      if (!sameSamples(whole, prefix)) fail(`resolution ${r} (level ${level}): prefix ${meta.ends[r]} differs from whole`);
    }
  }
  if (failed) process.exit(1);
  console.log(`${names.length} fixtures decoded`);
});
