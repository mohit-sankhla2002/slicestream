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
    const size = d.calculateSizeAtDecompositionLevel(level);
    if (level === 0) d.decode();
    else d.decodeSubResolution(level, 0);
    const out = d.getDecodedBuffer();
    const copy = new Uint8Array(out.length);
    copy.set(out);
    const buf = copy.buffer;
    return { w: size.width, h: size.height, px: signed ? new Int16Array(buf) : new Uint16Array(buf) };
  } finally {
    d.delete();
  }
}

function isConstant(a) {
  for (let i = 1; i < a.length; i++) if (a[i] !== a[0]) return false;
  return true;
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
    if (full.w !== meta.w || full.h !== meta.h || full.px.length !== meta.w * meta.h) {
      fail(`full decode is ${full.w}x${full.h} (${full.px.length} samples), expected ${meta.w}x${meta.h}`);
      continue;
    }
    if (!sameSamples(full.px, raw)) {
      fail('full decode differs from raw');
      continue;
    }
    const rawConstant = isConstant(raw);
    let negativeChecked = false;
    for (let r = 0; r <= L; r++) {
      const level = L - r;
      const whole = decode(M, j2c, level, meta.signed);
      const prefix = decode(M, j2c.subarray(0, meta.ends[r]), level, meta.signed);
      const where = `resolution ${r} (level ${level})`;
      if (whole.w * whole.h === 0 || whole.px.length !== whole.w * whole.h) {
        fail(`${where}: whole decode has ${whole.px.length} samples for ${whole.w}x${whole.h}`);
        continue;
      }
      if (prefix.w !== whole.w || prefix.h !== whole.h || prefix.px.length !== whole.px.length) {
        fail(`${where}: prefix decode is ${prefix.w}x${prefix.h} (${prefix.px.length}), whole is ${whole.w}x${whole.h}`);
        continue;
      }
      if (!rawConstant && isConstant(whole.px)) fail(`${where}: whole decode is constant`);
      if (!sameSamples(whole.px, prefix.px)) fail(`${where}: prefix ${meta.ends[r]} differs from whole`);
      // Negative control: without resolution r's data the output must differ.
      if (r > 0) {
        const short = decode(M, j2c.subarray(0, meta.ends[r - 1]), level, meta.signed);
        if (!sameSamples(short.px, whole.px)) negativeChecked = true;
      }
    }
    if (L > 0 && !negativeChecked) fail('no shorter prefix ever decoded differently (truncation not detected)');
  }
  if (names.length === 0) {
    console.error('FAIL: no fixtures found');
    process.exit(1);
  }
  if (failed) process.exit(1);
  console.log(`htj2k interop: ${names.length} fixtures ok`);
});
