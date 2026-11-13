import { test } from "node:test";
import assert from "node:assert/strict";
import {
  parseFrame,
  frameSupport,
  windowLevel,
  renderGray,
  formatWL,
  seriesLabel,
  formatStudyDate,
  clampIndex,
} from "../lib/viewer-core.js";

const NATIVE = "1.2.840.10008.1.2.1";

function info(over = {}) {
  return {
    rows: 1, columns: 2, bitsAllocated: 16, bitsStored: 12, samplesPerPixel: 1,
    pixelRepresentation: 0, photometricInterpretation: "MONOCHROME2", transferSyntax: NATIVE,
    rescaleSlope: 1, rescaleIntercept: 0, windowCenter: 40, windowWidth: 400, ...over,
  };
}

function encode(header, payload) {
  const h = new TextEncoder().encode(JSON.stringify(header));
  const buf = new Uint8Array(8 + h.length + payload.length);
  buf.set([0x53, 0x4c, 0x53, 0x31], 0); // "SLS1"
  new DataView(buf.buffer).setUint32(4, h.length, true);
  buf.set(h, 8);
  buf.set(payload, 8 + h.length);
  return buf.buffer;
}

test("parseFrame: decodes header and payload", () => {
  const f = parseFrame(encode({ index: 7, info: info() }, new Uint8Array([1, 2, 3, 4])));
  assert.equal(f.index, 7);
  assert.equal(f.info.columns, 2);
  assert.deepEqual([...new Uint8Array(f.data)], [1, 2, 3, 4]);
});

test("parseFrame: rejects wrong magic and short buffers", () => {
  const buf = encode({ index: 0, info: info() }, new Uint8Array(2));
  new Uint8Array(buf)[3] = 0x32; // "SLS2"
  assert.equal(parseFrame(buf), null);
  assert.equal(parseFrame(new ArrayBuffer(3)), null);
  const bad = new Uint8Array(12);
  bad.set([0x53, 0x4c, 0x53, 0x31]);
  new DataView(bad.buffer).setUint32(4, 1000, true); // header longer than buffer
  assert.equal(parseFrame(bad.buffer), null);
});

test("frameSupport: native grayscale only", () => {
  assert.equal(frameSupport(info()), null);
  assert.equal(frameSupport(info({ transferSyntax: "1.2.840.10008.1.2" })), null);
  assert.equal(frameSupport(info({ bitsAllocated: 8 })), null);
  assert.match(frameSupport(info({ transferSyntax: "1.2.840.10008.1.2.4.201" })), /unsupported transfer syntax/);
  assert.match(frameSupport(info({ samplesPerPixel: 3 })), /color/);
  assert.match(frameSupport(info({ bitsAllocated: 32 })), /32-bit/);
});

test("windowLevel: frame values, else client.js defaults", () => {
  assert.deepEqual(windowLevel(info({ windowCenter: 50, windowWidth: 350 })), { center: 50, width: 350 });
  assert.deepEqual(windowLevel(info({ windowCenter: null, windowWidth: null })), { center: 40, width: 400 });
  assert.deepEqual(windowLevel(info({ windowCenter: undefined, windowWidth: 0 })), { center: 40, width: 400 });
});

test("renderGray: modality LUT then linear window", () => {
  // stored 1064 with intercept -1024 -> 40 HU = center -> 127.5; 2000 -> clamp 255; 0 -> clamp 0
  const i = info({ columns: 3, rescaleIntercept: -1024 });
  const px = new Uint16Array([1064, 2000, 0]);
  const out = new Uint8ClampedArray(12);
  renderGray(i, px.buffer, out);
  assert.deepEqual([...out], [128, 128, 128, 255, 255, 255, 255, 255, 0, 0, 0, 255]);
});

test("renderGray: signed 16-bit and 8-bit", () => {
  const s = info({ pixelRepresentation: 1, windowCenter: 0, windowWidth: 200 });
  const out = new Uint8ClampedArray(8);
  renderGray(s, new Int16Array([-100, 100]).buffer, out);
  assert.deepEqual([out[0], out[4]], [0, 255]);
  const b = info({ bitsAllocated: 8, windowCenter: 127.5, windowWidth: 255 });
  renderGray(b, new Uint8Array([0, 255]).buffer, out);
  assert.deepEqual([out[0], out[4], out[7]], [0, 255, 255]);
});

test("renderGray: MONOCHROME1 is inverted", () => {
  const i = info({ photometricInterpretation: "MONOCHROME1", windowCenter: 0, windowWidth: 200 });
  const out = new Uint8ClampedArray(8);
  renderGray(i, new Uint16Array([0, 100]).buffer, out);
  assert.deepEqual([out[0], out[4]], [128, 0]);
});

test("renderGray: short payload renders what is there, rest black", () => {
  const out = new Uint8ClampedArray(8).fill(9);
  renderGray(info({ windowCenter: 200, windowWidth: 400 }), new Uint16Array([400]).buffer, out);
  assert.deepEqual([...out], [255, 255, 255, 255, 0, 0, 0, 255]);
});

test("formatWL", () => {
  assert.equal(formatWL(400), "400");
  assert.equal(formatWL(40.5), "40.5");
  assert.equal(formatWL(-1000.25), "-1000.3");
});

test("seriesLabel: description, else modality and UID tail", () => {
  assert.equal(seriesLabel({ series_description: "CT Axial 1.0", modality: "CT", series_uid: "1.2.3" }), "CT Axial 1.0");
  assert.equal(seriesLabel({ series_description: "  ", modality: "MR", series_uid: "1.2.840.99.123456789" }), "MR · …23456789");
  assert.equal(seriesLabel({ series_description: null, modality: null, series_uid: "1.2.3" }), "? · 1.2.3");
});

test("formatStudyDate", () => {
  assert.equal(formatStudyDate("20261113"), "2026-11-13");
  assert.equal(formatStudyDate("2026-11-13"), "2026-11-13");
  assert.equal(formatStudyDate(null), "—");
  assert.equal(formatStudyDate(""), "—");
});

test("clampIndex", () => {
  assert.equal(clampIndex(-3, 10), 0);
  assert.equal(clampIndex(12, 10), 9);
  assert.equal(clampIndex(4, 10), 4);
});
