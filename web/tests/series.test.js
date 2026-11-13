import { test } from "node:test";
import assert from "node:assert/strict";
import { SampleRing, linePath, areaPath, niceMax } from "../lib/series.js";

const s = (t, v) => ({ sampled_at_ms: t, v });

test("SampleRing.values maps samples through the selector", () => {
  const r = new SampleRing();
  r.push(s(1000, 1));
  r.push(s(2000, 2));
  assert.deepEqual(r.values((x) => x.v), [1, 2]);
});

test("SampleRing inserts a null gap when samples are > 3 s apart", () => {
  const r = new SampleRing();
  r.push(s(1000, 1));
  r.push(s(4000, 2)); // exactly 3 s: no gap
  r.push(s(7001, 3)); // 3001 ms: gap
  assert.deepEqual(r.values((x) => x.v), [1, 2, null, 3]);
});

test("SampleRing selector returning null/undefined yields null", () => {
  const r = new SampleRing();
  r.push({ sampled_at_ms: 1, v: null });
  r.push({ sampled_at_ms: 2 });
  assert.deepEqual(r.values((x) => x.v), [null, null]);
});

test("SampleRing keeps at most cap entries (default 300)", () => {
  const r = new SampleRing();
  for (let i = 0; i < 350; i++) r.push(s(i * 1000, i));
  const v = r.values((x) => x.v);
  assert.equal(v.length, 300);
  assert.equal(v[0], 50);
  assert.equal(v[299], 349);
});

test("SampleRing custom cap", () => {
  const r = new SampleRing(3);
  for (let i = 0; i < 5; i++) r.push(s(i * 1000, i));
  assert.deepEqual(r.values((x) => x.v), [2, 3, 4]);
});

test("SampleRing.load replaces contents and applies gaps", () => {
  const r = new SampleRing();
  r.push(s(0, 99));
  r.load([s(1000, 1), s(2000, 2), s(9000, 3)]);
  assert.deepEqual(r.values((x) => x.v), [1, 2, null, 3]);
});

test("linePath: null breaks the line into two M commands", () => {
  const d = linePath([1, null, 3], 100, 10, 3);
  assert.equal((d.match(/M/g) || []).length, 2);
});

test("linePath: scales to width and height, y inverted", () => {
  const d = linePath([0, 2], 100, 10, 2);
  assert.equal(d, "M0 10 L100 0");
});

test("linePath: max defaults to max of values, or 1", () => {
  assert.equal(linePath([0, 4], 100, 10), "M0 10 L100 0");
  assert.equal(linePath([0, 0], 100, 10), "M0 10 L100 10");
});

test("linePath: empty or all-null gives empty path", () => {
  assert.equal(linePath([], 100, 10), "");
  assert.equal(linePath([null, null], 100, 10), "");
});

test("SampleRing ignores a sample that is not newer than the last", () => {
  const r = new SampleRing();
  r.push(s(1000, 1));
  r.push(s(1000, 1));
  r.push(s(500, 0));
  assert.deepEqual(r.values((x) => x.v), [1]);
});

test("areaPath closes each unbroken run to the baseline", () => {
  assert.equal(areaPath([0, 2], 100, 10, 2), "M0 10 L0 10 L100 0 L100 10 Z");
  const d = areaPath([1, null, 3], 100, 10, 3);
  assert.equal((d.match(/Z/g) || []).length, 2);
  assert.equal(areaPath([], 100, 10), "");
});

test("niceMax rounds up to 1/2/5 x 10^n", () => {
  assert.equal(niceMax(0), 1);
  assert.equal(niceMax(null), 1);
  assert.equal(niceMax(0.3), 0.5);
  assert.equal(niceMax(1), 1);
  assert.equal(niceMax(37.2), 50);
  assert.equal(niceMax(84.1), 100);
  assert.equal(niceMax(120), 200);
});
