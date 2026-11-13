import { test } from "node:test";
import assert from "node:assert/strict";
import { formatBytes, formatRate, formatDuration, formatNumber } from "../lib/format.js";

test("formatBytes: brief examples", () => {
  assert.equal(formatBytes(88184832), "84.1 MB");
  assert.equal(formatBytes(0), "0 B");
  assert.equal(formatBytes(1023), "1023 B");
  assert.equal(formatBytes(1536), "1.50 KB");
});

test("formatBytes: 3 significant digits across units", () => {
  assert.equal(formatBytes(1024), "1.00 KB");
  assert.equal(formatBytes(12 * 1024 + 512), "12.5 KB");
  assert.equal(formatBytes(1267015680), "1.18 GB");
  assert.equal(formatBytes(442381631488), "412 GB");
  assert.equal(formatBytes(1023.9 * 1024), "1.00 MB");
});

test("formatBytes: null and undefined become an em dash", () => {
  assert.equal(formatBytes(null), "—");
  assert.equal(formatBytes(undefined), "—");
});

test("formatRate appends /s", () => {
  assert.equal(formatRate(18454937), "17.6 MB/s");
  assert.equal(formatRate(0), "0 B/s");
  assert.equal(formatRate(null), "—");
});

test("formatDuration: HH:MM:SS", () => {
  assert.equal(formatDuration(761), "00:12:41");
  assert.equal(formatDuration(0), "00:00:00");
  assert.equal(formatDuration(360000), "100:00:00");
  assert.equal(formatDuration(null), "—");
});

test("formatNumber: fixed digits or em dash", () => {
  assert.equal(formatNumber(37.24), "37.2");
  assert.equal(formatNumber(17, 0), "17");
  assert.equal(formatNumber(2.5, 2), "2.50");
  assert.equal(formatNumber(null), "—");
  assert.equal(formatNumber(undefined), "—");
});
