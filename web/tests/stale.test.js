import { test } from "node:test";
import assert from "node:assert/strict";
import { createStaleTracker } from "../lib/stale.js";

test("stale after 3 polls without advance, live again when it advances", () => {
  const t = createStaleTracker();
  assert.equal(t.observe(1000), false);
  assert.equal(t.observe(1000), false); // 1
  assert.equal(t.observe(1000), false); // 2
  assert.equal(t.observe(1000), true); // 3
  assert.equal(t.observe(1000), true);
  assert.equal(t.observe(2000), false);
});

test("advancing samples are never stale", () => {
  const t = createStaleTracker();
  for (let i = 1; i < 10; i++) assert.equal(t.observe(i * 1000), false);
});
