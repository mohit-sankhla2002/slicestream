import { test } from "node:test";
import assert from "node:assert/strict";
import { createThrottle } from "../lib/throttle.js";

function fakeClock() {
  let t = 0;
  const timers = [];
  return {
    now: () => t,
    setTimeout: (fn, ms) => { const h = { at: t + ms, fn }; timers.push(h); return h; },
    clearTimeout: (h) => { const i = timers.indexOf(h); if (i >= 0) timers.splice(i, 1); },
    advance(ms) {
      t += ms;
      for (;;) {
        timers.sort((a, b) => a.at - b.at);
        if (!timers.length || timers[0].at > t) break;
        timers.shift().fn();
      }
    },
    pending: () => timers.length,
  };
}

test("first call runs immediately", () => {
  const c = fakeClock();
  let n = 0;
  const th = createThrottle(() => n++, 1000, c);
  th.call();
  assert.equal(n, 1);
});

test("burst inside the interval collapses into one trailing call", () => {
  const c = fakeClock();
  let n = 0;
  const th = createThrottle(() => n++, 1000, c);
  th.call();
  c.advance(100); th.call();
  c.advance(100); th.call();
  c.advance(100); th.call();
  assert.equal(n, 1);
  assert.equal(c.pending(), 1);
  c.advance(699);
  assert.equal(n, 1);
  c.advance(1);
  assert.equal(n, 2);
});

test("calls spaced beyond the interval each run at once", () => {
  const c = fakeClock();
  let n = 0;
  const th = createThrottle(() => n++, 1000, c);
  th.call(); c.advance(1500); th.call(); c.advance(1000); th.call();
  assert.equal(n, 3);
  assert.equal(c.pending(), 0);
});

test("cancel drops the trailing call", () => {
  const c = fakeClock();
  let n = 0;
  const th = createThrottle(() => n++, 1000, c);
  th.call(); th.call(); th.cancel();
  c.advance(5000);
  assert.equal(n, 1);
});
