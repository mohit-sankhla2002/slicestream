// Run `fn` at most once per `intervalMs`. A call inside the interval is not
// dropped: it becomes one trailing run at the end of the interval. Pure; the
// clock is injectable for tests.

const realClock = {
  now: () => Date.now(),
  setTimeout: (fn, ms) => setTimeout(fn, ms),
  clearTimeout: (h) => clearTimeout(h),
};

export function createThrottle(fn, intervalMs, clock = realClock) {
  let last = -Infinity;
  let timer = null;

  const run = () => {
    timer = null;
    last = clock.now();
    fn();
  };

  return {
    call() {
      if (timer !== null) return; // trailing run already queued
      const wait = last + intervalMs - clock.now();
      if (wait <= 0) run();
      else timer = clock.setTimeout(run, wait);
    },
    cancel() {
      if (timer !== null) clock.clearTimeout(timer);
      timer = null;
    },
  };
}
