// Detects a server whose sampler stopped: /api/metrics keeps answering but
// `sampled_at_ms` no longer advances.
export function createStaleTracker(limit = 3) {
  let last = null;
  let same = 0;
  return {
    // Feed one successful poll; returns true once the timestamp has not
    // advanced for `limit` consecutive polls.
    observe(sampledAtMs) {
      if (last !== null && sampledAtMs <= last) same += 1;
      else same = 0;
      if (last === null || sampledAtMs > last) last = sampledAtMs;
      return same >= limit;
    },
  };
}
