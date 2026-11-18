//! Scroll-aware prefetch ordering.
//!
//! The scheduler answers one question, over and over: *given where the user is
//! looking and which way they are scrolling, which slice should go on the wire
//! next?* It is pure logic with no I/O, so it is easy to test and to reuse in
//! other transports (HTTP/2 push, WebTransport, a WASM client-side planner...).

/// Tuning knobs for [`PrefetchScheduler`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SchedulerConfig {
    /// Cost multiplier for slices *behind* the scroll direction. `4.0` means a
    /// slice 2 behind is as urgent as a slice 8 ahead (ties go to the slice ahead).
    pub behind_penalty: f64,
    /// Stop prefetching beyond this many slices from the cursor (`None` = whole series).
    pub max_distance: Option<usize>,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            behind_penalty: 4.0,
            max_distance: None,
        }
    }
}

/// Decides the order in which slices are streamed.
#[derive(Debug, Clone)]
pub struct PrefetchScheduler {
    config: SchedulerConfig,
    sent: Vec<bool>,
    remaining: usize,
    cursor: usize,
    /// Slices per second; positive = increasing index.
    velocity: f64,
}

impl PrefetchScheduler {
    pub fn new(len: usize, config: SchedulerConfig) -> Self {
        Self {
            config,
            sent: vec![false; len],
            remaining: len,
            cursor: 0,
            velocity: 0.0,
        }
    }

    pub fn len(&self) -> usize {
        self.sent.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sent.is_empty()
    }

    /// Number of slices not yet handed out.
    pub fn remaining(&self) -> usize {
        self.remaining
    }

    /// Whether the slice has been handed out (or marked sent). Out of range is `false`.
    pub fn is_sent(&self, index: usize) -> bool {
        self.sent.get(index).copied().unwrap_or(false)
    }

    /// Update the viewer state. Call on every scroll event (cheap).
    pub fn update(&mut self, cursor: usize, velocity: f64) {
        self.cursor = cursor.min(self.len().saturating_sub(1));
        self.velocity = if velocity.is_finite() { velocity } else { 0.0 };
    }

    /// Forget that a slice was sent (e.g. the client evicted it from its cache).
    pub fn mark_unsent(&mut self, index: usize) {
        if let Some(s) = self.sent.get_mut(index)
            && *s
        {
            *s = false;
            self.remaining += 1;
        }
    }

    /// Mark a slice as already delivered by another path (e.g. plain HTTP fetch).
    pub fn mark_sent(&mut self, index: usize) {
        if let Some(s) = self.sent.get_mut(index)
            && !*s
        {
            *s = true;
            self.remaining -= 1;
        }
    }

    /// Return the most urgent unsent slice and mark it sent.
    ///
    /// Order: the slice under the cursor first, then by distance from the
    /// cursor, with slices behind the scroll direction penalized. O(n) per call, which is
    /// microseconds for real series (< 10k slices).
    pub fn pop_next(&mut self) -> Option<usize> {
        if self.remaining == 0 {
            return None;
        }
        let cursor = self.cursor as f64;
        // Note: f64::signum(0.0) == 1.0, so treat "not moving" explicitly.
        let direction = if self.velocity == 0.0 {
            0.0
        } else {
            self.velocity.signum()
        };

        // (index, cost, is_behind) — ties are broken in favour of slices ahead.
        let mut best: Option<(usize, f64, bool)> = None;
        for (i, &sent) in self.sent.iter().enumerate() {
            if sent {
                continue;
            }
            let dist = (i as f64 - cursor).abs();
            if let Some(max) = self.config.max_distance
                && dist > max as f64
            {
                continue;
            }
            let behind = direction != 0.0 && (i as f64 - cursor).signum() != direction;
            let cost = if i == self.cursor {
                -1.0 // always first
            } else {
                // Slices ahead are needed in distance order (the user scrolls
                // through every one of them); slices behind are discounted.
                if behind {
                    dist * self.config.behind_penalty
                } else {
                    dist
                }
            };
            if best.is_none_or(|(_, c, b)| cost < c || (cost == c && b && !behind)) {
                best = Some((i, cost, behind));
            }
        }

        let (idx, _, _) = best?;
        self.sent[idx] = true;
        self.remaining -= 1;
        Some(idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(s: &mut PrefetchScheduler, n: usize) -> Vec<usize> {
        (0..n).filter_map(|_| s.pop_next()).collect()
    }

    #[test]
    fn cursor_slice_comes_first() {
        let mut s = PrefetchScheduler::new(100, SchedulerConfig::default());
        s.update(42, 0.0);
        assert_eq!(s.pop_next(), Some(42));
    }

    #[test]
    fn idle_cursor_expands_symmetrically() {
        let mut s = PrefetchScheduler::new(100, SchedulerConfig::default());
        s.update(50, 0.0);
        let order = drain(&mut s, 5);
        assert_eq!(order[0], 50);
        let mut ring: Vec<_> = order[1..].to_vec();
        ring.sort();
        assert_eq!(ring, vec![48, 49, 51, 52]);
    }

    #[test]
    fn scrolling_forward_prefers_slices_ahead() {
        let mut s = PrefetchScheduler::new(100, SchedulerConfig::default());
        s.update(50, 20.0); // 20 slices/s forward
        let order = drain(&mut s, 5);
        assert_eq!(order[0], 50);
        assert!(order[1..].iter().all(|&i| i > 50), "{order:?}");
    }

    #[test]
    fn scrolling_backward_prefers_slices_behind() {
        let mut s = PrefetchScheduler::new(100, SchedulerConfig::default());
        s.update(50, -20.0);
        let order = drain(&mut s, 5);
        assert!(order[1..].iter().all(|&i| i < 50), "{order:?}");
    }

    #[test]
    fn forward_scroll_is_contiguous() {
        let mut s = PrefetchScheduler::new(100, SchedulerConfig::default());
        s.update(30, 40.0);
        assert_eq!(drain(&mut s, 5), vec![30, 31, 32, 33, 34]);
        assert_eq!(s.pop_next(), Some(29));
    }

    #[test]
    fn retargets_after_cursor_jump() {
        let mut s = PrefetchScheduler::new(100, SchedulerConfig::default());
        s.update(0, 0.0);
        drain(&mut s, 3);
        s.update(90, 0.0);
        assert_eq!(s.pop_next(), Some(90));
    }

    #[test]
    fn delivers_every_slice_exactly_once() {
        let mut s = PrefetchScheduler::new(37, SchedulerConfig::default());
        s.update(10, 5.0);
        let mut order = drain(&mut s, 100);
        assert_eq!(s.pop_next(), None);
        order.sort();
        assert_eq!(order, (0..37).collect::<Vec<_>>());
    }

    #[test]
    fn respects_max_distance() {
        let cfg = SchedulerConfig {
            max_distance: Some(2),
            ..Default::default()
        };
        let mut s = PrefetchScheduler::new(100, cfg);
        s.update(50, 0.0);
        assert_eq!(drain(&mut s, 100).len(), 5);
    }

    #[test]
    fn mark_unsent_requeues() {
        let mut s = PrefetchScheduler::new(3, SchedulerConfig::default());
        drain(&mut s, 3);
        s.mark_unsent(1);
        assert_eq!(s.pop_next(), Some(1));
    }
}
