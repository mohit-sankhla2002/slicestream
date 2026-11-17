//! Two-stage (preview, then final) slice ordering.
//!
//! A slice with an HTJ2K copy is delivered in two stages: a small `Preview`
//! (low resolution prefix) and the `Final` remainder. Slices without a copy
//! are *single stage*: only `Final`, sent whole. Ordering builds on two
//! [`PrefetchScheduler`]s, one per stage. Pure logic, no I/O.

use crate::scheduler::{PrefetchScheduler, SchedulerConfig};

/// Delivery stage of a slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stage {
    Preview,
    Final,
}

/// Hands out `(slice, stage)` pairs, previews first where it pays off.
///
/// Order after each [`update`](Self::update):
/// 1. the cursor slice: its preview if unsent, else its final if unsent;
/// 2. previews in scheduler order, while fewer than `preview_ahead` previews
///    were handed out since the last update (the cursor's does not count);
/// 3. otherwise the next slice in scheduler order that still needs a stage:
///    its preview if unsent, else its final.
#[derive(Debug, Clone)]
pub struct StagedScheduler {
    previews: PrefetchScheduler,
    finals: PrefetchScheduler,
    single: Vec<bool>,
    preview_ahead: usize,
    previews_since_update: usize,
    cursor: usize,
}

impl StagedScheduler {
    pub fn new(len: usize, config: SchedulerConfig, preview_ahead: usize) -> Self {
        Self {
            previews: PrefetchScheduler::new(len, config),
            finals: PrefetchScheduler::new(len, config),
            single: vec![false; len],
            preview_ahead,
            previews_since_update: 0,
            cursor: 0,
        }
    }

    /// The slice has no copy: only `Final`, sent whole.
    pub fn set_single_stage(&mut self, index: usize) {
        if let Some(s) = self.single.get_mut(index) {
            *s = true;
            self.previews.mark_sent(index);
        }
    }

    /// Whether the slice was made single stage.
    pub fn is_single_stage(&self, index: usize) -> bool {
        self.single.get(index).copied().unwrap_or(false)
    }

    /// Update the viewer state and restart the preview-ahead budget.
    pub fn update(&mut self, cursor: usize, velocity: f64) {
        self.cursor = cursor.min(self.finals.len().saturating_sub(1));
        self.previews.update(cursor, velocity);
        self.finals.update(cursor, velocity);
        self.previews_since_update = 0;
    }

    /// Forget both stages of a slice (the client evicted it).
    pub fn mark_unsent(&mut self, index: usize) {
        if !self.single.get(index).copied().unwrap_or(false) {
            self.previews.mark_unsent(index);
        }
        self.finals.mark_unsent(index);
    }

    /// The slice's `Final` was interrupted; hand it out again later.
    pub fn requeue_final(&mut self, index: usize) {
        self.finals.mark_unsent(index);
    }

    pub fn pop_next(&mut self) -> Option<(usize, Stage)> {
        if self.finals.is_empty() {
            return None;
        }
        let c = self.cursor;
        if !self.previews.is_sent(c) {
            self.previews.mark_sent(c);
            return Some((c, Stage::Preview));
        }
        if !self.finals.is_sent(c) {
            self.finals.mark_sent(c);
            return Some((c, Stage::Final));
        }
        if self.previews_since_update < self.preview_ahead
            && let Some(i) = self.previews.pop_next()
        {
            self.previews_since_update += 1;
            return Some((i, Stage::Preview));
        }
        let i = self.finals.pop_next()?;
        if !self.previews.is_sent(i) {
            self.previews.mark_sent(i);
            self.finals.mark_unsent(i);
            return Some((i, Stage::Preview));
        }
        Some((i, Stage::Final))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sched(len: usize, ahead: usize) -> StagedScheduler {
        StagedScheduler::new(len, SchedulerConfig::default(), ahead)
    }

    #[test]
    fn cursor_slice_preview_then_final() {
        let mut s = sched(10, 4);
        s.update(5, 0.0);
        assert_eq!(s.pop_next(), Some((5, Stage::Preview)));
        assert_eq!(s.pop_next(), Some((5, Stage::Final)));
    }

    #[test]
    fn previews_ahead_before_finals() {
        let mut s = sched(20, 3);
        s.update(5, 10.0);
        s.pop_next();
        s.pop_next();
        assert_eq!(s.pop_next(), Some((6, Stage::Preview)));
        assert_eq!(s.pop_next(), Some((7, Stage::Preview)));
        assert_eq!(s.pop_next(), Some((8, Stage::Preview)));
        assert_eq!(s.pop_next(), Some((6, Stage::Final)));
    }

    #[test]
    fn single_stage_slice_sent_once_as_final() {
        let mut s = sched(10, 4);
        assert!(!s.is_single_stage(5));
        s.set_single_stage(5);
        assert!(s.is_single_stage(5));
        assert!(!s.is_single_stage(4) && !s.is_single_stage(99));
        s.update(5, 0.0);
        assert_eq!(s.pop_next(), Some((5, Stage::Final)));
        let rest: Vec<_> = std::iter::from_fn(|| s.pop_next()).collect();
        assert!(!rest.contains(&(5, Stage::Preview)));
        assert!(!rest.contains(&(5, Stage::Final)));
    }

    #[test]
    fn evicted_resets_both_stages() {
        let mut s = sched(5, 2);
        s.update(0, 0.0);
        while s.pop_next().is_some() {}
        s.mark_unsent(2);
        s.update(2, 0.0);
        assert_eq!(s.pop_next(), Some((2, Stage::Preview)));
        assert_eq!(s.pop_next(), Some((2, Stage::Final)));
        assert_eq!(s.pop_next(), None);
    }

    #[test]
    fn evicted_single_stage_stays_final_only() {
        let mut s = sched(3, 2);
        s.set_single_stage(1);
        while s.pop_next().is_some() {}
        s.mark_unsent(1);
        s.update(1, 0.0);
        assert_eq!(s.pop_next(), Some((1, Stage::Final)));
        assert_eq!(s.pop_next(), None);
    }

    #[test]
    fn requeue_final_leaves_preview() {
        let mut s = sched(3, 2);
        while s.pop_next().is_some() {}
        s.requeue_final(1);
        s.update(1, 0.0);
        assert_eq!(s.pop_next(), Some((1, Stage::Final)));
        assert_eq!(s.pop_next(), None);
    }

    #[test]
    fn empty_series_yields_nothing() {
        let mut s = sched(0, 2);
        s.update(0, 0.0);
        assert_eq!(s.pop_next(), None);
        s.mark_unsent(0);
        s.requeue_final(0);
        s.set_single_stage(0);
        assert_eq!(s.pop_next(), None);
    }

    #[test]
    fn cursor_out_of_range_is_clamped() {
        let mut s = sched(4, 2);
        s.update(99, 0.0);
        assert_eq!(s.pop_next(), Some((3, Stage::Preview)));
        assert_eq!(s.pop_next(), Some((3, Stage::Final)));
    }

    #[test]
    fn requeue_final_on_cursor_slice_is_next() {
        let mut s = sched(5, 2);
        s.update(2, 0.0);
        assert_eq!(s.pop_next(), Some((2, Stage::Preview)));
        assert_eq!(s.pop_next(), Some((2, Stage::Final)));
        s.requeue_final(2);
        assert_eq!(s.pop_next(), Some((2, Stage::Final)));
        assert_ne!(s.pop_next(), Some((2, Stage::Preview)));
    }

    #[test]
    fn every_stage_handed_out_exactly_once() {
        let n = 40;
        let mut s = sched(n, 5);
        s.set_single_stage(7);
        let mut seen = std::collections::HashSet::new();
        let mut x: u64 = 12345;
        loop {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let cursor = (x >> 33) as usize % n;
            let vel = ((x >> 20) % 41) as f64 - 20.0;
            s.update(cursor, vel);
            let mut any = false;
            for _ in 0..3 {
                if let Some(p) = s.pop_next() {
                    any = true;
                    assert!(seen.insert(p), "duplicate {p:?}");
                }
            }
            if !any && s.pop_next().is_none() {
                break;
            }
        }
        assert_eq!(seen.len(), 2 * n - 1);
        assert!(!seen.contains(&(7, Stage::Preview)));
    }
}
