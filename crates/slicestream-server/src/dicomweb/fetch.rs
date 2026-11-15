//! Stored metadata of many instances, read in order under a byte budget.
//!
//! A response that carries the metadata of many instances (WADO-RS metadata,
//! QIDO-RS `includefield`) reads it in batches: as many instances as fit in
//! [`BUDGET`] bytes of stored metadata, at most [`MAX_BATCH`], and always at
//! least one. Each instance's metadata is dropped once it has been used, so
//! a request holds at most one batch at a time.

use std::collections::{HashMap, VecDeque};

use slicestream_store::{Index, MetadataState};

/// Most stored metadata bytes one batch reads (a larger instance is read
/// alone).
pub(super) const BUDGET: u64 = 8 << 20;
/// Most instances one batch reads.
pub(super) const MAX_BATCH: usize = 128;

/// Reads the metadata of instances in order, one batch at a time.
pub(super) struct MetadataReader {
    /// Instance id and stored metadata length, in reading order.
    items: Vec<(i64, u64)>,
    /// First item not read yet.
    next: usize,
    /// Read, not handed out yet. `None`: the instance had no metadata row
    /// when it was read (deleted meanwhile).
    pending: VecDeque<(i64, Option<MetadataState>)>,
}

impl MetadataReader {
    /// `items`: instance id and the length of its stored metadata (from
    /// [`Index::metadata_states`]), in the order they are wanted.
    pub(super) fn new(items: Vec<(i64, u64)>) -> Self {
        MetadataReader {
            items,
            next: 0,
            pending: VecDeque::new(),
        }
    }

    /// The next instance and its metadata state now; `None` after the last.
    pub(super) async fn next(
        &mut self,
        index: &Index,
    ) -> anyhow::Result<Option<(i64, Option<MetadataState>)>> {
        if self.pending.is_empty() {
            let end = batch_end(&self.items, self.next, BUDGET, MAX_BATCH);
            if end == self.next {
                return Ok(None);
            }
            let ids: Vec<i64> = self.items[self.next..end].iter().map(|i| i.0).collect();
            self.next = end;
            let mut found: HashMap<i64, MetadataState> =
                index.metadata_batch(&ids).await?.into_iter().collect();
            self.pending
                .extend(ids.into_iter().map(|id| (id, found.remove(&id))));
        }
        Ok(self.pending.pop_front())
    }
}

/// End of the batch that starts at `from`: the items whose lengths add up
/// to at most `budget`, at most `max` of them, and at least one.
fn batch_end(items: &[(i64, u64)], from: usize, budget: u64, max: usize) -> usize {
    let mut end = from;
    let mut total = 0u64;
    for &(_, len) in items.iter().skip(from).take(max) {
        if end > from && total.saturating_add(len) > budget {
            break;
        }
        total = total.saturating_add(len);
        end += 1;
    }
    end
}

#[cfg(test)]
mod tests {
    use super::*;

    fn items(lens: &[u64]) -> Vec<(i64, u64)> {
        lens.iter()
            .enumerate()
            .map(|(i, &l)| (i as i64, l))
            .collect()
    }

    /// Every batch from the start, as `[from, end)` ranges.
    fn batches(lens: &[u64], budget: u64, max: usize) -> Vec<(usize, usize)> {
        let items = items(lens);
        let mut out = Vec::new();
        let mut from = 0;
        while from < items.len() {
            let end = batch_end(&items, from, budget, max);
            out.push((from, end));
            from = end;
        }
        out
    }

    #[test]
    fn batches_respect_budget_and_count() {
        const MIB: u64 = 1 << 20;
        // Two 5 MiB instances do not fit together in 8 MiB.
        assert_eq!(
            batches(&[5 * MIB, 5 * MIB, 1024], BUDGET, MAX_BATCH),
            [(0, 1), (1, 3)]
        );
        // An instance over the budget is read alone.
        assert_eq!(
            batches(&[1024, 20 * MIB, 1024], BUDGET, MAX_BATCH),
            [(0, 1), (1, 2), (2, 3)]
        );
        // Small instances: MAX_BATCH at a time.
        assert_eq!(
            batches(&[100; 200], BUDGET, MAX_BATCH),
            [(0, 128), (128, 200)]
        );
        // Exactly the budget fits.
        assert_eq!(batches(&[4 * MIB, 4 * MIB], BUDGET, MAX_BATCH), [(0, 2)]);
        assert_eq!(batch_end(&items(&[1, 2]), 2, BUDGET, MAX_BATCH), 2);
    }
}
