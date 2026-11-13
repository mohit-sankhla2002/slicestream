//! Ingest counters and a sliding latency window, read by the metrics sampler.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const WINDOW: Duration = Duration::from_secs(60);
const MAX_ENTRIES: usize = 10_000;

/// A snapshot of ingest activity since the ingestor opened.
#[derive(Debug, Clone, Copy, PartialEq, Default, serde::Serialize)]
pub struct IngestStats {
    /// Instances stored or replaced.
    pub stored: u64,
    /// Instances that were already indexed (duplicates).
    pub duplicate: u64,
    /// Calls to `ingest` or `ingest_nifti` that returned an error.
    pub failed: u64,
    /// Spooled bytes of every successful ingest (DICOM and NIfTI).
    pub bytes: u64,
    /// NIfTI volumes stored.
    pub nifti_stored: u64,
    /// NIfTI volumes that were already indexed (duplicates).
    pub nifti_duplicate: u64,
    /// Median DICOM ingest latency over the last 60 s, in milliseconds.
    pub latency_p50_ms: Option<f64>,
    /// 95th percentile DICOM ingest latency over the last 60 s, in milliseconds.
    pub latency_p95_ms: Option<f64>,
}

/// Successful-ingest latencies from the last 60 s, at most 10 000 of them.
#[derive(Debug, Default)]
pub(crate) struct LatencyWindow {
    entries: VecDeque<(Instant, f64)>,
}

impl LatencyWindow {
    pub(crate) fn record(&mut self, at: Instant, ms: f64) {
        self.entries.push_back((at, ms));
        while self.entries.len() > MAX_ENTRIES {
            self.entries.pop_front();
        }
    }

    pub(crate) fn percentiles(&mut self, now: Instant) -> (Option<f64>, Option<f64>) {
        while let Some(&(at, _)) = self.entries.front() {
            if now.saturating_duration_since(at) > WINDOW {
                self.entries.pop_front();
            } else {
                break;
            }
        }
        if self.entries.is_empty() {
            return (None, None);
        }
        let mut v: Vec<f64> = self.entries.iter().map(|&(_, ms)| ms).collect();
        v.sort_by(f64::total_cmp);
        let rank = |p: f64| v[((p / 100.0 * v.len() as f64).ceil() as usize).clamp(1, v.len()) - 1];
        (Some(rank(50.0)), Some(rank(95.0)))
    }
}

/// Live counters owned by the ingestor.
#[derive(Debug, Default)]
pub(crate) struct Stats {
    stored: AtomicU64,
    duplicate: AtomicU64,
    failed: AtomicU64,
    bytes: AtomicU64,
    nifti_stored: AtomicU64,
    nifti_duplicate: AtomicU64,
    window: Mutex<LatencyWindow>,
}

impl Stats {
    pub(crate) fn record_success(&self, duplicate: bool, bytes: u64, elapsed: Duration) {
        let counter = if duplicate {
            &self.duplicate
        } else {
            &self.stored
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        let mut w = self.window.lock().unwrap_or_else(|e| e.into_inner());
        w.record(Instant::now(), elapsed.as_secs_f64() * 1000.0);
    }

    /// A successful `ingest_nifti`. Not part of the latency window.
    pub(crate) fn record_nifti_success(&self, duplicate: bool, bytes: u64) {
        let counter = if duplicate {
            &self.nifti_duplicate
        } else {
            &self.nifti_stored
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn record_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> IngestStats {
        let (p50, p95) = self
            .window
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .percentiles(Instant::now());
        IngestStats {
            stored: self.stored.load(Ordering::Relaxed),
            duplicate: self.duplicate.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            nifti_stored: self.nifti_stored.load(Ordering::Relaxed),
            nifti_duplicate: self.nifti_duplicate.load(Ordering::Relaxed),
            latency_p50_ms: p50,
            latency_p95_ms: p95,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_nearest_rank() {
        let mut w = LatencyWindow::default();
        let t = Instant::now();
        for ms in 1..=100 {
            w.record(t, ms as f64);
        }
        assert_eq!(w.percentiles(t), (Some(50.0), Some(95.0)));
    }

    #[test]
    fn window_drops_entries_older_than_60s() {
        let mut w = LatencyWindow::default();
        let t0 = Instant::now();
        w.record(t0, 500.0);
        w.record(t0 + Duration::from_secs(61), 10.0);
        assert_eq!(
            w.percentiles(t0 + Duration::from_secs(61)),
            (Some(10.0), Some(10.0))
        );
    }

    #[test]
    fn window_empty_is_none() {
        assert_eq!(
            LatencyWindow::default().percentiles(Instant::now()),
            (None, None)
        );
    }

    #[test]
    fn window_caps_at_10000() {
        let mut w = LatencyWindow::default();
        let t = Instant::now();
        for i in 0..10_050 {
            w.record(t, i as f64);
        }
        assert_eq!(w.percentiles(t).0, Some(5_049.0));
    }
}
