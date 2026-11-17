//! Ack-driven send window.
//!
//! The sender may have at most `window` unacknowledged payload bytes in
//! flight. The window adapts to the measured delivery rate. Pure logic: the
//! caller supplies `now`, the time since the connection started, so core never
//! reads a clock.
//!
//! RTT estimate: each send records `(time, cumulative bytes sent)`. An ack
//! for total `A` was sent at the time of the first send record with
//! cumulative bytes >= `A`; `ack time - that time` is an RTT sample, smoothed
//! as `srtt = (7 * srtt + sample) / 8` (first sample taken as is).
//!
//! Window rule: the newest earlier ack sample at least `srtt` old gives the
//! bytes acknowledged over the last round trip; `window = clamp(2 * bytes,
//! FLOOR, CEILING)`. Until such a sample exists the window stays `INITIAL`.

use std::collections::VecDeque;
use std::time::Duration;

pub const INITIAL: u64 = 262_144;
pub const FLOOR: u64 = 131_072;
pub const CEILING: u64 = 16_777_216;

#[derive(Debug, Clone)]
pub struct FlowWindow {
    window: u64,
    sent: u64,
    acked: u64,
    /// `(send time, cumulative bytes sent)`, oldest first, only unacked.
    sends: VecDeque<(Duration, u64)>,
    /// `(ack time, total acked)`, oldest first.
    acks: VecDeque<(Duration, u64)>,
    srtt: Option<Duration>,
}

impl Default for FlowWindow {
    fn default() -> Self {
        Self::new()
    }
}

impl FlowWindow {
    pub fn new() -> FlowWindow {
        FlowWindow {
            window: INITIAL,
            sent: 0,
            acked: 0,
            sends: VecDeque::new(),
            acks: VecDeque::new(),
            srtt: None,
        }
    }

    pub fn on_sent(&mut self, bytes: u64, now: Duration) {
        if bytes == 0 {
            return;
        }
        self.sent += bytes;
        self.sends.push_back((now, self.sent));
    }

    pub fn on_ack(&mut self, total_acked: u64, now: Duration) {
        let total = total_acked.min(self.sent);
        if total <= self.acked {
            return;
        }
        self.acked = total;
        while self.sends.front().is_some_and(|&(_, cum)| cum < total) {
            self.sends.pop_front();
        }
        if let Some(&(sent_at, _)) = self.sends.front() {
            let sample = now.saturating_sub(sent_at);
            self.srtt = Some(match self.srtt {
                None => sample,
                Some(s) => (s * 7 + sample) / 8,
            });
        }
        let srtt = self.srtt.unwrap_or_default();
        // Newest earlier sample that is at least one RTT old.
        let base = self
            .acks
            .iter()
            .rev()
            .find(|&&(t, _)| t + srtt <= now)
            .copied();
        if let Some((t, acked)) = base {
            self.window = (2 * (total - acked)).clamp(FLOOR, CEILING);
            // Samples older than the base are no longer needed.
            while self.acks.front().is_some_and(|&(ft, _)| ft < t) {
                self.acks.pop_front();
            }
        }
        self.acks.push_back((now, total));
    }

    pub fn window(&self) -> u64 {
        self.window
    }

    pub fn unacked(&self) -> u64 {
        self.sent - self.acked
    }

    /// Bytes that may still be sent: window minus unacked, saturating.
    pub fn room(&self) -> u64 {
        self.window.saturating_sub(self.unacked())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// Link of `bytes_per_s` with `rtt_ms` latency; sender sends whenever it
    /// has room, in 16 KiB parts, every 1 ms tick, for `secs` seconds.
    fn simulate(bytes_per_s: u64, rtt_ms: u64, secs: u64) -> FlowWindow {
        let mut f = FlowWindow::new();
        let mut inflight: VecDeque<(u64, u64)> = VecDeque::new(); // (ack due ms, cumulative)
        let mut link_free_ms: f64 = 0.0;
        let mut sent = 0u64;
        for t in 0..secs * 1000 {
            while let Some(&(due, cum)) = inflight.front() {
                if due > t {
                    break;
                }
                f.on_ack(cum, ms(t));
                inflight.pop_front();
            }
            while f.room() >= 16_384 {
                f.on_sent(16_384, ms(t));
                sent += 16_384;
                let start = link_free_ms.max(t as f64);
                link_free_ms = start + 16_384.0 * 1000.0 / bytes_per_s as f64;
                inflight.push_back((link_free_ms.ceil() as u64 + rtt_ms, sent));
            }
        }
        f
    }

    #[test]
    fn window_blocks_without_acks() {
        let mut f = FlowWindow::new();
        f.on_sent(INITIAL, ms(0));
        assert_eq!(f.room(), 0);
        assert_eq!(f.unacked(), INITIAL);
    }

    #[test]
    fn over_ack_is_clamped() {
        let mut f = FlowWindow::new();
        f.on_sent(1000, ms(0));
        f.on_ack(5000, ms(10));
        assert_eq!(f.unacked(), 0);
    }

    #[test]
    fn window_grows_with_fast_acks() {
        let f = simulate(12_500_000, 80, 5);
        assert!(f.window() >= 1_000_000, "{}", f.window());
        assert!(f.window() <= CEILING);
    }

    #[test]
    fn window_floor_on_slow_acks() {
        let f = simulate(1_250, 80, 60);
        assert_eq!(f.window(), FLOOR);
    }

    #[test]
    fn stale_ack_ignored() {
        let mut f = FlowWindow::new();
        f.on_sent(1000, ms(0));
        f.on_ack(800, ms(10));
        f.on_ack(500, ms(20));
        assert_eq!(f.unacked(), 200);
    }

    #[test]
    fn window_stays_initial_until_two_samples() {
        let mut f = FlowWindow::new();
        f.on_sent(1000, ms(0));
        f.on_ack(500, ms(50));
        assert_eq!(f.window(), INITIAL);
    }
}
