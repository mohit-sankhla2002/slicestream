//! Ack-driven send window.
//!
//! The sender may have at most `window` unacknowledged payload bytes in
//! flight. The window adapts to the measured delivery rate. Pure logic: the
//! caller supplies `now`, the time since the connection started, so core never
//! reads a clock.
//!
//! All byte counts are binary message PAYLOAD bytes only (not the `SLS1`
//! header): `on_sent` takes what the server sends, and the client's
//! `ack { bytes }` is the total payload it has received.
//!
//! RTT estimate: each send records `(time, cumulative bytes sent)`. An ack
//! for total `A` was sent at the time of the first send record with
//! cumulative bytes >= `A`; `ack time - that time` is an RTT sample. The
//! round-trip interval used below is the *minimum* sample seen since the
//! connection started: a smoothed sample would include the sender's own
//! queueing (about window / rate) and make the window grow without bound.
//!
//! Window rule: the newest earlier ack sample at least one round trip old
//! gives the bytes acknowledged over the last round trip; `window = clamp(2 *
//! bytes, FLOOR, CEILING)`. Until such a sample exists the window stays
//! `INITIAL`.
//!
//! App-limited rule: a send is window-limited when, right after it, at least
//! half the window is unacked. The window is never *decreased* from an
//! interval without a window-limited send, since a low rate then reflects an
//! idle sender, not the link. Increases always apply.

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
    /// `(ack time, total acked, limited_sends)`, oldest first.
    acks: VecDeque<(Duration, u64, u64)>,
    min_rtt: Option<Duration>,
    /// Count of window-limited sends so far.
    limited_sends: u64,
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
            min_rtt: None,
            limited_sends: 0,
        }
    }

    pub fn on_sent(&mut self, bytes: u64, now: Duration) {
        if bytes == 0 {
            return;
        }
        self.sent += bytes;
        self.sends.push_back((now, self.sent));
        if self.unacked() * 2 >= self.window {
            self.limited_sends += 1;
        }
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
            self.min_rtt = Some(self.min_rtt.map_or(sample, |m| m.min(sample)));
        }
        let rtt = self.min_rtt.unwrap_or_default();
        // Newest earlier sample that is at least one RTT old.
        let base = self
            .acks
            .iter()
            .rev()
            .find(|&&(t, _, _)| t + rtt <= now)
            .copied();
        if let Some((t, acked, limited)) = base {
            let candidate = (2 * (total - acked)).clamp(FLOOR, CEILING);
            if candidate >= self.window || self.limited_sends > limited {
                self.window = candidate;
            }
            // Samples older than the base are no longer needed.
            while self.acks.front().is_some_and(|&(ft, _, _)| ft < t) {
                self.acks.pop_front();
            }
        }
        self.acks.push_back((now, total, self.limited_sends));
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

    const PART: u64 = 65_536;

    /// Simulated link of `bytes_per_s` with `rtt_ms` latency, driven by an
    /// explicit 1 ms clock. The sender sends 64 KiB parts whenever it has room.
    struct Sim {
        f: FlowWindow,
        bytes_per_s: u64,
        rtt_ms: u64,
        t: u64,
        inflight: VecDeque<(u64, u64)>, // (ack due ms, cumulative)
        link_free_ms: f64,
        sent: u64,
        part: u64,
    }

    impl Sim {
        fn new(bytes_per_s: u64, rtt_ms: u64) -> Sim {
            Sim {
                f: FlowWindow::new(),
                bytes_per_s,
                rtt_ms,
                t: 0,
                inflight: VecDeque::new(),
                link_free_ms: 0.0,
                sent: 0,
                part: PART,
            }
        }

        /// Run `ms` milliseconds; the sender only sends if `sending`.
        fn run(&mut self, ms_total: u64, sending: bool) {
            for _ in 0..ms_total {
                self.t += 1;
                while let Some(&(due, cum)) = self.inflight.front() {
                    if due > self.t {
                        break;
                    }
                    self.f.on_ack(cum, ms(self.t));
                    self.inflight.pop_front();
                }
                while sending && self.f.room() >= self.part {
                    self.f.on_sent(self.part, ms(self.t));
                    self.sent += self.part;
                    let start = self.link_free_ms.max(self.t as f64);
                    self.link_free_ms = start + self.part as f64 * 1000.0 / self.bytes_per_s as f64;
                    self.inflight
                        .push_back((self.link_free_ms.ceil() as u64 + self.rtt_ms, self.sent));
                }
            }
        }
    }

    fn simulate(bytes_per_s: u64, rtt_ms: u64, secs: u64) -> FlowWindow {
        let mut sim = Sim::new(bytes_per_s, rtt_ms);
        sim.run(secs * 1000, true);
        sim.f
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
        // 10 kbit/s with 1 KiB parts (a 64 KiB part would take 52 s).
        let mut sim = Sim::new(1_250, 80);
        sim.part = 1024;
        sim.run(600_000, true);
        assert_eq!(sim.f.window(), FLOOR);
    }

    #[test]
    fn slow_links_do_not_inflate_the_window() {
        // 1 Mbit/s: the window must not chase its own queueing delay.
        let f = simulate(125_000, 80, 120);
        assert!(f.window() <= 512 * 1024, "{}", f.window());
        // 10 Mbit/s.
        let f = simulate(1_250_000, 80, 120);
        assert!(f.window() <= 768 * 1024, "{}", f.window());
    }

    #[test]
    fn idle_gap_does_not_shrink_window() {
        let mut sim = Sim::new(12_500_000, 80);
        sim.run(5_000, true);
        let before = sim.f.window();
        assert!(before >= 1_000_000, "{before}");
        sim.run(3_000, false);
        sim.run(2_000, true);
        assert!(sim.f.window() >= before, "{} < {before}", sim.f.window());
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
