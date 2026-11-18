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
//! round-trip interval used below is the *minimum* sample since the
//! last probe (see below), or since the connection started: a smoothed sample
//! would include the sender's own queueing (about window / rate) and make the
//! window grow without bound, while a minimum that never resets could not
//! follow a path whose base delay rises.
//!
//! Window rule: the newest earlier ack sample at least one round trip old
//! gives the bytes acknowledged over the last round trip; `window = clamp(2 *
//! bytes, FLOOR, CEILING)`. Until such a sample exists the window stays
//! `INITIAL`.
//!
//! App-limited rule: a send is window-limited when, right after it, at least
//! half the window is unacked. The window is never *decreased* from an
//! interval without a window-limited send, since a low rate then reflects an
//! idle sender, not the link. Likewise not from a base sample taken before the
//! pipe last went from empty to non-empty (an idle gap): the interval then
//! spans the gap. Increases always apply.
//!
//! Sending: callers gate every message with [`FlowWindow::can_send`], not
//! with `room()` or `window()`, which are informational. `can_send` admits a
//! message when `unacked + len` fits the current limit (the probe cap while
//! probing, the window otherwise), and always admits one message of any size
//! into an empty pipe, so a message larger than the limit (e.g. a 512 KiB
//! whole frame) is never blocked forever: it waits for the pipe to drain.
//!
//! RTT probing: a standing queue raises every sample, so expiring the minimum
//! by age alone would let it drift up with the window. Every `PROBE_INTERVAL`
//! (10 s) the sender is held to `FLOOR` unacked bytes (`FLOOR / 2` when the
//! window is already at `FLOOR`) until the first message sent after the probe
//! began is fully acked; that sample sees a short queue and replaces the
//! minimum. If that message is larger than the cap it goes out once the pipe
//! is empty and its own ack ends the probe (its sample then includes the
//! message's transmission time). The adaptive window is untouched while
//! probing, and decreases wait for a base sample taken one round trip after
//! the probe.
//!
//! Limitation: on a very slow link a probe takes as long as the capped queue
//! needs to drain (e.g. about 52 s at 10 kbit/s for `FLOOR / 2`), during which
//! the minimum is not refreshed.

use std::collections::VecDeque;
use std::time::Duration;

pub const INITIAL: u64 = 262_144;
pub const FLOOR: u64 = 131_072;
pub const CEILING: u64 = 16_777_216;
/// Connection time between RTT probes.
const PROBE_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct FlowWindow {
    window: u64,
    sent: u64,
    acked: u64,
    /// `(send time, cumulative bytes sent)`, oldest first, only unacked.
    sends: VecDeque<(Duration, u64)>,
    /// `(ack time, total acked, limited_sends)`, oldest first.
    acks: VecDeque<(Duration, u64, u64)>,
    /// Smallest RTT sample since the last probe (or the connection start).
    min_rtt: Option<Duration>,
    /// When the pipe last went from empty to non-empty.
    busy_since: Duration,
    /// Unacked cap while probing: `FLOOR`, or `FLOOR / 2` when the window is
    /// already at `FLOOR`.
    probe_cap: u64,
    /// Probing: the unacked cap is `probe_cap` until `probe_target` is acked.
    probing: bool,
    /// Cumulative bytes sent once the probe's first message left (it may be
    /// larger than `probe_cap` if sent into an empty pipe); acked ends it.
    probe_target: Option<u64>,
    next_probe: Duration,
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
            busy_since: Duration::ZERO,
            probing: false,
            probe_cap: FLOOR,
            probe_target: None,
            next_probe: PROBE_INTERVAL,
            limited_sends: 0,
        }
    }

    pub fn on_sent(&mut self, bytes: u64, now: Duration) {
        if bytes == 0 {
            return;
        }
        if self.unacked() == 0 {
            self.busy_since = now;
        }
        self.sent += bytes;
        self.sends.push_back((now, self.sent));
        if self.probing && self.probe_target.is_none() {
            self.probe_target = Some(self.sent);
        }
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
        let sample = self
            .sends
            .front()
            .map(|&(sent_at, _)| now.saturating_sub(sent_at));
        if let Some(sample) = sample {
            self.min_rtt = Some(self.min_rtt.map_or(sample, |m| m.min(sample)));
        }
        let mut probe_ended = false;
        if self.probing {
            if self.probe_target.is_some_and(|t| total >= t) {
                self.probing = false;
                self.probe_target = None;
                self.next_probe = now + PROBE_INTERVAL;
                probe_ended = true;
                // The probe sample is queue-free: it replaces older minima.
                if sample.is_some() {
                    self.min_rtt = sample;
                }
            }
        } else if now >= self.next_probe {
            self.probing = true;
            self.probe_cap = if self.window > FLOOR {
                FLOOR
            } else {
                FLOOR / 2
            };
        }
        let rtt = self.min_rtt.unwrap_or_default();
        // Newest earlier sample that is at least one RTT old.
        let base = self
            .acks
            .iter()
            .rev()
            .find(|&&(t, _, _)| t + rtt <= now)
            .copied();
        if probe_ended {
            // Acks of probe-capped sends must not feed the rate: decreases
            // wait for a base sample one round trip after the probe.
            self.busy_since = now + rtt;
        }
        if let (Some((t, acked, limited)), false) = (base, self.probing) {
            let candidate = (2 * (total - acked)).clamp(FLOOR, CEILING);
            let steady = t > self.busy_since && self.limited_sends > limited;
            if candidate >= self.window || steady {
                self.window = candidate;
            }
            // Samples older than the base are no longer needed.
            while self.acks.front().is_some_and(|&(ft, _, _)| ft < t) {
                self.acks.pop_front();
            }
        }
        self.acks.push_back((now, total, self.limited_sends));
    }

    /// Whether a message of `len` payload bytes may be sent now. This is the
    /// send gate: an empty pipe admits one message of any size; otherwise
    /// `unacked + len` must fit the current limit (the probe cap while
    /// probing, the window otherwise).
    pub fn can_send(&self, len: u64) -> bool {
        let unacked = self.unacked();
        unacked == 0 || unacked.saturating_add(len) <= self.limit()
    }

    /// The adaptive window. Informational: gate sends with `can_send`.
    pub fn window(&self) -> u64 {
        self.window
    }

    pub fn unacked(&self) -> u64 {
        self.sent - self.acked
    }

    /// Current limit minus unacked, saturating. Informational: gate sends
    /// with `can_send` (it also admits a message into an empty pipe).
    pub fn room(&self) -> u64 {
        self.limit().saturating_sub(self.unacked())
    }

    /// Unacked byte limit: the probe cap while probing, else the window.
    fn limit(&self) -> u64 {
        if self.probing {
            self.probe_cap
        } else {
            self.window
        }
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
    /// explicit 1 ms clock. The sender sends `part`-byte messages (64 KiB by
    /// default) whenever `can_send` admits one.
    struct Sim {
        f: FlowWindow,
        bytes_per_s: u64,
        rtt_ms: u64,
        t: u64,
        inflight: VecDeque<(u64, u64)>, // (ack due ms, cumulative)
        link_free_ms: f64,
        sent: u64,
        part: u64,
        /// Sender is paused until the pipe drains, then resumes in that very tick.
        gap: bool,
        /// Last tick in which a part was sent (or sending was off).
        last_send_ms: u64,
        /// Longest run of ticks with sending wanted but nothing sent.
        max_stall_ms: u64,
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
                gap: false,
                last_send_ms: 0,
                max_stall_ms: 0,
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
                if self.gap && self.inflight.is_empty() {
                    self.gap = false;
                }
                let mut did_send = false;
                while sending && !self.gap && self.f.can_send(self.part) {
                    self.f.on_sent(self.part, ms(self.t));
                    self.sent += self.part;
                    did_send = true;
                    let start = self.link_free_ms.max(self.t as f64);
                    self.link_free_ms = start + self.part as f64 * 1000.0 / self.bytes_per_s as f64;
                    self.inflight
                        .push_back((self.link_free_ms.ceil() as u64 + self.rtt_ms, self.sent));
                }
                if did_send || !sending {
                    self.last_send_ms = self.t;
                } else {
                    self.max_stall_ms = self.max_stall_ms.max(self.t - self.last_send_ms);
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
        let mut min = u64::MAX;
        for _ in 0..3_000 {
            sim.run(1, true);
            min = min.min(sim.f.window());
        }
        assert!(
            min >= before,
            "window fell to {min} (before the gap: {before})"
        );
    }

    #[test]
    fn window_shrinks_after_link_slows() {
        let mut sim = Sim::new(12_500_000, 80);
        sim.run(10_000, true);
        assert!(sim.f.window() >= 1_000_000);
        sim.bytes_per_s = 125_000;
        sim.run(120_000, true);
        assert!(sim.f.window() <= 512 * 1024, "{}", sim.f.window());
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
    fn same_tick_restart_does_not_shrink_window() {
        let mut sim = Sim::new(12_500_000, 80);
        sim.run(5_000, true);
        let before = sim.f.window();
        sim.gap = true;
        let mut min = u64::MAX;
        for _ in 0..3_000 {
            sim.run(1, true);
            min = min.min(sim.f.window());
        }
        assert!(!sim.gap, "pipe never drained");
        assert!(min >= before, "window fell to {min} (before: {before})");
    }

    #[test]
    fn window_at_floor_regrows_after_rtt_rises() {
        // Small parts and a 1 ms RTT pin the window at FLOOR; then the RTT
        // rises 100x on the same fast link.
        let mut sim = Sim::new(12_500_000, 1);
        sim.part = 16_384;
        sim.run(5_000, true);
        assert_eq!(sim.f.window(), FLOOR);
        sim.rtt_ms = 100;
        sim.run(30_000, true);
        let at = sim.sent;
        sim.run(3_000, true);
        let goodput = (sim.sent - at) as f64 / 3.0;
        assert!(goodput >= 0.8 * 12_500_000.0, "goodput {goodput}");
    }

    #[test]
    fn window_regrows_after_rtt_rises() {
        let mut sim = Sim::new(12_500_000, 20);
        sim.run(5_000, true);
        sim.rtt_ms = 100;
        sim.run(15_000, true);
        let at = sim.sent;
        sim.run(3_000, true);
        let goodput = (sim.sent - at) as f64 / 3.0;
        assert!(goodput >= 0.8 * 12_500_000.0, "goodput {goodput}");
    }

    #[test]
    fn can_send_admits_any_size_into_an_empty_pipe() {
        let mut f = FlowWindow::new();
        assert!(f.can_send(CEILING * 2));
        f.on_sent(1, ms(0));
        assert!(f.can_send(INITIAL - 1));
        assert!(!f.can_send(INITIAL));
    }

    #[test]
    fn message_larger_than_floor_probe_cap_is_sent_and_ends_the_probe() {
        // Small parts and a 1 ms RTT pin the window at FLOOR.
        let mut sim = Sim::new(12_500_000, 1);
        sim.part = 16_384;
        while !sim.f.probing {
            sim.run(1, true);
            assert!(sim.t < 15_000, "no probe started");
        }
        assert_eq!(sim.f.window(), FLOOR);
        assert_eq!(sim.f.probe_cap, FLOOR / 2);
        assert_eq!(sim.f.probe_target, None, "probe already has a first send");
        // From now on every message is 100,000 bytes, above the probe cap.
        sim.part = 100_000;
        let at = sim.sent;
        let start = sim.t;
        while sim.f.probing {
            sim.run(1, true);
            assert!(sim.t - start < 1_000, "probe never ended");
        }
        assert!(sim.sent >= at + 100_000, "the large message was not sent");
        sim.run(5_000, true);
        assert!(sim.max_stall_ms <= 50, "stalled {} ms", sim.max_stall_ms);
    }

    #[test]
    fn whole_frames_keep_flowing_across_probes() {
        // 512 KiB whole-frame messages exceed the probe cap above FLOOR.
        let mut sim = Sim::new(12_500_000, 80);
        sim.part = 524_288;
        let mut probes = 0;
        for _ in 0..45_000 {
            let was_probing = sim.f.probing;
            sim.run(1, true);
            if was_probing && !sim.f.probing {
                probes += 1;
            }
        }
        assert!(probes >= 3, "only {probes} probes completed");
        assert!(sim.max_stall_ms <= 1_000, "stalled {} ms", sim.max_stall_ms);
        assert!(sim.sent >= 45 * 12_500_000 / 2, "sent only {}", sim.sent);
    }

    /// Probing at FLOOR caps unacked bytes at 64 KiB: a 64 KiB part cannot
    /// follow 40 KiB unacked, so a client acking only every 64 KiB would
    /// stall here. An ack after every message drains the pipe and the part
    /// goes out.
    #[test]
    fn per_message_ack_opens_the_floor_probe() {
        let mut sim = Sim::new(12_500_000, 1);
        sim.part = 16_384;
        while !sim.f.probing {
            sim.run(1, true);
            assert!(sim.t < 15_000, "no probe started");
        }
        assert_eq!(sim.f.window(), FLOOR);
        assert_eq!(sim.f.probe_cap, FLOOR / 2);
        // Settle the messages in flight, keeping the probe on.
        let f = &mut sim.f;
        let at = ms(sim.t);
        f.acked = f.sent;
        f.sends.clear();
        f.probe_target = None;
        f.on_sent(40_960, at);
        assert!(f.probing);
        assert!(!f.can_send(65_536), "64 KiB does not fit next to 40 KiB");
        f.on_ack(f.sent, at + ms(2));
        assert!(f.can_send(65_536));
    }

    #[test]
    fn window_stays_initial_until_two_samples() {
        let mut f = FlowWindow::new();
        f.on_sent(1000, ms(0));
        f.on_ack(500, ms(50));
        assert_eq!(f.window(), INITIAL);
    }
}
