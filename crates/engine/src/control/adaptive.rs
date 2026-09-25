//! Conservative adaptive range-concurrency controller (§, tasks 9.1-9.3,
//! design D6).
//!
//! Opt-in only: fixed-mode jobs never change behavior. When enabled, the
//! active range-worker count starts at the configured minimum, probes +1,
//! and is retained only when the useful (unique completed-byte) goodput
//! improves materially. Retries, throttling and storage pressure cause
//! holds/reverts/reductions; every change requires a cooldown before the
//! next probe. Raw wire throughput is never the primary objective — only
//! useful unique-byte goodput is. Manual `set_concurrency` overrides the
//! controller for the remainder of the job (design D5: no competing
//! controllers).

use std::time::{Duration, Instant};

/// Bound on how many opening windows may be spent anchoring the goodput
/// reference before the first probe. Convergence normally ends anchoring
/// far earlier; the cap only bounds the delay when goodput keeps drifting
/// (for example a long slow-start ramp).
const OPENING_WINDOW_CAP: u32 = 8;
/// Opt-in adaptive concurrency configuration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdaptiveConfig {
    /// Observation window per decision.
    pub window: Duration,
    /// Smoothed-goodput relative gain required to KEEP a probe
    /// (hysteresis band, 5% default).
    pub material_gain_band: f64,
    /// Cooldown after any change before the next probe.
    pub cooldown: Duration,
    /// Maximum tolerated retries per window before pressure response.
    pub max_retries_per_window: u64,
    /// Maximum tolerated throttle responses (429/503) per window.
    pub max_throttled_per_window: u64,
    /// Storage-pressure veto: outstanding-write depth p95 at or
    /// above which growth is suppressed even when raw network goodput rises.
    /// Provisional until the phase-4 gate records measured thresholds.
    pub writer_queue_p95_ceiling: f64,
    /// Acknowledgement-latency p95 (ms) at or above which growth is
    /// suppressed — a sink that acknowledges slowly must not be fed harder.
    pub writer_ack_p95_ceiling_ms: f64,
    /// Byte-budget wait (ms per window) at or above which growth is
    /// suppressed (workers blocked on the write-byte budget).
    pub budget_wait_ceiling_ms: u64,
    /// Process RSS ceiling in bytes above which growth is suppressed.
    pub rss_ceiling_bytes: u64,
    /// Window CPU ceiling as a percentage of one core above which growth is
    /// suppressed.
    pub cpu_ceiling_percent: f64,
    /// Consecutive pressure windows required before REDUCING (hysteresis):
    /// one saturated window vetoes growth but does not shrink the level; an
    /// in-flight probe still reverts immediately.
    pub sustained_pressure_windows: u32,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_millis(500),
            material_gain_band: 0.05,
            cooldown: Duration::from_millis(1000),
            max_retries_per_window: 2,
            max_throttled_per_window: 0,
            writer_queue_p95_ceiling: 8.0,
            writer_ack_p95_ceiling_ms: 250.0,
            budget_wait_ceiling_ms: 250,
            rss_ceiling_bytes: 1024 * 1024 * 1024,
            cpu_ceiling_percent: 400.0,
            sustained_pressure_windows: 2,
        }
    }
}

/// What one window observed: DELTAS over the window — the caller
/// folds the counters at window boundaries and subtracts the previous fold.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct WindowSample {
    /// Unique completed bytes over the window (never includes reused
    /// checkpoint bytes — the goodput objective; resumed bytes never
    /// inflate it).
    pub completed_bytes: u64,
    /// Network (wire) bytes received over the window.
    pub network_bytes: u64,
    /// Wasted/retransmitted bytes over the window.
    pub wasted_bytes: u64,
    /// Retry count over the window.
    pub retries: u64,
    /// Throttling responses (429/503) over the window.
    pub throttled: u64,
    /// Interval-weighted worker time spent holding a lease:
    /// worker-milliseconds accumulated by sub-interval sampling, never an
    /// instantaneous count.
    pub active_worker_ms: u64,
    /// Interval-weighted worker time spent provisioned and parked without a
    /// lease. Dormant workers above the desired count are not
    /// counted — they hold no capacity.
    pub idle_worker_ms: u64,
    /// Interval-weighted idle share:
    /// `idle_worker_ms / (active_worker_ms + idle_worker_ms)`; `0.0` when no
    /// worker time was observed. Replaces the previous instantaneous cell
    /// sample.
    pub worker_idle_ratio: f64,
    /// Writer acknowledgement-latency percentiles over the window (ms);
    /// `None` when no acknowledgement completed in the window.
    pub writer_ack_p50_ms: Option<f64>,
    /// Writer acknowledgement-latency p95 over the window (ms).
    pub writer_ack_p95_ms: Option<f64>,
    /// Outstanding-write queue-depth percentiles over the window (sampled at
    /// submit time); `None` when no write was submitted.
    pub writer_queue_p50: Option<f64>,
    /// Outstanding-write queue-depth p95 over the window.
    pub writer_queue_p95: Option<f64>,
    /// Worker time blocked waiting for write-byte budget over the window
    /// (ms); `0` on the legacy writer-lane path (no byte budget).
    pub budget_wait_ms: u64,
    /// Process resident set size at the window boundary (bytes); `None`
    /// when the platform cannot report it (labeled unavailable, never
    /// guessed).
    pub rss_bytes: Option<u64>,
    /// CPU consumed during the window as a percentage of one core; `None`
    /// when unavailable.
    pub cpu_percent: Option<f64>,
    /// Window length (real elapsed).
    pub elapsed: Duration,
}

impl WindowSample {
    /// Useful goodput over the window (bytes/second).
    #[must_use]
    pub fn useful_goodput(&self) -> f64 {
        self.completed_bytes as f64 / self.elapsed.as_secs_f64().max(f64::EPSILON)
    }

    /// Pressure signals: retries or throttling above the configured
    /// tolerances.
    #[must_use]
    pub fn under_pressure(&self, config: &AdaptiveConfig) -> bool {
        self.retries > config.max_retries_per_window
            || self.throttled > config.max_throttled_per_window
            || self.wasted_bytes > 0
                && self.wasted_bytes >= self.completed_bytes
                && self.completed_bytes > 0
    }

    /// Storage-pressure signals: writer backlog or acknowledgement
    /// latency beyond the configured ceilings, or workers blocked on the
    /// write-byte budget. These veto growth regardless of raw network
    /// throughput — a saturated sink must not be fed harder, or the
    /// bottleneck simply hides in queued payload.
    #[must_use]
    pub fn storage_pressure(&self, config: &AdaptiveConfig) -> bool {
        self.writer_queue_p95
            .is_some_and(|p95| p95 >= config.writer_queue_p95_ceiling)
            || self
                .writer_ack_p95_ms
                .is_some_and(|p95| p95 >= config.writer_ack_p95_ceiling_ms)
            || self.budget_wait_ms >= config.budget_wait_ceiling_ms
    }

    /// Process-resource pressure: RSS or CPU above the configured
    /// ceilings. Unavailable signals never trigger — they are reported as
    /// unavailable rather than guessed.
    #[must_use]
    pub fn resource_pressure(&self, config: &AdaptiveConfig) -> bool {
        self.rss_bytes
            .is_some_and(|rss| rss >= config.rss_ceiling_bytes)
            || self
                .cpu_percent
                .is_some_and(|cpu| cpu >= config.cpu_ceiling_percent)
    }
}

/// Interval-weighted worker activity: converts point samples of
/// the actual active/idle worker counts into worker-time, so one slow
/// observation cannot masquerade as a whole window of idleness.
#[derive(Debug, Default)]
pub struct WorkerActivity {
    active_ms: u64,
    idle_ms: u64,
    last: Option<Instant>,
    last_active: u64,
    last_idle: u64,
}

impl WorkerActivity {
    /// Observe the current actual counts. The time since the previous
    /// observation is credited to the counts that were in effect at the
    /// START of that interval; the first observation only establishes state.
    pub fn observe(&mut self, active: u64, idle: u64, now: Instant) {
        if let Some(last) = self.last {
            let elapsed_ms =
                u64::try_from(now.saturating_duration_since(last).as_millis()).unwrap_or(u64::MAX);
            self.active_ms = self
                .active_ms
                .saturating_add(elapsed_ms.saturating_mul(self.last_active));
            self.idle_ms = self
                .idle_ms
                .saturating_add(elapsed_ms.saturating_mul(self.last_idle));
        }
        self.last = Some(now);
        self.last_active = active;
        self.last_idle = idle;
    }

    /// Fold the accumulated worker-time and reset the accumulators. The last
    /// observed state is retained so window boundaries lose no time.
    pub fn take(&mut self) -> (u64, u64) {
        let active = std::mem::take(&mut self.active_ms);
        let idle = std::mem::take(&mut self.idle_ms);
        (active, idle)
    }

    /// Interval-weighted idle share for accumulated worker-time.
    #[must_use]
    pub fn idle_ratio(active_ms: u64, idle_ms: u64) -> f64 {
        let total = active_ms.saturating_add(idle_ms);
        if total == 0 {
            0.0
        } else {
            idle_ms as f64 / total as f64
        }
    }
}

/// Why the controller reached its last decision: stable
/// report reason codes, so a phase report can explain holds and reductions
/// without changing the decision enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecisionReason {
    /// No window has been evaluated yet.
    None,
    /// A manual override pins concurrency for the job's remainder.
    ManualOverride,
    /// The window observed no traffic; the baseline is untouched.
    EmptyWindow,
    /// A cooldown from an earlier change or veto is still active.
    Cooldown,
    /// The first observation established the comparison baseline.
    Baseline,
    /// A probe's marginal goodput gain was material and its cost bounded.
    GainKept,
    /// A probe's marginal gain was not material; it was reverted.
    NoGainReverted,
    /// A stable level with no pressure: probe one more worker.
    Probe,
    /// Retry/throttle pressure (immediate response).
    RetryPressure,
    /// Writer backlog, acknowledgement latency or byte-budget wait.
    StoragePressure,
    /// Process RSS or CPU above the configured ceiling.
    ResourcePressure,
    /// The configured maximum is already reached.
    AtMaximum,
}

/// The controller's decision for one window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Retain the current concurrency (no change).
    Hold,
    /// Probe one additional active worker (additive, never above max).
    ProbeUp,
    /// Reduce by one active worker (never below min).
    Reduce,
}

/// The adaptive controller state (one per adaptive job).
#[derive(Debug)]
pub struct AdaptiveController {
    config: AdaptiveConfig,
    min: u64,
    max: u64,
    /// Best smoothed useful goodput observed while the desired level was
    /// stable (no probe in flight). This high-water reference is what a new
    /// level must beat to be kept: a startup-dipped or mid-transfer first
    /// window must not make a slower level look like a gain, and probe
    /// windows must not raise the bar for their own evaluation.
    best_goodput: Option<f64>,
    /// Whether the goodput reference has been anchored. The opening windows
    /// include the probe request and connection ramp-up; a probe decided
    /// against an unconverged reference can keep a slower level for the
    /// whole transfer, so anchoring waits for convergence or the cap.
    reference_anchored: bool,
    /// Opening windows evaluated while the reference was not yet anchored.
    baseline_windows: u32,
    smoothed_goodput: Option<f64>,
    /// The concurrency level before the current probe (the revert target).
    pre_probe_level: Option<u64>,
    /// Instant until which probes are held (cooldown).
    cooldown_until: Option<Instant>,
    /// Manual override active: the controller suspends for the job's
    /// remainder.
    manual_override: bool,
    /// Consecutive windows under storage/resource pressure (hysteresis:
    /// sustained pressure reduces, one window only vetoes).
    pressure_windows: u32,
    /// Reason code for the last decision.
    reason: DecisionReason,
}

impl AdaptiveController {
    /// Create a controller for the configured bounds (the job
    /// starts at `min`).
    #[must_use]
    pub fn new(config: AdaptiveConfig, min: u64, max: u64) -> Self {
        Self {
            config,
            min: min.max(1),
            max: max.max(min.max(1)),
            best_goodput: None,
            reference_anchored: false,
            baseline_windows: 0,
            smoothed_goodput: None,
            pre_probe_level: None,
            cooldown_until: None,
            manual_override: false,
            pressure_windows: 0,
            reason: DecisionReason::None,
        }
    }

    /// Reason code for the last decision: lets reports and
    /// diagnostics explain why concurrency was held or reduced.
    #[must_use]
    pub fn last_reason(&self) -> DecisionReason {
        self.reason
    }

    /// A manual `set_concurrency` occurred: pin the desired count and
    /// suspend auto-adjustment for the job's remainder.
    pub fn manual_override(&mut self) {
        self.manual_override = true;
    }

    #[must_use]
    pub fn is_manual_override(&self) -> bool {
        self.manual_override
    }

    /// Whether the controller may act this window (not in cooldown).
    fn in_cooldown(&self, now: Instant) -> bool {
        self.cooldown_until.is_some_and(|until| now < until)
    }

    /// The next decision given one window's deltas. Deterministic
    /// pure function over the sample + internal state.
    ///
    /// `current` is the desired concurrency now; the returned decision never
    /// leaves `[min, max]`.
    #[must_use]
    pub fn decide(&mut self, sample: WindowSample, current: u64) -> Decision {
        // Manual override wins unconditionally.
        if self.manual_override {
            self.reason = DecisionReason::ManualOverride;
            return Decision::Hold;
        }
        // Empty window (no elapsed time or no bytes at all): hold without
        // disturbing the baseline (empty windows never decide).
        if sample.elapsed.is_zero() || sample.completed_bytes == 0 && sample.network_bytes == 0 {
            self.reason = DecisionReason::EmptyWindow;
            return Decision::Hold;
        }

        // Exponential smoothing of useful goodput (α = 0.5 — the tunable
        // stays internal until benchmarks justify exposure).
        // Exponential smoothing of useful goodput (α = 0.5 — the tunable
        // stays internal until benchmarks justify exposure).
        let instant_goodput = sample.useful_goodput();
        let smoothed = match self.smoothed_goodput {
            Some(prev) => 0.5 * prev + 0.5 * instant_goodput,
            None => instant_goodput,
        };
        // Anchor the reference before any probe. The opening windows include
        // the probe request and connection ramp-up; a probe decided against
        // an unconverged reference can keep a slower level for the whole
        // transfer, because a depressed reference makes even a worse level
        // look like material gain. Observe until smoothing converges — the
        // window-over-window change falls inside the material band — or
        // until OPENING_WINDOW_CAP windows have passed, whichever comes
        // first. The check reads the previous window's smoothed value, so
        // convergence reflects the settled trend rather than this window's
        // own sample.
        if !self.reference_anchored {
            let anchored = match self.smoothed_goodput {
                Some(prev) => {
                    (smoothed - prev).abs()
                        < self.config.material_gain_band * prev.max(f64::EPSILON)
                }
                None => false,
            };
            if anchored || self.baseline_windows >= OPENING_WINDOW_CAP {
                self.reference_anchored = true;
            }
        }
        self.smoothed_goodput = Some(smoothed);

        let now = Instant::now();
        // Pressure: hold or reduce — never probe into trouble (tasks 9.3 and
        // 4.2). Retry/throttle pressure responds immediately; storage and
        // resource pressure veto growth on the first saturated window but
        // only shrink after the configured number of sustained windows, so
        // one noisy sample cannot oscillate the level.
        let retry_pressure = sample.under_pressure(&self.config);
        let storage_pressure = sample.storage_pressure(&self.config);
        let resource_pressure = sample.resource_pressure(&self.config);
        if retry_pressure || storage_pressure || resource_pressure {
            self.reason = if retry_pressure {
                DecisionReason::RetryPressure
            } else if storage_pressure {
                DecisionReason::StoragePressure
            } else {
                DecisionReason::ResourcePressure
            };
            self.pressure_windows = if retry_pressure {
                self.config.sustained_pressure_windows.max(1)
            } else {
                self.pressure_windows.saturating_add(1)
            };
            // A probe that coincides with pressure has no support: revert to
            // the pre-probe level regardless of the raw goodput it produced
            // (marginal gain is only kept when its cost is bounded).
            if let Some(pre) = self.pre_probe_level.take() {
                self.cooldown_until = Some(now + self.config.cooldown);
                return if pre < current {
                    Decision::Reduce
                } else {
                    Decision::Hold
                };
            }
            if self.pressure_windows >= self.config.sustained_pressure_windows.max(1) {
                self.cooldown_until = Some(now + self.config.cooldown);
                if current > self.min {
                    return Decision::Reduce;
                }
                return Decision::Hold;
            }
            // First saturated window: veto growth, hold the level and cool
            // down; recovery can probe again afterwards.
            self.cooldown_until = Some(now + self.config.cooldown);
            return Decision::Hold;
        }
        self.pressure_windows = 0;

        // Ratchet the stable-level reference: only unpressured windows with
        // no probe in flight may raise it. A depressed opening window must
        // not lower the bar a new level has to beat, and probe or pressure
        // windows must not inflate the bar with samples that do not
        // describe a steady level. The ratchet sits after the pressure
        // gates so pressured windows never fold into the reference.
        if self.pre_probe_level.is_none() {
            self.best_goodput = Some(match self.best_goodput {
                Some(best) => best.max(smoothed),
                None => smoothed,
            });
        }

        if self.in_cooldown(now) {
            self.reason = DecisionReason::Cooldown;
            return Decision::Hold;
        }

        if !self.reference_anchored {
            // Still anchoring the reference: observe without deciding. The
            // counter only advances on unpressured, uncooled windows, so a
            // rough start cannot spend the cap on windows that could not
            // describe steady state anyway.
            self.baseline_windows += 1;
            self.reason = DecisionReason::Baseline;
            return Decision::Hold;
        }

        match self.pre_probe_level {
            // A probe is in flight: compare against the best stable-level
            // observation. Measuring against the level the probe left (or
            // against a startup-dipped opening window) can make a slower
            // level look like material gain and lock the job above its best
            // level for the remainder of the transfer.
            Some(_pre) => {
                let reference = self.best_goodput.unwrap_or(0.0);
                let smoothed = self.smoothed_goodput.unwrap_or(reference);
                let gain = (smoothed - reference) / reference.max(f64::EPSILON);
                if gain >= self.config.material_gain_band {
                    // The probe helped: KEEP the increase and allow another
                    // probe after cooldown (additive +1, strict bounds). The
                    // kept level becomes the new high-water reference.
                    self.best_goodput = self.smoothed_goodput;
                    self.pre_probe_level = None;
                    self.cooldown_until = Some(now + self.config.cooldown);
                    if current < self.max {
                        self.pre_probe_level = Some(current);
                        self.reason = DecisionReason::GainKept;
                        Decision::ProbeUp
                    } else {
                        self.reason = DecisionReason::AtMaximum;
                        Decision::Hold
                    }
                } else {
                    // No material gain: revert the probe and cool down, so
                    // an unhelpful level cannot persist by immediately
                    // re-probing.
                    self.pre_probe_level = None;
                    self.cooldown_until = Some(now + self.config.cooldown);
                    self.reason = DecisionReason::NoGainReverted;
                    if current > self.min {
                        Decision::Reduce
                    } else {
                        Decision::Hold
                    }
                }
            }
            // Stable state: probe +1 (additive probing). Every change
            // requires a cooldown before the next evaluation.
            None => {
                if current < self.max {
                    self.pre_probe_level = Some(current);
                    self.cooldown_until = Some(now + self.config.cooldown);
                    self.reason = DecisionReason::Probe;
                    Decision::ProbeUp
                } else {
                    self.reason = DecisionReason::AtMaximum;
                    Decision::Hold
                }
            }
        }
    }

    /// Apply a decision to a desired count (strict bounds).
    #[must_use]
    pub fn apply(&self, decision: Decision, current: u64) -> u64 {
        match decision {
            Decision::Hold => current,
            Decision::ProbeUp => (current + 1).clamp(self.min, self.max),
            Decision::Reduce => (current.saturating_sub(1)).clamp(self.min, self.max),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(completed: u64, window_ms: u64) -> WindowSample {
        WindowSample {
            completed_bytes: completed,
            network_bytes: completed,
            wasted_bytes: 0,
            retries: 0,
            throttled: 0,
            active_worker_ms: 0,
            idle_worker_ms: 0,
            worker_idle_ratio: 0.0,
            writer_ack_p50_ms: None,
            writer_ack_p95_ms: None,
            writer_queue_p50: None,
            writer_queue_p95: None,
            budget_wait_ms: 0,
            rss_bytes: None,
            cpu_percent: None,
            elapsed: Duration::from_millis(window_ms),
        }
    }

    /// Deterministic trace: gain keeps probes; no gain reverts;
    /// pressure reduces; cooldown holds; manual override pins.
    #[test]
    fn controller_trace_gain_no_gain_pressure_cooldown() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);

        // Window 1: establish the baseline (hold).
        assert_eq!(controller.decide(sample(500_000, 500), 1), Decision::Hold);
        // Window 2: same goodput → the first probe (+1).
        assert_eq!(
            controller.decide(sample(500_000, 500), 1),
            Decision::ProbeUp
        );
        // The probe applied: current 2. Immediately after a change: cooldown
        // holds even with great samples.
        assert_eq!(controller.decide(sample(2_000_000, 500), 2), Decision::Hold);
        // (Cooldown is wall-clock: for the deterministic trace, force expiry.)
        controller.cooldown_until = None;
        // Material gain → keep the probe and probe again.
        assert_eq!(
            controller.decide(sample(2_000_000, 500), 2),
            Decision::ProbeUp
        );
    }

    #[test]
    fn controller_trace_no_gain_reverts() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);
        // Baseline at level 1.
        assert_eq!(controller.decide(sample(500_000, 500), 1), Decision::Hold);
        // Probe (+1).
        assert_eq!(
            controller.decide(sample(500_000, 500), 1),
            Decision::ProbeUp
        );
        controller.cooldown_until = None;
        // The probe produced NO gain (same goodput): revert to 1.
        assert_eq!(controller.decide(sample(500_000, 500), 2), Decision::Reduce);
        assert_eq!(controller.apply(Decision::Reduce, 2), 1);
    }

    /// A startup-dipped opening window must not make a slower level look
    /// like material gain. The reference is the best stable-level
    /// observation, so a probe that never beats the best level reverts;
    /// comparing against the opening window instead locked jobs at an
    /// unhelpful level for the whole transfer on slow or loaded runners
    /// (observed as macOS CI never returning to the minimum).
    #[test]
    fn depressed_opening_window_cannot_lock_a_worse_level() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);
        // Window 1 is startup-contaminated: far below steady state. The
        // controller observes until smoothing converges on the reference.
        assert_eq!(controller.decide(sample(200_000, 500), 1), Decision::Hold);
        // Steady level-1 windows; the reference climbs toward the true
        // level instead of staying at the depressed opening value.
        for _ in 0..4 {
            assert_eq!(controller.decide(sample(4_000_000, 500), 1), Decision::Hold);
        }
        // Converged: the first probe raises desired to 2.
        assert_eq!(
            controller.decide(sample(4_000_000, 500), 1),
            Decision::ProbeUp
        );
        // Probe windows (cooldown): the new level is clearly slower.
        assert_eq!(controller.decide(sample(2_000_000, 500), 2), Decision::Hold);
        controller.cooldown_until = None;
        // Evaluation: the probe never beat the best stable observation, so
        // it must revert to 1 rather than be kept as a "gain" against the
        // depressed opening window.
        assert_eq!(
            controller.decide(sample(2_000_000, 500), 2),
            Decision::Reduce
        );
        assert_eq!(controller.apply(Decision::Reduce, 2), 1);
    }

    #[test]
    fn pressure_reduces_and_holds_bounds() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);
        // Heavy retry pressure at the MINIMUM: hold (never below min).
        let pressured = WindowSample {
            retries: 10,
            ..sample(100_000, 500)
        };
        assert_eq!(controller.decide(pressured, 1), Decision::Hold);
        // Pressure at level 3 with a probe in flight: revert toward the
        // pre-probe level.
        let mut controller = AdaptiveController::new(config, 1, 8);
        assert_eq!(controller.decide(sample(500_000, 500), 3), Decision::Hold);
        assert_eq!(
            controller.decide(sample(500_000, 500), 3),
            Decision::ProbeUp
        );
        controller.cooldown_until = None;
        let pressured = WindowSample {
            retries: 10,
            ..sample(100_000, 500)
        };
        let decision = controller.decide(pressured, 4);
        assert_eq!(decision, Decision::Reduce);
        assert_eq!(controller.apply(decision, 4), 3);
    }

    #[test]
    fn throttling_is_pressure() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);
        let throttled = WindowSample {
            throttled: 1, // a single 429/503 is above tolerance
            ..sample(100_000, 500)
        };
        assert!(throttled.under_pressure(&config));
        assert_eq!(controller.decide(throttled, 2), Decision::Reduce);
    }

    #[test]
    fn manual_override_pins_concurrency() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);
        controller.manual_override();
        // Even great samples change nothing after a manual override.
        assert_eq!(controller.decide(sample(5_000_000, 500), 2), Decision::Hold);
        assert!(controller.is_manual_override());
    }

    /// Empty windows (no elapsed, no bytes) never decide.
    #[test]
    fn empty_windows_hold() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);
        assert_eq!(
            controller.decide(WindowSample::default(), 1),
            Decision::Hold
        );
        assert_eq!(
            controller.decide(
                WindowSample {
                    elapsed: Duration::from_millis(100),
                    ..Default::default()
                },
                1,
            ),
            Decision::Hold
        );
        // And the baseline is untouched by empty windows: the next real
        // window establishes it.
        assert_eq!(controller.decide(sample(500_000, 500), 1), Decision::Hold);
    }

    /// Resumed bytes never inflate the objective: reused bytes
    /// are not part of `completed_bytes` deltas.
    #[test]
    fn window_arithmetic_excludes_reused_bytes() {
        // `completed_bytes` counts only UNIQUE newly completed bytes; reused
        // checkpoint bytes live in a separate counter and never enter the
        // goodput objective (no resumed-byte inflation).
        let sample = WindowSample {
            completed_bytes: 1_000,
            network_bytes: 1_000,
            wasted_bytes: 0,
            retries: 0,
            throttled: 0,
            active_worker_ms: 0,
            idle_worker_ms: 0,
            worker_idle_ratio: 0.0,
            writer_ack_p50_ms: None,
            writer_ack_p95_ms: None,
            writer_queue_p50: None,
            writer_queue_p95: None,
            budget_wait_ms: 0,
            rss_bytes: None,
            cpu_percent: None,
            elapsed: Duration::from_secs(1),
        };
        assert_eq!(sample.useful_goodput(), 1000.0);
    }

    /// Interval weighting: the elapsed interval is credited to the
    /// counts in effect at its start, so one slow observation cannot
    /// masquerade as a whole window of idleness.
    #[test]
    fn worker_activity_weights_intervals_not_instants() {
        let start = Instant::now();
        let mut activity = WorkerActivity::default();
        // First observation only establishes state.
        activity.observe(2, 2, start);
        assert_eq!(activity.take(), (0, 0));
        // The interval [start, start+100ms) was spent with 2 active/2 idle.
        activity.observe(3, 1, start + Duration::from_millis(100));
        assert_eq!(activity.take(), (200, 200));
        // The next interval credits the state that was in effect.
        activity.observe(0, 4, start + Duration::from_millis(200));
        assert_eq!(activity.take(), (300, 100));
        // A take() empties the accumulators without losing the boundary.
        assert_eq!(activity.take(), (0, 0));
        assert_eq!(activity.take(), (0, 0));
    }

    /// A momentary idle sample inside a busy window must not read as a mostly
    /// idle window (the instantaneous-sample failure mode).
    #[test]
    fn momentary_idle_does_not_dominate_the_window() {
        let start = Instant::now();
        let mut activity = WorkerActivity::default();
        activity.observe(4, 0, start);
        for step in 1..=9u64 {
            activity.observe(4, 0, start + Duration::from_millis(step * 10));
        }
        // One 10ms blip with all workers idle, then busy again.
        activity.observe(0, 4, start + Duration::from_millis(100));
        activity.observe(4, 0, start + Duration::from_millis(110));
        let (active, idle) = activity.take();
        assert_eq!(active, 4 * 100, "active worker-ms {active}");
        assert_eq!(idle, 4 * 10, "idle worker-ms {idle}");
        let ratio = WorkerActivity::idle_ratio(active, idle);
        assert!(
            ratio < 0.1,
            "interval-weighted idle share {ratio} must stay small"
        );
    }

    /// The idle ratio is defined (zero) when no worker time was observed.
    #[test]
    fn idle_ratio_is_zero_without_worker_time() {
        assert_eq!(WorkerActivity::idle_ratio(0, 0), 0.0);
        assert_eq!(WorkerActivity::idle_ratio(100, 0), 0.0);
        assert_eq!(WorkerActivity::idle_ratio(0, 100), 1.0);
    }

    /// A synthetic window with writer/process instrumentation:
    /// tests drive the veto paths deterministically instead of racing a
    /// real sink.
    fn instrumented_sample(completed: u64, window_ms: u64) -> WindowSample {
        WindowSample {
            completed_bytes: completed,
            network_bytes: completed,
            elapsed: Duration::from_millis(window_ms),
            ..WindowSample::default()
        }
    }

    /// Pressure predicates at the configured ceilings: at-or-above
    /// is pressure, unavailable signals are not.
    #[test]
    fn pressure_predicates_are_threshold_exact() {
        let config = AdaptiveConfig::default();
        let mut sample = instrumented_sample(1000, 500);
        assert!(!sample.storage_pressure(&config));
        assert!(!sample.resource_pressure(&config));
        sample.writer_queue_p95 = Some(config.writer_queue_p95_ceiling);
        assert!(sample.storage_pressure(&config), "queue p95 at ceiling");
        sample.writer_queue_p95 = None;
        sample.writer_ack_p95_ms = Some(config.writer_ack_p95_ceiling_ms);
        assert!(sample.storage_pressure(&config), "ack p95 at ceiling");
        sample.writer_ack_p95_ms = None;
        sample.budget_wait_ms = config.budget_wait_ceiling_ms;
        assert!(sample.storage_pressure(&config), "budget wait at ceiling");
        sample.budget_wait_ms = 0;
        sample.rss_bytes = Some(config.rss_ceiling_bytes);
        assert!(sample.resource_pressure(&config), "rss at ceiling");
        sample.rss_bytes = None;
        sample.cpu_percent = Some(config.cpu_ceiling_percent);
        assert!(sample.resource_pressure(&config), "cpu at ceiling");
        sample.cpu_percent = None;
        assert!(
            !sample.storage_pressure(&config) && !sample.resource_pressure(&config),
            "unavailable signals never trigger pressure"
        );
    }

    /// Storage saturation reverts a probe that raised raw goodput:
    /// a saturated sink must not be fed harder, and recovery still probes.
    #[test]
    fn storage_saturation_reverts_a_probe_despite_raw_gain() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);
        assert_eq!(
            controller.decide(instrumented_sample(500_000, 500), 1),
            Decision::Hold
        );
        assert_eq!(
            controller.decide(instrumented_sample(500_000, 500), 1),
            Decision::ProbeUp
        );
        controller.cooldown_until = None;
        // The probe window reports 4x the raw goodput but a writer backlog
        // above the ceiling: the increase is reverted, not kept.
        let mut saturated = instrumented_sample(2_000_000, 500);
        saturated.writer_queue_p95 = Some(config.writer_queue_p95_ceiling + 4.0);
        assert_eq!(controller.decide(saturated, 2), Decision::Reduce);
        assert_eq!(controller.last_reason(), DecisionReason::StoragePressure);
        // After the cooldown, a recovered window probes again.
        controller.cooldown_until = None;
        assert_eq!(
            controller.decide(instrumented_sample(600_000, 500), 1),
            Decision::ProbeUp
        );
        assert_eq!(controller.last_reason(), DecisionReason::Probe);
    }

    /// One saturated window vetoes growth; sustained saturation reduces
    ///.
    #[test]
    fn sustained_storage_pressure_reduces_after_the_configured_windows() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);
        assert_eq!(
            controller.decide(instrumented_sample(500_000, 500), 4),
            Decision::Hold
        );
        let mut saturated = instrumented_sample(500_000, 500);
        saturated.budget_wait_ms = config.budget_wait_ceiling_ms + 50;
        // First saturated window: veto growth, keep the level.
        assert_eq!(controller.decide(saturated, 4), Decision::Hold);
        assert_eq!(controller.last_reason(), DecisionReason::StoragePressure);
        // Second consecutive saturated window: reduce.
        controller.cooldown_until = None;
        assert_eq!(controller.decide(saturated, 4), Decision::Reduce);
        assert_eq!(controller.last_reason(), DecisionReason::StoragePressure);
        // The floor still holds.
        controller.cooldown_until = None;
        assert_eq!(controller.decide(saturated, 1), Decision::Hold);
    }

    /// High RSS or CPU vetoes growth and never fabricates an unavailable
    /// measurement into pressure.
    #[test]
    fn process_resource_pressure_vetoes_growth() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);
        assert_eq!(
            controller.decide(instrumented_sample(500_000, 500), 1),
            Decision::Hold
        );
        let mut high_rss = instrumented_sample(900_000, 500);
        high_rss.rss_bytes = Some(config.rss_ceiling_bytes + 1024);
        assert_eq!(controller.decide(high_rss, 1), Decision::Hold);
        assert_eq!(controller.last_reason(), DecisionReason::ResourcePressure);
        controller.cooldown_until = None;
        let mut high_cpu = instrumented_sample(900_000, 500);
        high_cpu.cpu_percent = Some(config.cpu_ceiling_percent + 100.0);
        assert_eq!(controller.decide(high_cpu, 1), Decision::Hold);
        assert_eq!(controller.last_reason(), DecisionReason::ResourcePressure);
        // Recovery: smoothing must reconverge on clean windows before the
        // controller probes again (the pressure window's own smoothed
        // value decays out of the trend first).
        controller.cooldown_until = None;
        assert_eq!(
            controller.decide(instrumented_sample(950_000, 500), 1),
            Decision::Hold
        );
        assert_eq!(
            controller.decide(instrumented_sample(950_000, 500), 1),
            Decision::ProbeUp
        );
    }

    /// Acknowledged-latency saturation is storage pressure on its own
    ///: slow acknowledgements mean the sink is the bottleneck.
    #[test]
    fn acknowledgement_latency_is_storage_pressure() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);
        assert_eq!(
            controller.decide(instrumented_sample(500_000, 500), 1),
            Decision::Hold
        );
        let mut slow_ack = instrumented_sample(1_500_000, 500);
        slow_ack.writer_ack_p95_ms = Some(config.writer_ack_p95_ceiling_ms * 2.0);
        assert_eq!(controller.decide(slow_ack, 1), Decision::Hold);
        assert_eq!(controller.last_reason(), DecisionReason::StoragePressure);
        // An in-flight probe under slow acknowledgements reverts too.
        // Recovery: smoothing must reconverge before the controller probes
        // again; the post-pressure smoothed value decays toward the steady
        // level over several windows (halving each window).
        controller.cooldown_until = None;
        for _ in 0..4 {
            assert_eq!(
                controller.decide(instrumented_sample(500_000, 500), 1),
                Decision::Hold
            );
        }
        assert_eq!(
            controller.decide(instrumented_sample(500_000, 500), 1),
            Decision::ProbeUp
        );
        controller.cooldown_until = None;
        assert_eq!(controller.decide(slow_ack, 2), Decision::Reduce);
        assert_eq!(controller.last_reason(), DecisionReason::StoragePressure);
    }

    /// Retry and 429/503 pressure still respond immediately and report
    /// their own reason.
    #[test]
    fn retry_and_throttle_pressure_still_reduce_immediately() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);
        assert_eq!(
            controller.decide(instrumented_sample(500_000, 500), 4),
            Decision::Hold
        );
        let mut retrying = instrumented_sample(500_000, 500);
        retrying.retries = config.max_retries_per_window + 1;
        assert_eq!(controller.decide(retrying, 4), Decision::Reduce);
        assert_eq!(controller.last_reason(), DecisionReason::RetryPressure);
        controller.cooldown_until = None;
        let mut throttled = instrumented_sample(500_000, 500);
        throttled.throttled = config.max_throttled_per_window + 1;
        assert_eq!(controller.decide(throttled, 4), Decision::Reduce);
        assert_eq!(controller.last_reason(), DecisionReason::RetryPressure);
    }

    /// Noisy windows must not oscillate the level: near-band
    /// alternating windows move the level at most one worker per window and
    /// oscillate at most one step around the floor instead of running away.
    #[test]
    fn noisy_windows_do_not_oscillate_the_level() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);
        let mut level = 1u64;
        let decision = controller.decide(instrumented_sample(1_000_000, 500), level);
        level = controller.apply(decision, level);
        let mut levels = vec![level];
        for index in 0..24 {
            // Cooldown expiry between windows: the worst case for thrash.
            controller.cooldown_until = None;
            // +/- 6% around the baseline: a probe is either marginally kept or
            // reverted, never a sustained climb.
            let completed = if index % 2 == 0 { 1_060_000 } else { 940_000 };
            let decision = controller.decide(instrumented_sample(completed, 500), level);
            let next = controller.apply(decision, level);
            assert!(
                next.abs_diff(level) <= 1,
                "at most one worker per window: {level} -> {next}"
            );
            level = next;
            levels.push(level);
        }
        let max = *levels.iter().max().expect("levels");
        let min = *levels.iter().min().expect("levels");
        assert!(
            max - min <= 1,
            "near-band noise oscillates at most one step: {levels:?}"
        );
        assert!(max <= 2, "near-band noise cannot climb: {levels:?}");
        assert!(levels.iter().all(|l| (1..=8).contains(l)));
    }

    /// Even strong alternating windows move the level at most one worker per
    /// window: no jump-to-max reaction to a single spike.
    #[test]
    fn strong_swings_move_at_most_one_worker_per_window() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);
        let mut level = 1u64;
        let decision = controller.decide(instrumented_sample(500_000, 500), level);
        level = controller.apply(decision, level);
        for index in 0..16 {
            controller.cooldown_until = None;
            let completed = if index % 2 == 0 { 4_000_000 } else { 50_000 };
            let decision = controller.decide(instrumented_sample(completed, 500), level);
            let next = controller.apply(decision, level);
            assert!(
                next.abs_diff(level) <= 1,
                "spikes move at most one worker: {level} -> {next}"
            );
            level = next;
        }
        assert!((1..=8).contains(&level), "bounds hold: {level}");
    }

    /// Sub-band noise never climbs: at the floor the smoothed gain never
    /// reaches the hysteresis band, so the level stays within one probe
    ///.
    #[test]
    fn sub_band_noise_never_climbs_above_the_floor() {
        let config = AdaptiveConfig::default();
        let mut controller = AdaptiveController::new(config, 1, 8);
        let mut level = 1u64;
        let decision = controller.decide(instrumented_sample(1_000_000, 500), level);
        level = controller.apply(decision, level);
        for index in 0..20 {
            controller.cooldown_until = None;
            // +/- 3%: inside the 5% material-gain band.
            let completed = if index % 2 == 0 { 1_030_000 } else { 970_000 };
            let decision = controller.decide(instrumented_sample(completed, 500), level);
            level = controller.apply(decision, level);
            assert!(level <= 2, "sub-band noise cannot climb: {level}");
        }
        assert_eq!(level, 1, "the level settles back to the floor");
    }

    /// Strict bounds: apply never leaves [min, max].
    #[test]
    fn apply_respects_strict_bounds() {
        let config = AdaptiveConfig::default();
        let controller = AdaptiveController::new(config, 2, 4);
        assert_eq!(controller.apply(Decision::ProbeUp, 4), 4, "max bound");
        assert_eq!(controller.apply(Decision::Reduce, 2), 2, "min bound");
        assert_eq!(controller.apply(Decision::ProbeUp, 3), 4);
        assert_eq!(controller.apply(Decision::Reduce, 3), 2);
        assert_eq!(controller.apply(Decision::Hold, 3), 3);
    }
}
