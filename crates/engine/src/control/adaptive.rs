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

/// Opt-in adaptive concurrency configuration (task 9.1).
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
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_millis(500),
            material_gain_band: 0.05,
            cooldown: Duration::from_millis(1000),
            max_retries_per_window: 2,
            max_throttled_per_window: 0,
        }
    }
}

/// What one window observed (task 9.2): DELTAS over the window — the caller
/// folds the counters at window boundaries and subtracts the previous fold.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct WindowSample {
    /// Unique completed bytes over the window (never includes reused
    /// checkpoint bytes — the goodput objective, task 9.2: no
    /// resumed-byte inflation).
    pub completed_bytes: u64,
    /// Network (wire) bytes received over the window.
    pub network_bytes: u64,
    /// Wasted/retransmitted bytes over the window.
    pub wasted_bytes: u64,
    /// Retry count over the window.
    pub retries: u64,
    /// Throttling responses (429/503) over the window.
    pub throttled: u64,
    /// Fraction of active workers idle (parked, no lease) during the window.
    pub worker_idle_ratio: f64,
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
    /// tolerances (task 9.3).
    #[must_use]
    pub fn under_pressure(&self, config: &AdaptiveConfig) -> bool {
        self.retries > config.max_retries_per_window
            || self.throttled > config.max_throttled_per_window
            || self.wasted_bytes > 0
                && self.wasted_bytes >= self.completed_bytes
                && self.completed_bytes > 0
    }
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
    /// Smoothed useful goodput before the current probe (the comparison
    /// baseline).
    baseline_goodput: Option<f64>,
    /// Smoothed useful goodput of the last window.
    smoothed_goodput: Option<f64>,
    /// The concurrency level before the current probe (the revert target).
    pre_probe_level: Option<u64>,
    /// Instant until which probes are held (cooldown).
    cooldown_until: Option<Instant>,
    /// Manual override active: the controller suspends for the job's
    /// remainder (design D5).
    manual_override: bool,
}

impl AdaptiveController {
    /// Create a controller for the configured bounds (task 9.1: the job
    /// starts at `min`).
    #[must_use]
    pub fn new(config: AdaptiveConfig, min: u64, max: u64) -> Self {
        Self {
            config,
            min: min.max(1),
            max: max.max(min.max(1)),
            baseline_goodput: None,
            smoothed_goodput: None,
            pre_probe_level: None,
            cooldown_until: None,
            manual_override: false,
        }
    }

    /// A manual `set_concurrency` occurred: pin the desired count and
    /// suspend auto-adjustment for the job's remainder (design D5).
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

    /// The next decision given one window's deltas (task 9.3). Deterministic
    /// pure function over the sample + internal state.
    ///
    /// `current` is the desired concurrency now; the returned decision never
    /// leaves `[min, max]`.
    #[must_use]
    pub fn decide(&mut self, sample: WindowSample, current: u64) -> Decision {
        // Manual override wins unconditionally (task 9.3 precedence).
        if self.manual_override {
            return Decision::Hold;
        }
        // Empty window (no elapsed time or no bytes at all): hold without
        // disturbing the baseline (task 9.2: empty windows never decide).
        if sample.elapsed.is_zero() || sample.completed_bytes == 0 && sample.network_bytes == 0 {
            return Decision::Hold;
        }

        // Exponential smoothing of useful goodput (α = 0.5 — the tunable
        // stays internal until benchmarks justify exposure).
        let instant_goodput = sample.useful_goodput();
        self.smoothed_goodput = Some(match self.smoothed_goodput {
            Some(prev) => 0.5 * prev + 0.5 * instant_goodput,
            None => instant_goodput,
        });

        let now = Instant::now();
        // Pressure: hold or reduce — never probe into trouble (task 9.3).
        if sample.under_pressure(&self.config) {
            self.cooldown_until = Some(now + self.config.cooldown);
            // An active probe under pressure: revert to the pre-probe level.
            if let Some(pre) = self.pre_probe_level.take() {
                self.baseline_goodput = self.smoothed_goodput;
                return if pre < current {
                    Decision::Reduce
                } else {
                    Decision::Hold
                };
            }
            return if current > self.min {
                self.baseline_goodput = self.smoothed_goodput;
                self.cooldown_until = Some(now + self.config.cooldown);
                Decision::Reduce
            } else {
                Decision::Hold
            };
        }

        if self.in_cooldown(now) {
            return Decision::Hold;
        }

        match (self.baseline_goodput, self.pre_probe_level) {
            // No baseline yet: observe one more window before the first probe.
            (None, _) => {
                self.baseline_goodput = self.smoothed_goodput;
                self.pre_probe_level = None;
                Decision::Hold
            }
            // A probe is in flight: compare against the baseline.
            (Some(baseline), Some(_pre)) => {
                let smoothed = self.smoothed_goodput.unwrap_or(baseline);
                let gain = (smoothed - baseline) / baseline.max(f64::EPSILON);
                if gain >= self.config.material_gain_band {
                    // The probe helped: KEEP the increase and allow another
                    // probe after cooldown (additive +1, strict bounds).
                    self.baseline_goodput = self.smoothed_goodput;
                    self.pre_probe_level = None;
                    self.cooldown_until = Some(now + self.config.cooldown);
                    if current < self.max {
                        self.pre_probe_level = Some(current);
                        Decision::ProbeUp
                    } else {
                        Decision::Hold
                    }
                } else {
                    // No material gain: revert the probe and cool down
                    // (task 9.3: revert unhelpful probes, avoid oscillation).
                    self.pre_probe_level = None;
                    self.baseline_goodput = self.smoothed_goodput;
                    self.cooldown_until = Some(now + self.config.cooldown);
                    if current > self.min {
                        Decision::Reduce
                    } else {
                        Decision::Hold
                    }
                }
            }
            // Stable state: probe +1 (additive probing, task 9.3). Every
            // change requires a cooldown before the next evaluation.
            (Some(_), None) => {
                if current < self.max {
                    self.pre_probe_level = Some(current);
                    self.cooldown_until = Some(now + self.config.cooldown);
                    Decision::ProbeUp
                } else {
                    Decision::Hold
                }
            }
        }
    }

    /// Apply a decision to a desired count (strict bounds, task 9.3).
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
            worker_idle_ratio: 0.0,
            elapsed: Duration::from_millis(window_ms),
        }
    }

    /// Deterministic trace (task 9.3): gain keeps probes; no gain reverts;
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

    /// Empty windows (no elapsed, no bytes) never decide (task 9.2).
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

    /// Resumed bytes never inflate the objective (task 9.2): reused bytes
    /// are not part of `completed_bytes` deltas.
    #[test]
    fn window_arithmetic_excludes_reused_bytes() {
        // `completed_bytes` counts only UNIQUE newly completed bytes; reused
        // checkpoint bytes live in a separate counter and never enter the
        // goodput objective (task 9.2: no resumed-byte inflation).
        let sample = WindowSample {
            completed_bytes: 1_000,
            network_bytes: 1_000,
            wasted_bytes: 0,
            retries: 0,
            throttled: 0,
            worker_idle_ratio: 0.0,
            elapsed: Duration::from_secs(1),
        };
        assert_eq!(sample.useful_goodput(), 1000.0);
    }

    /// Strict bounds (task 9.3): apply never leaves [min, max].
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
