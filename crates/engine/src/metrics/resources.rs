//! Process resource sampling for the adaptive controller (task 4.2).
//!
//! The controller's storage/resource veto needs to know whether the process
//! itself is under pressure. Linux reports both through /proc; platforms
//! without an equivalent return `None`, and the observability contract is to
//! label the measurement unavailable rather than fabricate it.
//!
//! CPU time comes from /proc/self/stat utime+stime in clock ticks. The
//! conversion assumes USER_HZ = 100, which holds on all mainstream Linux
//! systems; the value is only used as a relative pressure signal against a
//! configured ceiling, so a hypothetical tick-rate difference cannot flip a
//! correctness decision.

use std::time::Duration;

/// Process resident set size in bytes, when the platform can report it.
#[cfg(target_os = "linux")]
#[must_use]
pub(crate) fn rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kib: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kib.saturating_mul(1024));
        }
    }
    None
}

/// CPU time consumed by this process so far, in microseconds.
#[cfg(target_os = "linux")]
#[must_use]
pub(crate) fn cpu_time_us() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Fields 14 and 15 (1-based) are utime and stime, after the command
    // name, which may itself contain spaces inside parentheses.
    let close = stat.rfind(')')?;
    let rest = stat.get(close + 1..)?;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // After the state field (index 0 here) utime is index 11, stime 12.
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    let ticks = utime.saturating_add(stime);
    // USER_HZ = 100 -> 10_000 microseconds per tick.
    Some(ticks.saturating_mul(10_000))
}

/// Process resident set size in bytes (unavailable on this platform).
#[cfg(not(target_os = "linux"))]
#[must_use]
pub(crate) fn rss_bytes() -> Option<u64> {
    None
}

/// CPU time consumed by this process so far (unavailable on this platform).
#[cfg(not(target_os = "linux"))]
#[must_use]
pub(crate) fn cpu_time_us() -> Option<u64> {
    None
}

/// CPU consumed over `window` as a percentage of one core.
#[must_use]
pub(crate) fn cpu_percent_from_delta(delta_us: u64, window: Duration) -> f64 {
    delta_us as f64 / window.as_secs_f64().max(f64::EPSILON) / 10_000.0
}

/// Per-window process resource sampler (task 4.2): RSS is a level, CPU is a
/// delta against the previous window's sample.
#[derive(Debug, Default)]
pub(crate) struct ResourceSampler {
    last_cpu_us: Option<u64>,
}

impl ResourceSampler {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            last_cpu_us: cpu_time_us(),
        }
    }

    /// Sample one window: `(rss_bytes, cpu_percent)`, each `None` where the
    /// platform reports nothing.
    pub(crate) fn sample(&mut self, window: Duration) -> (Option<u64>, Option<f64>) {
        let rss = rss_bytes();
        let cpu = cpu_time_us();
        let percent = match (self.last_cpu_us, cpu) {
            (Some(before), Some(after)) => {
                Some(cpu_percent_from_delta(after.saturating_sub(before), window))
            }
            _ => None,
        };
        if cpu.is_some() {
            self.last_cpu_us = cpu;
        }
        (rss, percent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_percent_is_relative_to_the_window() {
        // 500 ms of CPU over a 1 s window is half a core.
        let half = cpu_percent_from_delta(500_000, Duration::from_secs(1));
        assert!((half - 50.0).abs() < 1e-6, "half core: {half}");
        // 4 s of CPU over a 1 s window is four cores.
        let four = cpu_percent_from_delta(4_000_000, Duration::from_secs(1));
        assert!((four - 400.0).abs() < 1e-6, "four cores: {four}");
        // A zero window must not divide by zero.
        assert!(cpu_percent_from_delta(1_000, Duration::ZERO).is_finite());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_reports_plausible_process_resources() {
        let rss = rss_bytes().expect("linux reports VmRSS");
        assert!(rss > 1024 * 1024, "plausible RSS: {rss}");
        let first = cpu_time_us().expect("linux reports CPU time");
        // Burn a little CPU, then the counter must not go backwards.
        let mut spin = 0u64;
        for index in 0..2_000_000u64 {
            spin = spin.wrapping_add(index);
        }
        assert!(spin > 0);
        let second = cpu_time_us().expect("linux reports CPU time");
        assert!(second >= first, "cpu time is monotonic: {first} {second}");
    }

    #[test]
    fn sampler_reports_a_window() {
        let mut sampler = ResourceSampler::new();
        let (rss, cpu) = sampler.sample(Duration::from_millis(500));
        #[cfg(target_os = "linux")]
        {
            assert!(rss.is_some(), "linux reports RSS");
            let percent = cpu.expect("linux reports CPU");
            assert!(percent >= 0.0 && percent.is_finite(), "cpu {percent}");
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(rss, None, "unavailable measurements are labeled");
            assert_eq!(cpu, None, "unavailable measurements are labeled");
        }
    }
}
