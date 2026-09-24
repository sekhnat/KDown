//! Engine-wide metrics export (§19.5, task 7.5).
//!
//! Counters/gauges are atomic on the transfer path; retry/category maps are
//! updated at lifecycle boundaries, never while scheduler locks are held.
//! [`EngineMetrics::snapshot`] returns a stable, serializable view suitable
//! for Prometheus adapters, JSON export, or host-application telemetry.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;

use crate::error::{DownloadError, ErrorCategory};
use crate::job::controller::{DownloadResult, ResultStatus};

/// Engine-wide metric registry; clone the `Arc` and share it across jobs.
#[derive(Debug, Default)]
pub struct EngineMetrics {
    active_jobs: AtomicU64,
    jobs_started: AtomicU64,
    jobs_completed: AtomicU64,
    jobs_failed: AtomicU64,
    jobs_cancelled: AtomicU64,
    network_bytes: AtomicU64,
    reused_bytes: AtomicU64,
    retries: AtomicU64,
    retry_categories: Mutex<BTreeMap<String, u64>>,
    status_counts: Mutex<BTreeMap<String, u64>>,
    range_violations: AtomicU64,
    integrity_failures: AtomicU64,
    latency_count: AtomicU64,
    latency_sum_micros: AtomicU64,
    latency_max_micros: AtomicU64,
}

impl EngineMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub fn job_started(&self) {
        self.jobs_started.fetch_add(1, Ordering::Relaxed);
        self.active_jobs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn retry(&self, category: ErrorCategory) {
        self.retries.fetch_add(1, Ordering::Relaxed);
        let mut map = self.retry_categories.lock().expect("retry metrics");
        *map.entry(format!("{category:?}")).or_default() += 1;
    }

    /// Record one terminal [`DownloadResult`]. This is called after the
    /// job task settles, outside scheduler/sink locks.
    pub fn record_result(&self, result: &DownloadResult) {
        self.active_jobs.fetch_sub(1, Ordering::Relaxed);
        match result.status {
            ResultStatus::Completed => {
                self.jobs_completed.fetch_add(1, Ordering::Relaxed);
            }
            ResultStatus::Failed => {
                self.jobs_failed.fetch_add(1, Ordering::Relaxed);
            }
            ResultStatus::Cancelled => {
                self.jobs_cancelled.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.network_bytes
            .fetch_add(result.bytes_downloaded_from_network, Ordering::Relaxed);
        self.reused_bytes
            .fetch_add(result.bytes_reused_from_checkpoint, Ordering::Relaxed);
        self.record_latency(result.elapsed);
        if let Some(error) = &result.error {
            self.record_error(error);
        }
    }

    /// Record a task error before it produced a DownloadResult.
    pub fn record_task_error(&self, error: &DownloadError) {
        self.active_jobs.fetch_sub(1, Ordering::Relaxed);
        self.jobs_failed.fetch_add(1, Ordering::Relaxed);
        self.record_error(error);
    }

    pub fn record_error(&self, error: &DownloadError) {
        if let Some(status) = error.http_status() {
            let mut map = self.status_counts.lock().expect("status metrics");
            *map.entry(status.to_string()).or_default() += 1;
        }
        match error.category() {
            ErrorCategory::InvalidRangeResponse | ErrorCategory::RangeUnsupported => {
                self.range_violations.fetch_add(1, Ordering::Relaxed);
            }
            ErrorCategory::IntegrityMismatch => {
                self.integrity_failures.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        let mut map = self.retry_categories.lock().expect("category metrics");
        *map.entry(format!("errors/{:?}", error.category()))
            .or_default() += 1;
    }

    pub fn record_latency(&self, elapsed: Duration) {
        let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        self.latency_count.fetch_add(1, Ordering::Relaxed);
        self.latency_sum_micros.fetch_add(micros, Ordering::Relaxed);
        self.latency_max_micros.fetch_max(micros, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        let count = self.latency_count.load(Ordering::Relaxed);
        let sum = self.latency_sum_micros.load(Ordering::Relaxed);
        MetricsSnapshot {
            active_jobs: self.active_jobs.load(Ordering::Relaxed),
            jobs_started: self.jobs_started.load(Ordering::Relaxed),
            jobs_completed: self.jobs_completed.load(Ordering::Relaxed),
            jobs_failed: self.jobs_failed.load(Ordering::Relaxed),
            jobs_cancelled: self.jobs_cancelled.load(Ordering::Relaxed),
            network_bytes: self.network_bytes.load(Ordering::Relaxed),
            reused_bytes: self.reused_bytes.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            retry_categories: self.retry_categories.lock().expect("retry metrics").clone(),
            status_counts: self.status_counts.lock().expect("status metrics").clone(),
            range_violations: self.range_violations.load(Ordering::Relaxed),
            integrity_failures: self.integrity_failures.load(Ordering::Relaxed),
            latency_count: count,
            latency_sum_micros: sum,
            latency_max_micros: self.latency_max_micros.load(Ordering::Relaxed),
            latency_avg_micros: sum.checked_div(count).unwrap_or(0),
        }
    }
}

/// Exportable metrics view (§19.5).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub active_jobs: u64,
    pub jobs_started: u64,
    pub jobs_completed: u64,
    pub jobs_failed: u64,
    pub jobs_cancelled: u64,
    pub network_bytes: u64,
    pub reused_bytes: u64,
    pub retries: u64,
    pub retry_categories: BTreeMap<String, u64>,
    pub status_counts: BTreeMap<String, u64>,
    pub range_violations: u64,
    pub integrity_failures: u64,
    pub latency_count: u64,
    pub latency_sum_micros: u64,
    pub latency_max_micros: u64,
    pub latency_avg_micros: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_exports_outcomes_categories_status_and_latency() {
        let m = EngineMetrics::new();
        m.job_started();
        m.retry(ErrorCategory::Connection);
        m.record_result(&DownloadResult {
            status: ResultStatus::Failed,
            final_path: None,
            bytes_downloaded_from_network: 42,
            bytes_reused_from_checkpoint: 3,
            completed_bytes: 42,
            wasted_bytes: 0,
            retries: 0,
            total_size: Some(42),
            elapsed: Duration::from_millis(7),
            validators: Default::default(),
            warnings: vec![],
            error: Some(DownloadError::NotFound { status: 404 }),
        });
        let s = m.snapshot();
        assert_eq!(s.jobs_started, 1);
        assert_eq!(s.jobs_failed, 1);
        assert_eq!(s.active_jobs, 0);
        assert_eq!(s.network_bytes, 42);
        assert_eq!(s.reused_bytes, 3);
        assert_eq!(s.retries, 1);
        assert_eq!(s.status_counts.get("404"), Some(&1));
        assert!(s.retry_categories.contains_key("Connection"));
        assert!(s.retry_categories.contains_key("errors/NotFound"));
        assert_eq!(s.latency_count, 1);
        assert!(s.latency_avg_micros > 0);
    }
}
