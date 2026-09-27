//! Engine-wide metrics export (§19.5).
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

use crate::error::{CompletedDownload, DownloadError, DownloadRunError, ErrorCategory};

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
    /// Last transfer-memory sample (task 3.7): recorded at every job
    /// terminal and replaceable by a live sampler.
    transfer_memory: std::sync::Mutex<Option<crate::io::transfer_ledger::TransferMemorySnapshot>>,
    /// Max high-water of any single job's accounted pipeline memory.
    job_memory_high_water_max: AtomicU64,
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

    /// Record one verified, published completion. Called once after the
    /// job task settles, outside scheduler/sink locks.
    pub fn record_completed(&self, result: &CompletedDownload) {
        self.active_jobs.fetch_sub(1, Ordering::Relaxed);
        self.jobs_completed.fetch_add(1, Ordering::Relaxed);
        self.network_bytes.fetch_add(
            result.accounting.bytes_downloaded_from_network,
            Ordering::Relaxed,
        );
        self.reused_bytes.fetch_add(
            result.accounting.bytes_reused_from_checkpoint,
            Ordering::Relaxed,
        );
        self.record_latency(result.accounting.elapsed);
    }

    /// Record one non-success terminal outcome (transfer failure,
    /// infrastructure failure, or cancellation). Called once after the job
    /// task settles; every terminal branch reaches exactly one of
    /// [`EngineMetrics::record_completed`] /
    /// [`EngineMetrics::record_run_error`].
    pub fn record_run_error(&self, error: &DownloadRunError) {
        self.active_jobs.fetch_sub(1, Ordering::Relaxed);
        match error {
            DownloadRunError::Transfer(_) | DownloadRunError::Infrastructure(_) => {
                self.jobs_failed.fetch_add(1, Ordering::Relaxed);
                if let Some(e) = error.as_engine_error() {
                    self.record_error(e);
                }
            }
            DownloadRunError::Cancelled(_) => {
                self.jobs_cancelled.fetch_add(1, Ordering::Relaxed);
            }
        }
        // Partial accounting from failed/cancelled jobs still crossed the
        // wire: it belongs in the engine totals.
        let accounting = error.accounting();
        self.network_bytes
            .fetch_add(accounting.bytes_downloaded_from_network, Ordering::Relaxed);
        self.reused_bytes
            .fetch_add(accounting.bytes_reused_from_checkpoint, Ordering::Relaxed);
        self.record_latency(accounting.elapsed);
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

    /// Record a transfer-memory sample (task 3.7): the engine ledger's
    /// live view (current + high-water per component/aggregate, caps
    /// included). Callers sample at job terminal (failure cleanup visible)
    /// or from a periodic live sampler.
    pub fn record_transfer_memory(
        &self,
        snapshot: crate::io::transfer_ledger::TransferMemorySnapshot,
    ) {
        *self.transfer_memory.lock().expect("memory metrics") = Some(snapshot);
    }

    /// Record one job's accounted-pipeline high-water (task 3.7): the
    /// engine keeps the maximum across jobs.
    pub fn record_job_memory_high_water(&self, high_water: u64) {
        self.job_memory_high_water_max
            .fetch_max(high_water, Ordering::Relaxed);
    }

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
            transfer_memory: self.transfer_memory.lock().expect("memory metrics").clone(),
            job_memory_high_water_max: self.job_memory_high_water_max.load(Ordering::Relaxed),
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
    /// Last transfer-memory sample (task 3.7); `None` before the first
    /// sample. The snapshot's own `scope` field documents what is and is
    /// not accounted (no unknown-buffer-as-zero reporting).
    pub transfer_memory: Option<crate::io::transfer_ledger::TransferMemorySnapshot>,
    /// Max high-water of any single job's accounted pipeline memory.
    pub job_memory_high_water_max: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::error::{ArtifactDisposition, TransferAccounting, TransferFailure};
    #[test]
    fn snapshot_exports_outcomes_categories_status_and_latency() {
        let m = EngineMetrics::new();
        m.job_started();
        m.retry(ErrorCategory::Connection);
        m.record_run_error(&DownloadRunError::Transfer(Box::new(TransferFailure {
            error: DownloadError::NotFound { status: 404 },
            partial: TransferAccounting {
                bytes_downloaded_from_network: 42,
                bytes_reused_from_checkpoint: 3,
                completed_bytes: 42,
                total_size: Some(42),
                elapsed: Duration::from_millis(7),
                ..TransferAccounting::default()
            },
            artifacts: ArtifactDisposition::default(),
        })));
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
