//! Gated test launcher: records every launch the supervisor requests and
//! lets tests resolve completions explicitly. The completion future stays
//! pending until `complete_job` is called, which is what holds a supervisor
//! slot open deterministically.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use kdown_app::domain::{CancelArtifactPolicy, JobId, SourceUrl};
use kdown_app::engine_adapter::{
    EngineControl, EngineLaunch, EngineLauncher, EngineOutcome, EngineRun, EngineSnapshotView,
    EngineStateView,
};
use kdown_app::error::AppError;

/// One recorded launch as observed by the test.
#[derive(Clone, Debug)]
#[allow(dead_code)] // destination is inspected by later-task scenarios
pub struct ObservedLaunch {
    pub job_id: JobId,
    pub source: SourceUrl,
    pub destination: PathBuf,
}

type CompletionSender = tokio::sync::oneshot::Sender<EngineOutcome>;

#[derive(Default)]
struct GateInner {
    launches: Mutex<Vec<EngineLaunch>>,
    cursor: Mutex<usize>,
    completions: Mutex<HashMap<JobId, Vec<CompletionSender>>>,
    cancel_policies: Mutex<Vec<CancelArtifactPolicy>>,
    notify: tokio::sync::Notify,
}

/// Test double implementing [`EngineLauncher`].
#[derive(Clone)]
pub struct GateLauncher {
    inner: Arc<GateInner>,
}

impl Default for GateLauncher {
    fn default() -> Self {
        Self {
            inner: Arc::new(GateInner {
                launches: Mutex::new(Vec::new()),
                cursor: Mutex::new(0),
                completions: Mutex::new(HashMap::new()),
                cancel_policies: Mutex::new(Vec::new()),
                notify: tokio::sync::Notify::new(),
            }),
        }
    }
}

impl GateLauncher {
    /// Waits for and returns the next recorded launch in order.
    pub async fn next_launch(&self) -> ObservedLaunch {
        loop {
            let found = {
                let mut cursor = self.inner.cursor.lock().unwrap();
                let launches = self.inner.launches.lock().unwrap();
                if *cursor < launches.len() {
                    let launch = launches[*cursor].clone();
                    *cursor += 1;
                    Some(launch)
                } else {
                    None
                }
            };
            if let Some(launch) = found {
                return ObservedLaunch {
                    job_id: launch.job_id,
                    source: launch.source,
                    destination: launch.destination,
                };
            }
            self.inner.notify.notified().await;
        }
    }

    /// Resolves the pending completion of `observed`.
    pub async fn complete(&self, observed: ObservedLaunch, outcome: EngineOutcome) {
        self.complete_job(observed.job_id, outcome).await;
    }

    /// Resolves one pending completion for `job_id`.
    pub async fn complete_job(&self, job_id: JobId, outcome: EngineOutcome) {
        let sender = self
            .inner
            .completions
            .lock()
            .unwrap()
            .get_mut(&job_id)
            .and_then(Vec::pop);
        if let Some(sender) = sender {
            let _ = sender.send(outcome);
        }
    }

    /// Number of launches recorded so far.
    pub fn launch_count(&self) -> u64 {
        self.inner.launches.lock().unwrap().len() as u64
    }

    /// Waits until `job_id` has been recorded at least once.
    pub async fn wait_for_launch(&self, job_id: JobId) {
        loop {
            if self.total_launches_for(job_id) > 0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// Total recorded launches for one job across retries.
    pub fn total_launches_for(&self, job_id: JobId) -> u64 {
        self.inner
            .launches
            .lock()
            .unwrap()
            .iter()
            .filter(|launch| launch.job_id == job_id)
            .count() as u64
    }

    /// The most recent cancellation policy a handle received.
    #[allow(dead_code)] // consumed by Task 5 shutdown semantics
    pub fn cancel_policy(&self) -> Option<CancelArtifactPolicy> {
        self.inner.cancel_policies.lock().unwrap().last().copied()
    }
}

pub type GateCompletion =
    std::pin::Pin<Box<dyn std::future::Future<Output = EngineOutcome> + Send>>;

impl EngineLauncher for GateLauncher {
    type Handle = GateControl;
    type Completion = GateCompletion;

    async fn launch(
        &self,
        launch: kdown_app::engine_adapter::EngineLaunch,
    ) -> Result<EngineRun<Self::Handle, Self::Completion>, AppError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut launches = self.inner.launches.lock().unwrap();
            launches.push(launch.clone());
        }
        self.inner
            .completions
            .lock()
            .unwrap()
            .entry(launch.job_id)
            .or_default()
            .push(tx);
        self.inner.notify.notify_one();

        let handle = GateControl {
            job_id: launch.job_id,
            destination: launch.destination.clone(),
            inner: Arc::clone(&self.inner),
        };
        let completion: GateCompletion = Box::pin(async move {
            rx.await.unwrap_or(EngineOutcome::Failed {
                code: "gate_dropped".to_string(),
                detail: None,
            })
        });
        Ok(EngineRun { handle, completion })
    }

    fn set_global_rate_limit(&self, _bytes_per_second: Option<u64>) -> Result<(), AppError> {
        Ok(())
    }
}

/// Control surface for a gated launch.
pub struct GateControl {
    job_id: JobId,
    destination: PathBuf,
    inner: Arc<GateInner>,
}

impl EngineControl for GateControl {
    fn state(&self) -> EngineStateView {
        EngineStateView {
            label: "Running".to_string(),
            terminal: false,
        }
    }

    fn snapshot(&self) -> EngineSnapshotView {
        EngineSnapshotView {
            state_label: "Running".to_string(),
            bytes_received: 0,
            network_bytes: 0,
            reused_bytes: 0,
            retries: 0,
            elapsed_ms: 0,
        }
    }

    fn pause(&self) -> Result<(), AppError> {
        Ok(())
    }

    fn resume_now(&self) -> Result<(), AppError> {
        Ok(())
    }

    fn cancel_with(&self, policy: CancelArtifactPolicy) -> Result<(), AppError> {
        self.inner.cancel_policies.lock().unwrap().push(policy);
        if let Some(sender) = self
            .inner
            .completions
            .lock()
            .unwrap()
            .get_mut(&self.job_id)
            .and_then(Vec::pop)
        {
            let _ = sender.send(EngineOutcome::Cancelled);
        }
        Ok(())
    }

    fn resolved_destination(&self) -> Option<PathBuf> {
        Some(self.destination.clone())
    }
}
