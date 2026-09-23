//! Test-only fault scripting for the output lifecycle boundary.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use crate::error::DownloadError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum OutputOperation {
    Open,
    Write,
    Flush,
    VerificationRead,
    Finalize,
    Publish,
    Cleanup,
}

#[derive(Default)]
struct ScriptState {
    operations: Vec<OutputOperation>,
    failures: HashMap<OutputOperation, VecDeque<DownloadError>>,
    gates: HashMap<OutputOperation, VecDeque<GateSpec>>,
}

struct GateSpec {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

/// One-shot gate handle returned to a test while the scripted operation waits.
pub(crate) struct OutputFaultGate {
    entered: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
}

impl OutputFaultGate {
    pub(crate) fn wait_until_entered(&self) {
        self.entered
            .recv_timeout(Duration::from_secs(10))
            .expect("scripted output operation reached gate");
    }

    pub(crate) fn release(&self) {
        let _ = self.release.send(());
    }
}

/// Shared operation log and FIFO fault/gate queues for one destination.
pub(crate) struct OutputFaultScript {
    state: Mutex<ScriptState>,
}

impl OutputFaultScript {
    pub(crate) fn register(destination: &Path) -> OutputFaultRegistration {
        let script = Arc::new(Self {
            state: Mutex::new(ScriptState::default()),
        });
        registry()
            .lock()
            .expect("output fault registry")
            .insert(destination.to_path_buf(), Arc::downgrade(&script));
        OutputFaultRegistration {
            destination: destination.to_path_buf(),
            script,
        }
    }

    pub(crate) fn fail_next(&self, operation: OutputOperation, error: DownloadError) {
        self.state
            .lock()
            .expect("output fault script")
            .failures
            .entry(operation)
            .or_default()
            .push_back(error);
    }

    pub(crate) fn hold_next(&self, operation: OutputOperation) -> OutputFaultGate {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        self.state
            .lock()
            .expect("output fault script")
            .gates
            .entry(operation)
            .or_default()
            .push_back(GateSpec {
                entered: entered_tx,
                release: release_rx,
            });
        OutputFaultGate {
            entered: entered_rx,
            release: release_tx,
        }
    }

    pub(crate) fn operations(&self) -> Vec<OutputOperation> {
        self.state
            .lock()
            .expect("output fault script")
            .operations
            .clone()
    }

    pub(crate) fn check(&self, operation: OutputOperation) -> Result<(), DownloadError> {
        let gate = {
            let mut state = self.state.lock().expect("output fault script");
            state.operations.push(operation);
            state
                .gates
                .get_mut(&operation)
                .and_then(VecDeque::pop_front)
        };
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            gate.release
                .recv_timeout(Duration::from_secs(30))
                .map_err(|_| DownloadError::Protocol("output fault gate timed out".into()))?;
        }
        self.state
            .lock()
            .expect("output fault script")
            .failures
            .get_mut(&operation)
            .and_then(VecDeque::pop_front)
            .map_or(Ok(()), Err)
    }
}

pub(crate) struct OutputFaultRegistration {
    destination: PathBuf,
    script: Arc<OutputFaultScript>,
}

impl OutputFaultRegistration {
    pub(crate) fn script(&self) -> &Arc<OutputFaultScript> {
        &self.script
    }
}

impl Drop for OutputFaultRegistration {
    fn drop(&mut self) {
        let mut scripts = registry().lock().expect("output fault registry");
        let registered = scripts.get(&self.destination).and_then(Weak::upgrade);
        if registered
            .as_ref()
            .is_some_and(|script| Arc::ptr_eq(script, &self.script))
        {
            scripts.remove(&self.destination);
        }
    }
}

fn registry() -> &'static Mutex<HashMap<PathBuf, Weak<OutputFaultScript>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<OutputFaultScript>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn for_destination(destination: &Path) -> Option<Arc<OutputFaultScript>> {
    let mut scripts = registry().lock().expect("output fault registry");
    let script = scripts.get(destination).and_then(Weak::upgrade);
    if script.is_none() {
        scripts.remove(destination);
    }
    script
}
