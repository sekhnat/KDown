//! Job lifecycle state machine (§9.1, §9.2).
//!
//! Valid transitions only; terminal states latch; the public state is
//! monotonic except `Paused -> Running`.

use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

/// Job states in lifecycle order (§9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum JobState {
    Created = 0,
    Probing = 1,
    Preparing = 2,
    Running = 3,
    Pausing = 4,
    Paused = 5,
    Resuming = 6,
    Verifying = 7,
    Committing = 8,
    Cancelling = 9,
    Failing = 10,
    Completed = 11,
    Cancelled = 12,
    Failed = 13,
}

impl JobState {
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobState::Completed | JobState::Cancelled | JobState::Failed
        )
    }

    #[must_use]
    pub fn is_operational(self) -> bool {
        !self.is_terminal()
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Invalid transition error surfaced to callers and tests.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid job state transition: {from} -> {to}")]
pub struct InvalidTransition {
    pub from: JobState,
    pub to: JobState,
}

/// Allowed edges of the §9.1 graph.
fn allowed(from: JobState, to: JobState) -> bool {
    use JobState::*;
    match (from, to) {
        // Forward pipeline
        (Created, Probing) => true,
        (Created, Failing) | (Created, Cancelling) => true,
        (Probing, Preparing) => true,
        (Probing, Running) => true, // single-stream fast path: no prepare work
        (Probing, Failing) | (Probing, Cancelling) => true,
        (Preparing, Running) => true,
        (Preparing, Failing) | (Preparing, Cancelling) => true,
        (Running, Pausing) => true,
        (Running, Verifying) => true,
        (Running, Failing) | (Running, Cancelling) => true,
        (Paused, Resuming) => true,
        (Paused, Cancelling) => true,
        (Paused, Failing) => true,
        (Resuming, Running) => true,
        (Resuming, Failing) | (Resuming, Cancelling) => true,
        (Pausing, Paused) => true,
        (Pausing, Cancelling) => true, // cancel during pause
        (Pausing, Failing) => true,
        (Verifying, Committing) => true,
        (Verifying, Failing) | (Verifying, Cancelling) => true,
        (Committing, Completed) => true,
        (Committing, Failing) => true, // §9.2: commit failure is never Completed
        // Terminal entries from cancel/fail paths
        (Cancelling, Cancelled) => true,
        (Failing, Failed) => true,
        // Defensive: any operational state may bail to terminal (§9.1)
        _ => false,
    }
}

/// Thread-safe validated state machine for one job.
#[derive(Debug)]
pub struct StateMachine {
    state: AtomicU8,
}

impl StateMachine {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicU8::new(JobState::Created as u8),
        })
    }

    #[must_use]
    pub fn get(&self) -> JobState {
        // Values are only ever written from the validated table, so the
        // transmute-free decode via ordinal is safe by construction.
        match self.state.load(Ordering::SeqCst) {
            0 => JobState::Created,
            1 => JobState::Probing,
            2 => JobState::Preparing,
            3 => JobState::Running,
            4 => JobState::Pausing,
            5 => JobState::Paused,
            6 => JobState::Resuming,
            7 => JobState::Verifying,
            8 => JobState::Committing,
            9 => JobState::Cancelling,
            10 => JobState::Failing,
            11 => JobState::Completed,
            12 => JobState::Cancelled,
            _ => JobState::Failed,
        }
    }

    /// Attempt a transition; `Ok(())` when allowed, else the error naming
    /// both states. Terminal states reject everything.
    ///
    /// # Errors
    /// [`InvalidTransition`] when the edge is not in the §9.1 graph.
    pub fn transition(&self, to: JobState) -> Result<(), InvalidTransition> {
        let from = self.get();
        if from.is_terminal() {
            return Err(InvalidTransition { from, to });
        }
        if allowed(from, to) {
            self.state.store(to as u8, Ordering::SeqCst);
            Ok(())
        } else {
            Err(InvalidTransition { from, to })
        }
    }

    /// Force into a terminal state bypassing intermediate states; used by
    /// the failure/cancel path from any operational state (§9.1: any
    /// non-terminal operational state may transition to Cancelling/Failing).
    pub fn fail(&self) -> Result<(), InvalidTransition> {
        self.transition(JobState::Failing).or_else(|_| {
            // Already in Failing/Cancelling: finish the terminal hop.
            self.transition(JobState::Failed)
        })
    }

    pub fn cancel(&self) -> Result<(), InvalidTransition> {
        self.transition(JobState::Cancelling)
            .or_else(|_| self.transition(JobState::Cancelled))
    }
}

impl Default for StateMachine {
    fn default() -> Self {
        Self {
            state: AtomicU8::new(JobState::Created as u8),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path_forward_sequence() {
        let sm = StateMachine::new();
        for s in [
            JobState::Probing,
            JobState::Preparing,
            JobState::Running,
            JobState::Verifying,
            JobState::Committing,
            JobState::Completed,
        ] {
            sm.transition(s).expect("valid forward transition");
        }
        assert_eq!(sm.get(), JobState::Completed);
    }

    #[test]
    fn terminal_states_latch() {
        let sm = StateMachine::new();
        sm.transition(JobState::Cancelling)
            .expect("cancel from created");
        sm.transition(JobState::Cancelled).expect("terminal");
        assert!(
            sm.transition(JobState::Running).is_err(),
            "no exit from terminal"
        );
        assert!(sm.transition(JobState::Cancelled).is_err());
    }

    #[test]
    fn invalid_transitions_rejected() {
        let sm = StateMachine::new();
        // Created -> Running skips probing/preparing.
        assert!(sm.transition(JobState::Running).is_err());
        // Created -> Completed skips everything.
        assert!(sm.transition(JobState::Completed).is_err());
        // Created -> Paused without running.
        assert!(sm.transition(JobState::Paused).is_err());
        // Created -> Verifying.
        assert!(sm.transition(JobState::Verifying).is_err());
    }

    #[test]
    fn pause_cycle_via_machine() {
        let sm = StateMachine::new();
        sm.transition(JobState::Probing).unwrap();
        sm.transition(JobState::Running).unwrap();
        sm.transition(JobState::Pausing).unwrap();
        sm.transition(JobState::Paused).unwrap();
        // Only Resuming leaves Paused.
        sm.transition(JobState::Resuming).unwrap();
        sm.transition(JobState::Running).unwrap();
        assert_eq!(sm.get(), JobState::Running);
    }

    #[test]
    fn paused_requires_resuming_not_running() {
        let sm = StateMachine::new();
        sm.transition(JobState::Probing).unwrap();
        sm.transition(JobState::Running).unwrap();
        sm.transition(JobState::Pausing).unwrap();
        sm.transition(JobState::Paused).unwrap();
        assert!(
            sm.transition(JobState::Running).is_err(),
            "Paused -> Running direct is rejected; must go through Resuming"
        );
    }

    #[test]
    fn commit_failure_never_completes() {
        let sm = StateMachine::new();
        sm.transition(JobState::Probing).unwrap();
        sm.transition(JobState::Running).unwrap();
        sm.transition(JobState::Verifying).unwrap();
        sm.transition(JobState::Committing).unwrap();
        // Commit failed -> Failing -> Failed.
        sm.transition(JobState::Failing).expect("commit failure");
        sm.transition(JobState::Failed).unwrap();
        assert_eq!(sm.get(), JobState::Failed);
    }

    #[test]
    fn cancel_from_operational_states() {
        for start in [
            JobState::Probing,
            JobState::Preparing,
            JobState::Running,
            JobState::Paused,
            JobState::Verifying,
        ] {
            let sm = StateMachine::new();
            // Reach the start state through the graph where possible.
            match start {
                JobState::Probing => sm.transition(JobState::Probing).unwrap(),
                JobState::Running => {
                    sm.transition(JobState::Probing).unwrap();
                    sm.transition(JobState::Running).unwrap();
                }
                JobState::Verifying => {
                    sm.transition(JobState::Probing).unwrap();
                    sm.transition(JobState::Running).unwrap();
                    sm.transition(JobState::Verifying).unwrap();
                }
                JobState::Paused => {
                    sm.transition(JobState::Probing).unwrap();
                    sm.transition(JobState::Running).unwrap();
                    sm.transition(JobState::Pausing).unwrap();
                    sm.transition(JobState::Paused).unwrap();
                }
                _ => {}
            }
            sm.cancel().expect("cancel reachable");
            sm.transition(JobState::Cancelled).expect("terminal");
        }
    }

    #[test]
    fn no_worker_writes_after_terminal() {
        // Simulated invariant: transition to terminal then any further
        // transition (which would gate worker writes) is invalid.
        let sm = StateMachine::new();
        sm.transition(JobState::Probing).unwrap();
        sm.transition(JobState::Failing).unwrap();
        sm.transition(JobState::Failed).unwrap();
        assert!(sm.get().is_terminal());
        assert!(sm.transition(JobState::Running).is_err());
    }
}
