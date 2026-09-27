//! Single accounted transfer-memory ledger (design D3, task 3.2).
//!
//! Data-path wiring lands with tasks 3.4/3.5; until then the surface is
//! exercised by this module's tests only.
#![allow(dead_code)]
//!
//! Every live pipeline allocation — network ingress, held/queued frames,
//! writer-held bytes, checkpoint state — is admitted through this ledger
//! once, tagged with its owning component, and released exactly once on
//! drop, write acknowledgement, failure or cancellation. Per-job and
//! engine-wide aggregate caps both apply; per-job component maxima bound
//! each stage of one download.
//!
//! Admission is **fair** (FIFO per pool: a later request never bypasses an
//! earlier waiter that its capacity could not satisfy), **cancellation
//! aware** (dropping a pending reservation acquires nothing — partial
//! acquisitions roll back), and rejects an atomic allocation larger than a
//! cap immediately with a typed [`LedgerRefusal::Oversize`] instead of
//! waiting forever.
//!
//! No hold-and-wait cycles: the acquisition order is fixed
//! (job-component pool → job pool → controller aggregate pool). Every
//! waited-for pool can free capacity independently of the pools the caller
//! already holds, so waiting never blocks the releases that would satisfy
//! the wait. Component *retagging* (frames → writer on ownership transfer)
//! keeps the job and controller charges — the same `Bytes` are never
//! counted twice.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

use crate::config::TransferMemoryConfig;

/// An accounted pipeline component (design D3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Component {
    /// Network/client ingress: read buffers and header metadata admitted
    /// before frame ownership.
    NetworkIngress,
    /// Held/queued payload frames moving toward the writer.
    Frames,
    /// Writer-held bytes: queued and in-flight writes.
    Writer,
    /// Checkpoint state: in-memory ranges plus serialization buffers.
    Checkpoint,
}

impl Component {
    /// All components, in configuration field order.
    #[must_use]
    pub(crate) fn const_all() -> [Component; 4] {
        [
            Component::NetworkIngress,
            Component::Frames,
            Component::Writer,
            Component::Checkpoint,
        ]
    }

    /// Index into ledger component arrays.
    #[must_use]
    pub(crate) fn index(self) -> usize {
        match self {
            Component::NetworkIngress => 0,
            Component::Frames => 1,
            Component::Writer => 2,
            Component::Checkpoint => 3,
        }
    }

    /// Configuration field name (diagnostics and metrics labels).
    #[must_use]
    pub(crate) fn name(self) -> &'static str {
        match self {
            Component::NetworkIngress => "network_ingress",
            Component::Frames => "frames",
            Component::Writer => "writer",
            Component::Checkpoint => "checkpoint",
        }
    }
}

/// Typed insufficient-budget refusal (design D3): an atomic allocation
/// larger than a cap can never fit, so admission refuses immediately
/// instead of waiting forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OversizeRefusal {
    /// Component whose maximum (or a scope cap it feeds) was exceeded.
    pub component: Component,
    /// Atomic allocation size in bytes.
    pub requested: u64,
    /// Tightest binding cap in bytes.
    pub cap: u64,
}

/// Typed admission refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LedgerRefusal {
    /// The atomic allocation exceeds a cap; it can never be admitted.
    Oversize(OversizeRefusal),
    /// The allocation fits the caps but capacity is currently held;
    /// `try_reserve` does not wait. A pending [`JobLedger::reserve`]
    /// would queue fairly instead.
    NoCapacity(Component),
}

/// Outcome of a non-blocking capacity probe.
enum Probe {
    /// Acquired.
    Acquired,
    /// Can never fit: typed oversize refusal.
    Oversize(OversizeRefusal),
    /// Fits caps but capacity is currently held.
    Full,
}

/// One fair FIFO byte pool: capacity `cap`, atomically tracked outstanding
/// bytes, a FIFO waiter queue, and a high-water mark. Pure counting — no
/// payload data flows through a pool, so relaxed semantics suffice.
struct Pool {
    cap: u64,
    state: Mutex<PoolState>,
}

struct PoolState {
    outstanding: u64,
    high_water: u64,
    /// FIFO queue of waiters; only the head may acquire (fairness).
    waiters: VecDeque<Arc<Notify>>,
}

/// A registered waiter slot. Removing it on drop is what makes admission
/// cancellation-aware: a dropped future leaves the queue and capacity
/// unchanged and the next waiter proceeds.
struct Ticket {
    pool: Arc<Pool>,
    notify: Arc<Notify>,
    queued: bool,
}

impl Ticket {
    /// Register at the queue tail before the capacity re-check so a
    /// concurrent release can never be slept through.
    fn register(pool: &Arc<Pool>) -> Self {
        let notify = Arc::new(Notify::new());
        {
            let mut state = pool.state.lock().expect("pool state poisoned");
            state.waiters.push_back(Arc::clone(&notify));
        }
        Self {
            pool: Arc::clone(pool),
            notify,
            queued: true,
        }
    }

    /// Remove the ticket from the queue and wake the new head.
    fn deregister(&mut self) {
        if !self.queued {
            return;
        }
        self.queued = false;
        let next_head = {
            let mut state = self.pool.state.lock().expect("pool state poisoned");
            let pos = state
                .waiters
                .iter()
                .position(|w| Arc::ptr_eq(w, &self.notify));
            if let Some(pos) = pos {
                drop(state.waiters.remove(pos));
            }
            state.waiters.front().cloned()
        };
        if let Some(head) = next_head {
            head.notify_one();
        }
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.deregister();
    }
}

impl Pool {
    fn new(cap: u64) -> Self {
        Self {
            cap,
            state: Mutex::new(PoolState {
                outstanding: 0,
                high_water: 0,
                waiters: VecDeque::new(),
            }),
        }
    }

    /// Non-blocking capacity probe and, on success, the actual charge.
    /// A queued waiter gates the fast path (fairness): a new request never
    /// bypasses an earlier waiter.
    fn probe_and_acquire(&self, bytes: u64) -> Probe {
        if bytes > self.cap {
            return Probe::Oversize(OversizeRefusal {
                component: Component::Frames, // replaced by the caller
                requested: bytes,
                cap: self.cap,
            });
        }
        let mut state = self.state.lock().expect("pool state poisoned");
        if !state.waiters.is_empty() {
            return Probe::Full;
        }
        Self::acquire_locked(&mut state, bytes, self.cap)
    }

    /// Head-only admission from the waiter queue. On success the caller's
    /// bytes are charged; the ticket must then be deregistered.
    fn poll_head(&self, ticket: &Ticket, bytes: u64) -> Probe {
        let mut state = self.state.lock().expect("pool state poisoned");
        let is_head = state
            .waiters
            .front()
            .is_some_and(|w| Arc::ptr_eq(w, &ticket.notify));
        if !is_head {
            return Probe::Full;
        }
        Self::acquire_locked(&mut state, bytes, self.cap)
    }

    /// Charge `bytes` when capacity allows. Caller holds the lock.
    fn acquire_locked(state: &mut PoolState, bytes: u64, cap: u64) -> Probe {
        debug_assert!(bytes <= cap, "oversize allocation reached pool admission");
        let Some(new) = state.outstanding.checked_add(bytes) else {
            return Probe::Full; // headroom guarded by config validation
        };
        if new > cap {
            return Probe::Full;
        }
        state.outstanding = new;
        state.high_water = state.high_water.max(new);
        Probe::Acquired
    }

    /// Fair, cancellation-aware acquire: waits in FIFO order for capacity.
    /// Dropping the returned future before completion acquires nothing.
    /// `self` is held as an `Arc` so the cancellation guard can deregister.
    async fn acquire_fair(self: &Arc<Self>, bytes: u64) -> Result<(), LedgerRefusal> {
        match self.probe_and_acquire(bytes) {
            Probe::Acquired => return Ok(()),
            Probe::Oversize(refusal) => return Err(LedgerRefusal::Oversize(refusal)),
            Probe::Full => {}
        }
        let mut ticket = Ticket::register(self);
        loop {
            let notify = Arc::clone(&ticket.notify);
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.poll_head(&ticket, bytes) {
                Probe::Acquired => {
                    ticket.deregister();
                    return Ok(());
                }
                Probe::Oversize(_) => unreachable!("capacity was pre-checked"),
                Probe::Full => {}
            }
            notified.await;
        }
    }

    /// Release `bytes` and wake the head waiter so it re-checks.
    fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let head = {
            let mut state = self.state.lock().expect("pool state poisoned");
            debug_assert!(
                state.outstanding >= bytes,
                "ledger released more bytes than it held"
            );
            state.outstanding -= bytes;
            state.waiters.front().cloned()
        };
        if let Some(head) = head {
            head.notify_one();
        }
    }

    /// Bytes currently held.
    #[must_use]
    fn outstanding(&self) -> u64 {
        self.state.lock().expect("pool state poisoned").outstanding
    }

    /// Peak bytes ever held (monotonic).
    #[must_use]
    fn high_water(&self) -> u64 {
        self.state.lock().expect("pool state poisoned").high_water
    }

    /// Configured capacity.
    #[must_use]
    fn cap(&self) -> u64 {
        self.cap
    }
}

/// One component's configured cap and observed memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct ComponentMemory {
    /// Configured cap in bytes.
    pub cap: u64,
    /// Bytes currently held (accounted pipeline memory; true zero when
    /// nothing is held — unaccounted buffers are outside this snapshot by
    /// construction and are never folded into this number).
    pub current: u64,
    /// Peak bytes ever held (monotonic for the ledger's lifetime).
    pub high_water: u64,
}

/// Point-in-time view of the transfer-memory ledger (task 3.7).
///
/// Scope (documented lifetime): the snapshot covers the ENGINE-ACCOUNTED
/// pipeline memory only — the ledger pools of live jobs, connection
/// ingress footprints, and checkpoint reservations. Operating-system
/// socket/kernel buffers, allocator overhead, and HTTP client internals
/// beyond the configured ingress windows are unaccounted; they are never
/// reported here as zero. The high-water marks live for the ledger's
/// lifetime (the engine context).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TransferMemorySnapshot {
    /// What the snapshot accounts (and what it deliberately does not).
    pub scope: &'static str,
    /// Engine-wide aggregate pool.
    pub aggregate: ComponentMemory,
    /// Per-component pools, keyed by component name.
    pub components: std::collections::BTreeMap<&'static str, ComponentMemory>,
}

/// Per-component counters with high-water tracking, at both scopes. At
/// controller scope these are metrics-only (admission is governed by the
/// job component pools and the aggregate pool; component maxima are
/// per-job by design D3). At job scope the ledger pools themselves carry
/// the same numbers, so only the controller scope needs separate meters.
#[derive(Debug)]
struct ComponentMeter {
    state: Mutex<(u64, u64)>, // (outstanding, high_water)
}

impl ComponentMeter {
    fn new() -> Self {
        Self {
            state: Mutex::new((0, 0)),
        }
    }

    fn charge(&self, bytes: u64) {
        let mut state = self.state.lock().expect("meter poisoned");
        state.0 += bytes;
        state.1 = state.1.max(state.0);
    }

    fn release(&self, bytes: u64) {
        let mut state = self.state.lock().expect("meter poisoned");
        debug_assert!(state.0 >= bytes, "meter released more than held");
        state.0 -= bytes;
    }

    fn outstanding(&self) -> u64 {
        self.state.lock().expect("meter poisoned").0
    }

    fn high_water(&self) -> u64 {
        self.state.lock().expect("meter poisoned").1
    }
}

type Meters = Arc<[ComponentMeter; 4]>;

/// The engine-wide (controller) scope of the ledger: one aggregate pool
/// shared by every job plus per-component meters for observability.
pub(crate) struct TransferLedger {
    aggregate: Arc<Pool>,
    meters: Meters,
    /// Configured component caps (context for the metrics snapshot).
    component_caps: [u64; 4],
    /// The configured aggregate cap (the pool's cap is that minus the
    /// connection-ingress carve-out).
    aggregate_config_cap: u64,
}

impl TransferLedger {
    /// Build from validated configuration (construction validation lives
    /// in `EngineConfig::validate`). `connection_ingress_reserve` is the
    /// pre-accounted worst-case total connection footprint
    /// (`max_connections_total × connection_ingress_footprint`, validated
    /// to fit): the aggregate pool's cap is reduced by it so the pipeline
    /// can never be starved by long-lived connections (task 3.8).
    #[must_use]
    pub(crate) fn new(config: &TransferMemoryConfig, connection_ingress_reserve: u64) -> Self {
        let pipeline_cap = config
            .aggregate_max_bytes
            .saturating_sub(connection_ingress_reserve);
        Self {
            aggregate: Arc::new(Pool::new(pipeline_cap)),
            meters: Arc::new(std::array::from_fn(|_| ComponentMeter::new())),
            component_caps: Self::component_caps(config),
            aggregate_config_cap: config.aggregate_max_bytes,
        }
    }

    /// Component caps in configuration order.
    fn component_caps(config: &TransferMemoryConfig) -> [u64; 4] {
        [
            config.network_ingress_max_bytes,
            config.frames_max_bytes,
            config.writer_max_bytes,
            config.checkpoint_max_bytes,
        ]
    }

    /// Derive one job's ledger handle sharing this aggregate pool.
    #[must_use]
    pub(crate) fn job(&self, config: &TransferMemoryConfig) -> JobLedger {
        JobLedger {
            controller: Arc::clone(&self.aggregate),
            job: Arc::new(Pool::new(config.job_max_bytes)),
            components: Component::const_all().map(|component| {
                let cap = Self::component_caps(config)[component.index()];
                Arc::new(Pool::new(cap))
            }),
            meters: Arc::clone(&self.meters),
            component_caps: Self::component_caps(config),
        }
    }

    /// Engine-wide aggregate bytes currently held.
    #[must_use]
    pub(crate) fn aggregate_outstanding(&self) -> u64 {
        self.aggregate.outstanding()
    }

    /// Engine-wide aggregate peak.
    #[must_use]
    pub(crate) fn aggregate_high_water(&self) -> u64 {
        self.aggregate.high_water()
    }

    /// Engine-wide aggregate cap.
    #[must_use]
    pub(crate) fn aggregate_cap(&self) -> u64 {
        self.aggregate.cap()
    }

    /// Account engine-wide connection ingress: the worst-case buffered
    /// footprint of one transport connection, metered (current + high
    /// water) for observability while the connection lives. The total
    /// connection memory is bounded by the carve-out taken from the
    /// aggregate cap at construction — the footprint is known and
    /// accounted BEFORE any connection is established (design D3, tasks
    /// 3.3/3.8); the meter charge is observation, not admission.
    pub(crate) fn charge_connection_ingress(&self, bytes: u64) -> ConnectionIngressReservation {
        self.meters[Component::NetworkIngress.index()].charge(bytes);
        ConnectionIngressReservation {
            ledger: LedgerArcs {
                meters: Arc::clone(&self.meters),
            },
            bytes,
        }
    }

    /// Per-component aggregate bytes currently held (all jobs).
    #[must_use]
    pub(crate) fn component_outstanding(&self, component: Component) -> u64 {
        self.meters[component.index()].outstanding()
    }

    /// Per-component aggregate peak (all jobs).
    #[must_use]
    pub(crate) fn component_high_water(&self, component: Component) -> u64 {
        self.meters[component.index()].high_water()
    }

    /// Point-in-time view for metrics export (task 3.7).
    #[must_use]
    pub(crate) fn snapshot(&self) -> TransferMemorySnapshot {
        TransferMemorySnapshot {
            scope: SCOPE_NOTE,
            aggregate: ComponentMemory {
                cap: self.aggregate_config_cap,
                current: self.aggregate_outstanding(),
                high_water: self.aggregate_high_water(),
            },
            components: Component::const_all()
                .into_iter()
                .map(|component| {
                    (
                        component.name(),
                        ComponentMemory {
                            cap: self.component_cap_of(component),
                            current: self.component_outstanding(component),
                            high_water: self.component_high_water(component),
                        },
                    )
                })
                .collect(),
        }
    }

    /// The configured component cap (the aggregate pool has none of its
    /// own: component maxima are per-job by design D3, so the controller
    /// scope reports the configured job-level component cap for context).
    fn component_cap_of(&self, component: Component) -> u64 {
        self.component_caps[component.index()]
    }
}

/// Documented scope of every snapshot (task 3.7: no unknown-buffer-as-zero
/// reporting — unaccounted memory is named, never folded into zeros).
const SCOPE_NOTE: &str = "accounted pipeline memory only: ledger pools of live jobs, connection ingress footprints, and checkpoint reservations. OS socket/kernel buffers, allocator overhead, and HTTP client internals beyond the configured ingress windows are unaccounted and never reported as zero.";

impl std::fmt::Debug for TransferLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransferLedger")
            .field("aggregate_outstanding", &self.aggregate_outstanding())
            .field("aggregate_high_water", &self.aggregate_high_water())
            .finish()
    }
}

/// One job's ledger handle: per-component pools (the job's component
/// maxima), a job pool (the job cap), and the shared controller aggregate
/// pool. Every reservation charges exactly one component, the job pool and
/// the aggregate pool.
pub(crate) struct JobLedger {
    controller: Arc<Pool>,
    job: Arc<Pool>,
    components: [Arc<Pool>; 4],
    meters: Meters,
    component_caps: [u64; 4],
}

impl JobLedger {
    /// The tightest cap an atomic allocation of `component` must fit.
    fn tightest_cap(&self, component: Component) -> u64 {
        self.component_caps[component.index()]
            .min(self.job.cap())
            .min(self.controller.cap())
    }

    /// Reserve `bytes` of `component`, waiting fairly for capacity.
    ///
    /// Cancellation-aware: dropping the future mid-wait acquires nothing
    /// (partially acquired pools roll back). An atomic allocation larger
    /// than any cap fails immediately with [`LedgerRefusal::Oversize`]
    /// instead of waiting.
    ///
    /// # Errors
    /// Returns a typed refusal when the allocation can never fit.
    pub(crate) async fn reserve(
        &self,
        component: Component,
        bytes: u64,
    ) -> Result<TransferReservation, LedgerRefusal> {
        let index = component.index();
        if bytes > self.tightest_cap(component) {
            return Err(LedgerRefusal::Oversize(OversizeRefusal {
                component,
                requested: bytes,
                cap: self.tightest_cap(component),
            }));
        }
        // Fixed order: component → job → controller. Each later pool can
        // free capacity independently of the earlier ones, so no wait is
        // ever blocked by the pools already held (no hold-and-wait cycle).
        self.components[index].acquire_fair(bytes).await?;
        // Armed before the next await so cancellation rolls back.
        let rollback_component = PoolRollback {
            pool: Arc::clone(&self.components[index]),
            armed: bytes,
        };
        self.job.acquire_fair(bytes).await?;
        let rollback_job = PoolRollback {
            pool: Arc::clone(&self.job),
            armed: bytes,
        };
        self.controller.acquire_fair(bytes).await?;
        // All three pools are charged; disarm the rollback guards by
        // value (forgetting a reference would leave the guard armed).
        std::mem::forget(rollback_job);
        std::mem::forget(rollback_component);
        self.meters[index].charge(bytes);
        Ok(TransferReservation {
            links: ReservationLinks {
                controller: Arc::clone(&self.controller),
                job: Arc::clone(&self.job),
                component: Arc::clone(&self.components[index]),
                components: std::array::from_fn(|i| Arc::clone(&self.components[i])),
                meter_index: index,
                meters: Arc::clone(&self.meters),
                tightest_cap: self.tightest_cap(component),
            },
            component,
            bytes,
        })
    }

    /// Non-blocking variant: `Ok` on success, typed refusal otherwise.
    /// `LedgerRefusal::NoCapacity` means a waiting `reserve` would queue.
    ///
    /// # Errors
    /// Returns a typed refusal instead of waiting for capacity.
    pub(crate) fn try_reserve(
        &self,
        component: Component,
        bytes: u64,
    ) -> Result<TransferReservation, LedgerRefusal> {
        let index = component.index();
        if bytes > self.tightest_cap(component) {
            return Err(LedgerRefusal::Oversize(OversizeRefusal {
                component,
                requested: bytes,
                cap: self.tightest_cap(component),
            }));
        }
        match self.components[index].probe_and_acquire(bytes) {
            Probe::Acquired => {}
            Probe::Oversize(refusal) => return Err(LedgerRefusal::Oversize(refusal)),
            Probe::Full => return Err(LedgerRefusal::NoCapacity(component)),
        }
        let rollback_component = PoolRollback {
            pool: Arc::clone(&self.components[index]),
            armed: bytes,
        };
        match self.job.probe_and_acquire(bytes) {
            Probe::Acquired => {}
            Probe::Oversize(refusal) => return Err(LedgerRefusal::Oversize(refusal)),
            Probe::Full => return Err(LedgerRefusal::NoCapacity(component)),
        }
        std::mem::forget(rollback_component);
        let rollback_job = PoolRollback {
            pool: Arc::clone(&self.job),
            armed: bytes,
        };
        match self.controller.probe_and_acquire(bytes) {
            Probe::Acquired => {}
            Probe::Oversize(refusal) => return Err(LedgerRefusal::Oversize(refusal)),
            Probe::Full => return Err(LedgerRefusal::NoCapacity(component)),
        }
        std::mem::forget(rollback_job);
        self.meters[index].charge(bytes);
        Ok(TransferReservation {
            links: ReservationLinks {
                controller: Arc::clone(&self.controller),
                job: Arc::clone(&self.job),
                component: Arc::clone(&self.components[index]),
                components: std::array::from_fn(|i| Arc::clone(&self.components[i])),
                meter_index: index,
                meters: Arc::clone(&self.meters),
                tightest_cap: self.tightest_cap(component),
            },
            component,
            bytes,
        })
    }

    /// Per-component bytes currently held by this job.
    #[must_use]
    pub(crate) fn component_outstanding(&self, component: Component) -> u64 {
        self.components[component.index()].outstanding()
    }

    /// Per-component peak held by this job.
    #[must_use]
    pub(crate) fn component_high_water(&self, component: Component) -> u64 {
        self.components[component.index()].high_water()
    }

    /// Job-total bytes currently held (all components).
    #[must_use]
    pub(crate) fn job_outstanding(&self) -> u64 {
        self.job.outstanding()
    }

    /// Job-total peak.
    #[must_use]
    pub(crate) fn job_high_water(&self) -> u64 {
        self.job.high_water()
    }

    /// Job cap.
    #[must_use]
    pub(crate) fn job_cap(&self) -> u64 {
        self.job.cap()
    }

    /// Component cap for this job.
    #[must_use]
    pub(crate) fn component_cap(&self, component: Component) -> u64 {
        self.component_caps[component.index()]
    }
}

impl std::fmt::Debug for JobLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobLedger")
            .field("job_outstanding", &self.job_outstanding())
            .field("job_high_water", &self.job_high_water())
            .finish()
    }
}

/// Back-links a reservation keeps so it releases exactly what it charged.
struct ReservationLinks {
    controller: Arc<Pool>,
    job: Arc<Pool>,
    /// The pool of the CURRENT component tag (retagging swaps this).
    component: Arc<Pool>,
    /// All component pools, so a retag can acquire the target pool.
    components: [Arc<Pool>; 4],
    meter_index: usize,
    meters: Meters,
    /// Tightest cap an atomic allocation (including growth) must fit.
    tightest_cap: u64,
}

/// Arcs a connection-footprint meter charge keeps for its release.
struct LedgerArcs {
    meters: Meters,
}

/// The engine-wide ingress charge for one transport connection, held for
/// the connection's lifetime and released exactly once on drop (task 3.3).
pub(crate) struct ConnectionIngressReservation {
    ledger: LedgerArcs,
    bytes: u64,
}

impl std::fmt::Debug for ConnectionIngressReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionIngressReservation")
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl ConnectionIngressReservation {
    /// Reserved footprint in bytes.
    #[must_use]
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for ConnectionIngressReservation {
    fn drop(&mut self) {
        self.ledger.meters[Component::NetworkIngress.index()].release(self.bytes);
    }
}

/// An ownership-tagged byte reservation (design D3): the ledger charge for
/// `bytes` of `component` at job and controller scope, released exactly
/// once on drop.
pub(crate) struct TransferReservation {
    links: ReservationLinks,
    component: Component,
    bytes: u64,
}

impl std::fmt::Debug for TransferReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransferReservation")
            .field("component", &self.component)
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl TransferReservation {
    /// Reserved size in bytes.
    #[must_use]
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Owning component tag.
    #[must_use]
    pub(crate) fn component(&self) -> Component {
        self.component
    }

    /// Acquire `extra` bytes onto this reservation with the same fair,
    /// cancellation-aware admission as [`JobLedger::reserve`] (used to
    /// reconcile an oversize frame in the pipelined write path).
    ///
    /// # Errors
    /// Returns a typed refusal when the enlarged allocation can never fit;
    /// the reservation is unchanged.
    pub(crate) async fn grow(&mut self, extra: u64) -> Result<(), LedgerRefusal> {
        if extra == 0 {
            return Ok(());
        }
        let component = self.component;
        // Oversize check for the enlarged atomic allocation.
        let grown = self.bytes + extra;
        if grown > self.links.tightest_cap {
            return Err(LedgerRefusal::Oversize(OversizeRefusal {
                component,
                requested: grown,
                cap: self.links.tightest_cap,
            }));
        }
        // Same fixed order as `reserve`; the pools already held stay
        // charged throughout (they free independently — no cycle).
        self.links.component.acquire_fair(extra).await?;
        let rollback_component = PoolRollback {
            pool: Arc::clone(&self.links.component),
            armed: extra,
        };
        self.links.job.acquire_fair(extra).await?;
        let rollback_job = PoolRollback {
            pool: Arc::clone(&self.links.job),
            armed: extra,
        };
        self.links.controller.acquire_fair(extra).await?;
        std::mem::forget(rollback_job);
        std::mem::forget(rollback_component);
        self.links.meters[self.links.meter_index].charge(extra);
        self.bytes += extra;
        Ok(())
    }

    /// Release down to `bytes` when the actual frame is smaller than the
    /// pre-read quantum (pipelined reconcile): the excess returns to every
    /// pool immediately.
    pub(crate) fn shrink_to(&mut self, bytes: u64) {
        let Some(excess) = self.bytes.checked_sub(bytes) else {
            debug_assert!(false, "shrink_to above the held size is a no-op");
            return;
        };
        if excess == 0 {
            return;
        }
        self.bytes = bytes;
        self.links.controller.release(excess);
        self.links.meters[self.links.meter_index].release(excess);
        self.links.job.release(excess);
        self.links.component.release(excess);
    }

    /// Reconcile the reservation to the actual frame size: a smaller frame
    /// releases its excess; a larger frame must go through the async
    /// [`grow`](Self::grow) first (the caller's contract).
    pub(crate) fn reconcile(&mut self, actual: u64) {
        if actual < self.bytes {
            self.shrink_to(actual);
        }
    }

    /// Move the ownership tag to `to` without re-charging the job or
    /// controller pools: the same bytes remain counted exactly once while
    /// the component attribution follows the payload.
    ///
    /// Waits fairly for `to` capacity while the bytes stay charged to the
    /// current component (they genuinely are still held), then releases
    /// the old component charge. Cancelling the wait keeps the reservation
    /// tagged as before.
    ///
    /// # Errors
    /// Returns [`LedgerRefusal::Oversize`] when the transfer can never fit
    /// the target component cap; the reservation is unchanged.
    pub(crate) async fn retag(&mut self, to: Component) -> Result<(), LedgerRefusal> {
        if to == self.component {
            return Ok(());
        }
        let target = Arc::clone(&self.links.components[to.index()]);
        if self.bytes > target.cap() {
            return Err(LedgerRefusal::Oversize(OversizeRefusal {
                component: to,
                requested: self.bytes,
                cap: target.cap(),
            }));
        }
        // Wait for target capacity while the bytes stay charged to the
        // current component: they genuinely are still held, so the charge
        // must not move yet. Cancelling here leaves the tag unchanged.
        target.acquire_fair(self.bytes).await?;
        // Swap the tag: release the old component charge (waking its head
        // waiter), move the controller-scope meter attribution, then adopt
        // the new pool. No await intervenes, so the swap is atomic with
        // respect to cancellation.
        self.links.component.release(self.bytes);
        self.links.meters[self.links.meter_index].release(self.bytes);
        self.links.component = target;
        self.links.meter_index = to.index();
        self.component = to;
        self.links.meters[self.links.meter_index].charge(self.bytes);
        Ok(())
    }
}

impl Drop for TransferReservation {
    fn drop(&mut self) {
        // Release in reverse acquisition order; each release wakes the
        // pool's head waiter if capacity changed.
        self.links.controller.release(self.bytes);
        self.links.meters[self.links.meter_index].release(self.bytes);
        self.links.job.release(self.bytes);
        self.links.component.release(self.bytes);
    }
}

/// Rollback guard for a partially completed multi-pool acquisition: on
/// drop (including cancellation), releases the pool charged so far.
struct PoolRollback {
    pool: Arc<Pool>,
    armed: u64,
}

impl Drop for PoolRollback {
    fn drop(&mut self) {
        if self.armed > 0 {
            self.pool.release(self.armed);
            self.armed = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small ledger set: 1 KiB components, 2 KiB job cap, 4 KiB
    /// aggregate.
    fn small_ledger() -> (TransferLedger, TransferMemoryConfig) {
        let config = TransferMemoryConfig {
            aggregate_max_bytes: 4 * 1024,
            job_max_bytes: 2 * 1024,
            network_ingress_max_bytes: 1024,
            frames_max_bytes: 1024,
            writer_max_bytes: 1024,
            checkpoint_max_bytes: 1024,
        };
        // Unit tests exercise the pipeline pools with no connections: a
        // zero carve-out keeps the small test caps meaningful.
        (TransferLedger::new(&config, 0), config)
    }

    /// Run `fut` under a deadline so a lost wakeup fails instead of hang.
    async fn within<T>(limit: std::time::Duration, fut: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(limit, fut)
            .await
            .expect("test future must not hang")
    }

    fn ms(n: u64) -> std::time::Duration {
        std::time::Duration::from_millis(n)
    }

    #[tokio::test]
    async fn reserve_release_round_trip_tracks_outstanding_and_peak() {
        let (ledger, config) = small_ledger();
        let job = ledger.job(&config);
        let reservation = within(ms(1000), job.reserve(Component::Frames, 512))
            .await
            .expect("reserve");
        assert_eq!(reservation.component(), Component::Frames);
        assert_eq!(reservation.bytes(), 512);
        assert_eq!(job.component_outstanding(Component::Frames), 512);
        assert_eq!(job.job_outstanding(), 512);
        assert_eq!(ledger.aggregate_outstanding(), 512);
        assert_eq!(ledger.component_outstanding(Component::Frames), 512);
        assert_eq!(job.component_high_water(Component::Frames), 512);
        assert_eq!(job.job_high_water(), 512);
        assert_eq!(ledger.aggregate_high_water(), 512);
        assert_eq!(ledger.component_high_water(Component::Frames), 512);
        drop(reservation);
        assert_eq!(job.component_outstanding(Component::Frames), 0);
        assert_eq!(job.job_outstanding(), 0);
        assert_eq!(ledger.aggregate_outstanding(), 0);
        assert_eq!(ledger.component_outstanding(Component::Frames), 0);
        // Peaks stay at the observed maximum after release.
        assert_eq!(job.component_high_water(Component::Frames), 512);
        assert_eq!(ledger.aggregate_high_water(), 512);
    }

    #[tokio::test]
    async fn try_reserve_reports_no_capacity_without_waiting() {
        let (ledger, config) = small_ledger();
        let job = ledger.job(&config);
        let first = job
            .try_reserve(Component::Writer, 1024)
            .expect("first fits exactly");
        assert_eq!(
            job.try_reserve(Component::Writer, 1).expect_err("full"),
            LedgerRefusal::NoCapacity(Component::Writer)
        );
        drop(first);
        assert!(
            job.try_reserve(Component::Writer, 1024).is_ok(),
            "released capacity must be reservable again"
        );
    }

    #[tokio::test]
    async fn oversize_atomic_allocations_are_refused_immediately() {
        let (ledger, config) = small_ledger();
        let job = ledger.job(&config);
        // Above the component cap: the component maximum binds.
        assert_eq!(
            job.try_reserve(Component::Frames, 2048)
                .expect_err("refuse"),
            LedgerRefusal::Oversize(OversizeRefusal {
                component: Component::Frames,
                requested: 2048,
                cap: 1024,
            })
        );
        // Between the component cap and the job cap: the component cap
        // still binds (it is the smaller cap).
        match job.try_reserve(Component::Frames, 1500) {
            Err(LedgerRefusal::Oversize(refusal)) => {
                assert_eq!(refusal.cap, 1024);
                assert_eq!(refusal.requested, 1500);
            }
            other => panic!("expected oversize refusal, got {other:?}"),
        }
        // The async reserve refuses oversize immediately (never waits).
        let refused = within(ms(1000), job.reserve(Component::Frames, 2048)).await;
        assert!(matches!(
            refused,
            Err(LedgerRefusal::Oversize(OversizeRefusal { .. }))
        ));
        // Nothing was charged by any refused attempt.
        assert_eq!(ledger.aggregate_outstanding(), 0);
        assert_eq!(job.job_outstanding(), 0);
    }

    #[tokio::test]
    async fn admission_is_fair_fifo_and_never_bypasses_the_head() {
        let (ledger, config) = small_ledger();
        // Component pools are per-job, so holds and waiters must share one
        // job ledger to queue on the same frames pool.
        let job = Arc::new(ledger.job(&config));
        let hold_a = job
            .try_reserve(Component::Frames, 512)
            .expect("half the component");
        let hold_b = job
            .try_reserve(Component::Frames, 512)
            .expect("fill the component");
        // Two waiters queue on that frames pool: first wants 600 (more
        // than will be free), second wants 100.
        let (first_tx, first_rx) = tokio::sync::oneshot::channel::<TransferReservation>();
        #[allow(unused_mut)]
        let mut first_rx = first_rx;
        let (second_tx, second_rx) = tokio::sync::oneshot::channel::<TransferReservation>();
        #[allow(unused_mut)]
        let mut second_rx = second_rx;
        let job_first = Arc::clone(&job);
        let job_second = Arc::clone(&job);
        let waiter_first = tokio::spawn(async move {
            let reservation = job_first.reserve(Component::Frames, 600).await;
            let _ = first_tx.send(reservation.expect("head acquires"));
        });
        let waiter_second = tokio::spawn(async move {
            let reservation = job_second.reserve(Component::Frames, 100).await;
            let _ = second_tx.send(reservation.expect("queued waiter acquires"));
        });
        tokio::time::sleep(ms(30)).await;
        // Free 512: the head wants 600, so it must keep waiting, and the
        // 100-byte waiter behind it must not bypass the head.
        drop(hold_b);
        tokio::time::sleep(ms(30)).await;
        assert!(
            !first_rx.try_recv().is_ok(),
            "head waiter (600) must still wait with 512 free"
        );
        assert!(
            !second_rx.try_recv().is_ok(),
            "later waiter (100) must not bypass the head"
        );
        // Free the rest: the head acquires first, then the queued waiter.
        drop(hold_a);
        let first = within(ms(1000), first_rx).await.expect("head reservation");
        let second = within(ms(1000), second_rx)
            .await
            .expect("queued reservation");
        waiter_first.await.expect("first task");
        waiter_second.await.expect("second task");
        // Both reservations are still held here: 600 + 100 outstanding.
        assert_eq!(ledger.aggregate_outstanding(), 700);
        assert_eq!(first.bytes(), 600);
        assert_eq!(second.bytes(), 100);
    }

    #[tokio::test]
    async fn cancelling_a_queued_waiter_leaks_nothing() {
        let (ledger, config) = small_ledger();
        let job = ledger.job(&config);
        let hold = job
            .try_reserve(Component::Frames, 1024)
            .expect("fill the job's frames pool");
        // The reservation waits while the pool is full; the timeout drops
        // the future mid-wait, which must cancel the admission cleanly.
        let timed_out = tokio::time::timeout(ms(30), job.reserve(Component::Frames, 512)).await;
        assert!(
            timed_out.is_err(),
            "the reservation must wait while capacity is held"
        );
        assert_eq!(ledger.aggregate_outstanding(), 1024);
        drop(hold);
        let reservation = within(ms(1000), job.reserve(Component::Frames, 1024))
            .await
            .expect("capacity fully available after cancel");
        assert_eq!(reservation.bytes(), 1024);
    }

    #[tokio::test]
    async fn cancelling_mid_chain_rolls_back_partially_acquired_pools() {
        // The checkpoint component is empty but the job pool is full:
        // the waiter acquires checkpoint bytes, then waits for the job
        // pool while holding them — a cancelled wait must give those
        // checkpoint bytes back.
        let (ledger, config) = small_ledger();
        let job = ledger.job(&config);
        let hold_a = job
            .try_reserve(Component::Writer, 1024)
            .expect("half the job pool");
        let hold_b = job
            .try_reserve(Component::Frames, 1024)
            .expect("fill the job pool");
        let timed_out =
            tokio::time::timeout(ms(30), job.reserve(Component::Checkpoint, 1024)).await;
        assert!(timed_out.is_err(), "the job pool is exhausted");
        tokio::time::sleep(ms(10)).await;
        // Nothing is held anywhere for the cancelled attempt.
        assert_eq!(ledger.component_outstanding(Component::Checkpoint), 0);
        assert_eq!(job.job_outstanding(), 2 * 1024);
        assert_eq!(ledger.aggregate_outstanding(), 2 * 1024);
        drop(hold_a);
        drop(hold_b);
    }

    #[tokio::test]
    async fn retag_moves_the_component_charge_without_double_counting() {
        let (ledger, config) = small_ledger();
        let job = ledger.job(&config);
        let mut reservation = job
            .try_reserve(Component::Frames, 700)
            .expect("reserve frames");
        assert_eq!(ledger.component_outstanding(Component::Frames), 700);
        assert_eq!(ledger.component_outstanding(Component::Writer), 0);
        assert_eq!(ledger.aggregate_outstanding(), 700);
        assert_eq!(job.job_outstanding(), 700);
        within(ms(1000), reservation.retag(Component::Writer))
            .await
            .expect("retag");
        assert_eq!(reservation.component(), Component::Writer);
        // Single charge: job and aggregate totals unchanged.
        assert_eq!(job.job_outstanding(), 700);
        assert_eq!(ledger.aggregate_outstanding(), 700);
        // Component attribution moved exactly.
        assert_eq!(ledger.component_outstanding(Component::Frames), 0);
        assert_eq!(ledger.component_outstanding(Component::Writer), 700);
        assert_eq!(job.component_outstanding(Component::Frames), 0);
        assert_eq!(job.component_outstanding(Component::Writer), 700);
        // Drop releases the retagged charge exactly once.
        drop(reservation);
        assert_eq!(ledger.aggregate_outstanding(), 0);
        assert_eq!(ledger.component_outstanding(Component::Writer), 0);
        assert_eq!(job.job_outstanding(), 0);
        // Peaks: frames peaked at 700, writer peaked at 700.
        assert_eq!(ledger.component_high_water(Component::Frames), 700);
        assert_eq!(ledger.component_high_water(Component::Writer), 700);
    }

    #[tokio::test]
    async fn oversize_retag_is_refused_and_leaves_the_tag_unchanged() {
        let config = TransferMemoryConfig {
            aggregate_max_bytes: 4096,
            job_max_bytes: 2048,
            network_ingress_max_bytes: 1024,
            frames_max_bytes: 1024,
            writer_max_bytes: 256,
            checkpoint_max_bytes: 1024,
        };
        let ledger = TransferLedger::new(&config, 0);
        let job = ledger.job(&config);
        let mut reservation = job
            .try_reserve(Component::Frames, 700)
            .expect("frames holds 700");
        let refused = within(ms(1000), reservation.retag(Component::Writer)).await;
        match refused {
            Err(LedgerRefusal::Oversize(oversize)) => {
                assert_eq!(oversize.component, Component::Writer);
                assert_eq!(oversize.requested, 700);
                assert_eq!(oversize.cap, 256);
            }
            other => panic!("expected oversize refusal, got {other:?}"),
        }
        assert_eq!(reservation.component(), Component::Frames);
        assert_eq!(ledger.component_outstanding(Component::Frames), 700);
        assert_eq!(ledger.component_outstanding(Component::Writer), 0);
        drop(reservation);
        assert_eq!(ledger.aggregate_outstanding(), 0);
    }

    #[tokio::test]
    async fn cancelled_retag_keeps_the_original_tag() {
        let (ledger, config) = small_ledger();
        let job = ledger.job(&config);
        let hold = job
            .try_reserve(Component::Writer, 1024)
            .expect("fill the job's writer pool");
        let mut reservation = job
            .try_reserve(Component::Frames, 512)
            .expect("reserve frames");
        // Writer capacity is exhausted, so the retag waits holding the
        // frames charge; the timeout drops it, which must cancel cleanly.
        let timed_out = tokio::time::timeout(ms(30), reservation.retag(Component::Writer)).await;
        assert!(timed_out.is_err(), "retag must wait for writer capacity");
        assert_eq!(reservation.component(), Component::Frames);
        assert_eq!(ledger.component_outstanding(Component::Frames), 512);
        assert_eq!(ledger.component_outstanding(Component::Writer), 1024);
        assert_eq!(ledger.aggregate_outstanding(), 512 + 1024);
        drop(hold);
        // After capacity frees, the retag succeeds.
        within(ms(1000), reservation.retag(Component::Writer))
            .await
            .expect("retag after capacity frees");
        assert_eq!(reservation.component(), Component::Writer);
        drop(reservation);
        assert_eq!(ledger.aggregate_outstanding(), 0);
    }

    #[tokio::test]
    async fn aggregate_pool_bounds_concurrent_jobs() {
        let (ledger, config) = small_ledger();
        // Two jobs each take their full job cap: the aggregate (4 KiB)
        // admits exactly 2 × 2 KiB.
        let a = ledger.job(&config);
        let b = ledger.job(&config);
        let c = ledger.job(&config);
        let hold_a1 = a.try_reserve(Component::Frames, 1024).expect("a1");
        let hold_a2 = a.try_reserve(Component::Writer, 1024).expect("a2");
        let hold_b1 = b.try_reserve(Component::Frames, 1024).expect("b1");
        let hold_b2 = b.try_reserve(Component::Checkpoint, 1024).expect("b2");
        assert_eq!(ledger.aggregate_outstanding(), 4 * 1024);
        // The third job cannot take anything: the aggregate is full.
        let timed_out = tokio::time::timeout(ms(30), c.reserve(Component::Frames, 1)).await;
        assert!(timed_out.is_err(), "aggregate admission must wait");
        // Peak equals the cap; release frees it.
        assert_eq!(ledger.aggregate_high_water(), 4 * 1024);
        drop(hold_a1);
        drop(hold_a2);
        drop(hold_b1);
        drop(hold_b2);
        assert_eq!(ledger.aggregate_outstanding(), 0);
    }

    #[tokio::test]
    async fn concurrent_reservations_stay_within_caps_and_settle_to_zero() {
        let (ledger, config) = small_ledger();
        let ledger = Arc::new(ledger);
        let config = Arc::new(config);
        let mut handles = Vec::new();
        for task in 0..16u32 {
            let ledger = Arc::clone(&ledger);
            let config = Arc::clone(&config);
            handles.push(tokio::spawn(async move {
                let job = ledger.job(&config);
                for step in 0..50u32 {
                    let component = Component::const_all()[((task + step) % 4) as usize];
                    let bytes = 1 + u64::from((task * 7 + step * 13) % 256);
                    let mut reservation = match job.try_reserve(component, bytes) {
                        Ok(reservation) => reservation,
                        Err(LedgerRefusal::NoCapacity(_)) => {
                            match within(ms(2000), job.reserve(component, bytes)).await {
                                Ok(reservation) => reservation,
                                Err(refusal) => panic!("unexpected refusal {refusal:?}"),
                            }
                        }
                        Err(refusal) => panic!("unexpected refusal {refusal:?}"),
                    };
                    // Occasional retag moves the charge exactly once.
                    if step % 3 == 0 {
                        let target = Component::const_all()[((task + step + 1) % 4) as usize];
                        if target != component {
                            let _ = within(ms(2000), reservation.retag(target)).await;
                        }
                    }
                    drop(reservation);
                }
            }));
        }
        for handle in handles {
            handle.await.expect("task");
        }
        assert_eq!(ledger.aggregate_outstanding(), 0, "all released");
        for component in Component::const_all() {
            assert_eq!(ledger.component_outstanding(component), 0);
            assert!(
                ledger.component_high_water(component) <= config.job_max_bytes,
                "per-component aggregate peak stays within the job cap"
            );
        }
        assert!(ledger.aggregate_high_water() <= config.aggregate_max_bytes);
    }
}
