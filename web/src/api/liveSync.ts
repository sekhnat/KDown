/**
 * Live reconciliation over the SSE stream.
 *
 * The exact order the design requires: open the stream, receive `hello`,
 * buffer subsequent events, fetch the authoritative collections, replace
 * the query caches, apply only buffered views newer by
 * `(stream_epoch, attempt_id, sample_seq)`, then enter live mode. A
 * disconnect marks the caches stale and disables mutations until a fresh
 * handshake completes.
 */

export type JobStatus =
  | 'Queued'
  | 'Recovering'
  | 'Active'
  | 'Paused'
  | 'Completed'
  | 'Failed'
  | 'Cancelled'

export type DesiredState = 'Running' | 'Paused' | 'Cancelled'

export interface EngineSnapshotView {
  stateLabel: string
  bytesReceived: number
  networkBytes: number
  reusedBytes: number
  retries: number
  elapsedMs: number
}

/** Display-safe job view as the frontend consumes it. */
export interface JobView {
  id: string
  status: JobStatus
  desiredState: DesiredState
  controlVersion: number
  attemptId: string | null
  /** Per-attempt monotonic sequence; a new attempt restarts at 0. */
  sampleSeq: number
  sourceDisplay: string
  rootId: string
  rootLabel: string
  relativeDirectory: string | null
  filenameOverride: string | null
  destinationDisplay: string | null
  createdAt: number
  updatedAt: number
  snapshot: EngineSnapshotView | null
}

export interface EventEnvelope {
  kind: 'hello' | 'job.snapshot' | 'job.removed' | 'settings.changed' | 'service.degraded'
  streamEpoch: string
  build?: string
  job?: JobView
}

export interface Collections {
  active: JobView[]
  history: JobView[]
}

export interface StreamHandle {
  close(): void
}

export interface LiveSyncDeps {
  openStream(handlers: {
    onEvent(envelope: EventEnvelope): void
    onDisconnected(): void
  }): StreamHandle
  fetchCollections(): Promise<Collections>
  replaceCaches(collections: Collections): void
  applySnapshot(view: JobView): void
  markStale(): void
}

export type LivePhase = 'connecting' | 'syncing' | 'live' | 'stale'

interface SeenRevision {
  epoch: string
  attemptId: string | null
  sampleSeq: number
}

export class LiveSync {
  private currentPhase: LivePhase = 'connecting'
  private epoch = ''
  private buffer: EventEnvelope[] = []
  private seen = new Map<string, SeenRevision>()
  private handle: StreamHandle | null = null

  constructor(private readonly deps: LiveSyncDeps) {}

  start(): void {
    this.handle = this.deps.openStream({
      onEvent: (envelope) => this.onEvent(envelope),
      onDisconnected: () => this.onDisconnected(),
    })
  }

  close(): void {
    this.handle?.close()
    this.handle = null
  }

  phase(): LivePhase {
    return this.currentPhase
  }

  canMutate(): boolean {
    return this.currentPhase === 'live'
  }

  epochOf(): string {
    return this.epoch
  }

  /** Test and reconnect seam: jump straight to live with fresh caches. */
  forceLive(collections: Collections, epoch = this.epoch): void {
    this.epoch = epoch
    this.seen.clear()
    this.buffer = []
    this.deps.replaceCaches(collections)
    this.currentPhase = 'live'
  }

  private onEvent(envelope: EventEnvelope): void {
    if (envelope.kind === 'hello') {
      this.epoch = envelope.streamEpoch
      this.currentPhase = 'syncing'
      void this.deps
        .fetchCollections()
        .then((collections) => {
          this.deps.replaceCaches(collections)
          this.seen.clear()
          for (const view of [...collections.active, ...collections.history]) {
            this.seen.set(view.id, {
              epoch: this.epoch,
              attemptId: view.attemptId,
              sampleSeq: view.sampleSeq,
            })
          }
          const buffered = this.buffer
          this.buffer = []
          for (const pending of buffered) {
            if (pending.kind === 'job.snapshot' && pending.job) {
              this.applySnapshot(pending)
            }
          }
          this.currentPhase = 'live'
        })
        .catch(() => {
          // Collection fetch failed: stay stale until a reconnect.
          this.onDisconnected()
        })
      return
    }
    if (envelope.kind === 'job.snapshot') {
      if (envelope.streamEpoch && this.epoch && envelope.streamEpoch !== this.epoch) {
        // A new epoch supersedes everything held for the previous one.
        this.epoch = envelope.streamEpoch
        this.seen.clear()
      }
      if (this.currentPhase === 'syncing') {
        this.buffer.push(envelope)
        return
      }
      if (this.currentPhase === 'live' && envelope.job) {
        this.applySnapshot(envelope)
      }
      return
    }
    if (envelope.kind === 'service.degraded') {
      this.deps.markStale()
      this.currentPhase = 'stale'
      void this.deps.fetchCollections().then((collections) => {
        this.deps.replaceCaches(collections)
        this.currentPhase = 'live'
      })
    }
  }

  private applySnapshot(envelope: EventEnvelope): void {
    const view = envelope.job
    if (!view) {
      return
    }
    const current = this.seen.get(view.id)
    if (current) {
      if (envelope.streamEpoch !== current.epoch) {
        // Cross-epoch application: adopt the newer epoch wholesale.
        if (envelope.streamEpoch < current.epoch) {
          return
        }
      } else if (view.attemptId !== current.attemptId) {
        // A different attempt arrives later on the same stream: newer.
      } else if (view.sampleSeq <= current.sampleSeq) {
        return
      }
    }
    this.seen.set(view.id, {
      epoch: envelope.streamEpoch || this.epoch,
      attemptId: view.attemptId,
      sampleSeq: view.sampleSeq,
    })
    this.deps.applySnapshot(view)
  }

  private onDisconnected(): void {
    this.currentPhase = 'stale'
    this.buffer = []
    this.deps.markStale()
  }
}
