/**
 * Test harness for LiveSync: a fake stream and a controllable collection
 * fetch, exposing the exact handshake order the design requires.
 */
import { LiveSync, type Collections, type EventEnvelope, type JobView } from './liveSync'

export interface HarnessOptions {
  /** Start fully live with empty caches. */
  connected?: boolean
}

export function createLiveSyncHarness(options: HarnessOptions = {}) {
  let onEvent: ((envelope: EventEnvelope) => void) | null = null
  let onDisconnected: (() => void) | null = null
  let resolveCollections: ((collections: Collections) => void) | null = null
  const applied = new Map<string, JobView>()
  let replaced: Collections = { active: [], history: [] }

  const live = new LiveSync({
    openStream(handlers) {
      onEvent = handlers.onEvent
      onDisconnected = handlers.onDisconnected
      return { close() {} }
    },
    fetchCollections() {
      return new Promise<Collections>((resolve) => {
        resolveCollections = resolve
      })
    },
    replaceCaches(collections) {
      replaced = collections
      applied.clear()
      for (const view of [...collections.active, ...collections.history]) {
        applied.set(view.id, view)
      }
    },
    applySnapshot(view) {
      applied.set(view.id, view)
    },
    markStale() {
      applied.clear()
    },
  })

  live.start()
  if (options.connected) {
    live.forceLive({ active: [], history: [] }, 'epoch-a')
  }

  return {
    /** Delivers the hello and starts the authoritative handshake. */
    open({ streamEpoch }: { streamEpoch: string }) {
      onEvent?.({ kind: 'hello', streamEpoch })
    },
    receive(envelope: EventEnvelope) {
      onEvent?.(envelope)
    },
    /** Resolves the pending collection fetch and lets the drain run. */
    async resolveCollection(views: JobView[]) {
      const resolve = resolveCollections
      resolve?.({ active: views, history: [] })
      await Promise.resolve()
    },
    disconnect() {
      onDisconnected?.()
    },
    job(id: string): JobView | undefined {
      return applied.get(id)
    },
    replacedCaches(): Collections {
      return replaced
    },
    phase() {
      return live.phase()
    },
    canMutate() {
      return live.canMutate()
    },
    epoch() {
      return live.epochOf()
    },
  }
}
