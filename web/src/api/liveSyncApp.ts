/**
 * Bridges LiveSync into TanStack Query caches: authoritative collection
 * replacement after the hello handshake, newer-by-revision snapshot
 * application, and live-phase propagation to the shell.
 */
import type { QueryClient } from '@tanstack/react-query'
import { LiveSync, type Collections, type EventEnvelope } from './liveSync'
import { api, fromDto, type JobView } from './apiClient'
import { queryKeys } from './queryKeys'

function mergeIntoDetail(queryClient: QueryClient, view: JobView) {
  const existing = queryClient.getQueryData<{ job: JobView; attempts: unknown[] }>(
    queryKeys.jobs.detail(view.id),
  )
  if (existing) {
    queryClient.setQueryData(queryKeys.jobs.detail(view.id), {
      job: view,
      attempts: existing.attempts,
    })
  }
}

function mergeIntoListCaches(queryClient: QueryClient, view: JobView) {
  const update = (page: { jobs: JobView[]; nextCursor: string | null } | undefined) => {
    if (!page) {
      return page
    }
    const jobs = page.jobs.some((job) => job.id === view.id)
      ? page.jobs.map((job) => (job.id === view.id ? view : job))
      : [...page.jobs, view]
    return { jobs, nextCursor: page.nextCursor }
  }
  queryClient.setQueryData(queryKeys.jobs.list({}), update)
}

function isTerminal(job: JobView): boolean {
  return job.status === 'Completed' || job.status === 'Failed' || job.status === 'Cancelled'
}

/** Starts the app-wide LiveSync against `/api/v1/events`. */
export function startAppLiveSync(
  queryClient: QueryClient,
  onPhaseChange: (phase: 'connecting' | 'syncing' | 'live' | 'stale') => void,
): LiveSync {
  const live = new LiveSync({
    openStream(handlers) {
      const source = new EventSource('/api/v1/events')
      source.addEventListener('hello', (event) =>
        handlers.onEvent(JSON.parse((event as MessageEvent).data) as EventEnvelope),
      )
      source.addEventListener('job.snapshot', (event) => {
        const envelope = JSON.parse((event as MessageEvent).data) as {
          kind: string
          stream_epoch: string
          job?: Record<string, unknown>
        }
        // The wire DTO is snake_case; convert to the app view before the
        // sync's newer-by-revision logic sees it.
        handlers.onEvent({
          kind: 'job.snapshot',
          streamEpoch: envelope.stream_epoch,
          job: envelope.job ? fromDto(envelope.job as never) : undefined,
        })
      })
      source.addEventListener('job.removed', () => {
        void queryClient.invalidateQueries({ queryKey: queryKeys.jobs.lists() })
      })
      source.addEventListener('settings.changed', () => {
        void queryClient.invalidateQueries({ queryKey: queryKeys.settings.all })
        void queryClient.invalidateQueries({ queryKey: queryKeys.roots.all })
      })
      source.addEventListener('service.degraded', () =>
        handlers.onEvent({ kind: 'service.degraded', streamEpoch: '' }),
      )
      source.onerror = () => handlers.onDisconnected()
      return {
        close() {
          source.close()
        },
      }
    },
    async fetchCollections() {
      const page = await api.listJobs({ limit: 200 })
      const active = page.jobs.filter((job) => !isTerminal(job))
      const history = page.jobs.filter(isTerminal)
      return { active, history } satisfies Collections
    },
    replaceCaches(collections) {
      queryClient.setQueryData(queryKeys.jobs.list({}), {
        jobs: [...collections.active, ...collections.history],
        nextCursor: null,
      })
    },
    applySnapshot(view) {
      mergeIntoDetail(queryClient, view)
      mergeIntoListCaches(queryClient, view)
    },
    markStale() {
      // Caches stay visible; the shell flips to its stale presentation.
    },
    onPhaseChange,
  })

  live.start()
  return live
}
