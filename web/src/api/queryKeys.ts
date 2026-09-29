/** Stable TanStack Query key factories. */
export const queryKeys = {
  bootstrap: ['bootstrap'] as const,
  jobs: {
    all: ['jobs'] as const,
    lists: () => [...queryKeys.jobs.all, 'list'] as const,
    list: (
      params: { status?: string; source?: string; from?: string; to?: string; cursor?: string } = {},
    ) => [...queryKeys.jobs.lists(), params] as const,
    details: () => [...queryKeys.jobs.all, 'detail'] as const,
    detail: (id: string) => [...queryKeys.jobs.details(), id] as const,
    // The history collection is an infinite query: its cache holds a
    // {pages, pageParams} shape and must never share a key with the plain
    // list cache (undefined params hash to the same key otherwise).
    history: (
      params: { status?: string; source?: string } = {},
    ) => [...queryKeys.jobs.all, 'history', params] as const,
  },
  roots: {
    all: ['roots'] as const,
  },
  settings: {
    all: ['settings'] as const,
  },
}
