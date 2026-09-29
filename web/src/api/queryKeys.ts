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
  },
  roots: {
    all: ['roots'] as const,
  },
  settings: {
    all: ['settings'] as const,
  },
}
