import { useInfiniteQuery, useQuery } from '@tanstack/react-query'
import { Link, useSearchParams } from 'react-router-dom'
import { useState } from 'react'
import { api } from '../../api/apiClient'
import { queryKeys } from '../../api/queryKeys'
import { HistoryFilters } from './HistoryFilters'
import { VirtualJobList } from '../../components/VirtualJobList'

interface HistoryRowData {
  id: string
  status: string
  display: string
}

/**
 * Durable history: paginated, filterable, windowed. Filters live in the
 * URL so a refresh restores the same view; Load more appends cursor pages.
 */
export function HistoryPage() {
  const [searchParams] = useSearchParams()
  const status = searchParams.get('status') ?? undefined
  const source = searchParams.get('source') ?? undefined
  const from = searchParams.get('from') ?? undefined
  const to = searchParams.get('to') ?? undefined
  const [expandedId, setExpandedId] = useState<string | null>(null)

  const query = useInfiniteQuery({
    queryKey: queryKeys.jobs.list({ status, source }),
    queryFn: ({ pageParam }) =>
      api.listJobs({
        status: status || undefined,
        source: source || undefined,
        from: from ? Number(from) : undefined,
        to: to ? Number(to) : undefined,
        cursor: pageParam || undefined,
        limit: 100,
      }),
    initialPageParam: '',
    getNextPageParam: (lastPage) => lastPage.nextCursor ?? undefined,
  })

  const rows: HistoryRowData[] =
    query.data?.pages.flatMap((page) =>
      page.jobs.map((job) => ({
        id: job.id,
        status: job.status,
        display: job.destinationDisplay ?? job.sourceDisplay,
      })),
    ) ?? []

  return (
    <section aria-labelledby="history-heading">
      <h1 id="history-heading">History</h1>
      <HistoryFilters />
      {query.isPending ? (
        <p role="status">Loading history…</p>
      ) : query.isError ? (
        <p role="alert">History is unavailable. It will retry automatically.</p>
      ) : rows.length === 0 ? (
        <p>No history matches these filters.</p>
      ) : (
        <>
          <VirtualJobList
            count={rows.length}
            estimateSize={72}
            renderItem={(index) => (
              <HistoryRow
                jobId={rows[index].id}
                status={rows[index].status}
                display={rows[index].display}
                expanded={expandedId === rows[index].id}
                onToggle={() =>
                  setExpandedId((current) => (current === rows[index].id ? null : rows[index].id))
                }
              />
            )}
          />
          {query.hasNextPage ? (
            <button type="button" onClick={() => void query.fetchNextPage()}>
              Load more
            </button>
          ) : null}
        </>
      )}
    </section>
  )
}

function HistoryRow({
  jobId,
  status,
  display,
  expanded,
  onToggle,
}: {
  jobId: string
  status: string
  display: string
  expanded: boolean
  onToggle(): void
}) {
  const detail = useJobDetail(jobId, expanded)
  const failedOutcome = detail?.attempts.find((attempt) => attempt.outcome?.kind === 'failed')
    ?.outcome

  return (
    <div
      style={{
        display: 'flex',
        gap: '0.75rem',
        alignItems: 'center',
        flexWrap: 'wrap',
        borderBottom: '1px solid var(--border)',
        padding: '0.5rem 0',
      }}
    >
      <Link to={`/downloads/${jobId}`} style={{ marginRight: 'auto' }}>
        {display}
      </Link>
      <span data-status={status}>{status}</span>
      {status === 'Failed' ? (
        <button type="button" aria-expanded={expanded} onClick={onToggle}>
          Failure detail
        </button>
      ) : null}
      {expanded && failedOutcome ? (
        <span style={{ color: 'var(--text-muted)' }}>
          {failedOutcome.code}
          {failedOutcome.detail ? `: ${failedOutcome.detail}` : ''}
        </span>
      ) : null}
    </div>
  )
}

function useJobDetail(jobId: string, enabled: boolean) {
  const query = useQuery({
    queryKey: queryKeys.jobs.detail(jobId),
    queryFn: () => api.getJob(jobId),
    enabled,
  })
  return query.data
}
