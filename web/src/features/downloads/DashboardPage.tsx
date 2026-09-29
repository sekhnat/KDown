import { useMemo } from 'react'
import type { JobView } from '../../api/liveSync'
import { JobCard } from './JobCard'
import { NewDownloadDrawer } from './NewDownloadDrawer'
import { displayStatus } from './jobState'

export interface DashboardPageProps {
  jobs: JobView[]
  livePhase: 'live' | 'stale' | 'connecting' | 'syncing'
}

function isTerminal(job: JobView): boolean {
  return ['Completed', 'Failed', 'Cancelled'].includes(job.status)
}

/**
 * The dashboard: aggregate effective rate, active/queued counts, cards
 * ordered by activity and creation, and recent terminal jobs.
 */
export function DashboardPage({ jobs, livePhase }: DashboardPageProps) {
  const nonTerminal = useMemo(
    () =>
      jobs
        .filter((job) => !isTerminal(job))
        .sort((a, b) => b.createdAt - a.createdAt),
    [jobs],
  )
  const recentTerminal = useMemo(
    () => jobs.filter(isTerminal).sort((a, b) => b.updatedAt - a.updatedAt).slice(0, 5),
    [jobs],
  )
  const activeCount = nonTerminal.filter((job) => displayStatus(job) !== 'Queued').length
  const queuedCount = nonTerminal.length - activeCount
  const aggregateRate = useMemo(
    () =>
      nonTerminal.reduce((sum, job) => {
        if (!job.snapshot || job.snapshot.elapsedMs <= 0) return sum
        return sum + job.snapshot.bytesReceived / (job.snapshot.elapsedMs / 1000)
      }, 0),
    [nonTerminal],
  )

  return (
    <section aria-labelledby="downloads-heading">
      <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
        <h1 id="downloads-heading" style={{ marginRight: 'auto' }}>
          Downloads
        </h1>
        <NewDownloadDrawer />
      </div>

      <dl
        className="telemetry"
        style={{ display: 'flex', gap: '2rem', margin: '1rem 0', flexWrap: 'wrap' }}
      >
        <div>
          <dt>Effective rate</dt>
          <dd>{aggregateRate > 0 ? `${(aggregateRate / 1024).toFixed(1)} KiB/s` : '—'}</dd>
        </div>
        <div>
          <dt>Active</dt>
          <dd>{activeCount} active</dd>
        </div>
        <div>
          <dt>Queued</dt>
          <dd>{queuedCount} queued</dd>
        </div>
      </dl>

      {nonTerminal.length === 0 ? (
        <p>No active downloads. Start one with New download.</p>
      ) : (
        <div style={{ display: 'grid', gap: '0.75rem' }}>
          {nonTerminal.map((job) => (
            <JobCard key={job.id} job={job} livePhase={livePhase} onCancelRequested={() => undefined} />
          ))}
        </div>
      )}

      {recentTerminal.length > 0 ? (
        <>
          <h2>Recent history</h2>
          <ul style={{ listStyle: 'none', padding: 0, display: 'grid', gap: '0.5rem' }}>
            {recentTerminal.map((job) => (
              <li key={job.id}>
                {job.destinationDisplay ?? job.sourceDisplay} — {job.status}
              </li>
            ))}
          </ul>
        </>
      ) : null}

      <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>
        Completed bytes today are shown per job in history.
      </p>
      <span aria-live="polite" className="visually-hidden-live" />
    </section>
  )
}
