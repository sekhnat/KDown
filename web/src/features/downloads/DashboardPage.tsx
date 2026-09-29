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
      <div style={{ display: 'flex', alignItems: 'baseline', gap: '1rem', flexWrap: 'wrap' }}>
        <h1 id="downloads-heading" style={{ marginRight: 'auto' }}>
          Downloads
        </h1>
        <NewDownloadDrawer />
      </div>

      <p className="status-sentence telemetry">
        <span>{activeCount} active</span> and <span>{queuedCount} queued</span>
        {aggregateRate > 0 ? (
          <>
            , moving <span className="figure">{(aggregateRate / 1024).toFixed(1)} KiB/s</span>
          </>
        ) : null}
      </p>

      {nonTerminal.length === 0 ? (
        <div className="empty">
          <p>No active downloads. Start one with New download.</p>
        </div>
      ) : (
        <div className="job-list">
          {nonTerminal.map((job) => (
            <JobCard key={job.id} job={job} livePhase={livePhase} onCancelRequested={() => undefined} />
          ))}
        </div>
      )}

      {recentTerminal.length > 0 ? (
        <>
          <h2>Recent history</h2>
          <ul className="recent">
            {recentTerminal.map((job) => (
              <li key={job.id}>
                <span className="name">{job.destinationDisplay ?? job.sourceDisplay}</span>
                <span>{job.status}</span>
              </li>
            ))}
          </ul>
        </>
      ) : null}

      <span aria-live="polite" className="visually-hidden-live" />
    </section>
  )
}
