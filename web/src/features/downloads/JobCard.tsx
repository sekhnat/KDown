import { Link } from 'react-router-dom'
import type { JobView } from '../../api/liveSync'
import { displayStatus, statusGlyph } from './jobState'
import { JobControls } from './JobControls'

function formatBytes(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined) return '—'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`
}

function formatRate(bytesPerSecond: number | null | undefined): string {
  if (bytesPerSecond === null || bytesPerSecond === undefined || bytesPerSecond <= 0) return '—'
  return `${formatBytes(bytesPerSecond)}/s`
}

export interface JobCardProps {
  job: JobView
  livePhase: 'live' | 'stale' | 'connecting' | 'syncing'
  onCancelRequested(job: JobView): void
}

/** One dashboard card: identity, state, progress, and legal controls. */
export function JobCard({ job, livePhase, onCancelRequested }: JobCardProps) {
  const status = displayStatus(job)
  const received = job.snapshot?.bytesReceived ?? null
  // The engine snapshot does not yet expose total size in v1 views; the
  // progress bar is indeterminate until it does.
  const rate =
    job.snapshot && job.snapshot.elapsedMs > 0
      ? job.snapshot.bytesReceived / (job.snapshot.elapsedMs / 1000)
      : null
  return (
    <article
      aria-label={`Job ${job.sourceDisplay}`}
      style={{
        background: 'var(--surface)',
        border: '1px solid var(--border)',
        borderRadius: 'var(--radius-medium)',
        padding: '1rem',
        display: 'grid',
        gap: '0.5rem',
      }}
    >
      <header style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
        <span aria-hidden="true">{statusGlyph(status)}</span>
        <strong style={{ marginRight: 'auto' }}>
          <Link to={`/downloads/${job.id}`}>{job.destinationDisplay ?? job.sourceDisplay}</Link>
        </strong>
        <span data-status={status}>
          {statusGlyph(status)} {status}
        </span>
      </header>
      <div
        role="progressbar"
        aria-valuetext="indeterminate"
        style={{ height: '0.5rem', background: 'var(--border)', borderRadius: 999 }}
      >
        <div
          style={{
            width: received === null ? '30%' : `${Math.min(100, (received / Math.max(received, 1)) * 100)}%`,
            background: 'var(--accent)',
            height: '100%',
            borderRadius: 999,
          }}
        />
      </div>
      <dl
        className="telemetry"
        style={{ display: 'flex', gap: '1rem', margin: 0, fontSize: '0.85rem' }}
      >
        <div>
          <dt style={{ display: 'inline' }}>Received: </dt>
          <dd style={{ display: 'inline', margin: 0 }}>{formatBytes(received)}</dd>
        </div>
        <div>
          <dt style={{ display: 'inline' }}>Rate: </dt>
          <dd style={{ display: 'inline', margin: 0 }}>{formatRate(rate)}</dd>
        </div>
        <div>
          <dt style={{ display: 'inline' }}>ETA: </dt>
          <dd style={{ display: 'inline', margin: 0 }}>—</dd>
        </div>
      </dl>
      <JobControls
        job={job}
        livePhase={livePhase}
        onAction={() => undefined}
        onCancelRequested={() => onCancelRequested(job)}
      />
    </article>
  )
}
