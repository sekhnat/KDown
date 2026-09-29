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

/**
 * One dashboard row: identity, state, a byte tape, and legal controls.
 * The engine does not report a total size, so the tape shows what the
 * bytes on disk are made of instead of a percentage: bytes banked from an
 * earlier checkpoint, bytes fetched this run, and bytes sent again.
 */
export function JobCard({ job, livePhase, onCancelRequested }: JobCardProps) {
  const status = displayStatus(job)
  const snapshot = job.snapshot
  const received = snapshot?.bytesReceived ?? null
  const banked = Math.min(snapshot?.reusedBytes ?? 0, received ?? 0)
  const fetched = Math.max((received ?? 0) - banked, 0)
  const resent = Math.max((snapshot?.networkBytes ?? 0) - fetched, 0)
  const rate =
    snapshot && snapshot.elapsedMs > 0 ? snapshot.bytesReceived / (snapshot.elapsedMs / 1000) : null
  return (
    <article aria-label={`Job ${job.sourceDisplay}`} className="job-row">
      <header className="job-head">
        <h3 className="job-name">
          <Link to={`/downloads/${job.id}`}>{job.destinationDisplay ?? job.sourceDisplay}</Link>
        </h3>
        <span className="job-status" data-status={status}>
          <span aria-hidden="true">{statusGlyph(status)} </span>
          {status}
        </span>
      </header>
      <div
        role="progressbar"
        aria-valuetext="indeterminate"
        aria-label={
          received === null
            ? 'Bytes on disk: not reported yet'
            : `Bytes on disk: ${formatBytes(banked)} banked, ${formatBytes(fetched)} fetched this run, ${formatBytes(resent)} sent again`
        }
        className="tape"
      >
        <i className="banked" style={{ flexGrow: banked }} />
        <i className="fetched" style={{ flexGrow: fetched }} />
        {resent > 0 ? <i className="resent" /> : null}
        <i className="unknown" style={{ flexGrow: Math.max((received ?? 0) * 0.4, 1) }} />
      </div>
      <dl className="tape-legend telemetry">
        <div>
          <dt>Received</dt>
          <dd className="figure">{formatBytes(received)}</dd>
        </div>
        <div>
          <dt>Rate</dt>
          <dd className="figure">{formatRate(rate)}</dd>
        </div>
        <div>
          <dt>ETA</dt>
          <dd className="figure">—</dd>
        </div>
        {banked > 0 ? (
          <div>
            <dt>Resumed from checkpoint</dt>
            <dd className="figure">{formatBytes(banked)}</dd>
          </div>
        ) : null}
        {snapshot && snapshot.retries > 0 ? (
          <div>
            <dt>Retries</dt>
            <dd className="figure">{snapshot.retries}</dd>
          </div>
        ) : null}
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
