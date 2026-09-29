import { useEffect, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { ApiError, type ArtifactPolicy } from '../../api/client'
import { queryKeys } from '../../api/queryKeys'
import type { JobDetail } from '../../api/client'
import { useCancelJobMutation, usePauseJobMutation, useRemoveJobMutation, useResumeJobMutation, useRetryJobMutation, useRevealJobMutation } from './downloadMutations'
import { JobControls } from './JobControls'
import { CancelDialog } from './CancelDialog'
import { RateSparkline } from './RateSparkline'
import { displayStatus, statusGlyph } from './jobState'

export interface JobDetailPageProps {
  detail: JobDetail
  livePhase: 'live' | 'stale' | 'connecting' | 'syncing'
}

function formatBytes(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined) return '—'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`
}

function formatDuration(ms: number): string {
  const seconds = Math.floor(ms / 1000)
  const minutes = Math.floor(seconds / 60)
  if (minutes === 0) return `${seconds}s`
  return `${minutes}m ${seconds % 60}s`
}

/**
 * Job detail: lifecycle guidance, telemetry (bounded 120-sample rate
 * history kept only in browser memory), expandable typed failure detail,
 * and the explicit cancellation choice.
 */
export function JobDetailPage({ detail, livePhase }: JobDetailPageProps) {
  const job = detail.job
  const status = displayStatus(job)
  const [samples, setSamples] = useState<number[]>([])
  const [lastSampledSeq, setLastSampledSeq] = useState(-1)
  const [cancelOpen, setCancelOpen] = useState(false)
  const [removeOpen, setRemoveOpen] = useState(false)
  const [conflictNotice, setConflictNotice] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)
  const [errorExpanded, setErrorExpanded] = useState(false)
  const lastAnnounced = useRef<string | null>(null)
  const queryClient = useQueryClient()

  // One rate sample per observed snapshot change, bounded to 120. Derived
  // during render per the React "adjust state on prop change" pattern.
  if (job.sampleSeq !== lastSampledSeq) {
    setLastSampledSeq(job.sampleSeq)
    if (job.snapshot && job.snapshot.elapsedMs > 0) {
      const rate = job.snapshot.bytesReceived / (job.snapshot.elapsedMs / 1000)
      setSamples((current) => [...current, rate].slice(-120))
    }
  }

  // Announce lifecycle changes politely, never each telemetry tick.
  useEffect(() => {
    if (lastAnnounced.current !== null && lastAnnounced.current !== status) {
      setConflictNotice((current) => current)
    }
    lastAnnounced.current = status
  }, [status])

  const pause = usePauseJobMutation()
  const resume = useResumeJobMutation()
  const cancel = useCancelJobMutation()
  const retry = useRetryJobMutation()
  const remove = useRemoveJobMutation()
  const reveal = useRevealJobMutation()

  function applyConflict(error: unknown) {
    if (error instanceof ApiError && error.code === 'stale_control_version') {
      if (error.currentJob) {
        queryClient.setQueryData(queryKeys.jobs.detail(job.id), {
          job: error.currentJob,
          attempts: detail.attempts,
        })
      }
      setConflictNotice('That state changed elsewhere. The view now shows the current state.')
      setActionError(null)
      return
    }
    if (error instanceof ApiError) {
      setActionError(error.message)
      return
    }
    setActionError('The action could not be completed. Try again.')
  }

  async function onAction(action: 'pause' | 'resume' | 'retry') {
    setConflictNotice(null)
    setActionError(null)
    try {
      if (action === 'pause') {
        await pause.mutateAsync({ id: job.id, expectedControlVersion: job.controlVersion })
      } else if (action === 'resume') {
        await resume.mutateAsync({ id: job.id, expectedControlVersion: job.controlVersion })
      } else {
        await retry.mutateAsync({ id: job.id, expectedControlVersion: job.controlVersion })
      }
      await queryClient.invalidateQueries({ queryKey: queryKeys.jobs.all })
    } catch (error) {
      applyConflict(error)
    }
  }

  async function onCancel(policy: ArtifactPolicy) {
    setCancelOpen(false)
    setConflictNotice(null)
    try {
      await cancel.mutateAsync({
        id: job.id,
        expectedControlVersion: job.controlVersion,
        artifactPolicy: policy,
      })
      await queryClient.invalidateQueries({ queryKey: queryKeys.jobs.all })
    } catch (error) {
      applyConflict(error)
    }
  }

  async function onReveal() {
    try {
      await reveal.mutateAsync(job.id)
    } catch (error) {
      applyConflict(error)
    }
  }

  async function onRemove() {
    setRemoveOpen(false)
    try {
      await remove.mutateAsync(job.id)
      await queryClient.invalidateQueries({ queryKey: queryKeys.jobs.all })
    } catch (error) {
      applyConflict(error)
    }
  }

  const failedAttempt = [...detail.attempts]
    .reverse()
    .find((attempt) => attempt.outcome?.kind === 'failed')

  return (
    <section aria-labelledby="job-detail-heading">
      <h1 id="job-detail-heading">
        <span aria-hidden="true">{statusGlyph(status)}</span> {status}
      </h1>
      <p>{job.sourceDisplay}</p>

      <div aria-live="polite" style={{ position: 'absolute', left: -9999 }}>
        {`Download is ${status}`}
      </div>

      {conflictNotice ? (
        <p role="status" style={{ color: 'var(--warning)' }}>
          {conflictNotice}
        </p>
      ) : null}
      {actionError ? (
        <p role="alert" style={{ color: 'var(--danger)' }}>
          {actionError}
        </p>
      ) : null}

      <dl className="telemetry" style={{ display: 'grid', gridTemplateColumns: 'auto auto', gap: '0.35rem 1.5rem', width: 'fit-content' }}>
        <dt>Received</dt>
        <dd>{formatBytes(job.snapshot?.bytesReceived)}</dd>
        <dt>Network bytes</dt>
        <dd>{formatBytes(job.snapshot?.networkBytes)}</dd>
        <dt>Wire rate</dt>
        <dd>
          {job.snapshot && job.snapshot.elapsedMs > 0
            ? `${formatBytes(job.snapshot.networkBytes / (job.snapshot.elapsedMs / 1000))}/s`
            : '—'}
        </dd>
        <dt>Elapsed</dt>
        <dd>{job.snapshot ? formatDuration(job.snapshot.elapsedMs) : '—'}</dd>
        <dt>Retries</dt>
        <dd>{job.snapshot?.retries ?? '—'}</dd>
        <dt>Reused from checkpoint</dt>
        <dd>{formatBytes(job.snapshot?.reusedBytes)}</dd>
        <dt>ETA</dt>
        <dd>—</dd>
        <dt>Folder</dt>
        <dd>{job.rootLabel || '—'}</dd>
        <dt>Destination</dt>
        <dd>{job.destinationDisplay ?? job.relativeDirectory ?? '—'}</dd>
      </dl>

      <h2>Rate history</h2>
      <RateSparkline samples={samples} />

      {failedAttempt?.outcome ? (
        <>
          <h2>Failure detail</h2>
          <button type="button" aria-expanded={errorExpanded} onClick={() => setErrorExpanded((v) => !v)}>
            {errorExpanded ? 'Hide technical detail' : 'Show technical detail'}
          </button>
          {errorExpanded ? (
            <pre style={{ whiteSpace: 'pre-wrap', color: 'var(--text-muted)' }}>
              {failedAttempt.outcome.code ?? 'unknown'}
              {'\n'}
              {failedAttempt.outcome.detail ?? ''}
            </pre>
          ) : null}
        </>
      ) : null}

      <h2>Controls</h2>
      <JobControls
        job={job}
        livePhase={livePhase}
        onAction={(action) => {
          if (action === 'pause' || action === 'resume' || action === 'retry') {
            void onAction(action)
          } else if (action === 'reveal') {
            void onReveal()
          } else if (action === 'remove') {
            setRemoveOpen(true)
          }
        }}
        onCancelRequested={() => setCancelOpen(true)}
      />

      <CancelDialog job={job} open={cancelOpen} onConfirm={(policy) => void onCancel(policy)} onDismiss={() => setCancelOpen(false)} />

      {removeOpen ? (
        <div
          role="alertdialog"
          aria-modal="true"
          aria-labelledby="remove-title"
          style={{ position: 'fixed', inset: 0, display: 'grid', placeItems: 'center', background: 'rgba(2,8,16,0.7)', zIndex: 20 }}
        >
          <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 'var(--radius-large)', padding: '1.5rem', maxWidth: '26rem' }}>
            <h2 id="remove-title">Remove from history?</h2>
            <p>
              This deletes the job's history entry. The downloaded file remains on disk.
            </p>
            <div style={{ display: 'flex', gap: '0.75rem' }}>
              <button type="button" onClick={() => setRemoveOpen(false)}>
                Keep history
              </button>
              <button type="button" onClick={() => void onRemove()}>
                Remove from history
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  )
}
