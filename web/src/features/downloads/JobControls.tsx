import { actionsFor, type JobAction } from './jobState'
import type { JobView } from '../../api/liveSync'

const actionLabels: Record<JobAction, string> = {
  pause: 'Pause',
  resume: 'Resume',
  cancel: 'Cancel',
  retry: 'Retry',
  remove: 'Remove from history',
  reveal: 'Reveal in folder',
}

interface JobControlsProps {
  job: JobView
  livePhase: 'live' | 'stale' | 'connecting' | 'syncing'
  onAction(action: JobAction): void
  onCancelRequested?(): void
}

/**
 * Lifecycle controls for one job: only legal actions render, everything
 * disables while the connection is stale, and the visible explanation
 * says why.
 */
export function JobControls({ job, livePhase, onAction, onCancelRequested }: JobControlsProps) {
  const actions = actionsFor(job)
  const stale = livePhase !== 'live'
  return (
    <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', flexWrap: 'wrap' }}>
      {actions.map((action) =>
        action === 'cancel' ? (
          <button
            key={action}
            type="button"
            disabled={stale}
            onClick={onCancelRequested}
          >
            {actionLabels[action]}
          </button>
        ) : (
          <button
            key={action}
            type="button"
            disabled={stale}
            onClick={() => onAction(action)}
          >
            {actionLabels[action]}
          </button>
        ),
      )}
      {stale ? <span style={{ color: 'var(--warning)' }}>Reconnecting…</span> : null}
    </div>
  )
}
