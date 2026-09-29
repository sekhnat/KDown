import { useState } from 'react'
import type { ArtifactPolicy } from '../../api/client'
import type { JobView } from '../../api/liveSync'

interface CancelDialogProps {
  job: JobView
  open: boolean
  onConfirm(policy: ArtifactPolicy): void
  onDismiss(): void
}

const choices: { value: ArtifactPolicy; title: string; detail: string }[] = [
  {
    value: 'preserve_partial',
    title: 'Keep partial data',
    detail: 'Keeps the partial file and its resume checkpoint. You can resume later.',
  },
  {
    value: 'delete_partial',
    title: 'Delete partial data',
    detail: 'Deletes the partial file and its resume checkpoint. This cannot be undone.',
  },
  {
    value: 'keep_file_discard_checkpoint',
    title: 'Keep file, discard checkpoint',
    detail: 'Keeps the file but the download cannot resume from it.',
  },
]

/**
 * Cancellation confirmation: preserving resumable data is the default and
 * the destructive options are explicit, never silently discarded.
 */
export function CancelDialog({ job, open, onConfirm, onDismiss }: CancelDialogProps) {
  const [policy, setPolicy] = useState<ArtifactPolicy>('preserve_partial')
  if (!open) {
    return null
  }
  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="cancel-dialog-title"
      style={{
        position: 'fixed',
        inset: 0,
        display: 'grid',
        placeItems: 'center',
        background: 'rgba(2, 8, 16, 0.7)',
        zIndex: 20,
      }}
      onClick={onDismiss}
    >
      <div
        role="document"
        style={{
          background: 'var(--surface)',
          border: '1px solid var(--border)',
          borderRadius: 'var(--radius-large)',
          padding: '1.5rem',
          maxWidth: '28rem',
        }}
        onClick={(event) => event.stopPropagation()}
      >
        <h2 id="cancel-dialog-title">Cancel this download?</h2>
        <p>Choose what happens to the partial data already on disk for <strong>{job.sourceDisplay}</strong>.</p>
        <fieldset style={{ display: 'grid', gap: '0.75rem', border: 'none', padding: 0 }}>
          <legend style={{ position: 'absolute', left: -9999 }}>Artifact policy</legend>
          {choices.map((choice) => (
            <label key={choice.value} style={{ display: 'grid', gap: '0.15rem' }}>
              <span style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
                <input
                  type="radio"
                  name="artifact-policy"
                  value={choice.value}
                  checked={policy === choice.value}
                  onChange={() => setPolicy(choice.value)}
                />
                <strong>{choice.title}</strong>
              </span>
              <span style={{ color: 'var(--text-muted)' }}>{choice.detail}</span>
            </label>
          ))}
        </fieldset>
        <div style={{ display: 'flex', gap: '0.75rem', marginTop: '1.25rem' }}>
          <button type="button" onClick={onDismiss}>
            Keep downloading
          </button>
          <button type="button" onClick={() => onConfirm(policy)} data-testid="confirm-cancel">
            Cancel download
          </button>
        </div>
      </div>
    </div>
  )
}
