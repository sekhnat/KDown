import { useRef, useState, type FormEvent } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import * as AlertDialog from '@radix-ui/react-alert-dialog'
import { useNavigate } from 'react-router-dom'
import { ApiError } from '../../api/apiClient'
import { useRootsQuery } from '../settings/rootQueries'
import { useCreateJobMutation } from './downloadMutations'
import { FieldError, fieldErrorId } from '../../components/FieldError'
import { queryKeys } from '../../api/queryKeys'
import { useQueryClient } from '@tanstack/react-query'

const conflictChoices = [
  { value: 'fail_if_exists', label: 'Fail if the file exists' },
  { value: 'overwrite', label: 'Replace the existing file' },
  { value: 'rename', label: 'Save alongside with a new name' },
  { value: 'resume', label: 'Resume an existing partial download' },
] as const

/**
 * The new-download drawer: URL, allowed root, optional relative directory
 * and filename, and conflict behavior. The host remains authoritative;
 * the form only catches empty/malformed basics and never fabricates a
 * lifecycle state.
 */
export function NewDownloadDrawer() {
  const [open, setOpen] = useState(false)
  const [dirty, setDirty] = useState(false)
  const [confirmDiscard, setConfirmDiscard] = useState(false)
  const triggerRef = useRef<HTMLButtonElement>(null)

  function close() {
    setOpen(false)
    setDirty(false)
    // jsdom does not always run Radix's focus restoration; return focus to
    // the trigger explicitly so keyboard users are not dropped.
    requestAnimationFrame(() => triggerRef.current?.focus())
  }

  return (
    <>
      <button
        ref={triggerRef}
        type="button"
        onClick={() => setOpen(true)}
        data-testid="new-download-trigger"
      >
        New download
      </button>
      <Dialog.Root
        open={open}
        onOpenChange={(next) => {
          if (!next && dirty) {
            setConfirmDiscard(true)
            return
          }
          if (next) {
            setOpen(true)
          } else {
            close()
          }
        }}
      >
        <Dialog.Portal>
          <Dialog.Overlay className="scrim" />
          <Dialog.Content
            aria-describedby="new-download-description"
            style={{
              position: 'fixed',
              top: 0,
              right: 0,
              bottom: 0,
              width: 'min(30rem, 100vw)',
              background: 'var(--surface)',
              padding: '1.5rem',
              overflowY: 'auto',
              borderLeft: '1px solid var(--border)',
            }}
          >
            <Dialog.Title>New download</Dialog.Title>
            <Dialog.Description id="new-download-description">
              Add an HTTP or HTTPS download. It stays inside the folder you choose.
            </Dialog.Description>
            <NewDownloadForm onDirty={() => setDirty(true)} onDone={close} />
            <Dialog.Close asChild>
              <button type="button" aria-label="Close">
                Close
              </button>
            </Dialog.Close>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      <AlertDialog.Root open={confirmDiscard} onOpenChange={setConfirmDiscard}>
        <AlertDialog.Portal>
          <AlertDialog.Overlay className="scrim" />
          <AlertDialog.Content
            style={{
              position: 'fixed',
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              background: 'var(--surface)',
              padding: '1.5rem',
              borderRadius: 'var(--radius-large)',
              border: '1px solid var(--border)',
              maxWidth: '24rem',
            }}
          >
            <AlertDialog.Title>Discard this download?</AlertDialog.Title>
            <AlertDialog.Description>
              The form has unsaved changes. Closing now discards them.
            </AlertDialog.Description>
            <div style={{ display: 'flex', gap: '0.75rem', marginTop: '1rem' }}>
              <AlertDialog.Cancel asChild>
                <button type="button">Keep editing</button>
              </AlertDialog.Cancel>
              <AlertDialog.Action asChild>
                <button
                  type="button"
                  onClick={() => {
                    setConfirmDiscard(false)
                    close()
                  }}
                >
                  Discard
                </button>
              </AlertDialog.Action>
            </div>
          </AlertDialog.Content>
        </AlertDialog.Portal>
      </AlertDialog.Root>
    </>
  )
}

function NewDownloadForm({ onDirty, onDone }: { onDirty(): void; onDone(): void }) {
  const [sourceUrl, setSourceUrl] = useState('')
  const [relativeDirectory, setRelativeDirectory] = useState('')
  const [filenameOverride, setFilenameOverride] = useState('')
  const [conflictPolicy, setConflictPolicy] = useState<string>('fail_if_exists')
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({})
  const [formError, setFormError] = useState<string | null>(null)
  const roots = useRootsQuery()
  const createJob = useCreateJobMutation()
  const queryClient = useQueryClient()
  const navigate = useNavigate()

  const defaultRoot = roots.data?.find((root) => root.is_default) ?? roots.data?.[0]

  async function submit(event: FormEvent) {
    event.preventDefault()
    setFormError(null)
    if (!sourceUrl.trim()) {
      setFieldErrors({ source_url: 'Enter a URL.' })
      return
    }
    if (!defaultRoot) {
      setFormError('Add a download folder first.')
      return
    }
    try {
      const job = await createJob.mutateAsync({
        sourceUrl: sourceUrl.trim(),
        rootId: defaultRoot.id,
        relativeDirectory: relativeDirectory.trim() ? relativeDirectory.trim() : null,
        filenameOverride: filenameOverride.trim() ? filenameOverride.trim() : null,
        conflictPolicy: conflictPolicy as never,
      })
      await queryClient.invalidateQueries({ queryKey: queryKeys.jobs.lists() })
      navigate(`/downloads/${job.id}`)
      onDone()
    } catch (error) {
      if (error instanceof ApiError) {
        setFieldErrors(error.fieldErrors ?? {})
        if (!error.fieldErrors || Object.keys(error.fieldErrors).length === 0) {
          setFormError(error.message)
        }
      } else {
        setFormError('The download could not be started. Try again.')
      }
    }
  }

  return (
    <form onSubmit={submit} noValidate style={{ display: 'grid', gap: '1rem', marginTop: '1rem' }}>
      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
        <label htmlFor="download-url">URL</label>
        <input
          id="download-url"
          type="url"
          value={sourceUrl}
          onChange={(event) => {
            setSourceUrl(event.target.value)
            onDirty()
            setFieldErrors((current) => ({ ...current, source_url: '' }))
          }}
          aria-describedby={fieldErrors.source_url ? fieldErrorId('download-url') : undefined}
          aria-invalid={fieldErrors.source_url ? true : undefined}
          required
        />
        <FieldError id={fieldErrorId('download-url')} message={fieldErrors.source_url} />
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
        <label htmlFor="download-root">Download folder</label>
        <select
          id="download-root"
          value={defaultRoot?.id ?? ''}
          onChange={onDirty}
          disabled={(roots.data?.length ?? 0) === 0}
        >
          {(roots.data ?? []).map((root) => (
            <option key={root.id} value={root.id}>
              {root.label}
            </option>
          ))}
        </select>
        <FieldError id={fieldErrorId('download-root')} message={fieldErrors.root_id} />
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
        <label htmlFor="download-subfolder">Subfolder (optional)</label>
        <input
          id="download-subfolder"
          type="text"
          value={relativeDirectory}
          onChange={(event) => {
            setRelativeDirectory(event.target.value)
            onDirty()
          }}
          aria-describedby={fieldErrors.relative_directory ? fieldErrorId('download-subfolder') : undefined}
        />
        <FieldError id={fieldErrorId('download-subfolder')} message={fieldErrors.relative_directory} />
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
        <label htmlFor="download-filename">Filename (optional)</label>
        <input
          id="download-filename"
          type="text"
          value={filenameOverride}
          onChange={(event) => {
            setFilenameOverride(event.target.value)
            onDirty()
          }}
          aria-describedby={fieldErrors.filename_override ? fieldErrorId('download-filename') : undefined}
        />
        <FieldError id={fieldErrorId('download-filename')} message={fieldErrors.filename_override} />
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
        <label htmlFor="download-conflict">If the file already exists</label>
        <select
          id="download-conflict"
          value={conflictPolicy}
          onChange={(event) => {
            setConflictPolicy(event.target.value)
            onDirty()
          }}
        >
          {conflictChoices.map((choice) => (
            <option key={choice.value} value={choice.value}>
              {choice.label}
            </option>
          ))}
        </select>
      </div>

      {formError ? (
        <p role="alert" style={{ color: 'var(--danger)' }}>
          {formError}
        </p>
      ) : null}

      <button type="submit" disabled={createJob.isPending}>
        {createJob.isPending ? 'Starting…' : 'Start download'}
      </button>
    </form>
  )
}
