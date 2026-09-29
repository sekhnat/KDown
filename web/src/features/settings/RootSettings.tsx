/** Root administration: the only surface that shows canonical paths. */
import { useState, type FormEvent } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { api, ApiError } from '../../api/apiClient'
import { queryKeys } from '../../api/queryKeys'
import { useRootsQuery } from './rootQueries'
import { FieldError, fieldErrorId } from '../../components/FieldError'

export function RootSettings() {
  const roots = useRootsQuery()
  const queryClient = useQueryClient()
  const [label, setLabel] = useState('')
  const [absolutePath, setAbsolutePath] = useState('')
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({})
  const [formError, setFormError] = useState<string | null>(null)

  async function invalidate() {
    await queryClient.invalidateQueries({ queryKey: queryKeys.roots.all })
    await queryClient.invalidateQueries({ queryKey: queryKeys.settings.all })
  }

  const addRoot = useMutation({
    mutationFn: (input: { label: string; absolutePath: string; makeDefault: boolean }) =>
      api.createRoot(input),
    onSuccess: async () => {
      setLabel('')
      setAbsolutePath('')
      await invalidate()
    },
    onError: (error) => {
      if (error instanceof ApiError) {
        setFieldErrors(error.fieldErrors ?? {})
        if (!error.fieldErrors || Object.keys(error.fieldErrors).length === 0) {
          setFormError(error.message)
        }
      } else {
        setFormError('The folder could not be added. Try again.')
      }
    },
  })

  const patchRoot = useMutation({
    mutationFn: ({ id, patch }: { id: string; patch: { enabled?: boolean; makeDefault?: boolean } }) =>
      api.patchRoot(id, patch),
    onSuccess: async () => {
      await invalidate()
    },
    onError: (error) => {
      if (error instanceof ApiError) {
        setFormError(error.message)
      } else {
        setFormError('The change could not be applied. Try again.')
      }
    },
  })

  function submit(event: FormEvent) {
    event.preventDefault()
    setFormError(null)
    setFieldErrors({})
    if (!absolutePath.trim().startsWith('/')) {
      setFieldErrors({ absolute_path: 'Enter an absolute directory path.' })
      return
    }
    addRoot
      .mutateAsync({
        label: label.trim() || 'Downloads',
        absolutePath: absolutePath.trim(),
        makeDefault: false,
      })
      .catch(() => undefined)
  }

  return (
    <section aria-labelledby="roots-heading">
      <h2 id="roots-heading">Download folders</h2>
      <table style={{ borderCollapse: 'collapse', width: '100%' }}>
        <caption style={{ textAlign: 'left', color: 'var(--text-muted)' }}>
          Allowed folders; downloads always stay inside them.
        </caption>
        <thead>
          <tr>
            <th scope="col">Label</th>
            <th scope="col">Folder</th>
            <th scope="col">State</th>
            <th scope="col">Actions</th>
          </tr>
        </thead>
        <tbody>
          {(roots.data ?? []).map((root) => (
            <tr key={root.id}>
              <td>{root.label}</td>
              <td>{root.canonical_path}</td>
              <td>{root.enabled ? 'Enabled' : 'Disabled'}</td>
              <td style={{ display: 'flex', gap: '0.5rem' }}>
                {root.enabled ? (
                  <button
                    type="button"
                    onClick={() => {
                      setFormError(null)
                      patchRoot.mutateAsync({ id: root.id, patch: { enabled: false } }).catch(() => undefined)
                    }}
                  >
                    Disable downloads
                  </button>
                ) : (
                  <button
                    type="button"
                    onClick={() => {
                      setFormError(null)
                      patchRoot.mutateAsync({ id: root.id, patch: { enabled: true } }).catch(() => undefined)
                    }}
                  >
                    Enable
                  </button>
                )}
                {!root.is_default ? (
                  <button
                    type="button"
                    onClick={() => {
                      setFormError(null)
                      patchRoot.mutateAsync({ id: root.id, patch: { makeDefault: true } }).catch(() => undefined)
                    }}
                  >
                    Make default
                  </button>
                ) : (
                  <span style={{ color: 'var(--accent)' }}>Default</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {formError ? (
        <p role="alert" style={{ color: 'var(--danger)' }}>
          {formError}
        </p>
      ) : null}
      <form onSubmit={submit} noValidate style={{ display: 'grid', gap: '0.5rem', marginTop: '1rem', maxWidth: '30rem' }}>
        <h3>Add a folder</h3>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.15rem' }}>
          <label htmlFor="root-label">Label</label>
          <input id="root-label" value={label} onChange={(event) => setLabel(event.target.value)} />
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.15rem' }}>
          <label htmlFor="root-path">Absolute folder path</label>
          <input
            id="root-path"
            value={absolutePath}
            onChange={(event) => {
              setAbsolutePath(event.target.value)
              setFieldErrors({})
            }}
            aria-describedby={fieldErrors.absolute_path ? fieldErrorId('root-path') : undefined}
            aria-invalid={fieldErrors.absolute_path ? true : undefined}
            autoComplete="off"
            spellCheck={false}
          />
          <FieldError id={fieldErrorId('root-path')} message={fieldErrors.absolute_path} />
        </div>
        <button type="submit" disabled={addRoot.isPending}>
          {addRoot.isPending ? 'Adding…' : 'Add folder'}
        </button>
      </form>
    </section>
  )
}
