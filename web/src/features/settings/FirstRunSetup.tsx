import { useState, type FormEvent } from 'react'
import { FieldError, fieldErrorId } from '../../components/FieldError'
import { ApiError } from '../../api/apiClient'
import { useCreateRootMutation } from './rootQueries'

/**
 * Blocking first-run surface: the SPA cannot offer downloads until one
 * enabled root exists. The suggested XDG Downloads path is prefilled; the
 * user confirms it or enters another existing absolute directory.
 */
export function FirstRunSetup({ suggestedPath }: { suggestedPath: string | null }) {
  const [path, setPath] = useState(suggestedPath ?? '')
  const [pathError, setPathError] = useState<string | null>(null)
  const [formError, setFormError] = useState<string | null>(null)
  const createRoot = useCreateRootMutation()

  async function submit(event: FormEvent) {
    event.preventDefault()
    setFormError(null)
    if (!path.trim().startsWith('/')) {
      setPathError('Enter an absolute directory path, for example /home/user/Downloads.')
      return
    }
    try {
      await createRoot.mutateAsync({
        label: 'Downloads',
        absolutePath: path.trim(),
        makeDefault: true,
      })
      // Success: the bootstrap invalidation removes this surface and the
      // router reveals the app.
    } catch (error) {
      if (error instanceof ApiError) {
        setPathError(error.fieldErrors?.absolute_path ?? null)
        if (!error.fieldErrors?.absolute_path) {
          setFormError(error.message)
        }
      } else {
        setFormError('The folder could not be added. Try again.')
      }
    }
  }

  return (
    <section aria-labelledby="first-run-heading" style={{ maxWidth: '32rem', margin: '4rem auto' }}>
      <h1 id="first-run-heading">Choose a download folder</h1>
      <p>
        KDown keeps every download inside folders you allow. Confirm the suggested folder
        or enter another existing directory.
      </p>
      <form onSubmit={submit} noValidate>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
          <label htmlFor="first-run-path">Download folder</label>
          <input
            id="first-run-path"
            type="text"
            value={path}
            onChange={(event) => {
              setPath(event.target.value)
              setPathError(null)
            }}
            aria-describedby={pathError ? fieldErrorId('first-run-path') : undefined}
            aria-invalid={pathError ? true : undefined}
            autoComplete="off"
            spellCheck={false}
          />
          <FieldError id={fieldErrorId('first-run-path')} message={pathError} />
        </div>
        {formError ? (
          <p role="alert" style={{ color: 'var(--danger)' }}>
            {formError}
          </p>
        ) : null}
        <div style={{ display: 'flex', gap: '0.75rem', marginTop: '1rem' }}>
          <button
            type="submit"
            disabled={createRoot.isPending}
            data-testid="use-this-folder"
          >
            {createRoot.isPending ? 'Adding folder…' : 'Use this folder'}
          </button>
        </div>
      </form>
    </section>
  )
}
