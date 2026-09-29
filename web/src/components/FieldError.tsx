import { useId } from 'react'

/**
 * Field-level error display: an alert tied to the input by
 * `aria-describedby` so screen readers announce it with the field.
 */
export function FieldError({ id, message }: { id?: string; message?: string | null }) {
  const fallbackId = useId()
  const errorId = id ?? fallbackId
  if (!message) {
    return null
  }
  return (
    <p id={errorId} role="alert" className="field-error" style={{ color: 'var(--danger)' }}>
      {message}
    </p>
  )
}

/** Stable describedby id helper for inputs paired with a FieldError. */
export function fieldErrorId(base: string): string {
  return `${base}-error`
}
