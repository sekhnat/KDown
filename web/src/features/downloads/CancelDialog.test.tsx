import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { CancelDialog } from './CancelDialog'
import type { JobView } from '../../api/liveSync'

function jobView(): JobView {
  return {
    id: 'job-1',
    status: 'Active',
    desiredState: 'Running',
    controlVersion: 7,
    attemptId: 'a1',
    sampleSeq: 2,
    sourceDisplay: 'https://example.test/file.iso?…',
    rootId: 'root-1',
    rootLabel: 'Downloads',
    relativeDirectory: null,
    filenameOverride: null,
    destinationDisplay: null,
    createdAt: 0,
    updatedAt: 0,
    snapshot: null,
  }
}

function renderCancelDialog(onConfirm = vi.fn()) {
  render(<CancelDialog job={jobView()} open onConfirm={onConfirm} onDismiss={() => undefined} />)
  return { onConfirm }
}

describe('CancelDialog', () => {
  it('defaults cancellation to preserving resumable data', () => {
    renderCancelDialog()
    expect(screen.getByRole('radio', { name: /keep partial/i })).toBeChecked()
    expect(screen.getByRole('radio', { name: /delete partial/i })).not.toBeChecked()
  })

  it('explains what each artifact choice does', () => {
    renderCancelDialog()
    expect(screen.getByText(/keeps the partial file and its resume checkpoint/i)).toBeVisible()
    expect(screen.getByText(/deletes the partial file and its resume checkpoint/i)).toBeVisible()
    expect(screen.getByText(/keeps the file but the download cannot resume/i)).toBeVisible()
  })

  it('confirms with the chosen artifact policy', async () => {
    const user = userEvent.setup()
    const { onConfirm } = renderCancelDialog()
    await user.click(screen.getByRole('radio', { name: /delete partial/i }))
    await user.click(screen.getByRole('button', { name: /cancel download/i }))
    expect(onConfirm).toHaveBeenCalledWith('delete_partial')
  })
})
