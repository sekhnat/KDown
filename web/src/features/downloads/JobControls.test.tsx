import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { JobControls } from './JobControls'
import type { JobView } from '../../api/liveSync'

type JobViewFixture = Omit<JobView, 'status'> & { status: string }

function jobView(overrides: Partial<JobViewFixture>): JobView {
  const status = overrides.status ?? 'Running'
  return {
    id: 'job-1',
    status: 'Active' as JobView['status'],
    desiredState: 'Running',
    controlVersion: 3,
    attemptId: 'a1',
    sampleSeq: 5,
    sourceDisplay: 'https://example.test/file.iso?…',
    rootId: 'root-1',
    rootLabel: 'Downloads',
    relativeDirectory: 'isos',
    filenameOverride: null,
    destinationDisplay: null,
    createdAt: 0,
    updatedAt: 0,
    snapshot: { stateLabel: status, bytesReceived: 10, networkBytes: 12, reusedBytes: 0, retries: 0, elapsedMs: 1000 },
    ...(overrides as Partial<JobView>),
  } as JobView
}

function renderControls(job: JobView, options: { livePhase?: 'live' | 'stale' } = {}) {
  const onAction = vi.fn()
  render(
    <JobControls job={job} livePhase={options.livePhase ?? 'live'} onAction={onAction} />,
  )
  return { onAction }
}

describe('JobControls', () => {
  it.each([
    ['Running', ['Pause', 'Cancel']],
    ['Paused', ['Resume', 'Cancel']],
    ['Failed', ['Retry', 'Remove from history']],
    ['Completed', ['Reveal in folder', 'Remove from history']],
  ] as const)('shows only legal controls for %s', (status, labels) => {
    renderControls(jobView({ status }))
    for (const label of labels) {
      expect(screen.getByRole('button', { name: label })).toBeVisible()
    }
  })

  it('shows no lifecycle controls for transient states', () => {
    renderControls(jobView({ status: 'Committing' }))
    expect(screen.queryByRole('button', { name: 'Pause' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Cancel' })).not.toBeInTheDocument()
  })

  it('disables all mutations while the connection is stale', () => {
    renderControls(jobView({ status: 'Running' }), { livePhase: 'stale' })
    expect(screen.getByRole('button', { name: 'Pause' })).toBeDisabled()
    expect(screen.getByText(/reconnecting/i)).toBeVisible()
  })

  it('reports the requested action to the caller', async () => {
    const user = userEvent.setup()
    const { onAction } = renderControls(jobView({ status: 'Running' }))
    await user.click(screen.getByRole('button', { name: 'Pause' }))
    expect(onAction).toHaveBeenCalledWith('pause')
  })
})
