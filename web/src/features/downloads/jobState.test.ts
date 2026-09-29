import { describe, expect, it } from 'vitest'
import { displayStatus, actionsFor } from './jobState'
import type { JobView } from '../../api/liveSync'
import type { JobStatus } from '../../api/liveSync'

function jobWith(overrides: {
  status: JobStatus
  stateLabel?: string
}): JobView {
  return {
    id: 'job-1',
    status: overrides.status,
    desiredState: 'Running',
    controlVersion: 1,
    attemptId: null,
    sampleSeq: 0,
    sourceDisplay: 'https://example.test/file.bin',
    rootId: 'root-1',
    rootLabel: 'Downloads',
    relativeDirectory: null,
    filenameOverride: null,
    destinationDisplay: null,
    createdAt: 0,
    updatedAt: 0,
    snapshot: overrides.stateLabel
      ? {
          stateLabel: overrides.stateLabel,
          bytesReceived: 0,
          networkBytes: 0,
          reusedBytes: 0,
          retries: 0,
          elapsedMs: 0,
        }
      : null,
  } as unknown as JobView
}

describe('displayStatus', () => {
  it('shows the user-commanded Paused state even when the engine label lags', () => {
    // The engine's cooperative pause can leave its own state at Running
    // until workers notice; the user commanded Paused and the UI must say so.
    const job = jobWith({ status: 'Paused', stateLabel: 'Running' })
    expect(displayStatus(job)).toBe('Paused')
  })

  it('keeps engine lifecycle labels authoritative while the job is Active', () => {
    const job = jobWith({ status: 'Active', stateLabel: 'Running' })
    expect(displayStatus(job)).toBe('Running')
  })

  it('prefers durable terminal states over engine labels', () => {
    expect(displayStatus(jobWith({ status: 'Cancelled', stateLabel: 'Cancelling' }))).toBe(
      'Cancelled',
    )
    expect(displayStatus(jobWith({ status: 'Failed', stateLabel: 'Failing' }))).toBe('Failed')
    expect(displayStatus(jobWith({ status: 'Completed', stateLabel: 'Committing' }))).toBe(
      'Completed',
    )
  })

  it('offers resume controls for a paused job whose engine label lags', () => {
    const job = jobWith({ status: 'Paused', stateLabel: 'Running' })
    expect(actionsFor(job)).toEqual(['resume', 'cancel'])
  })
})
