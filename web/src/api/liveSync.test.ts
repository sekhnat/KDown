import { describe, expect, it } from 'vitest'
import { createLiveSyncHarness } from './liveSyncHarness'
import type { JobView } from './liveSync'

function jobView(overrides: Partial<JobView> & { id?: string }): JobView {
  return {
    id: overrides.id ?? 'job-1',
    status: 'Active',
    desiredState: 'Running',
    controlVersion: 1,
    attemptId: overrides.attemptId ?? null,
    sampleSeq: overrides.sampleSeq ?? 0,
    sourceDisplay: 'https://example.test/file.iso?…',
    rootId: 'root-1',
    rootLabel: 'Downloads',
    relativeDirectory: null,
    filenameOverride: null,
    destinationDisplay: null,
    createdAt: 0,
    updatedAt: 0,
    snapshot: null,
    ...overrides,
  }
}

function jobSnapshot(overrides: {
  attemptId?: string
  sampleSeq?: number
  id?: string
  streamEpoch?: string
}) {
  return {
    kind: 'job.snapshot' as const,
    streamEpoch: overrides.streamEpoch ?? 'epoch-a',
    job: jobView({
      id: overrides.id ?? 'job-1',
      attemptId: overrides.attemptId ?? null,
      sampleSeq: overrides.sampleSeq ?? 0,
    }),
  }
}

describe('LiveSync reconciliation', () => {
  it('buffers stream snapshots until collection replacement completes', async () => {
    const harness = createLiveSyncHarness()
    harness.open({ streamEpoch: 'epoch-a' })
    harness.receive(jobSnapshot({ attemptId: 'a1', sampleSeq: 7 }))

    await harness.resolveCollection([jobView({ attemptId: 'a1', sampleSeq: 6 })])

    expect(harness.job('job-1')?.sampleSeq).toBe(7)
    expect(harness.phase()).toBe('live')
  })

  it('marks data stale and disables mutations after disconnect', () => {
    const harness = createLiveSyncHarness({ connected: true })
    harness.disconnect()
    expect(harness.phase()).toBe('stale')
    expect(harness.canMutate()).toBe(false)
  })

  it('ignores buffered snapshots the collection already superseded', async () => {
    const harness = createLiveSyncHarness()
    harness.open({ streamEpoch: 'epoch-a' })
    harness.receive(jobSnapshot({ attemptId: 'a1', sampleSeq: 5 }))

    await harness.resolveCollection([jobView({ attemptId: 'a1', sampleSeq: 6 })])

    expect(harness.job('job-1')?.sampleSeq).toBe(6)
  })

  it('applies live snapshots only when newer by epoch, attempt, and sequence', () => {
    const harness = createLiveSyncHarness({ connected: true })

    harness.receive(jobSnapshot({ attemptId: 'a1', sampleSeq: 8 }))
    expect(harness.job('job-1')?.sampleSeq).toBe(8)

    // Same attempt, older sequence: ignored.
    harness.receive(jobSnapshot({ attemptId: 'a1', sampleSeq: 3 }))
    expect(harness.job('job-1')?.sampleSeq).toBe(8)

    // A new attempt arrives after the old one on the same stream: applied.
    harness.receive(jobSnapshot({ attemptId: 'a2', sampleSeq: 0 }))
    expect(harness.job('job-1')?.attemptId).toBe('a2')

    // A different stream epoch replaces everything.
    harness.receive(jobSnapshot({ streamEpoch: 'epoch-b', attemptId: 'a3', sampleSeq: 2 }))
    expect(harness.job('job-1')?.attemptId).toBe('a3')
    expect(harness.epoch()).toBe('epoch-b')
  })
})
