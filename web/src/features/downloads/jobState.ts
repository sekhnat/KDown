/**
 * The single source of truth for which controls are legal in which
 * displayed state, plus the mapping from durable + snapshot state to the
 * status label the UI presents.
 */
import type { JobView } from '../../api/liveSync'

export type JobAction = 'pause' | 'resume' | 'cancel' | 'retry' | 'remove' | 'reveal'

/**
 * Every status the UI can present: durable app states plus the engine
 * lifecycle labels carried by telemetry snapshots.
 */
export type UiStatus =
  | 'Queued'
  | 'Recovering'
  | 'Active'
  | 'Created'
  | 'Probing'
  | 'Preparing'
  | 'Running'
  | 'Pausing'
  | 'Paused'
  | 'Resuming'
  | 'Verifying'
  | 'Committing'
  | 'Cancelling'
  | 'Cancelled'
  | 'Failing'
  | 'Failed'
  | 'Completed'

export const legalActions: Record<UiStatus, readonly JobAction[]> = {
  Queued: ['cancel'],
  Recovering: ['cancel'],
  Active: ['cancel'],
  Created: ['cancel'],
  Probing: ['cancel'],
  Preparing: ['cancel'],
  Running: ['pause', 'cancel'],
  Pausing: ['cancel'],
  Paused: ['resume', 'cancel'],
  Resuming: ['cancel'],
  Verifying: ['cancel'],
  Committing: [],
  Cancelling: [],
  Cancelled: ['retry', 'remove'],
  Failing: [],
  Failed: ['retry', 'remove'],
  Completed: ['reveal', 'remove'],
}

/** The engine snapshot is authoritative for live states. */
export function displayStatus(job: JobView): UiStatus {
  const label = job.snapshot?.stateLabel
  if (label && label in legalActions) {
    return label as UiStatus
  }
  return (job.status in legalActions ? job.status : 'Failed') as UiStatus
}

export function actionsFor(job: JobView): readonly JobAction[] {
  return legalActions[displayStatus(job)]
}

export function isTerminalStatus(status: UiStatus): boolean {
  return status === 'Completed' || status === 'Failed' || status === 'Cancelled'
}

/** Status icon name; color never carries status alone. */
export function statusGlyph(status: UiStatus): string {
  switch (status) {
    case 'Completed':
      return '✓'
    case 'Failed':
    case 'Failing':
      return '✗'
    case 'Cancelled':
    case 'Cancelling':
      return '⃠'
    case 'Paused':
      return '‖'
    case 'Queued':
    case 'Recovering':
      return '…'
    case 'Running':
      return '▶'
    default:
      return '•'
  }
}
