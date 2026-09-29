/**
 * Typed fetch client for the versioned local API. The CSRF token lives
 * only in memory; the browser supplies `Origin` naturally; mutations are
 * JSON only. Console output never contains the token or full source URLs.
 */
import type { components } from './schema'
import type { JobView as LiveJobView } from './liveSync'

type ApiErrorEnvelopeDto = components['schemas']['ApiErrorEnvelope']
type JobViewDto = components['schemas']['JobViewDto']
type BootstrapDto = components['schemas']['BootstrapDto']
type CreateJobRequest = components['schemas']['CreateJobRequest']
type RootDto = components['schemas']['RootDto']
type AppSettingsDto = components['schemas']['AppSettingsDto']
type JobPageDto = components['schemas']['JobPageDto']
type JobDetailDto = components['schemas']['JobDetailDto']

/** App-facing camelCase view mapped from the wire DTO. */
export type JobView = LiveJobView
export type { EngineSnapshotView } from './liveSync'

export interface Bootstrap {
  csrfToken: string
  origin: string
  build: string
  streamEpoch: string
  suggestedDownloadRoot: string | null
  roots: { id: string; label: string; enabled: boolean; isDefault: boolean }[]
}

export class ApiError extends Error {
  readonly code: string
  readonly retryable: boolean
  readonly fieldErrors: Record<string, string> | null
  readonly currentJob: JobView | null
  readonly detail: string | null
  readonly status: number

  constructor(envelope: ApiErrorEnvelopeDto, status: number) {
    super(envelope.message)
    this.name = 'ApiError'
    this.code = envelope.code
    this.retryable = envelope.retryable
    this.fieldErrors = envelope.field_errors ?? null
    this.currentJob = envelope.current_job ? fromDto(envelope.current_job) : null
    this.detail = envelope.detail ?? null
    this.status = status
  }
}

export function fromDto(dto: JobViewDto): JobView {
  return {
    id: dto.id,
    status: dto.status,
    desiredState: dto.desired_state,
    controlVersion: dto.control_version,
    attemptId: dto.attempt_id ?? null,
    sampleSeq: dto.sample_seq,
    sourceDisplay: dto.source_display,
    rootId: dto.root_id,
    rootLabel: dto.root_label ?? '',
    relativeDirectory: dto.relative_directory ?? null,
    filenameOverride: dto.filename_override ?? null,
    destinationDisplay: dto.destination_display ?? null,
    createdAt: dto.created_at,
    updatedAt: dto.updated_at,
    snapshot: dto.snapshot
      ? {
          stateLabel: dto.snapshot.state_label,
          bytesReceived: dto.snapshot.bytes_received,
          networkBytes: dto.snapshot.network_bytes,
          reusedBytes: dto.snapshot.reused_bytes,
          retries: dto.snapshot.retries,
          elapsedMs: dto.snapshot.elapsed_ms,
        }
      : null,
  }
}

export interface JobPage {
  jobs: JobView[]
  nextCursor: string | null
}

export interface JobDetail {
  job: JobView
  attempts: {
    id: string
    reason: string
    startedAt: number
    finishedAt: number | null
    outcome: {
      kind: string
      code?: string
      detail?: string
      metrics?: { bytesReceived: number; networkBytes: number; durationMs: number }
    } | null
  }[]
}

export interface JobListParams {
  cursor?: string
  limit?: number
  status?: string
  source?: string
  from?: number
  to?: number
}

export type ConflictPolicy = 'fail_if_exists' | 'overwrite' | 'rename' | 'resume'
export type ArtifactPolicy = 'preserve_partial' | 'delete_partial' | 'keep_file_discard_checkpoint'

export class ApiClient {
  private csrf: string | null = null

  /**
   * Fetches fresh session data on every call. The token stays in memory
   * only; first-run completion relies on re-reading the bootstrap.
   */
  async bootstrap(): Promise<Bootstrap> {
    const response = await fetch('/api/v1/bootstrap', {
      headers: { accept: 'application/json' },
      cache: 'no-store',
    })
    if (!response.ok) {
      throw new ApiError(await errorEnvelope(response), response.status)
    }
    const dto = (await response.json()) as BootstrapDto
    this.csrf = dto.csrf_token
    return {
      csrfToken: dto.csrf_token,
      origin: dto.origin,
      build: dto.build,
      streamEpoch: dto.stream_epoch,
      suggestedDownloadRoot: dto.suggested_download_root ?? null,
      roots: (dto.roots ?? []).map((root) => ({
        id: root.id,
        label: root.label,
        enabled: root.enabled,
        isDefault: root.is_default,
      })),
    }
  }

  private async request<T>(path: string, init: RequestInit): Promise<T> {
    const response = await fetch(path, init)
    if (!response.ok) {
      throw new ApiError(await errorEnvelope(response), response.status)
    }
    if (response.status === 204) {
      return undefined as T
    }
    return (await response.json()) as T
  }

  private async mutate<T>(method: string, path: string, body?: unknown): Promise<T> {
    await this.bootstrap()
    return this.request<T>(path, {
      method,
      headers: {
        'content-type': 'application/json',
        ...(this.csrf ? { 'x-kdown-csrf': this.csrf } : {}),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
      credentials: 'same-origin',
    })
  }

  /** Test seam: forget the cached bootstrap so each test starts fresh. */
  resetBootstrapCache(): void {
    this.csrf = null
  }

  async listJobs(params: JobListParams = {}): Promise<JobPage> {
    const query = new URLSearchParams()
    if (params.cursor) query.set('cursor', params.cursor)
    if (params.limit !== undefined) query.set('limit', String(params.limit))
    if (params.status) query.set('status', params.status)
    if (params.source) query.set('source', params.source)
    if (params.from !== undefined) query.set('from', String(params.from))
    if (params.to !== undefined) query.set('to', String(params.to))
    const suffix = query.size > 0 ? `?${query.toString()}` : ''
    const dto = await this.request<JobPageDto>(`/api/v1/jobs${suffix}`, {})
    return {
      jobs: dto.jobs.map(fromDto),
      nextCursor: dto.next_cursor ?? null,
    }
  }

  async getJob(id: string): Promise<JobDetail> {
    const dto = await this.request<JobDetailDto>(`/api/v1/jobs/${id}`, {})
    return {
      job: fromDto(dto.job),
      attempts: dto.attempts.map((attempt) => ({
        id: attempt.id,
        reason: attempt.reason,
        startedAt: attempt.started_at,
        finishedAt: attempt.finished_at ?? null,
        outcome: attempt.outcome
          ? {
              kind: attempt.outcome.kind,
              code: attempt.outcome.code ?? undefined,
              detail: attempt.outcome.detail ?? undefined,
              metrics: attempt.outcome.metrics
                ? {
                    bytesReceived: attempt.outcome.metrics.bytes_received,
                    networkBytes: attempt.outcome.metrics.network_bytes,
                    durationMs: attempt.outcome.metrics.duration_ms,
                  }
                : undefined,
            }
          : null,
      })),
    }
  }

  async createJob(input: {
    sourceUrl: string
    rootId: string
    relativeDirectory?: string | null
    filenameOverride?: string | null
    conflictPolicy?: ConflictPolicy
  }): Promise<JobView> {
    const body: CreateJobRequest = {
      source_url: input.sourceUrl,
      root_id: input.rootId,
      relative_directory: input.relativeDirectory ?? null,
      filename_override: input.filenameOverride ?? null,
      conflict_policy: input.conflictPolicy ?? undefined,
    }
    const dto = await this.mutate<JobViewDto>('POST', '/api/v1/jobs', body)
    return fromDto(dto)
  }

  async pauseJob(id: string, expectedControlVersion: number): Promise<JobView> {
    const dto = await this.mutate<JobViewDto>('POST', `/api/v1/jobs/${id}/pause`, {
      expected_control_version: expectedControlVersion,
    })
    return fromDto(dto)
  }

  async resumeJob(id: string, expectedControlVersion: number): Promise<JobView> {
    const dto = await this.mutate<JobViewDto>('POST', `/api/v1/jobs/${id}/resume`, {
      expected_control_version: expectedControlVersion,
    })
    return fromDto(dto)
  }

  async cancelJob(
    id: string,
    expectedControlVersion: number,
    artifactPolicy: ArtifactPolicy,
  ): Promise<JobView> {
    const dto = await this.mutate<JobViewDto>('POST', `/api/v1/jobs/${id}/cancel`, {
      expected_control_version: expectedControlVersion,
      artifact_policy: artifactPolicy,
    })
    return fromDto(dto)
  }

  async retryJob(id: string, expectedControlVersion: number): Promise<JobView> {
    const dto = await this.mutate<JobViewDto>('POST', `/api/v1/jobs/${id}/retry`, {
      expected_control_version: expectedControlVersion,
    })
    return fromDto(dto)
  }

  async revealJob(id: string): Promise<void> {
    await this.mutate<void>('POST', `/api/v1/jobs/${id}/reveal`, {})
  }

  async removeJob(id: string): Promise<void> {
    await this.mutate<void>('DELETE', `/api/v1/jobs/${id}`)
  }

  async listRoots(): Promise<RootDto[]> {
    return this.request<RootDto[]>('/api/v1/roots', {})
  }

  async createRoot(input: {
    label: string
    absolutePath: string
    makeDefault: boolean
  }): Promise<RootDto> {
    return this.mutate<RootDto>('POST', '/api/v1/roots', {
      label: input.label,
      absolute_path: input.absolutePath,
      make_default: input.makeDefault,
    })
  }

  async patchRoot(
    id: string,
    patch: { label?: string; enabled?: boolean; makeDefault?: boolean },
  ): Promise<RootDto> {
    return this.mutate<RootDto>('PATCH', `/api/v1/roots/${id}`, {
      label: patch.label,
      enabled: patch.enabled,
      make_default: patch.makeDefault,
    })
  }

  async getSettings(): Promise<AppSettingsDto> {
    return this.request<AppSettingsDto>('/api/v1/settings', {})
  }

  async updateSettings(input: {
    activeConcurrency: number
    rateLimitBytesPerSecond: number | null
    defaultRootId: string | null
    notificationsEnabled: boolean
    startupMode: string
  }): Promise<AppSettingsDto> {
    return this.mutate<AppSettingsDto>('PUT', '/api/v1/settings', {
      active_concurrency: input.activeConcurrency,
      rate_limit_bytes_per_second: input.rateLimitBytesPerSecond,
      default_root_id: input.defaultRootId,
      notifications_enabled: input.notificationsEnabled,
      startup_mode: input.startupMode,
    })
  }
}

async function errorEnvelope(response: Response): Promise<ApiErrorEnvelopeDto> {
  try {
    return (await response.json()) as ApiErrorEnvelopeDto
  } catch {
    return {
      code: 'unexpected_response',
      message: 'the service returned an unexpected error',
      retryable: false,
    }
  }
}
