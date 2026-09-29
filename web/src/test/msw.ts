/**
 * MSW handlers covering the API surface the UI exercises in tests. Each
 * test can override with `server.use(...)` for error or edge cases.
 */
import { http, HttpResponse, delay } from 'msw'
import { setupServer } from 'msw/node'

export const oneRootBootstrap = {
  csrf_token: 'test-csrf-token',
  origin: 'http://127.0.0.1:8734',
  build: '0.1.0',
  stream_epoch: 'epoch-test',
  suggested_download_root: '/home/user/Downloads',
  roots: [{ id: 'root-1', label: 'Downloads', enabled: true, is_default: true }],
}

export const noRootsBootstrap = {
  csrf_token: 'test-csrf-token',
  origin: 'http://127.0.0.1:8734',
  build: '0.1.0',
  stream_epoch: 'epoch-test',
  suggested_download_root: '/home/user/Downloads',
  roots: [],
}

export const handlers = [
  http.get('*/api/v1/bootstrap', () => HttpResponse.json(oneRootBootstrap)),
  http.get('*/api/v1/roots', () =>
    HttpResponse.json([
      {
        id: 'root-1',
        label: 'Downloads',
        canonical_path: '/home/user/Downloads',
        enabled: true,
        is_default: true,
        created_at: 0,
        updated_at: 0,
      },
    ]),
  ),
  http.post('*/api/v1/roots', async () => {
    await delay(0)
    return HttpResponse.json(
      {
        id: 'root-new',
        label: 'Downloads',
        canonical_path: '/home/user/Downloads',
        enabled: true,
        is_default: true,
        created_at: 0,
        updated_at: 0,
      },
      { status: 201 },
    )
  }),
  http.get('*/api/v1/settings', () =>
    HttpResponse.json({
      active_concurrency: 3,
      rate_limit_bytes_per_second: null,
      default_root_id: 'root-1',
      notifications_enabled: false,
      startup_mode: 'manual',
    }),
  ),
  http.get('*/api/v1/jobs', () =>
    HttpResponse.json({
      jobs: [
        {
          id: 'job-1',
          status: 'Active',
          desired_state: 'Running',
          control_version: 2,
          attempt_id: 'a1',
          sample_seq: 3,
          source_display: 'https://example.test/file.iso?…',
          root_id: 'root-1',
          root_label: 'Downloads',
          relative_directory: 'isos',
          filename_override: null,
          destination_display: null,
          created_at: 0,
          updated_at: 0,
          snapshot: {
            state_label: 'Running',
            bytes_received: 1024,
            network_bytes: 1100,
            reused_bytes: 0,
            retries: 0,
            elapsed_ms: 5000,
          },
        },
      ],
    }),
  ),
  http.post('*/api/v1/jobs', async () => {
    await delay(0)
    return HttpResponse.json(jobDto('job-new'), { status: 201 })
  }),
  http.get('*/api/v1/jobs/:id', ({ params }) =>
    HttpResponse.json({
      job: jobDto(String(params.id)),
      attempts: [],
    }),
  ),
]

export function jobDto(id: string) {
  return {
    id,
    status: 'Queued',
    desired_state: 'Running',
    control_version: 1,
    attempt_id: null,
    sample_seq: 0,
    source_display: 'https://example.test/file.iso?…',
    root_id: 'root-1',
    root_label: 'Downloads',
    relative_directory: null,
    filename_override: null,
    destination_display: null,
    created_at: 0,
    updated_at: 0,
    snapshot: null,
  }
}

/** Error envelope with field errors for validation scenarios. */
export function fieldErrorEnvelope(field: string, message: string) {
  return {
    code: 'source_scheme_unsupported',
    message,
    retryable: false,
    field_errors: { [field]: message },
  }
}

export const server = setupServer(...handlers)
