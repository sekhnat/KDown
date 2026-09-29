import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { cleanup, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { renderApp } from '../../test/renderApp'
import { jobDto, server } from '../../test/msw'

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }))
afterEach(() => {
  cleanup()
  server.resetHandlers()
})
afterAll(() => server.close())

const seenRequests: URL[] = []
function lastRequest(): URL {
  return seenRequests[seenRequests.length - 1]
}

function trackRequests() {
  server.use(
    http.get('*/api/v1/jobs', ({ request }) => {
      seenRequests.push(new URL(request.url))
      const url = new URL(request.url)
      const cursor = url.searchParams.get('cursor')
      if (!cursor) {
        return HttpResponse.json({
          jobs: [jobDto('job-1'), jobDto('job-2')],
          next_cursor: 'next-2',
        })
      }
      return HttpResponse.json({
        jobs: [jobDto('job-3')],
        next_cursor: null,
      })
    }),
  )
}

function renderHistory(search = '') {
  return renderApp(`/history${search}`)
}

describe('HistoryPage', () => {
  it('keeps stable filters while loading the next history cursor', async () => {
    const user = userEvent.setup()
    trackRequests()
    renderHistory('?status=failed&source=iso')

    await screen.findByText('job-1.iso')
    await user.click(screen.getByRole('button', { name: /load more/i }))

    expect(lastRequest().searchParams.get('status')).toBe('failed')
    expect(lastRequest().searchParams.get('source')).toBe('iso')
    expect(lastRequest().searchParams.get('cursor')).toBe('next-2')
    expect(await screen.findByText('job-3.iso')).toBeVisible()
  })

  it('shows durable error summaries for failed jobs', async () => {
    server.use(
      http.get('*/api/v1/jobs', () =>
        HttpResponse.json({
          jobs: [
            {
              ...jobDto('job-failed'),
              status: 'Failed',
              attempts: [],
            },
          ],
        }),
      ),
      http.get('*/api/v1/jobs/:id', () =>
        HttpResponse.json({
          job: { ...jobDto('job-failed'), status: 'Failed' },
          attempts: [
            {
              id: 'a1',
              reason: 'initial',
              started_at: 0,
              finished_at: 1,
              outcome: {
                kind: 'failed',
                code: 'transfer_failed',
                detail: 'The connection was reset while downloading.',
              },
            },
          ],
        }),
      ),
    )
    renderHistory('?status=failed')
    await screen.findByText('job-failed.iso')
    const row = screen.getByRole('button', { name: /failure detail/i })
    await userEvent.click(row)
    expect(await screen.findByText(/connection was reset/i)).toBeVisible()
  })

  it('keeps the rendered history window bounded for ten thousand rows', async () => {
    const many = Array.from({ length: 10_000 }, (_, index) => jobDto(`job-${index}`))
    server.use(
      http.get('*/api/v1/jobs', () =>
        HttpResponse.json({ jobs: many, next_cursor: null }),
      ),
    )
    renderHistory()
    await screen.findByText('job-0.iso')
    const rows = document.querySelectorAll('[data-history-row]').length
    expect(rows).toBeLessThanOrEqual(80)
    expect(rows).toBeGreaterThan(0)
  })
})
