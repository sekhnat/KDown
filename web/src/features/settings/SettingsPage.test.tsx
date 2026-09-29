import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { cleanup, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { renderApp } from '../../test/renderApp'
import { server } from '../../test/msw'

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }))
afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
  server.resetHandlers()
})
afterAll(() => server.close())

let lastPut: Record<string, unknown> | null = null
beforeEach(() => {
  lastPut = null
  server.use(
    http.put('*/api/v1/settings', async ({ request }) => {
      lastPut = (await request.json()) as Record<string, unknown>
      return HttpResponse.json({
        active_concurrency: Number(lastPut.active_concurrency ?? 3),
        rate_limit_bytes_per_second: lastPut.rate_limit_bytes_per_second ?? null,
        default_root_id: 'root-1',
        notifications_enabled: Boolean(lastPut.notifications_enabled),
        startup_mode: 'manual',
      })
    }),
  )
})

describe('SettingsPage', () => {
  it('shows root administration with canonical paths', async () => {
    renderApp('/settings')
    expect(await screen.findByText('/home/user/Downloads')).toBeVisible()
    expect(screen.getByRole('table')).toBeVisible()
  })

  it('shows root-in-use conflict without removing the root', async () => {
    const user = userEvent.setup()
    server.use(
      http.patch('*/api/v1/roots/:id', () =>
        HttpResponse.json(
          {
            code: 'root_in_use',
            message: 'An active download uses this folder.',
            retryable: false,
          },
          { status: 409 },
        ),
      ),
    )
    renderApp('/settings')
    await user.click(await screen.findByRole('button', { name: /disable downloads/i }))
    expect(await screen.findByText(/active download uses this folder/i)).toBeVisible()
    expect(screen.getByText('/home/user/Downloads')).toBeVisible()
  })

  it('persists transfer limit changes', async () => {
    const user = userEvent.setup()
    renderApp('/settings')
    const concurrency = await screen.findByLabelText(/active downloads/i)
    await user.clear(concurrency)
    await user.type(concurrency, '5')
    await user.click(screen.getByRole('button', { name: /save settings/i }))
    await vi.waitFor(() => {
      expect(lastPut).toMatchObject({ active_concurrency: 5 })
    })
  })

  it('requests notification permission only from an explicit user action', async () => {
    const user = userEvent.setup()
    vi.stubGlobal(
      'Notification',
      { permission: 'default', requestPermission: vi.fn().mockResolvedValue('granted') },
    )
    const requestPermission = window.Notification.requestPermission as ReturnType<typeof vi.fn>
    renderApp('/settings')

    expect(requestPermission).not.toHaveBeenCalled()
    await user.click(await screen.findByRole('button', { name: /enable notifications/i }))
    expect(requestPermission).toHaveBeenCalledOnce()
    await vi.waitFor(() => {
      expect(lastPut).toMatchObject({ notifications_enabled: true })
    })
  })

  it('does not request permission when notifications are already enabled', async () => {
    server.use(
      http.get('*/api/v1/settings', () =>
        HttpResponse.json({
          active_concurrency: 3,
          rate_limit_bytes_per_second: null,
          default_root_id: 'root-1',
          notifications_enabled: true,
          startup_mode: 'manual',
        }),
      ),
    )
    vi.stubGlobal(
      'Notification',
      { permission: 'default', requestPermission: vi.fn().mockResolvedValue('granted') },
    )
    const requestPermission = window.Notification.requestPermission as ReturnType<typeof vi.fn>
    renderApp('/settings')
    await screen.findByRole('button', { name: /disable notifications/i })
    expect(requestPermission).not.toHaveBeenCalled()
  })
})
