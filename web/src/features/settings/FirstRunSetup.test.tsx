import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { cleanup, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { renderApp } from '../../test/renderApp'
import { noRootsBootstrap, server } from '../../test/msw'

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }))
afterEach(() => {
  cleanup()
  server.resetHandlers()
})
afterAll(() => server.close())

describe('FirstRunSetup', () => {
  it('blocks download routes until an allowed root is confirmed', async () => {
    server.use(
      http.get('*/api/v1/bootstrap', () => HttpResponse.json(noRootsBootstrap)),
    )
    renderApp('/downloads')
    expect(
      await screen.findByRole('heading', { name: /choose a download folder/i }),
    ).toBeVisible()
    expect(screen.queryByRole('button', { name: /new download/i })).not.toBeInTheDocument()
  })

  it('confirms the suggested path and makes it the default', async () => {
    const user = userEvent.setup()
    let rootAdded = false
    server.use(
      http.get('*/api/v1/bootstrap', () =>
        HttpResponse.json(
          rootAdded
            ? {
                ...noRootsBootstrap,
                roots: [
                  {
                    id: 'root-new',
                    label: 'Downloads',
                    enabled: true,
                    is_default: true,
                  },
                ],
              }
            : noRootsBootstrap,
        ),
      ),
      http.post('*/api/v1/roots', async (request) => {
        const body = (await request.request.json()) as Record<string, unknown>
        expect(body).toMatchObject({
          label: 'Downloads',
          absolute_path: '/home/user/Downloads',
          make_default: true,
        })
        rootAdded = true
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
    )
    renderApp('/downloads')

    await screen.findByRole('heading', { name: /choose a download folder/i })
    const input = screen.getByLabelText('Download folder')
    expect(input).toHaveValue('/home/user/Downloads')
    await user.click(screen.getByRole('button', { name: /use this folder/i }))

    expect(
      await screen.findByRole('heading', { name: /^downloads$/i }, { timeout: 4000 }),
    ).toBeVisible()
  })

  it('associates server validation with the path field', async () => {
    const user = userEvent.setup()
    server.use(
      http.get('*/api/v1/bootstrap', () => HttpResponse.json(noRootsBootstrap)),
      http.post('*/api/v1/roots', () =>
        HttpResponse.json(
          {
            code: 'root_unavailable',
            message: 'that folder does not exist',
            retryable: false,
            field_errors: { absolute_path: 'That folder does not exist.' },
          },
          { status: 422 },
        ),
      ),
    )
    renderApp('/downloads')

    await screen.findByRole('heading', { name: /choose a download folder/i })
    const input = screen.getByLabelText('Download folder')
    await user.clear(input)
    await user.type(input, '/definitely/missing')
    await user.click(screen.getByRole('button', { name: /use this folder/i }))

    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent(/that folder does not exist/i)
    expect(input).toHaveAccessibleDescription(/that folder does not exist/i)
    expect(input).toBeVisible()
  })
})
