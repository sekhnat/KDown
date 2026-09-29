import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { renderApp } from '../../test/renderApp'
import { fieldErrorEnvelope, jobDto, server } from '../../test/msw'

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }))
afterEach(() => server.resetHandlers())
afterAll(() => server.close())

function renderNewDownload() {
  return renderApp('/downloads', 'live')
}

describe('NewDownloadDrawer', () => {
  it('maps server field errors and keeps the drawer open', async () => {
    const user = userEvent.setup()
    server.use(
      http.post('*/api/v1/jobs', () =>
        HttpResponse.json(
          fieldErrorEnvelope('source_url', 'Only HTTP and HTTPS URLs are supported.'),
          { status: 422 },
        ),
      ),
    )
    renderNewDownload()
    await user.click(await screen.findByRole('button', { name: /new download/i }))
    await user.type(await screen.findByLabelText(/url/i), 'file:///tmp/a')
    await user.click(screen.getByRole('button', { name: /start download/i }))
    expect(await screen.findByText(/only http and https/i)).toBeVisible()
    expect(screen.getByRole('dialog')).toBeVisible()
  })

  it('submits the selected root id and relative directory', async () => {
    const user = userEvent.setup()
    let captured: Record<string, unknown> | null = null
    server.use(
      http.post('*/api/v1/jobs', async (request) => {
        captured = (await request.request.json()) as Record<string, unknown>
        return HttpResponse.json(jobDto('job-created'), { status: 201 })
      }),
    )
    renderNewDownload()
    await user.click(await screen.findByRole('button', { name: /new download/i }))
    await user.type(screen.getByLabelText(/url/i), 'https://example.test/file.iso')
    await user.type(screen.getByLabelText(/subfolder/i), 'isos')
    await user.click(screen.getByRole('button', { name: /start download/i }))

    await waitFor(() => expect(captured).not.toBeNull())
    expect(captured).toMatchObject({
      root_id: 'root-1',
      relative_directory: 'isos',
      source_url: 'https://example.test/file.iso',
    })
  })

  it('navigates to the created job after a successful creation', async () => {
    const user = userEvent.setup()
    renderNewDownload()
    await user.click(await screen.findByRole('button', { name: /new download/i }))
    const dialog = await screen.findByRole('dialog')
    await user.type(screen.getByLabelText(/url/i), 'https://example.test/file.iso')
    await user.click(screen.getByRole('button', { name: /start download/i }))

    await waitFor(() => expect(dialog).not.toBeVisible())
    // The real detail page renders the created job's display source.
    expect(await screen.findByText(/file\.iso/i)).toBeVisible()
  })

  it('returns focus to the New Download button after Escape', async () => {
    const user = userEvent.setup()
    renderNewDownload()
    const trigger = await screen.findByRole('button', { name: /new download/i })
    await user.click(trigger)
    const dialog = await screen.findByRole('dialog')
    expect(dialog).toBeVisible()
    await user.keyboard('{Escape}')
    await waitFor(() => expect(dialog).not.toBeVisible())
    await waitFor(() => expect(trigger).toHaveFocus())
  })

  it('confirms before discarding a dirty form on Escape', async () => {
    const user = userEvent.setup()
    renderNewDownload()
    await user.click(await screen.findByRole('button', { name: /new download/i }))
    await user.type(await screen.findByLabelText(/url/i), 'https://example.test/file.iso')
    await user.keyboard('{Escape}')

    const confirmation = await screen.findByRole('alertdialog')
    expect(
      within(confirmation).getByRole('heading', { name: /discard/i }),
    ).toBeVisible()
    await user.click(within(confirmation).getByRole('button', { name: /keep editing/i }))
    expect(screen.getByRole('dialog')).toBeVisible()
  })
})
