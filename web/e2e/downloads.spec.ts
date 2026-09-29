import { test, expect, createDownload, configureRoot } from './support/fixtures'

test('completes a small download, records history, and keeps the file', async ({
  page,
  processes,
}) => {
  await page.goto(processes.kdownUrl)
  await configureRoot(page, processes.downloadRoot)
  await createDownload(page, `${processes.fixtureUrl}/small.bin`, 'small-done.bin')

  await expect(page.getByRole('region', { name: 'Completed' })).toBeVisible({ timeout: 60_000 })
  await processes.expectDownloadedSha256('small-done.bin')

  await page.getByRole('navigation', { name: 'Primary' }).getByRole('link', { name: 'History' }).click()
  await expect(page.getByText(/small-done\.bin/i)).toBeVisible()
})

test('download, pause, restart service, recover, and complete', async ({
  page,
  processes,
}) => {
  test.setTimeout(240_000)
  await page.goto(processes.kdownUrl)
  await configureRoot(page, processes.downloadRoot)
  await createDownload(page, `${processes.slowFixtureUrl}/throttled.bin`)

  await expect(page.getByRole('region', { name: 'Running' })).toBeVisible()
  await page.getByRole('button', { name: 'Pause' }).click()
  await expect(page.getByRole('region', { name: 'Paused' })).toBeVisible()
  await page.getByRole('button', { name: 'Resume' }).click()
  await expect(page.getByRole('region', { name: 'Running' })).toBeVisible()

  await processes.restartApp(async () => {
    await expect(page.getByRole('banner').getByText(/reconnecting/i)).toBeVisible()
  })
  await expect(page.getByRole('region', { name: 'Running' })).toBeVisible()
  await expect(page.getByRole('region', { name: 'Completed' })).toBeVisible({ timeout: 120_000 })
  await processes.expectDownloadedSha256('throttled.bin', 'slow')
})

test('cancelling preserves partial data by default and can delete it', async ({
  page,
  processes,
}) => {
  await page.goto(processes.kdownUrl)
  await configureRoot(page, processes.downloadRoot)
  await createDownload(page, `${processes.slowFixtureUrl}/cancel.bin`, 'cancel.bin')
  await expect(page.getByRole('region', { name: 'Running' })).toBeVisible()

  await page.getByRole('button', { name: 'Cancel' }).click()
  await page.getByRole('radio', { name: /keep partial/i }).check()
  await page.getByRole('button', { name: /cancel download/i }).click()
  await expect(page.getByRole('region', { name: 'Cancelled' })).toBeVisible()
  await expect
    .poll(() => processes.listDownloadRoot().some((name) => name.includes('cancel.bin')), {
      timeout: 15_000,
    })
    .toBe(true)

  await page.getByRole('navigation', { name: 'Primary' }).getByRole('link', { name: 'Downloads' }).click()
  await createDownload(page, `${processes.slowFixtureUrl}/cancel2.bin`, 'cancel2.bin')
  await expect(page.getByRole('region', { name: 'Running' })).toBeVisible()
  await page.getByRole('button', { name: 'Cancel' }).click()
  await page.getByRole('radio', { name: /delete partial/i }).check()
  await page.getByRole('button', { name: /cancel download/i }).click()
  await expect(page.getByRole('region', { name: 'Cancelled' })).toBeVisible()
  await expect
    .poll(() => processes.listDownloadRoot().some((name) => name.includes('cancel2.bin')), {
      timeout: 15_000,
    })
    .toBe(false)
})

test('rejects destinations outside the configured roots', async ({ page, processes }) => {
  await page.goto(processes.kdownUrl)
  await configureRoot(page, processes.downloadRoot)
  await page.getByRole('button', { name: /new download/i }).click()
  const dialog = page.getByRole('dialog')
  await dialog.getByLabel(/url/i).fill(`${processes.fixtureUrl}/escape.bin`)
  await dialog.getByLabel(/subfolder/i).fill('../escape')
  await dialog.getByRole('button', { name: /start download/i }).click()
  await expect(dialog.getByText(/inside the configured download root|destination/i)).toBeVisible()
  expect(processes.listDownloadRoot().some((name) => name === 'escape')).toBe(false)
})

test('typed failures stay inspectable and retryable', async ({ page, processes }) => {
  test.setTimeout(180_000)
  await page.goto(processes.kdownUrl)
  await configureRoot(page, processes.downloadRoot)

  const failingUrl = await processes.failingFixtureUrl()
  await createDownload(page, `${failingUrl}/doomed.bin`, 'doomed.bin')
  await expect(page.getByRole('region', { name: 'Failed' })).toBeVisible({ timeout: 90_000 })
  await expect(page.getByRole('button', { name: 'Retry' })).toBeVisible()
  await page.getByRole('button', { name: 'Show technical detail' }).click()
  await expect(page.locator('pre')).toContainText(/transfer_failed|failed|http/i)
})

test('reloading the browser during active work keeps the job visible', async ({
  page,
  processes,
}) => {
  await page.goto(processes.kdownUrl)
  await configureRoot(page, processes.downloadRoot)
  await createDownload(page, `${processes.slowFixtureUrl}/reload.bin`, 'reload.bin')
  await expect(page.getByRole('region', { name: 'Running' })).toBeVisible()
  await page.reload()
  await expect(page.getByRole('region', { name: 'Running' })).toBeVisible({ timeout: 30_000 })
})

test('removing completed history keeps the downloaded file', async ({ page, processes }) => {
  await page.goto(processes.kdownUrl)
  await configureRoot(page, processes.downloadRoot)
  await createDownload(page, `${processes.fixtureUrl}/remove.bin`, 'remove.bin')
  await expect(page.getByRole('region', { name: 'Completed' })).toBeVisible({ timeout: 60_000 })
  expect(processes.listDownloadRoot().includes('remove.bin')).toBe(true)

  await page.getByRole('button', { name: /remove from history/i }).click()
  await expect(page.getByText(/downloaded file remains/i)).toBeVisible()
  await page
    .getByRole('alertdialog')
    .getByRole('button', { name: /remove from history/i })
    .click()
  await expect(page.getByText('Completed', { exact: true })).toBeHidden()
  expect(processes.listDownloadRoot().includes('remove.bin')).toBe(true)
})
