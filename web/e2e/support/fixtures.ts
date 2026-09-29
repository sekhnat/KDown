/**
 * Test-scoped `processes` fixture: every scenario gets isolated state,
 * downloads, and child processes, all stopped afterwards.
 */
import { test as base, expect } from '@playwright/test'
import type { Page } from '@playwright/test'
import { startTestProcesses, type TestProcesses } from './processes'

export const test = base.extend<{ processes: TestProcesses }>({
  processes: [
    // Playwright requires fixture dependencies to use object destructuring.
    // eslint-disable-next-line no-empty-pattern
    async ({}, run) => {
      const processes = await startTestProcesses()
      try {
        await run(processes)
      } finally {
        await processes.stop()
      }
    },
    { timeout: 180_000 },
  ],
})

export { expect }

/** Fills the new-download drawer and submits. */
export async function createDownload(page: Page, url: string, filename?: string) {
  await page.getByRole('button', { name: /new download/i }).click()
  const dialog = page.getByRole('dialog')
  await dialog.getByLabel(/url/i).fill(url)
  if (filename) {
    await dialog.getByLabel(/filename/i).fill(filename)
  }
  await dialog.getByRole('button', { name: /start download/i }).click()
}

/** First-run setup: confirms the suggested root (or the test's own). */
export async function configureRoot(page: Page, suggested: string) {
  const heading = page.getByRole('heading', { name: /choose a download folder/i })
  if (await heading.isVisible()) {
    const input = page.getByLabel('Download folder')
    await input.fill(suggested)
    await page.getByRole('button', { name: /use this folder/i }).click()
  }
  await expect(heading).toBeHidden()
}
