import { test, expect, configureRoot } from './support/fixtures'
import { AxeBuilder } from '@axe-core/playwright'

test('dashboard and settings pass the accessibility audit', async ({ page, processes }) => {
  await page.goto(processes.kdownUrl)
  await configureRoot(page, processes.downloadRoot)

  const dashboard = await new AxeBuilder({ page }).analyze()
  const seriousDashboard = dashboard.violations.filter(
    (violation) => violation.impact === 'serious' || violation.impact === 'critical',
  )
  expect(seriousDashboard).toEqual([])

  await page.getByRole('navigation', { name: 'Primary' }).getByRole('link', { name: 'Settings' }).click()
  await expect(page.getByRole('heading', { name: /settings/i })).toBeVisible()
  const settings = await new AxeBuilder({ page }).analyze()
  const seriousSettings = settings.violations.filter(
    (violation) => violation.impact === 'serious' || violation.impact === 'critical',
  )
  expect(seriousSettings).toEqual([])
})

test('narrow layout supports keyboard operation', async ({ page, processes }) => {
  await page.setViewportSize({ width: 480, height: 800 })
  await page.goto(processes.kdownUrl)
  await configureRoot(page, processes.downloadRoot)

  await page.keyboard.press('Tab')
  await expect(page.getByRole('link', { name: /skip to main content/i })).toBeFocused()
  // The narrow layout keeps the primary rail in the DOM (stacked by CSS);
  // keyboard order continues through it.
  await page.keyboard.press('Tab')
  await page.keyboard.press('Enter')
  await expect(page).toHaveURL(/downloads/)
})
