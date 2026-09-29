import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { DashboardPage } from './DashboardPage'
import { server } from '../../test/msw'
import { renderApp } from '../../test/renderApp'

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }))
afterEach(() => server.resetHandlers())
afterAll(() => server.close())

describe('DashboardPage (routed)', () => {
  it('shows aggregate counts and job cards from the collection', async () => {
    renderApp('/downloads')
    expect(await screen.findByText(/1 active/i)).toBeVisible()
    const cards = await screen.findAllByRole('article')
    expect(cards.length).toBeGreaterThan(0)
    expect(within(cards[0]).getByText(/file\.iso/i)).toBeVisible()
  })

  it('renders an em dash when the ETA is unavailable', async () => {
    renderApp('/downloads')
    const card = await screen.findByRole('article')
    expect(within(card).getAllByText('—').length).toBeGreaterThan(0)
  })
})

describe('DashboardPage (unit)', () => {
  it('renders the empty state without jobs', () => {
    render(<DashboardPage jobs={[]} livePhase="live" />)
    expect(screen.getByText(/no active downloads/i)).toBeVisible()
  })
})
