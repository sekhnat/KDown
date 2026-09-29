import { describe, expect, it } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { AppShell } from './AppShell'

function renderShell() {
  return render(
    <MemoryRouter initialEntries={['/downloads']}>
      <AppShell />
    </MemoryRouter>,
  )
}

describe('AppShell', () => {
  it('names the primary landmarks and navigation targets', () => {
    renderShell()
    const primary = screen.getByRole('navigation', { name: /primary/i })
    expect(primary).toBeVisible()
    for (const name of ['Downloads', 'History', 'Settings']) {
      expect(within(primary).getByRole('link', { name })).toBeVisible()
    }
    expect(screen.getByRole('main')).toBeVisible()
  })

  it('exposes a skip link and named mobile navigation', () => {
    renderShell()
    expect(screen.getByRole('link', { name: /skip to main content/i })).toBeVisible()
    const mobile = screen.getByRole('navigation', { name: /mobile/i })
    for (const name of ['Downloads', 'History', 'Settings']) {
      expect(within(mobile).getByRole('link', { name })).toBeInTheDocument()
    }
  })

  it('shows the connection status region', () => {
    renderShell()
    expect(screen.getByRole('status')).toBeInTheDocument()
  })
})
