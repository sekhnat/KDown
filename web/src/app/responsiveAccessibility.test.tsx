import { describe, expect, it } from 'vitest'
import { readFileSync } from 'node:fs'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { AppShell } from '../app/AppShell'
import { JobCard } from '../features/downloads/JobCard'
import type { JobView } from '../api/liveSync'

function renderShell() {
  return render(
    <MemoryRouter initialEntries={['/downloads']}>
      <AppShell />
    </MemoryRouter>,
  )
}

describe('Responsive and accessibility structure', () => {
  it('keeps a skip link, named landmarks, and visible focus styling', () => {
    renderShell()
    expect(screen.getByRole('link', { name: /skip to main content/i })).toBeVisible()
    expect(screen.getByRole('navigation', { name: /primary/i })).toBeVisible()
    expect(screen.getByRole('navigation', { name: /mobile/i })).toBeVisible()
    const main = screen.getByRole('main')
    expect(main).toBeVisible()
    expect(main).toHaveAttribute('id', 'main-content')
  })

  it('defines reduced-motion, focus-visible, and narrow touch-target rules', () => {
    const css = `${readFileSync('src/styles/global.css', 'utf8')}\n${readFileSync('src/styles/tokens.css', 'utf8')}`
    expect(css).toMatch(/prefers-reduced-motion: reduce/)
    expect(css).toMatch(/:focus-visible/)
    expect(css).toMatch(/min-height: 44px/)
    expect(css).toMatch(/@media \(max-width: 639px\)/)
    expect(css).toMatch(/@media \(max-width: 1023px\)/)
  })

  it('exposes progressbar state for indeterminate and determinate values', () => {
    const job: JobView = {
      id: 'job-1',
      status: 'Active',
      desiredState: 'Running',
      controlVersion: 1,
      attemptId: 'a1',
      sampleSeq: 0,
      sourceDisplay: 'https://example.test/file.iso?…',
      rootId: 'root-1',
      rootLabel: 'Downloads',
      relativeDirectory: null,
      filenameOverride: null,
      destinationDisplay: null,
      createdAt: 0,
      updatedAt: 0,
      snapshot: { stateLabel: 'Running', bytesReceived: 0, networkBytes: 0, reusedBytes: 0, retries: 0, elapsedMs: 0 },
    }
    render(
      <MemoryRouter>
        <JobCard job={job} livePhase="live" onCancelRequested={() => undefined} />
      </MemoryRouter>,
    )
    const progress = screen.getByRole('progressbar')
    expect(progress.getAttribute('aria-valuetext')).toBe('indeterminate')
  })
})
