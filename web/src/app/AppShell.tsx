import { Component, type ErrorInfo, type ReactNode, createContext, useContext } from 'react'
import { NavLink, Outlet } from 'react-router-dom'
import type { LivePhase } from '../api/liveSync'

export const LivePhaseContext = createContext<LivePhase>('connecting')

const primaryLinks = [
  { to: '/downloads', label: 'Downloads' },
  { to: '/history', label: 'History' },
  { to: '/settings', label: 'Settings' },
]

const phaseLabel: Record<LivePhase, string> = {
  connecting: 'Connecting…',
  syncing: 'Syncing…',
  live: 'Live',
  stale: 'Reconnecting…',
}

function NavLinks({ labelled }: { labelled: boolean }) {
  return (
    <>
      {primaryLinks.map((link) => (
        <NavLink key={link.to} to={link.to} className="touch-target">
          <span aria-hidden="true" data-icon={link.label} />
          <span className={labelled ? 'nav-label' : 'bottom-nav-label'}>{link.label}</span>
        </NavLink>
      ))}
    </>
  )
}

class ErrorBoundary extends Component<{ children: ReactNode }, { error: Error | null }> {
  state = { error: null as Error | null }

  static getDerivedStateFromError(error: Error) {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // Never log job payloads or URLs here.
    console.error('Unhandled UI error', error.name, info.componentStack ? 'with stack' : '')
  }

  render() {
    if (this.state.error) {
      return (
        <div role="alert" style={{ padding: '2rem' }}>
          <h1>Something went wrong</h1>
          <p>The interface hit an unexpected error. Your downloads are unaffected.</p>
          <button type="button" onClick={() => this.setState({ error: null })}>
            Try again
          </button>
        </div>
      )
    }
    return this.props.children
  }
}

/**
 * The Ocean Precision application shell: skip link, persistent or compact
 * rail, bottom navigation on narrow layouts, connection status, and a
 * global error boundary around the routed content.
 */
export function AppShell() {
  const phase = useContext(LivePhaseContext)
  return (
    <div className="shell">
      <a className="skip-link" href="#main-content">
        Skip to main content
      </a>
      <nav aria-label="Primary" className="rail">
        <NavLinks labelled />
      </nav>
      <div>
        <header
          style={{
            display: 'flex',
            alignItems: 'center',
            padding: '0.75rem 1.5rem',
            borderBottom: '1px solid var(--border)',
          }}
        >
          <strong>KDown</strong>
          <span className="connection-status" role="status" data-phase={phase}>
            {phaseLabel[phase]}
          </span>
        </header>
        <main id="main-content" className="content">
          <ErrorBoundary>
            <Outlet />
          </ErrorBoundary>
        </main>
      </div>
      <nav aria-label="Mobile" className="rail mobile-rail">
        <NavLinks labelled={false} />
      </nav>
    </div>
  )
}
