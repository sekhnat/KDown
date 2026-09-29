import { Component, type ErrorInfo, type ReactNode, createContext, useContext } from 'react'
import { NavLink, Outlet } from 'react-router-dom'
import { Download, History, Settings, type LucideIcon } from 'lucide-react'
import type { LivePhase } from '../api/liveSync'

export const LivePhaseContext = createContext<LivePhase>('connecting')

const primaryLinks: { to: string; label: string; icon: LucideIcon }[] = [
  { to: '/downloads', label: 'Downloads', icon: Download },
  { to: '/history', label: 'History', icon: History },
  { to: '/settings', label: 'Settings', icon: Settings },
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
          <link.icon aria-hidden="true" size={20} strokeWidth={1.75} />
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
      <nav aria-label="Primary" className="rail desktop-rail">
        <div className="mark" aria-hidden="true">
          K<span>Down</span>
        </div>
        <NavLinks labelled />
      </nav>
      <div className="content-wrap">
        <header className="topbar">
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
