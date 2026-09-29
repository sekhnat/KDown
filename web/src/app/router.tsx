import { createBrowserRouter, Navigate, Outlet, useParams } from 'react-router-dom'
import { AppShell } from './AppShell'
import { DownloadsPage, HistoryPage, SettingsPage } from './pages'
import { FirstRunSetup } from '../features/settings/FirstRunSetup'
import { useBootstrapQuery } from '../features/settings/rootQueries'

function JobDetailStub() {
  const { jobId } = useParams()
  return (
    <section aria-label="Job detail">
      <h1>
        Job <span data-testid="job-id">{jobId}</span>
      </h1>
      <p>Detail telemetry arrives with the dashboard task.</p>
    </section>
  )
}

/** Blocks every app route until at least one root exists. */
function FirstRunGate() {
  const bootstrap = useBootstrapQuery()
  if (bootstrap.isPending) {
    return <p role="status">Starting…</p>
  }
  if (bootstrap.isError) {
    return <p role="alert">The service is unreachable. Retry by reopening KDown.</p>
  }
  if (bootstrap.data.roots.length === 0) {
    return <FirstRunSetup suggestedPath={bootstrap.data.suggestedDownloadRoot} />
  }
  return <Outlet />
}

export const router = createBrowserRouter([
  {
    path: '/',
    element: <AppShell />,
    children: [
      {
        element: <FirstRunGate />,
        children: [
          { index: true, element: <Navigate to="/downloads" replace /> },
          { path: 'downloads', element: <DownloadsPage /> },
          { path: 'downloads/:jobId', element: <JobDetailStub /> },
          { path: 'history', element: <HistoryPage /> },
          { path: 'settings', element: <SettingsPage /> },
        ],
      },
    ],
  },
])
