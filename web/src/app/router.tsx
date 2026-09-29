import { createBrowserRouter, Navigate, Outlet } from 'react-router-dom'
import { AppShell } from './AppShell'
import { DownloadsPage, HistoryPage, JobDetailRoute } from './pages'
import { SettingsPage } from '../features/settings/SettingsPage'
import { FirstRunSetup } from '../features/settings/FirstRunSetup'
import { useBootstrapQuery } from '../features/settings/rootQueries'

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
          { path: 'downloads/:jobId', element: <JobDetailRoute /> },
          { path: 'history', element: <HistoryPage /> },
          { path: 'settings', element: <SettingsPage /> },
        ],
      },
    ],
  },
])
