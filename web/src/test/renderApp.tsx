/**
 * Renders the real application at a given path with a fresh query client
 * and the MSW-intercepted network. The shell's live phase is fixed by the
 * caller so stale/disabled states are testable without a real stream.
 */
import type { ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render } from '@testing-library/react'
import { createMemoryRouter, RouterProvider } from 'react-router-dom'
import { api } from '../api/apiClient'
import { router } from '../app/router'
import { LivePhaseContext } from '../app/AppShell'
import type { LivePhase } from '../api/liveSync'

/** Debug handle: the most recent render's query client. */
let lastQueryClient: QueryClient | null = null
export function getQueryClientForDebug(): QueryClient | null {
  return lastQueryClient
}

export function renderApp(path = '/downloads', livePhase: LivePhase = 'live') {
  // The ApiClient singleton caches bootstrap per process; tests need a
  // fresh handshake for every render.
  api.resetBootstrapCache()
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })

  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <LivePhaseContext.Provider value={livePhase}>{children}</LivePhaseContext.Provider>
      </QueryClientProvider>
    )
  }

  lastQueryClient = queryClient
  const memoryRouter = createMemoryRouter(router.routes, { initialEntries: [path] })
  return render(
    <Wrapper>
      <RouterProvider router={memoryRouter} />
    </Wrapper>,
  )
}
