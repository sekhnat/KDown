import { useEffect, useState } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { RouterProvider } from 'react-router-dom'
import { router } from './router'
import { startAppLiveSync } from '../api/liveSyncApp'
import { LivePhaseContext } from './AppShell'
import type { LivePhase } from '../api/liveSync'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // The SSE stream carries live updates; refetch on focus would only
      // add noise. Stale data stays visible while disconnected.
      refetchOnWindowFocus: false,
      retry: 1,
      staleTime: 5_000,
    },
  },
})

function AppInner() {
  const [livePhase, setLivePhase] = useState<LivePhase>('connecting')

  useEffect(() => {
    const live = startAppLiveSync(queryClient, setLivePhase)
    return () => live.close()
  }, [])

  return (
    <LivePhaseContext.Provider value={livePhase}>
      <RouterProvider router={router} />
    </LivePhaseContext.Provider>
  )
}

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <AppInner />
    </QueryClientProvider>
  )
}
