import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { RouterProvider } from 'react-router-dom'
import { router } from './router'

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

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  )
}
