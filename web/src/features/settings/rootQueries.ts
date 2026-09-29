/** Root and bootstrap query/mutation hooks. */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '../../api/apiClient'
import { queryKeys } from '../../api/queryKeys'

export interface BootstrapView {
  csrfToken: string
  build: string
  streamEpoch: string
  suggestedDownloadRoot: string | null
  roots: { id: string; label: string; enabled: boolean; isDefault: boolean }[]
}

export function useBootstrapQuery() {
  // No static staleTime: React Query never refetches static queries, and
  // first-run completion must invalidate and re-read the bootstrap.
  return useQuery({
    queryKey: queryKeys.bootstrap,
    queryFn: () => api.bootstrap(),
  })
}

export function useRootsQuery() {
  return useQuery({ queryKey: queryKeys.roots.all, queryFn: () => api.listRoots() })
}

export function useCreateRootMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: { label: string; absolutePath: string; makeDefault: boolean }) =>
      api.createRoot(input),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap })
      console.log('DEBUG createRoot onSuccess: invalidation done')
      await queryClient.invalidateQueries({ queryKey: queryKeys.roots.all })
      await queryClient.invalidateQueries({ queryKey: queryKeys.settings.all })
    },
  })
}
