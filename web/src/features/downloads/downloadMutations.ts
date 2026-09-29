/** Job mutation hooks shared by the drawer and lifecycle controls. */
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { api, type ArtifactPolicy, type ConflictPolicy } from '../../api/apiClient'
import { queryKeys } from '../../api/queryKeys'

export interface CreateJobInput {
  sourceUrl: string
  rootId: string
  relativeDirectory?: string | null
  filenameOverride?: string | null
  conflictPolicy?: ConflictPolicy
}

export function useCreateJobMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: CreateJobInput) => api.createJob(input),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.jobs.lists() })
    },
  })
}

export function usePauseJobMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, expectedControlVersion }: { id: string; expectedControlVersion: number }) =>
      api.pauseJob(id, expectedControlVersion),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.jobs.all })
    },
  })
}

export function useResumeJobMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, expectedControlVersion }: { id: string; expectedControlVersion: number }) =>
      api.resumeJob(id, expectedControlVersion),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.jobs.all })
    },
  })
}

export function useCancelJobMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      id,
      expectedControlVersion,
      artifactPolicy,
    }: {
      id: string
      expectedControlVersion: number
      artifactPolicy: ArtifactPolicy
    }) => api.cancelJob(id, expectedControlVersion, artifactPolicy),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.jobs.all })
    },
  })
}

export function useRetryJobMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, expectedControlVersion }: { id: string; expectedControlVersion: number }) =>
      api.retryJob(id, expectedControlVersion),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.jobs.all })
    },
  })
}

export function useRemoveJobMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => api.removeJob(id),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.jobs.all })
    },
  })
}

export function useRevealJobMutation() {
  return useMutation({
    mutationFn: (id: string) => api.revealJob(id),
  })
}
