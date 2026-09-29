import { useQuery } from '@tanstack/react-query'
import { api } from '../api/apiClient'
import { queryKeys } from '../api/queryKeys'
import { DashboardPage } from '../features/downloads/DashboardPage'
import { JobDetailPage } from '../features/downloads/JobDetailPage'
import { LivePhaseContext } from './AppShell'
import { useContext } from 'react'
import { useParams } from 'react-router-dom'

export function DownloadsPage() {
  const livePhase = useContext(LivePhaseContext)
  const jobsQuery = useQuery({
    queryKey: queryKeys.jobs.list(),
    queryFn: () => api.listJobs({ limit: 100 }),
  })
  if (jobsQuery.isPending) {
    return <p role="status">Loading downloads…</p>
  }
  if (jobsQuery.isError) {
    return <p role="alert">The download list is unavailable. It will retry automatically.</p>
  }
  return <DashboardPage jobs={jobsQuery.data.jobs} livePhase={livePhase} />
}

export function JobDetailRoute() {
  const livePhase = useContext(LivePhaseContext)
  const { jobId } = useParams()
  const detailQuery = useQuery({
    queryKey: queryKeys.jobs.detail(jobId ?? ''),
    queryFn: () => api.getJob(jobId ?? ''),
    enabled: !!jobId,
  })
  if (detailQuery.isPending) {
    return <p role="status">Loading job…</p>
  }
  if (detailQuery.isError) {
    return <p role="alert">This job could not be loaded.</p>
  }
  return <JobDetailPage detail={detailQuery.data} livePhase={livePhase} />
}

export function HistoryPage() {
  return (
    <section aria-labelledby="history-heading">
      <h1 id="history-heading">History</h1>
      <p>History arrives with the history task.</p>
    </section>
  )
}

export function SettingsPage() {
  return (
    <section aria-labelledby="settings-heading">
      <h1 id="settings-heading">Settings</h1>
      <p>Settings arrive with the settings task.</p>
    </section>
  )
}
