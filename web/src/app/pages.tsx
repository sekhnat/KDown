import { useEffect, useContext, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api/apiClient'
import { queryKeys } from '../api/queryKeys'
import { DashboardPage } from '../features/downloads/DashboardPage'
import { JobDetailPage } from '../features/downloads/JobDetailPage'
import { LivePhaseContext } from './AppShell'
import { useParams } from 'react-router-dom'

export function DownloadsPage() {
  const livePhase = useContext(LivePhaseContext)
  const jobsQuery = useQuery({
    queryKey: queryKeys.jobs.list(),
    queryFn: () => api.listJobs({ limit: 100 }),
  })
  useTerminalNotifications(livePhase, jobsQuery.data?.jobs)
  if (jobsQuery.isPending) {
    return <p role="status">Loading downloads…</p>
  }
  if (jobsQuery.isError) {
    return <p role="alert">The download list is unavailable. It will retry automatically.</p>
  }
  return <DashboardPage jobs={jobsQuery.data.jobs} livePhase={livePhase} />
}

/** Foreground completion/failure notifications: only while connected and
 * only when the user granted permission. */
function useTerminalNotifications(
  livePhase: string,
  jobs: { id: string; status: string; sourceDisplay: string }[] | undefined,
) {
  const seen = useRef<Set<string>>(new Set())
  useEffect(() => {
    if (!jobs || livePhase !== 'live') {
      return
    }
    for (const job of jobs) {
      if (seen.current.has(job.id)) {
        continue
      }
      seen.current.add(job.id)
      if (
        typeof window.Notification !== 'undefined' &&
        window.Notification.permission === 'granted' &&
        (job.status === 'Completed' || job.status === 'Failed')
      ) {
        const notification = new window.Notification(
          job.status === 'Completed' ? 'Download completed' : 'Download failed',
          { body: job.sourceDisplay },
        )
        notification.onclick = () => window.focus()
      }
    }
  }, [jobs, livePhase])
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

export { HistoryPage } from '../features/history/HistoryPage'

export function SettingsPage() {
  return (
    <section aria-labelledby="settings-heading">
      <h1 id="settings-heading">Settings</h1>
      <p>Settings arrive with the settings task.</p>
    </section>
  )
}
