import { NewDownloadDrawer } from '../features/downloads/NewDownloadDrawer'

/** Route placeholders; Tasks 10–12 replace these with real surfaces. */
export function DownloadsPage() {
  return (
    <section aria-labelledby="downloads-heading">
      <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
        <h1 id="downloads-heading" style={{ marginRight: 'auto' }}>
          Downloads
        </h1>
        <NewDownloadDrawer />
      </div>
      <p>The dashboard arrives with the download flows.</p>
    </section>
  )
}

export function HistoryPage() {
  return (
    <section aria-labelledby="history-heading">
      <h1 id="history-heading">History</h1>
      <p>History arrives with the settings and history tasks.</p>
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
