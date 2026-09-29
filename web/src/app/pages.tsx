import { Link } from 'react-router-dom'

/** Route placeholders; Tasks 10–12 replace these with real surfaces. */
export function DownloadsPage() {
  return (
    <section aria-labelledby="downloads-heading">
      <h1 id="downloads-heading">Downloads</h1>
      <p>The dashboard arrives with the download flows.</p>
      <Link to="/downloads/new">New download</Link>
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
