/** History filter bar; filters live in the URL search parameters. */
import { useSearchParams } from 'react-router-dom'

const statuses = [
  { value: '', label: 'All' },
  { value: 'completed', label: 'Completed' },
  { value: 'failed', label: 'Failed' },
  { value: 'cancelled', label: 'Cancelled' },
]

export function HistoryFilters() {
  const [searchParams, setSearchParams] = useSearchParams()
  const status = searchParams.get('status') ?? ''
  const source = searchParams.get('source') ?? ''
  const from = searchParams.get('from') ?? ''
  const to = searchParams.get('to') ?? ''

  function update(next: Record<string, string>) {
    const params = new URLSearchParams(searchParams)
    for (const [key, value] of Object.entries(next)) {
      if (value) {
        params.set(key, value)
      } else {
        params.delete(key)
      }
    }
    params.delete('cursor')
    setSearchParams(params)
  }

  return (
    <form
      role="search"
      aria-label="History filters"
      style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap', alignItems: 'end' }}
      onSubmit={(event) => event.preventDefault()}
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.15rem' }}>
        <label htmlFor="history-status">Status</label>
        <select
          id="history-status"
          value={status}
          onChange={(event) => update({ status: event.target.value })}
        >
          {statuses.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.15rem' }}>
        <label htmlFor="history-source">Source contains</label>
        <input
          id="history-source"
          type="search"
          value={source}
          onChange={(event) => update({ source: event.target.value })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.15rem' }}>
        <label htmlFor="history-from">From (epoch ms)</label>
        <input
          id="history-from"
          type="number"
          value={from}
          onChange={(event) => update({ from: event.target.value })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.15rem' }}>
        <label htmlFor="history-to">To (epoch ms)</label>
        <input
          id="history-to"
          type="number"
          value={to}
          onChange={(event) => update({ to: event.target.value })}
        />
      </div>
    </form>
  )
}
