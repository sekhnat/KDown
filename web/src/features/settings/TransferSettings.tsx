/** Transfer settings inputs; values are owned by the settings page. */
export interface TransferValues {
  activeConcurrency: number
  rateLimitBytesPerSecond: number | null
}

export interface TransferSettingsProps {
  values: TransferValues
  onChange(values: TransferValues): void
}

import { useState } from 'react'

export function TransferSettings({ values, onChange }: TransferSettingsProps) {
  const [concurrencyText, setConcurrencyText] = useState(String(values.activeConcurrency))
  const [rateText, setRateText] = useState(
    values.rateLimitBytesPerSecond === null ? '' : String(values.rateLimitBytesPerSecond),
  )

  function commit() {
    onChange({
      activeConcurrency: Math.max(1, Number(concurrencyText) || 1),
      rateLimitBytesPerSecond: rateText === '' ? null : Math.max(1, Number(rateText)),
    })
  }

  return (
    <div style={{ display: 'grid', gap: '0.5rem', maxWidth: '24rem' }}>
      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.15rem' }}>
        <label htmlFor="setting-concurrency">Active downloads</label>
        <input
          id="setting-concurrency"
          type="number"
          min={1}
          value={concurrencyText}
          onChange={(event) => setConcurrencyText(event.target.value)}
          onBlur={commit}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.15rem' }}>
        <label htmlFor="setting-rate">Rate limit (bytes per second, empty for unlimited)</label>
        <input
          id="setting-rate"
          type="number"
          min={1}
          value={rateText}
          onChange={(event) => setRateText(event.target.value)}
          onBlur={commit}
        />
      </div>
    </div>
  )
}
