import { useMemo } from 'react'

/**
 * A bounded effective-rate sparkline. The parent keeps at most 120
 * samples; the polyline renders them left to right.
 */
export function RateSparkline({ samples }: { samples: number[] }) {
  const points = useMemo(() => {
    if (samples.length < 2) {
      return null
    }
    const max = Math.max(...samples, 1)
    const width = 240
    const height = 48
    return samples
      .map((value, index) => {
        const x = (index / (samples.length - 1)) * width
        const y = height - (value / max) * (height - 4) - 2
        return `${x.toFixed(1)},${y.toFixed(1)}`
      })
      .join(' ')
  }, [samples])

  if (points === null) {
    return (
      <svg width={240} height={48} role="img" aria-label="Rate history: collecting samples" />
    )
  }
  return (
    <svg width={240} height={48} role="img" aria-label="Effective rate history">
      <polyline points={points} fill="none" stroke="var(--data)" strokeWidth="2" />
    </svg>
  )
}
