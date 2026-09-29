import { useLayoutEffect, useRef } from 'react'
import { useVirtualizer } from '@tanstack/react-virtual'
import type { ReactNode } from 'react'

export interface VirtualJobListProps {
  count: number
  estimateSize: number
  renderItem(index: number): ReactNode
}

/**
 * Windowed rendering for large history lists: ten thousand records never
 * become ten thousand DOM rows. `initialRect` keeps jsdom/test rendering
 * deterministic.
 */
export function VirtualJobList({ count, estimateSize, renderItem }: VirtualJobListProps) {
  const parentRef = useRef<HTMLDivElement>(null)
  // eslint-disable-next-line react-hooks/incompatible-library -- the option object shape is stable here
  const rowVirtualizer = useVirtualizer({
    count,
    getScrollElement: () => parentRef.current,
    estimateSize: () => estimateSize,
    overscan: 8,
    // The scroll viewport is fixed at 600px by design (see the container
    // style); a static rect observer keeps jsdom deterministic too.
    observeElementRect: (_instance, cb) => cb({ width: 800, height: 600 }),
  })

  useLayoutEffect(() => {
    rowVirtualizer.measure()
  }, [rowVirtualizer])

  const items = rowVirtualizer.getVirtualItems()
  return (
    <div ref={parentRef} style={{ height: 600, overflowY: 'auto' }} data-testid="history-scroll">
      <div style={{ height: rowVirtualizer.getTotalSize(), position: 'relative' }}>
        {items.map((virtualRow) => (
          <div
            key={virtualRow.key}
            data-history-row=""
            style={{
              position: 'absolute',
              top: 0,
              left: 0,
              width: '100%',
              height: virtualRow.size,
              transform: `translateY(${virtualRow.start}px)`,
            }}
          >
            {renderItem(virtualRow.index)}
          </div>
        ))}
      </div>
    </div>
  )
}
