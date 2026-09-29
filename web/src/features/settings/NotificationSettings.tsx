/** Click-bound foreground notification preference. */
export interface NotificationSettingsProps {
  enabled: boolean
  onEnable(): void
  onDisable(): void
  busy: boolean
}

/**
 * The browser only allows permission requests from an explicit user
 * action; this surface never asks on load.
 */
export function NotificationSettings({ enabled, onEnable, onDisable, busy }: NotificationSettingsProps) {
  if (enabled) {
    return (
      <div style={{ display: 'grid', gap: '0.5rem' }}>
        <p>
          Desktop notifications are on. They appear for completions and failures while the
          interface is open.
        </p>
        <button type="button" onClick={onDisable} disabled={busy}>
          {busy ? 'Saving…' : 'Disable notifications'}
        </button>
      </div>
    )
  }
  return (
    <div style={{ display: 'grid', gap: '0.5rem' }}>
      <p>
        Get a desktop notification when a download completes or fails. KDown only asks the
        browser for permission when you press the button.
      </p>
      <button type="button" onClick={onEnable} disabled={busy}>
        {busy ? 'Saving…' : 'Enable notifications'}
      </button>
    </div>
  )
}
