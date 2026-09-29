import { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { api, ApiError } from '../../api/apiClient'
import { queryKeys } from '../../api/queryKeys'
import { RootSettings } from './RootSettings'
import { TransferSettings, type TransferValues } from './TransferSettings'
import { NotificationSettings } from './NotificationSettings'

/**
 * Application settings: root administration (the only surface with
 * canonical paths), transfer limits, notification preference, and
 * startup guidance.
 */
export function SettingsPage() {
  const settingsQuery = useQuery({
    queryKey: queryKeys.settings.all,
    queryFn: () => api.getSettings(),
  })
  const queryClient = useQueryClient()
  const [pending, setPending] = useState<TransferValues | null>(null)
  const [saveError, setSaveError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const settings = settingsQuery.data
  const values: TransferValues | null =
    pending ?? (settings
      ? {
          activeConcurrency: settings.active_concurrency,
          rateLimitBytesPerSecond: settings.rate_limit_bytes_per_second ?? null,
        }
      : null)

  async function save() {
    if (!values) {
      return
    }
    setBusy(true)
    setSaveError(null)
    try {
      await api.updateSettings({
        activeConcurrency: values.activeConcurrency,
        rateLimitBytesPerSecond: values.rateLimitBytesPerSecond,
        defaultRootId: settings?.default_root_id ?? null,
        notificationsEnabled: settings?.notifications_enabled ?? false,
        startupMode: settings?.startup_mode ?? 'manual',
      })
      await queryClient.invalidateQueries({ queryKey: queryKeys.settings.all })
    } catch (error) {
      setSaveError(error instanceof ApiError ? error.message : 'The settings could not be saved.')
    } finally {
      setBusy(false)
    }
  }

  async function enableNotifications() {
    // Permission requests are click-bound: this runs only from the button.
    setBusy(true)
    setSaveError(null)
    try {
      const permission = await window.Notification.requestPermission()
      await api.updateSettings({
        activeConcurrency: values?.activeConcurrency ?? settings?.active_concurrency ?? 1,
        rateLimitBytesPerSecond: values?.rateLimitBytesPerSecond ?? null,
        defaultRootId: settings?.default_root_id ?? null,
        notificationsEnabled: permission === 'granted',
        startupMode: settings?.startup_mode ?? 'manual',
      })
      await queryClient.invalidateQueries({ queryKey: queryKeys.settings.all })
    } catch (error) {
      setSaveError(error instanceof ApiError ? error.message : 'The preference could not be saved.')
    } finally {
      setBusy(false)
    }
  }

  async function disableNotifications() {
    setBusy(true)
    setSaveError(null)
    try {
      await api.updateSettings({
        activeConcurrency: values?.activeConcurrency ?? settings?.active_concurrency ?? 1,
        rateLimitBytesPerSecond: values?.rateLimitBytesPerSecond ?? null,
        defaultRootId: settings?.default_root_id ?? null,
        notificationsEnabled: false,
        startupMode: settings?.startup_mode ?? 'manual',
      })
      await queryClient.invalidateQueries({ queryKey: queryKeys.settings.all })
    } catch (error) {
      setSaveError(error instanceof ApiError ? error.message : 'The preference could not be saved.')
    } finally {
      setBusy(false)
    }
  }

  if (settingsQuery.isPending) {
    return (
      <section aria-labelledby="settings-heading">
        <h1 id="settings-heading">Settings</h1>
        <p role="status">Loading settings…</p>
      </section>
    )
  }
  if (settingsQuery.isError || !settings) {
    return (
      <section aria-labelledby="settings-heading">
        <h1 id="settings-heading">Settings</h1>
        <p role="alert">Settings are unavailable. They will retry automatically.</p>
      </section>
    )
  }

  return (
    <section aria-labelledby="settings-heading">
      <h1 id="settings-heading">Settings</h1>
      {saveError ? (
        <p role="alert" style={{ color: 'var(--danger)' }}>
          {saveError}
        </p>
      ) : null}
      <RootSettings />
      <section aria-labelledby="transfer-heading" style={{ marginTop: '2rem' }}>
        <h2 id="transfer-heading">Transfers</h2>
        {values ? (
          <>
            <TransferSettings values={values} onChange={setPending} />
            <button type="button" onClick={() => void save()} disabled={busy}>
              {busy ? 'Saving…' : 'Save settings'}
            </button>
          </>
        ) : null}
        <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>
          Lowering the active-download limit never cancels running downloads; it only delays
          the next launch until a slot frees up.
        </p>
      </section>
      <section aria-labelledby="notifications-heading" style={{ marginTop: '2rem' }}>
        <h2 id="notifications-heading">Notifications</h2>
        <NotificationSettings
          enabled={settings.notifications_enabled}
          busy={busy}
          onEnable={() => void enableNotifications()}
          onDisable={() => void disableNotifications()}
        />
      </section>
      <section aria-labelledby="startup-heading" style={{ marginTop: '2rem' }}>
        <h2 id="startup-heading">Startup</h2>
        <p>
          {settings.startup_mode === 'service'
            ? 'KDown is managed by a service unit; browser opening is disabled there.'
            : 'KDown was started manually. Use the systemd user unit to start it with the session.'}
        </p>
      </section>
    </section>
  )
}
