import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/test-setup.ts'],
    css: false,
    // Playwright acceptance specs live in e2e/ and run under Playwright,
    // not vitest; the default include glob would sweep them in.
    exclude: ['e2e/**', 'node_modules/**', 'dist/**'],
  },
})
