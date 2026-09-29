import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Development proxies the API and SSE stream to the Rust host so the
// browser talks to one origin even while Vite serves the modules.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8734',
        changeOrigin: false,
        ws: false,
      },
    },
  },
  build: {
    // Content-hashed output names are what the bundled host caches as
    // immutable; index.html stays no-cache.
    assetsInlineLimit: 0,
  },
})
