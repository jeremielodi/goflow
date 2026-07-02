import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 3000,
    proxy: {
      '/engine-rest': { target: 'http://localhost:8080', changeOrigin: true },
      '/auth':        { target: 'http://localhost:8080', changeOrigin: true },
      '/users':       { target: 'http://localhost:8080', changeOrigin: true },
      '/events':      { target: 'http://localhost:8080', changeOrigin: true },
      '/v2':          { target: 'http://localhost:8080', changeOrigin: true },
      '/ws':          { target: 'ws://localhost:8080',   changeOrigin: true, ws: true },
    },
  },
})
