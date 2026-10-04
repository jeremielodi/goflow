import { defineConfig, devices } from '@playwright/test';

// Runs against the real dockerized frontend (nginx-proxied, port 3000) by
// default — this is deliberate, not incidental: the whole point of these
// tests is to catch bugs that only exist in the nginx/Vite proxy layer
// (e.g. a missing proxied prefix), which scripted API-level tests that hit
// the backend directly on :8080 can never see. Override with BASE_URL to
// point at the Vite dev server instead.
export default defineConfig({
  testDir: './e2e',
  timeout: 30_000,
  fullyParallel: false,
  retries: 0,
  reporter: 'list',
  use: {
    baseURL: process.env.BASE_URL ?? 'http://localhost:3000',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
  ],
});
