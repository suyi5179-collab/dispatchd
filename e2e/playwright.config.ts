import { defineConfig } from '@playwright/test'

// Expects the compose stack and the Vite dev server on :5185;
// scripts/e2e.sh starts both.
export default defineConfig({
  testDir: '.',
  timeout: 120_000,
  workers: 1,
  reporter: [['list']],
  use: { baseURL: process.env.DISPATCHD_UI_URL || 'http://localhost:5185', viewport: { width: 1400, height: 900 }, trace: 'retain-on-failure' },
})
