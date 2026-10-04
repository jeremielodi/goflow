import { test, expect } from '@playwright/test';

// Real end-to-end coverage, driving an actual Chromium browser against the
// dockerized frontend (nginx on :3000, see playwright.config.ts). This is
// deliberately NOT an API-level test — the bug this suite exists to catch
// (nginx/Vite's reverse-proxy config silently missing the `/roles` and
// `/actions` prefixes, plus `/users` requiring a trailing path segment) is
// invisible to any test that talks to the Go backend directly on :8080.

test.describe('Admin — create user and create role via the real UI', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/login');
    await page.getByPlaceholder('admin@example.com').fill('admin@goflow.com');
    await page.getByPlaceholder('••••••••').fill('admin123');
    await page.getByRole('button', { name: /sign in/i }).click();
    await expect(page).toHaveURL('/');
  });

  test('New User button creates a user that appears in the list', async ({ page }) => {
    const email = `pw-user-${Date.now()}@goflow.com`;

    await page.getByRole('link', { name: 'Users' }).click();
    await expect(page).toHaveURL(/\/admin\/users/);

    await page.getByRole('button', { name: /new user/i }).click();
    await page.getByTestId('new-user-email').fill(email);
    await page.getByTestId('new-user-password').fill('password123');
    await page.getByRole('button', { name: 'Create', exact: true }).click();

    // Modal closes and the new row shows up — this round-trip only
    // succeeds if POST /users actually reached the backend through nginx.
    await expect(page.getByText(email)).toBeVisible({ timeout: 10_000 });
  });

  test('New Role button creates a role that appears in the list', async ({ page }) => {
    const label = `pw-role-${Date.now()}`;

    await page.getByRole('link', { name: 'Roles' }).click();
    await expect(page).toHaveURL(/\/admin\/roles/);

    await page.getByRole('button', { name: /new role/i }).click();
    await page.getByTestId('new-role-label').fill(label);
    await page.getByRole('button', { name: 'Create', exact: true }).click();

    await expect(page.getByText(label)).toBeVisible({ timeout: 10_000 });
  });
});
