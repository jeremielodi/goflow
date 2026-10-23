import { test, expect, type APIRequestContext } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

// The "Previous step" button on the instance page, through the real
// dockerized frontend: the preview (dry run) and the step back itself both
// go through nginx's /v2 proxy. The instance is driven to "approve" over
// the same proxy beforehand.

const BPMN = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..', 'examples', 'tests', 'bpmn', '53_step_back.bpmn');

async function api(request: APIRequestContext) {
  const login = await request.post('/auth/login', { data: { email: 'admin@goflow.com', password: 'admin123' } });
  const token = (await login.json()).access_token;
  return { Authorization: `Bearer ${token}` };
}

async function poll<T>(fn: () => Promise<T | undefined>): Promise<T> {
  for (let i = 0; i < 40; i++) {
    const v = await fn();
    if (v) return v;
    await new Promise(r => setTimeout(r, 250));
  }
  throw new Error('timed out');
}

test('Previous step sends the token from approve back to check', async ({ page, request }) => {
  const headers = await api(request);
  await request.post('/engine-rest/deployment/create', {
    headers,
    multipart: {
      resources: { name: '53_step_back.bpmn', mimeType: 'text/xml', buffer: fs.readFileSync(BPMN) },
      'deployment-name': 'Step Back E2E',
    },
  });
  const started = await (await request.post('/engine-rest/v2/process-definitions/StepBackProcess/start', {
    headers, data: { variables: {} },
  })).json();
  const id = started.processInstanceId ?? started.id;

  const openTask = (key: string) => poll(async () => {
    const tasks = await (await request.get('/engine-rest/tasks', { headers, params: { processInstanceId: id } })).json();
    return (tasks ?? []).find((t: any) => t.taskDefinitionKey === key && (t.status === 'created' || t.status === 'claimed'));
  });
  const fill = await openTask('fill');
  await request.post(`/engine-rest/tasks/${fill.id}/complete`, { headers, data: { variables: {} } });
  const job = await poll(async () => {
    const jobs = await (await request.post('/engine-rest/external-task/fetchAndLock', {
      headers,
      data: { workerId: 'e2e', maxTasks: 10, topics: [{ topicName: 'step_back_check', lockDuration: 30000 }] },
    })).json();
    return (jobs ?? []).find((j: any) => j.processInstanceId === id);
  });
  await request.post(`/engine-rest/external-task/${job.id}/complete`, { headers, data: { workerId: 'e2e', variables: {} } });
  await openTask('approve');

  await page.goto('/login');
  await page.getByPlaceholder('admin@example.com').fill('admin@goflow.com');
  await page.getByPlaceholder('••••••••').fill('admin123');
  await page.getByRole('button', { name: /sign in/i }).click();
  await expect(page).toHaveURL('/');

  await page.goto(`/instances/${id}`);
  await page.getByRole('button', { name: /previous step/i }).click();
  await expect(page.getByText('Check Request')).toBeVisible({ timeout: 10_000 });
  await page.getByRole('button', { name: 'Go back', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Go back', exact: true })).toBeHidden({ timeout: 10_000 });

  // The token is back at check: a new job, approve's task gone.
  await poll(async () => {
    const jobs = await (await request.post('/engine-rest/external-task/fetchAndLock', {
      headers,
      data: { workerId: 'e2e', maxTasks: 10, topics: [{ topicName: 'step_back_check', lockDuration: 30000 }] },
    })).json();
    return (jobs ?? []).find((j: any) => j.processInstanceId === id);
  });
  const tasks = await (await request.get('/engine-rest/tasks', { headers, params: { processInstanceId: id } })).json();
  expect((tasks ?? []).some((t: any) => t.taskDefinitionKey === 'approve' && t.status === 'created')).toBe(false);

  await request.delete(`/engine-rest/process-instance/${id}`, { headers });
});
