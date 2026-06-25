/**
 * Suite 11 — Signal Events  (BPMN: 11_inventory_broadcast.bpmn)
 * Source: https://camunda.com/bpmn/reference/ — Signal Events
 *
 * Tests:
 *   A) Single process parked at signal catch event, resumed by broadcast
 *   B) Multiple instances all receive the same broadcast signal
 *   C) Unknown signal returns empty count (no error)
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '11_inventory_broadcast.bpmn');
const client = new GoFlowClient();
const BASE = 'http://localhost:8080';

async function broadcastSignal(
  token: string,
  name: string,
  variables: Record<string, unknown> = {}
): Promise<any> {
  const r = await fetch(`${BASE}/engine-rest/signal`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ name, variables }),
  });
  if (!r.ok) throw new Error(`POST /engine-rest/signal → ${r.status}: ${await r.text()}`);
  return r.json();
}

async function completeExternalTask(token: string, topic: string, instanceId: string): Promise<void> {
  // Complete ALL tasks for this topic (locking them all then completing only the right one
  // would prevent others from being processed — complete all to avoid deadlock).
  const r = await fetch(`${BASE}/engine-rest/external-task/fetchAndLock`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ workerId: 'test-worker', maxJobsToActivate: 20, topics: [{ topicName: topic, lockDuration: 10000 }] }),
  });
  const tasks = await r.json() as any[];
  for (const t of tasks) {
    await fetch(`${BASE}/engine-rest/external-task/${t.id}/complete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ workerId: 'test-worker', variables: {} }),
    });
  }
}

async function completeExternalTaskForInstance(token: string, topic: string, instanceId: string): Promise<void> {
  const r = await fetch(`${BASE}/engine-rest/external-task/fetchAndLock`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ workerId: 'test-worker', maxJobsToActivate: 1, topics: [{ topicName: topic, lockDuration: 10000 }] }),
  });
  const tasks = await r.json() as any[];
  for (const t of tasks) {
    if (t.processInstanceId === instanceId) {
      await fetch(`${BASE}/engine-rest/external-task/${t.id}/complete`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ workerId: 'test-worker', variables: {} }),
      });
    }
  }
}

async function run() {
  await client.loginAsSuperUser();
  const token = (client as any).token as string;

  await client.deployBpmn(BPMN, 'Inventory Broadcast Test');

  await runSuite({
    name: '11 — Signal Events',
    tests: [

      // ── A: single instance receives signal ────────────────────────────────
      {
        name: '[A] Process parked at signal catch, resumed by RestockAlert broadcast',
        async fn() {
          const inst = await client.startProcess('inventoryAlert', { product: 'widget-A' });
          const instanceId = inst.processInstanceId ?? inst.id;

          // complete check-inventory service task
          await sleep(500);
          await completeExternalTask(token, 'check-inventory', instanceId);
          await sleep(500);

          // Process should now be waiting at waitForRestock signal
          const result = await broadcastSignal(token, 'RestockAlert', { restockedProduct: 'widget-A' });
          assert(result.count >= 1, `expected count >= 1, got ${result.count}`);

          // Complete update-inventory
          await sleep(500);
          await completeExternalTask(token, 'update-inventory', instanceId);
          await client.waitForProcessEnd(instanceId, 10000);

          const final = await client.getProcessInstance(instanceId);
          assert(!final || final.status === 'completed', `expected completed, got ${final?.status}`);
        },
      },

      // ── B: broadcast reaches multiple instances ───────────────────────────
      {
        name: '[B] Broadcast signal resumes ALL waiting instances',
        async fn() {
          // Start 3 instances, all waiting for RestockAlert
          const ids: string[] = [];
          for (let i = 0; i < 3; i++) {
            const inst = await client.startProcess('inventoryAlert', { product: `widget-B${i}` });
            ids.push(inst.processInstanceId ?? inst.id);
          }

          // Complete ALL check-inventory tasks in bulk (avoids lock contention)
          await sleep(600);
          await completeExternalTask(token, 'check-inventory', '');
          await sleep(800); // let them all park at signal

          // Broadcast once — all 3 should be resumed
          const result = await broadcastSignal(token, 'RestockAlert');
          assert(result.count >= 3, `expected count >= 3, got ${result.count}`);

          // Complete all update-inventory tasks in bulk
          await sleep(600);
          await completeExternalTask(token, 'update-inventory', '');
          for (const id of ids) {
            await client.waitForProcessEnd(id, 10000);
          }
        },
      },

      // ── C: unknown signal returns 0 count (no error) ─────────────────────
      {
        name: '[C] Broadcasting unknown signal returns count=0 without error',
        async fn() {
          const result = await broadcastSignal(token, 'NonExistentSignal_' + Date.now());
          assert(result.count === 0, `expected count=0, got ${result.count}`);
        },
      },

    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
