/**
 * Suite 09 — History API  (BPMN: 09_order_fulfillment.bpmn)
 *
 * Tests:
 *   A) GET /engine-rest/history/process-instance  — lists historic instances
 *   B) GET /engine-rest/history/process-instance/:id  — single instance details
 *   C) GET /engine-rest/history/activity-instance  — lists activity (execution) history
 *   D) Filter by processDefinitionKey
 *   E) Filter by state=completed
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '09_order_fulfillment.bpmn');
const client = new GoFlowClient();
const BASE = 'http://localhost:8080';

async function apiGet(token: string, path: string): Promise<any> {
  const r = await fetch(`${BASE}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!r.ok) throw new Error(`GET ${path} → ${r.status}: ${await r.text()}`);
  return r.json();
}

async function completeExternalTask(token: string, topic: string, instanceId: string): Promise<void> {
  // poll once with a short timeout
  const r = await fetch(`${BASE}/engine-rest/external-task/fetchAndLock`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ workerId: 'test-worker', maxJobsToActivate: 10, topics: [{ topicName: topic, lockDuration: 30000 }] }),
  });
  const tasks = await r.json() as any[];
  for (const t of tasks) {
    if (!instanceId || t.processInstanceId === instanceId) {
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

  await client.deployBpmn(BPMN, 'Order Fulfillment History Test');

  await runSuite({
    name: '09 — History API',
    tests: [

      // ── A: list historic process instances ─────────────────────────────────
      {
        name: '[A] GET /engine-rest/history/process-instance returns array',
        async fn() {
          const inst = await client.startProcess('orderFulfillment', { orderId: 'ORD-001' });
          const instanceId = inst.processInstanceId ?? inst.id;

          // Complete all service tasks
          for (const topic of ['validate-order', 'process-payment', 'ship-order']) {
            await sleep(300);
            await completeExternalTask(token, topic, instanceId);
          }
          await client.waitForProcessEnd(instanceId, 10000);

          const history = await apiGet(token, '/engine-rest/history/process-instance');
          assert(Array.isArray(history), 'expected array from history endpoint');
          assert(history.length > 0, 'expected at least one historic process instance');
          const found = history.find((h: any) => h.id === instanceId);
          assert(found !== undefined, `instance ${instanceId} not found in history`);
        },
      },

      // ── B: single instance ────────────────────────────────────────────────
      {
        name: '[B] GET /engine-rest/history/process-instance/:id returns instance details',
        async fn() {
          const inst = await client.startProcess('orderFulfillment', { orderId: 'ORD-002' });
          const instanceId = inst.processInstanceId ?? inst.id;

          for (const topic of ['validate-order', 'process-payment', 'ship-order']) {
            await sleep(300);
            await completeExternalTask(token, topic, instanceId);
          }
          await client.waitForProcessEnd(instanceId, 10000);

          const detail = await apiGet(token, `/engine-rest/history/process-instance/${instanceId}`);
          assert(detail.id === instanceId, `expected id ${instanceId}, got ${detail.id}`);
          assert(detail.state === 'completed', `expected state=completed, got ${detail.state}`);
          assert(detail.processDefinitionKey === 'orderFulfillment', `expected key orderFulfillment, got ${detail.processDefinitionKey}`);
          assert(detail.startTime != null, 'expected startTime');
          assert(detail.endTime != null, 'expected endTime for completed instance');
          assert(detail.durationInMillis != null, 'expected durationInMillis');
        },
      },

      // ── C: activity instance history ──────────────────────────────────────
      {
        name: '[C] GET /engine-rest/history/activity-instance returns executions for instance',
        async fn() {
          const inst = await client.startProcess('orderFulfillment', { orderId: 'ORD-003' });
          const instanceId = inst.processInstanceId ?? inst.id;

          for (const topic of ['validate-order', 'process-payment', 'ship-order']) {
            await sleep(300);
            await completeExternalTask(token, topic, instanceId);
          }
          await client.waitForProcessEnd(instanceId, 10000);

          const activities = await apiGet(
            token,
            `/engine-rest/history/activity-instance?processInstanceId=${instanceId}`
          );
          assert(Array.isArray(activities), 'expected array');
          assert(activities.length > 0, 'expected at least one activity instance');
          assert(
            activities.every((a: any) => a.processInstanceId === instanceId),
            'all activity instances should belong to the requested process instance'
          );
        },
      },

      // ── D: filter by processDefinitionKey ─────────────────────────────────
      {
        name: '[D] Filter history by processDefinitionKey',
        async fn() {
          const history = await apiGet(
            token,
            '/engine-rest/history/process-instance?processDefinitionKey=orderFulfillment'
          );
          assert(Array.isArray(history), 'expected array');
          assert(history.length >= 1, 'expected at least one result');
          assert(
            history.every((h: any) => h.processDefinitionKey === 'orderFulfillment'),
            'all results should have processDefinitionKey=orderFulfillment'
          );
        },
      },

      // ── E: filter by state=completed ──────────────────────────────────────
      {
        name: '[E] Filter history by state=completed',
        async fn() {
          const history = await apiGet(
            token,
            '/engine-rest/history/process-instance?state=completed'
          );
          assert(Array.isArray(history), 'expected array');
          assert(history.length >= 1, 'expected at least one completed instance');
          assert(
            history.every((h: any) => h.state === 'completed'),
            'all results should have state=completed'
          );
        },
      },

    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
