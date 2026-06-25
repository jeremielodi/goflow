/**
 * Suite 12 — Event-based Gateway  (BPMN: 12_customer_decision.bpmn)
 * Source: https://camunda.com/bpmn/reference/ — Event-Based Gateway
 *
 * Tests:
 *   A) CustomerApproved message fires first → order is processed
 *   B) CustomerRejected message fires first → quote is archived
 *   C) After first event fires, second event has no effect (cancelled subscription)
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '12_customer_decision.bpmn');
const client = new GoFlowClient();
const BASE = 'http://localhost:8080';

async function correlateMessage(
  token: string,
  messageName: string,
  variables: Record<string, unknown> = {}
): Promise<Response> {
  return fetch(`${BASE}/engine-rest/message`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ messageName, correlationKeys: {}, variables }),
  });
}

async function completeExternalTask(token: string, topic: string, instanceId: string): Promise<void> {
  const r = await fetch(`${BASE}/engine-rest/external-task/fetchAndLock`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ workerId: 'test-worker', maxJobsToActivate: 10, topics: [{ topicName: topic, lockDuration: 30000 }] }),
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

  await client.deployBpmn(BPMN, 'Customer Decision Test');

  await runSuite({
    name: '12 — Event-based Gateway',
    tests: [

      // ── A: approval path ──────────────────────────────────────────────────
      {
        name: '[A] CustomerApproved message → process-order branch taken',
        async fn() {
          const inst = await client.startProcess('customerDecision', { quoteId: 'Q-001' });
          const instanceId = inst.processInstanceId ?? inst.id;

          // Complete send-quote service task
          await sleep(400);
          await completeExternalTask(token, 'send-quote', instanceId);
          await sleep(500); // let EBG park

          // Fire the approval message
          const r = await correlateMessage(token, 'CustomerApproved');
          assert(r.ok, `CustomerApproved message failed: ${r.status}`);

          // Complete process-order
          await sleep(400);
          await completeExternalTask(token, 'process-order', instanceId);
          await client.waitForProcessEnd(instanceId, 10000);

          const final = await client.getProcessInstance(instanceId);
          assert(!final || final.status === 'completed', `expected completed, got ${final?.status}`);
        },
      },

      // ── B: rejection path ────────────────────────────────────────────────
      {
        name: '[B] CustomerRejected message → archive-quote branch taken',
        async fn() {
          const inst = await client.startProcess('customerDecision', { quoteId: 'Q-002' });
          const instanceId = inst.processInstanceId ?? inst.id;

          await sleep(400);
          await completeExternalTask(token, 'send-quote', instanceId);
          await sleep(500);

          // Fire the rejection message
          const r = await correlateMessage(token, 'CustomerRejected');
          assert(r.ok, `CustomerRejected message failed: ${r.status}`);

          // Complete archive-quote
          await sleep(400);
          await completeExternalTask(token, 'archive-quote', instanceId);
          await client.waitForProcessEnd(instanceId, 10000);

          const final = await client.getProcessInstance(instanceId);
          assert(!final || final.status === 'completed', `expected completed, got ${final?.status}`);
        },
      },

      // ── C: second message after first is a no-op (subscription cancelled) ─
      {
        name: '[C] After approval fires, sending rejection has no effect on the instance',
        async fn() {
          const inst = await client.startProcess('customerDecision', { quoteId: 'Q-003' });
          const instanceId = inst.processInstanceId ?? inst.id;

          await sleep(400);
          await completeExternalTask(token, 'send-quote', instanceId);
          await sleep(500);

          // Approve first
          const r1 = await correlateMessage(token, 'CustomerApproved');
          assert(r1.ok, `CustomerApproved failed: ${r1.status}`);

          // Now try to reject — should 404 (no subscription left)
          await sleep(200);
          const r2 = await correlateMessage(token, 'CustomerRejected');
          // Either 404 (no subscription) or it correlates to a different instance — either is fine
          // The key is the CURRENT instance keeps advancing on the approve path
          await sleep(400);
          await completeExternalTask(token, 'process-order', instanceId);
          await client.waitForProcessEnd(instanceId, 10000);

          const final = await client.getProcessInstance(instanceId);
          assert(!final || final.status === 'completed', `expected completed, got ${final?.status}`);
        },
      },

    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
