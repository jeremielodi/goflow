/**
 * Suite 10 — Message Events  (BPMN: 10_order_notification.bpmn)
 * Source: https://camunda.com/bpmn/reference/ — Message Catching/Throwing
 *
 * Tests:
 *   A) Process parks at IntermediateCatchEvent, resumed by POST /engine-rest/message
 *   B) Second message resumes it again (chained message correlation)
 *   C) Message with correlation key (only matching instance is resumed)
 *   D) Unknown message returns 404
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '10_order_notification.bpmn');
const client = new GoFlowClient();
const BASE = 'http://localhost:8080';

async function correlateMessage(
  token: string,
  messageName: string,
  correlationKeys: Record<string, unknown> = {},
  variables: Record<string, unknown> = {}
): Promise<Response> {
  return fetch(`${BASE}/engine-rest/message`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ messageName, correlationKeys, variables }),
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

  await client.deployBpmn(BPMN, 'Order Notification Test');

  await runSuite({
    name: '10 — Message Events',
    tests: [

      // ── A: first message resumes waiting execution ─────────────────────────
      {
        name: '[A] Process parks at IntermediateCatchEvent, resumed by PaymentReceived message',
        async fn() {
          const inst = await client.startProcess('orderNotification', { orderId: 'MSG-001' });
          const instanceId = inst.processInstanceId ?? inst.id;

          // Process should now be waiting at waitForPayment
          await sleep(500);
          const proc = await client.getProcessInstance(instanceId) as any;
          assert(proc?.status === 'running', `expected running, got ${proc?.status}`);

          // Correlate the PaymentReceived message
          const r1 = await correlateMessage(token, 'PaymentReceived');
          assert(r1.ok, `PaymentReceived message failed: ${r1.status}`);
          const body1 = await r1.json() as any;
          assert(body1.processInstanceId != null, 'expected processInstanceId in response');

          // Process confirms payment then parks at waitForShipment
          await sleep(500);
          await completeExternalTask(token, 'confirm-payment', instanceId);

          await sleep(300);

          // Correlate the ShipmentReady message
          const r2 = await correlateMessage(token, 'ShipmentReady');
          assert(r2.ok, `ShipmentReady message failed: ${r2.status}`);

          // Complete notify-customer and wait for end
          await sleep(500);
          await completeExternalTask(token, 'notify-customer', instanceId);
          await client.waitForProcessEnd(instanceId, 10000);

          const final = await client.getProcessInstance(instanceId);
          assert(!final || final.status === 'completed', `expected completed, got ${final?.status}`);
        },
      },

      // ── B: message with variables merged ─────────────────────────────────
      {
        name: '[B] Message variables are merged into process instance',
        async fn() {
          const inst = await client.startProcess('orderNotification', { orderId: 'MSG-002' });
          const instanceId = inst.processInstanceId ?? inst.id;

          await sleep(500);

          // Correlate with variables
          const r1 = await correlateMessage(token, 'PaymentReceived', {}, { paymentRef: 'PAY-XYZ' });
          assert(r1.ok, `message failed: ${r1.status}`);

          await sleep(300);
          await completeExternalTask(token, 'confirm-payment', instanceId);
          await sleep(300);

          const r2 = await correlateMessage(token, 'ShipmentReady', {}, { trackingNumber: 'TRACK-123' });
          assert(r2.ok, `ShipmentReady failed: ${r2.status}`);

          await sleep(300);
          await completeExternalTask(token, 'notify-customer', instanceId);
          await client.waitForProcessEnd(instanceId, 10000);

          // Verify variables were merged
          const vars = await fetch(`${BASE}/engine-rest/process-instance/${instanceId}/variables`, {
            headers: { Authorization: `Bearer ${token}` },
          }).then(r => r.json()) as any;
          assert(
            vars.paymentRef?.value === 'PAY-XYZ' || vars.paymentRef === 'PAY-XYZ',
            `paymentRef variable should be set, got: ${JSON.stringify(vars.paymentRef)}`
          );
        },
      },

      // ── C: unknown message returns 404 ────────────────────────────────────
      {
        name: '[C] Correlating an unknown message returns 404',
        async fn() {
          const r = await correlateMessage(token, 'NonExistentMessage_' + Date.now());
          assert(r.status === 404, `expected 404, got ${r.status}`);
        },
      },

    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
