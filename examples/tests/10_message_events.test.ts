/**
 * Suite 10 — Message Events  (BPMN: 10_order_notification.bpmn)
 * Source: https://camunda.com/bpmn/reference/ — Message Catching/Throwing
 *
 * Tests:
 *   A) Process parks at IntermediateCatchEvent, resumed by POST /engine-rest/message, then
 *      by a second message; the reply is the Camunda 7 result list with resultEnabled,
 *      204 without
 *   B) Message variables are merged into the process instance
 *   C) Unknown message returns 400 MismatchingMessageCorrelationException (as in Camunda 7)
 *   D) businessKey / processInstanceId target one instance; all correlates every waiting one
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '10_order_notification.bpmn');
const client = new GoFlowClient();
const BASE = process.env.GOFLOW_URL ?? 'http://127.0.0.1:8080';

async function correlateMessage(
  token: string,
  messageName: string,
  correlationKeys: Record<string, unknown> = {},
  variables: Record<string, unknown> = {},
  extra: Record<string, unknown> = {}
): Promise<Response> {
  return fetch(`${BASE}/engine-rest/message`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ messageName, correlationKeys, variables, ...extra }),
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
          // As in Camunda 7: the correlations come back only with resultEnabled.
          const r1 = await correlateMessage(token, 'PaymentReceived', {}, {}, { resultEnabled: true });
          assert(r1.ok, `PaymentReceived message failed: ${r1.status}`);
          const body1 = await r1.json() as any;
          assert(Array.isArray(body1) && body1[0]?.resultType === 'Execution', `expected a Camunda 7 result list, got ${JSON.stringify(body1)}`);
          assert(body1[0].execution.processInstanceId === instanceId, 'expected the correlated instance in the result');

          // Process confirms payment then parks at waitForShipment
          await sleep(500);
          await completeExternalTask(token, 'confirm-payment', instanceId);

          await sleep(300);

          // Correlate the ShipmentReady message
          const r2 = await correlateMessage(token, 'ShipmentReady');
          assert(r2.status === 204, `ShipmentReady: expected 204 without resultEnabled, got ${r2.status}`);

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

      // ── C: unknown message: 400, as in Camunda 7 ──────────────────────────
      {
        name: '[C] Correlating an unknown message returns 400 MismatchingMessageCorrelationException',
        async fn() {
          const r = await correlateMessage(token, 'NonExistentMessage_' + Date.now());
          assert(r.status === 400, `expected 400, got ${r.status}`);
          const body = await r.json() as any;
          assert(/MismatchingMessageCorrelationException/.test(body.message), `unexpected body ${JSON.stringify(body)}`);
        },
      },

      // ── D: Camunda 7 targeting: businessKey, processInstanceId, all ────────
      {
        name: '[D] businessKey and processInstanceId target one instance; all correlates every waiting one',
        async fn() {
          const tag = Date.now();
          const start = async (bk: string) => {
            const r = await fetch(`${BASE}/engine-rest/process-definition/key/orderNotification/start`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
              body: JSON.stringify({ businessKey: bk, variables: { orderId: { value: bk, type: 'String' } } }),
            });
            return ((await r.json()) as any).id as string;
          };
          const ids = [await start(`E-${tag}-1`), await start(`E-${tag}-2`), await start(`E-${tag}-3`), await start(`E-${tag}-4`)];
          await sleep(500);
          const correlated = async (extra: Record<string, unknown>) => {
            const r = await correlateMessage(token, 'PaymentReceived', {}, {}, { resultEnabled: true, ...extra });
            assert(r.ok, `correlation ${JSON.stringify(extra)} failed: ${r.status}`);
            return ((await r.json()) as any[]).map(x => x.execution.processInstanceId);
          };

          const byKey = await correlated({ businessKey: `E-${tag}-2` });
          assert(byKey.length === 1 && byKey[0] === ids[1], `businessKey should hit instance 2, got ${byKey}`);
          const byId = await correlated({ processInstanceId: ids[3] });
          assert(byId.length === 1 && byId[0] === ids[3], `processInstanceId should hit instance 4, got ${byId}`);

          // Instances 1 and 3 still wait: "all" takes both (and nothing else of this test's).
          const all = await correlated({ all: true });
          assert(all.includes(ids[0]) && all.includes(ids[2]), `all should include instances 1 and 3, got ${all}`);
          assert(!all.includes(ids[1]) && !all.includes(ids[3]), 'all must not correlate an instance twice');
        },
      },

    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
