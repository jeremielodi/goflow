/**
 * Suite 01 — Boundary Timer Event  (BPMN: 01_pizza_delivery.bpmn)
 * Source: https://camunda.com/bpmn/reference/ — Boundary Timer example
 *
 * Scenario: Customer orders pizza. If the pizza arrives (user completes the task)
 * before the 60-second timer, the process ends normally. If the timer fires first,
 * an external worker makes the anxious phone call.
 *
 * Tests:
 *   A) Normal path — task completed before timer fires
 *   B) Timer path  — task NOT completed; timer fires and triggers escalation worker
 *   C) Claim / unclaim on a candidateGroup task (adapted variant)
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '01_pizza_delivery.bpmn');
const client = new GoFlowClient();

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Pizza Delivery Test');

  await runSuite({
    name: '01 — Boundary Timer (Pizza Delivery)',
    tests: [

      // ── A: normal path ──────────────────────────────────────────────────────
      {
        name: '[Normal path] Task completed before timer — process ends as "Pizza Delivered"',
        async fn() {
          const inst = await client.startProcess('PizzaDeliveryProcess', {
            customer: 'alice',
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          // The "order_pizza" user task should appear immediately
          const task = await client.waitForTask('order_pizza', instanceId, 15000);
          assert(task.assignee === 'alice', `expected assignee "alice", got "${task.assignee}"`);

          // Complete the task before the 60s timer — simulates pizza arriving on time
          await client.completeTask(task.id, { delivered: true });

          await client.waitForProcessEnd(instanceId, 15000);
          const final = await client.getProcessInstance(instanceId);
          assert(!final || final.status === 'completed', 'process should be completed');
        },
      },

      // ── B: timer path ───────────────────────────────────────────────────────
      {
        name: '[Timer path] Timer fires after 60s — escalation worker receives "call_restaurant" job',
        async fn() {
          // NOTE: This test takes ~65 seconds to pass (timer PT60S).
          // Reduce PT60S to PT10S in the BPMN for faster CI runs.
          const inst = await client.startProcess('PizzaDeliveryProcess', {
            customer: 'bob',
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          // Confirm the user task is waiting
          await client.waitForTask('order_pizza', instanceId, 10000);
          console.log('       ⏳  Waiting for boundary timer to fire (up to 75s)…');

          // Do NOT complete the task — wait for timer to interrupt it
          // Poll the external task topic that appears after the timer fires,
          // filtering to THIS process instance to avoid picking up stale jobs.
          await client.pollExternalTask(
            'call_restaurant',
            async () => ({ calledAt: new Date().toISOString() }),
            75_000,
            instanceId
          );

          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      // ── C: claim / unclaim ──────────────────────────────────────────────────
      {
        name: '[Claim/Unclaim] Task can be claimed, unclaimed, then re-claimed by another user',
        async fn() {
          const inst = await client.startProcess('PizzaDeliveryProcess', {
            customer: 'charlie',
          });
          const instanceId = inst.processInstanceId ?? inst.id;
          const task = await client.waitForTask('order_pizza', instanceId, 10000);

          // The task starts with assignee=charlie from the BPMN expression ${customer}.
          // Unclaim it first so we can re-claim it (API rejects claim if already assigned).
          await client.unclaimTask(task.id);
          await client.claimTask(task.id, 'charlie');
          const claimed = await client.getTasks(instanceId);
          assert(
            claimed.some(t => t.id === task.id && t.status === 'claimed'),
            'task should be claimed'
          );

          // Unclaim it
          await client.unclaimTask(task.id);
          const unclaimed = await client.getTasks(instanceId);
          const t = unclaimed.find(t => t.id === task.id);
          assert(t !== undefined, 'task should still exist after unclaim');
          assert(t!.status !== 'claimed', 'task should no longer be claimed');

          // Re-claim by a different user and complete
          await client.claimTask(task.id, 'diana');
          await client.completeTask(task.id, { delivered: true });
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

    ],
  });
}

run().catch(console.error);
