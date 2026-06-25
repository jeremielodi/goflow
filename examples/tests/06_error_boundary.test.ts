/**
 * Suite 06 — Error Boundary Event  (BPMN: 06_payment_error.bpmn)
 * Source: https://camunda.com/bpmn/reference/ — Error Boundary Event example
 *
 * Scenario: An order is processed by a payment service task. If the worker
 * throws a BPMN error (errorCode: PAYMENT_FAILED), the error boundary event
 * catches it and routes to a human support task. On success, the order is confirmed.
 *
 * Tests:
 *   A) Normal path — payment worker succeeds → process ends as "Order Confirmed"
 *   B) Error path  — payment worker throws PAYMENT_FAILED → boundary event fires
 *                    → "Handle Payment Error" user task appears → process ends as "Order Failed"
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert } from './client';

const BPMN = path.join(__dirname, 'bpmn', '06_payment_error.bpmn');
const client = new GoFlowClient();

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Payment Error Boundary Test');

  await runSuite({
    name: '06 — Error Boundary Event (Payment)',
    tests: [

      // ── A: normal path ────────────────────────────────────────────────────────
      {
        name: '[Normal path] Payment succeeds — process ends as "Order Confirmed"',
        async fn() {
          const inst = await client.startProcess('PaymentErrorProcess', {
            orderId: 'ORD-001',
            amount: 99.99,
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          // Worker completes successfully
          await client.pollExternalTask(
            'process_payment',
            async () => ({ paymentRef: 'PAY-OK-001', charged: true }),
            15000,
            instanceId
          );

          await client.waitForProcessEnd(instanceId, 10000);
          const final = await client.getProcessInstance(instanceId);
          assert(!final || final.status === 'completed', `expected completed, got ${final?.status}`);
        },
      },

      // ── B: error path ─────────────────────────────────────────────────────────
      {
        name: '[Error path] Payment throws PAYMENT_FAILED → error boundary fires → support task appears',
        async fn() {
          const inst = await client.startProcess('PaymentErrorProcess', {
            orderId: 'ORD-002',
            amount: 0.01,
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          // Lock the job, then fail it with the BPMN error code in one call
          const tasks = await client.fetchAndLock([{ name: 'process_payment' }]);
          const job = tasks.find(t => t.processInstanceId === instanceId);
          assert(job !== undefined, 'process_payment job not found');

          // Single failure call with errorCode — retries=0 means permanent, triggers error boundary
          await client.api.post(`/engine-rest/external-task/${job!.id}/failure`, {
            workerId: client.workerId,
            errorMessage: 'Card declined',
            errorCode: 'PAYMENT_FAILED',
            retries: 0,
            retryTimeout: 0,
          });

          // Error boundary should have fired → support user task must appear
          const errorTask = await client.waitForTask('handle_payment_error', instanceId, 10000);
          assert(errorTask !== undefined, 'handle_payment_error task should exist after error boundary');
          assert(errorTask.assignee === 'support', `expected assignee "support", got "${errorTask.assignee}"`);

          // Complete the error-handling task
          await client.completeTask(errorTask.id, { resolution: 'refunded' });

          await client.waitForProcessEnd(instanceId, 10000);
          const final = await client.getProcessInstance(instanceId);
          assert(!final || final.status === 'completed', `expected completed, got ${final?.status}`);
        },
      },

    ],
  });
}

run().catch(console.error);
