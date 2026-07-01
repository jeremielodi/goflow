/**
 * Suite 14 — Task Listener Notifications (SSE)  (BPMN: 14_task_listener.bpmn)
 *
 * Scenario: A request goes through a human review, an external-worker
 *           notification step, then a second human approval. A client
 *           subscribes to GET /events/tasks (SSE) and must be able to
 *           reconstruct every step of the process purely from the events
 *           the TaskEventDispatcher pushes out — for both human-driven
 *           steps (claim/complete) and worker-driven steps (fetchAndLock/
 *           complete on an external task).
 *
 * Tests:
 *   A) Human task lifecycle: TASK_CREATED → TASK_CLAIMED → TASK_COMPLETED
 *      are all observed over SSE, in order, with correct payload fields.
 *   B) A step completed by an external task worker still triggers a
 *      TASK_CREATED notification for the task that follows it, so a
 *      listening client can track progress across mixed human/worker steps.
 *   C) Unclaiming a task emits TASK_UNCLAIMED, and the SSE processKeys
 *      filter excludes events from processes the client didn't ask for.
 */
import * as path from 'path';
import { GoFlowClient, TaskSSEClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '14_task_listener.bpmn');
const PIZZA_BPMN = path.join(__dirname, 'bpmn', '01_pizza_delivery.bpmn');
const client = new GoFlowClient();

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Task Listener SSE Test');
  await client.deployBpmn(PIZZA_BPMN, 'Task Listener SSE Test — Pizza (filter check)');

  await runSuite({
    name: '14 — Task Listener Notifications (SSE)',
    tests: [

      // ── A: human task lifecycle over SSE ──────────────────────────────────────
      {
        name: '[Human lifecycle] SSE observes TASK_CREATED → TASK_CLAIMED → TASK_COMPLETED',
        async fn() {
          const sse = await TaskSSEClient.connect(client.api, {
            processKeys: ['TaskListenerProcess'],
          });
          try {
            const inst = await client.startProcess('TaskListenerProcess', {});
            const instanceId = inst.processInstanceId ?? inst.id;

            const created = await sse.waitFor(
              e => e.eventType === 'TASK_CREATED' && e.processInstanceId === instanceId
            );
            assert(created.taskName === 'Review Request', `expected "Review Request", got ${created.taskName}`);
            assert(created.processKey === 'TaskListenerProcess', `unexpected processKey ${created.processKey}`);

            const reviewTask = await client.waitForTask('reviewTask', instanceId);
            assert(reviewTask.id === created.taskId, 'SSE taskId should match the created task');

            await client.claimTask(reviewTask.id, 'alice');
            const claimed = await sse.waitFor(
              e => e.eventType === 'TASK_CLAIMED' && e.taskId === reviewTask.id
            );
            assert(claimed.assignee === 'alice', `expected assignee "alice", got ${claimed.assignee}`);

            await client.completeTask(reviewTask.id);
            const completed = await sse.waitFor(
              e => e.eventType === 'TASK_COMPLETED' && e.taskId === reviewTask.id
            );
            assert(completed.newStatus === 'completed', `expected newStatus "completed", got ${completed.newStatus}`);

            // Events must have been observed in lifecycle order.
            const order = sse.events
              .filter(e => e.taskId === reviewTask.id)
              .map(e => e.eventType);
            assert(
              order.indexOf('TASK_CREATED') < order.indexOf('TASK_CLAIMED') &&
                order.indexOf('TASK_CLAIMED') < order.indexOf('TASK_COMPLETED'),
              `unexpected event order: ${order.join(', ')}`
            );
          } finally {
            sse.close();
          }
        },
      },

      // ── B: worker-driven step still notifies the next task ────────────────────
      {
        name: '[Worker-driven] External task completion via fetchAndLock notifies SSE for the next step',
        async fn() {
          const sse = await TaskSSEClient.connect(client.api, {
            processKeys: ['TaskListenerProcess'],
          });
          try {
            const inst = await client.startProcess('TaskListenerProcess', {});
            const instanceId = inst.processInstanceId ?? inst.id;

            const reviewTask = await client.waitForTask('reviewTask', instanceId);
            await client.claimTask(reviewTask.id, 'bob');
            await client.completeTask(reviewTask.id);

            // Worker picks up the external task, does its work, and completes it —
            // no human interaction, no dedicated SSE event for the job itself.
            await client.pollExternalTask(
              'listener_worker_task',
              async () => ({ notified: true }),
              15000,
              instanceId
            );

            // The task that follows the worker step must still be announced over SSE.
            const approveCreated = await sse.waitFor(
              e =>
                e.eventType === 'TASK_CREATED' &&
                e.processInstanceId === instanceId &&
                e.taskName === 'Approve Request'
            );
            assert(approveCreated !== undefined, 'expected TASK_CREATED for approveTask after worker step');

            const approveTask = await client.waitForTask('approveTask', instanceId);
            await client.claimTask(approveTask.id, 'dana');
            await sse.waitFor(e => e.eventType === 'TASK_CLAIMED' && e.taskId === approveTask.id);

            await client.completeTask(approveTask.id);
            await sse.waitFor(e => e.eventType === 'TASK_COMPLETED' && e.taskId === approveTask.id);

            await client.waitForProcessEnd(instanceId, 15000);
            const final = await client.getProcessInstance(instanceId);
            assert(!final || final.status === 'completed', `expected completed, got ${final?.status}`);
          } finally {
            sse.close();
          }
        },
      },

      // ── C: unclaim event + processKeys filter isolation ────────────────────────
      {
        name: '[Unclaim + filter] TASK_UNCLAIMED fires, and processKeys filter excludes other processes',
        async fn() {
          const sse = await TaskSSEClient.connect(client.api, {
            processKeys: ['TaskListenerProcess'],
          });
          try {
            const inst = await client.startProcess('TaskListenerProcess', {});
            const instanceId = inst.processInstanceId ?? inst.id;

            const reviewTask = await client.waitForTask('reviewTask', instanceId);
            await client.claimTask(reviewTask.id, 'erin');
            await sse.waitFor(e => e.eventType === 'TASK_CLAIMED' && e.taskId === reviewTask.id);

            await client.unclaimTask(reviewTask.id);
            const unclaimed = await sse.waitFor(
              e => e.eventType === 'TASK_UNCLAIMED' && e.taskId === reviewTask.id
            );
            assert(unclaimed.newStatus === 'created', `expected newStatus "created", got ${unclaimed.newStatus}`);

            // Start an unrelated process (different processKey) while the filtered
            // SSE client is still connected — its events must not leak through.
            const pizzaInst = await client.startProcess('PizzaDeliveryProcess', { customer: 'zoe' });
            const pizzaInstanceId = pizzaInst.processInstanceId ?? pizzaInst.id;
            await client.waitForTask('order_pizza', pizzaInstanceId);
            await sleep(1000); // give the dispatcher a chance to (wrongly) deliver it

            const leaked = sse.events.some(e => e.processInstanceId === pizzaInstanceId);
            assert(!leaked, 'SSE client filtered to TaskListenerProcess must not receive PizzaDeliveryProcess events');

            // Finish the review task normally so the process doesn't dangle.
            await client.claimTask(reviewTask.id, 'erin');
            await client.completeTask(reviewTask.id);
          } finally {
            sse.close();
          }
        },
      },

    ],
  });
}

run().catch(console.error);
