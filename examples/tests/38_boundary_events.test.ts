/**
 * Suite 38 — Boundary events  (BPMN: 38_boundary_events.bpmn)
 *
 * Tests:
 *   A) The boundary timer of a task completed in time never fires later.
 *   B) Interrupting message boundary on a user task: cancels the task and
 *      takes the boundary's path.
 *   C) Non-interrupting signal boundary: starts the boundary's path while the
 *      task stays open.
 *   D) Non-interrupting timer boundary (reminder): same, driven by a timer;
 *      the instance ends only once both paths are done.
 *   E) Interrupting timer boundary on a service task: the job is withdrawn.
 *   F) The message boundary of a task completed in time no longer correlates.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '38_boundary_events.bpmn');
const client = new GoFlowClient();

async function historicTaskStatuses(instanceId: string, taskKey: string): Promise<string[]> {
  const res = await client.api.get(`/engine-rest/history/task?processInstanceId=${instanceId}`);
  return (res.data as any[]).filter(t => t.taskDefinitionKey === taskKey).map(t => t.status);
}

async function openTasks(instanceId: string, taskKey: string) {
  return (await client.getTasks(instanceId)).filter(t => t.taskDefinitionKey === taskKey);
}

async function isRunning(instanceId: string): Promise<boolean> {
  const inst = await client.getProcessInstance(instanceId);
  return !!inst && inst.status === 'running';
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Boundary Events Test');

  await runSuite({
    name: '38 — Boundary events',
    tests: [
      {
        name: '[A] Boundary timer of a task completed in time never fires later',
        async fn() {
          const inst = await client.startProcess('BeStaleTimer', {});
          const instanceId = inst.processInstanceId ?? inst.id;
          const a = await client.waitForTask('bst_a', instanceId, 10000);
          await client.completeTask(a.id);
          await client.waitForTask('bst_b', instanceId, 10000);

          await sleep(7000); // the 4s timer would have fired by now
          assert((await openTasks(instanceId, 'bst_timeout')).length === 0, 'stale boundary timer fired after its task completed');
          const b = await openTasks(instanceId, 'bst_b');
          assert(b.length === 1, 'stale boundary timer cancelled the next task');
          await client.completeTask(b[0].id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      {
        name: '[B] Interrupting message boundary cancels the task and takes the boundary path',
        async fn() {
          const inst = await client.startProcess('BeMessageBoundary', {});
          const instanceId = inst.processInstanceId ?? inst.id;
          await client.waitForTask('bm_task', instanceId, 10000);

          const res = await client.api.post('/engine-rest/message', { messageName: 'BeEscalate' }, { validateStatus: () => true });
          assert(res.status < 300, `BeEscalate did not correlate (HTTP ${res.status})`);

          const escalated = await client.waitForTask('bm_escalated', instanceId, 10000);
          assert((await openTasks(instanceId, 'bm_task')).length === 0, 'interrupted task still open');
          await client.completeTask(escalated.id);
          await client.waitForProcessEnd(instanceId, 10000);
          const status = await historicTaskStatuses(instanceId, 'bm_task');
          assert(status.length === 1 && status[0] === 'cancelled', `bm_task should be cancelled, got ${status}`);
        },
      },

      {
        name: '[C] Non-interrupting signal boundary starts its path while the task stays open',
        async fn() {
          const inst = await client.startProcess('BeSignalNonInterrupting', {});
          const instanceId = inst.processInstanceId ?? inst.id;
          const task = await client.waitForTask('bs_task', instanceId, 10000);

          await client.api.post('/engine-rest/signal', { name: 'BeNotify' });
          const notified = await client.waitForTask('bs_notified', instanceId, 10000);
          assert((await openTasks(instanceId, 'bs_task')).length === 1, 'non-interrupting boundary cancelled the task');

          await client.completeTask(task.id);
          await sleep(500);
          assert(await isRunning(instanceId), 'instance ended while the boundary path was still open');
          await client.completeTask(notified.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      {
        name: '[D] Non-interrupting timer boundary sends a reminder without cancelling the task',
        async fn() {
          const inst = await client.startProcess('BeTimerNonInterrupting', {});
          const instanceId = inst.processInstanceId ?? inst.id;
          const task = await client.waitForTask('bt_task', instanceId, 10000);

          const reminder = await client.waitForTask('bt_reminder', instanceId, 30000);
          assert((await openTasks(instanceId, 'bt_task')).length === 1, 'reminder timer cancelled the task');

          await client.completeTask(task.id);
          await client.completeTask(reminder.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      {
        name: '[E] Interrupting timer boundary on a service task withdraws the job',
        async fn() {
          const inst = await client.startProcess('BeServiceTimer', {});
          const instanceId = inst.processInstanceId ?? inst.id;

          const timeout = await client.waitForTask('bsv_timeout', instanceId, 30000);
          const jobs = await client.fetchAndLock([{ name: 'be_slow' }]);
          assert(!jobs.some(j => j.processInstanceId === instanceId), 'job of the interrupted service task can still be fetched');

          await client.completeTask(timeout.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      {
        name: '[F] Message boundary of a task completed in time no longer correlates',
        async fn() {
          const inst = await client.startProcess('BeStaleMessage', {});
          const instanceId = inst.processInstanceId ?? inst.id;
          const a = await client.waitForTask('bsm_a', instanceId, 10000);
          await client.completeTask(a.id);
          await client.waitForTask('bsm_b', instanceId, 10000);

          const res = await client.api.post('/engine-rest/message', { messageName: 'BeLate' }, { validateStatus: () => true });
          assert(res.status >= 400, `BeLate still correlated after its task completed (HTTP ${res.status})`);
          const b = await openTasks(instanceId, 'bsm_b');
          assert(b.length === 1, 'stale message boundary hijacked the token');
          await client.completeTask(b[0].id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },
    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
