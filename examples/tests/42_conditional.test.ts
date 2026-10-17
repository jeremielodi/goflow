/**
 * Suite 42 — Conditional events  (BPMN: 42_conditional.bpmn)
 *
 * Conditions are re-evaluated by the engine's conditional watcher (about
 * every second), so a condition made true by a variable change fires
 * within a couple of seconds.
 *
 * Tests:
 *   A) An intermediate conditional event already true lets the token through.
 *   B) Otherwise the token waits until a variable change makes it true.
 *   C) A non-interrupting conditional boundary fires on each false -> true
 *      transition — not repeatedly while the condition stays true — and
 *      leaves the task open.
 *   D) An interrupting conditional event sub-process cancels the main flow.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '42_conditional.bpmn');
const client = new GoFlowClient();

async function setVars(instanceId: string, vars: Record<string, unknown>) {
  await client.api.post(`/engine-rest/process-instance/${instanceId}/variables`, vars);
}

async function openTasks(instanceId: string, key: string) {
  return (await client.getTasks(instanceId)).filter(t => t.taskDefinitionKey === key);
}

async function waitForCount(instanceId: string, key: string, n: number, timeoutMs = 8000) {
  const deadline = Date.now() + timeoutMs;
  let tasks = await openTasks(instanceId, key);
  while (tasks.length < n && Date.now() < deadline) {
    await sleep(300);
    tasks = await openTasks(instanceId, key);
  }
  return tasks;
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Conditional Events Test');

  await runSuite({
    name: '42 — Conditional events',
    tests: [
      {
        name: '[A] An intermediate conditional event already true lets the token through',
        async fn() {
          const inst = await client.startProcess('CondIntermediate', {});
          const id = inst.processInstanceId ?? inst.id;
          const prep = await client.waitForTask('cond_prepare', id, 10000);
          await client.completeTask(prep.id, { stock: 3 });
          const ship = await client.waitForTask('cond_ship', id, 5000);
          await client.completeTask(ship.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[B] The token waits until a variable change makes the condition true',
        async fn() {
          const inst = await client.startProcess('CondIntermediate', {});
          const id = inst.processInstanceId ?? inst.id;
          const prep = await client.waitForTask('cond_prepare', id, 10000);
          await client.completeTask(prep.id, { stock: 0 });

          await sleep(2500);
          assert((await openTasks(id, 'cond_ship')).length === 0, 'token passed a false condition');

          await setVars(id, { stock: 5 });
          const ship = await client.waitForTask('cond_ship', id, 8000);
          await client.completeTask(ship.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[C] Non-interrupting conditional boundary fires once per false -> true transition',
        async fn() {
          const inst = await client.startProcess('CondBoundary', { priority: 'low' });
          const id = inst.processInstanceId ?? inst.id;
          const work = await client.waitForTask('cond_long', id, 10000);

          await setVars(id, { priority: 'high' });
          let alerts = await waitForCount(id, 'cond_alert', 1);
          assert(alerts.length === 1, `expected 1 alert, got ${alerts.length}`);

          // Still true: no new alert.
          await sleep(2500);
          alerts = await openTasks(id, 'cond_alert');
          assert(alerts.length === 1, `condition still true must not fire again, got ${alerts.length} alerts`);

          // False, then true again: a second alert.
          await setVars(id, { priority: 'low' });
          await sleep(2000);
          await setVars(id, { priority: 'high' });
          alerts = await waitForCount(id, 'cond_alert', 2);
          assert(alerts.length === 2, `expected a second alert after false -> true, got ${alerts.length}`);
          assert((await openTasks(id, 'cond_long')).length === 1, 'non-interrupting boundary cancelled the task');

          await client.completeTask(work.id);
          for (const a of alerts) await client.completeTask(a.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[D] Interrupting conditional event sub-process cancels the main flow',
        async fn() {
          const inst = await client.startProcess('CondEventSub', { cancelled: false });
          const id = inst.processInstanceId ?? inst.id;
          await client.waitForTask('cond_main', id, 10000);

          await setVars(id, { cancelled: true });
          const handler = await client.waitForTask('cond_cancel_handler', id, 8000);
          assert((await openTasks(id, 'cond_main')).length === 0, 'main task still open after the interrupting event sub-process');
          await client.completeTask(handler.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
