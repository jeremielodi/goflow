/**
 * Suite 41 — Escalation events  (BPMN: 41_escalation.bpmn)
 *
 * Tests:
 *   A) An escalation end event inside a sub-process is caught by an
 *      interrupting boundary (no cancelActivity attribute: BPMN default):
 *      the sub-process's other branch is cancelled.
 *   B) An intermediate escalation throw caught by a non-interrupting
 *      catch-all boundary: both paths run.
 *   C) A non-interrupting escalation event sub-process runs alongside.
 *   D) An uncaught escalation lets the token carry on, without an incident.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '41_escalation.bpmn');
const client = new GoFlowClient();

async function historicTaskStatus(instanceId: string, taskKey: string): Promise<string | undefined> {
  const res = await client.api.get(`/engine-rest/history/task?processInstanceId=${instanceId}`);
  return (res.data as any[]).find(t => t.taskDefinitionKey === taskKey)?.status;
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Escalation Test');

  await runSuite({
    name: '41 — Escalation events',
    tests: [
      {
        name: '[A] Escalation end event caught by an interrupting sub-process boundary',
        async fn() {
          const inst = await client.startProcess('EscInterrupting', {});
          const id = inst.processInstanceId ?? inst.id;
          await client.waitForTask('esc_other', id, 10000);
          const work = await client.waitForTask('esc_work', id, 10000);
          await client.completeTask(work.id);

          const handled = await client.waitForTask('esc_handled', id, 10000);
          await client.completeTask(handled.id);
          await client.waitForProcessEnd(id, 10000);
          const other = await historicTaskStatus(id, 'esc_other');
          assert(other === 'cancelled', `esc_other should be cancelled by the interrupting escalation, got ${other}`);
        },
      },
      {
        name: '[B] Intermediate escalation caught by a non-interrupting catch-all boundary',
        async fn() {
          const inst = await client.startProcess('EscNonInterrupting', {});
          const id = inst.processInstanceId ?? inst.id;
          const after = await client.waitForTask('esc_after_throw', id, 10000);
          const noted = await client.waitForTask('esc_noted', id, 10000);
          await client.completeTask(after.id);
          await client.completeTask(noted.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[C] Non-interrupting escalation event sub-process runs alongside',
        async fn() {
          const inst = await client.startProcess('EscEventSub', {});
          const id = inst.processInstanceId ?? inst.id;
          const main = await client.waitForTask('esc_main', id, 10000);
          const reaction = await client.waitForTask('esc_es_task', id, 10000);
          await client.completeTask(main.id);
          await sleep(500);
          const live = await client.getProcessInstance(id);
          assert(live?.status === 'running', 'instance ended before the event sub-process finished');
          await client.completeTask(reaction.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[D] Uncaught escalation: the token carries on, no incident',
        async fn() {
          const inst = await client.startProcess('EscUncaught', {});
          const id = inst.processInstanceId ?? inst.id;
          const next = await client.waitForTask('esc_continue', id, 10000);
          const incidents = (await client.api.get(`/engine-rest/incident?processInstanceId=${id}`)).data as any[];
          assert(incidents.length === 0, `expected no incident, got ${incidents.length}`);
          await client.completeTask(next.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
