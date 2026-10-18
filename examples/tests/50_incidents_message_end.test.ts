/**
 * Suite 50 — Message end events and runtime incidents
 *            (BPMN: 50_incidents_message_end.bpmn)
 *
 * Tests:
 *   A) A message end event with a job type is worked by a job worker, then
 *      ends only its own branch.
 *   B) An exclusive gateway whose conditions can't be decided raises an
 *      incident on the gateway (the token waits there); fixing the
 *      variable and resolving the incident runs the gateway again.
 *   C) A multi-instance input collection that isn't a list raises an
 *      incident; resolving it after fixing the variable runs the loop.
 *   D) An untyped task passes straight through (Camunda 8).
 *   E) Compensation thrown after a sub-process completed reaches the
 *      activities inside it (nested compensation, Camunda 8).
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '50_incidents_message_end.bpmn');
const client = new GoFlowClient();

async function start(key: string, vars: Record<string, unknown>) {
  const inst = await client.startProcess(key, vars);
  return (inst.processInstanceId ?? inst.id) as string;
}

async function openIncidents(id: string) {
  const res = await client.api.get(`/engine-rest/process-instance/${id}/incidents`);
  return (res.data as any[]).filter(i => i.state === 'open');
}

async function waitIncident(id: string, activityId: string) {
  const deadline = Date.now() + 8000;
  while (Date.now() < deadline) {
    const found = (await openIncidents(id)).find(i => i.activityId === activityId);
    if (found) return found;
    await sleep(300);
  }
  throw new Error(`no open incident on ${activityId}`);
}

async function main() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Incidents and Message End Test');

  await runSuite({
    name: '50 — Message end events and runtime incidents',
    tests: [
      {
        name: '[A] A message end event is worked by a job worker and ends its branch',
        async fn() {
          const id = await start('MessageEnd50', {});
          await client.pollExternalTask('notify50', async () => ({ notified: true }), 10000, id);
          await sleep(500);
          const other = (await client.getTasks(id)).filter(t => t.taskDefinitionKey === 'me_other');
          assert(other.length === 1, 'the other branch keeps going');
          const inst = await client.getProcessInstance(id);
          assert(inst?.status !== 'completed', 'the instance must not end with one branch left');
          await client.completeTask(other[0].id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[B] An undecidable gateway raises an incident; resolving it runs the gateway again',
        async fn() {
          const id = await start('GatewayIncident50', {});
          const incident = await waitIncident(id, 'g_gw');
          assert(incident.incidentType === 'executionError', `incident type: ${incident.incidentType}`);
          await client.api.post(`/engine-rest/process-instance/${id}/variables`, { amount: 500 });
          const res = await client.api.post(`/v2/incidents/${incident.id}/resolution`, {});
          assert(res.status === 204, `resolution: ${res.status}`);
          const big = await client.waitForTask('g_big', id, 10000);
          assert((await openIncidents(id)).length === 0, 'no open incident left');
          await client.completeTask(big.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[C] A bad input collection raises an incident until fixed',
        async fn() {
          const id = await start('CollectionIncident50', { items: 5 });
          const incident = await waitIncident(id, 'c_each');
          await client.api.post(`/engine-rest/process-instance/${id}/variables`, { items: ['a', 'b'] });
          await client.api.post(`/v2/incidents/${incident.id}/resolution`, {});
          const deadline = Date.now() + 8000;
          let tasks = (await client.getTasks(id)).filter(t => t.taskDefinitionKey === 'c_each');
          while (tasks.length < 2 && Date.now() < deadline) { await sleep(300); tasks = (await client.getTasks(id)).filter(t => t.taskDefinitionKey === 'c_each'); }
          assert(tasks.length === 2, `two inner instances after the fix, got ${tasks.length}`);
          for (const t of tasks) await client.completeTask(t.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[E] Compensation reaches inside a completed sub-process',
        async fn() {
          const id = await start('NestedComp50', {});
          const book = await client.waitForTask('n_book', id, 10000);
          await client.completeTask(book.id);
          const unbook = await client.waitForTask('n_unbook', id, 10000);
          assert((await client.getTasks(id)).every(t => t.taskDefinitionKey !== 'n_after'), 'the throw waits for the handler');
          await client.completeTask(unbook.id);
          const after = await client.waitForTask('n_after', id, 10000);
          await client.completeTask(after.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[D] An untyped task passes straight through',
        async fn() {
          const id = await start('Untyped50', {});
          const after = await client.waitForTask('u_after', id, 10000);
          const all = await client.getTasks(id);
          assert(!all.some(t => t.taskDefinitionKey === 'u_task'), 'no user task for the untyped task');
          await client.completeTask(after.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
    ],
  });
}

main().catch(err => { console.error(err); process.exit(1); });
