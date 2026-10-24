/**
 * Suite 52 — a Camunda 7 application and worker  (BPMN: 52_camunda7_worker.bpmn)
 *
 * Starts instances and works external tasks the way a Camunda 7 client
 * does (HTTP Basic auth, typed variables, Camunda 7 paths and formats).
 *
 * Tests:
 *   A) POST /process-definition/key/{key}/start with typed variables and a
 *      businessKey: Camunda 7's answer; GET /process-definition/key/{key};
 *      GET /process-instance?businessKey= finds it; history by businessKey.
 *   B) fetchAndLock returns Camunda 7's LockedExternalTaskDto fields and
 *      only the topic's requested variables.
 *   C) extendLock (by the owner: 204, by another worker: 500), unlock: the
 *      task can be fetched again.
 *   D) bpmnError: caught by the error boundary; with no catcher, an
 *      incident.
 *   E) failure: retries is what remains (1: fetched again, 0: incident).
 *   F) Long polling: fetchAndLock with asyncResponseTimeout returns the
 *      task created while it waits.
 */
import * as path from 'path';
import axios from 'axios';
import { GoFlowClient, runSuite, assert, sleep, BASE_URL } from './client';

const BPMN = path.join(__dirname, 'bpmn', '52_camunda7_worker.bpmn');
const client = new GoFlowClient();
const run = Date.now().toString(36);
const c7 = axios.create({
  baseURL: `${BASE_URL}/engine-rest`,
  auth: { username: 'admin@goflow.com', password: 'admin123' },
  validateStatus: () => true,
});

async function start(key: string, variables: Record<string, unknown> = {}, businessKey?: string) {
  const res = await c7.post(`/process-definition/key/${key}/start`, { variables, businessKey });
  assert(res.status === 200, `start ${key} → ${res.status} ${JSON.stringify(res.data)}`);
  return res.data;
}

// fetch locks the topic's tasks of instanceId for worker (waiting a bit).
async function fetch(topic: string, instanceId: string, worker = 'c7-worker', extra: Record<string, unknown> = {}) {
  for (let i = 0; i < 20; i++) {
    const res = await c7.post('/external-task/fetchAndLock', {
      workerId: worker, maxTasks: 10, topics: [{ topicName: topic, lockDuration: 30000, ...extra }],
    });
    assert(res.status === 200, `fetchAndLock → ${res.status} ${JSON.stringify(res.data)}`);
    const mine = res.data.find((t: any) => t.processInstanceId === instanceId);
    if (mine) return mine;
    await sleep(300);
  }
  throw new Error(`no ${topic} task for ${instanceId}`);
}

async function incidents(instanceId: string) {
  return (await client.api.get(`/engine-rest/process-instance/${instanceId}/incidents`)).data as any[];
}

async function main() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Camunda 7 worker');

  await runSuite({
    name: '52 — Camunda 7 application and worker',
    tests: [
      {
        name: '[A] Start by key with a businessKey; find it by businessKey',
        async fn() {
          const bk = `ORDER-${run}`;
          const inst = await start('C7Worker52', { amount: { value: 120, type: 'Integer' }, customer: { value: 'ACME', type: 'String' } }, bk);
          assert(inst.id && inst.definitionId && inst.businessKey === bk && inst.ended === false && inst.suspended === false && Array.isArray(inst.links),
            `ProcessInstanceWithVariablesDto: ${JSON.stringify(inst)}`);

          const def = (await c7.get('/process-definition/key/C7Worker52')).data;
          assert(def.key === 'C7Worker52' && def.id === inst.definitionId && typeof def.version === 'number', `definition: ${JSON.stringify(def)}`);

          const found = (await c7.get('/process-instance', { params: { businessKey: bk } })).data;
          assert(found.length === 1 && found[0].id === inst.id && found[0].businessKey === bk && found[0].definitionId === inst.definitionId,
            `GET /process-instance?businessKey=: ${JSON.stringify(found)}`);

          const vars = (await client.api.get(`/engine-rest/process-instance/${inst.id}/variables`)).data;
          assert(vars.amount?.value === 120 && vars.customer?.value === 'ACME', `typed start variables stored as values: ${JSON.stringify(vars)}`);

          // Finish it, then find it in history by its businessKey.
          const task = await fetch('c7-charge', inst.id);
          await c7.post(`/external-task/${task.id}/complete`, { workerId: 'c7-worker', variables: {} });
          const ship = await client.waitForTask('w_ship', inst.id, 10000);
          await c7.post(`/task/${ship.id}/complete`, {});
          await client.waitForProcessEnd(inst.id, 10000);
          const hist = (await c7.get('/history/process-instance', { params: { businessKey: bk } })).data;
          assert(hist.length === 1 && hist[0].businessKey === bk && hist[0].state === 'COMPLETED', `history by businessKey: ${JSON.stringify(hist)}`);
        },
      },
      {
        name: '[B] fetchAndLock: Camunda 7 fields and the requested variables only',
        async fn() {
          const bk = `B-${run}`;
          const inst = await start('C7Worker52', { amount: { value: 7, type: 'Integer' }, secret: { value: 'x', type: 'String' } }, bk);
          const t = await fetch('c7-charge', inst.id, 'c7-worker', { variables: ['amount'] });
          assert(t.activityId === 'w_charge' && t.processDefinitionKey === 'C7Worker52' && t.processDefinitionId === inst.definitionId,
            `LockedExternalTaskDto: ${JSON.stringify(t)}`);
          assert(t.businessKey === bk && t.workerId === 'c7-worker' && typeof t.executionId === 'string' && t.topicName === 'c7-charge', `LockedExternalTaskDto: ${JSON.stringify(t)}`);
          assert(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}[+-]\d{4}$/.test(t.lockExpirationTime), `lockExpirationTime: ${t.lockExpirationTime}`);
          assert(t.variables.amount?.value === 7 && !('secret' in t.variables), `only the requested variables: ${JSON.stringify(t.variables)}`);
          await c7.post(`/external-task/${t.id}/complete`, { workerId: 'c7-worker' });
        },
      },
      {
        name: '[C] extendLock by the owner and by another worker; unlock',
        async fn() {
          const inst = await start('C7Worker52');
          const t = await fetch('c7-charge', inst.id, 'worker-1');
          let r = await c7.post(`/external-task/${t.id}/extendLock`, { workerId: 'worker-1', newDuration: 60000 });
          assert(r.status === 204, `extendLock by the owner → ${r.status} ${JSON.stringify(r.data)}`);
          r = await c7.post(`/external-task/${t.id}/extendLock`, { workerId: 'worker-2', newDuration: 60000 });
          assert(r.status === 500 && r.data.type === 'BadUserRequestException', `extendLock by another → ${r.status} ${JSON.stringify(r.data)}`);

          r = await c7.post(`/external-task/${t.id}/unlock`);
          assert(r.status === 204, `unlock → ${r.status}`);
          const again = await fetch('c7-charge', inst.id, 'worker-2');
          assert(again.id === t.id, 'an unlocked task can be fetched again at once');
          await c7.post(`/external-task/${again.id}/complete`, { workerId: 'worker-2' });

          const unknown = await c7.post('/external-task/00000000-0000-4000-8000-000000000000/unlock');
          assert(unknown.status === 404, `unlock of an unknown task → ${unknown.status}`);
        },
      },
      {
        name: '[D] bpmnError: caught by the boundary; uncaught: an incident',
        async fn() {
          const inst = await start('C7Worker52');
          const t = await fetch('c7-charge', inst.id);
          const r = await c7.post(`/external-task/${t.id}/bpmnError`, {
            workerId: 'c7-worker', errorCode: 'PAYMENT_DECLINED', errorMessage: 'card refused',
            variables: { reason: { value: 'insufficient funds', type: 'String' } },
          });
          assert(r.status === 204, `bpmnError → ${r.status} ${JSON.stringify(r.data)}`);
          await client.waitForTask('w_declined', inst.id, 10000);
          const vars = (await client.api.get(`/engine-rest/process-instance/${inst.id}/variables`)).data;
          assert(vars.reason?.value === 'insufficient funds', `bpmnError variables: ${JSON.stringify(vars.reason)}`);

          const other = await start('C7NoCatch52');
          const n = await fetch('c7-nocatch', other.id);
          const wrongWorker = await c7.post(`/external-task/${n.id}/bpmnError`, { workerId: 'someone-else', errorCode: 'X' });
          assert(wrongWorker.status === 500, `bpmnError by a worker not holding the lock → ${wrongWorker.status}`);
          await c7.post(`/external-task/${n.id}/bpmnError`, { workerId: 'c7-worker', errorCode: 'NOBODY_CATCHES' });
          const incs = await incidents(other.id);
          assert(incs.length >= 1, `an uncaught BPMN error raises an incident, got ${JSON.stringify(incs)}`);
        },
      },
      {
        name: '[E] failure: retries is what remains',
        async fn() {
          const inst = await start('C7NoCatch52');
          const t = await fetch('c7-nocatch', inst.id);
          let r = await c7.post(`/external-task/${t.id}/failure`, { workerId: 'c7-worker', errorMessage: 'timeout', retries: 1, retryTimeout: 0 });
          assert(r.status === 200 || r.status === 204, `failure → ${r.status}`);
          assert((await incidents(inst.id)).length === 0, 'one retry left: no incident yet');
          const retry = await fetch('c7-nocatch', inst.id);
          assert(retry.id === t.id && retry.retries === 1, `fetched again with 1 retry: ${JSON.stringify(retry)}`);

          r = await c7.post(`/external-task/${retry.id}/failure`, { workerId: 'c7-worker', errorMessage: 'still down', retries: 0 });
          assert((await incidents(inst.id)).length === 1, 'retries 0: an incident');
        },
      },
      {
        name: '[F] Long polling: a task created while fetchAndLock waits is returned',
        async fn() {
          const topic = 'c7-nocatch';
          // Drain what may be left from earlier tests.
          await c7.post('/external-task/fetchAndLock', { workerId: 'drain', maxTasks: 100, topics: [{ topicName: topic, lockDuration: 60000 }] });

          const began = Date.now();
          const waiting = c7.post('/external-task/fetchAndLock', {
            workerId: 'long-poller', maxTasks: 1, asyncResponseTimeout: 15000,
            topics: [{ topicName: topic, lockDuration: 30000 }],
          });
          await sleep(1500);
          const inst = await start('C7NoCatch52');
          const res = await waiting;
          const took = Date.now() - began;
          assert(res.status === 200 && res.data.length === 1 && res.data[0].processInstanceId === inst.id,
            `long poll should return the new task: ${JSON.stringify(res.data)}`);
          assert(took < 10000, `returned as the task appeared, not at the timeout (${took} ms)`);

          const empty = Date.now();
          const none = await c7.post('/external-task/fetchAndLock', {
            workerId: 'long-poller', maxTasks: 1, asyncResponseTimeout: 1500, topics: [{ topicName: `nothing-${run}`, lockDuration: 1000 }],
          });
          assert(none.data.length === 0 && Date.now() - empty >= 1400, 'with nothing to fetch it waits asyncResponseTimeout, then answers empty');
          await c7.post(`/external-task/${res.data[0].id}/complete`, { workerId: 'long-poller' });
        },
      },
    ],
  });
}

main().catch(err => { console.error(err); process.exit(1); });
