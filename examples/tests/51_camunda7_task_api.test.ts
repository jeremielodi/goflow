/**
 * Suite 51 — Camunda 7 task API  (BPMN: 51_camunda7_tasks.bpmn)
 *
 * /engine-rest/task/... with Camunda 7's paths, formats and status codes,
 * called the way a Camunda 7 application does (HTTP Basic auth, typed
 * variables), so it runs against GoFlow unchanged.
 *
 * Tests:
 *   A) GET /task filters (processInstanceId, taskDefinitionKey,
 *      processDefinitionKey, unassigned), paging, and /task/count;
 *      the TaskDto fields and Camunda's date format.
 *   B) claim → 204; claimed by someone else → 500 TaskAlreadyClaimedException;
 *      unclaim → 204; assignee → 204.
 *   C) Variables: GET typed (Integer, String, Json), PUT one, POST
 *      modifications; GET /form gives the camunda:formKey.
 *   D) complete with typed variables (a Json string is parsed),
 *      withVariablesInReturn, then the next task; a completed task is a 404.
 *   E) POST /task with a JSON query, and an unknown task is a 404 with
 *      Camunda's error body.
 */
import * as path from 'path';
import axios from 'axios';
import { GoFlowClient, runSuite, assert, BASE_URL } from './client';

const BPMN = path.join(__dirname, 'bpmn', '51_camunda7_tasks.bpmn');
const client = new GoFlowClient();
// A Camunda 7 client: Basic auth, no GoFlow token.
const c7 = axios.create({
  baseURL: `${BASE_URL}/engine-rest`,
  auth: { username: 'admin@goflow.com', password: 'admin123' },
  validateStatus: () => true,
});

async function start(vars: Record<string, unknown>) {
  const inst = await client.startProcess('C7Tasks51', vars);
  return (inst.processInstanceId ?? inst.id) as string;
}

async function taskOf(instanceId: string, key: string) {
  const res = await c7.get('/task', { params: { processInstanceId: instanceId, taskDefinitionKey: key } });
  assert(res.status === 200, `GET /task answered ${res.status}: ${JSON.stringify(res.data)}`);
  assert(res.data.length === 1, `expected one ${key} task, got ${JSON.stringify(res.data)}`);
  return res.data[0];
}

async function main() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Camunda 7 task API');

  await runSuite({
    name: '51 — Camunda 7 task API',
    tests: [
      {
        name: '[A] GET /task filters, paging, /task/count and the TaskDto',
        async fn() {
          const i1 = await start({ amount: 10 });
          const i2 = await start({ amount: 20 });
          const t = await taskOf(i1, 'c7_review');
          assert(t.name === 'Review order' && t.processInstanceId === i1 && t.assignee === null, `TaskDto: ${JSON.stringify(t)}`);
          assert(typeof t.processDefinitionId === 'string' && t.suspended === false, `TaskDto: ${JSON.stringify(t)}`);
          assert(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}[+-]\d{4}$/.test(t.created), `Camunda date format expected, got ${t.created}`);
          for (const k of ['owner', 'due', 'followUp', 'delegationState', 'description', 'parentTaskId', 'caseInstanceId', 'tenantId']) {
            assert(k in t, `TaskDto field ${k} missing`);
          }

          const byKey = await c7.get('/task', { params: { processDefinitionKey: 'C7Tasks51', unassigned: true } });
          const ids = byKey.data.map((x: any) => x.processInstanceId);
          assert(ids.includes(i1) && ids.includes(i2), `processDefinitionKey filter missed instances: ${JSON.stringify(ids)}`);

          const count = await c7.get('/task/count', { params: { processDefinitionKey: 'C7Tasks51' } });
          assert(count.status === 200 && count.data.count >= 2, `count: ${JSON.stringify(count.data)}`);
          const page = await c7.get('/task', { params: { processDefinitionKey: 'C7Tasks51', firstResult: 0, maxResults: 1 } });
          assert(page.data.length === 1, `maxResults=1 should give one task, got ${page.data.length}`);
        },
      },
      {
        name: '[B] claim, claimed by someone else, unclaim, assignee',
        async fn() {
          const id = await start({});
          const t = await taskOf(id, 'c7_review');
          let r = await c7.post(`/task/${t.id}/claim`, { userId: 'demo' });
          assert(r.status === 204, `claim → ${r.status} ${JSON.stringify(r.data)}`);
          assert((await c7.get(`/task/${t.id}`)).data.assignee === 'demo', 'assignee should be demo');

          r = await c7.post(`/task/${t.id}/claim`, { userId: 'john' });
          assert(r.status === 500 && r.data.type === 'TaskAlreadyClaimedException', `claim by another → ${r.status} ${JSON.stringify(r.data)}`);

          r = await c7.post(`/task/${t.id}/unclaim`);
          assert(r.status === 204 && (await c7.get(`/task/${t.id}`)).data.assignee === null, `unclaim → ${r.status}`);

          r = await c7.post(`/task/${t.id}/assignee`, { userId: 'mary' });
          assert(r.status === 204 && (await c7.get(`/task/${t.id}`)).data.assignee === 'mary', `assignee → ${r.status}`);
        },
      },
      {
        name: '[C] Variables (typed), PUT, modifications, form key',
        async fn() {
          const id = await start({ amount: 42, customer: 'ACME', items: [{ sku: 'A1' }] });
          const t = await taskOf(id, 'c7_review');
          const vars = (await c7.get(`/task/${t.id}/variables`)).data;
          assert(vars.amount?.type === 'Integer' && vars.amount.value === 42, `amount: ${JSON.stringify(vars.amount)}`);
          assert(vars.customer?.type === 'String' && vars.customer.value === 'ACME', `customer: ${JSON.stringify(vars.customer)}`);
          assert(vars.items?.type === 'Json' && JSON.parse(vars.items.value)[0].sku === 'A1', `items: ${JSON.stringify(vars.items)}`);

          let r = await c7.put(`/task/${t.id}/variables/comment`, { value: 'looks good', type: 'String' });
          assert(r.status === 204, `PUT variable → ${r.status} ${JSON.stringify(r.data)}`);
          r = await c7.post(`/task/${t.id}/variables`, { modifications: { score: { value: 7, type: 'Integer' } } });
          assert(r.status === 204, `POST modifications → ${r.status} ${JSON.stringify(r.data)}`);
          const one = (await c7.get(`/task/${t.id}/variables/comment`)).data;
          assert(one.value === 'looks good', `variable after PUT: ${JSON.stringify(one)}`);
          assert((await c7.get(`/task/${t.id}/variables/score`)).data.value === 7, 'score should be 7');

          const form = (await c7.get(`/task/${t.id}/form`)).data;
          assert(form.key === 'embedded:app:forms/review.html', `form key: ${JSON.stringify(form)}`);
        },
      },
      {
        name: '[D] complete with typed variables, withVariablesInReturn, then the next task',
        async fn() {
          const id = await start({ amount: 5 });
          const t = await taskOf(id, 'c7_review');
          const r = await c7.post(`/task/${t.id}/complete`, {
            variables: { approved: { value: true, type: 'Boolean' }, address: { value: '{"city":"Kinshasa"}', type: 'Json' } },
            withVariablesInReturn: true,
          });
          assert(r.status === 200, `complete → ${r.status} ${JSON.stringify(r.data)}`);
          assert(r.data.approved?.value === true && r.data.amount?.value === 5, `returned variables: ${JSON.stringify(r.data)}`);

          const next = await taskOf(id, 'c7_approve');
          const vars = (await c7.get(`/task/${next.id}/variables`)).data;
          assert(vars.address?.type === 'Json' && JSON.parse(vars.address.value).city === 'Kinshasa', `Json variable stored as an object: ${JSON.stringify(vars.address)}`);

          const gone = await c7.get(`/task/${t.id}`);
          assert(gone.status === 404 && gone.data.type === 'InvalidRequestException', `completed task → ${gone.status}`);

          const done = await c7.post(`/task/${next.id}/complete`, {});
          assert(done.status === 204, `complete without body → ${done.status}`);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[E] POST /task with a JSON query; unknown task',
        async fn() {
          const id = await start({});
          const r = await c7.post('/task', { processInstanceIdIn: [id], sorting: [{ sortBy: 'created', sortOrder: 'desc' }] });
          assert(r.status === 200 && r.data.length === 1 && r.data[0].taskDefinitionKey === 'c7_review', `POST /task: ${JSON.stringify(r.data)}`);
          const count = await c7.post('/task/count', { processInstanceIdIn: [id] });
          assert(count.data.count === 1, `POST /task/count: ${JSON.stringify(count.data)}`);

          const unknown = await c7.get('/task/00000000-0000-4000-8000-000000000000');
          assert(unknown.status === 404 && unknown.data.type === 'InvalidRequestException' && typeof unknown.data.message === 'string',
            `unknown task → ${unknown.status} ${JSON.stringify(unknown.data)}`);
        },
      },
    ],
  });
}

main().catch(err => { console.error(err); process.exit(1); });
