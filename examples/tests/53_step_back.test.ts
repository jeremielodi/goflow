/**
 * Suite 53 — "Previous step" (POST /v2/process-instances/:id/step-back)
 *
 * Sends a token back to the wait state it went through before. The token
 * is moved in place: its current task or job is cancelled (with boundary
 * timers and incidents), and the earlier element runs again with a new
 * task or job. Variables are kept.
 *
 * Tests:
 *   A) Nothing to go back to at the first task: 409 STEP_BACK_REFUSED.
 *   B) approve → check: dry run says where, apply cancels approve's task and
 *      its boundary timer, a new check job appears, variables are kept.
 *   C) Pressed twice: approve → check → fill (not back to approve); a job
 *      a worker had locked can't be completed afterwards; the process then
 *      runs to its end again.
 *   D) Parallel branches: a branch token can't step back over the fork.
 *   E) Token history shows the step back.
 *   F) Permission: a user with only VIEW on the process gets 404.
 */
import * as path from 'path';
import { execSync } from 'child_process';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '53_step_back.bpmn');
const PARALLEL_BPMN = path.join(__dirname, 'bpmn', '53_step_back_parallel.bpmn');
const client = new GoFlowClient();

function psql(sql: string): string {
  return execSync(`docker compose exec -T postgres psql -U goflow -d goflow -tAc "${sql}"`, {
    cwd: path.join(__dirname, '..', '..'),
  }).toString().trim();
}

async function openTasks(instanceId: string) {
  const tasks = await client.getTasks(instanceId);
  return tasks.filter(t => t.status === 'created' || t.status === 'claimed');
}

async function waitOpenTask(instanceId: string, key: string) {
  for (let i = 0; i < 30; i++) {
    const t = (await openTasks(instanceId)).find(t => t.taskDefinitionKey === key);
    if (t) return t;
    await sleep(300);
  }
  throw new Error(`no open task ${key} in ${instanceId}`);
}

async function lockJob(instanceId: string) {
  for (let i = 0; i < 30; i++) {
    const jobs = await client.fetchAndLock([{ name: 'step_back_check' }]);
    const job = jobs.find(j => j.processInstanceId === instanceId);
    if (job) return job;
    await sleep(300);
  }
  throw new Error(`no step_back_check job for ${instanceId}`);
}

function stepBack(c: GoFlowClient, instanceId: string, body: Record<string, unknown>) {
  return c.api.post(`/v2/process-instances/${instanceId}/step-back`, body).catch(e => e.response);
}

async function tokenAt(instanceId: string, elementId: string): Promise<string> {
  const res = await client.api.get('/engine-rest/history/activity-instance', { params: { processInstanceId: instanceId } });
  const exec = res.data.find((a: any) => a.activityId === elementId && !a.endTime);
  assert(exec, `no active token at ${elementId}`);
  return exec.id;
}

// Runs a fresh instance up to "approve".
async function startToApprove(vars: Record<string, unknown>) {
  const inst = await client.startProcess('StepBackProcess', vars);
  const id = inst.processInstanceId ?? inst.id;
  const fill = await waitOpenTask(id, 'fill');
  await client.completeTask(fill.id, { filled: true });
  const job = await lockJob(id);
  await client.completeExternalTask(job.id, { checked: 'yes' });
  await waitOpenTask(id, 'approve');
  return id;
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Step Back Test');
  await client.deployBpmn(PARALLEL_BPMN, 'Step Back Parallel Test');

  await runSuite({
    name: '53 — Previous step (step back a token)',
    tests: [
      {
        name: '[A] No earlier step at the first task: refused',
        async fn() {
          const inst = await client.startProcess('StepBackProcess', {});
          const id = inst.processInstanceId ?? inst.id;
          await waitOpenTask(id, 'fill');
          const res = await stepBack(client, id, { elementId: 'fill', dryRun: true });
          assert(res.status === 409, `expected 409, got ${res.status} ${JSON.stringify(res.data)}`);
          assert(res.data.title === 'STEP_BACK_REFUSED', `unexpected body ${JSON.stringify(res.data)}`);
          await client.api.delete(`/engine-rest/process-instance/${id}`);
        },
      },
      {
        name: '[B] approve → check: task and boundary timer cancelled, new job, variables kept',
        async fn() {
          const id = await startToApprove({ amount: 7 });
          const execId = await tokenAt(id, 'approve');
          assert(psql(`SELECT count(*) FROM timer_jobs WHERE process_instance_id='${id}' AND is_triggered=false`) === '1',
            'expected the boundary timer of approve to be armed');

          const dry = await stepBack(client, id, { executionId: execId, dryRun: true });
          assert(dry.status === 200, `dry run failed: ${dry.status} ${JSON.stringify(dry.data)}`);
          assert(dry.data.toElementId === 'check' && dry.data.applied === false,
            `expected dry run to target check, got ${JSON.stringify(dry.data)}`);
          assert((await openTasks(id)).some(t => t.taskDefinitionKey === 'approve'), 'dry run must not change anything');

          const res = await stepBack(client, id, { executionId: execId });
          assert(res.status === 200 && res.data.applied, `step back failed: ${res.status} ${JSON.stringify(res.data)}`);
          assert(res.data.fromElementId === 'approve' && res.data.toElementId === 'check', JSON.stringify(res.data));
          assert(res.data.executionId === execId, 'the token keeps its execution');

          assert(!(await openTasks(id)).some(t => t.taskDefinitionKey === 'approve'), 'approve task should be cancelled');
          assert(psql(`SELECT count(*) FROM timer_jobs WHERE process_instance_id='${id}' AND is_triggered=false`) === '0',
            'the boundary timer of approve should be gone');
          assert(psql(`SELECT status FROM tasks WHERE process_instance_id='${id}' AND task_definition_key='approve'`) === 'cancelled',
            'approve task row should be cancelled');

          const vars: any = await client.getProcessVariables(id);
          assert(vars.amount?.value === 7 && vars.checked?.value === 'yes',
            `variables should be kept, got ${JSON.stringify(vars)}`);

          const job = await lockJob(id);
          await client.completeExternalTask(job.id, { checked: 'again' });
          const approve = await waitOpenTask(id, 'approve');
          await client.completeTask(approve.id, {});
          await client.waitForProcessEnd(id, 15000);
        },
      },
      {
        name: '[C] Pressed twice reaches fill; a stale locked job is rejected; the process ends',
        async fn() {
          const id = await startToApprove({});
          const execId = await tokenAt(id, 'approve');

          const first = await stepBack(client, id, { executionId: execId });
          assert(first.status === 200 && first.data.toElementId === 'check', JSON.stringify(first.data));

          const staleJob = await lockJob(id);

          const second = await stepBack(client, id, { elementId: 'check' });
          assert(second.status === 200 && second.data.toElementId === 'fill',
            `second step back should reach fill, got ${second.status} ${JSON.stringify(second.data)}`);

          const late = await client.completeExternalTask(staleJob.id, {}).then(() => null, e => e.response);
          assert(late && late.status >= 400, `completing the cancelled job should fail, got ${late?.status}`);

          const third = await stepBack(client, id, { elementId: 'fill', dryRun: true });
          assert(third.status === 409, `nothing before fill, got ${third.status} ${JSON.stringify(third.data)}`);

          const fill = await waitOpenTask(id, 'fill');
          await client.completeTask(fill.id, {});
          const job = await lockJob(id);
          await client.completeExternalTask(job.id, {});
          const approve = await waitOpenTask(id, 'approve');
          await client.completeTask(approve.id, {});
          await client.waitForProcessEnd(id, 15000);
        },
      },
      {
        name: '[D] A parallel branch cannot step back over the fork',
        async fn() {
          const inst = await client.startProcess('StepBackParallel', {});
          const id = inst.processInstanceId ?? inst.id;
          const prepare = await waitOpenTask(id, 'prepare');
          await client.completeTask(prepare.id, {});
          await waitOpenTask(id, 'left');
          await waitOpenTask(id, 'right');

          const res = await stepBack(client, id, { elementId: 'left', dryRun: true });
          assert(res.status === 409, `expected 409, got ${res.status} ${JSON.stringify(res.data)}`);
          assert(/parallel branch/.test(res.data.detail), `unexpected reason ${res.data.detail}`);
          await client.api.delete(`/engine-rest/process-instance/${id}`);
        },
      },
      {
        name: '[E] Token history shows the step back',
        async fn() {
          const id = await startToApprove({});
          await stepBack(client, id, { elementId: 'approve' });
          const res = await client.api.get(`/engine-rest/audit/process/${id}/token-history`);
          const steps = res.data.steps ?? res.data;
          assert(steps.some((s: any) => s.detail === 'Stepped back: approve → check'),
            `expected a step back entry, got ${JSON.stringify(steps.map((s: any) => s.detail))}`);
          await client.api.delete(`/engine-rest/process-instance/${id}`);
        },
      },
      {
        name: '[F] A user with only VIEW on the process gets 404',
        async fn() {
          const id = await startToApprove({});
          const email = `step-back-viewer-${Date.now()}@goflow.com`;
          const created = await client.api.post('/users', { email, full_name: email, password: 'password123' });
          const viewer = new GoFlowClient();
          await viewer.login(email, 'password123');
          await client.api.post('/engine-rest/process-definition/key/StepBackProcess/permissions', {
            granteeType: 'user', granteeId: created.data.user.id, permission: 'VIEW',
          });

          const denied = await stepBack(viewer, id, { elementId: 'approve' });
          assert(denied.status === 404, `expected 404 without MANAGE, got ${denied.status}`);
          const deniedModify = await viewer.api.post(`/v2/process-instances/${id}/modification`, {
            moveInstructions: [{ sourceElementId: 'approve', targetElementId: 'fill' }],
          }).catch(e => e.response);
          assert(deniedModify.status === 404, `expected 404 on modification without MANAGE, got ${deniedModify.status}`);
          assert((await openTasks(id)).some(t => t.taskDefinitionKey === 'approve'), 'the token must not have moved');

          const allowed = await stepBack(client, id, { elementId: 'approve' });
          assert(allowed.status === 200, `superuser should still step back, got ${allowed.status}`);
          await client.api.delete(`/engine-rest/process-instance/${id}`);
        },
      },
    ],
  });
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
