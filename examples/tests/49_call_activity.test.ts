/**
 * Suite 49 — Call activities, Camunda 8 style  (BPMN: 49_call_activity.bpmn)
 *
 * Tests:
 *   A) processId from a FEEL expression; the child gets the parent's
 *      variables plus the input mappings; all child variables come back.
 *   B) propagateAll*Variables="false": the child gets only the input
 *      mappings, and only the output mapping comes back.
 *   C) A BPMN error thrown by a job of the called process, uncaught there,
 *      is caught by the call activity's error boundary event; the called
 *      instance is terminated.
 *   D) The same for an error end event in the called process.
 *   E) An escalation in the called process is caught by a non-interrupting
 *      boundary event on the call activity; the called process carries on.
 *   F) A boundary timer interrupting the call activity terminates the
 *      called instance.
 *   G) Terminating the calling instance terminates the called one.
 *   H) Multi-instance call activity: one called instance per element, their
 *      results collected in order.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '49_call_activity.bpmn');
const client = new GoFlowClient();
const run = Date.now().toString(36);

type Vars = Record<string, { value?: any }>;
const plain = (vars: Record<string, any>) => Object.fromEntries(Object.entries(vars).map(([k, v]) => [k, v?.value]));

async function start(key: string, vars: Record<string, unknown>) {
  const inst = await client.startProcess(key, vars);
  return (inst.processInstanceId ?? inst.id) as string;
}

/** Waits for the open user task taskKey whose instance has variable run = marker. */
async function findTaskByMarker(taskKey: string, marker: string, timeoutMs = 10000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    for (const t of (await client.getTasks()).filter(t => t.taskDefinitionKey === taskKey)) {
      const vars = await client.getProcessVariables(t.processInstanceId) as Vars;
      if (vars.marker?.value === marker) return t;
    }
    await sleep(300);
  }
  throw new Error(`no open "${taskKey}" task with marker ${marker}`);
}

/** Fetches and completes the next job of topic whose variables have marker. */
async function workJob(topic: string, marker: string, handler: (vars: Record<string, any>) => Record<string, unknown> | 'BAD') {
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    for (const job of await client.fetchAndLock([{ name: topic }])) {
      const vars = plain(job.variables);
      if (vars.marker !== marker) continue;
      const result = handler(vars);
      if (result === 'BAD') {
        await client.api.post(`/engine-rest/external-task/${job.id}/failure`, { workerId: 'test', errorMessage: 'bad thing', errorCode: 'BAD', retries: 0 });
      } else {
        await client.completeExternalTask(job.id, result);
      }
      return { vars, instanceId: job.processInstanceId };
    }
    await sleep(300);
  }
  throw new Error(`no "${topic}" job with marker ${marker}`);
}

async function statusOf(id: string) {
  return (await client.getProcessInstance(id))?.status;
}

async function waitStatus(id: string, wanted: string, timeoutMs = 8000) {
  const deadline = Date.now() + timeoutMs;
  let s: string | undefined;
  while (Date.now() < deadline) {
    s = await statusOf(id);
    if (s === wanted) return;
    await sleep(300);
  }
  throw new Error(`instance ${id} should be ${wanted}, is ${s}`);
}

async function finish(id: string, taskKey: string) {
  const t = await client.waitForTask(taskKey, id, 10000);
  await client.completeTask(t.id);
  await client.waitForProcessEnd(id, 10000);
}

async function main() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Call Activity Test');

  await runSuite({
    name: '49 — Call activities',
    tests: [
      {
        name: '[A] FEEL processId, parent variables and input mappings in, child variables out',
        async fn() {
          const marker = `A-${run}`;
          const id = await start('CallVars49', { marker, childId: 'Child49', amount: 21 });
          const job = await workJob('child49', marker, () => ({ childResult: 7 }));
          assert(job.instanceId !== id, 'the job runs in the called instance');
          assert(job.vars.amount === 21 && job.vars.doubled === 42, `the child sees parent variables and inputs: ${JSON.stringify(job.vars)}`);
          await client.waitForTask('v_check', id, 10000);
          const vars = await client.getProcessVariables(id) as Vars;
          assert(vars.childResult?.value === 7, `child variables come back: ${JSON.stringify(vars.childResult)}`);
          await finish(id, 'v_check');
        },
      },
      {
        name: '[B] No propagation: only the mappings cross',
        async fn() {
          const marker = `B-${run}`;
          const id = await start('CallMapped49', { marker, tag: marker, secret: 'parent only' });
          const deadline = Date.now() + 15000;
          let job: any;
          while (!job && Date.now() < deadline) {
            for (const j of await client.fetchAndLock([{ name: 'child49' }])) {
              const vars = plain(j.variables);
              if (vars.tag === marker) { job = { j, vars }; break; }
            }
            if (!job) await sleep(300);
          }
          assert(!!job, 'the called instance runs');
          assert(job.vars.secret === undefined && job.vars.marker === undefined, `only the input mapping goes in: ${JSON.stringify(job.vars)}`);
          await client.completeExternalTask(job.j.id, { childResult: 7, noise: 1 });
          await client.waitForTask('m_check', id, 10000);
          const vars = await client.getProcessVariables(id) as Vars;
          assert(vars.mapped?.value === 8, `the output mapping comes back: ${JSON.stringify(vars.mapped)}`);
          assert(vars.childResult === undefined && vars.noise === undefined, `nothing else comes back: ${Object.keys(vars)}`);
          await finish(id, 'm_check');
        },
      },
      {
        name: '[C] A job error uncaught in the called process is caught by the call activity',
        async fn() {
          const marker = `C-${run}`;
          const id = await start('CallError49', { marker, childId: 'Child49' });
          const job = await workJob('child49', marker, () => 'BAD');
          await client.waitForTask('e_handled', id, 10000);
          await waitStatus(job.instanceId, 'terminated');
          const incidents = await client.api.get(`/engine-rest/process-instance/${job.instanceId}/incidents`).then(r => r.data).catch(() => []);
          assert(!Array.isArray(incidents) || incidents.length === 0, `no incident in the called instance: ${JSON.stringify(incidents)}`);
          await finish(id, 'e_handled');
        },
      },
      {
        name: '[D] An error end event in the called process is caught by the call activity',
        async fn() {
          const id = await start('CallError49', { marker: `D-${run}`, childId: 'ChildErrorEnd49' });
          await finish(id, 'e_handled');
        },
      },
      {
        name: '[E] A non-interrupting escalation from the called process',
        async fn() {
          const marker = `E-${run}`;
          const id = await start('CallEscalation49', { marker });
          await client.waitForTask('s_escalated', id, 10000);
          const childTask = await findTaskByMarker('cs_after', marker);
          assert(childTask.processInstanceId !== id, 'the called process carries on');
          await client.completeTask(childTask.id);
          const escalated = await client.waitForTask('s_escalated', id, 10000);
          await client.completeTask(escalated.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[F] Interrupting the call activity terminates the called instance',
        async fn() {
          const marker = `F-${run}`;
          const id = await start('CallTimeout49', { marker });
          const childTask = await findTaskByMarker('cw_task', marker);
          await client.waitForTask('t_late', id, 10000);
          await waitStatus(childTask.processInstanceId, 'terminated');
          await finish(id, 't_late');
        },
      },
      {
        name: '[G] Terminating the caller terminates the called instance',
        async fn() {
          const marker = `G-${run}`;
          const id = await start('CallPlain49', { marker });
          const childTask = await findTaskByMarker('cw_task', marker);
          const res = await client.api.delete(`/engine-rest/process-instance/${id}?deleteReason=suite-49`);
          assert(res.status === 204, `terminate: ${res.status}`);
          await waitStatus(childTask.processInstanceId, 'terminated');
        },
      },
      {
        name: '[H] Multi-instance call activity',
        async fn() {
          const marker = `H-${run}`;
          const id = await start('CallMulti49', { marker });
          // Both called instances' jobs, completed as fetched (a fetch locks
          // every job it returns).
          let done = 0;
          const deadline = Date.now() + 15000;
          while (done < 2 && Date.now() < deadline) {
            for (const job of await client.fetchAndLock([{ name: 'child49' }])) {
              const vars = plain(job.variables);
              if (vars.marker !== marker) continue;
              await client.completeExternalTask(job.id, { childResult: vars.n * 100 });
              done++;
            }
            if (done < 2) await sleep(300);
          }
          assert(done === 2, `two called instances, ${done} jobs worked`);
          await client.waitForTask('mi_check', id, 10000);
          const vars = await client.getProcessVariables(id) as Vars;
          assert(JSON.stringify(vars.results?.value) === '[100,200]', `results per element, in order: ${JSON.stringify(vars.results)}`);
          await finish(id, 'mi_check');
        },
      },
    ],
  });
}

main().catch(err => { console.error(err); process.exit(1); });
