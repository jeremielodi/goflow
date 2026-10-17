/**
 * Suite 48 — Variable scopes and multi-instance, Camunda 8 style
 *            (BPMN: 48_scopes_multi_instance.bpmn)
 *
 * Tests:
 *   A) Parallel multi-instance service task: each job sees its own
 *      inputElement, outputElement values land in outputCollection in loop
 *      order, and the iteration's local variables don't leak.
 *   B) Sequential multi-instance by cardinality: one job at a time, with
 *      loopCounter 1, 2, 3.
 *   C) Completion condition: two of three reviews complete the loop and the
 *      third task is cancelled.
 *   D) Multi-instance embedded sub-process with an input mapping: the
 *      mapping is local to each iteration and feeds outputElement.
 *   E) Sub-process input mappings stay local (a job's value for one updates
 *      the local), output mappings read the local scope; an activity's
 *      output mappings set only their targets.
 *   F) A branch ending on a task without outgoing flow ends just that
 *      branch, not the instance (it used to complete the whole instance).
 *   G) Empty input collection: the loop is skipped with an empty output
 *      collection; a boundary timer on a multi-instance activity interrupts
 *      every inner instance.
 */
import * as path from 'path';
import { GoFlowClient, ExternalTask, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '48_scopes_multi_instance.bpmn');
const client = new GoFlowClient();

type Vars = Record<string, { value?: any }>;
const plain = (t: ExternalTask) => Object.fromEntries(Object.entries(t.variables).map(([k, v]) => [k, (v as any)?.value]));

/** Completes the jobs of topic for instanceId as they come, until count are done. */
async function workJobs(topic: string, instanceId: string, count: number, handler: (vars: Record<string, any>) => Record<string, unknown>, timeoutMs = 15000) {
  const seen: Record<string, any>[] = [];
  const deadline = Date.now() + timeoutMs;
  while (seen.length < count && Date.now() < deadline) {
    const jobs = (await client.fetchAndLock([{ name: topic }])).filter(j => j.processInstanceId === instanceId);
    for (const job of jobs) {
      const vars = plain(job);
      seen.push(vars);
      await client.completeExternalTask(job.id, handler(vars));
    }
    if (jobs.length === 0) await sleep(300);
  }
  assert(seen.length === count, `expected ${count} "${topic}" jobs, got ${seen.length}`);
  return seen;
}

async function tasksOf(id: string, key: string) {
  return (await client.getTasks(id)).filter(t => t.taskDefinitionKey === key);
}

async function main() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Scopes and Multi-Instance Test');

  await runSuite({
    name: '48 — Variable scopes and multi-instance',
    tests: [
      {
        name: '[A] Parallel multi-instance: own element per instance, ordered output collection',
        async fn() {
          const inst = await client.startProcess('MiParallel48', { items: [3, 1, 2] });
          const id = inst.processInstanceId ?? inst.id;
          const seen = await workJobs('mi48-parallel', id, 3, v => ({ result: v.item * 10 }));
          const items = seen.map(v => v.item).sort();
          assert(JSON.stringify(items) === JSON.stringify([1, 2, 3]), `each job should see its own item, saw ${JSON.stringify(seen.map(v => v.item))}`);
          assert(seen.every(v => typeof v.loopCounter === 'number'), 'each job sees its loopCounter');

          await client.waitForTask('mp_check', id, 10000);
          const vars = await client.getProcessVariables(id) as Vars;
          assert(JSON.stringify(vars.results?.value) === JSON.stringify([30, 10, 20]), `results in loop order, got ${JSON.stringify(vars.results)}`);
          assert(vars.item === undefined && vars.loopCounter === undefined, `iteration locals must not leak: ${Object.keys(vars)}`);
          const check = await tasksOf(id, 'mp_check');
          await client.completeTask(check[0].id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[B] Sequential multi-instance by cardinality',
        async fn() {
          const inst = await client.startProcess('MiSequential48', { count: 3 });
          const id = inst.processInstanceId ?? inst.id;
          const counters: number[] = [];
          for (let i = 0; i < 3; i++) {
            await sleep(300);
            const open = (await client.fetchAndLock([{ name: 'mi48-sequential' }])).filter(j => j.processInstanceId === id);
            assert(open.length === 1, `sequential: one job at a time, got ${open.length}`);
            counters.push(plain(open[0]).loopCounter);
            await client.completeExternalTask(open[0].id, {});
          }
          assert(JSON.stringify(counters) === '[1,2,3]', `loopCounter 1, 2, 3, got ${JSON.stringify(counters)}`);
          const check = await client.waitForTask('ms_check', id, 10000);
          await client.completeTask(check.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[C] Completion condition ends the loop early',
        async fn() {
          const inst = await client.startProcess('MiCondition48', { reviewers: ['ann', 'bob', 'cid'] });
          const id = inst.processInstanceId ?? inst.id;
          const deadline = Date.now() + 10000;
          let reviews = await tasksOf(id, 'mc_review');
          while (reviews.length < 3 && Date.now() < deadline) { await sleep(300); reviews = await tasksOf(id, 'mc_review'); }
          assert(reviews.length === 3, `three reviews, got ${reviews.length}`);

          await client.completeTask(reviews[0].id, { approved: true });
          await sleep(300);
          assert((await tasksOf(id, 'mc_done')).length === 0, 'one review is not enough');
          await client.completeTask(reviews[1].id, { approved: true });
          const done = await client.waitForTask('mc_done', id, 10000);
          assert((await tasksOf(id, 'mc_review')).length === 0, 'the third review is cancelled');
          await client.completeTask(done.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[D] Multi-instance sub-process with a local input mapping',
        async fn() {
          const inst = await client.startProcess('MiSubProcess48', { items: [1, 2, 3] });
          const id = inst.processInstanceId ?? inst.id;
          const seen = await workJobs('mi48-sub', id, 3, () => ({ bonus: 1 }));
          assert(JSON.stringify(seen.map(v => v.double).sort()) === '[2,4,6]', `each iteration sees its own double, saw ${JSON.stringify(seen.map(v => v.double))}`);
          await client.waitForTask('msp_check', id, 10000);
          const vars = await client.getProcessVariables(id) as Vars;
          assert(JSON.stringify(vars.doubles?.value) === '[3,5,7]', `doubles = double + bonus per item, got ${JSON.stringify(vars.doubles)}`);
          assert(vars.double === undefined && vars.item === undefined, `sub-process locals must not leak: ${Object.keys(vars)}`);
          const check = await tasksOf(id, 'msp_check');
          await client.completeTask(check[0].id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[E] Sub-process scope and output mappings',
        async fn() {
          const inst = await client.startProcess('Scopes48', { tmp: 'outer' });
          const id = inst.processInstanceId ?? inst.id;
          const seen = await workJobs('scopes48', id, 1, () => ({ tmp: 'changed inside', created: 'inside' }));
          assert(seen[0].tmp === 'inner', `inside the sub-process its local tmp hides the outer one, got ${seen[0].tmp}`);
          await workJobs('scopes48-mapped', id, 1, () => ({ raw: 41 }));
          await client.waitForTask('sc_check', id, 10000);
          const vars = await client.getProcessVariables(id) as Vars;
          assert(vars.tmp?.value === 'outer', `the outer tmp is untouched, got ${JSON.stringify(vars.tmp)}`);
          assert(vars.tmpSeen?.value === 'changed inside', `the output mapping reads the local scope, got ${JSON.stringify(vars.tmpSeen)}`);
          assert(vars.created?.value === 'inside', 'a new variable from inside propagates to the instance');
          assert(vars.cooked?.value === 42 && vars.raw === undefined, `only the output mapping target is set: ${JSON.stringify(vars)}`);
          const check = await tasksOf(id, 'sc_check');
          await client.completeTask(check[0].id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[F] A task without outgoing flow ends its branch only',
        async fn() {
          const inst = await client.startProcess('ImplicitEnd48', {});
          const id = inst.processInstanceId ?? inst.id;
          await workJobs('implicit48', id, 1, () => ({}));
          await sleep(500);
          const other = await tasksOf(id, 'ie_other');
          assert(other.length === 1, 'the other branch keeps going');
          const live = await client.getProcessInstance(id);
          assert(live?.status === 'running' || live?.status === 'active' || (live as any)?.state === 'ACTIVE', `the instance is still running, got ${JSON.stringify(live?.status)}`);
          await client.completeTask(other[0].id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[G] Empty collection skips; a boundary timer interrupts the whole loop',
        async fn() {
          const inst = await client.startProcess('MiEdges48', {});
          const id = inst.processInstanceId ?? inst.id;
          const deadline = Date.now() + 8000;
          let loop = await tasksOf(id, 'me_loop');
          while (loop.length < 2 && Date.now() < deadline) { await sleep(300); loop = await tasksOf(id, 'me_loop'); }
          assert(loop.length === 2, `two inner instances, got ${loop.length}`);
          const vars = await client.getProcessVariables(id) as Vars;
          assert(JSON.stringify(vars.nothing?.value) === '[]', `an empty collection gives an empty output, got ${JSON.stringify(vars.nothing)}`);

          const late = await client.waitForTask('me_late', id, 10000);
          assert((await tasksOf(id, 'me_loop')).length === 0, 'the timer cancelled both inner instances');
          await client.completeTask(late.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
    ],
  });
}

main().catch(err => { console.error(err); process.exit(1); });
