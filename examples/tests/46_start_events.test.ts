/**
 * Suite 46 — Timer and signal start events  (BPMN: 46_*.bpmn)
 *
 * Tests:
 *   A) A timer cycle start event (R2/PT2S) starts exactly two instances.
 *   B) A timer date start event already past starts one instance right away.
 *   C) Deploying a new version without the timer start event stops the old
 *      version's cycle.
 *   D) A signal start event starts an instance with the signal's variables;
 *      the process's none start event still works.
 *   E) An invalid timer cycle rejects the deployment.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const bpmn = (f: string) => path.join(__dirname, 'bpmn', f);
const client = new GoFlowClient();

async function openTasks(taskKey: string) {
  const tasks = await client.getTasks();
  return tasks.filter(t => t.taskDefinitionKey === taskKey);
}

async function completeAll(taskKey: string) {
  for (const t of await openTasks(taskKey)) {
    await client.completeTask(t.id);
  }
}

async function main() {
  await client.loginAsSuperUser();

  await runSuite({
    name: '46 — Timer and signal start events',
    tests: [
      {
        name: '[A] + [B] Timer cycle and timer date start events start instances',
        async fn() {
          await client.deployBpmn(bpmn('46_start_events.bpmn'), 'Start Events Test');

          const deadline = Date.now() + 10000;
          while (Date.now() < deadline && (await openTasks('tc_task')).length < 2) await sleep(500);
          await sleep(3000); // a third firing would have come by now
          const cycle = await openTasks('tc_task');
          assert(cycle.length === 2, `R2/PT2S should start exactly 2 instances, got ${cycle.length}`);

          const date = await openTasks('td_task');
          assert(date.length === 1, `a past timeDate should start exactly 1 instance, got ${date.length}`);

          await completeAll('tc_task');
          await completeAll('td_task');
        },
      },
      {
        name: '[C] A new version without the timer start event stops the old cycle',
        async fn() {
          await client.deployBpmn(bpmn('46_timer_forever_v1.bpmn'), 'Timer Forever v1');
          const deadline = Date.now() + 8000;
          while (Date.now() < deadline && (await openTasks('tf_task')).length < 2) await sleep(300);
          assert((await openTasks('tf_task')).length >= 2, 'version 1 should tick every second');

          await client.deployBpmn(bpmn('46_timer_forever_v2.bpmn'), 'Timer Forever v2');
          await sleep(1500); // a tick already claimed may still land
          const after = (await openTasks('tf_task')).length;
          await sleep(3000);
          const later = (await openTasks('tf_task')).length;
          assert(later === after, `no more ticks after version 2: ${after} then ${later}`);
          await completeAll('tf_task');
        },
      },
      {
        name: '[D] A signal start event starts an instance with the signal variables',
        async fn() {
          const before = new Set((await openTasks('ss_task')).map(t => t.processInstanceId));
          const res = await client.api.post('/v2/signals/broadcast', { signalName: 'Go46', variables: { reason: 'signal' } });
          assert(res.data.resumed >= 1, `the broadcast should report the started instance, got ${JSON.stringify(res.data)}`);

          const deadline = Date.now() + 5000;
          let started = (await openTasks('ss_task')).filter(t => !before.has(t.processInstanceId));
          while (Date.now() < deadline && started.length === 0) {
            await sleep(300);
            started = (await openTasks('ss_task')).filter(t => !before.has(t.processInstanceId));
          }
          assert(started.length === 1, `one instance should start, got ${started.length}`);
          const vars = await client.getProcessVariables(started[0].processInstanceId) as Record<string, { value?: any }>;
          assert(vars.reason?.value === 'signal', `the signal variables start the instance, got ${JSON.stringify(vars.reason)}`);
          await completeAll('ss_task');

          // The none start event still starts it directly.
          const inst = await client.startProcess('SignalStart46', {});
          const id = inst.processInstanceId ?? inst.id;
          const task = await client.waitForTask('ss_task', id, 10000);
          await client.completeTask(task.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[E] An invalid timer cycle rejects the deployment',
        async fn() {
          let status = 0;
          try {
            await client.deployBpmn(bpmn('46_timer_invalid.bpmn'), 'Timer Invalid');
          } catch (e: any) {
            status = e.response?.status;
          }
          assert(status >= 400 && status < 500, `deployment should be refused with a 4xx, got ${status}`);
        },
      },
    ],
  });
}

main().catch(err => { console.error(err); process.exit(1); });
