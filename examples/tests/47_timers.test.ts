/**
 * Suite 47 — Camunda 8 timers  (BPMN: 47_timers.bpmn)
 *
 * Tests:
 *   A) A non-interrupting timer-cycle event sub-process (R3/PT1S) fires
 *      three times; each run carries on independently of the next firing.
 *   B) Non-interrupting boundary timers: an ISO cycle (R2/PT1S) fires twice,
 *      a cron (every second) keeps firing; a P1D timer doesn't fire.
 *   C) timeDuration and timeDate from FEEL expressions over the variables.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '47_timers.bpmn');
const client = new GoFlowClient();

async function tasksOf(instanceId: string, taskKey: string) {
  return (await client.getTasks(instanceId)).filter(t => t.taskDefinitionKey === taskKey);
}

async function main() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Timers Test');

  await runSuite({
    name: '47 — Camunda 8 timers',
    tests: [
      {
        name: '[A] A non-interrupting timer-cycle event sub-process fires on every cycle',
        async fn() {
          const inst = await client.startProcess('TimerEsCycle47', {});
          const id = inst.processInstanceId ?? inst.id;
          const deadline = Date.now() + 10000;
          while (Date.now() < deadline && (await tasksOf(id, 'ec_tick')).length < 3) await sleep(500);
          await sleep(2000); // a fourth firing would have come by now
          const ticks = await tasksOf(id, 'ec_tick');
          assert(ticks.length === 3, `R3/PT1S should run the event sub-process 3 times, got ${ticks.length}`);
          assert((await tasksOf(id, 'ec_main')).length === 1, 'the main work goes on');

          for (const t of ticks) await client.completeTask(t.id);
          const main = await tasksOf(id, 'ec_main');
          await client.completeTask(main[0].id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[B] Non-interrupting boundary timers: ISO cycle, cron; P1D does not fire',
        async fn() {
          const inst = await client.startProcess('TimerBoundaryCycle47', {});
          const id = inst.processInstanceId ?? inst.id;
          await client.waitForTask('bc_work', id, 10000);
          await sleep(4500);

          const iso = await tasksOf(id, 'bc_iso_task');
          const cron = await tasksOf(id, 'bc_cron_task');
          assert(iso.length === 2, `R2/PT1S should fire twice, got ${iso.length}`);
          assert(cron.length >= 3, `a every-second cron should keep firing, got ${cron.length}`);
          assert((await tasksOf(id, 'bc_work')).length === 1, 'the P1D interrupting timer must not have fired');

          // Leaving the activity disarms its boundary timers.
          const work = await tasksOf(id, 'bc_work');
          await client.completeTask(work[0].id);
          await sleep(2500);
          const cronAfter = await tasksOf(id, 'bc_cron_task');
          await sleep(2000);
          const cronLater = await tasksOf(id, 'bc_cron_task');
          assert(cronLater.length === cronAfter.length, `no cron firing after the activity was left: ${cronAfter.length} then ${cronLater.length}`);
          for (const t of [...iso, ...cronLater]) await client.completeTask(t.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[C] Timer values from FEEL expressions',
        async fn() {
          const started = Date.now();
          const when = new Date(started + 3000).toISOString();
          const inst = await client.startProcess('TimerFeel47', { delay: 'PT1S', when });
          const id = inst.processInstanceId ?? inst.id;
          await sleep(500);
          assert((await tasksOf(id, 'tf_after')).length === 0, 'still waiting on the timers');
          const task = await client.waitForTask('tf_after', id, 15000);
          const elapsed = Date.now() - started;
          assert(elapsed >= 2500, `the date timer should hold the instance until ${when} (${elapsed} ms)`);
          await client.completeTask(task.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
    ],
  });
}

main().catch(err => { console.error(err); process.exit(1); });
