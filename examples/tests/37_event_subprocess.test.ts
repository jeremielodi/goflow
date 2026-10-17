/**
 * Suite 37 — Event sub-processes and implicit end  (BPMN: 37_event_subprocess.bpmn)
 *
 * Tests:
 *   A) Interrupting message event sub-process cancels the main flow and runs
 *      its own body; the instance ends when the body ends.
 *   B) When the main flow finishes first, the armed event sub-process
 *      doesn't keep the instance alive, and its subscription is gone.
 *   C) Non-interrupting signal event sub-process runs once per signal,
 *      alongside the main flow; the instance ends only when every branch did.
 *   D) Interrupting timer event sub-process inside an embedded sub-process
 *      cancels the sub-process's work; the outer flow then continues.
 *   E) Error event sub-process catches a job's BPMN error.
 *   F) Parallel branches ending on separate end events: the first end
 *      doesn't cut off the other branch.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '37_event_subprocess.bpmn');
const client = new GoFlowClient();

async function historicTaskStatuses(instanceId: string, taskKey: string): Promise<string[]> {
  const res = await client.api.get(`/engine-rest/history/task?processInstanceId=${instanceId}`);
  return (res.data as any[]).filter(t => t.taskDefinitionKey === taskKey).map(t => t.status);
}

async function openTasks(instanceId: string, taskKey: string) {
  return (await client.getTasks(instanceId)).filter(t => t.taskDefinitionKey === taskKey);
}

async function isRunning(instanceId: string): Promise<boolean> {
  const inst = await client.getProcessInstance(instanceId);
  return !!inst && inst.status === 'running';
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Event Sub-Process Test');

  await runSuite({
    name: '37 — Event sub-processes and implicit end',
    tests: [
      {
        name: '[A] Interrupting message event sub-process replaces the main flow',
        async fn() {
          const inst = await client.startProcess('EsInterruptMsg', {});
          const instanceId = inst.processInstanceId ?? inst.id;
          await client.waitForTask('es_work', instanceId, 10000);

          const res = await client.api.post('/engine-rest/message', { messageName: 'EsCancel' });
          assert(res.status < 300, `correlating EsCancel failed with HTTP ${res.status}`);

          const handler = await client.waitForTask('es_cancelled', instanceId, 10000);
          assert((await openTasks(instanceId, 'es_work')).length === 0, 'main-flow task still open after an interrupting event sub-process');

          await client.completeTask(handler.id);
          await client.waitForProcessEnd(instanceId, 10000);
          const work = await historicTaskStatuses(instanceId, 'es_work');
          assert(work.length === 1 && work[0] === 'cancelled', `es_work should be cancelled, got ${work}`);
        },
      },

      {
        name: '[B] Armed event sub-process neither blocks completion nor outlives the instance',
        async fn() {
          const inst = await client.startProcess('EsInterruptMsg', {});
          const instanceId = inst.processInstanceId ?? inst.id;
          const work = await client.waitForTask('es_work', instanceId, 10000);
          await client.completeTask(work.id);
          await client.waitForProcessEnd(instanceId, 10000);

          // The instance's message subscription must be gone with it.
          const res = await client.api.post('/engine-rest/message', { messageName: 'EsCancel' }, { validateStatus: () => true });
          assert(res.status >= 400, `EsCancel still correlated to something after the instance ended (HTTP ${res.status})`);
        },
      },

      {
        name: '[C] Non-interrupting signal event sub-process runs once per signal, alongside the main flow',
        async fn() {
          const inst = await client.startProcess('EsNonInterruptSignal', {});
          const instanceId = inst.processInstanceId ?? inst.id;
          const work = await client.waitForTask('nis_work', instanceId, 10000);

          await client.api.post('/engine-rest/signal', { name: 'EsPing' });
          await client.api.post('/engine-rest/signal', { name: 'EsPing' });

          const deadline = Date.now() + 10000;
          let pings = await openTasks(instanceId, 'nis_ping');
          while (pings.length < 2 && Date.now() < deadline) {
            await sleep(500);
            pings = await openTasks(instanceId, 'nis_ping');
          }
          assert(pings.length === 2, `expected 2 ping tasks, got ${pings.length}`);
          assert((await openTasks(instanceId, 'nis_work')).length === 1, 'non-interrupting event sub-process cancelled the main flow');

          // Main flow done, pings still open: the instance keeps running.
          await client.completeTask(work.id);
          await sleep(1000);
          assert(await isRunning(instanceId), 'instance ended while event sub-process tasks were still open');

          for (const p of pings) await client.completeTask(p.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      {
        name: '[D] Timer event sub-process inside a sub-process interrupts it; the outer flow continues',
        async fn() {
          const inst = await client.startProcess('EsTimerInSub', {});
          const instanceId = inst.processInstanceId ?? inst.id;
          await client.waitForTask('tsb_wait', instanceId, 10000);

          const timeout = await client.waitForTask('tsb_timeout', instanceId, 30000);
          assert((await openTasks(instanceId, 'tsb_wait')).length === 0, 'tsb_wait still open after the timer event sub-process fired');

          await client.completeTask(timeout.id);
          const after = await client.waitForTask('after_block', instanceId, 10000);
          await client.completeTask(after.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      {
        name: '[E] Error event sub-process catches a job\'s BPMN error',
        async fn() {
          const inst = await client.startProcess('EsError', {});
          const instanceId = inst.processInstanceId ?? inst.id;

          let job;
          const deadline = Date.now() + 15000;
          while (!job && Date.now() < deadline) {
            const locked = await client.fetchAndLock([{ name: 'es_job' }]);
            job = locked.find(t => t.processInstanceId === instanceId);
            if (!job) await sleep(500);
          }
          assert(job !== undefined, 'es_job not found');

          await client.api.post(`/engine-rest/external-task/${job!.id}/failure`, {
            workerId: client.workerId,
            errorMessage: 'boom',
            errorCode: 'ES_ERR',
            retries: 0,
            retryTimeout: 0,
          });

          const handler = await client.waitForTask('es_err_handler', instanceId, 10000);
          await client.completeTask(handler.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      {
        name: '[F] A branch reaching its own end event does not cut off the other branch',
        async fn() {
          const inst = await client.startProcess('ImplicitEnd', {});
          const instanceId = inst.processInstanceId ?? inst.id;
          const a = await client.waitForTask('ie_a', instanceId, 10000);
          const b = await client.waitForTask('ie_b', instanceId, 10000);

          await client.completeTask(a.id);
          await sleep(1000);
          assert(await isRunning(instanceId), 'instance completed while branch B was still open');
          assert((await openTasks(instanceId, 'ie_b')).length === 1, 'branch B task disappeared');

          await client.completeTask(b.id);
          await client.waitForProcessEnd(instanceId, 10000);
          const bStatus = await historicTaskStatuses(instanceId, 'ie_b');
          assert(bStatus.length === 1 && bStatus[0] === 'completed', `ie_b should be completed, got ${bStatus}`);
        },
      },
    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
