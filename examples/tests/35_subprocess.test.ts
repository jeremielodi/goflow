/**
 * Suite 35 — Embedded Sub-Process  (BPMN: 35_subprocess.bpmn)
 *
 * Tests:
 *   A) Sub-process with a parallel fork/join inside runs to its inner end,
 *      then the outer flow continues on the sub-process's outgoing flow.
 *   B) A BPMN error thrown by a task inside the sub-process is caught by the
 *      error boundary on the sub-process; the sibling branch's open user
 *      task is cancelled.
 *   C) An interrupting timer boundary on the sub-process fires and cancels
 *      the user task waiting inside it.
 *   D) Nested sub-processes: the inner one completing completes the outer
 *      one, then the process.
 *   E) Deploying a sub-process without a start event is rejected.
 */
import * as path from 'path';
import * as fs from 'fs';
import * as os from 'os';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '35_subprocess.bpmn');
const client = new GoFlowClient();

// Final status of a task once its instance has finished (live tasks are
// archived into history when the instance ends).
async function historicTaskStatus(instanceId: string, taskKey: string): Promise<string | undefined> {
  const res = await client.api.get(`/engine-rest/history/task?processInstanceId=${instanceId}`);
  return (res.data as any[]).find(t => t.taskDefinitionKey === taskKey)?.status;
}

async function openTaskKeys(instanceId: string): Promise<string[]> {
  const tasks = await client.getTasks(instanceId);
  return tasks.map(t => t.taskDefinitionKey).sort();
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Embedded Sub-Process Test');

  await runSuite({
    name: '35 — Embedded Sub-Process',
    tests: [
      {
        name: '[A] Parallel fork/join inside a sub-process, then the outer flow continues',
        async fn() {
          const inst = await client.startProcess('SubProcessOrder', { orderId: 'SP-A' });
          const instanceId = inst.processInstanceId ?? inst.id;

          await client.pollExternalTask('sp_charge', async () => ({ charged: true }), 15000, instanceId);

          // Both branches of the inner fork are live at once.
          const review = await client.waitForTask('review', instanceId, 10000);
          await client.pollExternalTask('sp_notify', async () => ({ notified: true }), 15000, instanceId);

          // One branch done, the other still open: the sub-process must not
          // have finished yet.
          await sleep(1000);
          const midway = await openTaskKeys(instanceId);
          assert(!midway.includes('after_payment'), `sub-process left early, open tasks: ${midway}`);

          await client.completeTask(review.id, { approved: true });

          const after = await client.waitForTask('after_payment', instanceId, 10000);
          await client.completeTask(after.id);
          await client.waitForProcessEnd(instanceId, 10000);

          // Variables come back Camunda-7 style: { name: { value, type } }.
          const vars = await client.getProcessVariables(instanceId) as Record<string, { value?: unknown }>;
          assert(vars.charged?.value === true && vars.notified?.value === true && vars.approved?.value === true,
            `variables set inside the sub-process were lost: ${JSON.stringify(vars)}`);
        },
      },

      {
        name: '[B] Error thrown inside the sub-process is caught by its boundary; sibling task cancelled',
        async fn() {
          const inst = await client.startProcess('SubProcessOrder', { orderId: 'SP-B' });
          const instanceId = inst.processInstanceId ?? inst.id;

          await client.pollExternalTask('sp_charge', async () => ({ charged: true }), 15000, instanceId);
          await client.waitForTask('review', instanceId, 10000);

          let job;
          const deadline = Date.now() + 15000;
          while (!job && Date.now() < deadline) {
            const locked = await client.fetchAndLock([{ name: 'sp_notify' }]);
            job = locked.find(t => t.processInstanceId === instanceId);
            if (!job) await sleep(500);
          }
          assert(job !== undefined, 'sp_notify job not found');

          await client.api.post(`/engine-rest/external-task/${job!.id}/failure`, {
            workerId: client.workerId,
            errorMessage: 'Notification gateway rejected the charge',
            errorCode: 'PAYMENT_FAILED',
            retries: 0,
            retryTimeout: 0,
          });

          const failure = await client.waitForTask('handle_failure', instanceId, 10000);
          const open = await openTaskKeys(instanceId);
          assert(!open.includes('review'), `review task inside the interrupted sub-process is still open: ${open}`);
          assert(!open.includes('after_payment'), `normal continuation ran after an error: ${open}`);

          await client.completeTask(failure.id);
          await client.waitForProcessEnd(instanceId, 10000);
          const reviewStatus = await historicTaskStatus(instanceId, 'review');
          assert(reviewStatus === 'cancelled', `review should be cancelled by the error boundary, got ${reviewStatus}`);
        },
      },

      {
        name: '[C] Timer boundary on the sub-process interrupts the task waiting inside',
        async fn() {
          const inst = await client.startProcess('SubProcessTimeout', {});
          const instanceId = inst.processInstanceId ?? inst.id;

          await client.waitForTask('wait_inside', instanceId, 10000);
          const handler = await client.waitForTask('after_timeout', instanceId, 30000);

          const open = await openTaskKeys(instanceId);
          assert(!open.includes('wait_inside'), `task inside the timed-out sub-process is still open: ${open}`);

          await client.completeTask(handler.id);
          await client.waitForProcessEnd(instanceId, 10000);
          const waitStatus = await historicTaskStatus(instanceId, 'wait_inside');
          assert(waitStatus === 'cancelled', `wait_inside should be cancelled by the timer boundary, got ${waitStatus}`);
        },
      },

      {
        name: '[D] Nested sub-processes complete inner → outer → process',
        async fn() {
          const inst = await client.startProcess('SubProcessNested', {});
          const instanceId = inst.processInstanceId ?? inst.id;

          await client.pollExternalTask('sp_inner', async () => ({ done: true }), 15000, instanceId);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      {
        name: '[E] A sub-process without a start event is rejected at deploy time',
        async fn() {
          const xml = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="d" targetNamespace="x">
  <bpmn:process id="SubProcessBroken" isExecutable="true">
    <bpmn:startEvent id="s"/>
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="sub"/>
    <bpmn:subProcess id="sub"><bpmn:endEvent id="sub_end"/></bpmn:subProcess>
    <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="e"/>
    <bpmn:endEvent id="e"/>
  </bpmn:process>
</bpmn:definitions>`;
          const file = path.join(os.tmpdir(), `35_broken_${Date.now()}.bpmn`);
          fs.writeFileSync(file, xml);
          let rejected = false;
          try {
            await client.deployBpmn(file, 'Broken Sub-Process');
          } catch {
            rejected = true;
          } finally {
            fs.unlinkSync(file);
          }
          assert(rejected, 'deploying a sub-process with no start event should fail');
        },
      },
    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
