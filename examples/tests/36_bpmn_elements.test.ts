/**
 * Suite 36 — BPMN element coverage  (BPMN: 36_bpmn_elements.bpmn)
 *
 * Tests:
 *   A) Manual task, none throw event and a link throw/catch pair pass the
 *      token straight through to the next user task.
 *   B) A receive task waits for its message; a send task is worked like a
 *      service task.
 *   C) A signal intermediate throw event and a signal end event resume a
 *      process waiting for those signals.
 *   D) A terminate end event ends the whole instance and cancels the other
 *      branch's open task.
 *   E) A terminate end event inside a sub-process ends only the
 *      sub-process; the outer flow continues.
 *   F) An error end event inside a sub-process is caught by the error
 *      boundary on the sub-process.
 *   G) An error end event nothing catches raises an incident.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '36_bpmn_elements.bpmn');
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
  await client.deployBpmn(BPMN, 'BPMN Element Coverage Test');

  await runSuite({
    name: '36 — BPMN element coverage',
    tests: [
      {
        name: '[A] Manual task, none throw and link events pass straight through',
        async fn() {
          const inst = await client.startProcess('ElemPassThrough', {});
          const instanceId = inst.processInstanceId ?? inst.id;
          const done = await client.waitForTask('pt_done', instanceId, 10000);
          await client.completeTask(done.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      {
        name: '[B] Receive task waits for its message; send task runs as a job',
        async fn() {
          const inst = await client.startProcess('ElemSendReceive', {});
          const instanceId = inst.processInstanceId ?? inst.id;

          // Still parked on the receive task: no send job yet.
          await sleep(1000);
          const early = await client.fetchAndLock([{ name: 'elem_send' }]);
          assert(!early.some(t => t.processInstanceId === instanceId), 'send task ran before the message arrived');

          const res = await client.api.post('/engine-rest/message', { messageName: 'ElemOrderPaid', variables: { paid: true } });
          assert(res.status < 300, `message correlation failed with HTTP ${res.status}`);

          await client.pollExternalTask('elem_send', async () => ({ receiptSent: true }), 15000, instanceId);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      {
        name: '[C] Signal throw event and signal end event resume a waiting process',
        async fn() {
          const catcher = await client.startProcess('ElemSignalCatcher', {});
          const catcherId = catcher.processInstanceId ?? catcher.id;
          await sleep(500);

          const thrower = await client.startProcess('ElemSignalThrower', {});
          const throwerId = thrower.processInstanceId ?? thrower.id;
          await client.waitForProcessEnd(throwerId, 10000);

          const caught = await client.waitForTask('sc_caught', catcherId, 10000);
          await client.completeTask(caught.id);
          await client.waitForProcessEnd(catcherId, 10000);
        },
      },

      {
        name: '[D] Terminate end event ends the instance and cancels the other branch',
        async fn() {
          const inst = await client.startProcess('ElemTerminate', {});
          const instanceId = inst.processInstanceId ?? inst.id;

          await client.waitForTask('te_wait', instanceId, 10000);
          const quit = await client.waitForTask('te_quit', instanceId, 10000);
          await client.completeTask(quit.id);

          await client.waitForProcessEnd(instanceId, 10000);
          // A plain end event would also complete the instance; what proves
          // the terminate is the other branch's task ending up cancelled.
          const waitStatus = await historicTaskStatus(instanceId, 'te_wait');
          assert(waitStatus === 'cancelled', `te_wait should be cancelled by the terminate end event, got ${waitStatus}`);
        },
      },

      {
        name: '[E] Terminate end event inside a sub-process ends only the sub-process',
        async fn() {
          const inst = await client.startProcess('ElemTerminateSub', {});
          const instanceId = inst.processInstanceId ?? inst.id;

          await client.waitForTask('ts_wait', instanceId, 10000);
          const quit = await client.waitForTask('ts_quit', instanceId, 10000);
          await client.completeTask(quit.id);

          const after = await client.waitForTask('ts_after', instanceId, 10000);
          const open = await openTaskKeys(instanceId);
          assert(!open.includes('ts_wait'), `inner task still open after terminate: ${open}`);

          await client.completeTask(after.id);
          await client.waitForProcessEnd(instanceId, 10000);
          const waitStatus = await historicTaskStatus(instanceId, 'ts_wait');
          assert(waitStatus === 'cancelled', `ts_wait should be cancelled by the terminate end event, got ${waitStatus}`);
        },
      },

      {
        name: '[F] Error end event inside a sub-process is caught by the sub-process boundary',
        async fn() {
          const inst = await client.startProcess('ElemErrorEnd', {});
          const instanceId = inst.processInstanceId ?? inst.id;

          const step = await client.waitForTask('ee_step', instanceId, 10000);
          await client.completeTask(step.id);

          const handled = await client.waitForTask('ee_handled', instanceId, 10000);
          await client.completeTask(handled.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      {
        name: '[G] Uncaught error end event raises an incident',
        async fn() {
          const inst = await client.startProcess('ElemErrorUncaught', {});
          const instanceId = inst.processInstanceId ?? inst.id;

          await sleep(500);
          const res = await client.api.get(`/engine-rest/incident?processInstanceId=${instanceId}`);
          const incidents = res.data as any[];
          assert(incidents.length === 1, `expected 1 incident, got ${incidents.length}`);
          assert(incidents[0].incidentType === 'unhandledErrorEvent', `unexpected type: ${incidents[0].incidentType}`);
          assert(incidents[0].activityId === 'eu_throw', `unexpected activityId: ${incidents[0].activityId}`);

          const live = await client.getProcessInstance(instanceId);
          assert(live?.status === 'running', `instance should stay running with an incident, got ${live?.status}`);
          await client.api.delete(`/engine-rest/process-instance/${instanceId}`).catch(() => undefined);
        },
      },
    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
