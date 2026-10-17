/**
 * Suite 43 — Compensation  (BPMN: 43_compensation.bpmn)
 *
 * Tests:
 *   A) A payment error triggers a compensation throw: the handlers of both
 *      completed bookings (a job and a user task) run, and the process only
 *      continues once both are done.
 *   B) On the happy path no handler ever runs.
 *   C) A throw event with activityRef compensates only that activity.
 *   D) Compensation thrown from an error event sub-process undoes the work
 *      of the scope the error interrupted.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '43_compensation.bpmn');
const client = new GoFlowClient();

async function openTasks(instanceId: string, key: string) {
  return (await client.getTasks(instanceId)).filter(t => t.taskDefinitionKey === key);
}

async function failJob(topic: string, instanceId: string, errorCode: string) {
  let job;
  const deadline = Date.now() + 15000;
  while (!job && Date.now() < deadline) {
    job = (await client.fetchAndLock([{ name: topic }])).find(t => t.processInstanceId === instanceId);
    if (!job) await sleep(300);
  }
  assert(job !== undefined, `${topic} job not found`);
  await client.api.post(`/engine-rest/external-task/${job!.id}/failure`, {
    workerId: client.workerId, errorMessage: 'declined', errorCode, retries: 0, retryTimeout: 0,
  });
}

async function historicTaskKeys(instanceId: string): Promise<string[]> {
  const res = await client.api.get(`/engine-rest/history/task?processInstanceId=${instanceId}`);
  return (res.data as any[]).map(t => t.taskDefinitionKey).sort();
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Compensation Test');

  await runSuite({
    name: '43 — Compensation',
    tests: [
      {
        name: '[A] A payment error compensates both completed bookings before continuing',
        async fn() {
          const inst = await client.startProcess('CompBooking', {});
          const id = inst.processInstanceId ?? inst.id;
          await client.pollExternalTask('comp_hotel', async () => ({ hotel: 'H1' }), 15000, id);
          const flight = await client.waitForTask('book_flight', id, 10000);
          await client.completeTask(flight.id, { flight: 'F1' });
          await failJob('comp_charge', id, 'PAY_FAIL');

          // Both handlers run; the process waits for them.
          const cancelFlight = await client.waitForTask('cancel_flight', id, 10000);
          await client.pollExternalTask('comp_cancel_hotel', async () => ({ hotelCancelled: true }), 15000, id);
          await sleep(500);
          assert((await openTasks(id, 'booking_failed')).length === 0, 'process continued before every handler finished');

          await client.completeTask(cancelFlight.id);
          const failed = await client.waitForTask('booking_failed', id, 10000);
          await client.completeTask(failed.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[B] On the happy path no compensation handler runs',
        async fn() {
          const inst = await client.startProcess('CompBooking', {});
          const id = inst.processInstanceId ?? inst.id;
          await client.pollExternalTask('comp_hotel', async () => ({}), 15000, id);
          const flight = await client.waitForTask('book_flight', id, 10000);
          await client.completeTask(flight.id);
          await client.pollExternalTask('comp_charge', async () => ({ paid: true }), 15000, id);
          await client.waitForProcessEnd(id, 10000);
          const tasks = await historicTaskKeys(id);
          assert(!tasks.includes('cancel_flight'), `a handler ran on the happy path: ${tasks}`);
        },
      },
      {
        name: '[C] activityRef compensates only the referenced activity',
        async fn() {
          const inst = await client.startProcess('CompSpecific', {});
          const id = inst.processInstanceId ?? inst.id;
          await client.completeTask((await client.waitForTask('step_one', id, 10000)).id);
          await client.completeTask((await client.waitForTask('step_two', id, 10000)).id);

          const undoTwo = await client.waitForTask('undo_two', id, 10000);
          await sleep(500);
          assert((await openTasks(id, 'undo_one')).length === 0, 'step_one was compensated although activityRef targets step_two');
          await client.completeTask(undoTwo.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[D] Compensation thrown from an error event sub-process',
        async fn() {
          const inst = await client.startProcess('CompEventSub', {});
          const id = inst.processInstanceId ?? inst.id;
          await client.completeTask((await client.waitForTask('reserve', id, 10000)).id);
          await failJob('comp_pay2', id, 'PAY2_FAIL');

          const unreserve = await client.waitForTask('unreserve', id, 10000);
          await client.completeTask(unreserve.id);
          const failed = await client.waitForTask('comp_failed', id, 10000);
          await client.completeTask(failed.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
