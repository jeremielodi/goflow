/**
 * Suite 15 — Camunda 8 REST API ("/v2/...")
 *
 * Verifies the v2 API surface (internal/api/v2) added alongside the existing
 * Camunda 7 style /engine-rest/... routes. The v2 layer reuses the same
 * engine/repositories — these tests exercise the translated request/response
 * shapes end-to-end, reusing existing fixtures from earlier suites.
 *
 * Tests:
 *   A) Full v2 lifecycle on TaskListenerProcess: create → user-task search/
 *      assign/complete → job activation/completion → user-task again →
 *      GET process-instance falls back to history with state COMPLETED.
 *   B) POST /v2/process-instances/search finds the completed instance.
 *   C) POST /v2/jobs/:id/error with no matching boundary event creates an incident.
 *   D) POST /v2/messages/publication correlates to the right waiting instance.
 *   E) POST /v2/signals/broadcast resumes waiting instances.
 *   F) POST /v2/resources/deployments deploys a BPMN file.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const TASK_LISTENER_BPMN = path.join(__dirname, 'bpmn', '14_task_listener.bpmn');
const ORDER_BPMN = path.join(__dirname, 'bpmn', '10_order_notification.bpmn');
const INVENTORY_BPMN = path.join(__dirname, 'bpmn', '11_inventory_broadcast.bpmn');
const client = new GoFlowClient();

async function completeV2UserTask(processInstanceKey: string, elementId: string, assignee: string): Promise<void> {
  const search = await client.api.post('/v2/user-tasks/search', {
    filter: { processInstanceKey, elementId },
  });
  const task = search.data.items.find((t: any) => t.elementId === elementId);
  assert(task !== undefined, `v2 user task "${elementId}" not found for instance ${processInstanceKey}`);

  await client.api.post(`/v2/user-tasks/${task.userTaskKey}/assignment`, { assignee });
  await client.api.post(`/v2/user-tasks/${task.userTaskKey}/completion`, { variables: {} });
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(TASK_LISTENER_BPMN, 'v2 API Test — Task Listener');
  await client.deployBpmn(ORDER_BPMN, 'v2 API Test — Order Notification');
  await client.deployBpmn(INVENTORY_BPMN, 'v2 API Test — Inventory Broadcast');

  await runSuite({
    name: '15 — Camunda 8 REST API (/v2/...)',
    tests: [

      // ── A: full v2 lifecycle ────────────────────────────────────────────────
      {
        name: '[A] Create → user-task → job → user-task → completed, all via /v2',
        async fn() {
          const created = await client.api.post('/v2/process-instances', {
            processDefinitionId: 'TaskListenerProcess',
            variables: {},
          });
          const processInstanceKey = created.data.processInstanceKey;
          assert(!!processInstanceKey, 'expected processInstanceKey in create response');

          const got = await client.api.get(`/v2/process-instances/${processInstanceKey}`);
          assert(got.data.state === 'ACTIVE', `expected ACTIVE, got ${got.data.state}`);

          await completeV2UserTask(processInstanceKey, 'reviewTask', 'alice');

          // Worker completes the external task via the v2 job endpoints.
          const activation = await client.api.post('/v2/jobs/activation', {
            type: 'listener_worker_task',
            worker: 'v2-test-worker',
            timeout: 30000,
            maxJobsToActivate: 10,
          });
          const job = activation.data.jobs.find((j: any) => j.processInstanceKey === processInstanceKey);
          assert(job !== undefined, 'expected an activated job for our instance');
          await client.api.post(`/v2/jobs/${job.jobKey}/completion`, { variables: {} });

          await completeV2UserTask(processInstanceKey, 'approveTask', 'carol');

          await client.waitForProcessEnd(processInstanceKey, 15000);
          const final = await client.api.get(`/v2/process-instances/${processInstanceKey}`);
          assert(final.data.state === 'COMPLETED', `expected COMPLETED, got ${final.data.state}`);
        },
      },

      // ── B: search finds the completed instance ─────────────────────────────
      {
        name: '[B] POST /v2/process-instances/search filters by processDefinitionId + state',
        async fn() {
          const res = await client.api.post('/v2/process-instances/search', {
            filter: { processDefinitionId: 'TaskListenerProcess', state: 'COMPLETED' },
          });
          assert(res.data.items.length > 0, 'expected at least one completed TaskListenerProcess instance');
          for (const item of res.data.items) {
            assert(item.processDefinitionId === 'TaskListenerProcess', 'unexpected processDefinitionId in results');
            assert(item.state === 'COMPLETED', `unexpected state ${item.state} in filtered results`);
          }
        },
      },

      // ── C: throwing an unmatched error creates an incident ─────────────────
      {
        name: '[C] POST /v2/jobs/:id/error with no matching boundary → incident created',
        async fn() {
          const created = await client.api.post('/v2/process-instances', {
            processDefinitionId: 'TaskListenerProcess',
            variables: {},
          });
          const processInstanceKey = created.data.processInstanceKey;

          await completeV2UserTask(processInstanceKey, 'reviewTask', 'dana');

          const activation = await client.api.post('/v2/jobs/activation', {
            type: 'listener_worker_task',
            worker: 'v2-test-worker',
            timeout: 30000,
            maxJobsToActivate: 10,
          });
          const job = activation.data.jobs.find((j: any) => j.processInstanceKey === processInstanceKey);
          assert(job !== undefined, 'expected an activated job for our instance');

          await client.api.post(`/v2/jobs/${job.jobKey}/error`, {
            errorCode: 'NO_BOUNDARY_FOR_THIS',
            errorMessage: 'thrown via v2 API',
          });

          await sleep(500);
          const incidents = await client.api.get(`/engine-rest/incident?processInstanceId=${processInstanceKey}`);
          assert(incidents.data.length > 0, 'expected an incident after unmatched v2 error');
        },
      },

      // ── D: message publication ─────────────────────────────────────────────
      {
        name: '[D] POST /v2/messages/publication correlates to the waiting instance',
        async fn() {
          const inst = await client.startProcess('orderNotification', { orderId: 'V2-MSG-001' });
          const instanceId = inst.processInstanceId ?? inst.id;
          await sleep(500);

          const res = await client.api.post('/v2/messages/publication', {
            name: 'PaymentReceived',
            variables: {},
          });
          assert(res.data.processInstanceKey === instanceId, 'expected the message to correlate to our instance');

          // Drain the rest of the process so it doesn't dangle.
          await sleep(300);
          await client.pollExternalTask('confirm-payment', async () => ({}), 10000, instanceId);
          await sleep(300);
          await client.api.post('/v2/messages/publication', { name: 'ShipmentReady', variables: {} });
          await sleep(300);
          await client.pollExternalTask('notify-customer', async () => ({}), 10000, instanceId);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      // ── E: signal broadcast ─────────────────────────────────────────────────
      {
        name: '[E] POST /v2/signals/broadcast resumes waiting instances',
        async fn() {
          const inst = await client.startProcess('inventoryAlert', { product: 'v2-widget' });
          const instanceId = inst.processInstanceId ?? inst.id;

          await sleep(500);
          await client.pollExternalTask('check-inventory', async () => ({}), 10000, instanceId);
          await sleep(500);

          const res = await client.api.post('/v2/signals/broadcast', {
            signalName: 'RestockAlert',
            variables: { restockedProduct: 'v2-widget' },
          });
          assert(res.data.resumed >= 1, `expected resumed >= 1, got ${res.data.resumed}`);

          await sleep(500);
          await client.pollExternalTask('update-inventory', async () => ({}), 10000, instanceId);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      // ── F: v2 deployment endpoint ────────────────────────────────────────────
      {
        name: '[F] POST /v2/resources/deployments deploys a BPMN file',
        async fn() {
          const form = new (require('form-data'))();
          form.append('resources', require('fs').createReadStream(TASK_LISTENER_BPMN));
          form.append('deployment-name', 'v2 resources/deployments test');

          const res = await client.api.post('/v2/resources/deployments', form, {
            headers: form.getHeaders(),
          });
          assert(
            res.data.deployedProcessDefinitions?.TaskListenerProcess !== undefined,
            'expected TaskListenerProcess in deployedProcessDefinitions'
          );
        },
      },

    ],
  });
}

run().catch(console.error);
