/**
 * Suite 19 — Operate-equivalent (Camunda 8 v2 API)
 *
 * Verifies GoFlow's Phase 5 additions: POST /v2/flownode-instances/search,
 * POST /v2/variables/search, POST /v2/incidents/search, and process instance
 * modification (POST /v2/process-instances/:id/modification) — moving a
 * running token from one element to another, cancelling the source
 * execution's open task and creating a fresh one at the target.
 *
 * Tests:
 *   A) Deploy fixtures (OperateProcess for modification/variables/flownode
 *      tests, SupportTicketProcess reused from suite 07 for incidents).
 *   B) POST /v2/variables/search finds each started variable; the "name"
 *      filter isolates exactly one.
 *   C) POST /v2/flownode-instances/search shows the token parked ACTIVE at
 *      taskA.
 *   D) POST /v2/process-instances/:id/modification moves the token from
 *      taskA to taskB: taskA's task is CANCELED, taskB's task is CREATED,
 *      and flownode-instances/search shows taskA COMPLETED + taskB ACTIVE.
 *      Completing taskB then ends the process.
 *   E) POST /v2/incidents/search finds a permanently-failed job's incident,
 *      filterable by state and errorType.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const OPERATE_BPMN = path.join(__dirname, 'bpmn', '19_operate_process.bpmn');
const SUPPORT_TICKET_BPMN = path.join(__dirname, 'bpmn', '07_support_ticket.bpmn');
const client = new GoFlowClient();

async function searchUserTasks(filter: Record<string, unknown>): Promise<any[]> {
  const res = await client.api.post('/v2/user-tasks/search', { filter });
  return res.data.items;
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(OPERATE_BPMN, 'Operate Process Test');
  await client.deployBpmn(SUPPORT_TICKET_BPMN, 'Operate Incidents Test');

  await runSuite({
    name: '19 — Operate-equivalent (v2 flownode/variables/incidents/modification)',
    tests: [

      // ── B/C: variables search + flownode-instances search ────────────────────
      {
        name: '[B/C] Variables search finds started vars; flownode-instances shows token at taskA',
        async fn() {
          const inst = await client.startProcess('OperateProcess', { requester: 'alice', amount: 42 });
          const instanceId = inst.processInstanceId ?? inst.id;

          const allVars = await client.api.post('/v2/variables/search', {
            filter: { processInstanceKey: instanceId },
          });
          const varNames = allVars.data.items.map((v: any) => v.name);
          assert(varNames.includes('requester') && varNames.includes('amount'),
            `expected requester+amount in variables, got ${JSON.stringify(varNames)}`);

          const onlyAmount = await client.api.post('/v2/variables/search', {
            filter: { processInstanceKey: instanceId, name: 'amount' },
          });
          assert(onlyAmount.data.items.length === 1, `expected exactly 1 variable, got ${onlyAmount.data.items.length}`);
          assert(onlyAmount.data.items[0].value === 42, `expected value 42, got ${onlyAmount.data.items[0].value}`);

          const flowNodes = await client.api.post('/v2/flownode-instances/search', {
            filter: { processInstanceKey: instanceId },
          });
          const active = flowNodes.data.items.filter((n: any) => n.state === 'ACTIVE');
          assert(active.length === 1, `expected exactly 1 active flow node, got ${active.length}`);
          assert(active[0].elementId === 'taskA', `expected token at taskA, got ${active[0].elementId}`);
          assert(active[0].endDate === null || active[0].endDate === undefined,
            'expected no endDate on an active flow node');
        },
      },

      // ── D: process instance modification ──────────────────────────────────────
      {
        name: '[D] Modification moves the token from taskA to taskB',
        async fn() {
          const inst = await client.startProcess('OperateProcess', { requester: 'bob' });
          const instanceId = inst.processInstanceId ?? inst.id;

          const beforeA = await searchUserTasks({ processInstanceKey: instanceId, elementId: 'taskA' });
          assert(beforeA.length === 1 && beforeA[0].state === 'CREATED', 'expected taskA CREATED before modification');

          await client.api.post(`/v2/process-instances/${instanceId}/modification`, {
            moveInstructions: [{ sourceElementId: 'taskA', targetElementId: 'taskB' }],
          });

          const afterA = await searchUserTasks({ processInstanceKey: instanceId, elementId: 'taskA' });
          assert(afterA[0].state === 'CANCELED', `expected taskA CANCELED, got ${afterA[0].state}`);

          const afterB = await searchUserTasks({ processInstanceKey: instanceId, elementId: 'taskB' });
          assert(afterB.length === 1 && afterB[0].state === 'CREATED', `expected taskB CREATED, got ${JSON.stringify(afterB)}`);

          const flowNodes = await client.api.post('/v2/flownode-instances/search', {
            filter: { processInstanceKey: instanceId },
          });
          const taskAFlow = flowNodes.data.items.find((n: any) => n.elementId === 'taskA');
          const taskBFlow = flowNodes.data.items.find((n: any) => n.elementId === 'taskB');
          assert(taskAFlow.state === 'COMPLETED', `expected taskA flow node COMPLETED, got ${taskAFlow.state}`);
          assert(taskBFlow.state === 'ACTIVE', `expected taskB flow node ACTIVE, got ${taskBFlow.state}`);

          await client.api.post(`/v2/user-tasks/${afterB[0].userTaskKey}/assignment`, { assignee: 'carol' });
          await client.api.post(`/v2/user-tasks/${afterB[0].userTaskKey}/completion`, { variables: {} });

          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      // ── E: incidents search ────────────────────────────────────────────────────
      {
        name: '[E] Incidents search finds a permanently-failed job, filterable by state/errorType',
        async fn() {
          const inst = await client.startProcess('SupportTicketProcess', { ticketId: 'OPERATE-001' });
          const instanceId = inst.processInstanceId ?? inst.id;

          const tasks = await client.fetchAndLock([{ name: 'classify_ticket' }]);
          const job = tasks.find(t => t.processInstanceId === instanceId);
          assert(job !== undefined, 'classify_ticket job not found');

          await client.api.post(`/engine-rest/external-task/${job!.id}/failure`, {
            workerId: client.workerId,
            errorMessage: 'Classification service unavailable',
            retries: 0,
            retryTimeout: 0,
          });

          await sleep(500);

          const byInstance = await client.api.post('/v2/incidents/search', {
            filter: { processInstanceKey: instanceId },
          });
          assert(byInstance.data.items.length > 0, 'expected at least 1 incident for this instance');
          const incident = byInstance.data.items[0];
          assert(incident.state === 'ACTIVE', `expected ACTIVE, got ${incident.state}`);
          assert(incident.elementId === 'classifyTicket', `unexpected elementId ${incident.elementId}`);

          const byState = await client.api.post('/v2/incidents/search', {
            filter: { processInstanceKey: instanceId, state: 'ACTIVE' },
          });
          assert(byState.data.items.length > 0, 'expected state=ACTIVE filter to still find the incident');

          const byErrorType = await client.api.post('/v2/incidents/search', {
            filter: { processInstanceKey: instanceId, errorType: 'failedExternalTask' },
          });
          assert(byErrorType.data.items.length > 0, 'expected errorType filter to find the incident');

          const byWrongState = await client.api.post('/v2/incidents/search', {
            filter: { processInstanceKey: instanceId, state: 'RESOLVED' },
          });
          assert(byWrongState.data.items.length === 0, 'expected state=RESOLVED filter to exclude the open incident');
        },
      },

    ],
  });
}

run().catch(console.error);
