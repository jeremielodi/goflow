/**
 * Suite 22 — Audit trail completeness (data source for frontend token replay)
 *
 * Verifies GET /audit/process/:processId returns a complete, chronologically
 * ordered sequence of audit_logs entries covering the full lifecycle of a
 * process instance. This is exactly the data a frontend "replay" feature
 * would consume to reconstruct how a token moved through the diagram over
 * time — most of these entries were never written before this session
 * (AuditService methods existed but were mostly never called).
 *
 * Also verifies GET /audit/process/:processId/token-history, the derived
 * endpoint that turns those raw audit rows into a readable token path —
 * built specifically because /engine-rest/history/activity-instance (backed
 * by the live `executions` table) can only ever show a token's *current*
 * position, never the sequence of elements it actually visited.
 *
 * Reuses fixture 19_operate_process.bpmn (two sequential user tasks:
 * start -> taskA -> taskB -> end).
 *
 * Tests:
 *   A) A normal run through both user tasks produces, in order:
 *      PROCESS_STARTED, EXECUTION_CREATED, TASK_CREATED, TASK_CLAIMED,
 *      TASK_COMPLETED, EXECUTION_MOVED, TASK_CREATED, TASK_CLAIMED,
 *      TASK_COMPLETED, EXECUTION_COMPLETED, PROCESS_COMPLETED.
 *   B) A Move Token modification (taskA -> taskB) produces TASK_CANCELLED,
 *      EXECUTION_CREATED, and EXECUTION_MOVED entries.
 *   C) The token-history endpoint's non-empty elementIds, collapsed to
 *      consecutive-distinct values, match the diagram's real path:
 *      start -> taskA -> taskB -> end.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert } from './client';

const OPERATE_BPMN = path.join(__dirname, 'bpmn', '19_operate_process.bpmn');
const client = new GoFlowClient();

interface AuditLogEntry {
  action: string;
  processInstanceId?: string;
  taskId?: string;
  createdAt: string;
}

interface TokenHistoryStep {
  timestamp: string;
  action: string;
  elementId?: string;
  elementName?: string;
  detail?: string;
}

async function getAuditLogs(instanceId: string): Promise<AuditLogEntry[]> {
  const res = await client.api.get(`/audit/process/${instanceId}`);
  return res.data.logs ?? [];
}

async function getTokenHistory(instanceId: string): Promise<TokenHistoryStep[]> {
  const res = await client.api.get(`/audit/process/${instanceId}/token-history`);
  return res.data.steps ?? [];
}

// Collapses consecutive duplicate elementIds so a node visited across
// several steps (e.g. TASK_CREATED/CLAIMED/COMPLETED all at "taskA") counts
// once, leaving just the sequence of distinct elements the token passed
// through.
function collapseConsecutive(elementIds: string[]): string[] {
  const result: string[] = [];
  for (const id of elementIds) {
    if (result[result.length - 1] !== id) result.push(id);
  }
  return result;
}

// Asserts that every action in `expectedInOrder` appears in `logs`, in that
// relative order — other actions may appear in between (not an exact-match).
function assertOrderedSubsequence(logs: AuditLogEntry[], expectedInOrder: string[]) {
  let cursor = 0;
  for (const action of expectedInOrder) {
    const idx = logs.findIndex((l, i) => i >= cursor && l.action === action);
    assert(
      idx !== -1,
      `expected action "${action}" at or after position ${cursor}; got sequence: [${logs.map(l => l.action).join(', ')}]`
    );
    cursor = idx + 1;
  }
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(OPERATE_BPMN, 'Audit Trail Test');

  await runSuite({
    name: '22 — Audit Trail Completeness (replay data source)',
    tests: [
      {
        name: '[A] Full run through two sequential user tasks produces a complete, ordered audit trail',
        async fn() {
          const inst = await client.startProcess('OperateProcess', { requester: 'audit-test' });
          const instanceId = inst.processInstanceId ?? inst.id;

          const taskA = await client.waitForTask('taskA', instanceId, 10000);
          await client.claimTask(taskA.id, 'alice');
          await client.completeTask(taskA.id, {});

          const taskB = await client.waitForTask('taskB', instanceId, 10000);
          await client.claimTask(taskB.id, 'bob');
          await client.completeTask(taskB.id, {});

          await client.waitForProcessEnd(instanceId, 10000);

          const logs = await getAuditLogs(instanceId);
          assert(logs.length > 0, 'expected a non-empty audit trail');

          assertOrderedSubsequence(logs, [
            'PROCESS_STARTED',
            'EXECUTION_CREATED',
            'TASK_CREATED',
            'TASK_CLAIMED',
            'TASK_COMPLETED',
            'EXECUTION_MOVED',
            'TASK_CREATED',
            'TASK_CLAIMED',
            'TASK_COMPLETED',
            'EXECUTION_COMPLETED',
            'PROCESS_COMPLETED',
          ]);

          for (const log of logs) {
            assert(
              log.processInstanceId === instanceId,
              `audit log entry belongs to wrong instance: ${JSON.stringify(log)}`
            );
          }

          const steps = await getTokenHistory(instanceId);
          assert(steps.length > 0, 'expected a non-empty token history');

          const path = collapseConsecutive(
            steps.map(s => s.elementId ?? '').filter(id => id.length > 0)
          );
          assert(
            JSON.stringify(path) === JSON.stringify(['start', 'taskA', 'taskB', 'end']),
            `expected token path [start, taskA, taskB, end], got: [${path.join(', ')}]`
          );
        },
      },

      {
        name: '[B] Move Token modification produces TASK_CANCELLED + EXECUTION_CREATED + EXECUTION_MOVED',
        async fn() {
          const inst = await client.startProcess('OperateProcess', { requester: 'audit-test-2' });
          const instanceId = inst.processInstanceId ?? inst.id;

          await client.waitForTask('taskA', instanceId, 10000);

          await client.api.post(`/v2/process-instances/${instanceId}/modification`, {
            moveInstructions: [{ sourceElementId: 'taskA', targetElementId: 'taskB' }],
          });

          const logs = await getAuditLogs(instanceId);
          const actions = logs.map(l => l.action);
          assert(actions.includes('TASK_CANCELLED'), `expected TASK_CANCELLED, got: [${actions.join(', ')}]`);
          assert(actions.includes('EXECUTION_CREATED'), `expected EXECUTION_CREATED, got: [${actions.join(', ')}]`);
          assert(actions.includes('EXECUTION_MOVED'), `expected EXECUTION_MOVED (taskB advancing), got: [${actions.join(', ')}]`);

          // Clean up: complete taskB so the process doesn't linger for later suites.
          const taskB = await client.waitForTask('taskB', instanceId, 10000);
          await client.completeTask(taskB.id, {});
        },
      },
    ],
  });
}

run().catch(console.error);
