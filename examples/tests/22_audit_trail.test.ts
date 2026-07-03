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
 *   D) Terminating a running instance (with an open task) soft-deletes it:
 *      the instance persists with status=terminated, its audit trail and
 *      token-history remain fully queryable, and the previously-open task
 *      is cancelled — instead of the old hard-delete that wiped everything.
 *   E) A parallel-gateway instance (03_meal_preparation.bpmn) produces
 *      token-history steps with at least two distinct, concurrently-active
 *      executionIds — the data multi-token replay groups by.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert } from './client';

const OPERATE_BPMN = path.join(__dirname, 'bpmn', '19_operate_process.bpmn');
const PARALLEL_BPMN = path.join(__dirname, 'bpmn', '03_meal_preparation.bpmn');
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
  executionId?: string;
  detail?: string;
}

async function getAuditLogs(instanceId: string): Promise<AuditLogEntry[]> {
  const res = await client.api.get(`/engine-rest/audit/process/${instanceId}`);
  return res.data.logs ?? [];
}

async function getTokenHistory(instanceId: string): Promise<TokenHistoryStep[]> {
  const res = await client.api.get(`/engine-rest/audit/process/${instanceId}/token-history`);
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
  await client.deployBpmn(PARALLEL_BPMN, 'Audit Trail Parallel Test');

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

      {
        name: '[D] Terminating a running instance soft-deletes it (persists, cancels open task)',
        async fn() {
          const inst = await client.startProcess('OperateProcess', { requester: 'audit-test-3' });
          const instanceId = inst.processInstanceId ?? inst.id;

          const taskA = await client.waitForTask('taskA', instanceId, 10000);

          const res = await client.api.delete(
            `/engine-rest/process-instance/${instanceId}?deleteReason=integration-test`
          );
          assert(res.status === 204, `expected 204 from terminate, got ${res.status}`);

          // Old behavior (hard delete): this would now 404. New behavior: the
          // row persists with status=terminated.
          const terminated = await client.getProcessInstance(instanceId);
          assert(terminated !== null, 'expected the terminated instance to still be queryable, not hard-deleted');
          assert(
            terminated!.status === 'terminated',
            `expected status "terminated", got "${terminated?.status}"`
          );

          // The audit trail (and by extension the token-history replay data)
          // must survive termination — this is the whole point of the fix.
          const logs = await getAuditLogs(instanceId);
          const actions = logs.map(l => l.action);
          assert(actions.includes('PROCESS_STARTED'), `expected prior history to survive, got: [${actions.join(', ')}]`);
          assert(actions.includes('PROCESS_TERMINATED'), `expected PROCESS_TERMINATED, got: [${actions.join(', ')}]`);
          assert(actions.includes('TASK_CANCELLED'), `expected the open task to be cancelled, got: [${actions.join(', ')}]`);

          const steps = await getTokenHistory(instanceId);
          assert(steps.length > 0, 'expected token-history to remain queryable after termination');

          // The previously-open task must be cancelled, not vanish — checked
          // via the audit trail rather than GET /engine-rest/tasks, since
          // that endpoint deliberately excludes cancelled/completed tasks
          // from its default "open tasks" list.
          const cancelLog = logs.find(l => l.action === 'TASK_CANCELLED');
          assert(
            cancelLog?.taskId === taskA.id,
            `expected TASK_CANCELLED audit entry for taskA (${taskA.id}), got: ${JSON.stringify(cancelLog)}`
          );
        },
      },

      {
        name: '[E] Parallel gateway produces two concurrently-active executionIds in token-history',
        async fn() {
          const inst = await client.startProcess('MealPrepProcess');
          const instanceId = inst.processInstanceId ?? inst.id;

          // Wait for the user-task branch to appear — by this point the fork
          // has happened and both branches (cook_pasta job + prepare_salad
          // task) are concurrently active, each its own execution/token.
          const saladTask = await client.waitForTask('prepare_salad', instanceId, 10000);

          const steps = await getTokenHistory(instanceId);
          const executionIds = new Set(steps.map(s => s.executionId).filter((id): id is string => !!id));
          assert(
            executionIds.size >= 2,
            `expected at least 2 distinct executionIds for the two parallel branches, got: [${[...executionIds].join(', ')}]`
          );

          // Clean up both branches so the process doesn't linger.
          const cookDone = client.pollExternalTask('cook_pasta', async () => ({ pastaReady: true }), 20000);
          await client.completeTask(saladTask.id, { saladReady: true });
          await cookDone;

          const eatTask = await client.waitForTask('eat_meal', instanceId, 15000);
          await client.completeTask(eatTask.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },
    ],
  });
}

run().catch(console.error);
