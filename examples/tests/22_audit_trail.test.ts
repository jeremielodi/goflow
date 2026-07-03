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
 *   D) Terminating a running instance (with an open task) archives it: the
 *      raw audit log is gone (proving the live row was actually archived
 *      and deleted, not just soft-deleted) but GET /process-instance/:id
 *      and the token-history replay endpoint both keep working via the
 *      historic-archive fallback — see internal/service/archive_service.go.
 *   E) A parallel-gateway instance (03_meal_preparation.bpmn) produces
 *      token-history steps with at least two distinct, concurrently-active
 *      executionIds — the data multi-token replay groups by.
 *   F) GET /engine-rest/history/task merges live + archived tasks correctly
 *      (also the fix for the old bare /history/tasks routing bug — see
 *      historic_task_controller.go).
 *
 * Item 4 (historic archive) note: completing or terminating an instance now
 * triggers ArchiveAndDeleteProcessInstance, which copies the instance into
 * historic_process_instances/historic_activity_instances/historic_tasks/
 * historic_variables and then hard-deletes the live process_instances row
 * (cascading audit_logs/tasks/executions/variables away). So after
 * completion/termination: GET /audit/process/:id (raw) goes empty — that's
 * expected and is the proof archival really happened, not a bug — while
 * GET /process-instance/:id and GET /audit/process/:id/token-history keep
 * working unchanged via the live→historic fallback built into those
 * endpoints specifically.
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
  taskId?: string;
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
// Accepts anything with an `action` field, so it works for both raw
// AuditLogEntry rows and TokenHistoryStep rows.
function assertOrderedSubsequence(logs: { action: string }[], expectedInOrder: string[]) {
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

          // Snapshot the raw audit log while the instance is still live, to
          // check per-row processInstanceId scoping — this snapshot is only
          // valid before completion triggers archival (see suite header).
          const midLogs = await getAuditLogs(instanceId);
          assert(midLogs.length > 0, 'expected a non-empty audit trail while still live');
          for (const log of midLogs) {
            assert(
              log.processInstanceId === instanceId,
              `audit log entry belongs to wrong instance: ${JSON.stringify(log)}`
            );
          }

          const taskB = await client.waitForTask('taskB', instanceId, 10000);
          await client.claimTask(taskB.id, 'bob');
          await client.completeTask(taskB.id, {});

          await client.waitForProcessEnd(instanceId, 10000);

          // Completion triggers ArchiveAndDeleteProcessInstance: the raw
          // audit log is now gone (no historic fallback for this endpoint,
          // by design) — an empty result here is the expected proof that
          // archival actually deleted the live rows, not a bug.
          const logsAfterArchive = await getAuditLogs(instanceId);
          assert(
            logsAfterArchive.length === 0,
            `expected the raw audit log to be empty after archival, got: [${logsAfterArchive.map(l => l.action).join(', ')}]`
          );

          // The derived token-history endpoint must keep working, sourced
          // from historic_activity_instances instead — same ordering, same
          // action vocabulary, same JSON response shape as before archival.
          const steps = await getTokenHistory(instanceId);
          assert(steps.length > 0, 'expected token-history to remain queryable after archival');

          assertOrderedSubsequence(
            steps,
            [
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
            ]
          );

          const path = collapseConsecutive(
            steps.map(s => s.elementId ?? '').filter(id => id.length > 0)
          );
          assert(
            JSON.stringify(path) === JSON.stringify(['start', 'taskA', 'taskB', 'end']),
            `expected token path [start, taskA, taskB, end], got: [${path.join(', ')}]`
          );

          // GET /process-instance/:id must also keep working post-archival,
          // via the live→historic fallback added to v1 GetProcessInstance.
          const archivedInstance = await client.getProcessInstance(instanceId);
          assert(archivedInstance !== null, 'expected GET /process-instance/:id to still work after archival');
          assert(
            archivedInstance!.status === 'completed',
            `expected status "completed" from the historic fallback, got "${archivedInstance?.status}"`
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
        name: '[D] Terminating a running instance archives it (still queryable, open task cancelled, raw log gone)',
        async fn() {
          const inst = await client.startProcess('OperateProcess', { requester: 'audit-test-3' });
          const instanceId = inst.processInstanceId ?? inst.id;

          const taskA = await client.waitForTask('taskA', instanceId, 10000);

          const res = await client.api.delete(
            `/engine-rest/process-instance/${instanceId}?deleteReason=integration-test`
          );
          assert(res.status === 204, `expected 204 from terminate, got ${res.status}`);

          // Termination triggers cancel-then-archive-then-delete. The live
          // row is gone, but GET /process-instance/:id keeps working via the
          // historic fallback (old hard-delete behavior would 404 here).
          const terminated = await client.getProcessInstance(instanceId);
          assert(terminated !== null, 'expected the terminated instance to still be queryable via the historic archive');
          assert(
            terminated!.status === 'terminated',
            `expected status "terminated", got "${terminated?.status}"`
          );

          // Raw audit log is gone — proof the live row was actually
          // archived and deleted, not merely soft-deleted (expected, by
          // design: this endpoint has no historic fallback).
          const logs = await getAuditLogs(instanceId);
          assert(
            logs.length === 0,
            `expected the raw audit log to be empty after archival, got: [${logs.map(l => l.action).join(', ')}]`
          );

          // The token-history replay endpoint must keep working, sourced
          // from historic_activity_instances, and still show the full story:
          // the instance started, its open task was cancelled, then it was
          // terminated.
          const steps = await getTokenHistory(instanceId);
          assert(steps.length > 0, 'expected token-history to remain queryable after archival');
          const actions = steps.map(s => s.action);
          assert(actions.includes('PROCESS_STARTED'), `expected prior history to survive, got: [${actions.join(', ')}]`);
          assert(actions.includes('TASK_CANCELLED'), `expected the open task to be cancelled, got: [${actions.join(', ')}]`);
          assert(actions.includes('PROCESS_TERMINATED'), `expected PROCESS_TERMINATED, got: [${actions.join(', ')}]`);

          const cancelStep = steps.find(s => s.action === 'TASK_CANCELLED');
          assert(
            cancelStep?.taskId === taskA.id,
            `expected TASK_CANCELLED step for taskA (${taskA.id}), got: ${JSON.stringify(cancelStep)}`
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

      {
        name: '[F] GET /engine-rest/history/task merges live and archived tasks correctly (also fixes the old /history/tasks routing bug)',
        async fn() {
          const inst = await client.startProcess('OperateProcess', { requester: 'audit-test-4' });
          const instanceId = inst.processInstanceId ?? inst.id;

          const taskA = await client.waitForTask('taskA', instanceId, 10000);

          // While the instance is still live, historic tasks come from the
          // live `tasks` table.
          const liveResp = await client.api.get('/engine-rest/history/task', {
            params: { processInstanceId: instanceId },
          });
          assert(
            liveResp.data.some((t: any) => t.id === taskA.id && t.status === 'created'),
            `expected taskA (live, created) in /history/task, got: ${JSON.stringify(liveResp.data)}`
          );

          await client.claimTask(taskA.id, 'alice');
          await client.completeTask(taskA.id, {});
          const taskB = await client.waitForTask('taskB', instanceId, 10000);
          await client.claimTask(taskB.id, 'bob');
          await client.completeTask(taskB.id, {});
          await client.waitForProcessEnd(instanceId, 10000);

          // After archival, the same query now serves both tasks from
          // historic_tasks instead — same response shape, same endpoint.
          const archivedResp = await client.api.get('/engine-rest/history/task', {
            params: { processInstanceId: instanceId },
          });
          const archivedIds = archivedResp.data.map((t: any) => t.id);
          assert(
            archivedIds.includes(taskA.id) && archivedIds.includes(taskB.id),
            `expected both taskA and taskB in /history/task after archival, got: ${JSON.stringify(archivedResp.data)}`
          );
          assert(
            archivedResp.data.every((t: any) => t.status === 'completed'),
            `expected both archived tasks to show status "completed", got: ${JSON.stringify(archivedResp.data)}`
          );

          // GET /engine-rest/history/task/:id (new — the old code had no
          // single-task lookup at all).
          const singleResp = await client.api.get(`/engine-rest/history/task/${taskA.id}`);
          assert(singleResp.data.id === taskA.id, `expected taskA by id, got: ${JSON.stringify(singleResp.data)}`);

          // GET /engine-rest/history/task/count (new route — existed in
          // code before but was never actually registered).
          const countResp = await client.api.get('/engine-rest/history/task/count', {
            params: { processInstanceId: instanceId },
          });
          assert(countResp.data.count === 2, `expected count 2, got: ${JSON.stringify(countResp.data)}`);
        },
      },
    ],
  });
}

run().catch(console.error);
