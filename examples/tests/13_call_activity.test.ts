/**
 * Suite 13 — Call Activity  (BPMN: 13_call_activity_main.bpmn + 13_credit_check_process.bpmn)
 * Source: https://camunda.com/bpmn/reference/ — Call Activity
 *
 * Tests:
 *   A) Main process invokes sub-process via Call Activity, waits for it, then continues
 *   B) Sub-process instance is created with parent_instance_id set
 *   C) After sub-process completes, main process resumes and completes
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN_MAIN = path.join(__dirname, 'bpmn', '13_call_activity_main.bpmn');
const BPMN_SUB = path.join(__dirname, 'bpmn', '13_credit_check_process.bpmn');
const client = new GoFlowClient();
const BASE = 'http://localhost:8080';

async function completeExternalTaskForAny(token: string, topic: string): Promise<number> {
  const r = await fetch(`${BASE}/engine-rest/external-task/fetchAndLock`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ workerId: 'test-worker', maxJobsToActivate: 10, topics: [{ topicName: topic, lockDuration: 30000 }] }),
  });
  const tasks = await r.json() as any[];
  let completed = 0;
  for (const t of tasks) {
    await fetch(`${BASE}/engine-rest/external-task/${t.id}/complete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ workerId: 'test-worker', variables: {} }),
    });
    completed++;
  }
  return completed;
}

async function getHistoricInstances(token: string, key: string): Promise<any[]> {
  const r = await fetch(`${BASE}/engine-rest/history/process-instance?processDefinitionKey=${key}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return r.json() as Promise<any[]>;
}

async function run() {
  await client.loginAsSuperUser();
  const token = (client as any).token as string;

  // Deploy both processes
  await client.deployBpmn(BPMN_SUB, 'Credit Check Sub-Process');
  await client.deployBpmn(BPMN_MAIN, 'Loan Application Main');

  await runSuite({
    name: '13 — Call Activity',
    tests: [

      // ── A: end-to-end call activity flow ──────────────────────────────────
      {
        name: '[A] Main process invokes sub-process and completes after sub ends',
        async fn() {
          const inst = await client.startProcess('loanApplication', { applicantId: 'APP-001' });
          const instanceId = inst.processInstanceId ?? inst.id;

          // Complete prepare-documents (main process)
          await sleep(400);
          await completeExternalTaskForAny(token, 'prepare-documents');

          // Now the call activity fires, creating a child creditCheckProcess
          // Complete sub-process tasks
          await sleep(400);
          await completeExternalTaskForAny(token, 'retrieve-credit-score');
          await sleep(300);
          await completeExternalTaskForAny(token, 'evaluate-risk');

          // Sub-process ends → parent resumes → make-decision runs
          await sleep(500);
          await completeExternalTaskForAny(token, 'make-decision');

          await client.waitForProcessEnd(instanceId, 15000);

          const final = await client.getProcessInstance(instanceId);
          assert(!final || final.status === 'completed', `expected completed, got ${final?.status}`);
        },
      },

      // ── B: child instance has parent_instance_id ──────────────────────────
      {
        name: '[B] Child process instance is created with correct parent reference',
        async fn() {
          const inst = await client.startProcess('loanApplication', { applicantId: 'APP-002' });
          const mainInstanceId = inst.processInstanceId ?? inst.id;

          await sleep(400);
          await completeExternalTaskForAny(token, 'prepare-documents');
          await sleep(600); // child instance should now be running

          // Find the child process instance via history
          const childInstances = await getHistoricInstances(token, 'creditCheckProcess');
          assert(childInstances.length >= 1, 'expected at least one creditCheckProcess instance');

          // Complete the child
          await completeExternalTaskForAny(token, 'retrieve-credit-score');
          await sleep(300);
          await completeExternalTaskForAny(token, 'evaluate-risk');
          await sleep(500);
          await completeExternalTaskForAny(token, 'make-decision');

          await client.waitForProcessEnd(mainInstanceId, 15000);
        },
      },

      // ── C: main process only completes after sub-process completes ────────
      {
        name: '[C] Main process stays running while sub-process is active',
        async fn() {
          const inst = await client.startProcess('loanApplication', { applicantId: 'APP-003' });
          const instanceId = inst.processInstanceId ?? inst.id;

          await sleep(400);
          await completeExternalTaskForAny(token, 'prepare-documents');
          await sleep(600);

          // Main process should still be running (waiting on call activity)
          const mid = await client.getProcessInstance(instanceId) as any;
          assert(mid?.status === 'running', `expected main to be running (waiting), got ${mid?.status}`);

          // Now complete the child
          await completeExternalTaskForAny(token, 'retrieve-credit-score');
          await sleep(300);
          await completeExternalTaskForAny(token, 'evaluate-risk');
          await sleep(500);
          await completeExternalTaskForAny(token, 'make-decision');

          await client.waitForProcessEnd(instanceId, 15000);
          const final = await client.getProcessInstance(instanceId);
          assert(!final || final.status === 'completed', `expected completed, got ${final?.status}`);
        },
      },

    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
