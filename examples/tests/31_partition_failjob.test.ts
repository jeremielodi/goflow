/**
 * Suite 31 — Phase 5 partitioned write path: job failure (FailJob) and,
 * added in the same-day gap-closing follow-up, retry-setting/incident-
 * resolution (see architecture/event-sourced-kafka-design.md).
 *
 * Phases 1-4 only gated the happy path (start, complete) through the
 * Command Gateway + Partition Processor. This suite closes a real,
 * pre-existing gap found while auditing what wasn't done yet:
 * service.FailJob was called unconditionally from both the REST and gRPC
 * failure endpoints, with no partition.LookupJobOwner check — a job
 * belonging to a partitioned instance that failed bypassed Kafka/partition
 * ownership entirely. internal/partition.ValidateShape also gained support
 * for an attached error boundary event, so the new FailJob command's
 * boundary-trigger branch (architecturally free, like Phase 2/4's
 * gateway/DMN cases) is actually exercised by a real shape, not shipped
 * untested.
 *
 * Tests [F]/[G] close the identical gap for POST /engine-rest/job/:id/retries
 * (REST) and the gRPC ResolveIncident RPC — flagged as a known, deferred
 * gap when Phase 5 shipped, closed the same day after the user asked to
 * fix all remaining known gaps.
 */
import * as path from 'path';
import { ZBClient } from 'zeebe-node';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BOUNDARY_BPMN = path.join(__dirname, 'bpmn', '31_boundary_error_chain.bpmn');
const SINGLE_TASK_BPMN = path.join(__dirname, 'bpmn', '27_single_service_task.bpmn');
const DIAMOND_BPMN = path.join(__dirname, 'bpmn', '28_diamond_gateway.bpmn');
const CHAIN_BPMN = path.join(__dirname, 'bpmn', '29_two_task_chain.bpmn');
const client = new GoFlowClient();

async function findDefinitionId(processKey: string): Promise<string> {
  const res = await client.api.get(`/engine-rest/process-definition?key=${processKey}`);
  const defs = (res.data as any[]) ?? [];
  assert(defs.length > 0, `expected at least one deployed definition for key ${processKey}`);
  return defs.sort((a, b) => b.version - a.version)[0].id;
}

async function setWritePath(definitionId: string, writePath: 'legacy' | 'partitioned') {
  return client.api.patch(`/engine-rest/process-definition/${definitionId}/write-path`, { writePath });
}

async function findJob(topic: string, instanceId: string, timeoutMs = 15000): Promise<any> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const tasks = await client.fetchAndLock([{ name: topic }]);
    const task = tasks.find((t) => t.processInstanceId === instanceId);
    if (task) return task;
    await sleep(500);
  }
  throw new Error(`timed out waiting for a job on topic "${topic}" for instance ${instanceId}`);
}

function newZbClient(): ZBClient {
  return new ZBClient('localhost:26500', { useTLS: false, retry: false, eagerConnection: false });
}

// The gRPC gateway hands back Zeebe-protocol int64 keys, not GoFlow's
// internal UUIDs — resolve the UUID via REST by finding the most recently
// started running instance of processKey (same trick suite 24 uses).
async function findRunningInstanceUUID(processKey: string): Promise<string> {
  const res = await client.api.get('/engine-rest/process-instance', { params: { processKey, status: 'running' } });
  const running = (res.data as any[]) ?? [];
  assert(running.length >= 1, `expected at least one running ${processKey} instance to resolve via REST`);
  running.sort((a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime());
  return running[0].id;
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BOUNDARY_BPMN);
  await client.deployBpmn(SINGLE_TASK_BPMN);
  await client.deployBpmn(DIAMOND_BPMN);
  await client.deployBpmn(CHAIN_BPMN);

  await runSuite({
    name: '31 — Phase 5 job failure (FailJob)',
    tests: [
      {
        name: '[A] Flipping the boundary-error-chain definition to partitioned succeeds',
        async fn() {
          const defId = await findDefinitionId('BoundaryErrorChainProcess');
          const res = await setWritePath(defId, 'partitioned');
          assert(res.status === 200, `expected 200, got ${res.status}`);
          assert(res.data.writePath === 'partitioned', `expected writePath=partitioned, got ${JSON.stringify(res.data)}`);
        },
      },

      {
        name: '[B] Failing with a matching error code triggers the boundary event and completes via it, no incident',
        async fn() {
          const inst = await client.startProcess('BoundaryErrorChainProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          const job = await findJob('process-risky-task', instanceId);
          await client.api.post(`/engine-rest/external-task/${job.id}/failure`, {
            workerId: client.workerId,
            errorMessage: 'simulated risk failure',
            errorCode: 'RISK_ERROR',
            retries: 0,
            retryTimeout: 0,
          });

          await client.waitForProcessEnd(instanceId, 15000);
          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'completed', `expected the boundary path to complete the process, got ${JSON.stringify(hist.data)}`);

          const incidents = await client.api.get('/engine-rest/incident', { params: { processInstanceId: instanceId } });
          assert(((incidents.data as any[]) ?? []).length === 0, `expected no incident when the boundary event catches the failure, got ${JSON.stringify(incidents.data)}`);
        },
      },

      {
        name: '[C] Failing with an unmatched error code (no boundary present) creates an incident via the shared, unmodified incident table',
        async fn() {
          const inst = await client.startProcess('SingleTaskProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          const job = await findJob('process-item', instanceId);
          await client.api.post(`/engine-rest/external-task/${job.id}/failure`, {
            workerId: client.workerId,
            errorMessage: 'simulated unrecoverable failure',
            errorCode: 'NO_SUCH_BOUNDARY',
            retries: 0,
            retryTimeout: 0,
          });

          let incidentId: string | null = null;
          const deadline = Date.now() + 15000;
          while (!incidentId && Date.now() < deadline) {
            const res = await client.api.get('/engine-rest/incident', { params: { processInstanceId: instanceId } });
            const incidents = (res.data as any[]) ?? [];
            if (incidents.length > 0 && incidents[0].state === 'open') incidentId = incidents[0].id;
            else await sleep(500);
          }
          assert(!!incidentId, 'expected an open incident to appear via the existing, unmodified incident REST endpoint');

          // Clean up so it doesn't linger — resolve via the existing retry endpoint.
          await client.api.post(`/engine-rest/job/${job.id}/retries`, { retries: 1 });
        },
      },

      {
        name: '[D] Failing with retries remaining resets the job for retry, then completing it normally finishes the process',
        async fn() {
          const inst = await client.startProcess('SingleTaskProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          const job = await findJob('process-item', instanceId);
          await client.api.post(`/engine-rest/external-task/${job.id}/failure`, {
            workerId: client.workerId,
            errorMessage: 'transient failure, will retry',
            retries: 2, // Camunda 7 convention: current count, decremented to 1 (still > 0)
            retryTimeout: 0,
          });

          const retriedJob = await findJob('process-item', instanceId);
          await client.completeExternalTask(retriedJob.id, { done: true });
          await client.waitForProcessEnd(instanceId, 15000);

          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'completed', `expected the process to complete after a successful retry, got ${JSON.stringify(hist.data)}`);
        },
      },

      {
        name: '[E] Phases 1-4 shapes still work after gating FailJob and adding boundary-event support',
        async fn() {
          const singleDefId = await findDefinitionId('SingleTaskProcess');
          await setWritePath(singleDefId, 'partitioned');
          const single = await client.startProcess('SingleTaskProcess', {});
          const singleId = single.processInstanceId ?? (single as any).id;
          await client.pollExternalTask('process-item', async () => ({ done: true }), 15000, singleId);
          await client.waitForProcessEnd(singleId, 15000);
          const singleHist = await client.api.get(`/engine-rest/history/process-instance/${singleId}`);
          assert(singleHist.data?.state === 'completed', `expected Phase 1's linear shape to still complete, got ${JSON.stringify(singleHist.data)}`);

          const diamondDefId = await findDefinitionId('DiamondProcess');
          await setWritePath(diamondDefId, 'partitioned');
          const diamond = await client.startProcess('DiamondProcess', { branch: 'a' });
          const diamondId = diamond.processInstanceId ?? (diamond as any).id;
          await client.pollExternalTask('process-branch-a', async () => ({ done: true }), 15000, diamondId);
          await client.waitForProcessEnd(diamondId, 15000);
          const diamondHist = await client.api.get(`/engine-rest/history/process-instance/${diamondId}`);
          assert(diamondHist.data?.state === 'completed', `expected Phase 2's diamond shape to still complete, got ${JSON.stringify(diamondHist.data)}`);

          const chainDefId = await findDefinitionId('TwoTaskChainProcess');
          await setWritePath(chainDefId, 'partitioned');
          const chain = await client.startProcess('TwoTaskChainProcess', {});
          const chainId = chain.processInstanceId ?? (chain as any).id;
          await client.pollExternalTask('process-step-one', async () => ({ done: true }), 15000, chainId);
          await client.pollExternalTask('process-step-two', async () => ({ done: true }), 15000, chainId);
          await client.waitForProcessEnd(chainId, 15000);
          const chainHist = await client.api.get(`/engine-rest/history/process-instance/${chainId}`);
          assert(chainHist.data?.state === 'completed', `expected Phase 3's chain shape to still complete, got ${JSON.stringify(chainHist.data)}`);
        },
      },

      {
        name: '[F] POST /engine-rest/job/:id/retries on a partitioned job resolves the incident and lets the job complete again',
        async fn() {
          const singleDefId = await findDefinitionId('SingleTaskProcess');
          await setWritePath(singleDefId, 'partitioned');

          const inst = await client.startProcess('SingleTaskProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          const job = await findJob('process-item', instanceId);
          await client.api.post(`/engine-rest/external-task/${job.id}/failure`, {
            workerId: client.workerId,
            errorMessage: 'simulated failure for retries test',
            errorCode: 'NO_SUCH_BOUNDARY',
            retries: 0,
            retryTimeout: 0,
          });

          let incidentId: string | null = null;
          const deadline = Date.now() + 15000;
          while (!incidentId && Date.now() < deadline) {
            const res = await client.api.get('/engine-rest/incident', { params: { processInstanceId: instanceId } });
            const incidents = (res.data as any[]) ?? [];
            if (incidents.length > 0 && incidents[0].state === 'open') incidentId = incidents[0].id;
            else await sleep(500);
          }
          assert(!!incidentId, 'expected an open incident before setting retries');

          const res = await client.api.post(`/engine-rest/job/${job.id}/retries`, { retries: 1 });
          assert(res.status === 200, `expected 200 setting retries on a partitioned job, got ${res.status}`);

          let resolved = false;
          const resolveDeadline = Date.now() + 15000;
          while (!resolved && Date.now() < resolveDeadline) {
            const r = await client.api.get(`/engine-rest/incident/${incidentId}`);
            resolved = r.data?.state === 'resolved';
            if (!resolved) await sleep(500);
          }
          assert(resolved, 'expected the incident to be resolved via the (now-gated) partitioned retries path');

          const retriedJob = await findJob('process-item', instanceId);
          await client.completeExternalTask(retriedJob.id, { done: true });
          await client.waitForProcessEnd(instanceId, 15000);
        },
      },

      {
        name: '[G] gRPC ResolveIncident on a partitioned instance resolves via the Command Gateway',
        async fn() {
          const singleDefId = await findDefinitionId('SingleTaskProcess');
          await setWritePath(singleDefId, 'partitioned');

          const zbc = newZbClient();
          try {
            const created: any = await zbc.createProcessInstance({ bpmnProcessId: 'SingleTaskProcess', variables: {} });
            const instanceUUID = await findRunningInstanceUUID('SingleTaskProcess');

            let jobs: any[] = [];
            const jobDeadline = Date.now() + 15000;
            while (jobs.length === 0 && Date.now() < jobDeadline) {
              jobs = await zbc.activateJobs({
                type: 'process-item',
                worker: 'suite-31-worker',
                timeout: 30000,
                maxJobsToActivate: 10,
                requestTimeout: 3000,
              } as any);
              if (jobs.length === 0) await sleep(500);
            }
            const job = jobs.find((j: any) => j.processInstanceKey === created.processInstanceKey);
            assert(!!job, 'expected to activate the job for the new partitioned instance over gRPC');

            await zbc.failJob({ jobKey: job.key, retries: 0, errorMessage: 'grpc suite induced failure (partitioned)', retryBackOff: 0 });

            let incidents: any[] = [];
            const incidentDeadline = Date.now() + 15000;
            while (incidents.length === 0 && Date.now() < incidentDeadline) {
              const res = await client.api.get('/engine-rest/incident', { params: { processInstanceId: instanceUUID } });
              incidents = (res.data as any[]) ?? [];
              if (incidents.length === 0) await sleep(500);
            }
            assert(incidents.length >= 1, 'expected an open incident for the partitioned gRPC-failed job');
            assert(typeof incidents[0].zeebeKey === 'number' || typeof incidents[0].zeebeKey === 'string',
              `expected the REST incident response to include a zeebeKey, got ${JSON.stringify(incidents[0])}`);

            await zbc.resolveIncident({ incidentKey: String(incidents[0].zeebeKey) });

            let resolved = false;
            const resolveDeadline = Date.now() + 15000;
            while (!resolved && Date.now() < resolveDeadline) {
              const r = await client.api.get(`/engine-rest/incident/${incidents[0].id}`);
              resolved = r.data?.state === 'resolved';
              if (!resolved) await sleep(500);
            }
            assert(resolved, 'expected the incident to be resolved via gRPC ResolveIncident on a partitioned instance');
          } finally {
            await zbc.close();
          }
        },
      },
    ],
  });
}

run().catch(err => {
  // See suite 26-30's identical footer for why this must be process.exit(1).
  console.error(err);
  process.exit(1);
});
