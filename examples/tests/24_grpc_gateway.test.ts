/**
 * Suite 24 — gRPC gateway (Zeebe protocol subset)
 *
 * Verifies internal/zeebegrpc's core loop — Topology, DeployResource,
 * CreateProcessInstance, ActivateJobs, CompleteJob, FailJob — against a
 * real, unmodified Zeebe client SDK (`zeebe-node`), not a hand-rolled
 * client, since the whole point of this feature is that genuine Zeebe
 * client SDKs can talk to GoFlow directly. Points at localhost:26500
 * (Zeebe's own default gateway port) with zero client-side config beyond
 * disabling TLS, matching a self-managed Zeebe broker with no auth.
 *
 * Tests:
 *   A) Topology succeeds — basic connectivity/protocol-compatibility check.
 *   B) DeployResource deploys the BPMN and reports the process metadata.
 *   C) CreateProcessInstance + manual ActivateJobs + CompleteJob drives a
 *      process to completion end-to-end over gRPC.
 *   D) FailJob with retries exhausted creates an incident, verified via
 *      the existing REST incidents API (proving the gRPC and REST
 *      surfaces share the same underlying engine state).
 */
import * as path from 'path';
import { ZBClient } from 'zeebe-node';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '24_grpc_process.bpmn');
const restClient = new GoFlowClient();

function newZbClient(): ZBClient {
  return new ZBClient('localhost:26500', {
    useTLS: false,
    retry: false,
    eagerConnection: false,
  });
}

// The gRPC gateway hands back Zeebe-protocol int64 keys, not GoFlow's
// internal UUIDs — the REST API (used here only to verify gRPC and REST
// share the same underlying engine state) needs the UUID. Resolve it by
// finding the single running GrpcProcess instance right after creation.
async function findRunningInstanceUUID(): Promise<string> {
  const res = await restClient.api.get('/engine-rest/process-instance', {
    params: { processKey: 'GrpcProcess', status: 'running' },
  });
  const running = (res.data as any[]) ?? [];
  assert(running.length >= 1, 'expected exactly one running GrpcProcess instance to resolve via REST');
  running.sort((a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime());
  return running[0].id;
}

async function run() {
  await restClient.loginAsSuperUser();

  const zbc = newZbClient();

  await runSuite({
    name: '24 — gRPC gateway (Zeebe protocol subset)',
    tests: [

      {
        name: '[A] Topology succeeds (real Zeebe client connectivity check)',
        async fn() {
          const topo = await zbc.topology();
          assert(Array.isArray(topo.brokers) && topo.brokers.length >= 1, 'expected at least one broker in topology');
          assert(topo.brokers[0].partitions.length >= 1, 'expected at least one partition');
        },
      },

      {
        name: '[B] DeployResource deploys the BPMN and reports process metadata',
        async fn() {
          const res: any = await zbc.deployResource({ processFilename: BPMN });
          assert(res.deployments.length >= 1, 'expected at least one deployment');
          const proc = res.deployments.find((d: any) => d.process)?.process;
          assert(!!proc, 'expected a process deployment');
          assert(proc.bpmnProcessId === 'GrpcProcess', `expected bpmnProcessId GrpcProcess, got ${proc.bpmnProcessId}`);
          assert(!!proc.processDefinitionKey, 'expected a non-empty processDefinitionKey');
        },
      },

      {
        name: '[C] CreateProcessInstance + ActivateJobs + CompleteJob completes the process end-to-end over gRPC',
        async fn() {
          const created: any = await zbc.createProcessInstance({ bpmnProcessId: 'GrpcProcess', variables: {} });
          assert(!!created.processInstanceKey, 'expected a processInstanceKey');
          const instanceUUID = await findRunningInstanceUUID();

          let jobs: any[] = [];
          const deadline = Date.now() + 15000;
          while (jobs.length === 0 && Date.now() < deadline) {
            jobs = await zbc.activateJobs({
              type: 'grpc_review',
              worker: 'suite-24-worker',
              timeout: 30000,
              maxJobsToActivate: 10,
              requestTimeout: 3000,
            } as any);
            if (jobs.length === 0) await sleep(500);
          }
          assert(jobs.length >= 1, 'expected to activate at least one grpc_review job');
          const job = jobs.find(j => j.processInstanceKey === created.processInstanceKey);
          assert(!!job, 'expected the activated job to belong to the instance just created');

          await zbc.completeJob({ jobKey: job.key, variables: { reviewed: true } });

          // Verify completion via the existing REST API — proves gRPC and
          // REST share the same underlying engine state.
          await restClient.waitForProcessEnd(instanceUUID);
          const inst = await restClient.getProcessInstance(instanceUUID);
          assert(inst !== null && inst.status === 'completed', `expected the instance to be completed, got ${inst?.status}`);
        },
      },

      {
        name: '[D] FailJob with retries exhausted creates an incident (verified via REST)',
        async fn() {
          const created: any = await zbc.createProcessInstance({ bpmnProcessId: 'GrpcProcess', variables: {} });
          const instanceUUID = await findRunningInstanceUUID();

          let jobs: any[] = [];
          const deadline = Date.now() + 15000;
          while (jobs.length === 0 && Date.now() < deadline) {
            jobs = await zbc.activateJobs({
              type: 'grpc_review',
              worker: 'suite-24-worker',
              timeout: 30000,
              maxJobsToActivate: 10,
              requestTimeout: 3000,
            } as any);
            if (jobs.length === 0) await sleep(500);
          }
          const job = jobs.find(j => j.processInstanceKey === created.processInstanceKey);
          assert(!!job, 'expected to activate the job for the new instance');

          await zbc.failJob({ jobKey: job.key, retries: 0, errorMessage: 'grpc suite induced failure', retryBackOff: 0 });

          // Poll the REST incidents API — no waitFor helper exists for
          // incidents, so poll directly.
          let incidents: any[] = [];
          const incidentDeadline = Date.now() + 10000;
          while (incidents.length === 0 && Date.now() < incidentDeadline) {
            const res = await restClient.api.get(`/engine-rest/process-instance/${instanceUUID}/incidents`);
            incidents = res.data ?? [];
            if (incidents.length === 0) await sleep(500);
          }
          assert(incidents.length >= 1, 'expected an incident to be created after retries were exhausted via gRPC FailJob');
        },
      },

    ],
  });

  zbc.close();
}

run().catch(console.error);
