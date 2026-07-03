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
 *   E) PublishMessage correlates to a process instance parked at a
 *      message catch event (reuses the same orderNotification.bpmn /
 *      PaymentReceived pairing suite 15[D] already exercises via REST).
 *   F) CancelProcessInstance terminates a running instance, verified via
 *      the existing REST API.
 *   G) EvaluateDecision evaluates a deployed DMN table directly, outside
 *      of any process instance (reuses suite 17's approvalDecision.dmn).
 */
import * as path from 'path';
import { ZBClient } from 'zeebe-node';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '24_grpc_process.bpmn');
const ORDER_BPMN = path.join(__dirname, 'bpmn', '10_order_notification.bpmn');
const DMN = path.join(__dirname, 'bpmn', '17_approval_decision.dmn');
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
// finding the most recently started running instance of processKey.
async function findRunningInstanceUUID(processKey: string): Promise<string> {
  const res = await restClient.api.get('/engine-rest/process-instance', {
    params: { processKey, status: 'running' },
  });
  const running = (res.data as any[]) ?? [];
  assert(running.length >= 1, `expected at least one running ${processKey} instance to resolve via REST`);
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
          const instanceUUID = await findRunningInstanceUUID('GrpcProcess');

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
          const instanceUUID = await findRunningInstanceUUID('GrpcProcess');

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

      {
        name: '[E] PublishMessage correlates to a process instance parked at a message catch event',
        async fn() {
          await zbc.deployResource({ processFilename: ORDER_BPMN });
          await zbc.createProcessInstance({ bpmnProcessId: 'orderNotification', variables: { orderId: 'GRPC-MSG-001' } });
          const instanceUUID = await findRunningInstanceUUID('orderNotification');
          await sleep(500);

          const res: any = await zbc.publishMessage({ name: 'PaymentReceived', correlationKey: '', variables: {} });
          assert(!!res.key, 'expected a message key in the PublishMessage response');

          // Drain the rest of the process so it doesn't dangle — matches
          // suite 15[D]'s exact sequence for the same BPMN, just driven
          // over gRPC instead of REST.
          await sleep(300);
          await restClient.pollExternalTask('confirm-payment', async () => ({}), 10000, instanceUUID);
          await sleep(300);
          await zbc.publishMessage({ name: 'ShipmentReady', correlationKey: '', variables: {} });
          await sleep(300);
          await restClient.pollExternalTask('notify-customer', async () => ({}), 10000, instanceUUID);
          await restClient.waitForProcessEnd(instanceUUID, 10000);
        },
      },

      {
        name: '[F] CancelProcessInstance terminates a running instance',
        async fn() {
          const created: any = await zbc.createProcessInstance({ bpmnProcessId: 'GrpcProcess', variables: {} });
          const instanceUUID = await findRunningInstanceUUID('GrpcProcess');

          await zbc.cancelProcessInstance(created.processInstanceKey);

          const inst = await restClient.getProcessInstance(instanceUUID);
          assert(inst !== null && inst.status === 'terminated', `expected the instance to be terminated, got ${inst?.status}`);
        },
      },

      {
        name: '[G] EvaluateDecision evaluates a deployed DMN table directly',
        async fn() {
          await zbc.deployResource({ decisionFilename: DMN });

          const res: any = await zbc.evaluateDecision({ decisionId: 'approvalDecision', variables: { amount: 500, tier: 'gold' } });
          const output = JSON.parse(res.decisionOutput);
          assert(output.approved === true, `expected approved=true for a gold-tier order under 1000, got ${JSON.stringify(output)}`);
          assert(output.discount === 0.1, `expected discount 0.1, got ${output.discount}`);
        },
      },

      {
        name: '[H] ResolveIncident resolves an incident discovered via its REST-exposed zeebeKey',
        async fn() {
          const created: any = await zbc.createProcessInstance({ bpmnProcessId: 'GrpcProcess', variables: {} });
          const instanceUUID = await findRunningInstanceUUID('GrpcProcess');

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

          await zbc.failJob({ jobKey: job.key, retries: 0, errorMessage: 'grpc suite induced failure (for resolve)', retryBackOff: 0 });

          let incidents: any[] = [];
          const incidentDeadline = Date.now() + 10000;
          while (incidents.length === 0 && Date.now() < incidentDeadline) {
            const res = await restClient.api.get('/engine-rest/incident', { params: { processInstanceId: instanceUUID } });
            incidents = res.data ?? [];
            if (incidents.length === 0) await sleep(500);
          }
          assert(incidents.length >= 1, 'expected an incident to exist for the failed job');
          assert(typeof incidents[0].zeebeKey === 'number' || typeof incidents[0].zeebeKey === 'string',
            `expected the REST incident response to include a zeebeKey, got ${JSON.stringify(incidents[0])}`);

          await zbc.resolveIncident({ incidentKey: String(incidents[0].zeebeKey) });

          let resolved = false;
          const resolveDeadline = Date.now() + 10000;
          while (!resolved && Date.now() < resolveDeadline) {
            const res = await restClient.api.get(`/engine-rest/incident/${incidents[0].id}`);
            resolved = res.data?.state === 'resolved';
            if (!resolved) await sleep(500);
          }
          assert(resolved, 'expected the incident to be resolved via gRPC ResolveIncident');
        },
      },

      {
        name: '[I] ModifyProcessInstance moves the active token to a different element',
        async fn() {
          const created: any = await zbc.createProcessInstance({ bpmnProcessId: 'GrpcProcess', variables: {} });
          const instanceUUID = await findRunningInstanceUUID('GrpcProcess');

          // The instance is parked at reviewTask waiting for a grpc_review
          // job — move it straight to "end" without ever completing that
          // job, the classic process-instance-modification use case
          // (skip a step). Only one implied move is supported (see
          // gateway.proto's ModifyProcessInstance note): activateInstructions[0]
          // is the target, terminateInstructions is accepted on the wire
          // (the real SDK always sends one) but the source is resolved
          // server-side from the instance's single active execution.
          await zbc.modifyProcessInstance({
            processInstanceKey: created.processInstanceKey,
            activateInstructions: [{ elementId: 'end', ancestorElementInstanceKey: '-1', variableInstructions: [] }],
            terminateInstructions: [{ elementInstanceKey: '-1' }],
          });

          await restClient.waitForProcessEnd(instanceUUID, 10000);
          const inst = await restClient.getProcessInstance(instanceUUID);
          assert(inst !== null && inst.status === 'completed', `expected the instance to complete after moving to "end", got ${inst?.status}`);
        },
      },

    ],
  });

  zbc.close();
}

run().catch(console.error);
