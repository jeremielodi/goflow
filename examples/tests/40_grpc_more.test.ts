/**
 * Suite 40 — gRPC gateway, remaining RPCs  (BPMN: 40_grpc_more.bpmn)
 *
 * Driven by the real zeebe-node SDK where it exposes the RPC; for
 * StreamActivatedJobs and DeleteResource (absent from zeebe-node 8.3) by
 * grpc-js loading zeebe-node's own official zeebe.proto — the genuine
 * protocol definition, not GoFlow's.
 *
 * Tests:
 *   A) ThrowError: the job's BPMN error is caught by the error boundary.
 *   B) SetVariables: a variable set on the instance drives the next gateway.
 *   C) CreateProcessInstanceWithResult: returns once the instance completed,
 *      with the variables the worker produced.
 *   D) StreamActivatedJobs: jobs are pushed on the stream as they appear.
 *   E) DeployProcess (deprecated) deploys a process.
 *   F) DeleteResource: refused while an instance exists; afterwards the
 *      definition is gone.
 */
import * as path from 'path';
import * as fs from 'fs';
import * as os from 'os';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { ZBClient } from 'zeebe-node';
import { GoFlowClient, runSuite, assert, sleep } from './client';
import { GRPC_BASIC_AUTH, authInterceptor } from './grpc_client';

const BPMN = path.join(__dirname, 'bpmn', '40_grpc_more.bpmn');
const GRPC_ADDR = process.env.GRPC_ADDR ?? '127.0.0.1:26500';
const restClient = new GoFlowClient();

function newZbClient(): ZBClient {
  return new ZBClient(GRPC_ADDR, { useTLS: false, retry: false, eagerConnection: false, basicAuth: GRPC_BASIC_AUTH });
}

// Raw client on zeebe-node's official proto, for RPCs zeebe-node 8.3 lacks.
function officialGateway(): any {
  const def = protoLoader.loadSync(path.join(__dirname, 'node_modules', 'zeebe-node', 'proto', 'zeebe.proto'), {
    keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
  });
  const proto = grpc.loadPackageDefinition(def) as any;
  return new proto.gateway_protocol.Gateway(GRPC_ADDR, grpc.credentials.createInsecure(), { interceptors: [authInterceptor()] });
}

function call<T>(client: any, method: string, req: unknown): Promise<T> {
  return new Promise((resolve, reject) => client[method](req, (err: grpc.ServiceError | null, res: T) => (err ? reject(err) : resolve(res))));
}

async function instanceUUID(processKey: string): Promise<string> {
  const res = await restClient.api.get('/engine-rest/process-instance', { params: { processKey, status: 'running' } });
  const running = (res.data as any[]) ?? [];
  assert(running.length >= 1, `expected a running ${processKey} instance`);
  running.sort((a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime());
  return running[0].id;
}

async function activateOne(zbc: ZBClient, type: string, instanceKey: string): Promise<any> {
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    const jobs = await (zbc as any).activateJobs({ type, worker: 'suite40', maxJobsToActivate: 10, timeout: 30000, requestTimeout: 2000 });
    const job = (jobs ?? []).find((j: any) => String(j.processInstanceKey) === String(instanceKey));
    if (job) return job;
    await sleep(300);
  }
  throw new Error(`no ${type} job for instance ${instanceKey}`);
}

async function run() {
  await restClient.loginAsSuperUser();
  const zbc = newZbClient();
  const gateway = officialGateway();
  const deployed = await zbc.deployResource({ processFilename: BPMN });
  assert(deployed.deployments.length === 3, `expected 3 deployed processes, got ${deployed.deployments.length}`);

  try {
    await runSuite({
      name: '40 — gRPC gateway, remaining RPCs',
      tests: [
        {
          name: '[A] ThrowError: the BPMN error is caught by the error boundary',
          async fn() {
            const inst = await zbc.createProcessInstance({ bpmnProcessId: 'GrpcThrow', variables: {} });
            const job = await activateOne(zbc, 'grpc_throw_job', inst.processInstanceKey);
            await zbc.throwError({ jobKey: job.key, errorCode: 'GRPC_ERR', errorMessage: 'card declined' });

            const id = await instanceUUID('GrpcThrow');
            const caught = await restClient.waitForTask('grpc_caught', id, 10000);
            await restClient.completeTask(caught.id);
            await restClient.waitForProcessEnd(id, 10000);
          },
        },
        {
          name: '[B] SetVariables drives the next gateway',
          async fn() {
            const inst = await zbc.createProcessInstance({ bpmnProcessId: 'GrpcVars', variables: { approved: false } });
            const id = await instanceUUID('GrpcVars');
            const wait = await restClient.waitForTask('grpc_wait', id, 10000);

            const res = await zbc.setVariables({ elementInstanceKey: inst.processInstanceKey, variables: { approved: true }, local: false });
            assert(!!(res as any).key, 'SetVariables should return a key');

            await restClient.completeTask(wait.id);
            const approved = await restClient.waitForTask('grpc_approved', id, 10000);
            await restClient.completeTask(approved.id);
            await restClient.waitForProcessEnd(id, 10000);
          },
        },
        {
          name: '[C] CreateProcessInstanceWithResult returns the completed instance\'s variables',
          async fn() {
            const worker = (async () => {
              const deadline = Date.now() + 15000;
              while (Date.now() < deadline) {
                const jobs = await (zbc as any).activateJobs({ type: 'grpc_result_job', worker: 'suite40', maxJobsToActivate: 1, timeout: 30000, requestTimeout: 1000 });
                if (jobs && jobs.length > 0) {
                  await zbc.completeJob({ jobKey: jobs[0].key, variables: { result: 42 } });
                  return;
                }
              }
            })();
            const res: any = await zbc.createProcessInstanceWithResult({
              bpmnProcessId: 'GrpcResult', variables: { input: 'x' }, requestTimeout: 15000,
            } as any);
            await worker;
            assert(res.variables?.result === 42, `expected result 42, got ${JSON.stringify(res.variables)}`);
            assert(res.variables?.input === 'x', `expected input "x", got ${JSON.stringify(res.variables)}`);
          },
        },
        {
          name: '[D] StreamActivatedJobs pushes jobs as they appear',
          async fn() {
            const received: any[] = [];
            const stream = gateway.StreamActivatedJobs({ type: 'grpc_result_job', worker: 'suite40-stream', timeout: 30000, fetchVariable: [], tenantIds: [] });
            stream.on('data', (job: any) => received.push(job));
            stream.on('error', () => undefined);
            try {
              await sleep(500);
              const a = await zbc.createProcessInstance({ bpmnProcessId: 'GrpcResult', variables: {} });
              const b = await zbc.createProcessInstance({ bpmnProcessId: 'GrpcResult', variables: {} });
              const want = new Set([String(a.processInstanceKey), String(b.processInstanceKey)]);
              const deadline = Date.now() + 10000;
              while (received.filter(j => want.has(String(j.processInstanceKey))).length < 2 && Date.now() < deadline) {
                await sleep(200);
              }
              const mine = received.filter(j => want.has(String(j.processInstanceKey)));
              assert(mine.length === 2, `expected 2 streamed jobs, got ${mine.length}`);
              for (const j of mine) await zbc.completeJob({ jobKey: j.key, variables: {} });
            } finally {
              stream.cancel();
            }
          },
        },
        {
          name: '[E] DeployProcess (deprecated) deploys a process',
          async fn() {
            const xml = fs.readFileSync(BPMN, 'utf8').replace(/GrpcResult/g, 'GrpcLegacyDeploy');
            const res: any = await zbc.deployProcess({ definition: Buffer.from(xml), name: 'legacy.bpmn' } as any);
            const ids = (res.processes ?? []).map((p: any) => p.bpmnProcessId);
            assert(ids.includes('GrpcLegacyDeploy'), `DeployProcess response processes: ${JSON.stringify(ids)}`);
          },
        },
        {
          name: '[F] DeleteResource refuses while an instance exists, then deletes the definition',
          async fn() {
            const xml = fs.readFileSync(BPMN, 'utf8').replace(/GrpcVars/g, 'GrpcDeleteMe');
            const file = path.join(os.tmpdir(), `40_delete_${Date.now()}.bpmn`);
            fs.writeFileSync(file, xml);
            const dep = await zbc.deployResource({ processFilename: file });
            fs.unlinkSync(file);
            const def = dep.deployments.map((d: any) => d.process).find((p: any) => p?.bpmnProcessId === 'GrpcDeleteMe');
            assert(!!def, 'GrpcDeleteMe not deployed');

            await zbc.createProcessInstance({ bpmnProcessId: 'GrpcDeleteMe', variables: {} });
            let refused = false;
            try {
              await call(gateway, 'DeleteResource', { resourceKey: def.processDefinitionKey });
            } catch (err: any) {
              refused = err.code === grpc.status.FAILED_PRECONDITION;
            }
            assert(refused, 'deleting a definition with a running instance should fail with FAILED_PRECONDITION');

            const id = await instanceUUID('GrpcDeleteMe');
            await restClient.api.delete(`/engine-rest/process-instance/${id}`);
            await sleep(500);
            await call(gateway, 'DeleteResource', { resourceKey: def.processDefinitionKey });

            let gone = false;
            try {
              await zbc.createProcessInstance({ bpmnProcessId: 'GrpcDeleteMe', variables: {} });
            } catch {
              gone = true;
            }
            assert(gone, 'starting a deleted process definition should fail');
          },
        },
      ],
    });
  } finally {
    gateway.close();
    await zbc.close();
  }
}

run().catch(err => { console.error(err); process.exit(1); });
