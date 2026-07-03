/**
 * grpc_client.ts — a minimal, dynamically-loaded gRPC client for GoFlow's
 * Zeebe-protocol-subset gateway (internal/zeebegrpc). Uses
 * @grpc/proto-loader to compile internal/zeebegrpc/proto/gateway.proto at
 * runtime — no codegen needed on the TypeScript side.
 *
 * This exists for fast iteration while building/debugging each RPC by
 * hand. The real compatibility test uses the actual `zeebe-node` SDK
 * (see 24_grpc_gateway.test.ts) — this client is not itself proof of
 * protocol compatibility, since it's not a genuine third-party
 * implementation.
 */
import * as path from 'path';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

const PROTO_PATH = path.join(__dirname, '..', '..', 'internal', 'zeebegrpc', 'proto', 'gateway.proto');
const GRPC_ADDR = 'localhost:26500';

export class GrpcGatewayClient {
  private client: any;

  constructor() {
    const packageDef = protoLoader.loadSync(PROTO_PATH, {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });
    const proto = grpc.loadPackageDefinition(packageDef) as any;
    this.client = new proto.gateway_protocol.Gateway(GRPC_ADDR, grpc.credentials.createInsecure());
  }

  topology(): Promise<any> {
    return new Promise((resolve, reject) => {
      this.client.Topology({}, (err: grpc.ServiceError | null, res: any) => (err ? reject(err) : resolve(res)));
    });
  }

  deployResource(resources: Array<{ name: string; content: Buffer }>): Promise<any> {
    return new Promise((resolve, reject) => {
      this.client.DeployResource({ resources }, (err: grpc.ServiceError | null, res: any) =>
        err ? reject(err) : resolve(res)
      );
    });
  }

  createProcessInstance(bpmnProcessId: string, variables: Record<string, unknown> = {}): Promise<any> {
    return new Promise((resolve, reject) => {
      this.client.CreateProcessInstance(
        { bpmnProcessId, version: -1, variables: JSON.stringify(variables) },
        (err: grpc.ServiceError | null, res: any) => (err ? reject(err) : resolve(res))
      );
    });
  }

  completeJob(jobKey: string, variables: Record<string, unknown> = {}): Promise<any> {
    return new Promise((resolve, reject) => {
      this.client.CompleteJob(
        { jobKey, variables: JSON.stringify(variables) },
        (err: grpc.ServiceError | null, res: any) => (err ? reject(err) : resolve(res))
      );
    });
  }

  failJob(jobKey: string, retries: number, errorMessage: string): Promise<any> {
    return new Promise((resolve, reject) => {
      this.client.FailJob(
        { jobKey, retries, errorMessage },
        (err: grpc.ServiceError | null, res: any) => (err ? reject(err) : resolve(res))
      );
    });
  }

  /** Activates at most one batch of jobs (a single ActivateJobs call — matches how a real Zeebe client polls in a loop). */
  activateJobs(type: string, worker = 'grpc-client-ts', maxJobsToActivate = 10, requestTimeoutMs = 5000): Promise<any[]> {
    return new Promise((resolve, reject) => {
      const jobs: any[] = [];
      const call = this.client.ActivateJobs({
        type,
        worker,
        timeout: 30000,
        maxJobsToActivate,
        requestTimeout: requestTimeoutMs,
      });
      call.on('data', (res: any) => jobs.push(...(res.jobs ?? [])));
      call.on('end', () => resolve(jobs));
      call.on('error', (err: grpc.ServiceError) => reject(err));
    });
  }

  close(): void {
    this.client.close();
  }
}
