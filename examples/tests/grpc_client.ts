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
const GRPC_ADDR = '127.0.0.1:26500';

// The gateway refuses calls without valid credentials (see
// internal/zeebegrpc/auth.go). The suites authenticate as the seeded
// superuser with HTTP Basic, as zeebe-node's basicAuth option does.
export const GRPC_BASIC_AUTH = { username: 'admin@goflow.com', password: 'admin123' };

/**
 * One unary call on a fresh client that sends exactly the given
 * authorization header (none when undefined). Resolves to the response, or
 * rejects with the grpc ServiceError.
 */
export function rawGatewayCall(method: string, req: unknown, authorization?: string): Promise<any> {
  const packageDef = protoLoader.loadSync(PROTO_PATH, { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true });
  const proto = grpc.loadPackageDefinition(packageDef) as any;
  const client = new proto.gateway_protocol.Gateway(GRPC_ADDR, grpc.credentials.createInsecure(),
    authorization === undefined ? {} : { interceptors: [authInterceptor(authorization)] });
  return new Promise((resolve, reject) =>
    client[method](req, (err: grpc.ServiceError | null, res: any) => {
      client.close();
      err ? reject(err) : resolve(res);
    }));
}

/** A grpc-js client interceptor adding the given authorization header to every call. */
export function authInterceptor(authorization = 'Basic ' + Buffer.from(`${GRPC_BASIC_AUTH.username}:${GRPC_BASIC_AUTH.password}`).toString('base64')) {
  return (options: any, nextCall: any) => new grpc.InterceptingCall(nextCall(options), {
    start(metadata: grpc.Metadata, listener: any, next: any) {
      metadata.set('authorization', authorization);
      next(metadata, listener);
    },
  });
}

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
    this.client = new proto.gateway_protocol.Gateway(GRPC_ADDR, grpc.credentials.createInsecure(), {
      interceptors: [authInterceptor()],
    });
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
