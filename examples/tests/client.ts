import axios, { AxiosInstance } from 'axios';
import FormData from 'form-data';
import * as fs from 'fs';
import * as path from 'path';

export const BASE_URL = 'http://localhost:8080';

// ─── Response types ──────────────────────────────────────────────────────────

export interface LoginResponse {
  access_token: string;
  refresh_token: string;
  user: { id: string; email: string; name: string };
}

export interface Deployment {
  id: string;
  name: string;
}

export interface ProcessInstance {
  id: string;
  processInstanceId: string;
  processKey: string;
  status: string;
}

export interface Task {
  id: string;
  taskName: string;
  taskDefinitionKey: string;
  processInstanceId: string;
  assignee: string | null;
  candidateGroup: string | null;
  status: string;
  createdAt: string;
}

export interface ExternalTask {
  id: string;
  topicName: string;
  processInstanceId: string;
  variables: Record<string, { value: unknown; type: string }>;
}

export interface TaskListenerEvent {
  id: string;
  eventType: string;
  taskId: string;
  processInstanceId: string;
  processKey: string;
  executionId: string;
  taskName: string;
  assignee?: string | null;
  candidateGroup?: string | null;
  oldStatus?: string | null;
  newStatus: string;
  timestamp: string;
  variables?: Record<string, unknown>;
}

// ─── Utilities ───────────────────────────────────────────────────────────────

export const sleep = (ms: number): Promise<void> =>
  new Promise(r => setTimeout(r, ms));

export function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(`Assertion failed: ${message}`);
}

// ─── SSE task-listener client ─────────────────────────────────────────────────

/**
 * Consumes GET /events/tasks (text/event-stream) and buffers "task" events so
 * tests can assert that the backend's TaskEventDispatcher notified listeners
 * (e.g. an external client tracking process progress) at each lifecycle step.
 */
export class TaskSSEClient {
  private buffer = '';
  private closed = false;
  public readonly events: TaskListenerEvent[] = [];

  private constructor(private readonly stream: NodeJS.ReadableStream) {
    stream.on('data', (chunk: Buffer) => this.handleChunk(chunk));
    stream.on('error', () => {});
  }

  static async connect(
    api: AxiosInstance,
    opts: { processKeys?: string[]; taskFilter?: string } = {}
  ): Promise<TaskSSEClient> {
    const params: Record<string, string> = {};
    if (opts.processKeys?.length) params.processKeys = opts.processKeys.join(',');
    if (opts.taskFilter) params.taskFilter = opts.taskFilter;

    const res = await api.get('/events/tasks', {
      params,
      responseType: 'stream',
      timeout: 0,
    });
    return new TaskSSEClient(res.data);
  }

  private handleChunk(chunk: Buffer): void {
    this.buffer += chunk.toString('utf8');
    const frames = this.buffer.split('\n\n');
    this.buffer = frames.pop() ?? '';

    for (const frame of frames) {
      let eventName = 'message';
      let data = '';
      for (const line of frame.split('\n')) {
        if (line.startsWith('event: ')) eventName = line.slice(7);
        else if (line.startsWith('data: ')) data = line.slice(6);
      }
      if (eventName !== 'task' || !data) continue;
      try {
        this.events.push(JSON.parse(data) as TaskListenerEvent);
      } catch {
        // ignore malformed frame
      }
    }
  }

  /** Waits until an already-received or future event matches the predicate. */
  async waitFor(
    predicate: (e: TaskListenerEvent) => boolean,
    timeoutMs = 15000
  ): Promise<TaskListenerEvent> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const found = this.events.find(predicate);
      if (found) return found;
      await sleep(200);
    }
    throw new Error(`Timeout waiting for SSE task event (received ${this.events.length} events)`);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    (this.stream as any).destroy?.();
  }
}

// ─── Client ──────────────────────────────────────────────────────────────────

export class GoFlowClient {
  public readonly api: AxiosInstance;
  public workerId: string;
  private token: string | null = null;

  constructor() {
    this.api = axios.create({ baseURL: BASE_URL });
    this.workerId = `worker-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;

    this.api.interceptors.request.use(config => {
      if (this.token) {
        config.headers = config.headers ?? {};
        config.headers['Authorization'] = `Bearer ${this.token}`;
      }
      return config;
    });
  }

  // Auth ──────────────────────────────────────────────────────────────────────

  async login(email: string, password: string): Promise<LoginResponse> {
    const res = await this.api.post<LoginResponse>('/auth/login', { email, password });
    this.token = res.data.access_token;
    return res.data;
  }

  async loginAsSuperUser(): Promise<LoginResponse> {
    return this.login('admin@goflow.com', 'admin123');
  }

  async refreshToken(refreshToken: string): Promise<LoginResponse> {
    const res = await this.api.post<LoginResponse>('/auth/refresh', { refresh_token: refreshToken });
    this.token = res.data.access_token;
    return res.data;
  }

  async logout(): Promise<void> {
    await this.api.post('/auth/logout');
    this.token = null;
  }

  // Deployment ────────────────────────────────────────────────────────────────

  async deployBpmn(filePath: string, name?: string): Promise<Deployment> {
    const form = new FormData();
    form.append('resources', fs.createReadStream(filePath));
    form.append('deployment-name', name ?? path.basename(filePath, '.bpmn'));
    form.append('deployment-source', 'goflow-tests');

    const res = await this.api.post<Deployment>('/engine-rest/deployment/create', form, {
      headers: form.getHeaders(),
    });
    return res.data;
  }

  // Process instances ─────────────────────────────────────────────────────────

  async startProcess(key: string, variables: Record<string, unknown> = {}): Promise<ProcessInstance> {
    const res = await this.api.post<ProcessInstance>(
      `/engine-rest/v2/process-definitions/${key}/start`,
      { variables }
    );
    return res.data;
  }

  async getProcessInstance(id: string): Promise<ProcessInstance | null> {
    try {
      const res = await this.api.get<ProcessInstance>(`/engine-rest/process-instance/${id}`);
      return res.data;
    } catch (e: any) {
      if (e.response?.status === 404) return null;
      throw e;
    }
  }

  async getProcessVariables(id: string): Promise<Record<string, unknown>> {
    const res = await this.api.get(`/engine-rest/process-instance/${id}/variables`);
    return res.data ?? {};
  }

  // User tasks ────────────────────────────────────────────────────────────────

  async getTasks(processInstanceId?: string, assignee?: string): Promise<Task[]> {
    const params: Record<string, string> = {};
    if (processInstanceId) params.processInstanceId = processInstanceId;
    if (assignee) params.assignee = assignee;
    const res = await this.api.get<Task[] | null>('/tasks', { params });
    return res.data ?? [];
  }

  async claimTask(taskId: string, assignee: string): Promise<void> {
    await this.api.post(`/engine-rest/tasks/${taskId}/claim`, { assignee });
  }

  async unclaimTask(taskId: string): Promise<void> {
    await this.api.post(`/engine-rest/tasks/${taskId}/unclaim`, {});
  }

  async completeTask(taskId: string, variables: Record<string, unknown> = {}): Promise<void> {
    await this.api.post(`/tasks/${taskId}/complete`, { variables });
  }

  // External tasks ────────────────────────────────────────────────────────────

  async fetchAndLock(topics: Array<{ name: string; lockDuration?: number }>): Promise<ExternalTask[]> {
    const res = await this.api.post<ExternalTask[] | null>('/engine-rest/external-task/fetchAndLock', {
      workerId: this.workerId,
      maxTasks: 10,
      usePriority: true,
      topics: topics.map(t => ({
        topicName: t.name,
        lockDuration: t.lockDuration ?? 30000,
        variables: [],
      })),
    });
    return res.data ?? [];
  }

  async completeExternalTask(taskId: string, variables: Record<string, unknown> = {}): Promise<void> {
    const formatted: Record<string, { value: unknown; type: string }> = {};
    for (const [k, v] of Object.entries(variables)) {
      formatted[k] = {
        value: v,
        type:
          typeof v === 'boolean' ? 'boolean' :
          typeof v === 'number'  ? 'double'  : 'string',
      };
    }
    await this.api.post(`/engine-rest/external-task/${taskId}/complete`, {
      workerId: this.workerId,
      variables: formatted,
    });
  }

  async failExternalTask(taskId: string, errorMessage: string, retries = 0): Promise<void> {
    await this.api.post(`/engine-rest/external-task/${taskId}/failure`, {
      workerId: this.workerId,
      errorMessage,
      retries,
      retryTimeout: 5000,
    });
  }

  // Polling helpers ───────────────────────────────────────────────────────────

  async waitForTask(
    taskDefinitionKey: string,
    processInstanceId: string,
    timeoutMs = 30000
  ): Promise<Task> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const tasks = await this.getTasks(processInstanceId);
      const task = tasks.find(t => t.taskDefinitionKey === taskDefinitionKey);
      if (task) return task;
      await sleep(1000);
    }
    throw new Error(`Timeout waiting for task "${taskDefinitionKey}" in process ${processInstanceId}`);
  }

  async waitForProcessEnd(instanceId: string, timeoutMs = 60000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const inst = await this.getProcessInstance(instanceId);
      if (!inst || inst.status === 'completed') return;
      await sleep(1000);
    }
    throw new Error(`Timeout waiting for process ${instanceId} to end`);
  }

  async pollExternalTask(
    topic: string,
    handler: (task: ExternalTask) => Promise<Record<string, unknown>>,
    timeoutMs = 30000,
    processInstanceId?: string
  ): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const tasks = await this.fetchAndLock([{ name: topic }]);
      for (const task of tasks) {
        if (processInstanceId && task.processInstanceId !== processInstanceId) {
          // wrong instance — release by completing with no-op so it's not stuck locked
          // We can't "nack" in Camunda style, so just skip; the lock will expire
          continue;
        }
        const vars = await handler(task);
        await this.completeExternalTask(task.id, vars);
        return;
      }
      await sleep(1000);
    }
    throw new Error(`Timeout polling external task topic "${topic}"${processInstanceId ? ` for instance ${processInstanceId}` : ''}`);
  }
}

// ─── Test runner ─────────────────────────────────────────────────────────────

export interface TestResult {
  name: string;
  passed: boolean;
  error?: string;
  durationMs: number;
}

export interface Suite {
  name: string;
  tests: Array<{ name: string; fn: () => Promise<void> }>;
}

export async function runSuite(suite: Suite): Promise<TestResult[]> {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`  ${suite.name}`);
  console.log('═'.repeat(60));

  const results: TestResult[] = [];

  for (const test of suite.tests) {
    const t0 = Date.now();
    try {
      await test.fn();
      const durationMs = Date.now() - t0;
      console.log(`  ✅  ${test.name}  (${durationMs}ms)`);
      results.push({ name: test.name, passed: true, durationMs });
    } catch (e: any) {
      const durationMs = Date.now() - t0;
      const msg = e.response?.data
        ? `${e.message} — API: ${JSON.stringify(e.response.data)}`
        : e.message;
      console.log(`  ❌  ${test.name}  (${durationMs}ms)`);
      console.log(`       ${msg}`);
      results.push({ name: test.name, passed: false, error: msg, durationMs });
    }
  }

  const passed = results.filter(r => r.passed).length;
  console.log(`\n  ${passed} / ${results.length} passed\n`);

  // Without this, a suite whose assertions fail (but don't throw an
  // uncaught exception past this function) would still exit 0 — every
  // suite file's `run().catch(console.error)` never rethrows a per-test
  // failure, so run_all.ts's exit-code-based "X/Y suites passed" summary
  // would silently miss real assertion failures.
  if (passed < results.length) {
    process.exitCode = 1;
  }

  return results;
}
