/**
 * Suite 26 — Event log shadow mode (Phase 0 of the event-sourced engine
 * scoping, see architecture/event-sourced-kafka-design.md)
 *
 * The existing Postgres-transactional engine remains the only write path —
 * this suite verifies a *side effect* of that path: after each of the 7
 * wired transitions commits, a domain event should also land on the
 * Redpanda-backed event log, in the right order, keyed by process instance.
 * A background consumer (started in main.go when KAFKA_BROKERS is set)
 * keeps the last 500 consumed events in memory, exposed via the dev-only
 * GET /engine-rest/eventlog/recent debug endpoint this suite polls.
 *
 * Flow: start a process (ProcessStarted), complete its job cleanly
 * (JobCreated, JobCompleted, ProcessCompleted) on one instance; on a second
 * instance, permanently fail a job (JobFailed with incidentCreated=true),
 * resolve the resulting incident (IncidentResolved), then terminate a third
 * instance while its job is still pending (ProcessTerminated).
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '24_grpc_process.bpmn');
const client = new GoFlowClient();

interface RecentEvent {
  eventId: string;
  processInstanceId: string;
  occurredAt: string;
  type: string;
  detail: Record<string, unknown>;
}

async function fetchRecent(): Promise<RecentEvent[]> {
  const res = await client.api.get('/engine-rest/eventlog/recent');
  return (res.data?.events as RecentEvent[]) ?? [];
}

async function waitForEventTypes(instanceId: string, expectedTypes: string[], timeoutMs = 20000): Promise<RecentEvent[]> {
  const deadline = Date.now() + timeoutMs;
  let matched: RecentEvent[] = [];
  while (Date.now() < deadline) {
    const events = (await fetchRecent()).filter((e) => e.processInstanceId === instanceId);
    const types = new Set(events.map((e) => e.type));
    if (expectedTypes.every((t) => types.has(t))) {
      matched = events;
      break;
    }
    await sleep(500);
  }
  return matched;
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN);

  await runSuite({
    name: '26 — Event log shadow mode',
    tests: [
      {
        name: '[A] Event log shadow mode is enabled for this stack',
        async fn() {
          const res = await client.api.get('/engine-rest/eventlog/recent');
          assert(res.status === 200, `expected the eventlog debug endpoint to be enabled (KAFKA_BROKERS configured), got status ${res.status}`);
          assert(Array.isArray(res.data?.events), 'expected an events array in the response');
        },
      },

      {
        name: '[B] Clean completion publishes ProcessStarted, JobCreated, JobCompleted, ProcessCompleted in order',
        async fn() {
          const inst = await client.startProcess('GrpcProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          await client.pollExternalTask('grpc_review', async () => ({ reviewed: true }), 15000, instanceId);
          await client.waitForProcessEnd(instanceId);

          const events = await waitForEventTypes(instanceId, [
            'ProcessStarted',
            'JobCreated',
            'JobCompleted',
            'ProcessCompleted',
          ]);
          assert(events.length >= 4, `expected at least 4 events for instance ${instanceId}, got ${JSON.stringify(events.map((e) => e.type))}`);

          const order = events.map((e) => e.type);
          const idxStarted = order.indexOf('ProcessStarted');
          const idxCreated = order.indexOf('JobCreated');
          const idxCompleted = order.indexOf('JobCompleted');
          const idxProcessCompleted = order.indexOf('ProcessCompleted');
          assert(idxStarted < idxCreated, 'expected ProcessStarted to precede JobCreated (partition ordering)');
          assert(idxCreated < idxCompleted, 'expected JobCreated to precede JobCompleted');
          assert(idxCompleted <= idxProcessCompleted, 'expected JobCompleted to precede or coincide with ProcessCompleted');
        },
      },

      {
        name: '[C] Permanent job failure + incident resolution publishes JobFailed(incidentCreated=true) then IncidentResolved',
        async fn() {
          const inst = await client.startProcess('GrpcProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          let task: any = null;
          const deadline = Date.now() + 15000;
          while (!task && Date.now() < deadline) {
            const tasks = await client.fetchAndLock([{ name: 'grpc_review' }]);
            task = tasks.find((t) => t.processInstanceId === instanceId) ?? null;
            if (!task) await sleep(500);
          }
          assert(!!task, 'expected to fetch-and-lock the grpc_review job');
          await client.failExternalTask(task.id, 'eventlog suite induced permanent failure', 0);

          let incidentId: string | null = null;
          const incidentDeadline = Date.now() + 10000;
          while (!incidentId && Date.now() < incidentDeadline) {
            const res = await client.api.get('/engine-rest/incident', { params: { processInstanceId: instanceId } });
            const incidents = (res.data as any[]) ?? [];
            if (incidents.length > 0 && incidents[0].state === 'open') incidentId = incidents[0].id;
            else await sleep(500);
          }
          assert(!!incidentId, 'expected an open incident to appear via REST');

          const failedEvents = await waitForEventTypes(instanceId, ['JobFailed']);
          const jobFailed = failedEvents.find((e) => e.type === 'JobFailed');
          assert(!!jobFailed, 'expected a JobFailed event');
          // Note: the debug endpoint serializes the raw protobuf-generated Go
          // struct, whose json tags mirror the .proto field names verbatim
          // (snake_case), not camelCase — e.g. "incident_created", not
          // "incidentCreated".
          assert(jobFailed!.detail.incident_created === true, `expected JobFailed.incident_created=true, got ${JSON.stringify(jobFailed!.detail)}`);

          await client.api.post(`/engine-rest/job/${task.id}/retries`, { retries: 1 });

          const resolvedEvents = await waitForEventTypes(instanceId, ['IncidentResolved']);
          assert(resolvedEvents.some((e) => e.type === 'IncidentResolved'), 'expected an IncidentResolved event after setting job retries');
        },
      },

      {
        name: '[D] Terminating a running instance publishes ProcessTerminated',
        async fn() {
          const inst = await client.startProcess('GrpcProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          // Leave the job pending (don't complete it) and terminate directly.
          await client.api.delete(`/engine-rest/process-instance/${instanceId}`);

          const events = await waitForEventTypes(instanceId, ['ProcessTerminated']);
          assert(events.some((e) => e.type === 'ProcessTerminated'), `expected a ProcessTerminated event for ${instanceId}`);
        },
      },
    ],
  });
}

run().catch(console.error);
