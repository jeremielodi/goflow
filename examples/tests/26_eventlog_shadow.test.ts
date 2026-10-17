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
 *
 * Test [J] (added in a later pass, after auditing the schema): user-task
 * lifecycle events (TaskCreated/TaskClaimed/TaskCompleted) — added because
 * job lifecycle was fully instrumented here but task lifecycle had zero
 * coverage at all, neither the proto schema nor any call site.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '24_grpc_process.bpmn');
const PARALLEL_BPMN = path.join(__dirname, 'bpmn', '03_meal_preparation.bpmn');
const SHORT_TIMER_BPMN = path.join(__dirname, 'bpmn', '26_short_timer.bpmn');
const ORDER_BPMN = path.join(__dirname, 'bpmn', '10_order_notification.bpmn');
const SIGNAL_BPMN = path.join(__dirname, 'bpmn', '11_inventory_broadcast.bpmn');
const USER_TASK_BPMN = path.join(__dirname, 'bpmn', '18_tasklist_process.bpmn');
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
  await client.deployBpmn(PARALLEL_BPMN);
  await client.deployBpmn(SHORT_TIMER_BPMN);
  await client.deployBpmn(ORDER_BPMN);
  await client.deployBpmn(SIGNAL_BPMN);
  await client.deployBpmn(USER_TASK_BPMN);

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

      {
        name: '[E] Parallel gateway fork/join publishes GatewayForked then GatewayJoined',
        async fn() {
          const inst = await client.startProcess('MealPrepProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          // By the time the salad user task appears, the fork has already
          // happened — both branches (cook_pasta job + prepare_salad task)
          // are concurrently active.
          const saladTask = await client.waitForTask('prepare_salad', instanceId, 10000);

          const cookDone = client.pollExternalTask('cook_pasta', async () => ({ pastaReady: true }), 20000);
          await client.completeTask(saladTask.id, { saladReady: true });
          await cookDone;

          const events = await waitForEventTypes(instanceId, ['GatewayForked', 'GatewayJoined']);
          const order = events.map((e) => e.type);
          assert(order.includes('GatewayForked'), `expected a GatewayForked event, got ${JSON.stringify(order)}`);
          assert(order.includes('GatewayJoined'), `expected a GatewayJoined event, got ${JSON.stringify(order)}`);
          assert(order.indexOf('GatewayForked') < order.lastIndexOf('GatewayJoined'), 'expected GatewayForked to precede GatewayJoined');

          // Clean up so the process doesn't linger.
          const eatTask = await client.waitForTask('eat_meal', instanceId, 15000);
          await client.completeTask(eatTask.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      {
        name: '[F] A short intermediate timer publishes TimerTriggered',
        async fn() {
          const inst = await client.startProcess('ShortTimerProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          const events = await waitForEventTypes(instanceId, ['TimerTriggered'], 15000);
          assert(events.some((e) => e.type === 'TimerTriggered'), `expected a TimerTriggered event for ${instanceId}`);

          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      {
        name: '[G] Message correlation publishes MessageCorrelated',
        async fn() {
          const inst = await client.startProcess('orderNotification', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          // Give the instance a moment to park at the message catch event.
          await sleep(500);
          await client.api.post('/engine-rest/message', {
            messageName: 'PaymentReceived',
            processInstanceId: instanceId,
          });

          const events = await waitForEventTypes(instanceId, ['MessageCorrelated'], 15000);
          assert(events.some((e) => e.type === 'MessageCorrelated'), `expected a MessageCorrelated event for ${instanceId}`);
        },
      },

      {
        name: '[H] Signal broadcast publishes SignalBroadcast (no processInstanceId)',
        async fn() {
          const inst = await client.startProcess('inventoryAlert', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          // inventoryAlert runs a check-inventory external task BEFORE
          // parking at the signal catch event — must complete it first, or
          // Broadcast finds zero matching subscriptions and returns early
          // (internal/api/signal_controller.go's Broadcast short-circuits
          // before reaching LogSignalBroadcast/the eventlog publish call
          // when len(subs) == 0), so no SignalBroadcast event is published
          // at all.
          await client.pollExternalTask('check-inventory', async () => ({ checked: true }), 15000, instanceId);
          await sleep(500);

          // Compare event IDs, not counts: /eventlog/recent is a 500-entry
          // ring buffer, so once it's full a new event pushes an old one out
          // and the count of SignalBroadcast entries can stay flat.
          const seen = new Set((await fetchRecent()).filter((e) => e.type === 'SignalBroadcast').map((e) => e.eventId));
          await client.api.post('/engine-rest/signal', { name: 'RestockAlert' });

          const deadline = Date.now() + 15000;
          let found = false;
          while (Date.now() < deadline) {
            const fresh = (await fetchRecent()).filter((e) => e.type === 'SignalBroadcast' && !seen.has(e.eventId));
            if (fresh.length > 0) {
              found = true;
              // SignalBroadcast has no owning instance — confirmed empty here.
              assert(fresh[fresh.length - 1].processInstanceId === '', 'expected SignalBroadcast to have an empty processInstanceId');
              break;
            }
            await sleep(500);
          }
          assert(found, 'expected a new SignalBroadcast event after broadcasting RestockAlert');

          // Clean up so the instance doesn't linger.
          await client.pollExternalTask('update-inventory', async () => ({ updated: true }), 15000, instanceId);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      {
        name: '[I] Projector reflects real instance/job state after a full lifecycle',
        async fn() {
          const inst = await client.startProcess('GrpcProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          await client.pollExternalTask('grpc_review', async () => ({ reviewed: true }), 15000, instanceId);
          await client.waitForProcessEnd(instanceId);

          const deadline = Date.now() + 15000;
          let projectedInstance: any = null;
          let projectedJob: any = null;
          while (Date.now() < deadline) {
            const res = await client.api.get('/engine-rest/eventlog/projected');
            projectedInstance = (res.data?.instances ?? []).find((i: any) => i.id === instanceId);
            projectedJob = (res.data?.jobs ?? []).find((j: any) => j.processInstanceId === instanceId);
            if (projectedInstance?.status === 'completed' && projectedJob?.status === 'completed') break;
            await sleep(500);
          }
          assert(!!projectedInstance, `expected a projected instance row for ${instanceId}`);
          assert(projectedInstance.status === 'completed', `expected projected instance status=completed, got ${projectedInstance?.status}`);
          assert(!!projectedJob, `expected a projected job row for instance ${instanceId}`);
          assert(projectedJob.status === 'completed', `expected projected job status=completed, got ${projectedJob?.status}`);

          // Cross-check against the real REST view of the same instance.
          const real = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(real.data?.state === 'completed', `expected real historic instance to also report completed, got ${JSON.stringify(real.data)}`);
        },
      },

      {
        name: '[J] User task lifecycle publishes TaskCreated, TaskClaimed, TaskCompleted in order',
        async fn() {
          const inst = await client.startProcess('TasklistProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          const tasks = await client.getTasks(instanceId);
          assert(tasks.length === 1, `expected exactly one task for instance ${instanceId}, got ${tasks.length}`);
          const taskId = tasks[0].id;

          await client.claimTask(taskId, 'eventlog-suite-reviewer');
          await client.completeTask(taskId, {});
          await client.waitForProcessEnd(instanceId, 10000);

          const events = await waitForEventTypes(instanceId, ['TaskCreated', 'TaskClaimed', 'TaskCompleted']);
          assert(events.length >= 3, `expected at least 3 task events for instance ${instanceId}, got ${JSON.stringify(events.map((e) => e.type))}`);

          const order = events.map((e) => e.type);
          const idxCreated = order.indexOf('TaskCreated');
          const idxClaimed = order.indexOf('TaskClaimed');
          const idxCompleted = order.indexOf('TaskCompleted');
          assert(idxCreated < idxClaimed, 'expected TaskCreated to precede TaskClaimed');
          assert(idxClaimed < idxCompleted, 'expected TaskClaimed to precede TaskCompleted');

          const created = events.find((e) => e.type === 'TaskCreated')!.detail as any;
          assert(created.task_id === taskId, `expected TaskCreated.task_id to match ${taskId}, got ${created.task_id}`);
          assert(created.task_definition_key === 'reviewTask', `expected task_definition_key=reviewTask, got ${created.task_definition_key}`);

          const claimed = events.find((e) => e.type === 'TaskClaimed')!.detail as any;
          assert(claimed.assignee === 'eventlog-suite-reviewer', `expected TaskClaimed.assignee to match, got ${claimed.assignee}`);
        },
      },
    ],
  });
}

run().catch(err => {
  // A failure before runSuite() is invoked (e.g. a deploy error during
  // setup) would otherwise print but exit 0 — runSuite's own tests set
  // process.exitCode on failure, but only cover failures inside the test
  // loop itself, not setup. Explicit here so a broken fixture can't
  // silently pass.
  console.error(err);
  process.exit(1);
});
