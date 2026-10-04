/**
 * Suite 33 — Gap-closing pass 2: Terminate/Cancel and Modify (see
 * architecture/event-sourced-kafka-design.md).
 *
 * Auditing the codebase for "is there another gap or bug?" (rather than
 * just re-reading the deferred-features list) found two real,
 * previously-unflagged bypasses of partition ownership — the same class of
 * bug fixed for start/complete/fail/set-retries/resolve-incident/timers in
 * every prior phase, but on two operations that apply to *any* process
 * instance regardless of BPMN shape (unlike message/signal/gateway
 * constructs, which can never reach a partitioned instance because
 * ValidateShape already rejects those definitions at flip-time):
 *
 *  - service.TerminateProcessInstance (shared by REST DELETE
 *    /process-instance/:id and gRPC CancelProcessInstance) wrote directly
 *    to executions/jobs/timer_jobs with no write-path check.
 *  - ModifyProcessInstance (REST v2 and gRPC) moved an execution's graph
 *    position directly, same bypass.
 *
 * Terminate is now routed through the Command Gateway for partitioned
 * instances (reusing service.TerminateProcessInstance verbatim inside the
 * Partition Processor). Modify is explicitly rejected for partitioned
 * instances instead of being implemented through Kafka — arbitrary token
 * moves don't fit any of ValidateShape's narrow accepted shapes, and the
 * gRPC handler already scopes itself down for unrelated reasons
 * (single-token-only), so an explicit rejection is consistent with
 * precedent rather than a new kind of limitation.
 */
import * as path from 'path';
import { ZBClient } from 'zeebe-node';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const SINGLE_TASK_BPMN = path.join(__dirname, 'bpmn', '27_single_service_task.bpmn');
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

function newZbClient(): ZBClient {
  return new ZBClient('localhost:26500', { useTLS: false, retry: false, eagerConnection: false });
}

async function findRunningInstanceUUID(processKey: string): Promise<string> {
  const res = await client.api.get('/engine-rest/process-instance', { params: { processKey, status: 'running' } });
  const running = (res.data as any[]) ?? [];
  assert(running.length >= 1, `expected at least one running ${processKey} instance to resolve via REST`);
  running.sort((a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime());
  return running[0].id;
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(SINGLE_TASK_BPMN);

  const defId = await findDefinitionId('SingleTaskProcess');
  await setWritePath(defId, 'partitioned');

  await runSuite({
    name: '33 — Gap-closing pass 2: Terminate/Cancel and Modify',
    tests: [
      {
        name: '[A] REST DELETE /process-instance/:id on a partitioned instance terminates it via the Command Gateway',
        async fn() {
          const inst = await client.startProcess('SingleTaskProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          const res = await client.api.delete(`/engine-rest/process-instance/${instanceId}?deleteReason=suite33`);
          assert(res.status === 204, `expected 204 terminating a partitioned instance, got ${res.status}`);

          let terminated = false;
          const deadline = Date.now() + 15000;
          while (!terminated && Date.now() < deadline) {
            const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
            terminated = hist.data?.state === 'terminated';
            if (!terminated) await sleep(500);
          }
          assert(terminated, 'expected history to report terminated after gated REST termination');

          // The instance's job must have been cancelled too, not left
          // dangling — fetchAndLock should never surface it again.
          const tasks = await client.fetchAndLock([{ name: 'process-item' }]);
          assert(!tasks.some((t) => t.processInstanceId === instanceId), 'expected the terminated instance\'s job to be cancelled, not still fetchable');
        },
      },

      {
        name: '[B] gRPC CancelProcessInstance on a partitioned instance terminates it via the Command Gateway',
        async fn() {
          const zbc = newZbClient();
          try {
            const created: any = await zbc.createProcessInstance({ bpmnProcessId: 'SingleTaskProcess', variables: {} });
            const instanceUUID = await findRunningInstanceUUID('SingleTaskProcess');

            await zbc.cancelProcessInstance(created.processInstanceKey);

            let terminated = false;
            const deadline = Date.now() + 15000;
            while (!terminated && Date.now() < deadline) {
              const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceUUID}`);
              terminated = hist.data?.state === 'terminated';
              if (!terminated) await sleep(500);
            }
            assert(terminated, 'expected history to report terminated after gated gRPC CancelProcessInstance');
          } finally {
            await zbc.close();
          }
        },
      },

      {
        name: '[C] REST v2 modification on a partitioned instance is rejected, instance keeps running',
        async fn() {
          const inst = await client.startProcess('SingleTaskProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          let rejected = false;
          try {
            await client.api.post(`/v2/process-instances/${instanceId}/modification`, {
              moveInstructions: [{ sourceElementId: 'process-item-task', targetElementId: 'EndEvent' }],
            });
          } catch (e: any) {
            rejected = e.response?.status === 422;
          }
          assert(rejected, 'expected 422 rejecting modification of a partitioned instance');

          // Instance must be untouched — still active, job still fetchable.
          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'active', `expected the instance to remain active after a rejected modification, got ${JSON.stringify(hist.data)}`);

          // Clean up.
          await client.pollExternalTask('process-item', async () => ({ done: true }), 15000, instanceId);
          await client.waitForProcessEnd(instanceId, 15000);
        },
      },

      {
        name: '[D] gRPC ModifyProcessInstance on a partitioned instance is rejected with FailedPrecondition, instance keeps running',
        async fn() {
          const zbc = newZbClient();
          try {
            const created: any = await zbc.createProcessInstance({ bpmnProcessId: 'SingleTaskProcess', variables: {} });
            const instanceUUID = await findRunningInstanceUUID('SingleTaskProcess');

            let rejected = false;
            try {
              await zbc.modifyProcessInstance({
                processInstanceKey: created.processInstanceKey,
                activateInstructions: [{ elementId: 'EndEvent', ancestorElementInstanceKey: '-1', variableInstructions: [] }],
              } as any);
            } catch (e: any) {
              rejected = /FAILED_PRECONDITION|9/i.test(String(e.code ?? e.message ?? e));
            }
            assert(rejected, 'expected gRPC ModifyProcessInstance to be rejected with FailedPrecondition for a partitioned instance');

            // Clean up.
            await client.pollExternalTask('process-item', async () => ({ done: true }), 15000, instanceUUID);
            await client.waitForProcessEnd(instanceUUID, 15000);
          } finally {
            await zbc.close();
          }
        },
      },

      {
        name: '[E] Legacy (non-partitioned) terminate and modify still work exactly as before',
        async fn() {
          const path27 = path.join(__dirname, 'bpmn', '03_meal_preparation.bpmn');
          await client.deployBpmn(path27);
          const inst = await client.startProcess('MealPrepProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          const res = await client.api.delete(`/engine-rest/process-instance/${instanceId}?deleteReason=suite33-legacy`);
          assert(res.status === 204, `expected 204 terminating a legacy instance, got ${res.status}`);

          let terminated = false;
          const deadline = Date.now() + 15000;
          while (!terminated && Date.now() < deadline) {
            const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
            terminated = hist.data?.state === 'terminated';
            if (!terminated) await sleep(500);
          }
          assert(terminated, 'expected legacy termination to still work unmodified');
        },
      },
    ],
  });
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
