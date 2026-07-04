/**
 * Suite 27 — Phase 1 partitioned write path (see
 * architecture/event-sourced-kafka-design.md)
 *
 * Unlike suite 26 (Phase 0 shadow mode, where the legacy engine remains the
 * only write path and events are purely observational), this suite
 * exercises the real Kafka-command-log write path for a process definition
 * explicitly flagged write_path = 'partitioned'. The legacy engine handles
 * every other process key completely unchanged — suites 1-26 passing
 * unmodified is the load-bearing proof of that.
 *
 * Flow: deploy the one BPMN shape the partitioned engine supports so far
 * (start -> one external service task -> end), confirm a mismatched shape
 * is rejected at flip time, then start/fetch/complete an instance through
 * the exact same public REST endpoints legacy processes use — the
 * Command Gateway + Partition Processor should be invisible from the
 * outside, down to the response shapes.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const SINGLE_TASK_BPMN = path.join(__dirname, 'bpmn', '27_single_service_task.bpmn');
const MULTI_NODE_BPMN = path.join(__dirname, 'bpmn', '03_meal_preparation.bpmn');
const client = new GoFlowClient();

async function findDefinitionId(processKey: string): Promise<string> {
  const res = await client.api.get(`/engine-rest/process-definition?key=${processKey}`);
  const defs = (res.data as any[]) ?? [];
  assert(defs.length > 0, `expected at least one deployed definition for key ${processKey}`);
  // Latest version — deploys within this run always increment version.
  return defs.sort((a, b) => b.version - a.version)[0].id;
}

async function setWritePath(definitionId: string, writePath: 'legacy' | 'partitioned') {
  return client.api.patch(`/engine-rest/process-definition/${definitionId}/write-path`, { writePath });
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(SINGLE_TASK_BPMN);
  await client.deployBpmn(MULTI_NODE_BPMN);

  await runSuite({
    name: '27 — Phase 1 partitioned write path',
    tests: [
      {
        name: '[A] Flipping a definition with gateways/multiple tasks to partitioned is rejected',
        async fn() {
          const mealPrepId = await findDefinitionId('MealPrepProcess');
          let rejected = false;
          try {
            await setWritePath(mealPrepId, 'partitioned');
          } catch (e: any) {
            rejected = e.response?.status === 400;
          }
          assert(rejected, 'expected a 400 rejecting a definition with parallel gateways from the partitioned write path');
        },
      },

      {
        name: '[B] Flipping the single-service-task definition to partitioned succeeds',
        async fn() {
          const defId = await findDefinitionId('SingleTaskProcess');
          const res = await setWritePath(defId, 'partitioned');
          assert(res.status === 200, `expected 200 flipping SingleTaskProcess to partitioned, got ${res.status}`);
          assert(res.data.writePath === 'partitioned', `expected writePath=partitioned in response, got ${JSON.stringify(res.data)}`);
        },
      },

      {
        name: '[C] Starting a partitioned instance returns the same response shape as the legacy path',
        async fn() {
          const inst = await client.startProcess('SingleTaskProcess', { orderId: 'p1-order' });
          assert(!!inst.processInstanceId, `expected a processInstanceId in the start response, got ${JSON.stringify(inst)}`);
          assert(inst.processKey === 'SingleTaskProcess', `expected processKey=SingleTaskProcess, got ${inst.processKey}`);

          // Clean up so it doesn't linger across later tests.
          await client.pollExternalTask('process-item', async () => ({ done: true }), 15000, inst.processInstanceId);
          await client.waitForProcessEnd(inst.processInstanceId, 15000);
        },
      },

      {
        name: '[D] Full lifecycle: start -> fetchAndLock -> complete -> instance shows completed',
        async fn() {
          const inst = await client.startProcess('SingleTaskProcess', { orderId: 'p1-order-2' });
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          let task: any = null;
          const deadline = Date.now() + 15000;
          while (!task && Date.now() < deadline) {
            const tasks = await client.fetchAndLock([{ name: 'process-item' }]);
            task = tasks.find((t) => t.processInstanceId === instanceId) ?? null;
            if (!task) await sleep(500);
          }
          assert(!!task, 'expected to fetch-and-lock the process-item job through the unmodified legacy fetch endpoint');

          await client.completeExternalTask(task.id, { processed: true });

          // Eventual consistency by design (see the design doc's Phase 1
          // consistency-model decision) — poll rather than assert
          // immediately. Checked via the history endpoint (not the live
          // /process-instance/:id endpoint), matching suite 26's own
          // convention, since a completed instance may already have been
          // archived out of the live table by the time this runs.
          await client.waitForProcessEnd(instanceId, 15000);

          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'completed', `expected history to report completed, got ${JSON.stringify(hist.data)}`);
        },
      },

      {
        name: '[E] Re-completing an already-completed partitioned job is a safe no-op, not a crash',
        async fn() {
          const inst = await client.startProcess('SingleTaskProcess', { orderId: 'p1-order-3' });
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          let task: any = null;
          const deadline = Date.now() + 15000;
          while (!task && Date.now() < deadline) {
            const tasks = await client.fetchAndLock([{ name: 'process-item' }]);
            task = tasks.find((t) => t.processInstanceId === instanceId) ?? null;
            if (!task) await sleep(500);
          }
          assert(!!task, 'expected to fetch-and-lock the process-item job');

          await client.completeExternalTask(task.id, { processed: true });
          await client.waitForProcessEnd(instanceId, 15000);

          // Re-issuing completion for the same job must not silently
          // double-apply — internal/partition.Processor.handleCompleteJob
          // treats an already-completed job as a safe no-op and never
          // re-publishes ProcessCompleted, so the Command Gateway's
          // second wait is expected to fail (time out), not succeed.
          let secondCallFailed = false;
          try {
            await client.completeExternalTask(task.id, { processed: true });
          } catch {
            secondCallFailed = true;
          }
          assert(secondCallFailed, 'expected re-completing an already-completed partitioned job to fail rather than silently succeed twice');

          // Prove the processor is still healthy afterward: a brand-new
          // instance must still start and complete normally.
          const inst2 = await client.startProcess('SingleTaskProcess', { orderId: 'p1-order-4' });
          const instanceId2 = inst2.processInstanceId ?? (inst2 as any).id;
          await client.pollExternalTask('process-item', async () => ({ processed: true }), 15000, instanceId2);
          await client.waitForProcessEnd(instanceId2, 15000);
        },
      },

      {
        name: '[F] Legacy processes are completely unaffected by the partitioned write path',
        async fn() {
          // GrpcProcess (suite 24/26's fixture) is deployed with the
          // default write_path='legacy' — confirm it still runs the
          // ordinary synchronous transactional path end to end.
          const path24 = path.join(__dirname, 'bpmn', '24_grpc_process.bpmn');
          await client.deployBpmn(path24);
          const inst = await client.startProcess('GrpcProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;
          await client.pollExternalTask('grpc_review', async () => ({ reviewed: true }), 15000, instanceId);
          await client.waitForProcessEnd(instanceId, 15000);
          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'completed', `expected legacy GrpcProcess instance to complete normally, got ${JSON.stringify(hist.data)}`);
        },
      },
    ],
  });
}

run().catch(err => {
  // See suite 26's identical footer for why this must be process.exit(1),
  // not the bare form other pre-existing suites still use — a setup-phase
  // failure (e.g. a deploy error) must not silently count as a pass.
  console.error(err);
  process.exit(1);
});
