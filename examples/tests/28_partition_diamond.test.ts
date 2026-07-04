/**
 * Suite 28 — Phase 2 partitioned write path: exclusive gateway ("diamond" shape)
 * (see architecture/event-sourced-kafka-design.md)
 *
 * Phase 1 (suite 27) supported exactly one shape: start -> one service task
 * -> end. This suite exercises the second shape the partitioned engine now
 * supports: start -> exclusiveGateway (2 conditioned branches) -> {taskA |
 * taskB} -> one shared end event. `internal/partition.ValidateShape` and
 * the Partition Processor's redelivery-safety logic both changed to support
 * this — suite 27's fixture (SingleTaskProcess) must stay fully green
 * afterward (test [G]), proving Phase 1's shape wasn't disturbed.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const DIAMOND_BPMN = path.join(__dirname, 'bpmn', '28_diamond_gateway.bpmn');
const MISSING_CONDITION_BPMN = path.join(__dirname, 'bpmn', '28_diamond_missing_condition.bpmn');
const DIVERGING_ENDS_BPMN = path.join(__dirname, 'bpmn', '28_diamond_diverging_ends.bpmn');
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

// A short, non-throwing poll — unlike client.pollExternalTask, which throws
// on timeout. Used by test [F] to prove the WRONG branch's topic never
// gets a job for a given instance, where "nothing appeared" is the
// expected (passing) outcome, not a failure.
async function briefPollFindsNoJobFor(topic: string, instanceId: string, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const tasks = await client.fetchAndLock([{ name: topic }]);
    if (tasks.some((t) => t.processInstanceId === instanceId)) return false;
    await sleep(300);
  }
  return true;
}

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(DIAMOND_BPMN);
  await client.deployBpmn(MISSING_CONDITION_BPMN);
  await client.deployBpmn(DIVERGING_ENDS_BPMN);
  await client.deployBpmn(SINGLE_TASK_BPMN);

  await runSuite({
    name: '28 — Phase 2 diamond shape (exclusive gateway)',
    tests: [
      {
        name: '[A] Flipping the valid diamond definition to partitioned succeeds',
        async fn() {
          const defId = await findDefinitionId('DiamondProcess');
          const res = await setWritePath(defId, 'partitioned');
          assert(res.status === 200, `expected 200 flipping DiamondProcess to partitioned, got ${res.status}`);
          assert(res.data.writePath === 'partitioned', `expected writePath=partitioned in response, got ${JSON.stringify(res.data)}`);
        },
      },

      {
        name: '[B] Flipping a diamond with a missing gateway condition is rejected',
        async fn() {
          const defId = await findDefinitionId('DiamondMissingCondition');
          let rejected = false;
          try {
            await setWritePath(defId, 'partitioned');
          } catch (e: any) {
            rejected = e.response?.status === 400;
          }
          assert(rejected, 'expected a 400 rejecting a gateway flow with no explicit condition');
        },
      },

      {
        name: '[C] Flipping a diamond whose branches diverge to two end events is rejected',
        async fn() {
          const defId = await findDefinitionId('DiamondDivergingEnds');
          let rejected = false;
          try {
            await setWritePath(defId, 'partitioned');
          } catch (e: any) {
            rejected = e.response?.status === 400;
          }
          assert(rejected, 'expected a 400 rejecting branches that do not converge on a shared end event');
        },
      },

      {
        name: '[D] branch=a resolves through taskA, completes via process-branch-a, history shows completed',
        async fn() {
          const inst = await client.startProcess('DiamondProcess', { branch: 'a' });
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          await client.pollExternalTask('process-branch-a', async () => ({ done: true }), 15000, instanceId);
          await client.waitForProcessEnd(instanceId, 15000);

          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'completed', `expected history to report completed, got ${JSON.stringify(hist.data)}`);
        },
      },

      {
        name: '[E] branch=b resolves through taskB, completes via process-branch-b, history shows completed',
        async fn() {
          const inst = await client.startProcess('DiamondProcess', { branch: 'b' });
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          await client.pollExternalTask('process-branch-b', async () => ({ done: true }), 15000, instanceId);
          await client.waitForProcessEnd(instanceId, 15000);

          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'completed', `expected history to report completed, got ${JSON.stringify(hist.data)}`);
        },
      },

      {
        name: '[F] Branch selection is exclusive — a branch=a instance never produces a job on process-branch-b',
        async fn() {
          const inst = await client.startProcess('DiamondProcess', { branch: 'a' });
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          const clean = await briefPollFindsNoJobFor('process-branch-b', instanceId, 3000);
          assert(clean, `expected no process-branch-b job for a branch=a instance ${instanceId} (ResolveNext/ValidateShape regression)`);

          // Clean up so it doesn't linger.
          await client.pollExternalTask('process-branch-a', async () => ({ done: true }), 15000, instanceId);
          await client.waitForProcessEnd(instanceId, 15000);
        },
      },

      {
        name: "[G] Phase 1's linear shape (SingleTaskProcess) still works after Phase 2's validate/processor changes",
        async fn() {
          const defId = await findDefinitionId('SingleTaskProcess');
          await setWritePath(defId, 'partitioned');

          const inst = await client.startProcess('SingleTaskProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          await client.pollExternalTask('process-item', async () => ({ done: true }), 15000, instanceId);
          await client.waitForProcessEnd(instanceId, 15000);

          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'completed', `expected Phase 1's linear shape to still complete correctly, got ${JSON.stringify(hist.data)}`);
        },
      },
    ],
  });
}

run().catch(err => {
  // See suite 26/27's identical footer for why this must be process.exit(1).
  console.error(err);
  process.exit(1);
});
