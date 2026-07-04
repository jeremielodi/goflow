/**
 * Suite 29 — Phase 3 partitioned write path: multi-task linear chain
 * (see architecture/event-sourced-kafka-design.md)
 *
 * Phase 1 (suite 27) and Phase 2 (suite 28) both validated shapes where
 * completing the last job always finishes the process in exactly one hop
 * — internal/partition/gateway.go's CompleteJob bridge waits on
 * JobCompleted (correlated by job_id), not ProcessCompleted, specifically
 * to handle jobs that DON'T immediately finish the process, but that path
 * had never actually been exercised until this suite: a 2-task chain
 * (start -> stepOne -> stepTwo -> end) is the smallest shape where
 * completing stepOne's job leaves the process still running.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const CHAIN_BPMN = path.join(__dirname, 'bpmn', '29_two_task_chain.bpmn');
const SINGLE_TASK_BPMN = path.join(__dirname, 'bpmn', '27_single_service_task.bpmn');
const DIAMOND_BPMN = path.join(__dirname, 'bpmn', '28_diamond_gateway.bpmn');
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

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(CHAIN_BPMN);
  await client.deployBpmn(SINGLE_TASK_BPMN);
  await client.deployBpmn(DIAMOND_BPMN);

  await runSuite({
    name: '29 — Phase 3 multi-task linear chain',
    tests: [
      {
        name: '[A] Flipping the two-task chain definition to partitioned succeeds',
        async fn() {
          const defId = await findDefinitionId('TwoTaskChainProcess');
          const res = await setWritePath(defId, 'partitioned');
          assert(res.status === 200, `expected 200 flipping TwoTaskChainProcess to partitioned, got ${res.status}`);
          assert(res.data.writePath === 'partitioned', `expected writePath=partitioned in response, got ${JSON.stringify(res.data)}`);
        },
      },

      {
        name: '[B] Completing step one leaves the process still active (the multi-hop gap this suite exists to close)',
        async fn() {
          const inst = await client.startProcess('TwoTaskChainProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          await client.pollExternalTask('process-step-one', async () => ({ stepOneDone: true }), 15000, instanceId);

          // Give the partition processor a brief moment to have applied
          // the completion, then assert the process is genuinely NOT done
          // yet — stepTwo's job must still be pending.
          await sleep(500);
          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'active', `expected the process to still be active after only step one completed, got ${JSON.stringify(hist.data)}`);

          let stepTwoTask: any = null;
          const deadline = Date.now() + 15000;
          while (!stepTwoTask && Date.now() < deadline) {
            const tasks = await client.fetchAndLock([{ name: 'process-step-two' }]);
            stepTwoTask = tasks.find((t) => t.processInstanceId === instanceId) ?? null;
            if (!stepTwoTask) await sleep(500);
          }
          assert(!!stepTwoTask, 'expected step two\'s job to have been created after step one completed');

          // Clean up so it doesn't linger into later tests.
          await client.completeExternalTask(stepTwoTask.id, { stepTwoDone: true });
          await client.waitForProcessEnd(instanceId, 15000);
        },
      },

      {
        name: '[C] Completing both steps in sequence completes the process end to end',
        async fn() {
          const inst = await client.startProcess('TwoTaskChainProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          await client.pollExternalTask('process-step-one', async () => ({ stepOneDone: true }), 15000, instanceId);
          await client.pollExternalTask('process-step-two', async () => ({ stepTwoDone: true }), 15000, instanceId);
          await client.waitForProcessEnd(instanceId, 15000);

          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'completed', `expected history to report completed, got ${JSON.stringify(hist.data)}`);
        },
      },

      {
        name: "[D] Phase 1's linear shape and Phase 2's diamond shape still work after this generalization",
        async fn() {
          const singleDefId = await findDefinitionId('SingleTaskProcess');
          await setWritePath(singleDefId, 'partitioned');
          const single = await client.startProcess('SingleTaskProcess', {});
          const singleId = single.processInstanceId ?? (single as any).id;
          await client.pollExternalTask('process-item', async () => ({ done: true }), 15000, singleId);
          await client.waitForProcessEnd(singleId, 15000);
          const singleHist = await client.api.get(`/engine-rest/history/process-instance/${singleId}`);
          assert(singleHist.data?.state === 'completed', `expected Phase 1's linear shape to still complete, got ${JSON.stringify(singleHist.data)}`);

          const diamondDefId = await findDefinitionId('DiamondProcess');
          await setWritePath(diamondDefId, 'partitioned');
          const diamond = await client.startProcess('DiamondProcess', { branch: 'a' });
          const diamondId = diamond.processInstanceId ?? (diamond as any).id;
          await client.pollExternalTask('process-branch-a', async () => ({ done: true }), 15000, diamondId);
          await client.waitForProcessEnd(diamondId, 15000);
          const diamondHist = await client.api.get(`/engine-rest/history/process-instance/${diamondId}`);
          assert(diamondHist.data?.state === 'completed', `expected Phase 2's diamond shape to still complete, got ${JSON.stringify(diamondHist.data)}`);
        },
      },
    ],
  });
}

run().catch(err => {
  // See suite 26/27/28's identical footer for why this must be process.exit(1).
  console.error(err);
  process.exit(1);
});
