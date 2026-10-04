/**
 * Suite 34 — Phase 7 partitioned write path: parallel gateway (AND fork/join)
 * (see architecture/event-sourced-kafka-design.md)
 *
 * Phase 2 (suite 28) added the diamond shape: one exclusive gateway, XOR
 * semantics (exactly one branch runs). This suite exercises the fourth
 * shape: start -> parallelGateway (fork, unconditional) -> {taskA, taskB}
 * (both run) -> parallelGateway (join) -> one shared end event.
 * `internal/partition.ValidateShape` changed to recognize this shape; no
 * Processor/Command Gateway changes were needed — the fork's children are
 * created and walked to their first job by the same runtime.ExecuteExecution
 * call every prior shape already reuses, and each branch's own
 * CompleteJobCommand independently resumes that one child via the existing
 * generic resume-after-job-completion path, with runtime.go's
 * ParallelGatewayExecutor join logic (shared with the legacy/Citus paths)
 * doing the actual join bookkeeping. Suite 27/28's fixtures must stay green
 * afterward (test [E]), proving neither prior shape was disturbed.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert } from './client';

const PARALLEL_BPMN = path.join(__dirname, 'bpmn', '34_parallel_gateway.bpmn');
const CONDITIONED_FLOW_BPMN = path.join(__dirname, 'bpmn', '34_parallel_conditioned_flow.bpmn');
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
  await client.deployBpmn(PARALLEL_BPMN);
  await client.deployBpmn(CONDITIONED_FLOW_BPMN);
  await client.deployBpmn(SINGLE_TASK_BPMN);
  await client.deployBpmn(DIAMOND_BPMN);

  await runSuite({
    name: '34 — Phase 7 parallel gateway (AND fork/join)',
    tests: [
      {
        name: '[A] Flipping the valid parallel fork/join definition to partitioned succeeds',
        async fn() {
          const defId = await findDefinitionId('ParallelForkJoinProcess');
          const res = await setWritePath(defId, 'partitioned');
          assert(res.status === 200, `expected 200 flipping ParallelForkJoinProcess to partitioned, got ${res.status}`);
          assert(res.data.writePath === 'partitioned', `expected writePath=partitioned in response, got ${JSON.stringify(res.data)}`);
        },
      },

      {
        name: '[B] Flipping a parallel gateway with a conditioned fork flow is rejected',
        async fn() {
          const defId = await findDefinitionId('ParallelConditionedFlow');
          let rejected = false;
          try {
            await setWritePath(defId, 'partitioned');
          } catch (e: any) {
            rejected = e.response?.status === 400;
          }
          assert(rejected, 'expected a 400 rejecting a parallel fork flow with an explicit condition');
        },
      },

      {
        name: '[C] Both branches run and complete independently before the join, instance completes',
        async fn() {
          const inst = await client.startProcess('ParallelForkJoinProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          await client.pollExternalTask('parallel-branch-a', async () => ({ done: true }), 15000, instanceId);
          await client.pollExternalTask('parallel-branch-b', async () => ({ done: true }), 15000, instanceId);
          await client.waitForProcessEnd(instanceId, 15000);

          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'completed', `expected history to report completed, got ${JSON.stringify(hist.data)}`);
        },
      },

      {
        name: '[D] Branch order does not matter — completing B before A still joins and completes',
        async fn() {
          const inst = await client.startProcess('ParallelForkJoinProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          await client.pollExternalTask('parallel-branch-b', async () => ({ done: true }), 15000, instanceId);
          await client.pollExternalTask('parallel-branch-a', async () => ({ done: true }), 15000, instanceId);
          await client.waitForProcessEnd(instanceId, 15000);

          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'completed', `expected history to report completed regardless of branch completion order, got ${JSON.stringify(hist.data)}`);
        },
      },

      {
        name: "[E] Phase 1/2's linear and diamond shapes (SingleTaskProcess, DiamondProcess) still work after Phase 7's validate change",
        async fn() {
          const linearDefId = await findDefinitionId('SingleTaskProcess');
          await setWritePath(linearDefId, 'partitioned');
          const linearInst = await client.startProcess('SingleTaskProcess', {});
          const linearInstanceId = linearInst.processInstanceId ?? (linearInst as any).id;
          await client.pollExternalTask('process-item', async () => ({ done: true }), 15000, linearInstanceId);
          await client.waitForProcessEnd(linearInstanceId, 15000);
          const linearHist = await client.api.get(`/engine-rest/history/process-instance/${linearInstanceId}`);
          assert(linearHist.data?.state === 'completed', `expected Phase 1's linear shape to still complete correctly, got ${JSON.stringify(linearHist.data)}`);

          const diamondDefId = await findDefinitionId('DiamondProcess');
          await setWritePath(diamondDefId, 'partitioned');
          const diamondInst = await client.startProcess('DiamondProcess', { branch: 'a' });
          const diamondInstanceId = diamondInst.processInstanceId ?? (diamondInst as any).id;
          await client.pollExternalTask('process-branch-a', async () => ({ done: true }), 15000, diamondInstanceId);
          await client.waitForProcessEnd(diamondInstanceId, 15000);
          const diamondHist = await client.api.get(`/engine-rest/history/process-instance/${diamondInstanceId}`);
          assert(diamondHist.data?.state === 'completed', `expected Phase 2's diamond shape to still complete correctly, got ${JSON.stringify(diamondHist.data)}`);
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
