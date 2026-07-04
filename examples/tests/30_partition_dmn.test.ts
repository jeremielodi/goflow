/**
 * Suite 30 — Phase 4 partitioned write path: business rule task (DMN)
 * (see architecture/event-sourced-kafka-design.md)
 *
 * internal/partition.ValidateShape's linear chain now accepts business
 * rule tasks (zeebe:calledDecision) mixed with service tasks — confirmed
 * by research that runtime.ExecuteExecution already evaluates a decision
 * table synchronously inline (like a gateway, one atomic transaction, no
 * job/wait state), so this needed zero changes to internal/service or
 * internal/runtime, only the validator.
 */
import * as fs from 'fs';
import * as path from 'path';
import FormData from 'form-data';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '30_discount_decision.bpmn');
const DMN = path.join(__dirname, 'bpmn', '30_discount_decision.dmn');
const SINGLE_TASK_BPMN = path.join(__dirname, 'bpmn', '27_single_service_task.bpmn');
const DIAMOND_BPMN = path.join(__dirname, 'bpmn', '28_diamond_gateway.bpmn');
const CHAIN_BPMN = path.join(__dirname, 'bpmn', '29_two_task_chain.bpmn');
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

  const form = new FormData();
  form.append('resources', fs.createReadStream(BPMN));
  form.append('resources', fs.createReadStream(DMN));
  form.append('deployment-name', 'Discount Chain + Decision');
  await client.api.post('/engine-rest/deployment/create', form, { headers: form.getHeaders() });

  await client.deployBpmn(SINGLE_TASK_BPMN);
  await client.deployBpmn(DIAMOND_BPMN);
  await client.deployBpmn(CHAIN_BPMN);

  await runSuite({
    name: '30 — Phase 4 business rule task (DMN)',
    tests: [
      {
        name: '[A] Flipping the discount-chain definition to partitioned succeeds',
        async fn() {
          const defId = await findDefinitionId('DiscountChainProcess');
          const res = await setWritePath(defId, 'partitioned');
          assert(res.status === 200, `expected 200 flipping DiscountChainProcess to partitioned, got ${res.status}`);
          assert(res.data.writePath === 'partitioned', `expected writePath=partitioned in response, got ${JSON.stringify(res.data)}`);
        },
      },

      {
        name: '[B] amount=500 evaluates inline to discountRate=0, applies via job, completes',
        async fn() {
          const inst = await client.startProcess('DiscountChainProcess', { amount: 500 });
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          let task: any = null;
          const deadline = Date.now() + 15000;
          while (!task && Date.now() < deadline) {
            const tasks = await client.fetchAndLock([{ name: 'process-apply-discount' }]);
            task = tasks.find((t) => t.processInstanceId === instanceId) ?? null;
            if (!task) await sleep(500);
          }
          assert(!!task, 'expected the business rule task to evaluate inline and create the apply-discount job with no separate step for it');
          assert(task.variables?.discountResult?.value === 0, `expected discountResult=0 for amount=500, got ${JSON.stringify(task.variables?.discountResult)}`);

          await client.completeExternalTask(task.id, { applied: true });
          await client.waitForProcessEnd(instanceId, 15000);

          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'completed', `expected history to report completed, got ${JSON.stringify(hist.data)}`);
        },
      },

      {
        name: '[C] amount=2000 evaluates to a different rule (discountRate=0.1), proving evaluation is not cached/hardcoded',
        async fn() {
          const inst = await client.startProcess('DiscountChainProcess', { amount: 2000 });
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          let task: any = null;
          const deadline = Date.now() + 15000;
          while (!task && Date.now() < deadline) {
            const tasks = await client.fetchAndLock([{ name: 'process-apply-discount' }]);
            task = tasks.find((t) => t.processInstanceId === instanceId) ?? null;
            if (!task) await sleep(500);
          }
          assert(!!task, 'expected the apply-discount job to be created');
          assert(task.variables?.discountResult?.value === 0.1, `expected discountResult=0.1 for amount=2000, got ${JSON.stringify(task.variables?.discountResult)}`);

          await client.completeExternalTask(task.id, { applied: true });
          await client.waitForProcessEnd(instanceId, 15000);
        },
      },

      {
        name: "[D] Phase 1/2/3 shapes still work after mixing business rule tasks into the linear chain",
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

          const chainDefId = await findDefinitionId('TwoTaskChainProcess');
          await setWritePath(chainDefId, 'partitioned');
          const chain = await client.startProcess('TwoTaskChainProcess', {});
          const chainId = chain.processInstanceId ?? (chain as any).id;
          await client.pollExternalTask('process-step-one', async () => ({ done: true }), 15000, chainId);
          await client.pollExternalTask('process-step-two', async () => ({ done: true }), 15000, chainId);
          await client.waitForProcessEnd(chainId, 15000);
          const chainHist = await client.api.get(`/engine-rest/history/process-instance/${chainId}`);
          assert(chainHist.data?.state === 'completed', `expected Phase 3's chain shape to still complete, got ${JSON.stringify(chainHist.data)}`);
        },
      },
    ],
  });
}

run().catch(err => {
  // See suite 26-29's identical footer for why this must be process.exit(1).
  console.error(err);
  process.exit(1);
});
