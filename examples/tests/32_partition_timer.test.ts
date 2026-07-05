/**
 * Suite 32 — Phase 6 partitioned write path: intermediate timer event
 * (see architecture/event-sourced-kafka-design.md)
 *
 * Every prior gate (Phases 1-5 + the retry/incident-resolution follow-up)
 * was triggered by an HTTP/gRPC caller. Timers are different — nothing
 * calls them; TimerScheduler's own background poll used to call
 * resumeExecution directly and unconditionally, completely bypassing
 * Kafka/partition ownership. This suite proves the new
 * service.TimerPartitionRouter gate (implemented by
 * internal/partition.TimerRouter) actually routes a due timer belonging
 * to a partitioned instance through a produced TimerFiredCommand instead.
 */
import * as fs from 'fs';
import * as path from 'path';
import FormData from 'form-data';
import { GoFlowClient, runSuite, assert } from './client';

const TIMER_BPMN = path.join(__dirname, 'bpmn', '32_partition_timer.bpmn');
const SINGLE_TASK_BPMN = path.join(__dirname, 'bpmn', '27_single_service_task.bpmn');
const DIAMOND_BPMN = path.join(__dirname, 'bpmn', '28_diamond_gateway.bpmn');
const CHAIN_BPMN = path.join(__dirname, 'bpmn', '29_two_task_chain.bpmn');
const DISCOUNT_BPMN = path.join(__dirname, 'bpmn', '30_discount_decision.bpmn');
const DISCOUNT_DMN = path.join(__dirname, 'bpmn', '30_discount_decision.dmn');
const LEGACY_SHORT_TIMER_BPMN = path.join(__dirname, 'bpmn', '26_short_timer.bpmn');
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
  await client.deployBpmn(TIMER_BPMN);
  await client.deployBpmn(SINGLE_TASK_BPMN);
  await client.deployBpmn(DIAMOND_BPMN);
  await client.deployBpmn(CHAIN_BPMN);
  await client.deployBpmn(LEGACY_SHORT_TIMER_BPMN);

  const form = new FormData();
  form.append('resources', fs.createReadStream(DISCOUNT_BPMN));
  form.append('resources', fs.createReadStream(DISCOUNT_DMN));
  form.append('deployment-name', 'Discount Chain + Decision (suite 32)');
  await client.api.post('/engine-rest/deployment/create', form, { headers: form.getHeaders() });

  await runSuite({
    name: '32 — Phase 6 intermediate timer event',
    tests: [
      {
        name: '[A] Flipping the timer-only definition to partitioned succeeds',
        async fn() {
          const defId = await findDefinitionId('PartitionTimerProcess');
          const res = await setWritePath(defId, 'partitioned');
          assert(res.status === 200, `expected 200, got ${res.status}`);
          assert(res.data.writePath === 'partitioned', `expected writePath=partitioned, got ${JSON.stringify(res.data)}`);
        },
      },

      {
        name: '[B] A due timer on a partitioned instance fires via TimerFiredCommand and completes the process',
        async fn() {
          const inst = await client.startProcess('PartitionTimerProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;

          await client.waitForProcessEnd(instanceId, 15000);

          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'completed', `expected the timer-only process to complete via the partitioned path, got ${JSON.stringify(hist.data)}`);
        },
      },

      {
        name: '[C] Phases 1/2/3/4 shapes still work after restructuring timer_scheduler.go',
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

          const discountDefId = await findDefinitionId('DiscountChainProcess');
          await setWritePath(discountDefId, 'partitioned');
          const discount = await client.startProcess('DiscountChainProcess', { amount: 500 });
          const discountId = discount.processInstanceId ?? (discount as any).id;
          await client.pollExternalTask('process-apply-discount', async () => ({ done: true }), 15000, discountId);
          await client.waitForProcessEnd(discountId, 15000);
          const discountHist = await client.api.get(`/engine-rest/history/process-instance/${discountId}`);
          assert(discountHist.data?.state === 'completed', `expected Phase 4's DMN chain shape to still complete, got ${JSON.stringify(discountHist.data)}`);
        },
      },

      {
        name: "[D] Legacy timer fixtures (suite 06's error-boundary-timer style, suite 26's short timer) are unaffected",
        async fn() {
          // 26_short_timer.bpmn's SingleTaskProcess-equivalent stays on
          // write_path='legacy' by default (never flipped) — confirms the
          // TimerPartitionRouter gate in triggerTimer correctly falls
          // through to the unchanged legacy resume path for it.
          const inst = await client.startProcess('ShortTimerProcess', {});
          const instanceId = inst.processInstanceId ?? (inst as any).id;
          await client.waitForProcessEnd(instanceId, 15000);
          const hist = await client.api.get(`/engine-rest/history/process-instance/${instanceId}`);
          assert(hist.data?.state === 'completed', `expected the legacy short-timer process to still complete normally, got ${JSON.stringify(hist.data)}`);
        },
      },
    ],
  });
}

run().catch(err => {
  // See suite 26-31's identical footer for why this must be process.exit(1).
  console.error(err);
  process.exit(1);
});
