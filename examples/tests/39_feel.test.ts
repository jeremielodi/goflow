/**
 * Suite 39 — Native FEEL  (BPMN: 39_feel.bpmn)
 *
 * Runs only when the server uses the native interpreter: start the stack
 * with FEEL_MODE=feel and run this suite with FEEL_MODE=feel in the
 * environment. Otherwise it reports itself skipped.
 *
 * Tests:
 *   A) Output mappings with for/sum on exact decimals (0.1 * 3 = 0.3), a
 *      list filter with a path, if/then/else and count(); a gateway
 *      condition using list contains().
 *   B) A variable that was never set is null in a condition, not an error.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert } from './client';

const BPMN = path.join(__dirname, 'bpmn', '39_feel.bpmn');
const client = new GoFlowClient();

async function run() {
  if ((process.env.FEEL_MODE ?? '').toLowerCase() !== 'feel') {
    console.log('\n  ⏭️  39 — Native FEEL: skipped (set FEEL_MODE=feel on the server and for this suite)\n');
    return;
  }
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Native FEEL Test');

  await runSuite({
    name: '39 — Native FEEL',
    tests: [
      {
        name: '[A+B] FEEL-only expressions route the instance down the exact, no-discount path',
        async fn() {
          const inst = await client.startProcess('FeelShowcase', {});
          const instanceId = inst.processInstanceId ?? inst.id;

          const enter = await client.waitForTask('enter_order', instanceId, 10000);
          await client.completeTask(enter.id, {
            items: [
              { sku: 'A', qty: 2, price: 0.1 },
              { sku: 'B', qty: 1, price: 0.1 },
            ],
          });

          const exact = await client.waitForTask('exact', instanceId, 10000);
          const vars = await client.getProcessVariables(instanceId) as Record<string, { value?: unknown }>;
          assert(vars.total?.value === 0.3, `total should be exactly 0.3, got ${JSON.stringify(vars.total)}`);
          assert(vars.kind?.value === 'multi', `kind should be "multi", got ${JSON.stringify(vars.kind)}`);

          await client.completeTask(exact.id);
          const noDiscount = await client.waitForTask('no_discount', instanceId, 10000);
          await client.completeTask(noDiscount.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },
    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
