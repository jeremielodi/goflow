/**
 * Suite 44 — Camunda 8 script tasks  (BPMN: 44_script_task.bpmn)
 *
 * Tests:
 *   A) zeebe:script FEEL expressions: arithmetic, a list built with for,
 *      and an expression using an input mapping with its result renamed by
 *      an output mapping.
 *   B) A script task and a business rule task with zeebe:taskDefinition
 *      are worked by job workers.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert } from './client';

const BPMN = path.join(__dirname, 'bpmn', '44_script_task.bpmn');
const client = new GoFlowClient();

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Script Task Test');

  await runSuite({
    name: '44 — Camunda 8 script tasks',
    tests: [
      {
        name: '[A] FEEL script tasks compute results, with input/output mappings',
        async fn() {
          const inst = await client.startProcess('ScriptFeel', { a: 3, b: 4, items: [1, 2, 3], price: 100 });
          const id = inst.processInstanceId ?? inst.id;
          const check = await client.waitForTask('sf_check', id, 10000);

          const vars = await client.getProcessVariables(id) as Record<string, { value?: any }>;
          assert(vars.sum?.value === 7, `sum should be 7, got ${JSON.stringify(vars.sum)}`);
          assert(JSON.stringify(vars.doubled?.value) === JSON.stringify([2, 4, 6]), `doubled should be [2,4,6], got ${JSON.stringify(vars.doubled)}`);
          assert(vars.total?.value === 120, `total should be 120, got ${JSON.stringify(vars.total)}`);
          assert(vars.gross === undefined && vars.rate === undefined, `with output mappings only "total" should reach the process: ${JSON.stringify(Object.keys(vars))}`);

          await client.completeTask(check.id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
      {
        name: '[B] Script and business rule tasks with a job type are worked by job workers',
        async fn() {
          const inst = await client.startProcess('ScriptJob', {});
          const id = inst.processInstanceId ?? inst.id;
          await client.pollExternalTask('script_worker', async () => ({ scripted: true }), 15000, id);
          await client.pollExternalTask('rule_worker', async () => ({ decided: true }), 15000, id);
          await client.waitForProcessEnd(id, 10000);
        },
      },
    ],
  });
}

run().catch(err => { console.error(err); process.exit(1); });
