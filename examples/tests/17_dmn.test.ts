/**
 * Suite 17 — DMN Decision Engine  (BPMN: 17_dmn_process.bpmn, DMN: 17_approval_decision.dmn)
 *
 * Verifies GoFlow's new DMN decision table support: parsing, hit-policy
 * evaluation (UNIQUE, with numeric comparisons, ranges, string lists, and
 * wildcards), Business Rule Task execution (zeebe:calledDecision), deploying
 * a .bpmn and a .dmn file together in one request, and standalone decision
 * evaluation via the v2 API.
 *
 * Tests:
 *   A) Deploying a .bpmn + .dmn pair together reports both the process and
 *      the decision in the response.
 *   B) A Business Rule Task evaluates the decision and stores the full
 *      (multi-output) result under the configured resultVariable.
 *   C) A different input routes to a different rule (range/comparison logic).
 *   D) POST /v2/decisions/:key/evaluation evaluates the decision directly,
 *      without any process instance.
 */
import * as fs from 'fs';
import * as path from 'path';
import FormData from 'form-data';
import { GoFlowClient, runSuite, assert } from './client';

const BPMN = path.join(__dirname, 'bpmn', '17_dmn_process.bpmn');
const DMN = path.join(__dirname, 'bpmn', '17_approval_decision.dmn');
const client = new GoFlowClient();

async function run() {
  await client.loginAsSuperUser();

  await runSuite({
    name: '17 — DMN Decision Engine',
    tests: [

      // ── A: combined .bpmn + .dmn deployment ────────────────────────────────
      {
        name: '[A] Deploying .bpmn + .dmn together reports both resources',
        async fn() {
          const form = new FormData();
          form.append('resources', fs.createReadStream(BPMN));
          form.append('resources', fs.createReadStream(DMN));
          form.append('deployment-name', 'DMN Process + Decision Test');

          const res = await client.api.post('/engine-rest/deployment/create', form, {
            headers: form.getHeaders(),
          });

          assert(
            res.data.deployedProcessDefinitions?.DMNProcess !== undefined,
            'expected DMNProcess in deployedProcessDefinitions'
          );
          assert(
            res.data.deployedDecisions?.approvalDecision !== undefined,
            'expected approvalDecision in deployedDecisions'
          );
        },
      },

      // ── B: business rule task, multi-output decision, first rule ───────────
      {
        name: '[B] Business Rule Task stores the multi-output decision result',
        async fn() {
          const inst = await client.startProcess('DMNProcess', { amount: 500, tier: 'gold' });
          const instanceId = inst.processInstanceId ?? inst.id;

          await client.waitForProcessEnd(instanceId, 10000);

          const vars = await client.getProcessVariables(instanceId) as any;
          const result = vars.approvalResult?.value ?? vars.approvalResult;
          assert(result?.approved === true, `expected approved=true, got ${JSON.stringify(result)}`);
          assert(result?.discount === 0.1, `expected discount=0.1, got ${JSON.stringify(result)}`);
        },
      },

      // ── C: different input routes to a different rule ──────────────────────
      {
        name: '[C] Range/comparison rule selection: high amount → rejected, no discount',
        async fn() {
          const inst = await client.startProcess('DMNProcess', { amount: 9000, tier: 'gold' });
          const instanceId = inst.processInstanceId ?? inst.id;

          await client.waitForProcessEnd(instanceId, 10000);

          const vars = await client.getProcessVariables(instanceId) as any;
          const result = vars.approvalResult?.value ?? vars.approvalResult;
          assert(result?.approved === false, `expected approved=false, got ${JSON.stringify(result)}`);
          assert(result?.discount === 0, `expected discount=0, got ${JSON.stringify(result)}`);
        },
      },

      // ── D: standalone v2 decision evaluation ────────────────────────────────
      {
        name: '[D] POST /v2/decisions/:key/evaluation evaluates without a process instance',
        async fn() {
          const res = await client.api.post('/v2/decisions/approvalDecision/evaluation', {
            variables: { amount: 3000, tier: 'bronze' },
          });
          assert(res.data.output?.approved === true, `expected approved=true, got ${JSON.stringify(res.data.output)}`);
          assert(res.data.output?.discount === 0.05, `expected discount=0.05, got ${JSON.stringify(res.data.output)}`);
        },
      },

    ],
  });
}

run().catch(console.error);
