/**
 * Suite 16 — Zeebe BPMN Extensions  (BPMN: 16_zeebe_extensions.bpmn)
 *
 * Verifies that real Camunda 8 (Zeebe namespace) extension elements are
 * understood, not just Camunda 7 attributes: zeebe:taskDefinition,
 * zeebe:taskHeaders, zeebe:ioMapping (input + output), zeebe:assignmentDefinition,
 * zeebe:formDefinition, and a FEEL condition on an exclusive gateway.
 *
 * Tests:
 *   A) Service task: job type from zeebe:taskDefinition, zeebe:taskHeaders
 *      exposed on fetchAndLock, zeebe:ioMapping input visible in the job
 *      payload without leaking into process variables, output mapping
 *      renames the completion variable in process scope.
 *   B) User task: assignee resolved via a FEEL assignmentDefinition
 *      expression, candidateGroups literal, formKey exposed via v2 search,
 *      ioMapping input stored as the task's form data.
 *   C) FEEL condition on the exclusive gateway routes to "Approved" based on
 *      the output-mapped variable.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '16_zeebe_extensions.bpmn');
const client = new GoFlowClient();

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Zeebe Extensions Test');

  await runSuite({
    name: '16 — Zeebe BPMN Extensions',
    tests: [

      {
        name: '[A→C] Full run: taskHeaders, ioMapping (in/out), FEEL assignee, formKey, FEEL gateway',
        async fn() {
          const inst = await client.startProcess('ZeebeExtensionsProcess', {
            orderId: 'ORD-100',
            reviewer: 'alice',
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          // ── A: service task ──────────────────────────────────────────────
          const jobs = await client.fetchAndLock([{ name: 'zeebe_prepare' }]);
          const job = (jobs as any[]).find(j => j.processInstanceId === instanceId);
          assert(job !== undefined, 'expected a zeebe_prepare job for our instance');
          assert(job.headers?.priority === 'high', `expected taskHeaders.priority=high, got ${JSON.stringify(job.headers)}`);

          const mappedOrderId = job.variables?.mappedOrderId;
          assert(
            mappedOrderId?.value === 'ORD-100',
            `expected ioMapping input mappedOrderId=ORD-100 in job payload, got ${JSON.stringify(mappedOrderId)}`
          );

          await client.completeExternalTask(job.id, { prepResult: 'ok' });

          await sleep(300);
          const vars = await client.getProcessVariables(instanceId) as any;
          assert(vars.prepStatus?.value === 'ok' || vars.prepStatus === 'ok', `expected prepStatus=ok, got ${JSON.stringify(vars.prepStatus)}`);
          assert(vars.prepResult === undefined, 'raw prepResult should not leak into process variables when an output mapping is declared');

          // ── B: user task ─────────────────────────────────────────────────
          const reviewTask = await client.waitForTask('reviewTask', instanceId);
          assert(reviewTask.assignee === 'alice', `expected assignee resolved via FEEL "=reviewer" to be alice, got ${reviewTask.assignee}`);
          assert(reviewTask.candidateGroup === 'reviewers', `expected candidateGroup=reviewers, got ${reviewTask.candidateGroup}`);

          const formData = (reviewTask as any).formData;
          assert(
            formData?.reviewOrderId === 'ORD-100',
            `expected ioMapping input reviewOrderId=ORD-100 as task form data, got ${JSON.stringify(formData)}`
          );

          const searchRes = await client.api.post('/v2/user-tasks/search', {
            filter: { processInstanceKey: instanceId, elementId: 'reviewTask' },
          });
          const v2Task = searchRes.data.items.find((t: any) => t.elementId === 'reviewTask');
          assert(v2Task?.formKey === 'review-form', `expected formKey=review-form via v2 search, got ${v2Task?.formKey}`);

          // assignee was already resolved via the FEEL assignmentDefinition at
          // task creation, so no explicit claim is needed here.
          await client.completeTask(reviewTask.id);

          // ── C: FEEL gateway condition ────────────────────────────────────
          await client.waitForProcessEnd(instanceId, 10000);
          const activities = await client.api.get(`/engine-rest/history/activity-instance?processInstanceId=${instanceId}`);
          const activityIds = (activities.data as any[]).map(a => a.activityId);
          assert(activityIds.includes('end_approved'), `expected routing to end_approved, got activities: ${activityIds.join(', ')}`);
          assert(!activityIds.includes('end_rejected'), `should not have routed to end_rejected, got activities: ${activityIds.join(', ')}`);
        },
      },

    ],
  });
}

run().catch(console.error);
