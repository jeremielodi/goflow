/**
 * Suite 18 — Tasklist-equivalent (Camunda 8 v2 API)
 *
 * Verifies GoFlow's Phase 4 additions: richer POST /v2/user-tasks/search
 * filters (state, candidateGroup, priority, processInstanceKey, elementId),
 * per-task priority resolved from zeebe:priorityDefinition (FEEL expression),
 * and linked forms (zeebe:formDefinition formId, deployed as a separate
 * .form JSON resource alongside the BPMN) served via GET /v2/forms/:formKey.
 *
 * Tests:
 *   A) Deploying a .bpmn + .form pair together reports both resources.
 *   B) GET /v2/forms/:formKey serves the deployed form's schema.
 *   C) GET /v2/forms/:formKey 404s for an unknown key.
 *   D) A user task's priority is resolved via FEEL from a process variable
 *      and returned by v2 search; the search's processInstanceKey/elementId/
 *      formKey filters all resolve to the right task.
 *   E) The v2 search "priority" filter isolates exactly the task with that
 *      priority, and "candidateGroup" + "state" together find both tasks.
 *   F) Completing a task via /v2/user-tasks/:id/completion flips its state
 *      to COMPLETED in a subsequent search.
 */
import * as fs from 'fs';
import * as path from 'path';
import FormData from 'form-data';
import { GoFlowClient, runSuite, assert } from './client';

const BPMN = path.join(__dirname, 'bpmn', '18_tasklist_process.bpmn');
const FORM = path.join(__dirname, 'bpmn', '18_review_form.form');
const client = new GoFlowClient();

async function searchUserTasks(filter: Record<string, unknown>): Promise<any[]> {
  const res = await client.api.post('/v2/user-tasks/search', { filter });
  return res.data.items;
}

async function run() {
  await client.loginAsSuperUser();

  await runSuite({
    name: '18 — Tasklist-equivalent (v2 user-tasks search + forms)',
    tests: [

      // ── A: combined .bpmn + .form deployment ────────────────────────────────
      {
        name: '[A] Deploying .bpmn + .form together reports both resources',
        async fn() {
          const form = new FormData();
          form.append('resources', fs.createReadStream(BPMN));
          form.append('resources', fs.createReadStream(FORM));
          form.append('deployment-name', 'Tasklist Process + Form Test');

          const res = await client.api.post('/engine-rest/deployment/create', form, {
            headers: form.getHeaders(),
          });

          assert(
            res.data.deployedProcessDefinitions?.TasklistProcess !== undefined,
            'expected TasklistProcess in deployedProcessDefinitions'
          );
          assert(
            res.data.deployedForms?.['review-form'] !== undefined,
            'expected review-form in deployedForms'
          );
        },
      },

      // ── B: form retrieval ────────────────────────────────────────────────────
      {
        name: '[B] GET /v2/forms/:formKey serves the deployed form schema',
        async fn() {
          const res = await client.api.get('/v2/forms/review-form');
          assert(res.data.formId === 'review-form', `expected formId review-form, got ${res.data.formId}`);
          const keys = (res.data.schema.components ?? []).map((c: any) => c.key);
          assert(keys.includes('comments'), `expected a "comments" component, got ${JSON.stringify(keys)}`);
        },
      },

      // ── C: unknown form 404s ─────────────────────────────────────────────────
      {
        name: '[C] GET /v2/forms/:formKey 404s for an unknown key',
        async fn() {
          try {
            await client.api.get('/v2/forms/does-not-exist');
            assert(false, 'expected a 404 for an unknown form key');
          } catch (e: any) {
            assert(e.response?.status === 404, `expected 404, got ${e.response?.status}`);
          }
        },
      },

      // ── D/E: priority resolution + rich search filters ──────────────────────
      {
        name: '[D/E] Priority (FEEL) + processInstanceKey/elementId/formKey/priority/candidateGroup filters',
        async fn() {
          const highInst = await client.startProcess('TasklistProcess', { taskPriority: 80 });
          const highId = highInst.processInstanceId ?? highInst.id;

          const lowInst = await client.startProcess('TasklistProcess', { taskPriority: 20 });
          const lowId = lowInst.processInstanceId ?? lowInst.id;

          const highTasks = await searchUserTasks({ processInstanceKey: highId, elementId: 'reviewTask' });
          assert(highTasks.length === 1, `expected exactly 1 task for high instance, got ${highTasks.length}`);
          assert(highTasks[0].priority === 80, `expected priority 80, got ${highTasks[0].priority}`);
          assert(highTasks[0].formKey === 'review-form', `expected formKey review-form, got ${highTasks[0].formKey}`);
          assert(highTasks[0].state === 'CREATED', `expected CREATED, got ${highTasks[0].state}`);

          const lowTasks = await searchUserTasks({ processInstanceKey: lowId, elementId: 'reviewTask' });
          assert(lowTasks.length === 1, `expected exactly 1 task for low instance, got ${lowTasks.length}`);
          assert(lowTasks[0].priority === 20, `expected priority 20, got ${lowTasks[0].priority}`);

          const byPriority = await searchUserTasks({ priority: 80, elementId: 'reviewTask' });
          assert(
            byPriority.some((t: any) => t.processInstanceKey === highId),
            'expected the priority=80 filter to include the high-priority instance'
          );
          assert(
            !byPriority.some((t: any) => t.processInstanceKey === lowId),
            'expected the priority=80 filter to exclude the low-priority instance'
          );

          const byGroup = await searchUserTasks({ candidateGroup: 'reviewers', state: 'CREATED', elementId: 'reviewTask' });
          const groupInstanceIds = byGroup.map((t: any) => t.processInstanceKey);
          assert(groupInstanceIds.includes(highId) && groupInstanceIds.includes(lowId),
            'expected candidateGroup+state filter to find both tasks');

          // ── F: completing flips state to COMPLETED ─────────────────────────
          await client.api.post(`/v2/user-tasks/${highTasks[0].userTaskKey}/assignment`, { assignee: 'reviewer1' });
          await client.api.post(`/v2/user-tasks/${highTasks[0].userTaskKey}/completion`, { variables: {} });

          const afterComplete = await searchUserTasks({ processInstanceKey: highId, elementId: 'reviewTask' });
          assert(afterComplete.length === 1, 'expected the completed task to still be findable by instance');
          assert(afterComplete[0].state === 'COMPLETED', `expected COMPLETED, got ${afterComplete[0].state}`);

          await client.waitForProcessEnd(highId, 10000);
        },
      },

    ],
  });
}

run().catch(console.error);
