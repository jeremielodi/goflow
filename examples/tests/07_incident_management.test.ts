/**
 * Suite 07 — Incident Management  (BPMN: 07_support_ticket.bpmn)
 * Source: https://camunda.com/bpmn/reference/ — External Task / Incident Management
 *
 * Scenario: A ticket is created and sent to an external classification worker.
 *           When the worker fails permanently (retries=0, no error boundary),
 *           an incident is created. Operators can list/inspect incidents, resolve
 *           them by resetting job retries, or delete them.
 *
 * Tests:
 *   A) Permanent failure → incident is created and visible in the list
 *   B) Incident scoped to process instance via /process-instance/:id/incidents
 *   C) Set job retries → incident resolved, job retried, process completes
 *   D) Delete incident → incident disappears from open list
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '07_support_ticket.bpmn');
const client = new GoFlowClient();

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Support Ticket Incident Test');

  await runSuite({
    name: '07 — Incident Management (Support Ticket)',
    tests: [

      // ── A: failure creates an incident ────────────────────────────────────────
      {
        name: '[Create incident] Permanent job failure with no boundary → incident open',
        async fn() {
          const inst = await client.startProcess('SupportTicketProcess', {
            ticketId: 'TKT-001',
            priority: 'high',
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          // Lock then fail with retries=0 (no error code, no boundary event)
          const tasks = await client.fetchAndLock([{ name: 'classify_ticket' }]);
          const job = tasks.find(t => t.processInstanceId === instanceId);
          assert(job !== undefined, 'classify_ticket job not found');

          await client.api.post(`/engine-rest/external-task/${job!.id}/failure`, {
            workerId: client.workerId,
            errorMessage: 'Classification service unavailable',
            retries: 0,
            retryTimeout: 0,
          });

          // Wait for incident to appear
          await sleep(500);
          const res = await client.api.get(`/engine-rest/incident?processInstanceId=${instanceId}`);
          const incidents = res.data as any[];
          assert(incidents.length > 0, `expected at least 1 incident, got ${incidents.length}`);
          const incident = incidents[0];
          assert(incident.state === 'open', `expected state=open, got ${incident.state}`);
          assert(incident.incidentType === 'failedExternalTask', `unexpected type: ${incident.incidentType}`);
          assert(incident.activityId === 'classifyTicket', `unexpected activityId: ${incident.activityId}`);
          assert(incident.jobId === job!.id, `jobId mismatch`);
        },
      },

      // ── B: incidents scoped to process instance ───────────────────────────────
      {
        name: '[Process scope] GET /process-instance/:id/incidents returns only that instance\'s incidents',
        async fn() {
          const inst = await client.startProcess('SupportTicketProcess', {
            ticketId: 'TKT-002',
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          const tasks = await client.fetchAndLock([{ name: 'classify_ticket' }]);
          const job = tasks.find(t => t.processInstanceId === instanceId);
          assert(job !== undefined, 'classify_ticket job not found');

          await client.api.post(`/engine-rest/external-task/${job!.id}/failure`, {
            workerId: client.workerId,
            errorMessage: 'Timeout',
            retries: 0,
            retryTimeout: 0,
          });

          await sleep(500);
          const res = await client.api.get(`/engine-rest/process-instance/${instanceId}/incidents`);
          const incidents = res.data as any[];
          assert(incidents.length > 0, 'expected incidents for this instance');
          for (const inc of incidents) {
            assert(inc.processInstanceId === instanceId, `unexpected processInstanceId ${inc.processInstanceId}`);
          }
        },
      },

      // ── C: resolve incident by setting retries ────────────────────────────────
      {
        name: '[Resolve] Set job retries → incident resolved → process completes',
        async fn() {
          const inst = await client.startProcess('SupportTicketProcess', {
            ticketId: 'TKT-003',
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          const tasks = await client.fetchAndLock([{ name: 'classify_ticket' }]);
          const job = tasks.find(t => t.processInstanceId === instanceId);
          assert(job !== undefined, 'classify_ticket job not found');

          // Fail permanently → creates incident
          await client.api.post(`/engine-rest/external-task/${job!.id}/failure`, {
            workerId: client.workerId,
            errorMessage: 'Transient error',
            retries: 0,
            retryTimeout: 0,
          });

          await sleep(500);
          const before = await client.api.get(`/engine-rest/incident?processInstanceId=${instanceId}`);
          assert((before.data as any[]).some(i => i.state === 'open'), 'incident should be open before retry');

          // Reset job retries → incident resolved, job back to pending
          await client.api.post(`/engine-rest/job/${job!.id}/retries`, { retries: 1 });

          await sleep(300);
          const after = await client.api.get(`/engine-rest/incident?processInstanceId=${instanceId}`);
          const openAfter = (after.data as any[]).filter(i => i.jobId === job!.id && i.state === 'open');
          assert(openAfter.length === 0, `incident should be resolved, still open: ${openAfter.length}`);

          // Now the job is pending again — complete it
          await client.pollExternalTask(
            'classify_ticket',
            async () => ({ category: 'billing', resolved: true }),
            10000,
            instanceId
          );

          await client.waitForProcessEnd(instanceId, 10000);
          const final = await client.getProcessInstance(instanceId);
          assert(!final || final.status === 'completed', `expected completed, got ${final?.status}`);
        },
      },

      // ── D: delete incident ────────────────────────────────────────────────────
      {
        name: '[Delete] DELETE /engine-rest/incident/:id removes the incident',
        async fn() {
          const inst = await client.startProcess('SupportTicketProcess', {
            ticketId: 'TKT-004',
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          const tasks = await client.fetchAndLock([{ name: 'classify_ticket' }]);
          const job = tasks.find(t => t.processInstanceId === instanceId);
          assert(job !== undefined, 'classify_ticket job not found');

          await client.api.post(`/engine-rest/external-task/${job!.id}/failure`, {
            workerId: client.workerId,
            errorMessage: 'To be deleted',
            retries: 0,
            retryTimeout: 0,
          });

          await sleep(500);
          const listRes = await client.api.get(`/engine-rest/incident?processInstanceId=${instanceId}`);
          const openIncidents = (listRes.data as any[]).filter(i => i.state === 'open');
          assert(openIncidents.length > 0, 'need at least one open incident to delete');

          const incidentId = openIncidents[0].id;
          await client.api.delete(`/engine-rest/incident/${incidentId}`);

          await sleep(300);
          const res2 = await client.api.get(`/engine-rest/incident/${incidentId}`);
          assert(res2.data.state === 'deleted', `expected state=deleted, got ${res2.data.state}`);
        },
      },

    ],
  });
}

run().catch(console.error);
