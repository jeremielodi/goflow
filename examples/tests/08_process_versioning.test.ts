/**
 * Suite 08 — Process Definition Versioning
 * Source: https://camunda.com/bpmn/reference/ — Process Versioning
 *
 * Scenario: Deploy the same BPMN process key multiple times. Each deploy
 *           creates a new version. Instances started before the new deployment
 *           keep running on their original version; new instances use the latest.
 *           Users can also start a specific version by definition ID.
 *
 * Tests:
 *   A) Two deploys of the same key → version 1 and version 2 created
 *   B) List all versions for a key → both visible via /engine-rest/process-definition?key=X
 *   C) Start by key → latest version (v2) is used
 *   D) Start by definition ID → specific version is used
 *   E) Instance from v1 retains its original process_definition_id (not v2)
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert } from './client';

// We reuse the simple support ticket BPMN (SupportTicketProcess) for versioning tests —
// we just deploy it twice to produce v1 and v2 of the same process key.
const BPMN = path.join(__dirname, 'bpmn', '07_support_ticket.bpmn');
const client = new GoFlowClient();

async function run() {
  await client.loginAsSuperUser();

  let v1Id: string;
  let v2Id: string;
  const KEY = 'SupportTicketProcess';

  await runSuite({
    name: '08 — Process Definition Versioning',
    tests: [

      // ── A: deploy twice → two versions ───────────────────────────────────────
      {
        name: '[Deploy] Two deploys of the same key produce version 1 and version 2',
        async fn() {
          await client.deployBpmn(BPMN, 'SupportTicket v1 deploy');
          await client.deployBpmn(BPMN, 'SupportTicket v2 deploy');

          // List all versions
          const res = await client.api.get(`/engine-rest/process-definition?key=${KEY}`);
          const defs = res.data as any[];
          assert(defs.length >= 2, `expected at least 2 versions, got ${defs.length}`);

          const versions = defs.map(d => d.version).sort((a, b) => a - b);
          const v1 = defs.find(d => d.version === versions[versions.length - 2]); // second-to-latest
          const v2 = defs.find(d => d.version === versions[versions.length - 1]); // latest
          assert(v1 !== undefined, 'v1 not found');
          assert(v2 !== undefined, 'v2 not found');
          assert(v2.version > v1.version, `v2 version (${v2.version}) should be > v1 (${v1.version})`);

          v1Id = v1.id;
          v2Id = v2.id;
        },
      },

      // ── B: list versions ──────────────────────────────────────────────────────
      {
        name: '[List] GET /engine-rest/process-definition?key returns all versions, latestVersion=true returns one',
        async fn() {
          assert(v1Id !== undefined, 'run test A first');

          const allRes = await client.api.get(`/engine-rest/process-definition?key=${KEY}`);
          const allDefs = allRes.data as any[];
          assert(allDefs.length >= 2, `expected >=2, got ${allDefs.length}`);

          const latestRes = await client.api.get(`/engine-rest/process-definition?key=${KEY}&latestVersion=true`);
          const latestDefs = latestRes.data as any[];
          assert(latestDefs.length === 1, `expected exactly 1 latest, got ${latestDefs.length}`);
          assert(latestDefs[0].id === v2Id, `latest should be v2 (${v2Id}), got ${latestDefs[0].id}`);
        },
      },

      // ── C: start by key → latest version ─────────────────────────────────────
      {
        name: '[Start by key] Starting by process key always uses the latest version',
        async fn() {
          assert(v2Id !== undefined, 'run test A first');

          const inst = await client.startProcess(KEY, { ticketId: 'TKT-VER-001' });
          const instanceId = inst.processInstanceId ?? inst.id;

          // Resolve the job so the process can finish
          await client.pollExternalTask('classify_ticket', async () => ({ category: 'test' }), 10000, instanceId);
          await client.waitForProcessEnd(instanceId, 10000);

          // Verify the instance used the latest definition
          const piRes = await client.api.get(`/engine-rest/process-instance/${instanceId}`);
          const pi = piRes.data as any;
          assert(
            pi.processDefinitionId === v2Id,
            `expected processDefinitionId=${v2Id}, got ${pi.processDefinitionId}`
          );
        },
      },

      // ── D: start by definition ID → specific version ──────────────────────────
      {
        name: '[Start by ID] POST /engine-rest/process-definition/:id/start uses that exact version',
        async fn() {
          assert(v1Id !== undefined, 'run test A first');

          const res = await client.api.post(`/engine-rest/process-definition/${v1Id}/start`, {
            variables: { ticketId: 'TKT-VER-V1', source: 'versioning-test' },
          });
          const instanceId = res.data.processInstanceId;
          assert(res.data.definitionId === v1Id, `expected definitionId=${v1Id}, got ${res.data.definitionId}`);
          assert(typeof res.data.version === 'number', 'version field should be a number');

          // Drain the job
          await client.pollExternalTask('classify_ticket', async () => ({ done: true }), 10000, instanceId);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      // ── E: instance retains its original definition ID ────────────────────────
      {
        name: '[Isolation] Instance started from v1 still reports v1 processDefinitionId after v2 deployed',
        async fn() {
          assert(v1Id !== undefined, 'run test A first');

          // Start a fresh instance locked to v1
          const res = await client.api.post(`/engine-rest/process-definition/${v1Id}/start`, {
            variables: { ticketId: 'TKT-VER-ISO' },
          });
          const instanceId = res.data.processInstanceId;

          // Deploy v3 of the same process (simulates further evolution)
          await client.deployBpmn(BPMN, 'SupportTicket v3 deploy');

          // The v1 instance should still show the v1 definition ID
          const piRes = await client.api.get(`/engine-rest/process-instance/${instanceId}`);
          const pi = piRes.data as any;
          assert(
            pi.processDefinitionId === v1Id,
            `instance should still reference v1 (${v1Id}), got ${pi.processDefinitionId}`
          );

          // Clean up — complete the job
          await client.pollExternalTask('classify_ticket', async () => ({}), 10000, instanceId);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

    ],
  });
}

run().catch(console.error);
