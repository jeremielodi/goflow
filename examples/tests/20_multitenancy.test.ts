/**
 * Suite 20 — Multi-tenancy (Camunda 8 Phase 7)
 *
 * Verifies real tenant isolation: two tenants deploy the SAME process key
 * ("TenantProcess") independently, then start their own instances. A
 * tenant-scoped user must never see, list, search, or directly fetch by ID
 * another tenant's instances or process definitions — while a superuser
 * (no tenant) sees everything, matching existing superuser semantics.
 *
 * Because process definition versions aren't tenant-partitioned (two
 * tenants sharing a process key produce v1/v2 of "the same" key), the
 * running instance's tenant is always stamped from the CALLER's own
 * tenant at start time — not from whichever definition version happens to
 * resolve as "latest". This suite's core assertion is that this holds even
 * under that version ambiguity.
 *
 * Tests:
 *   A) Two tenants deploy the same process key under their own tenant.
 *   B) Each tenant starts an instance; GET/POST list+search endpoints only
 *      return the caller's own tenant's instances.
 *   C) Directly fetching another tenant's instance/process-definition by ID
 *      returns 404/null, not the resource.
 *   D) A superuser (no tenant) sees instances/definitions from BOTH tenants.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert } from './client';

const BPMN = path.join(__dirname, 'bpmn', '20_tenant_process.bpmn');
const superuser = new GoFlowClient();

async function createTenantUser(email: string, tenantId: string): Promise<GoFlowClient> {
  await superuser.api.post('/users', {
    email,
    full_name: email,
    password: 'password123',
    tenant_id: tenantId,
  });
  const client = new GoFlowClient();
  await client.login(email, 'password123');
  return client;
}

async function run() {
  await superuser.loginAsSuperUser();

  const userA = await createTenantUser('tenant-a-user@goflow.com', 'tenant-a');
  const userB = await createTenantUser('tenant-b-user@goflow.com', 'tenant-b');

  await runSuite({
    name: '20 — Multi-tenancy',
    tests: [

      // ── A: same process key deployed independently per tenant ─────────────
      {
        name: '[A] Two tenants deploy the same process key under their own tenant',
        async fn() {
          const fs = require('fs');
          const FormData = require('form-data');

          const formA = new FormData();
          formA.append('resources', fs.createReadStream(BPMN));
          formA.append('deployment-name', 'Tenant A Deployment');
          formA.append('tenant-id', 'tenant-a');
          const resA = await superuser.api.post('/engine-rest/deployment/create', formA, { headers: formA.getHeaders() });
          assert(resA.data.deployedProcessDefinitions?.TenantProcess !== undefined, 'expected TenantProcess deployed for tenant-a');

          const formB = new FormData();
          formB.append('resources', fs.createReadStream(BPMN));
          formB.append('deployment-name', 'Tenant B Deployment');
          formB.append('tenant-id', 'tenant-b');
          const resB = await superuser.api.post('/engine-rest/deployment/create', formB, { headers: formB.getHeaders() });
          assert(resB.data.deployedProcessDefinitions?.TenantProcess !== undefined, 'expected TenantProcess deployed for tenant-b');
        },
      },

      // ── B/C/D: isolation across list/search/get, superuser sees all ────────
      {
        name: '[B/C/D] Instances are isolated per tenant; superuser sees both',
        async fn() {
          const instA = await userA.startProcess('TenantProcess', {});
          const instanceIdA = instA.processInstanceId ?? instA.id;

          const instB = await userB.startProcess('TenantProcess', {});
          const instanceIdB = instB.processInstanceId ?? instB.id;

          // --- B: list endpoint (v1) ---
          const listA = await userA.api.get('/engine-rest/process-instance');
          const idsA = (listA.data as any[]).map(i => i.id);
          assert(idsA.includes(instanceIdA), 'tenant A should see its own instance in the list');
          assert(!idsA.includes(instanceIdB), 'tenant A should NOT see tenant B\'s instance in the list');

          const listB = await userB.api.get('/engine-rest/process-instance');
          const idsB = (listB.data as any[]).map(i => i.id);
          assert(idsB.includes(instanceIdB), 'tenant B should see its own instance in the list');
          assert(!idsB.includes(instanceIdA), 'tenant B should NOT see tenant A\'s instance in the list');

          // --- B: search endpoint (v2) ---
          const searchA = await userA.api.post('/v2/process-instances/search', {});
          const searchIdsA = (searchA.data.items as any[]).map(i => i.processInstanceKey);
          assert(searchIdsA.includes(instanceIdA), 'tenant A v2 search should include its own instance');
          assert(!searchIdsA.includes(instanceIdB), 'tenant A v2 search should NOT include tenant B\'s instance');

          // --- C: direct fetch by ID across tenants ---
          const crossFetch = await userA.getProcessInstance(instanceIdB);
          assert(crossFetch === null, 'tenant A fetching tenant B\'s instance by ID directly should 404/null');

          const crossFetchV2 = await userB.api.get(`/v2/process-instances/${instanceIdA}`).catch(e => e.response);
          assert(crossFetchV2.status === 404, `tenant B fetching tenant A's instance via v2 should 404, got ${crossFetchV2.status}`);

          // --- D: superuser sees both ---
          const listSuper = await superuser.api.get('/engine-rest/process-instance');
          const idsSuper = (listSuper.data as any[]).map(i => i.id);
          assert(idsSuper.includes(instanceIdA) && idsSuper.includes(instanceIdB),
            'superuser should see instances from both tenants');

          // --- process-definition listing isolation ---
          const defsA = await userA.api.get('/engine-rest/process-definition?key=TenantProcess');
          for (const def of defsA.data as any[]) {
            assert(def.tenantId === 'tenant-a', `tenant A should only see its own definitions, got tenantId=${def.tenantId}`);
          }
          const defsSuper = await superuser.api.get('/engine-rest/process-definition?key=TenantProcess');
          const tenantIdsSeen = new Set((defsSuper.data as any[]).map(d => d.tenantId));
          assert(tenantIdsSeen.has('tenant-a') && tenantIdsSeen.has('tenant-b'),
            'superuser should see process definitions from both tenants');
        },
      },

    ],
  });
}

run().catch(console.error);
