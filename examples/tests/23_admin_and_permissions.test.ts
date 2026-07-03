/**
 * Suite 23 — Admin UI backend (Users/Roles) + per-resource process permissions
 *
 * Roadmap item 5 ("Admin UI ... Users/Roles have a full API already, no
 * page") plus a new per-resource ACL for process definitions. The frontend
 * pages (pages/admin/Users.tsx, Roles.tsx, the Permissions modal on
 * ProcessDefinitions.tsx) call the exact same endpoints exercised here.
 *
 * Tests:
 *   A) Users/Roles CRUD round-trip: create a role, assign an action to it,
 *      assign the role to a fresh user, verify the gated action becomes
 *      reachable, then revoke and verify it's denied again.
 *   B) Process permissions — a process is unrestricted by default (open to
 *      any authenticated user); granting VIEW+START to one user makes it
 *      restricted, so a third, ungranted user can no longer see/start it,
 *      while the granted user can. MANAGE (deploying a new version) still
 *      requires MANAGE specifically, not just VIEW/START.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert } from './client';

const BPMN = path.join(__dirname, 'bpmn', '23_permission_process.bpmn');
const superuser = new GoFlowClient();

async function createUser(email: string): Promise<{ id: string; client: GoFlowClient }> {
  const res = await superuser.api.post('/users', {
    email,
    full_name: email,
    password: 'password123',
  });
  const client = new GoFlowClient();
  await client.login(email, 'password123');
  return { id: res.data.user.id, client };
}

async function deploy(client: GoFlowClient): Promise<void> {
  const fs = require('fs');
  const FormData = require('form-data');
  const form = new FormData();
  form.append('resources', fs.createReadStream(BPMN));
  form.append('deployment-name', 'Permission Process Deployment');
  await client.api.post('/engine-rest/deployment/create', form, { headers: form.getHeaders() });
}

async function run() {
  await superuser.loginAsSuperUser();

  await runSuite({
    name: '23 — Admin UI backend (Users/Roles) + process permissions',
    tests: [

      // ── A: Users/Roles CRUD round-trip ─────────────────────────────────────
      {
        name: '[A] Create role, assign action, assign to user, verify gated access, then revoke',
        async fn() {
          const { id: userId, client: userClient } = await createUser('crud-user@goflow.com');

          // Fresh user (default "user" role, no actions) — GET /tasks is
          // gated by CAN_READ_TASKS, should be forbidden.
          const before = await userClient.api.get('/tasks').catch(e => e.response);
          assert(before.status === 403, `expected 403 before role grant, got ${before.status}`);

          const roleRes = await superuser.api.post('/roles', { label: 'task-reader', is_default: 0 });
          const roleId = roleRes.data.role.id;

          const actionsRes = await superuser.api.get('/actions');
          const action = (actionsRes.data.actions as any[]).find(a => a.code === 'CAN_READ_TASKS');
          assert(!!action, 'expected CAN_READ_TASKS to be a seeded action');

          await superuser.api.post(`/roles/${roleId}/actions/${action.id}`);
          await superuser.api.post(`/users/${userId}/roles/${roleId}`);

          const after = await userClient.api.get('/tasks');
          assert(after.status === 200, `expected 200 after role grant, got ${after.status}`);

          await superuser.api.delete(`/users/${userId}/roles/${roleId}`);
          const afterRevoke = await userClient.api.get('/tasks').catch(e => e.response);
          assert(afterRevoke.status === 403, `expected 403 after role revoke, got ${afterRevoke.status}`);
        },
      },

      // ── B: per-resource process permissions ─────────────────────────────────
      {
        name: '[B] Unrestricted by default; granting VIEW+START restricts to grantees; MANAGE required to redeploy',
        async fn() {
          await deploy(superuser);

          const { id: userAId, client: userA } = await createUser('perm-user-a@goflow.com');
          const { client: userB } = await createUser('perm-user-b@goflow.com');

          // Regression: unrestricted — any authenticated user can see/start it.
          const listB = await userB.api.get('/engine-rest/process-definition?key=PermissionProcess');
          assert(listB.data.length > 0, 'expected unrestricted process to be visible to an unrelated user');
          const startB = await userB.startProcess('PermissionProcess', {});
          assert(!!(startB.processInstanceId ?? (startB as any).id), 'expected unrestricted process to be startable by an unrelated user');

          // Restrict: grant userA VIEW + START.
          await superuser.api.post('/engine-rest/process-definition/key/PermissionProcess/permissions', {
            granteeType: 'user', granteeId: userAId, permission: 'VIEW',
          });
          await superuser.api.post('/engine-rest/process-definition/key/PermissionProcess/permissions', {
            granteeType: 'user', granteeId: userAId, permission: 'START',
          });

          // A third, ungranted user created AFTER restriction — must be denied.
          const { client: userC } = await createUser('perm-user-c@goflow.com');
          const listC = await userC.api.get('/engine-rest/process-definition?key=PermissionProcess');
          assert(listC.data.length === 0, 'expected a restricted process to be filtered out of an ungranted user\'s list');
          const startC = await userC.startProcess('PermissionProcess', {}).catch(e => e.response);
          assert(startC.status === 403, `expected 403 starting a restricted process without a grant, got ${startC.status}`);

          // The granted user can still see and start it.
          const listA = await userA.api.get('/engine-rest/process-definition?key=PermissionProcess');
          assert(listA.data.length > 0, 'expected the granted user to still see the restricted process');
          const startA = await userA.startProcess('PermissionProcess', {});
          assert(!!(startA.processInstanceId ?? (startA as any).id), 'expected the granted user to be able to start the restricted process');

          // VIEW+START does not imply MANAGE — userA can't deploy a new version.
          const redeployA = await deploy(userA).catch(e => e.response);
          assert(redeployA && redeployA.status === 403, `expected 403 redeploying without MANAGE, got ${redeployA?.status}`);

          // The global CAN_MANAGE_DEPLOY_PROCESS bypass (superuser) can still redeploy.
          await deploy(superuser);
        },
      },

    ],
  });
}

run().catch(console.error);
