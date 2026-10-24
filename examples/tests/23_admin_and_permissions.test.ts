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
 *   C) Role management hardening (added in a later pass, after auditing the
 *      feature): role read routes require permission, editing a role's
 *      label persists, and deleting a role still assigned to a user is
 *      rejected with 409 instead of silently cascading their permissions
 *      away. This also regression-covers a real bug caught only by
 *      actually calling these endpoints: models.Role's `db:"isDefault"`
 *      struct tag didn't match the real Postgres column (`isdefault`, no
 *      separator), so GetRole/ListRoles/GetDefaultRole were crashing with
 *      a 500 on every call.
 *   D) User lifecycle hardening — DeleteUser is a soft-delete (is_active
 *      flips to false, the row survives) rather than a hard DELETE, which
 *      used to crash with a raw FK-violation error for any user who'd
 *      ever deployed/started an instance/been audit-logged. Admin
 *      password reset (a previously-unused CAN_RESET_PASSWORD action)
 *      actually persists — verified by logging in with the new password,
 *      not just checking for a 200.
 *   E) Per-instance permission-filtering audit (added after a user
 *      directive to verify every process/task/historic endpoint respects
 *      the caller's grants, not just list/start on process definitions).
 *      Test B already covered process-definition list/start; this covers
 *      everything downstream of a running instance: process instance
 *      detail/variables/suspend, historic instance/activity lists, audit
 *      logs + token history, incidents-by-instance, analytics'
 *      per-key breakdown, and task lists — all denied (404, matching
 *      GetDefinition's precedent of not revealing a restricted resource
 *      exists) or filtered out for a caller with no grant on the process.
 *      Also regression-covers a real, more severe bug found while writing
 *      this test: `/token-history` was accidentally listed in
 *      pkg/middleware.PublicRoutes, and that allowlist matches by
 *      substring — so the endpoint bypassed authentication ENTIRELY, not
 *      just the per-instance ACL, for anyone with no token at all.
 */
import * as path from 'path';
import { execSync } from 'child_process';
import { GoFlowClient, runSuite, assert } from './client';

function psql(sql: string): string {
  return execSync(`docker compose exec -T postgres psql -U goflow -d goflow -tAc "${sql}"`, {
    cwd: path.join(__dirname, '..', '..'),
  }).toString().trim();
}

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

      // ── C: role management hardening ────────────────────────────────────────
      {
        name: '[C] Role read routes are gated, edit persists, delete-while-in-use is rejected',
        async fn() {
          const { id: userId, client: userClient } = await createUser('role-hardening-user@goflow.com');

          // A fresh user has no CAN_EDIT_ROLE/CAN_MANAGE_USER — role read
          // routes must reject them (these used to have no guard at all).
          const listDenied = await userClient.api.get('/roles').catch(e => e.response);
          assert(listDenied.status === 403, `expected 403 listing roles without permission, got ${listDenied.status}`);
          const userRolesDenied = await userClient.api.get(`/users/${userId}/roles`).catch(e => e.response);
          assert(userRolesDenied.status === 403, `expected 403 reading a user's roles without permission, got ${userRolesDenied.status}`);

          // Create + edit — also regression-covers the isDefault struct-tag
          // bug: GetRole used to 500 on every call before that fix.
          const roleRes = await superuser.api.post('/roles', { label: 'hardening-role', is_default: 0 });
          const roleId = roleRes.data.role.id;
          await superuser.api.put(`/roles/${roleId}`, { label: 'hardening-role-renamed', is_default: 0 });
          const getRole = await superuser.api.get(`/roles/${roleId}`);
          assert(getRole.data.role.label === 'hardening-role-renamed', `expected updated label, got ${JSON.stringify(getRole.data)}`);

          // Assign to the user, then deletion must be rejected (409) while
          // still assigned — no silent permission-stripping cascade.
          await superuser.api.post(`/users/${userId}/roles/${roleId}`);
          const deleteWhileInUse = await superuser.api.delete(`/roles/${roleId}`).catch(e => e.response);
          assert(deleteWhileInUse.status === 409, `expected 409 deleting a role still assigned to a user, got ${deleteWhileInUse.status}`);

          // Unassign, then deletion succeeds.
          await superuser.api.delete(`/users/${userId}/roles/${roleId}`);
          await superuser.api.delete(`/roles/${roleId}`);
          const getDeleted = await superuser.api.get(`/roles/${roleId}`).catch(e => e.response);
          assert(getDeleted.status === 404, `expected 404 for a deleted role, got ${getDeleted.status}`);
        },
      },

      // ── D: user lifecycle hardening ──────────────────────────────────────────
      {
        name: '[D] DeleteUser soft-deletes (is_active=false, row survives); admin password reset persists',
        async fn() {
          const { id: userId, client: userClient } = await createUser('lifecycle-user@goflow.com');

          // Admin resets the password — verified by actually logging in
          // with the new one (and confirming the old one no longer works),
          // not just checking for a 200.
          await superuser.api.put(`/users/${userId}/reset-password`, { new_password: 'resetpass456' });
          const freshClient = new GoFlowClient();
          const loginNew = await freshClient.login('lifecycle-user@goflow.com', 'resetpass456');
          assert(!!loginNew.access_token, 'expected login with the admin-reset password to succeed');
          const loginOld = await freshClient.login('lifecycle-user@goflow.com', 'password123').catch(e => e.response);
          assert(loginOld?.status === 401, `expected the old password to be rejected after reset, got ${loginOld?.status}`);

          // DeleteUser soft-deletes: the row survives (is_active=false),
          // deployments/instances/audit_logs referencing it are untouched
          // (this used to hard-DELETE and crash with an FK-violation 500
          // for any user with such history).
          await superuser.api.delete(`/users/${userId}`);
          const getAfterDelete = await superuser.api.get(`/users/${userId}`);
          const deletedUser = getAfterDelete.data.user ?? getAfterDelete.data;
          assert(deletedUser.is_active === false, `expected the user row to survive with is_active=false, got ${JSON.stringify(deletedUser)}`);

          const loginDeactivated = await userClient.login('lifecycle-user@goflow.com', 'resetpass456').catch(e => e.response);
          assert(loginDeactivated?.status === 403, `expected a deactivated user's login to be rejected with 403, got ${loginDeactivated?.status}`);
        },
      },

      // ── E: per-instance permission-filtering audit ──────────────────────────
      {
        name: '[E] Every process-instance/task/historic/audit/incident/analytics endpoint respects per-instance grants',
        async fn() {
          await deploy(superuser);
          const inst = await superuser.startProcess('PermissionProcess', {});
          const instanceId = (inst as any).processInstanceId ?? (inst as any).id;

          // Two distinct users: granteeId makes the process "restricted" in
          // the first place (IsRestricted), stranger gets NO grant at all.
          const { id: strangerId, client: stranger } = await createUser('audit-stranger@goflow.com');
          const { id: granteeId, client: granteeClient } = await createUser('audit-grantee@goflow.com');
          await superuser.api.post('/engine-rest/process-definition/key/PermissionProcess/permissions', {
            granteeType: 'user', granteeId, permission: 'VIEW',
          });

          const denied404 = async (p: Promise<any>) => {
            const res = await p.catch((e: any) => e.response);
            return res?.status === 404;
          };

          assert(await denied404(stranger.api.get(`/engine-rest/process-instance/${instanceId}`)), 'GetProcessInstance denied without VIEW grant');
          assert(await denied404(stranger.api.get(`/engine-rest/process-instance/${instanceId}/variables`)), 'GetProcessVariables denied without VIEW grant');
          assert(await denied404(stranger.api.put(`/engine-rest/process-instance/${instanceId}/suspended`, { suspended: true })), 'SuspendProcess denied without MANAGE grant');
          assert(await denied404(stranger.api.get(`/engine-rest/audit/process/${instanceId}`)), 'GetProcessAuditLogs denied without VIEW grant');
          // Regression: this endpoint used to bypass authentication
          // entirely (an accidental substring match in
          // pkg/middleware.PublicRoutes), not just the per-instance ACL.
          assert(await denied404(stranger.api.get(`/engine-rest/audit/process/${instanceId}/token-history`)), 'GetTokenHistory denied without VIEW grant');
          assert(await denied404(stranger.api.get(`/engine-rest/process-instance/${instanceId}/incidents`)), 'incidents-by-instance denied without VIEW grant');
          assert(await denied404(stranger.api.get(`/engine-rest/analytics/process-stats?processKey=PermissionProcess`)), 'analytics single-key query denied without VIEW grant');

          const strangerHistList = await stranger.api.get('/engine-rest/history/process-instance?processKey=PermissionProcess');
          assert(!strangerHistList.data.some((h: any) => h.id === instanceId), 'restricted instance filtered out of historic list');

          const strangerActivities = await stranger.api.get(`/engine-rest/history/activity-instance?processInstanceId=${instanceId}`);
          assert(strangerActivities.data.length === 0, 'historic activities filtered out for a restricted instance');

          const strangerAllStats = await stranger.api.get('/engine-rest/analytics/process-stats');
          assert(!strangerAllStats.data.byProcessKey.some((s: any) => s.processKey === 'PermissionProcess'), 'restricted key filtered out of the all-keys analytics view');

          // GET /tasks is also gated by the route-level CAN_READ_TASKS
          // action; a fresh user has none by default. Grant it so this
          // assertion tests the per-instance filter, not that unrelated gate.
          const taskReaderRole = await superuser.api.post('/roles', { label: 'e-task-reader', is_default: 0 });
          const actionsForE = await superuser.api.get('/actions');
          const readTasksAction = (actionsForE.data.actions as any[]).find(a => a.code === 'CAN_READ_TASKS');
          await superuser.api.post(`/roles/${taskReaderRole.data.role.id}/actions/${readTasksAction.id}`);
          await superuser.api.post(`/users/${strangerId}/roles/${taskReaderRole.data.role.id}`);

          const strangerTasks = await stranger.api.get(`/tasks?processInstanceId=${instanceId}`);
          assert(strangerTasks.data.length === 0, 'task list filtered out for a restricted instance');

          // The granted user (VIEW only) can see it.
          const granteeGet = await granteeClient.api.get(`/engine-rest/process-instance/${instanceId}`);
          assert(granteeGet.status === 200, `expected the VIEW-granted user to see the instance, got ${granteeGet.status}`);
        },
      },

      // ── F: the /v2 searches filter by per-instance grants too ───────────────
      {
        name: '[F] /v2 user-task, incident, flow-node and variable searches respect per-instance grants',
        async fn() {
          await deploy(superuser);
          const inst = await superuser.startProcess('PermissionProcess', { secret: 'only-for-viewers' });
          const instanceId = (inst as any).processInstanceId ?? (inst as any).id;
          // PermissionProcess raises no incident on its own: add one so the
          // incident search has a row to hide.
          psql(`INSERT INTO incidents (process_instance_id, incident_type, activity_id, error_message) VALUES ('${instanceId}', 'executionError', 'reviewTask', 'v2 permission test')`);

          const { id: strangerId, client: stranger } = await createUser(`v2-stranger-${Date.now()}@goflow.com`);
          const { id: granteeId, client: grantee } = await createUser(`v2-grantee-${Date.now()}@goflow.com`);
          await superuser.api.post('/engine-rest/process-definition/key/PermissionProcess/permissions', {
            granteeType: 'user', granteeId, permission: 'VIEW',
          });
          // /v2/user-tasks/search is also gated by CAN_READ_TASKS: give it to
          // both, so this tests the per-instance filter.
          const role = await superuser.api.post('/roles', { label: `f-task-reader-${Date.now()}`, is_default: 0 });
          const actions = await superuser.api.get('/actions');
          const readTasks = (actions.data.actions as any[]).find(a => a.code === 'CAN_READ_TASKS');
          await superuser.api.post(`/roles/${role.data.role.id}/actions/${readTasks.id}`);
          await superuser.api.post(`/users/${strangerId}/roles/${role.data.role.id}`);
          await superuser.api.post(`/users/${granteeId}/roles/${role.data.role.id}`);

          const search = (c: GoFlowClient, what: string, filter: Record<string, unknown>) =>
            c.api.post(`/v2/${what}/search`, { filter }).catch((e: any) => e.response);
          const keys = (res: any) => (res.data.items as any[]).map(i => i.processInstanceKey);

          // Stranger: rows hidden, single-instance searches 404.
          const sTasks = await search(stranger, 'user-tasks', { processInstanceKey: instanceId });
          assert(sTasks.status === 200 && sTasks.data.items.length === 0, `stranger should see no task, got ${JSON.stringify(sTasks.data)}`);
          const sAllTasks = await search(stranger, 'user-tasks', {});
          assert(!keys(sAllTasks).includes(instanceId), 'unfiltered task search must hide the restricted instance');
          const sInc = await search(stranger, 'incidents', {});
          assert(!keys(sInc).includes(instanceId), 'incident search must hide the restricted instance');
          const sFlow = await search(stranger, 'flownode-instances', { processInstanceKey: instanceId });
          assert(sFlow.status === 404, `stranger flow-node search: expected 404, got ${sFlow.status}`);
          const sVars = await search(stranger, 'variables', { processInstanceKey: instanceId });
          assert(sVars.status === 404, `stranger variable search: expected 404, got ${sVars.status}`);

          // Grantee (VIEW): sees all of it.
          const gTasks = await search(grantee, 'user-tasks', { processInstanceKey: instanceId });
          assert(gTasks.data.items.length === 1, `grantee should see the task, got ${JSON.stringify(gTasks.data)}`);
          const gInc = await search(grantee, 'incidents', { processInstanceKey: instanceId });
          assert(gInc.data.items.length === 1, `grantee should see the incident, got ${JSON.stringify(gInc.data)}`);
          const gFlow = await search(grantee, 'flownode-instances', { processInstanceKey: instanceId });
          assert(gFlow.status === 200 && gFlow.data.items.length > 0, `grantee flow-node search failed: ${gFlow.status}`);
          const gVars = await search(grantee, 'variables', { processInstanceKey: instanceId, name: 'secret' });
          assert(gVars.status === 200 && gVars.data.items[0]?.value === 'only-for-viewers', `grantee variable search failed: ${JSON.stringify(gVars.data)}`);

          await superuser.api.delete(`/engine-rest/process-instance/${instanceId}`);
        },
      },

    ],
  });
}

run().catch(console.error);
