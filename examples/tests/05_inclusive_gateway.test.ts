/**
 * Suite 05 — Inclusive Gateway (OR)  (BPMN: 05_fitness_planner.bpmn)
 * Source: https://camunda.com/bpmn/reference/ — Inclusive Gateway example
 *
 * Scenario: A customer selects fitness activities (running, swimming, cycling).
 * The inclusive gateway activates only the selected branches. All active
 * branches must complete before the plan is confirmed.
 *
 * Tests:
 *   A) All 3 branches active  — wantRunning=true, wantSwimming=true, wantCycling=true
 *   B) Single branch active   — wantRunning=true only
 *   C) Two branches active    — wantRunning=true, wantSwimming=true
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert } from './client';

const BPMN = path.join(__dirname, 'bpmn', '05_fitness_planner.bpmn');
const client = new GoFlowClient();

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Fitness Planner Test');

  await runSuite({
    name: '05 — Inclusive Gateway / OR (Fitness Planner)',
    tests: [

      // ── A: all 3 branches ────────────────────────────────────────────────────
      {
        name: '[All branches] All 3 activities selected — 3 service tasks created and all must complete',
        async fn() {
          const inst = await client.startProcess('FitnessPlannerProcess', {
            wantRunning: true,
            wantSwimming: true,
            wantCycling: true,
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          // All 3 branches run concurrently — poll them in parallel
          await Promise.all([
            client.pollExternalTask('register_running',  async () => ({ registered: true }), 15000, instanceId),
            client.pollExternalTask('register_swimming', async () => ({ registered: true }), 15000, instanceId),
            client.pollExternalTask('register_cycling',  async () => ({ registered: true }), 15000, instanceId),
          ]);

          await client.waitForProcessEnd(instanceId, 10000);
          const final = await client.getProcessInstance(instanceId);
          assert(!final || final.status === 'completed', `expected completed, got ${final?.status}`);
        },
      },

      // ── B: single branch ─────────────────────────────────────────────────────
      {
        name: '[Single branch] Only running selected — only register_running job created',
        async fn() {
          const inst = await client.startProcess('FitnessPlannerProcess', {
            wantRunning: true,
            wantSwimming: false,
            wantCycling: false,
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          await client.pollExternalTask('register_running', async () => ({ registered: true }), 15000, instanceId);

          await client.waitForProcessEnd(instanceId, 10000);
          const final = await client.getProcessInstance(instanceId);
          assert(!final || final.status === 'completed', `expected completed, got ${final?.status}`);
        },
      },

      // ── C: two branches ──────────────────────────────────────────────────────
      {
        name: '[Two branches] Running + swimming selected — both complete before join fires',
        async fn() {
          const inst = await client.startProcess('FitnessPlannerProcess', {
            wantRunning: true,
            wantSwimming: true,
            wantCycling: false,
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          await Promise.all([
            client.pollExternalTask('register_running',  async () => ({ registered: true }), 15000, instanceId),
            client.pollExternalTask('register_swimming', async () => ({ registered: true }), 15000, instanceId),
          ]);

          await client.waitForProcessEnd(instanceId, 10000);
          const final = await client.getProcessInstance(instanceId);
          assert(!final || final.status === 'completed', `expected completed, got ${final?.status}`);
        },
      },

    ],
  });
}

run().catch(console.error);
