/**
 * Suite 04 — Intermediate Timer Catch Event  (BPMN: 04_oven_timer.bpmn)
 * Source: https://camunda.com/bpmn/reference/ — Intermediate Timer Event example
 *
 * Scenario: Preparing a frozen pizza. The oven must heat up (PT20S) before
 * placing the pizza, then bake (PT30S) before removing it.
 * Two intermediate catch timers gate the process.
 *
 * Tests:
 *   A) Full happy path — process advances through both timers automatically
 *   B) First timer blocks — place_pizza does NOT appear before heat-up timer fires
 *   C) Second timer blocks — remove_pizza does NOT appear before bake timer fires
 *   D) Process variables survive across timer boundaries
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '04_oven_timer.bpmn');
const client = new GoFlowClient();

// PT20S + PT30S = 50s of timer waiting in the BPMN.
// Adjust those durations in the BPMN file to speed up local runs.
const HEAT_TIMER_MS  = 20_000; // matches PT20S
const BAKE_TIMER_MS  = 30_000; // matches PT30S
const TIMER_BUFFER   =  5_000; // extra wait to account for engine polling latency

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Oven Timer Test');

  await runSuite({
    name: '04 — Intermediate Timer (Frozen Pizza)',
    tests: [

      // ── A: full happy path ──────────────────────────────────────────────────
      {
        name: '[Full path] turn_on_oven → heat timer → place_pizza → bake timer → remove_pizza → done',
        async fn() {
          const inst = await client.startProcess('OvenTimerProcess', { cook: 'christian' });
          const instanceId = inst.processInstanceId ?? inst.id;

          // Step 1: turn_on_oven external task
          await client.pollExternalTask('turn_on_oven', async () => ({
            ovenOn: true,
            temperature: 180,
          }), 10000);

          // Step 2: wait for heat-up intermediate timer (PT20S)
          console.log(`       ⏳  Waiting ${(HEAT_TIMER_MS + TIMER_BUFFER) / 1000}s for heat-up timer…`);
          await sleep(HEAT_TIMER_MS + TIMER_BUFFER);

          // Step 3: place_pizza user task should now be active
          const placeTask = await client.waitForTask('place_pizza', instanceId, 10000);
          assert(placeTask.assignee === 'christian', `expected assignee "christian", got "${placeTask.assignee}"`);
          await client.completeTask(placeTask.id, { pizzaIn: true });

          // Step 4: wait for bake intermediate timer (PT30S)
          console.log(`       ⏳  Waiting ${(BAKE_TIMER_MS + TIMER_BUFFER) / 1000}s for bake timer…`);
          await sleep(BAKE_TIMER_MS + TIMER_BUFFER);

          // Step 5: remove_pizza user task should now be active
          const removeTask = await client.waitForTask('remove_pizza', instanceId, 10000);
          assert(removeTask.assignee === 'christian', `expected assignee "christian", got "${removeTask.assignee}"`);
          await client.completeTask(removeTask.id, { pizzaOut: true });

          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      // ── B: first timer blocks place_pizza ───────────────────────────────────
      {
        name: '[Heat-up timer] place_pizza does NOT appear before PT20S elapses',
        async fn() {
          const inst = await client.startProcess('OvenTimerProcess', { cook: 'christian' });
          const instanceId = inst.processInstanceId ?? inst.id;

          await client.pollExternalTask('turn_on_oven', async () => ({ ovenOn: true }), 10000);

          // Immediately after the service task — timer should not have fired yet
          await sleep(3000);
          const earlyTasks = await client.getTasks(instanceId);
          const tooEarly = earlyTasks.find(t => t.taskDefinitionKey === 'place_pizza');
          assert(!tooEarly, 'place_pizza should NOT appear before heat-up timer fires');

          // Now wait for the timer to fire
          console.log(`       ⏳  Waiting ${(HEAT_TIMER_MS + TIMER_BUFFER) / 1000}s…`);
          await sleep(HEAT_TIMER_MS + TIMER_BUFFER - 3000);

          const placeTask = await client.waitForTask('place_pizza', instanceId, 10000);
          assert(placeTask !== undefined, 'place_pizza must appear after timer');

          // Clean up: complete the rest of the process
          await client.completeTask(placeTask.id);
          await sleep(BAKE_TIMER_MS + TIMER_BUFFER);
          const removeTask = await client.waitForTask('remove_pizza', instanceId, 10000);
          await client.completeTask(removeTask.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      // ── C: second timer blocks remove_pizza ─────────────────────────────────
      {
        name: '[Bake timer] remove_pizza does NOT appear before PT30S elapses after place_pizza',
        async fn() {
          const inst = await client.startProcess('OvenTimerProcess', { cook: 'christian' });
          const instanceId = inst.processInstanceId ?? inst.id;

          await client.pollExternalTask('turn_on_oven', async () => ({ ovenOn: true }), 10000);

          console.log(`       ⏳  Waiting for heat-up (${(HEAT_TIMER_MS + TIMER_BUFFER) / 1000}s)…`);
          await sleep(HEAT_TIMER_MS + TIMER_BUFFER);

          const placeTask = await client.waitForTask('place_pizza', instanceId, 10000);
          await client.completeTask(placeTask.id, { pizzaIn: true });

          // Immediately after placing — bake timer should not have fired yet
          await sleep(3000);
          const earlyTasks = await client.getTasks(instanceId);
          const tooEarly = earlyTasks.find(t => t.taskDefinitionKey === 'remove_pizza');
          assert(!tooEarly, 'remove_pizza should NOT appear before bake timer fires');

          // Wait for bake timer
          console.log(`       ⏳  Waiting for bake (${(BAKE_TIMER_MS + TIMER_BUFFER) / 1000}s)…`);
          await sleep(BAKE_TIMER_MS + TIMER_BUFFER - 3000);

          const removeTask = await client.waitForTask('remove_pizza', instanceId, 10000);
          assert(removeTask !== undefined, 'remove_pizza must appear after bake timer');
          await client.completeTask(removeTask.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      // ── D: variables survive timer boundaries ────────────────────────────────
      {
        name: '[Variables] variables set by turn_on_oven worker are accessible after timers',
        async fn() {
          const inst = await client.startProcess('OvenTimerProcess', {
            cook: 'christian',
            pizzaType: 'margherita',
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          await client.pollExternalTask('turn_on_oven', async () => ({
            ovenOn: true,
            temperature: 180,
            startedAt: new Date().toISOString(),
          }), 10000);

          console.log(`       ⏳  Waiting for heat-up (${(HEAT_TIMER_MS + TIMER_BUFFER) / 1000}s)…`);
          await sleep(HEAT_TIMER_MS + TIMER_BUFFER);

          // Variables set by the worker should still be present
          const vars = await client.getProcessVariables(instanceId);
          assert((vars as any).ovenOn?.value === true || (vars as any).ovenOn === true,
            'ovenOn variable should be true');
          assert((vars as any).temperature?.value === 180 || (vars as any).temperature === 180,
            'temperature variable should be 180');

          // Complete the rest
          const placeTask = await client.waitForTask('place_pizza', instanceId, 5000);
          await client.completeTask(placeTask.id);
          console.log(`       ⏳  Waiting for bake (${(BAKE_TIMER_MS + TIMER_BUFFER) / 1000}s)…`);
          await sleep(BAKE_TIMER_MS + TIMER_BUFFER);
          const removeTask = await client.waitForTask('remove_pizza', instanceId, 5000);
          await client.completeTask(removeTask.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

    ],
  });
}

run().catch(console.error);
