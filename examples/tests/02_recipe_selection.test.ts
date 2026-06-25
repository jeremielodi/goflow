/**
 * Suite 02 — Exclusive Gateway (XOR)  (BPMN: 02_recipe_selection.bpmn)
 * Source: https://camunda.com/bpmn/reference/ — Exclusive Gateway example
 *
 * Scenario: Christian is hungry. He picks a recipe (pasta, steak, or salad).
 *   • pasta / steak → external worker cooks the meal → user task serves it
 *   • salad         → user task prepares it directly
 *
 * Tests:
 *   A) Hot-meal path (recipe=pasta) — XOR routes to cook_meal service task
 *   B) Salad path   (recipe=salad)  — XOR routes to prepare_salad user task
 *   C) External task failure with retry
 *   D) Variables set by external worker are visible on subsequent tasks
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '02_recipe_selection.bpmn');
const client = new GoFlowClient();

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Recipe Selection Test');

  await runSuite({
    name: '02 — Exclusive Gateway (Recipe Selection)',
    tests: [

      // ── A: hot-meal path ────────────────────────────────────────────────────
      {
        name: '[Pasta path] XOR routes to cook_meal → serve_hot_meal → Meal Served',
        async fn() {
          const inst = await client.startProcess('RecipeSelectionProcess', {
            chef: 'christian',
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          // Step 1: Christian chooses his recipe
          const chooseTask = await client.waitForTask('choose_recipe', instanceId, 10000);
          assert(chooseTask.assignee === 'christian', 'wrong assignee on choose_recipe');
          await client.completeTask(chooseTask.id, { recipe: 'pasta' });

          // Step 2: External worker cooks the pasta
          await client.pollExternalTask('cook_meal', async task => ({
            mealReady: true,
            cookedAt: new Date().toISOString(),
            dish: task.variables?.recipe?.value ?? 'unknown',
          }), 15000);

          // Step 3: Serve the hot meal
          const serveTask = await client.waitForTask('serve_hot_meal', instanceId, 10000);
          assert(serveTask.assignee === 'christian', 'wrong assignee on serve_hot_meal');
          await client.completeTask(serveTask.id);

          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      // ── B: salad path ───────────────────────────────────────────────────────
      {
        name: '[Salad path] XOR routes to prepare_salad user task → Salad Ready',
        async fn() {
          const inst = await client.startProcess('RecipeSelectionProcess', {
            chef: 'christian',
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          const chooseTask = await client.waitForTask('choose_recipe', instanceId, 10000);
          await client.completeTask(chooseTask.id, { recipe: 'salad' });

          // No cook_meal external task — process goes directly to prepare_salad
          const saladTask = await client.waitForTask('prepare_salad', instanceId, 10000);
          assert(saladTask.candidateGroup === 'kitchen', `expected candidateGroup "kitchen", got "${saladTask.candidateGroup}"`);

          // Anyone in kitchen can claim it
          await client.claimTask(saladTask.id, 'sous_chef');
          await client.completeTask(saladTask.id, { saladDone: true });

          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      // ── C: steak path ───────────────────────────────────────────────────────
      {
        name: '[Steak path] XOR also routes recipe=steak to cook_meal service task',
        async fn() {
          const inst = await client.startProcess('RecipeSelectionProcess', {
            chef: 'christian',
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          const chooseTask = await client.waitForTask('choose_recipe', instanceId, 10000);
          await client.completeTask(chooseTask.id, { recipe: 'steak' });

          await client.pollExternalTask('cook_meal', async () => ({
            mealReady: true,
            dish: 'steak',
          }), 15000);

          const serveTask = await client.waitForTask('serve_hot_meal', instanceId, 10000);
          await client.completeTask(serveTask.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      // ── D: external task failure + retry ────────────────────────────────────
      {
        name: '[Failure/Retry] cook_meal worker fails once, succeeds on retry',
        async fn() {
          const inst = await client.startProcess('RecipeSelectionProcess', {
            chef: 'christian',
          });
          const instanceId = inst.processInstanceId ?? inst.id;

          const chooseTask = await client.waitForTask('choose_recipe', instanceId, 10000);
          await client.completeTask(chooseTask.id, { recipe: 'pasta' });

          // First fetch: fail the task with retries=1
          let failed = false;
          const startFail = Date.now();
          while (Date.now() - startFail < 10000) {
            const tasks = await client.fetchAndLock([{ name: 'cook_meal' }]);
            if (tasks.length > 0) {
              // Server decrements retries by 1 internally, so pass 2 to leave 1 retry remaining
              await client.failExternalTask(tasks[0].id, 'Temporary cooking failure', 2);
              failed = true;
              break;
            }
            await sleep(1000);
          }
          assert(failed, 'Expected to fetch cook_meal task for failure');

          // Short wait for retry to become available (retryTimeout=5000ms)
          console.log('       ⏳  Waiting 6s for retry timeout…');
          await sleep(6000);

          // Second fetch: succeed
          await client.pollExternalTask('cook_meal', async () => ({
            mealReady: true,
            retriedAt: new Date().toISOString(),
          }), 15000);

          const serveTask = await client.waitForTask('serve_hot_meal', instanceId, 10000);
          await client.completeTask(serveTask.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

    ],
  });
}

run().catch(console.error);
