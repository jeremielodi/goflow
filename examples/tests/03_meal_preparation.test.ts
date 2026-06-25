/**
 * Suite 03 — Parallel Gateway (AND)  (BPMN: 03_meal_preparation.bpmn)
 * Source: https://camunda.com/bpmn/reference/ — Parallel Gateway example
 *
 * Scenario: To save time, pasta is cooked (service task) AND salad is prepared
 * (user task) simultaneously. The AND join waits for BOTH before allowing
 * Christian to eat.
 *
 * Tests:
 *   A) Both branches complete normally — join fires, eat_meal appears
 *   B) Service task branch completes first — join does NOT fire until user task done
 *   C) User task branch completes first  — join does NOT fire until service task done
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '03_meal_preparation.bpmn');
const client = new GoFlowClient();

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Meal Preparation Test');

  await runSuite({
    name: '03 — Parallel Gateway (Meal Preparation)',
    tests: [

      // ── A: both branches complete, join fires ───────────────────────────────
      {
        name: '[Happy path] Both branches complete → AND join → eat_meal appears',
        async fn() {
          const inst = await client.startProcess('MealPrepProcess');
          const instanceId = inst.processInstanceId ?? inst.id;

          // Handle both branches concurrently (they run in parallel inside the engine)
          const cookDone = client.pollExternalTask('cook_pasta', async () => ({
            pastaReady: true,
            cookedAt: new Date().toISOString(),
          }), 20000);

          const saladTask = await client.waitForTask('prepare_salad', instanceId, 10000);
          assert(saladTask.candidateGroup === 'kitchen', 'expected candidateGroup "kitchen"');
          await client.claimTask(saladTask.id, 'sous_chef');
          await client.completeTask(saladTask.id, { saladReady: true });

          await cookDone; // ensure pasta worker also finished

          // Both done — AND join should have released eat_meal
          const eatTask = await client.waitForTask('eat_meal', instanceId, 15000);
          assert(eatTask !== undefined, 'eat_meal task should appear after join');

          await client.claimTask(eatTask.id, 'christian');
          await client.completeTask(eatTask.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      // ── B: service task finishes first; join must wait for user task ────────
      {
        name: '[Service first] cook_pasta completes instantly — join waits for user task',
        async fn() {
          const inst = await client.startProcess('MealPrepProcess');
          const instanceId = inst.processInstanceId ?? inst.id;

          // Complete cook_pasta immediately
          await client.pollExternalTask('cook_pasta', async () => ({ pastaReady: true }), 15000);

          // eat_meal must NOT appear yet — salad not done
          await sleep(2000);
          const tasksBeforeSalad = await client.getTasks(instanceId);
          const prematureEat = tasksBeforeSalad.find(t => t.taskDefinitionKey === 'eat_meal');
          assert(!prematureEat, 'eat_meal should NOT appear before salad is done');

          // Now complete salad
          const saladTask = await client.waitForTask('prepare_salad', instanceId, 10000);
          await client.completeTask(saladTask.id, { saladReady: true });

          // Now join fires
          const eatTask = await client.waitForTask('eat_meal', instanceId, 10000);
          assert(eatTask !== undefined, 'eat_meal should appear after both branches finish');
          await client.completeTask(eatTask.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

      // ── C: user task finishes first; join must wait for service task ────────
      {
        name: '[User first] prepare_salad completes instantly — join waits for cook_pasta',
        async fn() {
          const inst = await client.startProcess('MealPrepProcess');
          const instanceId = inst.processInstanceId ?? inst.id;

          // Complete salad immediately
          const saladTask = await client.waitForTask('prepare_salad', instanceId, 10000);
          await client.completeTask(saladTask.id, { saladReady: true });

          // eat_meal must NOT appear yet — pasta not done
          await sleep(2000);
          const tasksBeforePasta = await client.getTasks(instanceId);
          const prematureEat = tasksBeforePasta.find(t => t.taskDefinitionKey === 'eat_meal');
          assert(!prematureEat, 'eat_meal should NOT appear before pasta is done');

          // Now complete cook_pasta
          await client.pollExternalTask('cook_pasta', async () => ({ pastaReady: true }), 15000);

          // Now join fires
          const eatTask = await client.waitForTask('eat_meal', instanceId, 10000);
          assert(eatTask !== undefined, 'eat_meal should appear after both branches finish');
          await client.completeTask(eatTask.id);
          await client.waitForProcessEnd(instanceId, 10000);
        },
      },

    ],
  });
}

run().catch(console.error);
