/**
 * run_all.ts — Master test runner
 *
 * Starts Docker (postgres + app), runs all suites sequentially, then prints
 * a consolidated report.
 *
 * Usage:
 *   npx ts-node run_all.ts            # run everything
 *   npx ts-node run_all.ts --no-timers  # skip the slow timer suites (03, 04)
 *
 * Individual suites:
 *   npx ts-node 00_auth.test.ts
 *   npx ts-node 01_pizza_delivery.test.ts
 *   npx ts-node 02_recipe_selection.test.ts
 *   npx ts-node 03_meal_preparation.test.ts
 *   npx ts-node 04_oven_timer.test.ts
 */
import { execSync, spawn } from 'child_process';
import * as path from 'path';
import { GoFlowClient, sleep } from './client';

const BASE_URL = 'http://localhost:8080';
const noTimers = process.argv.includes('--no-timers');

// ─── Wait for app to be healthy ───────────────────────────────────────────────

async function waitForApp(timeoutMs = 120_000): Promise<void> {
  const axios = (await import('axios')).default;
  const start = Date.now();
  process.stdout.write('  Waiting for GoFlow app');
  while (Date.now() - start < timeoutMs) {
    try {
      await axios.get(`${BASE_URL}/health`, { timeout: 2000 });
      console.log(' ✅\n');
      return;
    } catch {
      process.stdout.write('.');
      await sleep(3000);
    }
  }
  throw new Error('GoFlow app did not become healthy within timeout');
}

// ─── Run a test file as a child process ───────────────────────────────────────

function runFile(file: string): Promise<number> {
  return new Promise(resolve => {
    const child = spawn(
      process.execPath,
      ['-r', 'ts-node/register', path.join(__dirname, file)],
      // Transpile only: type-checking every suite on launch is what made a
      // full run slow. Type-check once with "npm run typecheck" instead.
      { stdio: 'inherit', env: { TS_NODE_TRANSPILE_ONLY: 'true', ...process.env } }
    );
    child.on('close', code => resolve(code ?? 1));
  });
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('\n' + '═'.repeat(60));
  console.log('  GoFlow Integration Test Suite');
  console.log('═'.repeat(60));

  // 1. Tear down existing containers + volumes, then rebuild fresh
  console.log('\n📦  Tearing down existing Docker volumes…');
  try {
    execSync('docker compose down -v --remove-orphans', {
      cwd: path.join(__dirname, '..', '..'),
      stdio: 'inherit',
    });
  } catch {
    // nothing running — that's fine
  }

  console.log('📦  Starting Docker services (fresh build)…');
  execSync('docker compose up -d --build --wait', {
    cwd: path.join(__dirname, '..', '..'),
    stdio: 'inherit',
  });

  // 2. Wait until the app responds
  await waitForApp();

  // 3. Quick connectivity check
  const client = new GoFlowClient();
  await client.loginAsSuperUser();
  console.log('  Auth OK — superuser login successful\n');

  // 4. Run suites
  const suites = [
    '00_auth.test.ts',
    '01_pizza_delivery.test.ts',
    '02_recipe_selection.test.ts',
    ...(noTimers ? [] : ['03_meal_preparation.test.ts', '04_oven_timer.test.ts']),
    '05_inclusive_gateway.test.ts',
    '06_error_boundary.test.ts',
    '07_incident_management.test.ts',
    '08_process_versioning.test.ts',
    '09_history.test.ts',
    '10_message_events.test.ts',
    '11_signal_events.test.ts',
    '12_event_based_gateway.test.ts',
    '13_call_activity.test.ts',
    '14_task_listener.test.ts',
    '15_v2_api.test.ts',
    '16_zeebe_extensions.test.ts',
    '17_dmn.test.ts',
    '18_tasklist.test.ts',
    '19_operate.test.ts',
    '20_multitenancy.test.ts',
    '21_oidc.test.ts',
    '22_audit_trail.test.ts',
    '23_admin_and_permissions.test.ts',
    '24_grpc_gateway.test.ts',
    '25_analytics.test.ts',
    '26_eventlog_shadow.test.ts',
    '27_partition_engine.test.ts',
    '28_partition_diamond.test.ts',
    '29_partition_chain.test.ts',
    '30_partition_dmn.test.ts',
    '31_partition_failjob.test.ts',
    '32_partition_timer.test.ts',
    '33_partition_terminate_modify.test.ts',
    '34_partition_parallel.test.ts',
    '35_subprocess.test.ts',
    '36_bpmn_elements.test.ts',
    '37_event_subprocess.test.ts',
    '38_boundary_events.test.ts',
    '39_feel.test.ts',
    '40_grpc_more.test.ts',
    '41_escalation.test.ts',
    '42_conditional.test.ts',
    '43_compensation.test.ts',
    '44_script_task.test.ts',
    '45_messages.test.ts',
    '46_start_events.test.ts',
    '47_timers.test.ts',
  ];

  const exitCodes: number[] = [];
  for (const suite of suites) {
    const code = await runFile(suite);
    exitCodes.push(code);
  }

  // 5. Final summary
  const failed = exitCodes.filter(c => c !== 0).length;
  console.log('\n' + '═'.repeat(60));
  if (failed === 0) {
    console.log(`  ✅  All ${suites.length} suites passed`);
  } else {
    console.log(`  ❌  ${failed} / ${suites.length} suites had failures`);
  }
  console.log('═'.repeat(60) + '\n');

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(err => {
  console.error('\n❌  Fatal error:', err.message);
  process.exit(1);
});
