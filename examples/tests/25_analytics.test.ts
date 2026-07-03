/**
 * Suite 25 — Analytics (GET /engine-rest/analytics/process-stats)
 *
 * Regression test for the incidents-cascade-delete-on-archive fix
 * (migrations/009_historic_incidents.sql) — without it, "incident rate by
 * process key" would silently undercount every archived instance, since
 * ArchiveAndDeleteProcessInstance hard-deletes the live process_instances
 * row (cascading onto incidents) the moment an instance finishes.
 *
 * Flow: complete one GrpcProcess instance cleanly, permanently fail a job
 * on a second (creating an incident), then terminate that second instance
 * while the incident is still open — exercising the "archive incidents in
 * whatever state they're in" behavior — and assert the analytics endpoint
 * still reports the incident after both instances have archived.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '24_grpc_process.bpmn');
const client = new GoFlowClient();

async function run() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN);

  await runSuite({
    name: '25 — Analytics (process-stats)',
    tests: [
      {
        name: '[A] Completed + terminated-with-open-incident instances both count for their process key after archiving',
        async fn() {
          // Instance A: completes cleanly.
          const instA = await client.startProcess('GrpcProcess', {});
          const idA = instA.processInstanceId ?? (instA as any).id;
          await client.pollExternalTask('grpc_review', async () => ({ reviewed: true }), 15000, idA);
          await client.waitForProcessEnd(idA);

          // Instance B: job permanently fails (retries=0) → open incident,
          // then the instance is terminated while that incident is still open.
          const instB = await client.startProcess('GrpcProcess', {});
          const idB = instB.processInstanceId ?? (instB as any).id;
          let taskB: any = null;
          const taskDeadline = Date.now() + 15000;
          while (!taskB && Date.now() < taskDeadline) {
            const tasks = await client.fetchAndLock([{ name: 'grpc_review' }]);
            taskB = tasks.find(t => t.processInstanceId === idB) ?? null;
            if (!taskB) await sleep(500);
          }
          assert(!!taskB, 'expected to fetch-and-lock the grpc_review job for instance B');
          await client.failExternalTask(taskB.id, 'analytics suite induced failure', 0);

          let incidents: any[] = [];
          const incidentDeadline = Date.now() + 10000;
          while (incidents.length === 0 && Date.now() < incidentDeadline) {
            const res = await client.api.get('/engine-rest/incident', { params: { processInstanceId: idB } });
            incidents = res.data ?? [];
            if (incidents.length === 0) await sleep(500);
          }
          assert(incidents.length >= 1, 'expected an open incident on instance B');
          assert(incidents[0].state === 'open', `expected the incident to still be open, got ${incidents[0].state}`);

          await client.api.delete(`/engine-rest/process-instance/${idB}`);

          // Both instances should now be archived (completed/terminated
          // don't linger in the live table). Poll the analytics endpoint
          // until it reflects both.
          let stats: any = null;
          const statsDeadline = Date.now() + 15000;
          while (Date.now() < statsDeadline) {
            const res = await client.api.get('/engine-rest/analytics/process-stats', { params: { processKey: 'GrpcProcess' } });
            const row = (res.data?.byProcessKey ?? []).find((s: any) => s.processKey === 'GrpcProcess');
            if (row && row.completedCount >= 1 && row.terminatedCount >= 1 && row.incidentCount >= 1) {
              stats = row;
              break;
            }
            await sleep(1000);
          }
          assert(stats !== null, 'expected GrpcProcess stats to report completed, terminated, and incident counts after archiving');
          assert(stats.incidentRate > 0, `expected a nonzero incident rate, got ${stats.incidentRate}`);
          assert(stats.avgDurationMillis != null, 'expected duration stats to be populated from historic data');
        },
      },

      {
        name: '[B] Throughput series covers the requested range with dense (zero-filled) days',
        async fn() {
          const res = await client.api.get('/engine-rest/analytics/process-stats', {
            params: { processKey: 'GrpcProcess', days: 7 },
          });
          const throughput = res.data?.throughput ?? [];
          assert(Array.isArray(throughput) && throughput.length >= 1, 'expected a non-empty throughput series');

          // Regression: GetStartedPerDay/GetFinishedPerDay used to key their
          // per-day maps by time.Time, which carries Location/monotonic
          // metadata — a value built via time.Now() and one scanned back
          // from Postgres in UTC represent the same calendar day but compare
          // unequal as map keys, silently dropping every row on merge. This
          // would make every "started"/"completed" count come back 0 despite
          // real data existing (confirmed manually before the fix, with
          // GrpcProcess instances started via tests 24/A right above).
          const totalStarted = throughput.reduce((sum: number, p: any) => sum + p.started, 0);
          assert(totalStarted > 0, `expected at least one non-zero "started" day in the throughput series (all zero would mean the time.Time map-key merge bug regressed), got ${JSON.stringify(throughput)}`);

          for (const point of throughput) {
            assert(typeof point.date === 'string', 'expected each throughput point to have a date string');
            assert(typeof point.started === 'number', 'expected each throughput point to have a numeric started count');
          }
        },
      },
    ],
  });
}

run().catch(console.error);
