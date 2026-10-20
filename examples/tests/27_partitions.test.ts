/**
 * Suite 27 — the write path  (BPMN: 27_partitions.bpmn)
 *
 * Runs in both write paths (WRITE_PATH, see internal/cmdbus). Partitioned,
 * every command of an instance goes to its Kafka partition and runs on the
 * replica owning it, like Camunda 8; direct, it runs where it arrives.
 * Either way, commands of one instance arriving together must give the
 * same outcome as one after the other.
 *
 * Tests:
 *   A) GET /v2/topology: partitioned, every partition is owned by exactly
 *      one replica (each a "broker", as in Zeebe); direct, one partition.
 *   B) Four jobs of one instance completed at the same time: the join
 *      fires exactly once and the instance carries on.
 *   C) Thirty instances started and driven at the same time, spread over
 *      the partitions, all complete with their own variables.
 *   D) A message reaches its instance by correlation key, whichever
 *      partition the message and the instance land on.
 *   E) Commands answer like the direct path: a job completed twice is
 *      refused the second time, an unknown job is a 404.
 */
import * as path from 'path';
import { GoFlowClient, runSuite, assert, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '27_partitions.bpmn');
const client = new GoFlowClient();
const run = Date.now().toString(36);

async function start(vars: Record<string, unknown>) {
  const inst = await client.startProcess('Partitions27', vars);
  return (inst.processInstanceId ?? inst.id) as string;
}

// activate returns the activated p27-work jobs of the given instances.
async function activate(instances: string[], expected: number, timeoutMs = 15000) {
  const wanted = new Set(instances);
  const jobs: any[] = [];
  const deadline = Date.now() + timeoutMs;
  while (jobs.length < expected && Date.now() < deadline) {
    const res = await client.api.post('/v2/jobs/activation', {
      type: 'p27-work', worker: `p27-${run}`, timeout: 60000, maxJobsToActivate: 100,
    });
    for (const j of res.data.jobs ?? []) {
      if (wanted.has(String(j.processInstanceKey))) jobs.push(j);
    }
    if (jobs.length < expected) await sleep(300);
  }
  assert(jobs.length === expected, `expected ${expected} jobs, activated ${jobs.length}`);
  return jobs;
}

async function completeJob(jobKey: string, variables: Record<string, unknown>) {
  return client.api.post(`/v2/jobs/${jobKey}/completion`, { variables }, { validateStatus: () => true });
}

// until polls check until it holds.
async function until(check: () => Promise<boolean>, timeoutMs: number, what: string) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await sleep(300);
  }
  throw new Error(`timed out waiting for ${what}`);
}

async function publish(name: string, correlationKey: string, variables: Record<string, unknown> = {}) {
  const res = await client.api.post('/v2/messages/publication', { name, correlationKey, variables });
  return res.data as { correlatedProcessInstanceKeys: string[] };
}

async function historicStatus(id: string) {
  const res = await client.api.get(`/engine-rest/history/process-instance/${id}`, { validateStatus: () => true });
  return res.status === 200 ? res.data.state : undefined;
}

// finish sends the message, completes the review and waits for the end.
async function finish(id: string, orderId: string) {
  await until(async () => {
    const r = await publish('Go27', orderId);
    return r.correlatedProcessInstanceKeys.includes(id);
  }, 15000, `message to ${id}`);
  const task = await client.waitForTask('p_review', id, 15000);
  await client.completeTask(task.id, { reviewed: orderId });
  await client.waitForProcessEnd(id, 20000);
}

async function main() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Partitions Test');

  await runSuite({
    name: '27 — write path (partitions)',
    tests: [
      {
        name: '[A] Topology: each partition has exactly one owner',
        async fn() {
          const topo = (await client.api.get('/v2/topology')).data;
          assert(Array.isArray(topo.brokers) && topo.brokers.length >= 1, `expected brokers, got ${JSON.stringify(topo)}`);
          const owned: number[] = topo.brokers.flatMap((b: any) => b.partitions.map((p: any) => p.partitionId));
          if (topo.writePath === 'partitioned') {
            // Ownership settles once every replica has joined the group.
            let settled = topo;
            await until(async () => {
              settled = (await client.api.get('/v2/topology')).data;
              const ids: number[] = settled.brokers.flatMap((b: any) => b.partitions.map((p: any) => p.partitionId));
              return ids.length === settled.partitionsCount && new Set(ids).size === ids.length;
            }, 30000, 'every partition owned once');
            const replicas = Number(process.env.GOFLOW_REPLICAS || 1);
            assert(settled.clusterSize === replicas, `expected ${replicas} brokers (replicas), got ${settled.clusterSize}`);
            console.log(`      partitioned: ${settled.partitionsCount} partitions over ${settled.clusterSize} replica(s)`);
          } else {
            assert(topo.writePath === 'direct' && topo.partitionsCount === 1 && owned.length === 1, `direct mode is one partition, got ${JSON.stringify(topo)}`);
          }
        },
      },
      {
        name: '[B] Four jobs of one instance completed together: one join, carries on',
        async fn() {
          const orderId = `B-${run}`;
          const id = await start({ orderId });
          const jobs = await activate([id], 4);
          const results = await Promise.all(jobs.map((j, i) => completeJob(j.jobKey, { [`part${i}`]: i })));
          for (const r of results) assert(r.status === 200 || r.status === 204, `job completion answered ${r.status}: ${JSON.stringify(r.data)}`);

          const vars = await client.getProcessVariables(id);
          for (let i = 0; i < 4; i++) assert(`part${i}` in vars, `variable part${i} of a concurrent completion was lost: ${JSON.stringify(vars)}`);
          await finish(id, orderId);
          assert(await historicStatus(id) === 'completed', 'the instance should be completed');
        },
      },
      {
        name: '[C] Thirty instances driven at the same time all complete',
        async fn() {
          const orders = Array.from({ length: 30 }, (_, i) => `C${i}-${run}`);
          const ids = await Promise.all(orders.map(orderId => start({ orderId })));
          const jobs = await activate(ids, ids.length * 4, 30000);
          const results = await Promise.all(jobs.map(j => completeJob(j.jobKey, {})));
          const bad = results.filter(r => r.status !== 200 && r.status !== 204);
          assert(bad.length === 0, `${bad.length} completions failed: ${JSON.stringify(bad.slice(0, 2).map(r => r.data))}`);

          await Promise.all(ids.map((id, i) => finish(id, orders[i])));
          const statuses = await Promise.all(ids.map(historicStatus));
          assert(statuses.every(s => s === 'completed'), `not all completed: ${JSON.stringify(statuses)}`);
        },
      },
      {
        name: '[D] A message reaches only its instance, across partitions',
        async fn() {
          const a = `D1-${run}`, b = `D2-${run}`;
          const ia = await start({ orderId: a });
          const ib = await start({ orderId: b });
          for (const j of await activate([ia, ib], 8)) await completeJob(j.jobKey, {});

          await until(async () => {
            const r = await publish('Go27', b, { from: 'D' });
            return r.correlatedProcessInstanceKeys.includes(ib);
          }, 15000, 'message to the second instance');
          await client.waitForTask('p_review', ib, 15000);
          const tasksA = await client.getTasks(ia);
          assert(!tasksA.some(t => t.taskDefinitionKey === 'p_review'), 'the other instance must still wait for its message');
          await finish(ia, a);
          const task = await client.waitForTask('p_review', ib, 5000);
          await client.completeTask(task.id);
          await client.waitForProcessEnd(ib, 20000);
        },
      },
      {
        name: '[E] Refusals travel back like on the direct path',
        async fn() {
          const orderId = `E-${run}`;
          const id = await start({ orderId });
          const jobs = await activate([id], 4);
          const first = await completeJob(jobs[0].jobKey, {});
          assert(first.status === 200 || first.status === 204, `first completion answered ${first.status}`);
          const again = await completeJob(jobs[0].jobKey, {});
          assert(again.status >= 400 && again.status < 500, `completing a job twice must be refused, got ${again.status}`);

          const unknown = await completeJob('00000000-0000-4000-8000-000000000000', {});
          assert(unknown.status === 404, `an unknown job must be a 404, got ${unknown.status}: ${JSON.stringify(unknown.data)}`);

          for (const j of jobs.slice(1)) await completeJob(j.jobKey, {});
          await finish(id, orderId);
        },
      },
    ],
  });
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
