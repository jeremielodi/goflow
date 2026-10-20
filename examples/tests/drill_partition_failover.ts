/**
 * Failover drill for the partitioned write path (not part of run_all).
 *
 * Needs the 3-replica partitioned stack:
 *   COMPOSE_FILE="docker-compose.yaml;docker-compose.scale.yaml" WRITE_PATH=partitioned \
 *     docker compose up -d --build --wait --scale goflow=3
 *   npx ts-node --transpile-only drill_partition_failover.ts
 *
 * Starts instances, kills one GoFlow replica while their jobs are being
 * completed, and checks that its partitions move to the others and every
 * instance still completes. Restarts the replica at the end.
 */
import * as path from 'path';
import { execSync } from 'child_process';
import { GoFlowClient, sleep } from './client';

const BPMN = path.join(__dirname, 'bpmn', '27_partitions.bpmn');
const client = new GoFlowClient();
const run = Date.now().toString(36);
const INSTANCES = Number(process.env.DRILL_INSTANCES || 60);

async function topology() {
  return (await client.api.get('/v2/topology')).data as {
    clusterSize: number; partitionsCount: number; brokers: { host: string; partitions: { partitionId: number }[] }[];
  };
}

function covered(t: Awaited<ReturnType<typeof topology>>) {
  const ids = t.brokers.flatMap(b => b.partitions.map(p => p.partitionId));
  return ids.length === t.partitionsCount && new Set(ids).size === ids.length;
}

// retry runs fn until it answers, through the seconds a partition moves.
async function retry<T>(fn: () => Promise<T>, what: string, timeoutMs = 90000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      return await fn();
    } catch (err: any) {
      if (Date.now() > deadline) throw new Error(`${what}: ${err?.response?.status ?? ''} ${err?.message}`);
      await sleep(500);
    }
  }
}

async function main() {
  await client.loginAsSuperUser();
  await client.deployBpmn(BPMN, 'Failover drill');

  const before = await topology();
  if (before.clusterSize < 2 || !covered(before)) throw new Error(`needs ≥2 replicas owning every partition, got ${JSON.stringify(before)}`);
  console.log(`cluster: ${before.clusterSize} replicas, ${before.partitionsCount} partitions`);

  const orders = Array.from({ length: INSTANCES }, (_, i) => `F${i}-${run}`);
  const ids: string[] = await Promise.all(orders.map(orderId =>
    retry(async () => {
      const inst = await client.startProcess('Partitions27', { orderId });
      return (inst.processInstanceId ?? inst.id) as string;
    }, 'start')));
  console.log(`started ${ids.length} instances`);
  const wanted = new Set(ids);

  // Kill a replica owning partitions, while jobs are being completed.
  const victim = before.brokers.find(b => b.partitions.length > 0)!.host;
  let killedAt = 0;
  let takenOverAt = 0;
  const killer = (async () => {
    await sleep(1500);
    execSync(`docker kill ${victim}`);
    killedAt = Date.now();
    console.log(`killed replica ${victim} (owned partitions ${before.brokers.find(b => b.host === victim)!.partitions.map(p => p.partitionId).join(',')})`);
    // Watch the survivors take the partitions over: each logs the
    // partitions it owns at every new generation. (Asking the topology
    // through the load balancer would also time the balancer's retries
    // against the dead replica.)
    const since = new Date(killedAt - 1000).toISOString();
    const survivors = before.brokers.filter(b => b.host !== victim).map(b => b.host);
    for (;;) {
      const owned = new Set<number>();
      for (const host of survivors) {
        const logs = execSync(`docker logs --since ${since} ${host} 2>&1`).toString();
        const gens = [...logs.matchAll(/generation \d+, owning partitions \[([\d ]*)\]/g)];
        const last = gens[gens.length - 1];
        if (last) last[1].split(' ').filter(Boolean).forEach(p => owned.add(Number(p)));
      }
      if (owned.size === before.partitionsCount) break;
      await sleep(250);
    }
    takenOverAt = Date.now();
    console.log(`partitions taken over ${((takenOverAt - killedAt) / 1000).toFixed(1)}s after the kill`);
  })();

  let completed = 0;
  const deadline = Date.now() + 180000;
  while (completed < ids.length * 4 && Date.now() < deadline) {
    const res = await retry(() => client.api.post('/v2/jobs/activation', {
      type: 'p27-work', worker: `drill-${run}`, timeout: 20000, maxJobsToActivate: 50,
    }), 'activate');
    const jobs = (res.data.jobs ?? []).filter((j: any) => wanted.has(String(j.processInstanceKey)));
    await Promise.all(jobs.map(async (j: any) => {
      const r = await retry(() => client.api.post(`/v2/jobs/${j.jobKey}/completion`, { variables: {} }, {
        validateStatus: s => s < 500,
      }), 'complete');
      if (r.status < 300) completed++;
      // A 4xx: completed already by a request whose answer was lost with
      // the replica — the job is done either way.
      else completed++;
    }));
    if (jobs.length === 0) await sleep(500);
  }
  await killer;
  console.log(`completed ${completed} jobs`);

  if (!takenOverAt) throw new Error('partitions never taken over');

  for (let i = 0; i < ids.length; i++) {
    await retry(async () => {
      const r = await client.api.post('/v2/messages/publication', { name: 'Go27', correlationKey: orders[i] });
      if (!r.data.correlatedProcessInstanceKeys.includes(ids[i])) throw new Error('not waiting yet');
    }, `message to ${ids[i]}`);
    const task = await retry(() => client.waitForTask('p_review', ids[i], 5000), 'review task');
    await retry(() => client.completeTask(task.id), 'complete review');
  }

  const statuses = await Promise.all(ids.map(id =>
    retry(async () => (await client.api.get(`/engine-rest/history/process-instance/${id}`)).data.state as string, 'history')));
  const done = statuses.filter(s => s === 'completed').length;
  console.log(`${done} / ${ids.length} instances completed`);

  execSync(`docker start ${victim}`);
  await retry(async () => {
    const t = await topology();
    if (t.clusterSize !== before.clusterSize || !covered(t)) throw new Error('not back yet');
  }, 'replica back', 120000);
  console.log('replica restarted, partitions spread again');

  if (done !== ids.length) process.exit(1);
}

main().catch(err => { console.error(err); process.exit(1); });
