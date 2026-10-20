# Operations

Starting on one server and adding more later: [Ajouter des serveurs](adding-servers.md).

## Database migrations

Schema changes are plain SQL files applied by hand, never by the application — see [`database/migrations/README.md`](../database/migrations/README.md).

## Metrics

`GET /metrics` serves Prometheus metrics. Set `METRICS_TOKEN` and scrape with that bearer token:

```yaml
scrape_configs:
  - job_name: goflow
    authorization:
      credentials: <METRICS_TOKEN>
    static_configs:
      - targets: ["goflow:8080"]
```

| Metric | What it tells you |
|---|---|
| `goflow_process_instances{status}` | live instances by status |
| `goflow_jobs{status}`, `goflow_user_tasks{status}` | work waiting / in progress |
| `goflow_incidents_open` | failures needing an operator |
| `goflow_timers_pending`, `goflow_timers_overdue` | timer backlog; overdue > 0 means the scheduler is behind |
| `goflow_table_size_bytes{table}`, `goflow_table_rows_estimate{table}` | growth of `audit_logs`, `historic_*`, event-log projections |
| `goflow_retention_deleted_rows_total{table}` | rows removed by the retention purge |
| `goflow_http_requests_total`, `goflow_http_request_duration_seconds` | traffic, errors and latency per route pattern |
| `go_sql_*` | database connection pool (open, in use, wait time) |
| `goflow_worker_pool_*` | async worker pool queue and failures |
| `goflow_metrics_scrape_error` | 1 if the engine-state queries failed during the last scrape |

Engine-state metrics are read from Postgres at scrape time. On a Citus deployment, `goflow_table_size_bytes` only covers the coordinator's local tables.

`GET /health` now also pings the database and returns `503` when it is unreachable.

## Retention

With `RETENTION_DAYS=N`, every `RETENTION_INTERVAL` (default `1h`, plus once at startup) the server deletes every instance that **ended** more than N days ago: its `historic_*` rows, any remaining `audit_logs` rows, and its event-log projections, in batches of `RETENTION_BATCH_SIZE` per transaction. Live instances are never touched, and `audit_logs` rows not tied to an instance (logins, user and role administration) are kept forever. With several GoFlow instances, a Postgres advisory lock makes exactly one of them purge each round. Unset by default: nothing is deleted unless you opt in.

## Backups

Postgres is the source of truth (Redpanda only holds the event log and in-flight partitioned-path commands), so it is what must be backed up:

```bash
./scripts/backup-postgres.sh      # pg_dump -Fc into ./backups, checked readable, rotated after 14 days
./scripts/test-restore.sh         # restores the latest dump into a throwaway Postgres container and counts rows
```

Run the backup daily from cron (`0 3 * * * cd /path/to/goflow && ./scripts/backup-postgres.sh >> backups/backup.log 2>&1`), copy `backups/` off the server, and run `test-restore.sh` regularly: a backup that was never restored is not a backup. Variables: `BACKUP_DIR`, `BACKUP_KEEP_DAYS`, `POSTGRES_CONTAINER`.

With the high-availability cluster (`deploy/ha`), back up through HAProxy, which leads to whichever node is the primary: `PGHOST=haproxy PGPORT=5000 PGPASSWORD=… ./scripts/backup-postgres.sh` — `pg_dump` then runs in a throwaway `postgres:16` container on GoFlow's Docker network (`DOCKER_NETWORK`). The cluster creates the GoFlow database from `template0`, so its dumps restore into any plain Postgres, the standalone one included. `test-restore.sh` never touches the production database, standalone or cluster. For point-in-time recovery on top of daily dumps, Spilo can archive the write-ahead log continuously to object storage (WAL-G; its `WAL_S3_BUCKET` / `AWS_*` variables on `node.compose.yaml`).

## Database connections

Each GoFlow instance opens up to `DB_MAX_OPEN_CONNS` connections (default 80). Postgres's `max_connections` (`POSTGRES_MAX_CONNECTIONS`, default 200) must cover that times the number of GoFlow instances, plus a few for administration — otherwise, under load, Postgres refuses connections (`too many clients already`) and requests fail.

## Load testing

`cmd/loadtest` measures how many process instances per second a server sustains: it deploys a minimal process (start → service task → end), runs job workers, and starts instances at increasing rates until latency, errors or the job backlog show the server can't keep up.

```bash
go run ./cmd/loadtest -url http://127.0.0.1:8080 -email <admin> -password <pwd> -rates 25,50,100,200 -step 30s -workers 64
```

It creates real instances: run it against a non-production server, or in production only in a quiet window and knowing the instances will stay in history (until retention removes them).

## Horizontal scaling

Several GoFlow instances can share one database behind a load balancer:

```bash
docker compose -f docker-compose.yaml -f docker-compose.scale.yaml up -d --scale goflow=3
```

`docker-compose.scale.yaml` leaves `docker-compose.yaml` untouched for a single instance and adds:

- an nginx load balancer (`deploy/nginx-lb.conf`: HTTP, server-sent events, WebSocket and the gRPC gateway) in front of the replicas;
- PgBouncer in transaction mode: the replicas may open many client connections, Postgres only sees the pool (`PGBOUNCER_POOL_SIZE`). What needs a Postgres session — `LISTEN`, the advisory locks of the archiver and the retention job — reaches Postgres directly (`DB_DIRECT_HOST`). GoFlow uses the pgx driver, which sends each query in one round trip, as a transaction-mode pooler requires;
- a streaming read replica (`deploy/replica-entrypoint.sh`): history and analytics read it (`DB_READ_HOST`), a few milliseconds behind the primary; everything else reads and writes the primary.

Everything running in the background is safe with several instances (timers and conditional events are claimed with `SKIP LOCKED`, archiving under advisory locks, message correlation atomically), job wake-ups and task events (`TASK_EVENTS_FANOUT`) are relayed between instances through Postgres `NOTIFY`, and each instance watches the whole Kafka event log. The full regression suite passes against three replicas behind PgBouncer, with the read replica.

What bounds throughput is then Postgres — mostly waiting for its write-ahead log to reach the disk on each commit. GoFlow waits only where it matters: finished instances are archived in the background (`ARCHIVE_MODE=async`, the default; `sync` archives in the completing call), and writes that are safe to lose or redo on a crash — audit rows, archiving, the Kafka projections, job locks, a token passing through a step that doesn't wait — commit without waiting for the disk (`synchronous_commit` off for those transactions only); engine state stays durable. A server's SSD sustains far more commits per second than a laptop's virtualized disk, so measure on the real hardware with `cmd/loadtest`.

## Postgres high availability

With three database servers, Postgres runs as a Patroni cluster (`deploy/ha`): Patroni elects a primary (its lock lives in etcd, which runs on the same three servers), the other two follow it with synchronous replication — a durable commit is on two nodes before it is confirmed, so a failover loses none — and if the primary fails, a replica is promoted. HAProxy, on the application server, sends GoFlow to the current primary (port 5000) and history reads to any node that can serve them (port 5001), using Patroni's health checks. GoFlow needs nothing more: connections cut by a failover are reopened, `LISTEN` reconnects, and the instances carry on.

```bash
# on each database server (pg1, pg2, pg3 — its own name and address)
NODE_NAME=pg1 NODE_IP=10.0.0.1 \
ETCD_CLUSTER="pg1=http://10.0.0.1:2380,pg2=http://10.0.0.2:2380,pg3=http://10.0.0.3:2380" \
ETCD_HOSTS="'10.0.0.1:2379','10.0.0.2:2379','10.0.0.3:2379'" \
PG_SUPERUSER_PASSWORD=… PG_STANDBY_PASSWORD=… PG_ADMIN_PASSWORD=… \
docker compose -f deploy/ha/node.compose.yaml up -d

# on the application server
PG1_ADDR=10.0.0.1 PG2_ADDR=10.0.0.2 PG3_ADDR=10.0.0.3 PG_SUPERUSER_PASSWORD=… \
docker compose -f docker-compose.yaml -f deploy/ha/app.compose.yaml up -d
```

Several GoFlow instances in front of the cluster combine both setups — nginx in front of the instances, PgBouncer going through HAProxy (so following the primary across failovers), no separate read replica since the cluster has its own:

```bash
docker compose -f docker-compose.yaml -f docker-compose.scale.yaml \
  -f deploy/ha/app.compose.yaml -f deploy/ha/scale-ha.compose.yaml up -d --scale goflow=3
```

Open 2379, 2380, 5432 and 8008 between the database servers, and 5432/8008 to the application server — never to the Internet. `deploy/ha/docker-compose.ha.yaml` runs the whole cluster on one machine to try it out; `scripts/ha-failover-test.sh` stops the current primary under load and checks that no instance is lost and that GoFlow carries on. On this laptop, a replica took over 28 seconds after the primary stopped, and GoFlow was writing again at the same moment, with no instance lost; the full regression suite passes on the cluster.

Moving an existing standalone database to the cluster leaves the old one untouched:

1. Run the migrations of `database/migrations` on the old database, as usual.
2. Start the three nodes, check the cluster: `curl http://10.0.0.1:8008/cluster`.
3. Stop GoFlow (`docker compose stop goflow`) and dump the old database — a read: `docker exec goflow-postgres pg_dump -U goflow -Fc goflow > goflow.dump`.
4. Create the role and the empty database on the cluster: `docker compose -f docker-compose.yaml -f deploy/ha/app.compose.yaml run --rm -e GOFLOW_SKIP_SCHEMA=true ha-init`.
5. Restore into the primary: `pg_restore -h 10.0.0.x -U postgres -d goflow --no-owner --role=goflow goflow.dump` (any node: if it isn't the primary, the restore is refused — use the one `/cluster` lists as leader).
6. Start GoFlow on the cluster (`app.compose.yaml`). Keep the old database stopped but intact until you're satisfied: going back is restarting GoFlow without `app.compose.yaml`.

## Read replica on an existing standalone database

On an existing database, a read replica needs, on the primary, a replication role (`CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '…'`) and a `pg_hba.conf` line (`host replication replicator <replica address> scram-sha-256`, then `SELECT pg_reload_conf()`); the replica then starts from `pg_basebackup -R` (what `deploy/replica-entrypoint.sh` does). None of it changes the data.

## FEEL expressions

Expressions starting with `=` (Camunda 8 style) and DMN input entries can be evaluated by a native FEEL interpreter (`internal/feel`) instead of the historical FEEL-to-CEL text rewriting. Native FEEL brings FEEL semantics the rewriting can't: a missing variable is `null` instead of an error, numbers are exact decimals (`0.1 + 0.2 = 0.3`), and `if/then/else`, `for`, `some/every`, list filters (`items[qty > 1]`), ranges, contexts and built-in functions (`string length`, `list contains`, `sum`, `date`, `duration`…) work.

`FEEL_MODE` controls the switch:

- `shadow` (default): the legacy path still decides; the native interpreter runs alongside and every disagreement is logged once per expression (`FEEL shadow mismatch …`) and counted in `goflow_feel_shadow_mismatches_total{kind}`. Where the legacy path fails and the native interpreter succeeds (a string literal, decimal arithmetic, `if … then … else`), the native result is used, so nothing that worked changes and what failed now works.
- `feel`: the native interpreter decides. Switch once shadow mode shows no unexpected mismatches on real traffic.
- `cel`: legacy path only.

Camunda 7 `${…}` expressions and unprefixed conditions always use the legacy path: they aren't FEEL.

## Redpanda

`docker-compose.yaml` runs one Redpanda node next to the app. Writes are fsynced to the `redpanda_data` volume, but there is no replication on a single machine, so back that volume up together with Postgres. Ports `9092` (Redpanda, no authentication) and `5432` (Postgres) are published on `127.0.0.1` only; the app reaches both over the Docker network. Tune with `REDPANDA_MEMORY` (default `1G`) and `REDPANDA_SMP` (default `1`).
