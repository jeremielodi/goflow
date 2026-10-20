# GoFlow — Cloud-Native BPMN Workflow Engine

![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![BPMN 2.0](https://img.shields.io/badge/BPMN-2.0-orange)
![React](https://img.shields.io/badge/Frontend-React%2019-61DAFB?logo=react&logoColor=black)

GoFlow is a BPMN 2.0 workflow engine written in Go that runs models with Camunda 8 semantics, exposing REST APIs shaped after both **Camunda 7** (`/engine-rest/...`) and **Camunda 8** (`/v2/...`), plus a native **Zeebe gRPC gateway**. It's a lightweight, self-hosted alternative to the Java-based engines: PostgreSQL is its only database (a single server, a high-availability cluster with automatic failover, or a Citus cluster sharding the per-instance tables), the application runs as one instance or several behind a load balancer, and it comes with real-time event streaming and a React operations/tasklist frontend. Like Camunda 8, it can run every command of an instance on the one GoFlow instance owning its Kafka partition, with failover between instances. See [Comparison to Camunda 7 / Camunda 8](#comparison-to-camunda-7--camunda-8) for an honest look at what's covered and what isn't, and [Switching from Camunda 7 / Camunda 8](#switching-from-camunda-7--camunda-8) for what an existing Camunda application has to change.

## Contents

- [Current State](#current-state-v10)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Frontend](#frontend)
- [Architecture](#architecture)
- [Partitioned Write Path](#partitioned-write-path)
- [Integration Tests](#integration-tests)
- [Environment Variables](#environment-variables)
- [Operations](#operations)
- [Switching from Camunda 7 / Camunda 8](#switching-from-camunda-7--camunda-8)
- [Comparison to Camunda 7 / Camunda 8](#comparison-to-camunda-7--camunda-8)
- [Roadmap](#roadmap)

---

## Current State (v1.0)

### BPMN 2.0 Support

| Element | Status |
|---|---|
| Start Event: none, message, signal, timer (`timeDate`, `timeCycle` as ISO 8601 `R[n]/[start/]duration[/end]` or cron; armed on deploy, replaced by each new version) | ✅ |
| End Event | ✅ |
| Service Task (External Worker) | ✅ |
| User Task (with assignee / candidateGroups) | ✅ |
| Script Task (`zeebe:script` FEEL expression, or a job worker) | ✅ |
| Messages with Camunda 8 semantics: `zeebe:subscription` correlation keys, time to live (buffered messages), `messageId` deduplication, once per process, message start events (one active instance per key) | ✅ |
| Call Activity: `zeebe:calledElement` (processId as FEEL, `propagateAllChildVariables`, `propagateAllParentVariables`), input/output mappings, Camunda 7 `camunda:in`/`camunda:out`; uncaught errors and escalations rise to the call activity; the called instance is terminated with its call activity | ✅ |
| Exclusive Gateway (XOR) | ✅ |
| Inclusive Gateway (OR) | ✅ |
| Parallel Gateway (AND) | ✅ |
| Event-based Gateway | ✅ |
| Intermediate Timer Catch Event | ✅ |
| Intermediate Message Catch Event | ✅ |
| Intermediate Signal Catch Event | ✅ |
| Boundary Timer / Message / Signal / Conditional Event (interrupting and non-interrupting) | ✅ |
| Boundary Error / Escalation Event | ✅ |
| Embedded Sub-Process (nested, with boundary events) | ✅ |
| Event Sub-Process (timer, message, signal, error, escalation, conditional; interrupting or not) | ✅ |
| Manual / Send / Receive Task; an untyped task passes straight through (as in Camunda 8; versions deployed before keep it as a user task) | ✅ |
| Intermediate Throw Event (none, signal, escalation, compensation, link) and Link Catch Event | ✅ |
| Intermediate Conditional Catch Event | ✅ |
| End Event: terminate, error, signal, escalation, compensation, message (worked by a job worker) | ✅ |
| Compensation (boundary + handler, throw with optional `activityRef`, nested into completed sub-processes) | ✅ |
| Multi-instance on any activity (every task type, embedded sub-process, call activity), parallel or sequential: `zeebe:loopCharacteristics` (inputCollection, inputElement, outputCollection, outputElement), loop cardinality, completion condition (`numberOf*Instances`), boundary events on the whole loop | ✅ |
| Variable scopes: local variables per sub-process, multi-instance iteration and activity (input mappings), propagation to the nearest scope that has the variable, output mappings evaluated in the activity scope, `SetVariables` with `local` | ✅ |
| Business Rule Task (DMN decision table) | ✅ |

Timers (every timer event) take `timeDuration` (ISO 8601, calendar units included: `P1M`, `P1DT2H`), `timeDate` (with offset, `@Zone`/`[Zone]` or local) and `timeCycle` (`R[n]/[start/]duration[/end]` or a Spring cron expression, `L`/`W`/`#` included), or a FEEL expression evaluated against the instance variables when armed; cycles repeat on non-interrupting boundary events and event sub-processes.

Conditional events are re-evaluated as soon as a variable of their instance changes (a database trigger notifies the engine, as Camunda 7 evaluates on each variable write; Camunda 8 has no conditional events). Known limit of the BPMN coverage: compensation runs its handlers all at once (it reaches into completed embedded sub-processes without a handler of their own, as in Camunda 8; there is no compensation event sub-process).

### Engine Features

| Feature | Status |
|---|---|
| Process definition versioning | ✅ |
| Process variables (JSONB) | ✅ |
| FEEL expressions: native interpreter (`FEEL_MODE`, shadow by default) alongside the legacy FEEL-to-CEL path | ✅ |
| External task worker pattern (fetchAndLock) | ✅ |
| Job retries with exponential back-off | ✅ |
| Incident management: failed jobs, unhandled errors, and any failure on an element (condition, script, input collection, called process, timer…) as an incident on it; resolving it runs the element again (`POST /v2/incidents/:id/resolution`, gRPC `ResolveIncident`) | ✅ |
| Timer scheduler (SKIP LOCKED) | ✅ |
| Message correlation (`POST /engine-rest/message`) | ✅ |
| Signal broadcast (`POST /engine-rest/signal`) | ✅ |
| DMN decision engine (decision tables, standalone evaluation) | ✅ |
| Multi-tenancy (per-user tenant isolation) | ✅ |
| JWT authentication + RBAC | ✅ |
| OIDC bearer-token auth (optional, alongside JWT) | ✅ |
| Zeebe-style extensions (task headers, I/O mapping, FEEL assignee, form key) | ✅ |
| Forms (deploy + serve form schemas) | ✅ |
| Process instance modification ("Move Token") | ✅ |
| Audit trail (`audit_logs`, one row per state transition) | ✅ |
| Token-history replay (derived from the audit trail, see below) | ✅ |
| History API (process + activity instances) | ⚠️ basic — see [gaps](#comparison-to-camunda-7--camunda-8) |
| SSE real-time events (`/events/tasks`) | ✅ |
| WebSocket real-time events (`/ws/tasks`) | ✅ |
| React frontend (Operate + Tasklist-lite) | ✅ |
| Docker / Docker Compose deployment | ✅ |
| Horizontal scaling: several instances behind nginx, PgBouncer, read replica, background archiving (see [Operations](#horizontal-scaling)) | ✅ |
| Postgres high availability: Patroni cluster, synchronous replication, automatic failover through HAProxy (see [Operations](#postgres-high-availability)) | ✅ |
| Backups (`pg_dump`, standalone or through the cluster) with a restore check | ✅ |
| gRPC / native Zeebe protocol | ✅ all 19 RPCs of the zeebe-node 8.3 client protocol (`MigrateProcessInstance`/`UpdateJobTimeout` from newer Zeebe versions not yet) |
| Partitioned write path, Camunda 8 style (`WRITE_PATH=partitioned`, see [below](#partitioned-write-path)) | ✅ |

---

## Quick Start

```bash
# Clone
git clone <repo>
cd camunda-like

# Start everything (PostgreSQL + GoFlow)
docker compose up -d --build --wait

# Default superuser: admin@goflow.com / admin123 (seeded from docker-compose.yaml)
```

The API is available at `http://localhost:8080` (REST) and `localhost:26500` (gRPC — Zeebe protocol core loop, see [Roadmap](#roadmap)).

---

## API Reference

### Authentication

```http
POST /auth/login
{ "email": "...", "password": "..." }
→ { "accessToken": "...", "refreshToken": "..." }

POST /auth/refresh
POST /auth/logout
```

All other endpoints require `Authorization: Bearer <accessToken>`.

### Process Definitions

```http
# Deploy a BPMN file
POST /engine-rest/deployment/create
Content-Type: multipart/form-data
file=@process.bpmn

# Camunda 8 style
POST /engine-rest/v2/deployments

# List definitions
GET /engine-rest/process-definition
GET /engine-rest/process-definition?key=myProcess
GET /engine-rest/process-definition?key=myProcess&latestVersion=true

# Get single definition
GET /engine-rest/process-definition/:id
```

### Process Instances

```http
# Start by process key (latest version)
POST /engine-rest/v2/process-definitions/:key/start
{ "variables": { "orderId": "123" } }

# Start by definition ID
POST /engine-rest/process-definition/:id/start

# List running instances
GET /engine-rest/process-instance
GET /engine-rest/process-instance?processDefinitionKey=myProcess

# Get instance
GET /engine-rest/process-instance/:id

# Variables
GET  /engine-rest/process-instance/:id/variables
POST /engine-rest/process-instance/:id/variables
PUT  /engine-rest/process-instance/:id/variables/:name
DELETE /engine-rest/process-instance/:id/variables/:name

# Lifecycle
PUT    /engine-rest/process-instance/:id/suspended    (suspend/resume)
DELETE /engine-rest/process-instance/:id              (terminate)
GET    /engine-rest/process-instance/:id/state
```

### User Tasks

```http
GET  /engine-rest/tasks                     (list, filter by processInstanceId / assignee)
POST /engine-rest/tasks/:id/complete        { "variables": {...} }
POST /engine-rest/tasks/:id/claim           { "assignee": "..." }
POST /engine-rest/tasks/:id/unclaim
```

### External Tasks (Service Workers)

```http
POST /engine-rest/external-task/fetchAndLock
{
  "workerId": "my-worker",
  "maxJobsToActivate": 10,
  "topics": [{ "topicName": "send-email", "lockDuration": 30000 }]
}

POST /engine-rest/external-task/:id/complete   { "workerId": "...", "variables": {...} }
POST /engine-rest/external-task/:id/failure    { "workerId": "...", "errorMessage": "..." }
```

### Messages & Signals

```http
# Correlate message to waiting process
POST /engine-rest/message
{
  "messageName": "PaymentReceived",
  "correlationKeys": { "orderId": "123" },
  "variables": { "paymentRef": "PAY-001" }
}

# Broadcast signal to ALL waiting instances
POST /engine-rest/signal
{
  "name": "RestockAlert",
  "variables": { "product": "widget" }
}
```

### History

```http
GET /engine-rest/history/process-instance
GET /engine-rest/history/process-instance?processDefinitionKey=myProcess&state=completed
GET /engine-rest/history/process-instance/:id

GET /engine-rest/history/activity-instance?processInstanceId=:id
```

### Incidents

```http
GET    /engine-rest/incident
GET    /engine-rest/incident/:id
DELETE /engine-rest/incident/:id
POST   /engine-rest/job/:id/retries   { "retries": 3 }
```

### Audit Trail & Token-History Replay

Every state transition (process start/complete/terminate/suspend, execution created/moved/waited/completed, task created/claimed/completed/cancelled, job locked/completed/failed, gateway fork/join, timer created/triggered/cancelled, message correlation, signal broadcast) is written to `audit_logs`. The token-history endpoint turns that raw trail into an ordered, human-readable path — this is what the frontend's diagram replay (play/pause/step/scrub, see below) consumes.

```http
# Raw audit log for an instance, chronological
GET /engine-rest/audit/process/:processId

# Derived token path: one step per element the token visited, with a
# readable `detail` string and (where applicable) an `elementId` you can
# highlight on the BPMN diagram
GET /engine-rest/audit/process/:processId/token-history

GET  /engine-rest/audit/task/:taskId
POST /engine-rest/audit/date-range   { "start": "...", "end": "..." }
```

### Real-Time Events

```http
# Server-Sent Events (HTTP streaming)
GET /events/tasks

# WebSocket
ws://localhost:8080/ws/tasks
```

### Camunda 8 REST API (`/v2/...`)

Parallel to the Camunda-7-style surface above, the same engine is exposed with Camunda 8 ("Orchestration Cluster") request/response shapes — same underlying services, translated shapes only.

```http
POST /v2/deployments                          (also POST /v2/resources/deployments)
POST /v2/process-definitions/:key/start
POST /v2/process-instances
GET  /v2/process-instances/:id
POST /v2/process-instances/search
POST /v2/process-instances/:id/modification   (Move Token)

POST /v2/jobs/activation                      (fetch-and-lock equivalent)
POST /v2/jobs/:id/completion
POST /v2/jobs/:id/failure
POST /v2/jobs/:id/error

POST /v2/user-tasks/search
POST /v2/user-tasks/:id/assignment            (claim/unclaim)
POST /v2/user-tasks/:id/completion

POST /v2/messages/publication
POST /v2/signals/broadcast

POST /v2/decisions/:key/evaluation            (DMN, no process instance needed)
GET  /v2/forms/:formKey

POST /v2/flownode-instances/search
POST /v2/incidents/search
POST /v2/variables/search
```

`POST /v2/messages/publication` and the gRPC `PublishMessage` follow Camunda 8: the message goes to the subscriptions whose `zeebe:subscription correlationKey` evaluates to the published `correlationKey` — at most once per process — or starts the processes waiting on it with a message start event. With a `timeToLive` (ms), it is kept and correlates to subscriptions that open later; a `messageId` already published and alive is refused (409 / `ALREADY_EXISTS`). Nothing waiting is not an error. Processes without a `correlationKey` expression keep matching a key equal to their `correlationKey` variable, as before. `POST /engine-rest/message` keeps Camunda 7's semantics.

---

## Frontend

A React 19 + `bpmn-js` single-page app (`frontend/`, dockerized behind nginx) covering a slice of Camunda's Operate + Tasklist:

- **Dashboard** — at-a-glance counts (running/completed instances, open tasks, incidents)
- **Process Definitions / Process Instances** — deploy, browse, filter by key/state
- **Instance Detail** — live BPMN diagram with an active-token overlay, plus a **replay control**: play/pause/step/scrub through the instance's full audit-log-derived token history, with a distinct pulsing marker on the diagram showing where the token was at each step (independent of the live-state overlay). Clicking a row in the Activity History timeline jumps the replay straight to that step. Also supports Move Token (process instance modification) from here.
- **Tasks** — task inbox with claim/unclaim/complete, filtered to open tasks by default
- **History** — historic process instance list/filter
- **Incidents** — incident list with retry/resolve
- **Analytics** — duration distributions, throughput over time, and incident rate by process key (Optimize-lite)
- **Admin** — Users/Roles management and per-process VIEW/START/MANAGE permission grants

A `/modeler` page also lets you author and deploy BPMN diagrams directly in-browser (bpmn-js's editable `Modeler` build + a properties panel for Zeebe task/gateway properties) — see below.

---

## Architecture

Every request goes through the same parser/engine/runtime and lands in the same PostgreSQL database. What the deployment chooses (`WRITE_PATH`) is *where* each command runs: right where it arrives (`direct`, the default), or — like Camunda 8 — on the one GoFlow instance owning the Kafka partition of its process instance (`partitioned`).

```
                  ┌──────────────────────────────────────────────────────────────┐
                  │              REST (v1 + v2) API / gRPC gateway               │
                  └──────────────────────────────────────────────────────────────┘
                                                 │  every command that changes an instance
                                                 ▼  (internal/cmdbus)
                     WRITE_PATH=direct (default)        WRITE_PATH=partitioned
                                 │                                   │
                                 │                    ┌─────────────────────────────┐
                                 │                    │  Kafka / Redpanda, topic    │
                                 │                    │  partition-commands, keyed  │
                                 │                    │  by process instance        │
                                 │                    └─────────────────────────────┘
                                 │                                   │
                                 │                    ┌─────────────────────────────┐
                                 │                    │  owner of the partition:    │
                                 │                    │  one GoFlow instance, one   │
                                 │                    │  command at a time; reply   │
                                 │                    │  sent back to the caller    │
                                 │                    └─────────────────────────────┘
                                 ▼                                   ▼
                  ┌──────────────────────────────────────────────────────────────┐
                  │        the same command handlers: Parser -> Engine ->        │
                  │                    Runtime.executeNode()                     │
                  └──────────────────────────────────────────────────────────────┘
                                                 ▼
                  ┌──────────────────────────────────────────────────────────────┐
                  │     PostgreSQL 16 — single server, Patroni HA cluster,       │
                  │      or Citus (per-instance tables sharded over workers)     │
                  └──────────────────────────────────────────────────────────────┘
```


### Key Design Decisions

- **NodeExecutor pattern** — each BPMN node type has its own `executor_<type>.go` file, all private methods on `*Runtime`
- **PostgreSQL SKIP LOCKED** — timers and jobs use `SELECT … FOR UPDATE SKIP LOCKED` for horizontal scaling without a queue broker
- **Graph cache** — parsed process graphs are stored as JSONB in `process_definitions.parsed_graph`, rebuilt only on deploy
- **Event subscriptions** — message/signal/EBG waiting is implemented via an `event_subscriptions` table; correlation is a SQL query
- **Call Activity** — child instance stores `parent_instance_id` + `parent_execution_id`; end event handler resumes parent

---

## Partitioned Write Path

Like Camunda 8, GoFlow can run every process instance on one partition: `WRITE_PATH=partitioned` (default `direct`). Full design: [`architecture/partitioned-write-path.md`](architecture/partitioned-write-path.md).

- **Every command, every BPMN construct** — start, job completion/failure/BPMN error, retries, incident resolution, cancellation, user-task completion, variables, modification, message publication (on the partition of its correlation key), signal broadcast, timers, conditional events. No shape restriction.
- **How** — the command goes to the Kafka partition of its process instance (`goflow.partition-commands`, `KAFKA_PARTITIONS`, 12 by default, fixed once instances exist). The GoFlow instances form one consumer group: each partition is owned by exactly one of them, which runs its commands one at a time and sends the outcome back (`goflow.partition-replies`). The same handler code runs in direct mode, without Kafka.
- **Failover** — when an instance stops, its partitions move to the others (measured: ~7 s after a `docker kill`, no instance lost — `examples/tests/drill_partition_failover.ts`). A command left half-applied by a dying owner is delivered again and completed from where it stopped.
- **Topology** — `GET /v2/topology` and the gRPC `Topology` RPC show which instance leads which partition.
- **Requires** `KAFKA_BROKERS` (the bundled Redpanda). Postgres (or Citus) stays the state; the full regression passes in both modes, on one instance and on three behind the load balancer, and on Citus.

---

## Integration Tests

51 test suites covering every major feature, run against a real Docker Postgres+app stack:

```bash
cd examples/tests
npm install

# Run all (skip the two slow 60s-timer suites)
npx ts-node run_all.ts --no-timers

# Run everything including the timer suites
npx ts-node run_all.ts
```

| Suite | Feature |
|---|---|
| 00 | Authentication (JWT) |
| 01 | Boundary Timer Event |
| 02 | Exclusive Gateway |
| 03 | Parallel Gateway |
| 04 | Intermediate Timer Event |
| 05 | Inclusive Gateway |
| 06 | Error Boundary Event |
| 07 | Incident Management |
| 08 | Process Definition Versioning |
| 09 | History API |
| 10 | Message Events |
| 11 | Signal Events |
| 12 | Event-based Gateway |
| 13 | Call Activity |
| 14 | Task Listener Notifications (SSE) |
| 15 | Camunda 8 REST API (`/v2/...`) |
| 16 | Zeebe BPMN Extensions (task headers, I/O mapping, FEEL assignee, form key) |
| 17 | DMN Decision Engine |
| 18 | Tasklist-equivalent (v2 user-tasks search + forms) |
| 19 | Operate-equivalent (v2 flownode/variables/incidents/modification) |
| 20 | Multi-tenancy |
| 21 | OIDC Authentication |
| 22 | Audit Trail Completeness (token-history replay data source) |
| 23 | Admin UI (Users/Roles) + Process Permissions |
| 24 | gRPC Gateway (Zeebe protocol subset) |
| 25 | Analytics (Optimize-lite) |

**Event log and write path** (26–27):

| Suite | Feature |
|---|---|
| 26 | Event Log Shadow Mode (observability only) |
| 27 | Write path: topology, concurrent commands on one instance, 30 instances at once, messages across partitions, refusals (runs in both modes) |

**BPMN coverage and Camunda 8 semantics** (35–50):

| Suite | Feature |
|---|---|
| 35 | Embedded sub-process |
| 36 | BPMN element coverage (manual/send/receive tasks, throw/link/terminate/error events) |
| 37 | Event sub-processes and implicit end |
| 38 | Boundary events |
| 39 | Native FEEL |
| 40 | gRPC gateway, remaining RPCs |
| 41 | Escalation events |
| 42 | Conditional events |
| 43 | Compensation |
| 44 | Camunda 8 script tasks |
| 45 | Camunda 8 message semantics (correlation keys, time to live, message start) |
| 46 | Timer and signal start events |
| 47 | Camunda 8 timers (cycles, cron, FEEL) |
| 48 | Variable scopes and multi-instance |
| 49 | Call activities (variables, errors, escalations, termination) |
| 50 | Message end events, runtime incidents, untyped tasks, nested compensation |

The same suites run against several instances behind the load balancer, and against the high-availability cluster (`COMPOSE_FILE` with the files of [Operations](#operations), plus `GOFLOW_REPLICAS=3` for several instances). With `WRITE_PATH=partitioned` they run on the partitioned write path, and with `COMPOSE_FILE="docker-compose.yaml;docker-compose.citus.yml"` on Citus.

---

## Environment Variables

Every variable can come either from the process environment or from a `.env` file (optional — real environment variables always win, and the Docker image no longer contains a `.env`). `docker-compose.yaml` reads secrets from `.env` next to it: `JWT_SECRET_KEY` is required, `POSTGRES_PASSWORD` defaults to `goflow123`.

```bash
# Database
DB_HOST=postgres
DB_PORT=5432
DB_USER=goflow
DB_PASS=goflow123
DB_SSLMODE=disable   # or require / verify-ca / verify-full
DB_NAME=goflow

# JWT — required; the server refuses to start without it, and refuses
# known sample values when GOFLOW_ENV=production. Generate with:
#   openssl rand -base64 48
JWT_SECRET_KEY=<random value>

# Superuser (seeded on first boot)
SUPER_USER_NAME=admin
SUPER_USER_EMAIL=admin@goflow.com
SUPER_USER_PASSWORD=admin123

# OIDC (optional) — set all three to accept bearer tokens from an external
# identity provider alongside the built-in JWT login; leave unset to disable
# OIDC_ISSUER=https://your-idp.example.com/realms/goflow
# OIDC_AUDIENCE=goflow-api
# OIDC_JWKS_URL=https://your-idp.example.com/realms/goflow/protocol/openid-connect/certs

# Kafka (Redpanda): event log, and the partitioned write path
# leave KAFKA_BROKERS unset to disable both
# KAFKA_BROKERS=redpanda:9092
# KAFKA_REPLICATION_FACTOR=1   # replicas per partition, applied when topics are created
# WRITE_PATH=direct            # or partitioned (Camunda 8 style, see above)
# KAFKA_PARTITIONS=12          # fixed once instances exist
# COMMAND_TIMEOUT=30s          # how long a request waits for its command

# Observability — bearer token required on GET /metrics (unset = open)
# METRICS_TOKEN=<random value>

# Retention — automatically delete instances that finished more than N days
# ago (unset or 0 = keep everything forever); see "Operations" below
# RETENTION_DAYS=365
# RETENTION_INTERVAL=1h
# RETENTION_BATCH_SIZE=500

# FEEL expressions ("=" prefix) and DMN tables: shadow (default: legacy
# FEEL-to-CEL path decides, native FEEL runs alongside and mismatches are
# logged), feel (native interpreter decides), cel (legacy only)
# FEEL_MODE=shadow

# Connection pool per GoFlow instance (keep instances × this under
# Postgres's max_connections, or put PgBouncer in front)
# DB_MAX_OPEN_CONNS=80

# Finished instances: archived in the background (async, default) or in the
# call that finishes them (sync)
# ARCHIVE_MODE=async

# Several instances (set by docker-compose.scale.yaml, see Operations):
# relay task events between instances for SSE/WebSocket clients
# TASK_EVENTS_FANOUT=true
# with PgBouncer in front of Postgres: where session-level work (LISTEN,
# advisory locks) reaches Postgres directly
# DB_DIRECT_HOST=postgres
# DB_DIRECT_PORT=5432
# read replica for history and analytics (unset = the primary)
# DB_READ_HOST=postgres-replica
# DB_READ_PORT=5432

# High-availability cluster (deploy/ha, see Operations): passwords of the
# Patroni nodes, the same on every node
# PG_SUPERUSER_PASSWORD=<random value>
# PG_STANDBY_PASSWORD=<random value>
# PG_ADMIN_PASSWORD=<random value>

# Citus-backed distributed storage (optional — see below)
# leave unset (the default) to run on a single plain Postgres instance;
# only set when using docker-compose.citus.yml, which sets both automatically
# CITUS_ENABLED=true
# CITUS_WORKERS=citus-worker-1:5432,citus-worker-2:5432
```

**Trying the Citus-backed deployment:**

```bash
docker compose -f docker-compose.yaml -f docker-compose.citus.yml up -d --build --wait
```

Brings up a Citus coordinator + 2 workers instead of the single Postgres instance; the plain `docker compose up` is unaffected. The per-instance tables — `process_instances`, `executions`, `tasks`, `variables`, `scope_variables`, `gateway_state`, `multi_instance_executions`, `incidents`, `jobs`, `timer_jobs` — are sharded over the workers, all colocated on the process instance, so an instance's rows live on one worker. Config tables are reference tables (copied to every node); the others stay on the coordinator as Citus local tables. Jobs and timers are claimed without `FOR UPDATE` (Citus forbids it across shards): candidates are read, then each is claimed by an update on its own shard that only succeeds while it is still free. See [`architecture/citus-distributed-storage-design.md`](architecture/citus-distributed-storage-design.md).

---

## Operations

### Database migrations

Schema changes are plain SQL files applied by hand, never by the application — see [`database/migrations/README.md`](database/migrations/README.md).

### Metrics

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

### Retention

With `RETENTION_DAYS=N`, every `RETENTION_INTERVAL` (default `1h`, plus once at startup) the server deletes every instance that **ended** more than N days ago: its `historic_*` rows, any remaining `audit_logs` rows, and its event-log projections, in batches of `RETENTION_BATCH_SIZE` per transaction. Live instances are never touched, and `audit_logs` rows not tied to an instance (logins, user and role administration) are kept forever. With several GoFlow instances, a Postgres advisory lock makes exactly one of them purge each round. Unset by default: nothing is deleted unless you opt in.

### Backups

Postgres is the source of truth (Redpanda only holds the event log and in-flight partitioned-path commands), so it is what must be backed up:

```bash
./scripts/backup-postgres.sh      # pg_dump -Fc into ./backups, checked readable, rotated after 14 days
./scripts/test-restore.sh         # restores the latest dump into a throwaway Postgres container and counts rows
```

Run the backup daily from cron (`0 3 * * * cd /path/to/goflow && ./scripts/backup-postgres.sh >> backups/backup.log 2>&1`), copy `backups/` off the server, and run `test-restore.sh` regularly: a backup that was never restored is not a backup. Variables: `BACKUP_DIR`, `BACKUP_KEEP_DAYS`, `POSTGRES_CONTAINER`.

With the high-availability cluster (`deploy/ha`), back up through HAProxy, which leads to whichever node is the primary: `PGHOST=haproxy PGPORT=5000 PGPASSWORD=… ./scripts/backup-postgres.sh` — `pg_dump` then runs in a throwaway `postgres:16` container on GoFlow's Docker network (`DOCKER_NETWORK`). The cluster creates the GoFlow database from `template0`, so its dumps restore into any plain Postgres, the standalone one included. `test-restore.sh` never touches the production database, standalone or cluster. For point-in-time recovery on top of daily dumps, Spilo can archive the write-ahead log continuously to object storage (WAL-G; its `WAL_S3_BUCKET` / `AWS_*` variables on `node.compose.yaml`).

### Database connections

Each GoFlow instance opens up to `DB_MAX_OPEN_CONNS` connections (default 80). Postgres's `max_connections` (`POSTGRES_MAX_CONNECTIONS`, default 200) must cover that times the number of GoFlow instances, plus a few for administration — otherwise, under load, Postgres refuses connections (`too many clients already`) and requests fail.

### Load testing

`cmd/loadtest` measures how many process instances per second a server sustains: it deploys a minimal process (start → service task → end), runs job workers, and starts instances at increasing rates until latency, errors or the job backlog show the server can't keep up.

```bash
go run ./cmd/loadtest -url http://127.0.0.1:8080 -email <admin> -password <pwd> -rates 25,50,100,200 -step 30s -workers 64
```

It creates real instances: run it against a non-production server, or in production only in a quiet window and knowing the instances will stay in history (until retention removes them).

### Horizontal scaling

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

### Postgres high availability

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

### Read replica on an existing standalone database

On an existing database, a read replica needs, on the primary, a replication role (`CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '…'`) and a `pg_hba.conf` line (`host replication replicator <replica address> scram-sha-256`, then `SELECT pg_reload_conf()`); the replica then starts from `pg_basebackup -R` (what `deploy/replica-entrypoint.sh` does). None of it changes the data.


### FEEL expressions

Expressions starting with `=` (Camunda 8 style) and DMN input entries can be evaluated by a native FEEL interpreter (`internal/feel`) instead of the historical FEEL-to-CEL text rewriting. Native FEEL brings FEEL semantics the rewriting can't: a missing variable is `null` instead of an error, numbers are exact decimals (`0.1 + 0.2 = 0.3`), and `if/then/else`, `for`, `some/every`, list filters (`items[qty > 1]`), ranges, contexts and built-in functions (`string length`, `list contains`, `sum`, `date`, `duration`…) work.

`FEEL_MODE` controls the switch:

- `shadow` (default): the legacy path still decides; the native interpreter runs alongside and every disagreement is logged once per expression (`FEEL shadow mismatch …`) and counted in `goflow_feel_shadow_mismatches_total{kind}`. Where the legacy path fails and the native interpreter succeeds (a string literal, decimal arithmetic, `if … then … else`), the native result is used, so nothing that worked changes and what failed now works.
- `feel`: the native interpreter decides. Switch once shadow mode shows no unexpected mismatches on real traffic.
- `cel`: legacy path only.

Camunda 7 `${…}` expressions and unprefixed conditions always use the legacy path: they aren't FEEL.

### Redpanda

`docker-compose.yaml` runs one Redpanda node next to the app. Writes are fsynced to the `redpanda_data` volume, but there is no replication on a single machine, so back that volume up together with Postgres. Ports `9092` (Redpanda, no authentication) and `5432` (Postgres) are published on `127.0.0.1` only; the app reaches both over the Docker network. Tune with `REDPANDA_MEMORY` (default `1G`) and `REDPANDA_SMP` (default `1`).

---

## Switching from Camunda 7 / Camunda 8

What an application already running against Camunda needs to change to run against GoFlow. "Tested" means covered by the integration suites with that client; anything else is expected to work from the API shape but hasn't been verified with that client.

### Camunda 8 (Zeebe gRPC, `/v2` REST)

| Area | Status | What to do |
|---|---|---|
| Zeebe gRPC clients | ✅ tested with **zeebe-node 8.3** (all 19 RPCs) | Point the gateway address to `goflow-host:26500`. The Java client, Spring Zeebe and `@camunda8/sdk` haven't been tested; `MigrateProcessInstance` and `UpdateJobTimeout` (newer clients) don't exist yet. |
| Authentication (gRPC) | ⚠️ | GoFlow checks its own JWT (`POST /auth/login`), sent as `authorization: Bearer …`. A client configured with OAuth against Camunda Identity won't authenticate. **A call with no token runs as the first super_admin** — keep port 26500 off untrusted networks. |
| `/v2` REST | ✅ subset (list below) | Same paths and request shapes. **Keys are UUID strings**, not 64-bit integers: a client typing keys as numbers (the Java client's `long`) breaks; one treating them as strings works. |
| BPMN models | ✅ | Run as they are: `zeebe:` task definitions, I/O mappings, FEEL, called elements, multi-instance, message subscriptions, timers, signals, escalations, compensation, user tasks with forms. |
| Operate, Tasklist, Optimize, Connectors, Web Modeler | ❌ | Not provided. GoFlow has its own frontend (instance inspection, task inbox, analytics, in-browser modeler). The Desktop Modeler can deploy through the gRPC gateway. |

`/v2` endpoints available: `POST /v2/process-instances`, `GET /v2/process-instances/:key`, `POST /v2/process-instances/search`, `POST /v2/process-instances/:key/modification`, `POST /v2/jobs/activation`, `POST /v2/jobs/:key/completion`, `/failure`, `/error`, `POST /v2/user-tasks/search`, `POST /v2/user-tasks/:key/completion`, `/assignment`, `DELETE /v2/user-tasks/:key/assignee`, `POST /v2/messages/publication`, `POST /v2/signals/broadcast`, `POST /v2/incidents/search`, `POST /v2/incidents/:key/resolution`, `POST /v2/flownode-instances/search`, `POST /v2/variables/search`, `POST /v2/decisions/:key/evaluation`, `GET /v2/forms/:formKey`, `POST /v2/resources/deployments`, `GET /v2/topology`.

### Camunda 7 (`/engine-rest`)

| Area | Status | What to do |
|---|---|---|
| Authentication | ✅ | HTTP Basic (GoFlow user e-mail and password) or a GoFlow JWT. |
| External task workers | ⚠️ partial | `fetchAndLock`, `complete`, `failure` exist. **`handleBpmnError`, `extendLock` and `unlock` don't**: a worker using them gets an error (throw a BPMN error with `failure` carrying an `errorCode` and `retries: 0`, or with `POST /v2/jobs/:id/error`). `asyncResponseTimeout` (long polling) is ignored: `fetchAndLock` answers at once. The official external task clients haven't been tested. |
| Starting instances | ⚠️ different path | `POST /engine-rest/process-definition/key/{key}/start` doesn't exist: use `POST /engine-rest/v2/process-definitions/{key}/start` or `POST /engine-rest/process-definition/{id}/start`. |
| User tasks | ⚠️ different path | `/engine-rest/tasks` (plural): `GET /engine-rest/tasks`, `POST /engine-rest/tasks/{id}/claim`, `/unclaim`, `/complete`. Camunda's `/engine-rest/task/...` paths don't exist. |
| Deployment | ✅ | `POST /engine-rest/deployment/create`, multipart, with `deployment-name` (and optionally `tenant-id`). |
| Process instances, variables, incidents, history, messages, signals | ✅ | `/engine-rest/process-instance/...` (variables, suspension, deletion), `/engine-rest/incident`, `POST /engine-rest/job/{id}/retries`, `/engine-rest/history/process-instance`, `/history/activity-instance`, `/history/task`, `POST /engine-rest/message`, `POST /engine-rest/signal`. |
| BPMN models | ⚠️ must be adapted | **No Java**: `camunda:class`, `camunda:delegateExpression`, JUEL `camunda:expression` and Java listeners can't run. Turn Java service tasks into external tasks (`camunda:type="external"` with `camunda:topic`, or a `zeebe:taskDefinition` type) handled by a worker, and conditions into FEEL. Camunda 8 models need no change, so moving a Camunda 7 model to GoFlow is close to the Camunda 7 → 8 migration. |
| Cockpit, Tasklist, Admin webapps | ❌ | Not provided; see the GoFlow frontend. |

---

## Comparison to Camunda 7 / Camunda 8

GoFlow deliberately targets both REST API shapes with one engine. It is **not** a drop-in replacement for either at scale — an honest summary:

**Solid parity with Camunda 7:** the `/engine-rest/...` surface covers the BPMN element set above, external task workers, DMN, incidents, message/signal correlation, multi-tenancy, a complete write-side audit log, and a real `ACT_RU_*`/`ACT_HI_*`-style split: finished instances are archived into dedicated historic tables and removed from the live ones, with `/engine-rest/history/...` (process instances, activity instances, tasks) and the audit/token-replay endpoints all transparently serving from whichever side an instance currently lives on.

**Solid REST-shape parity with Camunda 8:** `/v2/...` mirrors job activation, user-tasks, process-instance modification/search, DMN evaluation, forms, and flownode/incident/variable search. On top of that, a native gRPC gateway implements Zeebe's actual wire protocol for all 19 RPCs the zeebe-node 8.3 client speaks — job activation (long poll and `StreamActivatedJobs` push), completion, failure and `ThrowError`, instance creation (including `CreateProcessInstanceWithResult`), deployment and `DeleteResource`, messages, signals, `SetVariables`, incidents, decisions and instance modification — so real Zeebe client SDKs can talk to GoFlow directly on port `26500`, not just HTTP clients.

**Camunda 8 BPMN semantics:** beyond the API shapes, the engine executes models the way Camunda 8 does — `zeebe:` extensions (script tasks, `zeebe:calledElement`, `zeebe:loopCharacteristics`, `zeebe:subscription` correlation keys, I/O mappings), local variable scopes, multi-instance on any activity, call activities that pass variables and propagate errors and escalations, message buffering with a time to live, timer and signal start events, cron timers, runtime incidents that resolve by running the element again — each one covered by the integration suites (35–50). Untyped tasks pass straight through, as in Camunda 8.

**Where it's architecturally different, not just missing a feature:** Postgres remains the state, where Zeebe keeps each partition's state on its brokers (RocksDB, Raft-replicated). Around that, GoFlow now works the same way: with `WRITE_PATH=partitioned` every command of an instance runs on the one GoFlow instance owning its Kafka partition, partitions move to the survivors when an instance dies (~7 s measured), and `GET /v2/topology` shows who leads what. Storage scales out with Citus (per-instance tables, jobs and timers included, sharded over workers) or stays one Postgres — a single server or a Patroni cluster with synchronous replication and automatic failover. What isn't the same is how far that goes: a Citus cluster or a Postgres primary is still the shared store every partition writes to, so the ceiling is the database cluster, not the number of brokers.

**Frontend:** the React app covers a real slice of Operate (instance inspection, plus a token-replay feature Operate doesn't offer by default), Tasklist (task inbox), a slice of Optimize (`/analytics` — duration distributions, throughput over time, incident rate by process key), and a Modeler (`/modeler` — author and deploy BPMN diagrams directly in-browser, not just view them).

---

## Roadmap

Prioritized by value vs. effort, based on gaps identified during ongoing feature and audit-trail work.

### Done

- **Soft-delete on termination** — `TerminateProcess` now marks `status='terminated'` and explicitly cancels open tasks/executions/jobs/timers in one transaction, instead of a hard delete whose FK cascade used to erase the instance's own audit trail the moment it was terminated.
- **Multi-token-aware replay** — token-history and the frontend replay UI group steps by `executionId`, so a parallel gateway or multi-instance task gets one marker per concurrently active token instead of one ambiguous shared marker.
- **Consolidated start-process paths** — REST, v2, start-by-definition-ID, and the async worker now all share one `service.StartProcessInstance`, replacing four previously-divergent implementations.
- **Genuine historic archive** — dedicated `historic_*` tables mirror Camunda 7's `ACT_RU_*`/`ACT_HI_*` split. Finished instances are archived then removed from the live tables; every read path that could touch a finished instance (including the frontend) transparently merges both sides.
- **Admin UI + per-resource process permissions** — Users/Roles pages plus a VIEW/START/MANAGE ACL scoped to individual process keys, enforced across both REST APIs.
- **Users/Roles RBAC hardening** — a follow-up audit of the admin feature above found and fixed several real gaps: `DeleteUser` used to hard-`DELETE`, which crashed with a raw FK-violation error for any user who had ever deployed a BPMN, started an instance, or appeared in an audit log (`deployments`/`process_instances`/`audit_logs` all reference `users.id` with no cascade, deliberately, to preserve history) — now a proper soft-delete (`is_active = false`, already checked at login) via a new `UserRepository.Deactivate`. `DeleteRole` now rejects with 409 while any user is still assigned to it, instead of silently cascading their permissions away. Four read routes (`GET /roles`, `GET /roles/:id`, `GET /users/:id/roles`, `GET /roles/:id/actions`) had no permission guard at all — now gated. A duplicate/misspelled permission code (`CAN_MANGE_USER` used everywhere vs. an unused, correctly-spelled `CAN_MANAGE_USERS`) was consolidated into one correct `CAN_MANAGE_USER` (`database/archive/migrations-before-baseline/014_fix_can_manage_user_code.sql`). Added a real admin password-reset feature (`PUT /users/:id/reset-password`, gated by the previously-unused `CAN_RESET_PASSWORD` action) and the frontend's missing role-label-edit UI. Along the way, found and fixed a severe pre-existing bug unrelated to any of the above: `models.Role`'s `db:"isDefault"` struct tag didn't match the actual Postgres column (`isdefault`, no separator — unquoted DDL identifiers fold to lowercase), so every role read (`GetRole`, `ListRoles`, `GetDefaultRole`) was crashing with a 500 — caught only by a scripted end-to-end verification, not by code review, since the bug was invisible from reading the SQL alone.
- **Task lifecycle events in the Kafka event log** — an audit of the event log domain events found job lifecycle fully instrumented but user-task lifecycle completely uninstrumented: not a missing call site, the proto schema had no `Task*` message type at all. Added `TaskCreated`/`TaskClaimed`/`TaskUnclaimed`/`TaskCompleted`/`TaskCancelled`, mirroring the field shapes of the older, separate `internal/events.TaskEventDispatcher` (SSE/WebSocket/webhook push) that was the only prior task observability signal. See [`architecture/event-sourced-kafka-design.md`](architecture/event-sourced-kafka-design.md) Phase 0 section for exactly which layer each event publishes from and why (task_repository.go can't import the event log directly — an import cycle).
- **gRPC gateway — Zeebe protocol core loop** — a native gRPC server (`internal/zeebegrpc`, port `26500`) implementing all 19 Gateway RPCs (13 at first, the last six added later — see below) (the full RPC set the `zeebe-node` client this project tests against actually supports) as thin translations onto the same service functions REST already uses, verified against the real SDK. `ActivateJobs`'s long-poll wakes near-instantly on a new job via a broadcast channel instead of a fixed poll interval. `UpdateJobRetries` and `BroadcastSignal` were added as a deliberately scoped batch — both wrap an existing function verbatim (`service.SetJobRetries`, `SignalController.Broadcast`) with zero new business logic, chosen over a wider batch (`SetVariables`, `ThrowError`, `CreateProcessInstanceWithResult`, `StreamActivatedJobs`, `DeleteResource`) that each need either new plumbing or their own sizing pass.
- **Process-graph cache fix** — the cache is now keyed by process *definition* (not instance) and bounded, closing an unbounded-growth, no-dedup bug.
- **Two FEEL→CEL evaluator bugs fixed** — numeric variables weren't coerced to a common type before evaluation, and keyword substitution (`and`/`or`) used to corrupt matching text inside quoted string literals.
- **Composite index for fetch-and-lock** — the hottest query in the system got a partial composite index, measured ~18x faster at realistic table sizes.
- **Analytics (Optimize-lite)** — a duration/throughput/incident-rate endpoint and frontend page, merging live and historic data. Building it surfaced and fixed a real incident-archival cascade-delete bug and a `time.Time`-as-map-key bug that silently zeroed the throughput series.
- **Cross-instance gRPC job wake-ups** — job creation now broadcasts via Postgres `LISTEN`/`NOTIFY` instead of an in-process-only channel, closing the one real gap that was blocking the app tier from scaling cleanly across multiple instances.
- **Partitioned write path, Camunda 8 style** — the July per-definition, shape-restricted Kafka path is replaced by a deployment mode (`WRITE_PATH=partitioned`): every command of every instance, whatever the BPMN, goes through `internal/cmdbus` to the Kafka partition of its instance and runs on the GoFlow instance owning it. The full regression passes on one and on three instances; a failover drill (`examples/tests/drill_partition_failover.ts`) kills an instance under load: partitions taken over in ~7 s, 60/60 instances completed. Testing it with 120 concurrent job completions found a pre-existing bug of the direct path too: some repository `...Tx` functions wrote through the connection pool instead of their transaction — not atomic, and able to exhaust the pool and hang every request. Fixed, with the job row now locked while it is completed or failed. See [`architecture/partitioned-write-path.md`](architecture/partitioned-write-path.md).
- **Citus-backed distributed storage, Phases 1-2** — an opt-in `docker-compose.citus.yml` override brings up a Citus coordinator + 2 workers alongside the app. Phase 1: global config/lookup tables (definitions, deployments, users/roles, forms, DMN, permissions) are Citus reference tables. Phase 2: the actual per-instance runtime tables — `process_instances`, `executions`, `tasks`, `variables`, `gateway_state`, `multi_instance_executions`, `incidents` — are real, sharded, colocated Citus distributed tables, verified end to end against a real Citus cluster (not just reasoned about). This is a different architectural axis from the Kafka write path — storage sharding rather than write-path ordering — see [`architecture/citus-distributed-storage-design.md`](architecture/citus-distributed-storage-design.md) for the full story, including two genuine pre-existing races (a termination/completion race, a parallel-gateway-join race) that Citus's added network latency exposed and this work fixed for both the Citus and default deployments. `jobs`/`timer_jobs` deliberately stay undistributed — their global `FOR UPDATE SKIP LOCKED` claim queries need their own redesign first, the one remaining piece of this effort.
- **Citus performance follow-up: single-shard routing** — the queries against those six per-instance tables that used to filter by row ID alone (correct, but a scatter-gather across every shard) now also filter by `process_instance_id`, confirmed via `EXPLAIN` on the coordinator to flip the plan from `Task Count: 32` to `Task Count: 1`. Found and fixed a real, silent regression along the way: two `gateway_state` lookup queries weren't selecting `process_instance_id`, so a join's second-arriving branch computed its follow-up update against a zero-value UUID and silently updated zero rows — any 2+ branch parallel/inclusive gateway join got permanently stuck right after the join with no error logged. See [`architecture/citus-distributed-storage-design.md`](architecture/citus-distributed-storage-design.md#6-performance-follow-up--route-single-shard-instead-of-scatter-gather-done-2026-07-05) for the full inventory and fix.
- **Parallel gateway (AND fork/join) on the partitioned write path** — turned out to be architecturally free, same as the DMN business-rule-task phase: `runtime.ExecuteExecution`'s existing fork/join handling (shared with the legacy/Citus paths) already works generically regardless of who calls it, so the only change needed was a new shape in the deploy-time validator. Sibling branches' job completions are never actually concurrent at the write layer — every command for one instance hashes to the same Kafka partition and is processed strictly serially — so none of the `FOR UPDATE`-locking concerns the legacy/Citus path needs apply here. See [`architecture/event-sourced-kafka-design.md`](architecture/event-sourced-kafka-design.md) "Phase 7" for the full story.
- **Two more gRPC RPCs: `UpdateJobRetries` and `BroadcastSignal`** — brings the gateway to 13 of the 19 RPCs the `zeebe-node` client this project verifies against actually supports. Both are thin wrappers with zero new business logic: `UpdateJobRetries` mirrors the existing REST `SetJobRetries` handler exactly, including its partitioned-write-path gate; `BroadcastSignal` needs no such gate at all, since `internal/partition.ValidateShape` never allows a signal catch event in any partitioned-write-path shape, so a partitioned instance can structurally never be one of its subscribers. Verified against the real client SDK, not a hand-rolled one.
- **gRPC gateway completed: the last six RPCs** — `ThrowError` (reuses the BPMN error catching of the engine: activity boundary, error event sub-process, sub-process boundary), `SetVariables`, `CreateProcessInstanceWithResult`, `StreamActivatedJobs` (push counterpart of `ActivateJobs`), `DeployProcess` (deprecated alias of `DeployResource`) and `DeleteResource` (refused while instances of the definition still exist, as in Zeebe). The gateway now covers all 19 RPCs of the zeebe-node 8.3 client, verified with the real SDK and, for the two RPCs that SDK lacks, with grpc-js on its official `zeebe.proto` (suite 40).
- **Citus: jobs and timers distributed** — `jobs`, `timer_jobs` and `scope_variables` are now sharded with the other per-instance tables. Claims avoid `FOR UPDATE` (which Citus refuses across shards): candidates are read, then each is claimed by an update on its own shard that only succeeds while it is free — simpler than the hot-index design first written up. The remaining coordinator tables are Citus local tables, so they can be joined with the distributed ones; the variables `NOTIFY` trigger (Citus refuses triggers on distributed tables) is replaced by the engine notifying after each write. Full regression on a coordinator + 2 workers.
- **In-browser BPMN Modeler** — a new `/modeler` page swaps the existing view-only `NavigatedViewer` for bpmn-js's editable `Modeler` build (already the same npm package, no new dependency for the canvas itself), plus `bpmn-js-properties-panel` + `zeebe-bpmn-moddle` (two new dependencies) so task types, gateway conditions, and every other `zeebe:`-namespaced property that actually makes a diagram deployable can be set from the UI, not just shapes and labels. New/Import/Export/Deploy toolbar; Deploy reuses the exact same `deployBpmn` REST call the file-upload flow in Definitions already uses. Verified end to end with a scripted deploy-and-run cycle of a Modeler-equivalent diagram (not just a build check) — the actual canvas/palette/properties-panel rendering itself needs a real browser to verify, which wasn't available in this environment.
- **Per-instance permission-filtering audit** — the existing VIEW/START/MANAGE process-definition ACL only gated the process-definition list/start endpoints; everything downstream of a running instance (process-instance detail/variables/suspend/terminate, historic process/activity lists, audit logs + token history, incidents, analytics' per-key breakdown, task lists) had no permission check at all. Added a shared `service.CanAccessProcessInstance(db, userID, instanceID, level)` helper (resolves an instance ID to its owning process key, checking the live table then the historic archive, then delegates to the existing `CanAccessProcess`) and wired it into ~25 handlers across `process_instance_controller.go`, `task_controller.go`, `history_controller.go`, `historic_task_controller.go`, `audit_controller.go`, `incident_controller.go`, and `analytics_controller.go` — single-resource endpoints return 404 (matching the existing `GetDefinition` precedent of not revealing a restricted resource exists), list endpoints filter per-row. Found and removed 6 confirmed-dead functions (4 on `TaskController`, 2 on `TaskRepository`) and one dead duplicate route registration, discovered while tracing which handlers were actually reachable. The standout finding was unrelated to permissions as such: `/token-history` was listed in `pkg/middleware.PublicRoutes`, an allowlist matched by **substring**, not prefix — so `GET /engine-rest/audit/process/:id/token-history` bypassed authentication entirely, not just the ACL, for anyone with no token at all. Fixed by removing the entry; regression-covered in suite 23's new test [E].
- **Removed Redis** — it had been fully vestigial for some time: no Go code ever constructed a client or called it (no `go-redis`/`redigo` dependency at all), the cross-instance job-wake mechanism has used Postgres `LISTEN`/`NOTIFY` from the start (see the `Cross-instance gRPC job wake-ups` entry above), and the `redis`/`redis-commander` services existed only as dead orchestration weight — `goflow`'s `depends_on: redis: condition: service_healthy` blocked app startup on a container the binary never talked to. Removed both services, the `redis_data` volume, and the unused `Redis*` fields/`RedisAddr()`/`GetRedisOptions()` from `internal/config/config.go`. Verified the stack starts clean and a spot-check of core suites still passes with Redis gone entirely.
- **Fixed a real nginx/Vite reverse-proxy gap: Admin UI couldn't create a user or a role** — `frontend/nginx.conf`'s proxy regex (`^/(engine-rest|auth|users|events|v2)/`) never listed `roles` or `actions` at all, and required a trailing path segment, so an exact `POST /users` (no further segment) also silently missed it — all four fell through to the SPA fallback and got HTML back instead of a JSON response. `vite.config.ts`'s dev-server proxy had the identical `roles`/`actions` gap. This was invisible to every test in this project so far because they all call the Go backend directly on `:8080`, bypassing nginx/Vite entirely. Fixed both proxy configs (`(/|$)` instead of a bare `/`, plus the two missing prefixes) and added a first real **Playwright** browser e2e suite (`frontend/e2e/`, `npm run test:e2e`) that drives an actual Chromium instance against the dockerized frontend on port 3000 — specifically to catch this whole bug class going forward, since it's structurally invisible to API-level scripted tests. Confirmed the new tests fail against the old proxy config and pass against the fix, not just that they pass. Also added `Cache-Control: no-store` on every proxied API response, closing a related staleness risk (a browser's private cache heuristically retaining an old fallback-HTML response, or a stale JWT surviving a dev-environment DB reset, both silently manifesting as "the list is empty and I can't add anything" with no visible error).
- **Process-definition BPMN diagram viewer + live token-count overlay** — the Definitions page's chevron link and "N versions" link had pointed at `/processes/:id` since some earlier pass, but that route was never registered — a dead link. Added the missing `/processes/:id` page, reusing the existing `BpmnViewer` component (previously only used on the per-instance detail page) to render the deployed XML read-only. On top of that, added a live "where are running instances currently sitting" overlay: a new `GET /engine-rest/process-definition/key/:key/token-counts` endpoint aggregates active/waiting executions across every running/suspended instance of a key, grouped by BPMN node, polled every 3s and rendered as a cyan badge distinct from the per-instance colored badges on the instance-detail page. Building the query surfaced a real bug: `process_instances` has no `process_key` column at all (only `process_definition_id`) — `process_key` lives on `process_definitions` and needs an explicit join, caught immediately by a live Postgres error rather than silently returning wrong data. Verified end to end with Playwright against a real deployed+started instance, not just a build check. Followed up with: (1) an inline process-instance list at the bottom of the same page (ID, current node, started/ended, status), polled every 5s, with a click-to-open variables modal (Name/Type/Value, reusing the same rendering as the instance-detail page) instead of navigating away; (2) a per-instance "Current Node" column, powered by a new `GET .../key/:key/instance-positions` endpoint (the same live snapshot as token-counts, broken out per instance instead of aggregated) — showing at a glance where each running instance's token currently sits, without opening the full replay view.
- **Camunda 8 BPMN parity** — the gap list against Camunda 8 is closed: script tasks, message semantics (correlation keys, time to live, deduplication, message start), timer and signal start events, timers with calendar durations, cycles and cron, local variable scopes, multi-instance on any activity (rewritten — it used to share one element variable across parallel iterations), call activities (variables in and out, errors and escalations rising to the caller, called instances terminated with their call activity), runtime incidents, untyped tasks passing through, nested compensation, conditional events evaluated on each variable change. Migrations `001`–`004`.
- **Horizontal scaling** — several instances behind nginx with PgBouncer and a read replica (`docker-compose.scale.yaml`), the full regression passing on three instances. Measuring under load showed the bottleneck was the disk wait on each commit, not the engine: background archiving and commits that don't wait where a crash can't hurt cut the write-ahead-log syncs per instance by half. Running several instances found three real bugs (a shared Kafka consumer group starving partitioned commands, unordered event publishing, finished-but-unarchived instances still receiving messages). Migration `005`.
- **Postgres high availability** — Patroni cluster, synchronous replication, failover through HAProxy (`deploy/ha`), with a failover drill (`scripts/ha-failover-test.sh`): a replica promoted in 21–28 s, no instance lost. Running on the cluster found a cycle-timer race and a JWT clock-skew issue, both fixed.
- **Backups for the cluster** — `backup-postgres.sh` dumps through HAProxy; `test-restore.sh` restores into a throwaway container, never into the production database.

### Medium-term (real features)

- **Newer-Zeebe gRPC RPCs** — `MigrateProcessInstance` and `UpdateJobTimeout` exist in newer Zeebe versions than the zeebe-node 8.3 client this project verifies against, if a real client workflow needs them. `MigrateProcessInstance` specifically has no existing primitive to wrap — remapping a running instance to a different process-definition version is new engine logic, not a thin protocol translation like the other RPCs.

### Long-term / architectural (flagged, not committed)

- **State on the partitions themselves** — Zeebe keeps each partition's state next to its leader; GoFlow keeps it in Postgres or Citus. Moving state onto the partition owners (with a replicated log as the source of truth) would remove the shared database as the ceiling, at the price of a different engine core — not planned.
