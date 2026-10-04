# GoFlow — Cloud-Native BPMN Workflow Engine

![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![BPMN 2.0](https://img.shields.io/badge/BPMN-2.0-orange)
![React](https://img.shields.io/badge/Frontend-React%2019-61DAFB?logo=react&logoColor=black)

GoFlow is a production-ready BPMN 2.0 workflow engine written in Go, exposing REST APIs shaped after both **Camunda 7** (`/engine-rest/...`) and **Camunda 8** (`/v2/...`), plus a native **Zeebe gRPC gateway**. It's a lightweight, self-hosted alternative to traditional Java-based engines: a single PostgreSQL backend, real-time event streaming, and a React operations/tasklist frontend. See [Comparison to Camunda 7 / Camunda 8](#comparison-to-camunda-7--camunda-8) for an honest look at what's covered and what isn't.

## Contents

- [Current State](#current-state-v10)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Frontend](#frontend)
- [Architecture](#architecture)
- [Event-Sourced Write Path (Experimental)](#event-sourced-write-path-experimental)
- [Integration Tests](#integration-tests)
- [Environment Variables](#environment-variables)
- [Comparison to Camunda 7 / Camunda 8](#comparison-to-camunda-7--camunda-8)
- [Roadmap](#roadmap)

---

## Current State (v1.0)

### BPMN 2.0 Support

| Element | Status |
|---|---|
| Start Event (plain, Message, Signal) | ✅ |
| End Event | ✅ |
| Service Task (External Worker) | ✅ |
| User Task (with assignee / candidateGroups) | ✅ |
| Script Task | ✅ |
| Call Activity (sub-process invocation) | ✅ |
| Exclusive Gateway (XOR) | ✅ |
| Inclusive Gateway (OR) | ✅ |
| Parallel Gateway (AND) | ✅ |
| Event-based Gateway | ✅ |
| Intermediate Timer Catch Event | ✅ |
| Intermediate Message Catch Event | ✅ |
| Intermediate Signal Catch Event | ✅ |
| Boundary Timer Event (interrupting) | ✅ |
| Boundary Error Event | ✅ |
| Boundary Message Event | ✅ |
| Boundary Signal Event | ✅ |
| Multi-instance (parallel / sequential) | ✅ |
| Business Rule Task (DMN decision table) | ✅ |

### Engine Features

| Feature | Status |
|---|---|
| Process definition versioning | ✅ |
| Process variables (JSONB) | ✅ |
| CEL expression evaluation on flows (FEEL-*like*, not full FEEL) | ✅ |
| External task worker pattern (fetchAndLock) | ✅ |
| Job retries with exponential back-off | ✅ |
| Incident management | ✅ |
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
| Horizontal scaling (PostgreSQL SKIP LOCKED + cross-instance job wake-ups) | ✅ |
| gRPC / native Zeebe protocol | ⚠️ 13 of 19 RPCs — see [Roadmap](#roadmap) |
| Kafka-partitioned write path (opt-in, per definition) | 🧪 experimental — see [below](#event-sourced-write-path-experimental) |

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

Every request goes through the same parser/engine/runtime and lands in the same PostgreSQL database — the only thing that varies **per process definition** is how it gets there: a direct call by default, or through Kafka for the [experimental partitioned path](#event-sourced-write-path-experimental).

```
                  ┌──────────────────────────────────────────────────────────────┐
                  │              REST (v1 + v2) API / gRPC gateway               │
                  └──────────────────────────────────────────────────────────────┘
                                                 │
                write_path = 'legacy' (default)     write_path = 'partitioned' (opt-in)
                                 ▼                                    ▼
┌─────────────────────────────┐      ┌─────────────────────────────┐
│  Direct call, one ACID tx:  │      │       Command Gateway       │
│  Parser -> Engine Build ->  │      │ produce a command, wait for │
│    Runtime.executeNode()    │      │   the confirmation event    │
└─────────────────────────────┘      └─────────────────────────────┘
                                                    │
                                     ┌─────────────────────────────┐
                                     │      Kafka / Redpanda       │
                                     │   process-commands topic    │
                                     │    process-events topic     │
                                     └─────────────────────────────┘
                                                    │
                                     ┌─────────────────────────────┐
                                     │     Partition Processor     │
                                     │  one owner per partition,   │
                                     │  calls the SAME Runtime /   │
                                     │ Service functions as legacy │
                                     └─────────────────────────────┘
                                 ▼                                    ▼
                                                 ▼
                  ┌──────────────────────────────────────────────────────────────┐
                  │                        PostgreSQL 16                         │
                  │           process_definitions, process_instances,            │
                  │                 executions, jobs, variables,                 │
                  │               event_subscriptions, timer_jobs                │
                  └──────────────────────────────────────────────────────────────┘
```

The left branch (direct call, one ACID transaction) is the **default, always-on path** — every process definition uses it unless explicitly flipped. The right branch is real, tested, but experimental — see [Event-Sourced Write Path](#event-sourced-write-path-experimental) below for what it currently covers and its own zoomed-in diagram.

### Key Design Decisions

- **NodeExecutor pattern** — each BPMN node type has its own `executor_<type>.go` file, all private methods on `*Runtime`
- **PostgreSQL SKIP LOCKED** — timers and jobs use `SELECT … FOR UPDATE SKIP LOCKED` for horizontal scaling without a queue broker
- **Graph cache** — parsed process graphs are stored as JSONB in `process_definitions.parsed_graph`, rebuilt only on deploy
- **Event subscriptions** — message/signal/EBG waiting is implemented via an `event_subscriptions` table; correlation is a SQL query
- **Call Activity** — child instance stores `parent_instance_id` + `parent_execution_id`; end event handler resumes parent

---

## Event-Sourced Write Path (Experimental)

Alongside the default transactional engine, GoFlow also ships an **opt-in, per-process-definition** write path built on Kafka (Redpanda in the bundled Compose stack) — an incremental step toward Zeebe's partitioned execution model, without a rewrite of the working engine. See the right-hand branch in the [global diagram](#architecture) above for where this fits; full design and phase-by-phase history: [`architecture/event-sourced-kafka-design.md`](architecture/event-sourced-kafka-design.md).

- **Opt-in, reversible per definition** — flip a process definition with `PATCH /engine-rest/process-definition/:id/write-path { "writePath": "partitioned" }`. Everything not flipped keeps running on the original, unmodified engine untouched by any of this.
- **How it works** — for a partitioned definition, a **Command Gateway** produces a command to a Kafka topic keyed by process-instance ID instead of writing to Postgres directly. A **Partition Processor** — one owner per Kafka partition, via a standard consumer group — is the only thing that ever mutates that instance's state, and it does so by calling the *exact same* `internal/service` / `internal/runtime` functions the default engine already uses. Postgres is still the durable store; Kafka only decides serialization order and ownership.
- **What's gated today** — start, complete, fail, retry-reset, incident-resolution, timer firing, and termination are all routed through the gateway for a partitioned instance, so nothing can bypass partition ownership and race the processor's writes.
- **Validated shapes** — a deploy-time shape validator accepts a linear service-task chain, an exclusive-gateway diamond, a DMN business rule task, an attached boundary error event, a single intermediate timer, and a parallel-gateway fork/join (AND semantics, unconditional — unlike the diamond's XOR). Anything else (inclusive gateways, multi-instance, message/signal correlation, process-instance modification) is explicitly rejected at flip-time rather than silently unsafe.
- **Requires** `KAFKA_BROKERS` — already wired to the bundled Redpanda service in `docker-compose.yaml`, nothing extra to configure to try it locally.
- **Status** — real, tested code (dedicated suites 26–34 in the [integration tests](#integration-tests)), but experimental: not the default, and not yet merged to `main`.

---

## Integration Tests

34 test suites covering every major feature, run against a real Docker Postgres+app stack:

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

**[Event-Sourced Write Path](#event-sourced-write-path-experimental) suites** (26–34), each verifying one phase of the Kafka-partitioned engine end to end, plus a regression pass of every earlier shape:

| Suite | Feature |
|---|---|
| 26 | Event Log Shadow Mode (Phase 0 — observability only, no write-path change) |
| 27 | Partitioned Write Path — linear shape (start/complete) |
| 28 | Partitioned Write Path — exclusive-gateway diamond shape |
| 29 | Partitioned Write Path — multi-task chain |
| 30 | Partitioned Write Path — DMN business rule task |
| 31 | Partitioned Write Path — job failure, retry-reset, incident resolution |
| 32 | Partitioned Write Path — intermediate timer event |
| 33 | Partitioned Write Path — instance termination + modification gating |
| 34 | Partitioned Write Path — parallel-gateway fork/join (AND semantics) |

---

## Environment Variables

The authoritative values are the ones baked into `docker-compose.yaml`'s `goflow` service — `.env` is only used for local (non-Docker) runs and uses different key names than the code expects in a couple of spots, so don't assume the two are in sync.

```bash
# Database
DB_HOST=postgres
DB_PORT=5432
DB_USER=goflow
DB_PASS=goflow123
DB_NAME=goflow

# JWT
goflow_secret_key=change-me-in-production

# Superuser (seeded on first boot)
SUPER_USER_NAME=admin
SUPER_USER_EMAIL=admin@goflow.com
SUPER_USER_PASSWORD=admin123

# OIDC (optional) — set all three to accept bearer tokens from an external
# identity provider alongside the built-in JWT login; leave unset to disable
# OIDC_ISSUER=https://your-idp.example.com/realms/goflow
# OIDC_AUDIENCE=goflow-api
# OIDC_JWKS_URL=https://your-idp.example.com/realms/goflow/protocol/openid-connect/certs

# Event-sourced write path (optional, experimental — see above)
# leave unset to disable both Kafka shadow-mode logging and the
# partitioned write path entirely
# KAFKA_BROKERS=redpanda:9092

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

Brings up a Citus coordinator + 2 workers alongside the app instead of the single default Postgres instance; the plain `docker compose up` command above is completely unaffected either way. See [`architecture/citus-distributed-storage-design.md`](architecture/citus-distributed-storage-design.md) for what's actually distributed today (config/lookup tables only, as Citus reference tables) and what's intentionally deferred.

---

## Comparison to Camunda 7 / Camunda 8

GoFlow deliberately targets both REST API shapes with one engine. It is **not** a drop-in replacement for either at scale — an honest summary:

**Solid parity with Camunda 7:** the `/engine-rest/...` surface covers the BPMN element set above, external task workers, DMN, incidents, message/signal correlation, multi-tenancy, a complete write-side audit log, and a real `ACT_RU_*`/`ACT_HI_*`-style split: finished instances are archived into dedicated historic tables and removed from the live ones, with `/engine-rest/history/...` (process instances, activity instances, tasks) and the audit/token-replay endpoints all transparently serving from whichever side an instance currently lives on.

**Solid REST-shape parity with Camunda 8:** `/v2/...` mirrors job activation, user-tasks, process-instance modification/search, DMN evaluation, forms, and flownode/incident/variable search. On top of that, a native gRPC gateway implements Zeebe's actual wire protocol for 11 of the Gateway's 23 RPCs — the core client loop (`Topology`, `DeployResource`, `CreateProcessInstance`, `ActivateJobs`, `CompleteJob`, `FailJob`) plus `PublishMessage`, `CancelProcessInstance`, `EvaluateDecision`, `ResolveIncident`, `ModifyProcessInstance` (see [Roadmap](#roadmap)) — so real Zeebe client SDKs can talk to GoFlow directly on port `26500`, not just HTTP clients.

**Where it's architecturally different, not just missing a feature:** the default engine is a single Postgres-backed transactional engine, not Zeebe's partitioned/distributed broker model. The app tier itself is stateless and already scales horizontally — job/timer claiming uses `SKIP LOCKED`, and cross-instance gRPC job wake-ups go through Postgres `LISTEN`/`NOTIFY` — but there's one shared Postgres primary behind it, so this scales up (bigger box, read replicas, sharding via an extension like Citus) rather than out the way Zeebe's Raft-replicated, no-shared-database partitions do. The [experimental Kafka-partitioned write path](#event-sourced-write-path-experimental) is a real, working step toward closing that gap for a growing (but still narrow) set of BPMN shapes — not a full answer yet.

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
- **Users/Roles RBAC hardening** — a follow-up audit of the admin feature above found and fixed several real gaps: `DeleteUser` used to hard-`DELETE`, which crashed with a raw FK-violation error for any user who had ever deployed a BPMN, started an instance, or appeared in an audit log (`deployments`/`process_instances`/`audit_logs` all reference `users.id` with no cascade, deliberately, to preserve history) — now a proper soft-delete (`is_active = false`, already checked at login) via a new `UserRepository.Deactivate`. `DeleteRole` now rejects with 409 while any user is still assigned to it, instead of silently cascading their permissions away. Four read routes (`GET /roles`, `GET /roles/:id`, `GET /users/:id/roles`, `GET /roles/:id/actions`) had no permission guard at all — now gated. A duplicate/misspelled permission code (`CAN_MANGE_USER` used everywhere vs. an unused, correctly-spelled `CAN_MANAGE_USERS`) was consolidated into one correct `CAN_MANAGE_USER` (`database/migrations/014_fix_can_manage_user_code.sql`). Added a real admin password-reset feature (`PUT /users/:id/reset-password`, gated by the previously-unused `CAN_RESET_PASSWORD` action) and the frontend's missing role-label-edit UI. Along the way, found and fixed a severe pre-existing bug unrelated to any of the above: `models.Role`'s `db:"isDefault"` struct tag didn't match the actual Postgres column (`isdefault`, no separator — unquoted DDL identifiers fold to lowercase), so every role read (`GetRole`, `ListRoles`, `GetDefaultRole`) was crashing with a 500 — caught only by a scripted end-to-end verification, not by code review, since the bug was invisible from reading the SQL alone.
- **Task lifecycle events in the Kafka event log** — an audit of the [event-sourced write path's](#event-sourced-write-path-experimental) domain events found job lifecycle fully instrumented but user-task lifecycle completely uninstrumented: not a missing call site, the proto schema had no `Task*` message type at all. Added `TaskCreated`/`TaskClaimed`/`TaskUnclaimed`/`TaskCompleted`/`TaskCancelled`, mirroring the field shapes of the older, separate `internal/events.TaskEventDispatcher` (SSE/WebSocket/webhook push) that was the only prior task observability signal. See [`architecture/event-sourced-kafka-design.md`](architecture/event-sourced-kafka-design.md) Phase 0 section for exactly which layer each event publishes from and why (task_repository.go can't import the event log directly — an import cycle).
- **gRPC gateway — Zeebe protocol core loop** — a native gRPC server (`internal/zeebegrpc`, port `26500`) implementing 13 of 19 Gateway RPCs (the full RPC set the `zeebe-node` client this project tests against actually supports) as thin translations onto the same service functions REST already uses, verified against the real SDK. `ActivateJobs`'s long-poll wakes near-instantly on a new job via a broadcast channel instead of a fixed poll interval. `UpdateJobRetries` and `BroadcastSignal` were added as a deliberately scoped batch — both wrap an existing function verbatim (`service.SetJobRetries`, `SignalController.Broadcast`) with zero new business logic, chosen over a wider batch (`SetVariables`, `ThrowError`, `CreateProcessInstanceWithResult`, `StreamActivatedJobs`, `DeleteResource`) that each need either new plumbing or their own sizing pass.
- **Process-graph cache fix** — the cache is now keyed by process *definition* (not instance) and bounded, closing an unbounded-growth, no-dedup bug.
- **Two FEEL→CEL evaluator bugs fixed** — numeric variables weren't coerced to a common type before evaluation, and keyword substitution (`and`/`or`) used to corrupt matching text inside quoted string literals.
- **Composite index for fetch-and-lock** — the hottest query in the system got a partial composite index, measured ~18x faster at realistic table sizes.
- **Analytics (Optimize-lite)** — a duration/throughput/incident-rate endpoint and frontend page, merging live and historic data. Building it surfaced and fixed a real incident-archival cascade-delete bug and a `time.Time`-as-map-key bug that silently zeroed the throughput series.
- **Cross-instance gRPC job wake-ups** — job creation now broadcasts via Postgres `LISTEN`/`NOTIFY` instead of an in-process-only channel, closing the one real gap that was blocking the app tier from scaling cleanly across multiple instances.
- **Event-sourced Kafka write path** — see [above](#event-sourced-write-path-experimental) for the current, still-narrow shape and command coverage; full phase-by-phase history in [`architecture/event-sourced-kafka-design.md`](architecture/event-sourced-kafka-design.md).
- **Citus-backed distributed storage, Phases 1-2** — an opt-in `docker-compose.citus.yml` override brings up a Citus coordinator + 2 workers alongside the app. Phase 1: global config/lookup tables (definitions, deployments, users/roles, forms, DMN, permissions) are Citus reference tables. Phase 2: the actual per-instance runtime tables — `process_instances`, `executions`, `tasks`, `variables`, `gateway_state`, `multi_instance_executions`, `incidents` — are real, sharded, colocated Citus distributed tables, verified end to end against a real Citus cluster (not just reasoned about). This is a different architectural axis from the Kafka write path — storage sharding rather than write-path ordering — see [`architecture/citus-distributed-storage-design.md`](architecture/citus-distributed-storage-design.md) for the full story, including two genuine pre-existing races (a termination/completion race, a parallel-gateway-join race) that Citus's added network latency exposed and this work fixed for both the Citus and default deployments. `jobs`/`timer_jobs` deliberately stay undistributed — their global `FOR UPDATE SKIP LOCKED` claim queries need their own redesign first, the one remaining piece of this effort.
- **Citus performance follow-up: single-shard routing** — the queries against those six per-instance tables that used to filter by row ID alone (correct, but a scatter-gather across every shard) now also filter by `process_instance_id`, confirmed via `EXPLAIN` on the coordinator to flip the plan from `Task Count: 32` to `Task Count: 1`. Found and fixed a real, silent regression along the way: two `gateway_state` lookup queries weren't selecting `process_instance_id`, so a join's second-arriving branch computed its follow-up update against a zero-value UUID and silently updated zero rows — any 2+ branch parallel/inclusive gateway join got permanently stuck right after the join with no error logged. See [`architecture/citus-distributed-storage-design.md`](architecture/citus-distributed-storage-design.md#6-performance-follow-up--route-single-shard-instead-of-scatter-gather-done-2026-07-05) for the full inventory and fix.
- **Parallel gateway (AND fork/join) on the partitioned write path** — turned out to be architecturally free, same as the DMN business-rule-task phase: `runtime.ExecuteExecution`'s existing fork/join handling (shared with the legacy/Citus paths) already works generically regardless of who calls it, so the only change needed was a new shape in the deploy-time validator. Sibling branches' job completions are never actually concurrent at the write layer — every command for one instance hashes to the same Kafka partition and is processed strictly serially — so none of the `FOR UPDATE`-locking concerns the legacy/Citus path needs apply here. See [`architecture/event-sourced-kafka-design.md`](architecture/event-sourced-kafka-design.md) "Phase 7" for the full story.
- **Two more gRPC RPCs: `UpdateJobRetries` and `BroadcastSignal`** — brings the gateway to 13 of the 19 RPCs the `zeebe-node` client this project verifies against actually supports. Both are thin wrappers with zero new business logic: `UpdateJobRetries` mirrors the existing REST `SetJobRetries` handler exactly, including its partitioned-write-path gate; `BroadcastSignal` needs no such gate at all, since `internal/partition.ValidateShape` never allows a signal catch event in any partitioned-write-path shape, so a partitioned instance can structurally never be one of its subscribers. Verified against the real client SDK, not a hand-rolled one.
- **Job/timer claiming redesign — fully specified, deliberately not implemented** — scoping this surfaced a real, empirically-confirmed constraint that changed the plan: Citus refuses to plan a `FOR UPDATE` claim query on a distributed table unless it's filtered to exactly one literal instance ID, so a Partition Processor genuinely cannot "claim across the instances it owns" in a single query the way Phase 1's original assessment assumed. The resulting design (a small Citus reference table indexing claimable work, separate from a distributed table holding the full payload) is written up in full in [`architecture/citus-distributed-storage-design.md`](architecture/citus-distributed-storage-design.md) section 5 as a ready-to-build plan, not built this pass — the actual implementation touches every job/timer creation, claim, and cleanup path and deserves its own session.
- **In-browser BPMN Modeler** — a new `/modeler` page swaps the existing view-only `NavigatedViewer` for bpmn-js's editable `Modeler` build (already the same npm package, no new dependency for the canvas itself), plus `bpmn-js-properties-panel` + `zeebe-bpmn-moddle` (two new dependencies) so task types, gateway conditions, and every other `zeebe:`-namespaced property that actually makes a diagram deployable can be set from the UI, not just shapes and labels. New/Import/Export/Deploy toolbar; Deploy reuses the exact same `deployBpmn` REST call the file-upload flow in Definitions already uses. Verified end to end with a scripted deploy-and-run cycle of a Modeler-equivalent diagram (not just a build check) — the actual canvas/palette/properties-panel rendering itself needs a real browser to verify, which wasn't available in this environment.
- **Per-instance permission-filtering audit** — the existing VIEW/START/MANAGE process-definition ACL only gated the process-definition list/start endpoints; everything downstream of a running instance (process-instance detail/variables/suspend/terminate, historic process/activity lists, audit logs + token history, incidents, analytics' per-key breakdown, task lists) had no permission check at all. Added a shared `service.CanAccessProcessInstance(db, userID, instanceID, level)` helper (resolves an instance ID to its owning process key, checking the live table then the historic archive, then delegates to the existing `CanAccessProcess`) and wired it into ~25 handlers across `process_instance_controller.go`, `task_controller.go`, `history_controller.go`, `historic_task_controller.go`, `audit_controller.go`, `incident_controller.go`, and `analytics_controller.go` — single-resource endpoints return 404 (matching the existing `GetDefinition` precedent of not revealing a restricted resource exists), list endpoints filter per-row. Found and removed 6 confirmed-dead functions (4 on `TaskController`, 2 on `TaskRepository`) and one dead duplicate route registration, discovered while tracing which handlers were actually reachable. The standout finding was unrelated to permissions as such: `/token-history` was listed in `pkg/middleware.PublicRoutes`, an allowlist matched by **substring**, not prefix — so `GET /engine-rest/audit/process/:id/token-history` bypassed authentication entirely, not just the ACL, for anyone with no token at all. Fixed by removing the entry; regression-covered in suite 23's new test [E].
- **Removed Redis** — it had been fully vestigial for some time: no Go code ever constructed a client or called it (no `go-redis`/`redigo` dependency at all), the cross-instance job-wake mechanism has used Postgres `LISTEN`/`NOTIFY` from the start (see the `Cross-instance gRPC job wake-ups` entry above), and the `redis`/`redis-commander` services existed only as dead orchestration weight — `goflow`'s `depends_on: redis: condition: service_healthy` blocked app startup on a container the binary never talked to. Removed both services, the `redis_data` volume, and the unused `Redis*` fields/`RedisAddr()`/`GetRedisOptions()` from `internal/config/config.go`. Verified the stack starts clean and a spot-check of core suites still passes with Redis gone entirely.
- **Fixed a real nginx/Vite reverse-proxy gap: Admin UI couldn't create a user or a role** — `frontend/nginx.conf`'s proxy regex (`^/(engine-rest|auth|users|events|v2)/`) never listed `roles` or `actions` at all, and required a trailing path segment, so an exact `POST /users` (no further segment) also silently missed it — all four fell through to the SPA fallback and got HTML back instead of a JSON response. `vite.config.ts`'s dev-server proxy had the identical `roles`/`actions` gap. This was invisible to every test in this project so far because they all call the Go backend directly on `:8080`, bypassing nginx/Vite entirely. Fixed both proxy configs (`(/|$)` instead of a bare `/`, plus the two missing prefixes) and added a first real **Playwright** browser e2e suite (`frontend/e2e/`, `npm run test:e2e`) that drives an actual Chromium instance against the dockerized frontend on port 3000 — specifically to catch this whole bug class going forward, since it's structurally invisible to API-level scripted tests. Confirmed the new tests fail against the old proxy config and pass against the fix, not just that they pass. Also added `Cache-Control: no-store` on every proxied API response, closing a related staleness risk (a browser's private cache heuristically retaining an old fallback-HTML response, or a stale JWT surviving a dev-environment DB reset, both silently manifesting as "the list is empty and I can't add anything" with no visible error).
- **Process-definition BPMN diagram viewer + live token-count overlay** — the Definitions page's chevron link and "N versions" link had pointed at `/processes/:id` since some earlier pass, but that route was never registered — a dead link. Added the missing `/processes/:id` page, reusing the existing `BpmnViewer` component (previously only used on the per-instance detail page) to render the deployed XML read-only. On top of that, added a live "where are running instances currently sitting" overlay: a new `GET /engine-rest/process-definition/key/:key/token-counts` endpoint aggregates active/waiting executions across every running/suspended instance of a key, grouped by BPMN node, polled every 3s and rendered as a cyan badge distinct from the per-instance colored badges on the instance-detail page. Building the query surfaced a real bug: `process_instances` has no `process_key` column at all (only `process_definition_id`) — `process_key` lives on `process_definitions` and needs an explicit join, caught immediately by a live Postgres error rather than silently returning wrong data. Verified end to end with Playwright against a real deployed+started instance, not just a build check. Followed up with: (1) an inline process-instance list at the bottom of the same page (ID, current node, started/ended, status), polled every 5s, with a click-to-open variables modal (Name/Type/Value, reusing the same rendering as the instance-detail page) instead of navigating away; (2) a per-instance "Current Node" column, powered by a new `GET .../key/:key/instance-positions` endpoint (the same live snapshot as token-counts, broken out per instance instead of aggregated) — showing at a glance where each running instance's token currently sits, without opening the full replay view.

### Medium-term (real features)

- **Remaining gRPC RPCs** — `SetVariables`, `ThrowError`, `CreateProcessInstanceWithResult`, `StreamActivatedJobs`, and `DeleteResource` from the client SDK's supported set, plus `MigrateProcessInstance`/`UpdateJobTimeout` from newer Zeebe versions the vendored client doesn't even expose yet, if a real client workflow needs them. `MigrateProcessInstance` specifically has no existing primitive to wrap — remapping a running instance to a different process-definition version is new engine logic, not a thin protocol translation like every other RPC added so far. `DeleteResource` likewise has no existing deployment-deletion API surface to reuse.
- **More BPMN shapes on the partitioned write path** — inclusive gateways, multi-instance, message/signal correlation, user tasks (would need a brand-new Kafka command type, since none exists for "complete a user task" today), and process-instance modification are all explicitly rejected by today's shape validator; each is a real, scoped follow-on rather than a redesign.

### Long-term / architectural (flagged, not committed)

- **Full partitioned/distributed execution** — two real, working pieces of progress exist toward this now: the [experimental Kafka write path](#event-sourced-write-path-experimental) (partition ownership for commands) and [Citus-backed distributed storage](architecture/citus-distributed-storage-design.md) (the core per-instance tables are genuinely sharded and colocated now, not just config tables). Neither is the whole picture yet, and hasn't been merged to `main` — `jobs`/`timer_jobs` still can't distribute until their claim-query redesign happens, and that redesign turned out to need more than aligning hash keys: Citus rejects any `FOR UPDATE` claim query on a distributed table that isn't filtered to exactly one literal instance ID (confirmed empirically — not even a known set of owned instance IDs is allowed), so a Partition Processor can't simply "claim across the instances it owns" in one query. The fully-specified design (a small Citus reference table indexing claimable work, separate from the distributed table holding the full job/timer payload) is written up in [`architecture/citus-distributed-storage-design.md`](architecture/citus-distributed-storage-design.md) section 5, not yet implemented. Meanwhile the default engine's app tier already scales horizontally against one Postgres (stateless instances, `SKIP LOCKED` claiming, cross-instance job wake-ups via `LISTEN`/`NOTIFY`).
