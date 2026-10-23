# GoFlow — Cloud-Native BPMN Workflow Engine

![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![BPMN 2.0](https://img.shields.io/badge/BPMN-2.0-orange)
![React](https://img.shields.io/badge/Frontend-React%2019-61DAFB?logo=react&logoColor=black)

GoFlow is a BPMN 2.0 workflow engine written in Go that runs models with Camunda 8 semantics, exposing REST APIs shaped after both **Camunda 7** (`/engine-rest/...`) and **Camunda 8** (`/v2/...`), plus a native **Zeebe gRPC gateway**. It's a lightweight, self-hosted alternative to the Java-based engines: PostgreSQL is its only database (a single server, a high-availability cluster with automatic failover, or a Citus cluster sharding the per-instance tables), the application runs as one instance or several behind a load balancer, and it comes with real-time event streaming and a React operations/tasklist frontend. Like Camunda 8, it can run every command of an instance on the one GoFlow instance owning its Kafka partition, with failover between instances. See [Comparison to Camunda 7 / Camunda 8](docs/comparison.md#comparison-to-camunda-7--camunda-8) for an honest look at what's covered and what isn't, and [Switching from Camunda 7 / Camunda 8](docs/switching-from-camunda.md#switching-from-camunda-7--camunda-8) for what an existing Camunda application has to change.

## Contents

- [Current State](#current-state-v10)
- [Quick Start](#quick-start)
- [Frontend](#frontend)
- [Architecture](#architecture)
- [Documentation](#documentation)

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
| Previous step (send a token back to its earlier wait state) | ✅ |
| Audit trail (`audit_logs`, one row per state transition) | ✅ |
| Token-history replay (derived from the audit trail, see below) | ✅ |
| History API (process + activity instances) | ⚠️ basic — see [gaps](docs/comparison.md#comparison-to-camunda-7--camunda-8) |
| SSE real-time events (`/events/tasks`) | ✅ |
| WebSocket real-time events (`/ws/tasks`) | ✅ |
| React frontend (Operate + Tasklist-lite) | ✅ |
| Docker / Docker Compose deployment | ✅ |
| Horizontal scaling: several instances behind nginx, PgBouncer, read replica, background archiving (see [Operations](docs/operations.md#horizontal-scaling)) | ✅ |
| Postgres high availability: Patroni cluster, synchronous replication, automatic failover through HAProxy (see [Operations](docs/operations.md#postgres-high-availability)) | ✅ |
| Backups (`pg_dump`, standalone or through the cluster) with a restore check | ✅ |
| gRPC / native Zeebe protocol | ✅ all 19 RPCs of the zeebe-node 8.3 client protocol (`MigrateProcessInstance`/`UpdateJobTimeout` from newer Zeebe versions not yet) |
| Partitioned write path, Camunda 8 style (`WRITE_PATH=partitioned`, see [below](docs/architecture.md#partitioned-write-path)) | ✅ |

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

The API is available at `http://localhost:8080` (REST) and `localhost:26500` (gRPC — Zeebe protocol core loop, see [Roadmap](docs/roadmap.md#roadmap)).

---

## Frontend

A React 19 + `bpmn-js` single-page app (`frontend/`, dockerized behind nginx) covering a slice of Camunda's Operate + Tasklist:

- **Dashboard** — at-a-glance counts (running/completed instances, open tasks, incidents)
- **Process Definitions / Process Instances** — deploy, browse, filter by key/state
- **Instance Detail** — live BPMN diagram with an active-token overlay, plus a **replay control**: play/pause/step/scrub through the instance's full audit-log-derived token history, with a distinct pulsing marker on the diagram showing where the token was at each step (independent of the live-state overlay). Clicking a row in the Activity History timeline jumps the replay straight to that step. Also supports Move Token (process instance modification) and **Previous step** (send a token back to the wait state before, with a preview of where it goes) from here.
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

Design decisions and the partitioned write path: [docs/architecture.md](docs/architecture.md).

---

## Documentation

- [Switching from Camunda 7 / Camunda 8](docs/switching-from-camunda.md) — what an application already on Camunda changes, client by client
- [Comparison to Camunda 7 / Camunda 8](docs/comparison.md) — what's covered and what isn't
- [API Reference](docs/api.md) — REST (`/engine-rest`, `/v2`), events, audit trail
- [Architecture](docs/architecture.md) — design decisions, partitioned write path
- [Environment Variables](docs/configuration.md) — every setting
- [Operations](docs/operations.md) — migrations, metrics, backups, scaling, high availability
- [Ajouter des serveurs](docs/adding-servers.md) — de 1 à 4 serveurs et plus : quels fichiers, et ce qu'il faut faire côté base de données à chaque étape
- [Integration Tests](docs/testing.md) — the suites and how to run them
- [Roadmap](docs/roadmap.md) — what's done and what's next
- [`architecture/`](architecture/) — design documents (partitioned write path, Citus storage, history of the event-sourced phases)
- [`database/migrations/`](database/migrations/README.md) — the SQL migrations, applied by hand
