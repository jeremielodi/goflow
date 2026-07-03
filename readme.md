# GoFlow — Cloud-Native BPMN Workflow Engine

GoFlow is a production-ready BPMN 2.0 workflow engine written in Go, exposing REST APIs shaped after both **Camunda 7** (`/engine-rest/...`) and **Camunda 8** (`/v2/...`). It is designed as a lightweight, self-hosted alternative to traditional Java-based engines, with a single PostgreSQL backend, real-time event streaming, and a React operations/tasklist frontend. See [Comparison to Camunda 7 / Camunda 8](#comparison-to-camunda-7--camunda-8) for an honest look at what's covered and what isn't.

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
| Horizontal scaling (PostgreSQL SKIP LOCKED) | ✅ |
| gRPC / native Zeebe protocol | ⚠️ 11 of 23 RPCs — see [Roadmap](#roadmap) |

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

Not covered by the frontend: a BPMN Modeler (diagrams are authored externally and deployed as files, not edited in-browser).

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   GoFlow (Go / Fiber)                │
│                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │
│  │  Parser  │  │  Engine  │  │    Runtime       │  │
│  │ BPMN XML │→ │  Build   │→ │  executeNode()   │  │
│  │ → structs│  │  Graph   │  │  per-type files  │  │
│  └──────────┘  └──────────┘  └──────────────────┘  │
│                                       │              │
│  ┌────────────────────────────────────▼───────────┐  │
│  │                  Repository                   │  │
│  │   EngineRepository / EventSubscriptionRepo    │  │
│  └────────────────────────────────────────────────┘  │
│                         │                           │
│          ┌──────────────▼──────────┐               │
│          │  PostgreSQL 16          │               │
│          │  process_definitions    │               │
│          │  process_instances      │               │
│          │  executions             │               │
│          │  jobs / variables       │               │
│          │  event_subscriptions    │               │
│          │  timer_jobs             │               │
│          └─────────────────────────┘               │
└─────────────────────────────────────────────────────┘
```

### Key Design Decisions

- **NodeExecutor pattern** — each BPMN node type has its own `executor_<type>.go` file, all private methods on `*Runtime`
- **PostgreSQL SKIP LOCKED** — timers and jobs use `SELECT … FOR UPDATE SKIP LOCKED` for horizontal scaling without a queue broker
- **Graph cache** — parsed process graphs are stored as JSONB in `process_definitions.parsed_graph`, rebuilt only on deploy
- **Event subscriptions** — message/signal/EBG waiting is implemented via an `event_subscriptions` table; correlation is a SQL query
- **Call Activity** — child instance stores `parent_instance_id` + `parent_execution_id`; end event handler resumes parent

---

## Integration Tests

22 test suites covering every major feature, run against a real Docker Postgres+app stack:

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
```

---

## Comparison to Camunda 7 / Camunda 8

GoFlow deliberately targets both REST API shapes with one engine. It is **not** a drop-in replacement for either at scale — an honest summary:

**Solid parity with Camunda 7:** the `/engine-rest/...` surface covers the BPMN element set above, external task workers, DMN, incidents, message/signal correlation, multi-tenancy, a complete write-side audit log, and — since the historic-archive work — a real `ACT_RU_*`/`ACT_HI_*`-style split: finished instances are archived into dedicated historic tables and removed from the live ones, with `/engine-rest/history/...` (process instances, activity instances, tasks) and the audit/token-replay endpoints all transparently serving from whichever side an instance currently lives on.

**Solid REST-shape parity with Camunda 8:** `/v2/...` mirrors job activation, user-tasks, process-instance modification/search, DMN evaluation, forms, and flownode/incident/variable search. On top of that, a native gRPC gateway now implements Zeebe's actual wire protocol for 11 of the Gateway's 23 RPCs — the core client loop (`Topology`, `DeployResource`, `CreateProcessInstance`, `ActivateJobs`, `CompleteJob`, `FailJob`) plus `PublishMessage`, `CancelProcessInstance`, `EvaluateDecision`, `ResolveIncident`, `ModifyProcessInstance` (see [Roadmap](#roadmap)) — so real Zeebe client SDKs can talk to GoFlow directly on port `26500`, not just HTTP clients. One thing remains architecturally different, not just a missing feature:
- Single Postgres-backed monolith, not Zeebe's partitioned/distributed broker model — fine for small-to-medium workloads, not a horizontal-scaling story like Zeebe's.

**Frontend:** the React app covers a real slice of Operate (instance inspection, plus a token-replay feature Operate doesn't offer by default), Tasklist (task inbox), and now a slice of Optimize (`/analytics` — duration distributions, throughput over time, incident rate by process key), but there's still no Modeler (BPMN authored externally).

---

## Roadmap

Prioritized by value vs. effort, based on gaps identified above and during the audit-trail/replay work.

### Done

- **Soft-delete on process termination** — `TerminateProcess` used to hard-delete the instance, which cascaded and erased its own `audit_logs` the moment it was terminated. Fixed: it now marks `status='terminated'` and explicitly cancels open tasks, marks active executions terminal, cancels pending jobs, and removes not-yet-fired timers in one transaction — everything a hard-delete's FK cascade used to clean up implicitly, done explicitly instead. (Also found and removed a duplicate, unused `DeleteProcessInstance` handler that was registered on the exact same route and still hard-deleted — a latent route-shadowing bug.)
- **Multi-token-aware replay** — the token-history endpoint and the frontend replay UI now group steps by `executionId`, so a parallel gateway or multi-instance task shows one marker per concurrently active token instead of one ambiguous marker jumping between branches.
- **Consolidated the duplicate "start process" code paths** — `StartProcess`, `v2 CreateProcessInstance`, `StartByDefinitionID`, and the async worker's `processStartProcess` now all share one `service.StartProcessInstance` function instead of four independent (and previously divergent) implementations.
- **A genuine historic archive** — dedicated `historic_process_instances`/`historic_activity_instances`/`historic_tasks`/`historic_variables` tables, decoupled from the live tables, matching Camunda 7's `ACT_RU_*` (runtime) vs `ACT_HI_*` (history) split. Once an instance completes or terminates, `service.ArchiveAndDeleteProcessInstance` copies its data across and hard-deletes the live rows — live tables now only ever hold currently-running instances. Every read endpoint that could touch a finished instance (`GET /process-instance/:id`, its variables, `GET /audit/process/:id/token-history`, the whole History API, `POST /v2/process-instances/search`, `POST /v2/user-tasks/search`) transparently falls back to (or merges with) the historic tables, so nothing changed shape for callers — including zero changes needed in the frontend. The `HistoricTaskController` (previously misrouted at bare `/history/tasks`, unreachable through nginx) got fixed and folded into this work at `/engine-rest/history/task(/:id)(/count)`.
- **Admin UI (Users/Roles) + per-resource process permissions** — the existing Users/Roles/Actions REST API now has a real frontend under an "Admin" nav section (`pages/admin/Users.tsx`, `Roles.tsx`), gated on the current user's own actions (`useAuth`'s `hasAction`, now populated from `login`/`GET /users/me`, previously discarded). On top of that, a new per-resource ACL lets specific users/roles be granted VIEW/START/MANAGE access to a *specific* process (keyed by `process_key`, covering every version) — `process_permissions` table, `CanAccessProcess` (`internal/service/process_authorization_service.go`), enforced at every list/get/start/deploy call site across both the v1 and v2 APIs. A process with no grants is unrestricted (open to any authenticated user, unchanged from before); granting access to anyone makes it restricted to just those grantees plus holders of the existing global `CAN_MANAGE_DEPLOY_PROCESS` action. Deploying a **new** process key still requires that global action (unchanged); deploying a new version of an **existing** restricted key only requires MANAGE on that specific key, which a per-resource grant can now satisfy on its own. Scoped to process definitions only this pass — deployments have no standalone list/read/delete surface in this app today, so a parallel deployment-level ACL would have nowhere to be used; the schema's `resource_type` column is left generic for that later if it's ever needed.
- **gRPC gateway — Zeebe protocol core loop** — a native gRPC server (`internal/zeebegrpc`, port `26500`, Zeebe's own default gateway port) implements 9 of the Gateway protocol's 23 RPCs: the client core loop (`Topology`, `DeployResource`, `CreateProcessInstance`, `ActivateJobs` (server-streaming long-poll), `CompleteJob`, `FailJob`) plus `PublishMessage`, `CancelProcessInstance`, `EvaluateDecision` — each a thin translation onto the same service function the equivalent REST endpoint already uses (`MessageController.Correlate`, the newly-extracted `service.TerminateProcessInstance`, `dmn.EvaluateDecisionTable`), no new engine logic. The other 14 RPCs (`ResolveIncident`, `ModifyProcessInstance`, `MigrateProcessInstance`, etc.) aren't registered, so a real client calling them gets a standard gRPC `Unimplemented` status, the same experience as hitting an unsupported feature on a limited broker. Verified against the real `zeebe-node` SDK, not a hand-rolled client. Two pieces of new, unavoidable-for-compatibility infrastructure: a `zeebe_keys` table mapping GoFlow's UUIDs to the `int64` keys the protocol requires (assigned lazily, persisted so a key survives restarts), and `DeployResources`/`CompleteJob`/`FailJob`/`TerminateProcessInstance` were extracted out of their REST controllers into shared `internal/service` functions so gRPC and REST run the exact same engine logic. gRPC auth reuses the existing JWT validation via an `authorization: Bearer` metadata header, falling back to the seeded `super_admin` identity when no token is sent — matching self-managed Zeebe's own "no auth configured" default while still enforcing the same per-resource ACL for callers that do authenticate. `ActivateJobs`'s long-poll now wakes near-instantly on a new job instead of a fixed poll interval, via a small in-process per-job-type broadcast channel (`repository.NotifyJobAvailable`/`JobWakeChannel`) — a 2s fallback poll remains for jobs whose lock simply expired, which fire no such event.
- **Process-graph cache fixed to dedupe per definition** — `EngineRepository.GetProcessGraphByInstanceID`'s in-process cache used to key by `process_instance_id` and never evict, so every instance of the same process definition cached its own identical copy of the graph forever. Now keyed by `process_definition_id` (the graph is a property of the definition, not the instance) with a small `instanceID->definitionID` lookup so cache hits still cost zero DB round trips, and both maps are bounded (matching the existing `condition.go` compilation-cache pattern) instead of growing without limit.
- **Two real bugs fixed in the FEEL→CEL condition evaluator** (`internal/common/condition.go`) — the documented CEL "arithmetic limitation" was actually variables never being numerically normalized before evaluation: a Go `int` mixed with a `float64` in the same expression threw a CEL "no matching overload" error (confirmed directly against cel-go), now fixed by coercing all numeric variables to `float64` first. Separately, the FEEL→CEL keyword substitution (`and`/`or`/`=`/`not`) used to run across the whole expression string including quoted content, so a literal like `"cash and carry"` was silently corrupted into `"cash && carry"` before comparison; substitutions now only apply outside quoted segments.
- **Composite index for the hottest query in the system** — `FetchAndLockJobsTx` (called by every REST fetch-and-lock call, the v2 job controller, and gRPC `ActivateJobs`'s poll loop) filters on `job_type` + `status='pending'` but only had independent single-column indexes on each, forcing a bitmap AND. A new partial composite index (`jobs(job_type, status) WHERE status = 'pending'`) is a measured ~18x faster / ~770x fewer buffer reads for that exact query at realistic table sizes.
- **Basic analytics (Optimize-lite)** — a new `GET /engine-rest/analytics/process-stats` endpoint and `/analytics` frontend page (Recharts) report duration distributions (avg/p50/p95 via Postgres's `PERCENTILE_CONT`), throughput over time, and incident rate by process key, merging live and historic data in Go the same way the rest of the archive-aware read paths already do. Building this surfaced a real, pre-existing bug: `incidents.process_instance_id` has `ON DELETE CASCADE`, and the archive pipeline (`ArchiveAndDeleteProcessInstance`) never copied incidents anywhere before hard-deleting a finished instance's live row — every incident, including resolved ones, was silently destroyed the moment its instance archived. Fixed with a new `historic_incidents` table (same denormalized-`process_definition_key` pattern as `historic_process_instances`), archived in whatever state they're in so a still-open incident on a force-terminated instance doesn't vanish either. A second bug was caught only by checking real numbers, not just that the endpoint ran: the throughput series' day-bucket maps were keyed by `time.Time`, which carries Location/monotonic metadata — a value built via `time.Now()` and one scanned back from Postgres compare unequal as map keys even for the same calendar day, so every "started"/"completed" count silently came back zero; fixed by keying on a formatted date string instead.
- **2 more gRPC RPCs** — `ResolveIncident` (wraps a previously dead-code `IncidentRepository.Resolve`, never called from any REST path before this) and `ModifyProcessInstance` (wraps the existing `runtime.ModifyInstance`, already used by `POST /v2/process-instances/:id/modification`). Incidents never had a `zeebe_keys` entry minted anywhere (no gRPC-side incident listing exists to do it lazily, and real Zeebe's own Gateway protocol has none either), so `GetIncident`/`ListIncidents` REST responses now also return a `zeebeKey` field as the bridge. `ModifyProcessInstance` only supports one implied token move per call, deriving the source element from the instance's single active execution — the installed `zeebe-node` SDK's `TerminateInstruction` carries only an opaque `elementInstanceKey` GoFlow has no per-execution mapping for, and real Zeebe's newer dedicated `moveInstructions` field predates that SDK version, so it isn't used.

### Medium-term (real features)

- **In-browser BPMN Modeler** — `bpmn-js` is already a frontend dependency in view-only (`NavigatedViewer`) mode; swapping to the editable `Modeler` build for authoring + deploying from the browser is a moderate lift, not a new dependency.
- **Remaining gRPC RPCs** — `MigrateProcessInstance` and the rest of the 12 not yet implemented, if a real client workflow needs them. `MigrateProcessInstance` specifically has no existing primitive to wrap (confirmed via a full-repo search) — remapping a running instance to a different process-definition version is new engine logic, not a thin protocol translation like every other RPC added so far.

### Long-term / architectural (flagged, not committed)

- **Partitioned/distributed execution** — genuinely replicating Zeebe's horizontal-scaling model (partitioning, Raft-replicated logs, no shared central database) is a different engine, not an incremental change to this one — see [Comparison](#comparison-to-camunda-7--camunda-8) above. Noted here for completeness, not planned.
