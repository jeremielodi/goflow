# API Reference

## Conventions

These rules apply to every list and search endpoint below. The tables only state where an endpoint differs from them.

- **Authentication.** Every endpoint except `/auth/*`, `/health` and `/` requires `Authorization: Bearer <access_token>`.
- **Tenant.** There is no tenant parameter on most lists. A user that belongs to a tenant only ever sees that tenant's definitions, instances and tasks; a user without a tenant (the superuser) sees all of them. The Camunda 7 task API also accepts `tenantIdIn`.
- **Permissions.** Lists drop the rows of processes the caller has no `VIEW` grant on (see [process permissions](#process-permissions)). A process with no grant at all is open to every authenticated user. Single-resource reads, and the `/v2` searches that require a `processInstanceKey`, answer `404` rather than `403`.
- **Combining filters.** Several filters combine with AND. A filter left out, or sent empty, does not filter.
- **Exact match.** Filters are exact and case-sensitive unless the table says otherwise (`…Like`).
- **Enumerated values.** `status` and `state` must be one of the values listed. On the `/engine-rest` lists, an unknown value is a `500` (`invalid input value for enum`). On the `/v2` searches it is ignored, and the search returns everything.
- **Dates.** Dates are RFC 3339, for example `2026-10-24T09:30:00Z` or `2026-10-24T11:30:00+02:00`. In a query string, encode `+` as `%2B`. A date that doesn't parse is ignored without an error, so double-check the format if a date filter seems to have no effect.
- **Pagination.** Only the endpoints that list `firstResult` / `maxResults` paginate; all others return every match. `firstResult` is the 0-based offset.
- **Live and archived data.** A finished instance moves to the `historic_*` tables about half a second after it ends. The history endpoints read both. The runtime lists (`/engine-rest/process-instance`, `/engine-rest/tasks`, `/engine-rest/incident`, …) mostly see live data only.

## Authentication

```http
POST /auth/login
{ "email": "...", "password": "..." }
→ { "access_token": "...", "refresh_token": "...", ... }

POST /auth/refresh   { "refresh_token": "..." }   (or the refresh_token cookie)
POST /auth/logout
```

## Process Definitions

```http
# Deploy a BPMN file
POST /engine-rest/deployment/create
Content-Type: multipart/form-data
resources=@process.bpmn, deployment-name=...

# Camunda 8 style
POST /engine-rest/v2/deployments

# List definitions
GET /engine-rest/process-definition

# Get single definition
GET /engine-rest/process-definition/:id

# By key, as in Camunda 7 (ProcessDefinitionDto)
GET /engine-rest/process-definition/key/:key
GET /engine-rest/process-definition/key/:key/tenant-id/:tenantId

# Start an instance, as in Camunda 7 (also .../submit-form)
POST /engine-rest/process-definition/key/:key/start
POST /engine-rest/process-definition/key/:key/tenant-id/:tenantId/start
POST /engine-rest/process-definition/:id/start
{ "variables": { "amount": { "value": 120, "type": "Integer" } }, "businessKey": "ORDER-42", "withVariablesInReturn": false }
→ { "id", "definitionId", "businessKey", "ended", "suspended", "tenantId", "links",
    "processInstanceId", "executionId", "processKey", "version" }   (variables: typed or plain)

# Live view of one definition, across its running instances
GET /engine-rest/process-definition/key/:key/token-counts        → active tokens per element
GET /engine-rest/process-definition/key/:key/instance-positions  → current element(s) per instance
```

**`GET /engine-rest/process-definition`**

| Parameter | Type | Effect |
|---|---|---|
| `key` | string | Only the versions of this process key. |
| `latestVersion` | `true` | Only the latest version of each key. With `key`, returns a one-element array, or `404` if the key doesn't exist. |

```http
GET /engine-rest/process-definition?key=myProcess
GET /engine-rest/process-definition?key=myProcess&latestVersion=true
GET /engine-rest/process-definition?latestVersion=true
```

Each definition also carries Camunda 7's `suspended` (always `false`: GoFlow doesn't suspend definitions), `startableInTasklist` (`true`), `versionTag`, `description`, `category` and `resource`, which Camunda 7 clients expect.

### Process permissions

```http
GET    /engine-rest/process-definition/key/:key/permissions
POST   /engine-rest/process-definition/key/:key/permissions   { "granteeType": "user" | "role", "granteeId": "...", "permission": "VIEW" | "START" | "MANAGE" }
DELETE /engine-rest/process-definition/key/:key/permissions/:id
```

The first grant on a key restricts it to its grantees; holders of the global `CAN_MANAGE_DEPLOY_PROCESS` action keep full access.

## Process Instances

```http
# Start by process key (latest version)
POST /engine-rest/v2/process-definitions/:key/start
{ "variables": { "orderId": "123" } }

# Start by definition ID
POST /engine-rest/process-definition/:id/start

# List instances
GET /engine-rest/process-instance

# Get instance
GET /engine-rest/process-instance/:id

# Variables
GET    /engine-rest/process-instance/:id/variables
GET    /engine-rest/process-instance/:id/variables/:name
POST   /engine-rest/process-instance/:id/variables
PUT    /engine-rest/process-instance/:id/variables/:name
DELETE /engine-rest/process-instance/:id/variables/:name
DELETE /engine-rest/process-instance/:id/variables

# Lifecycle
PUT    /engine-rest/process-instance/:id/suspended    { "suspended": true | false }
DELETE /engine-rest/process-instance/:id              (terminate)
GET    /engine-rest/process-instance/:id/state
GET    /engine-rest/process-instance/:id/incidents
```

**`GET /engine-rest/process-instance`** — newest first.

| Parameter | Type | Effect |
|---|---|---|
| `status` | `running` \| `suspended` \| `completed` \| `terminated` | Instance status. In practice `running` or `suspended`: finished instances leave this table once archived (use the history API for them). |
| `processKey` (alias `processDefinitionKey`) | string | Process key, any version. |
| `businessKey` | string | Business key. |

```http
GET /engine-rest/process-instance?processDefinitionKey=myProcess&status=running
GET /engine-rest/process-instance?businessKey=ORDER-42
```

## User Tasks

```http
GET  /engine-rest/tasks                     (list, see filters below)
POST /engine-rest/tasks/:id/complete        { "variables": {...} }
POST /engine-rest/tasks/:id/claim           { "assignee": "..." }
POST /engine-rest/tasks/:id/unclaim
```

**`GET /engine-rest/tasks`** (also `GET /tasks`) — oldest first. Open tasks only (`created`, `claimed`), unless `status` is given.

| Parameter | Type | Effect |
|---|---|---|
| `processInstanceId` | UUID | Tasks of one instance. |
| `assignee` | string | Assigned to this user. |
| `candidateGroup` | string | Offered to this group. |
| `status` | `created` \| `claimed` \| `completed` \| `cancelled` | This status only. Includes closed tasks when asked, as long as their instance isn't archived yet. |
| `id` | UUID | One task. |

```http
GET /engine-rest/tasks?candidateGroup=managers
GET /engine-rest/tasks?assignee=alice&status=claimed
```

Each task comes back with its `formKey` when its element has one.

### Camunda 7 task API (`/engine-rest/task`)

Camunda 7's paths, query parameters, `TaskDto` and typed-variable formats, dates (`2013-01-23T13:42:42.000+0200`), status codes and error bodies (`{"type": "InvalidRequestException", "message": …}`), so a Camunda 7 application works unchanged:

```http
GET|POST /engine-rest/task                   (list, see filters below)
GET|POST /engine-rest/task/count             → { "count": n }   (same filters)
GET  /engine-rest/task/:id
POST /engine-rest/task/:id/claim             { "userId": "..." }   204; 500 TaskAlreadyClaimedException if someone else holds it
POST /engine-rest/task/:id/unclaim           204
POST /engine-rest/task/:id/assignee          { "userId": "..." | null }
POST /engine-rest/task/:id/complete          { "variables": { "x": { "value": 1, "type": "Integer" } }, "withVariablesInReturn": false }
POST /engine-rest/task/:id/submit-form       same as complete
GET  /engine-rest/task/:id/form              { "key": "<camunda:formKey>" }
GET  /engine-rest/task/:id/variables         typed: String, Boolean, Integer, Long, Double, Json (value as a JSON string), Null
GET  /engine-rest/task/:id/form-variables
GET  /engine-rest/task/:id/variables/:name
PUT  /engine-rest/task/:id/variables/:name   { "value": ..., "type": ... }
POST /engine-rest/task/:id/variables         { "modifications": { ... } }   (deletions not supported)
```

**`GET|POST /engine-rest/task`** — open tasks only. With `GET`, filters go in the query string and lists are comma-separated (`taskIdIn=a,b`). With `POST`, the same names go in a JSON body and lists are arrays. `firstResult` and `maxResults` stay in the query string in both cases.

| Parameter | Type | Effect |
|---|---|---|
| `processInstanceId` | UUID | One instance. |
| `processInstanceIdIn` | list of UUIDs | Any of these instances. |
| `processDefinitionId` | UUID | One process definition (one version). |
| `processDefinitionKey` | string | One process key, any version. |
| `executionId` | UUID | The task of this execution (token). |
| `taskId` / `taskIdIn` | UUID / list | One task / any of these tasks. |
| `taskDefinitionKey` / `taskDefinitionKeyIn` | string / list | BPMN element ID(s) of the user task. |
| `name` | string | Exact task name. |
| `nameLike` | string | SQL `LIKE` on the name, case-sensitive: put the `%` yourself (`nameLike=%25review%25` in a URL). |
| `assignee` | string | Exact assignee. |
| `assigneeLike` | string | SQL `LIKE` on the assignee, same rules as `nameLike`. |
| `assigned` | `true` | Only tasks with an assignee. |
| `unassigned` | `true` | Only tasks without an assignee. |
| `candidateGroup` | string | Offered to this group. |
| `tenantIdIn` | list | Instances of these tenants. |
| `active` | `true` | Instance not suspended. |
| `suspended` | `true` | Instance suspended. |
| `sortBy` + `sortOrder` | see below / `asc` \| `desc` | One sort key. Unknown keys are ignored. |
| `sorting` (POST body only) | `[{ "sortBy", "sortOrder" }]` | Several sort keys, in order. |
| `firstResult`, `maxResults` | integers | Pagination (default: everything). |

Sort keys: `id`, `name`, `assignee`, `created`, `priority`, `processInstanceId`, `taskDefinitionKey`, `lastUpdated`. Ties are broken by creation date, oldest first.

```http
GET  /engine-rest/task?candidateGroup=managers&unassigned=true&sortBy=created&sortOrder=desc&maxResults=20
POST /engine-rest/task?firstResult=0&maxResults=50
{ "processDefinitionKey": "invoice", "taskDefinitionKeyIn": ["approve", "review"], "sorting": [{ "sortBy": "priority", "sortOrder": "desc" }] }
```

**`GET /engine-rest/task/:id/variables`** (and `/form-variables`)

| Parameter | Type | Effect |
|---|---|---|
| `variableNames` | comma-separated list | Only these variables. |

### Variable instances (`/engine-rest/variable-instance`)

Camunda 7's variable query, which `RuntimeService.getVariables` calls when it runs over REST:

```http
GET|POST /engine-rest/variable-instance          (firstResult, maxResults)
GET|POST /engine-rest/variable-instance/count    → { "count": n }
→ [{ "id", "name", "type", "value", "valueInfo", "processInstanceId", "executionId",
     "activityInstanceId", "processDefinitionId", "tenantId", … }]
```

| Parameter | Type | Effect |
|---|---|---|
| `processInstanceIdIn` | list of UUIDs | Variables of these instances. |
| `executionIdIn` | list of UUIDs | Same (GoFlow keeps an instance's variables at its own scope, so `executionId` is the process instance id). |
| `variableName` / `variableNameIn` | string / list | These names. |
| `variableNameLike` | string | SQL `LIKE` on the name, case-sensitive (`%` and `_`). |
| `tenantIdIn` | list | Instances of these tenants. |

Running and suspended instances only; sub-process and multi-instance local variables aren't listed.

## External Tasks (Service Workers)

Camunda 7's external task API, with its request and response shapes:

```http
POST /engine-rest/external-task/fetchAndLock
{
  "workerId": "my-worker",
  "maxTasks": 10,
  "usePriority": true,
  "asyncResponseTimeout": 10000,        // long polling (ms, capped at 60 s): wait for a task when none is ready
  "topics": [{ "topicName": "send-email", "lockDuration": 30000, "variables": ["to", "subject"] }]   // variables: only these (absent or empty: all)
}
→ [{ "id", "topicName", "workerId", "activityId", "activityInstanceId", "executionId",
     "processInstanceId", "processDefinitionId", "processDefinitionKey", "businessKey",
     "tenantId", "retries", "lockExpirationTime", "priority", "variables": { "to": { "value", "type" } } }]

POST /engine-rest/external-task/:id/complete    { "workerId": "...", "variables": { "x": { "value": 1, "type": "Integer" } } }
POST /engine-rest/external-task/:id/failure     { "workerId": "...", "errorMessage": "...", "retries": 2 }
     retries is what remains after this failure: 0 raises an incident
POST /engine-rest/external-task/:id/bpmnError   { "workerId": "...", "errorCode": "...", "errorMessage": "...", "variables": {...} }
     caught by the nearest error boundary / error event sub-process / calling process; uncaught: an incident
POST /engine-rest/external-task/:id/extendLock  { "workerId": "...", "newDuration": 60000 }   500 if another worker holds it
POST /engine-rest/external-task/:id/unlock
```

### Jobs (read-only)

```http
GET /jobs            (list, see filters below)
GET /jobs/:id        (also /engine-rest/jobs/:id)
```

**`GET /jobs`** — newest first, every status (`pending`, `locked`, `completed`, `failed`, `cancelled`).

| Parameter | Type | Effect |
|---|---|---|
| `processInstanceId` | UUID | Jobs of one instance. A value that isn't a UUID matches nothing. |

When nothing matches, the body is `null` rather than `[]`.
| `topic` | string | Job type (the service task's topic / `zeebe:taskDefinition type`). |

## Timers (read-only)

```http
GET /timers          (list, see filters below)
GET /timers/:id
```

**`GET /timers`** — newest first, fired timers included (`is_triggered`).

| Parameter | Type | Effect |
|---|---|---|
| `processInstanceId` | UUID | Timers of one instance (`400` if not a UUID). |

## Messages & Signals

```http
# Correlate a message to a waiting process, as in Camunda 7
POST /engine-rest/message
{
  "messageName": "PaymentReceived",
  "correlationKeys": { "orderId": { "value": "123", "type": "String" } },
  "processVariables": { "paymentRef": { "value": "PAY-001", "type": "String" } },
  "resultEnabled": true
}
→ 204 (no body), or with resultEnabled:
  [{ "resultType": "Execution", "execution": { "id", "processInstanceId", "ended", "tenantId" },
     "processInstance": null, "variables": {…} }]   (variables with variablesInResultEnabled)

# Broadcast signal to ALL waiting instances
POST /engine-rest/signal
{
  "name": "RestockAlert",
  "variables": { "product": "widget" }
}
```

**`POST /engine-rest/message`** body (Camunda 7's `CorrelationMessageDto`):

| Field | Type | Effect |
|---|---|---|
| `messageName` | string | **Required.** |
| `correlationKeys` | object | Process variables the instance must have; values plain (`"123"`) or typed (`{"value", "type"}`). GoFlow matches on the first key. |
| `businessKey` | string | Only the instance with this business key. |
| `processInstanceId` | UUID | Only this instance. |
| `processVariables` | object | Variables set on the instance, plain or typed. `variables` (GoFlow's earlier name) still works. |
| `all` | boolean | Correlate every waiting execution, not just the first (at most 1000). |
| `resultEnabled` | boolean | Reply with the list of correlations instead of `204`. |
| `variablesInResultEnabled` | boolean | Include each instance's variables in that list. |

Nothing waiting for the message is `400` with `{"type": "RestException", "message": "org.camunda.bpm.engine.MismatchingMessageCorrelationException: …"}`, as in Camunda 7 (a token that hasn't reached the catch event yet gives the same answer: retry). Not supported: `localCorrelationKeys`, `processVariablesLocal`, `tenantId`, starting an instance from a message start event (use `POST /v2/messages/publication`).

## History

Reads live and archived data together: running instances and instances that ended long ago come back from the same call.

```http
GET  /engine-rest/history/process-instance
POST /engine-rest/history/process-instance    (filters in a JSON body, as Camunda 7 clients send them)
GET  /engine-rest/history/process-instance/:id

GET /engine-rest/history/activity-instance

GET /engine-rest/history/task
GET /engine-rest/history/task/count          → { "count": n }   (same filters)
GET /engine-rest/history/task/:id
```

**`GET|POST /engine-rest/history/process-instance`** — newest first (by start date). With `POST`, the filters go in a JSON body (`firstResult` and `maxResults` stay in the query string).

Each instance's `state` is Camunda 7's: `ACTIVE`, `SUSPENDED`, `COMPLETED` or `EXTERNALLY_TERMINATED` (GoFlow's terminated instances were stopped through the API). The response also carries `processDefinitionName`, `processDefinitionVersion`, `tenantId` and `deleteReason`.

| Parameter | Type | Effect |
|---|---|---|
| `processInstanceId` | UUID | One instance. |
| `processInstanceIds` | comma-separated UUIDs | These instances. |
| `processDefinitionKey` | string | Process key, any version. |
| `businessKey` (or Camunda 7's `processInstanceBusinessKey`) | string | Business key. |
| `state` | `ACTIVE` \| `SUSPENDED` \| `COMPLETED` \| `EXTERNALLY_TERMINATED`, or GoFlow's `active` \| `suspended` \| `completed` \| `terminated` | Instance state. |
| `active`, `suspended`, `completed`, `externallyTerminated` | `true` | Camunda 7's way to ask for one state (ignored when `state` is given). |
| `finished` / `unfinished` | `true` | Ended / not ended. |
| `startedAfter` | date | Started strictly after. |
| `startedBefore` | date | Started strictly before. |
| `finishedAfter` | date | Ended strictly after (running instances never match). |
| `firstResult` | integer | Offset (default 0). |
| `maxResults` | integer | Page size (default 100, at most 1000). |

Dates here are RFC 3339 or Camunda 7's format (`2026-10-01T00:00:00.000+0200`).

```http
GET /engine-rest/history/process-instance?processDefinitionKey=invoice&completed=true&finishedAfter=2026-10-01T00:00:00Z&maxResults=50
```

**`GET /engine-rest/history/activity-instance`** — one row per token (execution), oldest first. Archived instances fall back to one row per element visited.

| Parameter | Type | Effect |
|---|---|---|
| `processInstanceId` | UUID | Activities of one instance. Without it, the activities of every running instance. |
| `firstResult` | integer | Offset (default 0). |
| `maxResults` | integer | Page size (default 100, at most 1000). |

A row with no `endTime` is a token still there.

**`GET /engine-rest/history/task`** (and `/count`) — open and closed tasks, newest first.

| Parameter | Type | Effect |
|---|---|---|
| `taskId` | UUID | One task. |
| `taskName` | string | Exact task name. |
| `taskNameLike` | string | Task name contains this, case-insensitive (no `%` needed). |
| `assignee` | string | Exact assignee. |
| `assigneeLike` | string | Assignee contains this, case-insensitive. |
| `processInstanceId` | UUID | One instance. |
| `processDefinitionId` | UUID | One process definition (one version). |
| `processDefinitionKey` | string | One process key, any version. |
| `status` | `created` \| `claimed` \| `completed` \| `cancelled` | Task status. |
| `finished` | `true` | Completed tasks only. |
| `unfinished` | `true` | Tasks not completed (open or cancelled). |
| `startedAfter` / `startedBefore` | date | Created at or after / at or before. |
| `finishedAfter` / `finishedBefore` | date | Completed at or after / at or before. |
| `firstResult` | integer | Offset (default 0). |
| `maxResults` | integer | Page size (default 100). |

`sortBy` / `sortOrder` are accepted for Camunda 7 compatibility but have no effect yet: results are always newest first.

```http
GET /engine-rest/history/task?processDefinitionKey=invoice&finished=true&assigneeLike=ali
GET /engine-rest/history/task/count?unfinished=true&startedBefore=2026-10-20T00:00:00Z
```

## Incidents

```http
GET    /engine-rest/incident                       (list, see filters below)
GET    /engine-rest/incident/:id
DELETE /engine-rest/incident/:id
GET    /engine-rest/process-instance/:id/incidents
POST   /engine-rest/job/:id/retries   { "retries": 3 }
POST   /v2/incidents/:id/resolution
```

**`GET /engine-rest/incident`** — newest first. Incidents of archived instances aren't listed here.

| Parameter | Type | Effect |
|---|---|---|
| `processInstanceId` | UUID | One instance (`400` if not a UUID). |
| `state` | `open` \| `resolved` \| `deleted` | Incident state. |
| `incidentType` | `failedExternalTask` \| `executionError` \| `unhandledErrorEvent` | What raised it: a job out of retries, an element that failed to run (condition, script, input…), or a BPMN error nobody caught. |
| `activityId` | string | BPMN element ID where it happened. |

```http
GET /engine-rest/incident?state=open&incidentType=failedExternalTask
```

## Analytics

**`GET /engine-rest/analytics/process-stats`** — counts, durations and throughput per process key, over a time window.

| Parameter | Type | Effect |
|---|---|---|
| `processKey` | string | One process key (`404` without `VIEW` on it). Without it, every key the caller can see. |
| `days` | integer | Window of the last N days (default 30). |
| `from` / `to` | date | Explicit window; overrides `days`. `to` defaults to now. |

The cross-process `throughput` series is not filtered by permission.

## Audit Trail & Token-History Replay

Every state transition (process start/complete/terminate/suspend, execution created/moved/waited/completed, task created/claimed/completed/cancelled, job locked/completed/failed, gateway fork/join, timer created/triggered/cancelled, message correlation, signal broadcast) is written to `audit_logs`. The token-history endpoint turns that raw trail into an ordered, human-readable path — this is what the frontend's diagram replay (play/pause/step/scrub, see below) consumes.

```http
# Raw audit log for an instance, chronological
GET /engine-rest/audit/process/:processId

# Derived token path: one step per element the token visited, with a
# readable `detail` string and (where applicable) an `elementId` you can
# highlight on the BPMN diagram
GET /engine-rest/audit/process/:processId/token-history
→ { "processInstanceId", "steps": [...], "count" }

GET  /engine-rest/audit/task/:taskId
POST /engine-rest/audit/date-range   { "start": "2026-10-01T00:00:00Z", "end": "2026-10-24T00:00:00Z" }
```

The raw rows of an instance are deleted when it is archived; its token history then comes from `historic_activity_instances` instead. `date-range` also returns entries not tied to an instance (logins, user and role administration).

## Event Log (Kafka shadow mode)

```http
GET /engine-rest/eventlog/recent       → { "events": [...] }   (503 when the shadow-mode consumer is off)
GET /engine-rest/eventlog/projected    → { "instances": [...], "jobs": [...] }
```

No filters.

## Users, Roles & Actions

```http
GET    /users                     (CAN_MANAGE_USER)
GET    /users/me
GET    /users/:id
GET    /users/email/:email
POST   /users                     { "email", "full_name", "password" }
PUT    /users/:id
PUT    /users/:id/password
PUT    /users/:id/reset-password  (CAN_RESET_PASSWORD)
DELETE /users/:id                 (soft delete)

GET    /roles
GET    /roles/:id
POST   /roles                     (CAN_EDIT_ROLE)
PUT    /roles/:id
DELETE /roles/:id                 (refused while assigned)
GET    /users/:userId/roles
POST   /users/:userId/roles/:roleId
DELETE /users/:userId/roles/:roleId

GET    /actions
GET    /roles/:roleId/actions
POST   /roles/:roleId/actions/:actionId
DELETE /roles/:roleId/actions/:actionId
```

No filters or pagination: each list returns everything.

## Real-Time Events

```http
# Server-Sent Events (HTTP streaming)
GET /events/tasks

# WebSocket
ws://localhost:8080/ws/tasks
```

## Camunda 8 REST API (`/v2/...`)

Parallel to the Camunda-7-style surface above, the same engine is exposed with Camunda 8 ("Orchestration Cluster") request/response shapes — same underlying services, translated shapes only.

```http
POST /v2/resources/deployments                (also POST /engine-rest/v2/deployments)
POST /v2/process-definitions/:key/start
POST /v2/process-instances
GET  /v2/process-instances/:id
POST /v2/process-instances/search
POST /v2/process-instances/:id/modification   (Move Token)
POST /v2/process-instances/:id/step-back      (Previous step — GoFlow only)

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

GET  /v2/topology
```

### Search filters

Every search takes `{ "filter": { ... } }` and answers `{ "items": [...] }`. GoFlow supports the filter fields below; Camunda 8's other filter fields, `sort` and `page` are not supported yet and are ignored, so a search returns every match. An empty body means no filter.

**`POST /v2/process-instances/search`** — live and archived instances.

| Field | Type | Effect |
|---|---|---|
| `processDefinitionId` | string | The BPMN process ID (GoFlow's process key), any version. |
| `state` | `ACTIVE` \| `SUSPENDED` \| `COMPLETED` \| `TERMINATED` | Instance state. |

**`POST /v2/user-tasks/search`**

| Field | Type | Effect |
|---|---|---|
| `processInstanceKey` | UUID | One instance. Tasks of an instance that is already archived are found too (except together with `candidateGroup`). |
| `elementId` | string | BPMN element ID of the user task. |
| `assignee` | string | Exact assignee. |
| `candidateGroup` | string | Offered to this group. |
| `state` | `CREATED` \| `ASSIGNED` \| `COMPLETED` \| `CANCELED` | Task state. Without it, every state. |
| `priority` | integer | Exact priority. |

**`POST /v2/incidents/search`**

| Field | Type | Effect |
|---|---|---|
| `processInstanceKey` | UUID | One instance (`400` if not a UUID). |
| `state` | `ACTIVE` \| `RESOLVED` \| `DELETED` | Incident state. |
| `errorType` | `failedExternalTask` \| `executionError` \| `unhandledErrorEvent` | GoFlow's incident type (not Camunda 8's `JOB_NO_RETRIES` names). |
| `elementId` | string | BPMN element ID. |

**`POST /v2/flownode-instances/search`**

| Field | Type | Effect |
|---|---|---|
| `processInstanceKey` | UUID | **Required.** Every token of the instance, `ACTIVE` or not. |

**`POST /v2/variables/search`**

| Field | Type | Effect |
|---|---|---|
| `processInstanceKey` | UUID | **Required.** Instance-level variables of the instance. |
| `name` | string | One variable. |

**`POST /v2/jobs/activation`** is not a search, but takes these fields: `type` (required), `worker`, `timeout` (lock, ms), `maxJobsToActivate` (default 10).

```http
POST /v2/process-instances/search   { "filter": { "processDefinitionId": "invoice", "state": "ACTIVE" } }
POST /v2/user-tasks/search          { "filter": { "candidateGroup": "managers", "state": "CREATED" } }
POST /v2/incidents/search           { "filter": { "state": "ACTIVE", "errorType": "failedExternalTask" } }
```

### Previous step

`POST /v2/process-instances/:id/step-back` (the **Previous step** button on the instance page) sends a waiting token back to the wait state it went through before: `{ "executionId": "…" }` or `{ "elementId": "approve" }` (when one token waits there), optional `"steps": n` (default 1) and `"dryRun": true` (only answers where it would go). The token keeps its execution and variables; its task or job is cancelled, along with its boundary events, timers, subscriptions and open incidents, and the earlier element runs again with a new task or job. Nothing done since then is undone. Pressing it twice goes back two steps. Wait states are user and service tasks, catch events, event-based gateways, sub-processes and call activities; gateways, script and business-rule tasks are skipped. Refused (409 `STEP_BACK_REFUSED`, with the reason) across a parallel or inclusive gateway, out of the token's sub-process, on multi-instance activities, and on instances that aren't running. Like modification, it needs `MANAGE` on the process (404 otherwise).

### Messages

`POST /v2/messages/publication` and the gRPC `PublishMessage` follow Camunda 8: the message goes to the subscriptions whose `zeebe:subscription correlationKey` evaluates to the published `correlationKey` — at most once per process — or starts the processes waiting on it with a message start event. With a `timeToLive` (ms), it is kept and correlates to subscriptions that open later; a `messageId` already published and alive is refused (409 / `ALREADY_EXISTS`). Nothing waiting is not an error. Processes without a `correlationKey` expression keep matching a key equal to their `correlationKey` variable, as before. `POST /engine-rest/message` keeps Camunda 7's semantics.
