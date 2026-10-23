# API Reference

## Authentication

```http
POST /auth/login
{ "email": "...", "password": "..." }
→ { "accessToken": "...", "refreshToken": "..." }

POST /auth/refresh
POST /auth/logout
```

All other endpoints require `Authorization: Bearer <accessToken>`.

## Process Definitions

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
```

## Process Instances

```http
# Start by process key (latest version)
POST /engine-rest/v2/process-definitions/:key/start
{ "variables": { "orderId": "123" } }

# Start by definition ID
POST /engine-rest/process-definition/:id/start

# List running instances
GET /engine-rest/process-instance
GET /engine-rest/process-instance?processDefinitionKey=myProcess
GET /engine-rest/process-instance?businessKey=ORDER-42

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

## User Tasks

```http
GET  /engine-rest/tasks                     (list, filter by processInstanceId / assignee)
POST /engine-rest/tasks/:id/complete        { "variables": {...} }
POST /engine-rest/tasks/:id/claim           { "assignee": "..." }
POST /engine-rest/tasks/:id/unclaim
```

### Camunda 7 task API (`/engine-rest/task`)

Camunda 7's paths, query parameters, `TaskDto` and typed-variable formats, dates (`2013-01-23T13:42:42.000+0200`), status codes and error bodies (`{"type": "InvalidRequestException", "message": …}`), so a Camunda 7 application works unchanged:

```http
GET|POST /engine-rest/task                   query: processInstanceId(In), processDefinitionId/Key, taskId(In),
                                             taskDefinitionKey(In), executionId, name(Like), assignee(Like),
                                             assigned, unassigned, candidateGroup, tenantIdIn, active, suspended,
                                             sortBy/sortOrder (or "sorting" in the POST body), firstResult, maxResults
GET|POST /engine-rest/task/count             → { "count": n }
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

## External Tasks (Service Workers)

Camunda 7's external task API, with its request and response shapes:

```http
POST /engine-rest/external-task/fetchAndLock
{
  "workerId": "my-worker",
  "maxTasks": 10,
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

## Messages & Signals

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

## History

```http
GET /engine-rest/history/process-instance
GET /engine-rest/history/process-instance?processDefinitionKey=myProcess&state=completed
GET /engine-rest/history/process-instance/:id

GET /engine-rest/history/activity-instance?processInstanceId=:id
```

## Incidents

```http
GET    /engine-rest/incident
GET    /engine-rest/incident/:id
DELETE /engine-rest/incident/:id
POST   /engine-rest/job/:id/retries   { "retries": 3 }
```

## Audit Trail & Token-History Replay

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
POST /v2/deployments                          (also POST /v2/resources/deployments)
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
```

`POST /v2/process-instances/:id/step-back` (the **Previous step** button on the instance page) sends a waiting token back to the wait state it went through before: `{ "executionId": "…" }` or `{ "elementId": "approve" }` (when one token waits there), optional `"steps": n` (default 1) and `"dryRun": true` (only answers where it would go). The token keeps its execution and variables; its task or job is cancelled, along with its boundary events, timers, subscriptions and open incidents, and the earlier element runs again with a new task or job. Nothing done since then is undone. Pressing it twice goes back two steps. Wait states are user and service tasks, catch events, event-based gateways, sub-processes and call activities; gateways, script and business-rule tasks are skipped. Refused (409 `STEP_BACK_REFUSED`, with the reason) across a parallel or inclusive gateway, out of the token's sub-process, on multi-instance activities, and on instances that aren't running. Like modification, it needs `MANAGE` on the process (404 otherwise).

`POST /v2/messages/publication` and the gRPC `PublishMessage` follow Camunda 8: the message goes to the subscriptions whose `zeebe:subscription correlationKey` evaluates to the published `correlationKey` — at most once per process — or starts the processes waiting on it with a message start event. With a `timeToLive` (ms), it is kept and correlates to subscriptions that open later; a `messageId` already published and alive is refused (409 / `ALREADY_EXISTS`). Nothing waiting is not an error. Processes without a `correlationKey` expression keep matching a key equal to their `correlationKey` variable, as before. `POST /engine-rest/message` keeps Camunda 7's semantics.
