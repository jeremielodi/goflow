# Switching from Camunda 7 / Camunda 8

What an application already running against Camunda needs to change to run against GoFlow. "Tested" means covered by the integration suites with that client; anything else is expected to work from the API shape but hasn't been verified with that client.

## Camunda 8 (Zeebe gRPC, `/v2` REST)

| Area | Status | What to do |
|---|---|---|
| Zeebe gRPC clients | ✅ tested with **zeebe-node 8.3** (all 19 RPCs) | Point the gateway address to `goflow-host:26500`. The Java client, Spring Zeebe and `@camunda8/sdk` haven't been tested; `MigrateProcessInstance` and `UpdateJobTimeout` (newer clients) don't exist yet. |
| Authentication (gRPC) | ⚠️ | GoFlow checks its own JWT (`POST /auth/login`), sent as `authorization: Bearer …`. A client configured with OAuth against Camunda Identity won't authenticate. **A call with no token runs as the first super_admin** — keep port 26500 off untrusted networks. |
| `/v2` REST | ✅ subset (list below) | Same paths and request shapes. **Keys are UUID strings**, not 64-bit integers: a client typing keys as numbers (the Java client's `long`) breaks; one treating them as strings works. |
| BPMN models | ✅ | Run as they are: `zeebe:` task definitions, I/O mappings, FEEL, called elements, multi-instance, message subscriptions, timers, signals, escalations, compensation, user tasks with forms. |
| Operate, Tasklist, Optimize, Connectors, Web Modeler | ❌ | Not provided. GoFlow has its own frontend (instance inspection, task inbox, analytics, in-browser modeler). The Desktop Modeler can deploy through the gRPC gateway. |

`/v2` endpoints available: `POST /v2/process-instances`, `GET /v2/process-instances/:key`, `POST /v2/process-instances/search`, `POST /v2/process-instances/:key/modification`, `POST /v2/jobs/activation`, `POST /v2/jobs/:key/completion`, `/failure`, `/error`, `POST /v2/user-tasks/search`, `POST /v2/user-tasks/:key/completion`, `/assignment`, `DELETE /v2/user-tasks/:key/assignee`, `POST /v2/messages/publication`, `POST /v2/signals/broadcast`, `POST /v2/incidents/search`, `POST /v2/incidents/:key/resolution`, `POST /v2/flownode-instances/search`, `POST /v2/variables/search`, `POST /v2/decisions/:key/evaluation`, `GET /v2/forms/:formKey`, `POST /v2/resources/deployments`, `GET /v2/topology`.

## Camunda 7 (`/engine-rest`)

| Area | Status | What to do |
|---|---|---|
| Authentication | ✅ | HTTP Basic (GoFlow user e-mail and password) or a GoFlow JWT. |
| External task workers | ⚠️ partial | `fetchAndLock`, `complete`, `failure` exist. **`handleBpmnError`, `extendLock` and `unlock` don't**: a worker using them gets an error (throw a BPMN error with `failure` carrying an `errorCode` and `retries: 0`, or with `POST /v2/jobs/:id/error`). `asyncResponseTimeout` (long polling) is ignored: `fetchAndLock` answers at once. The official external task clients haven't been tested. |
| Starting instances | ⚠️ different path | `POST /engine-rest/process-definition/key/{key}/start` doesn't exist: use `POST /engine-rest/v2/process-definitions/{key}/start` or `POST /engine-rest/process-definition/{id}/start`. |
| User tasks | ✅ | `/engine-rest/task/...` as in Camunda 7: query (GET and POST), count, get, claim, unclaim, assignee, complete, submit-form, form key, variables — see [API Reference](api.md#camunda-7-task-api-engine-resttask). Not provided: delegation, candidate users/groups management, comments, attachments, identity links. |
| Deployment | ✅ | `POST /engine-rest/deployment/create`, multipart, with `deployment-name` (and optionally `tenant-id`). |
| Process instances, variables, incidents, history, messages, signals | ✅ | `/engine-rest/process-instance/...` (variables, suspension, deletion), `/engine-rest/incident`, `POST /engine-rest/job/{id}/retries`, `/engine-rest/history/process-instance`, `/history/activity-instance`, `/history/task`, `POST /engine-rest/message`, `POST /engine-rest/signal`. |
| BPMN models | ⚠️ must be adapted | **No Java**: `camunda:class`, `camunda:delegateExpression`, JUEL `camunda:expression` and Java listeners can't run. Turn Java service tasks into external tasks (`camunda:type="external"` with `camunda:topic`, or a `zeebe:taskDefinition` type) handled by a worker, and conditions into FEEL. Camunda 8 models need no change, so moving a Camunda 7 model to GoFlow is close to the Camunda 7 → 8 migration. |
| Cockpit, Tasklist, Admin webapps | ❌ | Not provided; see the GoFlow frontend. |
