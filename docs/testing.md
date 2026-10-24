# Integration Tests

48 test suites covering every major feature, run against a real Docker Postgres+app stack. Suite 54 also needs a JDK 17+ (Maven is downloaded by the wrapper).

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

**BPMN coverage, Camunda 8 semantics, Camunda 7 task API** (35–52):

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
| 51 | Camunda 7 task API (`/engine-rest/task`): query, count, claim, assignee, typed variables, complete |
| 52 | Camunda 7 application and worker: start by key with a business key, `fetchAndLock` fields and long polling, `extendLock`, `unlock`, `bpmnError`, `failure` retries |
| 53 | Previous step (step a token back): paths, refusals across a fork, stale job rejected, permissions |
| 54 | Java clients: runs [examples/java-client](../examples/java-client) and [examples/java-client-spring](../examples/java-client-spring) — plain REST, the Camunda 7 External Task Client (Basic and JWT), the Camunda 8 Java client over gRPC, the Camunda 7 OpenAPI REST client, a Camunda 7 Java-API application through the REST client Spring Boot starter (see [Java client](java-client.md)) |

To run against a GoFlow that isn't on `127.0.0.1:8080`, set `GOFLOW_URL` (and `GOFLOW_GRPC` for suite 54). `SKIP_SETUP=1` runs against the stack already up instead of rebuilding it with fresh volumes.

The same suites run against several instances behind the load balancer, and against the high-availability cluster (`COMPOSE_FILE` with the files of [Operations](operations.md#operations), plus `GOFLOW_REPLICAS=3` for several instances). With `WRITE_PATH=partitioned` they run on the partitioned write path, and with `COMPOSE_FILE="docker-compose.yaml;docker-compose.citus.yml"` on Citus.
