# Integration Tests

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

The same suites run against several instances behind the load balancer, and against the high-availability cluster (`COMPOSE_FILE` with the files of [Operations](operations.md#operations), plus `GOFLOW_REPLICAS=3` for several instances). With `WRITE_PATH=partitioned` they run on the partitioned write path, and with `COMPOSE_FILE="docker-compose.yaml;docker-compose.citus.yml"` on Citus.
