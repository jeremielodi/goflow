# GoFlow v2 — Event-Sourced, Kafka-Backed Engine (Design Scope)

Status: **Phase 0 (shadow mode) and Phase 1 (first vertical slice, partitioned
write path) both shipped on the `dev` branch, 2026-07-04.** Phases 2+ (more
BPMN constructs, a permanent-parallel-vs-cutover decision) remain scoped but
not committed. See "Phase 1 — what's actually running today" below for the
real, tested implementation, including one deliberate deviation from this
document's original Phase 1 sketch (§5/§9.2) that materially reduced risk —
explained in that section.

## Phase 0 — what's actually running today

The existing Postgres-transactional engine is **unchanged and remains the
only write path.** After every state-transition transaction commits, a
domain event is *also* published (fire-and-forget) to a Redpanda topic,
partitioned by `processInstanceId`. A background consumer both keeps an
in-memory debug ring buffer and folds a subset of events into read-model
tables — see below — proving the pipe is durable, ordered per instance,
well-formed, and genuinely capable of reconstructing state.

**Wired events, full coverage** (see `internal/eventlog/proto/events.proto`
for exact shapes) — 15 total, mirroring every generic transition already
instrumented by `internal/service/audit_service.go`'s `Log*` methods rather
than inventing new per-BPMN-element event logic:
- Core lifecycle (shipped first): `ProcessStarted`, `JobCreated`,
  `JobCompleted`, `JobFailed`, `IncidentResolved`, `ProcessTerminated`,
  `ProcessCompleted`.
- Extended coverage (shipped 2026-07-04): `ExecutionCreated`,
  `ExecutionMoved`, `ExecutionCompleted` (every BPMN element executor that
  moves a token — business rule/script/message/signal/timer/error-boundary
  tasks, exclusive/inclusive gateway advances, call-activity resume, process
  instance modification), `GatewayForked`/`GatewayJoined` (parallel and
  inclusive gateway fork/join, in `internal/runtime/runtime.go`),
  `TimerTriggered` (`internal/service/timer_scheduler.go`),
  `MessageCorrelated`/`SignalBroadcast` (`internal/api/message_controller.go`,
  `internal/api/signal_controller.go`). `SignalBroadcast` has no owning
  instance (a broadcast spans zero or more instances) and publishes with an
  empty partition key.
- ~30 call sites total wired, all following the identical
  `if p := eventlog.Global(); p != nil { p.X(...) }` idiom placed
  immediately after the corresponding existing audit call.

**Still not represented**: variable-update-only transitions with no
accompanying execution move (folded into the events above wherever they
already carry variables, but not a standalone event type).

## Read-model projector (Phase 0, not Phase 1)

`internal/eventlog.Consumer` folds a **coarse subset** of the event stream —
`ProcessStarted`/`ProcessCompleted`/`ProcessTerminated`/`JobCreated`/
`JobCompleted`/`JobFailed` only — into two new, purely additive Postgres
tables (`eventlog_projected_instances`, `eventlog_projected_jobs`;
`database/migrations/011_eventlog_projection.sql`), upserted one row per
event via `internal/repository/eventlog_projection_repository.go`. Nothing
else in the app reads or writes these tables. This is deliberately scoped to
prove "coarse instance/job state is reconstructable from events alone," not
a full read-model replacement — execution-level events (moves, gateways,
timers, correlation) remain visible only via the debug ring buffer.
Verified against real traffic post-ship: `GET /engine-rest/eventlog/projected`
showed 78 projected instances / 88 projected jobs with correct
statuses/timestamps/job-types matching the real engine's data, not just
passing assertions in isolation.

New debug endpoint: `GET /engine-rest/eventlog/projected` (alongside the
existing `/eventlog/recent` ring-buffer view), same dev-only framing.

**Resolved tech decisions** (were open questions in §11; superseded by real
constraints found in this codebase during implementation):
- **Redpanda** (`redpandadata/redpanda:v24.2.4`, single-node dev mode) over
  Kafka — wire-compatible, no ZooKeeper/KRaft cluster to run in
  `docker-compose.yaml`.
- **`segmentio/kafka-go`** over `confluent-kafka-go` — decisive, concrete
  reason: this repo's Dockerfile builds with `CGO_ENABLED=0
  -installsuffix cgo`; `confluent-kafka-go` requires cgo (wraps librdkafka)
  and cannot build in that image without changing it. `kafka-go` is pure Go.
- **Protobuf**, generated via `tools/protogen` (generalized from a single
  hardcoded `{protoDir, protoFile, outDir}` to a list of compile jobs, and
  fixed to resolve well-known-type imports like `google/protobuf/timestamp.proto`
  via `protocompile.WithStandardImports` plus a transitive-import collector —
  `protoc-gen-go` resolves types purely from the request's file list, so the
  full import closure has to be included, not just the top-level file).

**A real bug found and fixed during this work, not before it**: the first
implementation made each publish call *synchronous* (network round-trip to
Redpanda inside the request, logged-not-fatal on error). That still adds
real latency to the caller's critical path — and it shifted timing enough to
turn a **pre-existing, previously-benign race condition** in the inclusive
gateway's join logic (`gateway_state` insert, two concurrent job completions
racing for the same join) into an actual `duplicate key` failure, caught by
the full e2e regression (suite 05) after wiring `JobCompleted`. Fixed by
making every publish genuinely fire-and-forget — each spawns its own
goroutine with its own background context, so nothing in `internal/service`
or `internal/runtime`'s critical path ever blocks on Kafka, or on Phase 0
plumbing being configured/healthy at all. This is the concrete lesson for
Phase 1+: *any* new side-channel write into a hot path — even one that's
"best-effort" in its error handling — must also be fire-and-forget in its
latency, or it can silently perturb timing-sensitive existing code.

**Two more real bugs found while expanding coverage (not before it)**:
- A BPMN test fixture's XML comment contained the literal substring
  `--no-timers` (a flag name referenced in prose) — invalid inside an XML
  comment (`--` may not appear anywhere inside `<!-- -->`), which silently
  broke that fixture's deploy call.
- That deploy failure happened during test *setup*, before `runSuite(...)`
  was ever invoked — the test file's `run().catch(console.error)` printed
  the error but never set a nonzero exit code, so `run_all.ts` counted the
  entire suite (0 of 9 tests actually run) as passed. Same class of gap as
  the historical "runSuite exit-code bug" — that fix only covers failures
  *inside* the test loop, not setup failures before it. Fixed locally in
  this suite's footer (`run().catch(err => { console.error(err);
  process.exit(1); })`) — **note for later**: every other suite file in
  `examples/tests/` still uses the bare `run().catch(console.error)` form
  and has the same latent gap for setup-phase failures; not fixed
  repo-wide in this pass since it's outside this feature's scope, but worth
  a dedicated cleanup pass given it already caused one false-green result.

**Verification**: e2e suite `examples/tests/26_eventlog_shadow.test.ts`, 9
cases — the original 4 (clean completion, permanent-failure +
incident-resolution, termination) plus 5 more added with the extended
coverage: parallel gateway fork/join (reusing `03_meal_preparation.bpmn`),
a short (2s) timer fixture built for this suite specifically (existing
timer fixtures are 20-60s and reserved for the `--no-timers`-skipped
suites), message correlation (reusing `10_order_notification.bpmn`), signal
broadcast (reusing `11_inventory_broadcast.bpmn`), and projector parity
(cross-checking a projected row against the real REST API's view of the
same instance). Full 26-suite Docker regression green; manually verified
the projector against real traffic afterward (see above) rather than
trusting green tests alone.

**How to apply**: Phase 1 (an actual partition-owning write path) is a much
larger step than Phase 0 and still needs the open questions in §11 answered
first — target throughput, managed-vs-self-hosted Kafka, and the
eventual-consistency tradeoff in §6 — none of that was resolved by shipping
Phase 0, which only proves the transport, not the architecture change.

## Phase 1 — what's actually running today

Shipped the same day as this section was written, after the user explicitly
chose "full production-grade" infrastructure (real Kafka consumer-group
partition ownership/rebalancing, crash-safe offset-commit ordering, a
per-definition write-path flag) over a stripped prototype, plus **eventual
consistency, accepted** for the new path's read model (§6's second, simpler
option). BPMN coverage for this first vertical slice is deliberately narrow
— **start → one external service task → end, nothing else** — enforced by a
deploy-time graph-shape validator (`internal/partition.ValidateShape`),
following this doc's own §9.2 migration-path guidance to start with "one
narrow concern," not a full BPMN-parity rewrite. The legacy engine is
**completely unchanged** and remains the only write path for every process
definition not explicitly flipped.

**A deliberate deviation from this section's original sketch, found during
implementation, not before it**: §5's diagram describes a Partition Processor
that "runs `internal/engine` + `internal/common` (BPMN/FEEL, reused)" as if
re-implementing token-passing semantics as pure in-memory command handlers.
Building that revealed a simpler, safer option: `internal/engine`'s graph
compiler is indeed reusable as-is, but the actual per-element BPMN semantics
(multi-instance counting, variable merging, output-mapping evaluation, gateway
join thresholds) are not — they're deeply intertwined with `internal/runtime`
and `internal/service`'s existing, already-tested Postgres-transactional
functions. Rather than re-deriving that logic a second time (real risk of
subtle semantic divergence, on a scoped slice with no test coverage of its
own to catch it), **the Partition Processor calls the exact same
`service.StartProcessInstance` / `runtime.ExecuteExecution` /
`service.CompleteJob` functions the legacy REST/gRPC handlers already call**
— reusing their transactional writes and audit/eventlog side effects
verbatim. What changes is only *how* they're invoked: in strict per-partition
order, driven by a consumed Kafka command instead of directly inside a
request handler. Consequences:
- **Postgres itself is the state store** — no separate in-memory
  `InstanceRuntime` cache, no `partition_snapshots` table. A rebalance
  handoff to a different replica is trivially safe because there's no
  per-instance state to warm up or hand off; the new owner just resumes by
  calling the same functions, which read whatever the old owner last left in
  Postgres.
- **The existing `jobs`/`executions`/`process_instances` tables are shared**,
  not duplicated — a worker fetching jobs via the completely unmodified REST
  `fetchAndLock` or gRPC `ActivateJobs` gets partitioned-engine jobs for free,
  no merge logic needed at the read side at all. This is a stronger form of
  "callers see zero difference" than a parallel-tables design would have
  given, at the cost of Postgres remaining a literal write target (by the
  Partition Processor only) rather than a pure projection — an intentional,
  reasoned scope reduction for this slice, not the design's original
  "Postgres as pure read-model" end state (§7).
- **Idempotency is Postgres transaction atomicity, not deterministic event
  IDs.** `StartProcessInstanceWithIDs` (a new caller-supplied-ID variant of
  `StartProcessInstance`) hits a real Postgres unique-constraint violation on
  redelivery, mapped to a sentinel `service.ErrProcessAlreadyStarted`; job
  completion redelivery hits the already-existing `service.ErrJobInvalidState`
  sentinel (job status no longer `locked`). Both are treated as safe no-ops.
  This replaced the deterministic-event-ID scheme originally planned for a
  pure event-log architecture — validated as sound via a Plan-mode research
  pass, but made unnecessary once Postgres itself became the durable state.

**Components, as actually built**:
- **Command Gateway** (`internal/partition/gateway.go`): `StartProcess` /
  `CompleteJob` produce a `CommandEnvelope` (new `commands.proto`, a second
  proto file alongside Phase 0's `events.proto`) to `goflow.process-commands`
  (new topic, keyed by `processInstanceId`, same as Phase 0's events topic),
  then block on `eventlog.Consumer.Register(...).Await(...)` for the
  resulting domain event — reusing Phase 0's *existing* event bus rather than
  building new cross-replica coordination: every replica already runs a
  `Consumer` against the full events topic, so the HTTP-serving replica
  doesn't need to be the one whose Partition Processor handled the command.
  `LookupJobOwner` resolves whether a given job belongs to a partitioned
  instance via one join query, letting every call site branch cheaply.
- **Partition Processor** (`internal/partition/processor.go`): uses
  `kafka.ConsumerGroup` directly (not the higher-level `kafka.Reader`) so
  each locally-assigned partition gets one dedicated goroutine — confirmed via
  research that this gives real Kafka-protocol partition assignment/rebalance
  "for free," the same guarantee Zeebe's own partition ownership relies on.
  Commits a partition's offset only *after* successfully applying the
  command's effect (`kafka.Generation.CommitOffsets`), so a crash between
  consuming and applying redelivers rather than silently drops the command.
- **Feature flag**: `process_definitions.write_path` (`'legacy'` default /
  `'partitioned'`), flipped via `PATCH /engine-rest/process-definition/:id/
  write-path`, which runs `ValidateShape` before ever allowing `'partitioned'`
  — an explicit scope fence around unsupported constructs (gateways, timers,
  multi-instance, message/signal correlation, DMN), not a silent gap.
- **Branch points**: all 5 existing call sites of `service.StartProcessInstance`
  (`internal/api/process_definitions.go`, `process_instance_controller.go`,
  `internal/api/v2/process_instance_controller.go`, `internal/worker/pool.go`,
  `internal/zeebegrpc/server.go`) and both call sites of `service.CompleteJob`
  (`internal/api/external_task_controller.go`, `internal/zeebegrpc/server.go`)
  branch on the definition's/job's write path — `internal/partition` cannot be
  imported by `internal/service` itself (it would need to import
  `internal/service` back, a cycle), so every caller branches individually
  rather than the branch living in one shared function. Each branch is a small,
  mechanical early-return, not duplicated business logic.

**A real bug found only by running the real test suite, not by design
review**: the first cut of `handleCompleteJob` assumed completing the slice's
one job always completes the process directly via `service.CompleteJob`'s
return value. It doesn't — `service.CompleteJob` only marks a process complete
when the service task node has *zero* outgoing flows; a normal linear chain
(task → end event via an explicit sequence flow, this fixture's actual shape)
makes it just advance the execution pointer to the end-event node and return
`ProcessCompleted: false`, exactly like the legacy REST/gRPC handlers see —
which is why every one of those callers always follows up with
`runtime.ExecuteExecution` to actually reach the end event. The Partition
Processor's handler was missing that follow-up call, so every completion timed
out waiting for a `ProcessCompleted` that never fired. Fixed by mirroring the
legacy callers' exact resume-on-non-completion pattern.

**Verification**: new e2e suite `examples/tests/27_partition_engine.test.ts`
(6 cases: shape-rejection, successful flip, response-shape parity, full
start→fetch→complete→history lifecycle, safe-redelivery-is-a-no-op, and legacy
processes provably unaffected). Full 27-suite Docker regression green,
including all 26 pre-existing suites completely unmodified — the load-bearing
proof that the legacy engine's write path really wasn't touched.

**How to apply going forward**: the next vertical slice (per §9.3) would add
one more BPMN construct (most naturally a single exclusive gateway, the
smallest extension to `ValidateShape` and to `internal/partition`'s command
set) — each slice should keep reusing `service`/`runtime` functions rather
than reintroducing a parallel BPMN interpreter, per the reasoning above. The
`permanent parallel vs. cutover` question (§11) is still open and still not
worth answering until a slice sees real use.

## Phase 2 — the diamond shape (exclusive gateway)

Shipped the same day, immediately after Phase 1. Added exactly the
construct Phase 1's own "how to apply" section predicted: a "diamond" —
start → one exclusive gateway (2 outgoing flows, both with explicit
non-empty conditions) → {serviceTaskA | serviceTaskB} → one shared end
event. `internal/partition.ValidateShape` now accepts either this shape or
Phase 1's original linear chain; anything else is still rejected.

**Confirmed, not assumed, before writing any code**: `runtime.ExecuteExecution`'s
graph-walk loop and `service.CompleteJob`'s next-node resolution are both
already fully generic — neither has any shape assumption, so walking
through a gateway to reach a branch task, or from a branch task to a shared
end event, needed **zero changes** to either function. This is Phase 1's
reuse-over-reimplementation architecture paying off for a second shape
exactly as intended — the entire diff for Phase 2 is scoped to
`internal/partition` (validator + two bug fixes below), never touching
`internal/service`/`internal/runtime`.

**Two real, pre-existing correctness bugs found by two research passes
before writing code, not by testing after** (both were latent in Phase 1's
shipped code — the diamond shape just made them reachable in practice for
the first time, since Phase 1's one-hop shape never exercised the gap):

1. **`internal/partition/gateway.go`'s `CompleteJob` waited on
   `ProcessCompleted`,** assuming completing a job always finishes the
   process in the same step. True by coincidence for both Phase 1's and
   Phase 2's shapes (every task flows directly to an end event), but not
   guaranteed by anything — a future phase with a node after a decision
   point would silently time out instead of erroring clearly. Fixed to wait
   on `JobCompleted` instead, correlated by matching `job_id` in the event
   detail (added `eventlog.Waiter.Cancel()` so a caller can safely
   pre-register a replacement waiter before awaiting the current one,
   avoiding a delivery-gap race, since `Consumer` only correlates waiters
   by `(instanceID, eventType)`, not job ID).
2. **Both `handleStartProcess`'s and `handleCompleteJob`'s redelivery-safety
   checks used `current_element_id`** to decide "is it safe to resume
   `ExecuteExecution`," which is wrong in general: `ExecuteExecution`'s
   gateway-walk case updates `current_element_id` to the *next* node
   **before** that node's own handler commits its side effects (job row +
   status). A crash in exactly that gap would leave an instance permanently
   stranded under the old check (current node no longer matches "the start
   node," so it's wrongly treated as "already fully applied"). Fixed both
   to key off **`executions.status`** instead: every wait-state executor
   (`executeServiceTask` → `'waiting'`, `executeEndEvent` → `'completed'`)
   flips status away from `'active'` **atomically** with its own
   side-effecting commit, unlike `current_element_id` — so `status ==
   'active'` is the correct, shape-agnostic "nothing durable happened yet,
   safe to resume" signal. `handleCompleteJob`'s version of this bug
   (`resumeIfStranded`) was a genuine mirror-image gap found while fixing
   the start-side one, not something the diamond shape itself introduced —
   `service.CompleteJob`'s own commit (mark job completed + advance the
   pointer) and the *subsequent* `ExecuteExecution` walk are two separate
   transactions in Phase 1's code too, just never exercised by a crash in
   that exact window until this investigation looked for it.

**A caveat this investigation surfaced for future phases, not itself a bug
here**: the `status`-based redelivery signal only works because every
*currently reused* wait-state executor happens to flip `status` away from
`'active'` atomically with its side effect. `executeUserTask` does **not**
(it stays `'active'` while parked on a user task) — so if a future phase
adds `UserTaskType` to the partitioned engine's supported shapes, this
exact redelivery check would need a different signal for that node type.
Documented here so it isn't rediscovered the hard way later.

**Known pre-existing gap, explicitly left open**: suite 27 test [E]
re-completes an already-archived job, which actually returns
`ErrJobNotFound` (not `ErrJobInvalidState`) once archiving deletes the job
row — `handleCompleteJob` has no case for that today and falls through to
a hard error that retries forever on its partition (masked in the existing
test only because it separately checks a *different* instance still
works). Not introduced or worsened by Phase 2; not fixed here since it
isn't needed for the diamond shape to work. Worth a dedicated follow-up.

**Verification**: new `internal/partition/validate_test.go` (8 pure-Go unit
tests — no Go tests existed for this package before Phase 2; much cheaper
than Docker/e2e for shape-validation edge cases) plus new e2e suite
`examples/tests/28_partition_diamond.test.ts` (7 cases: valid-shape flip,
two distinct rejection paths — missing condition, diverging end events —
both-branches-complete-correctly, an exclusivity check that the wrong
branch's topic never receives a job, and an explicit regression check that
Phase 1's linear shape still works). Full 27-suite Docker regression green
on the first attempt after the `internal/partition` changes; a follow-up
message-quality fix (the diverging-ends rejection initially returned a
generic fallback message instead of naming the shared-end violation
specifically, since two end events fail the top-level node-count check
before ever reaching the diamond-specific validator) was verified by curling
the live endpoint directly, then a second full regression confirmed it
didn't change any test's pass/fail outcome.

## Phase 3 — multi-task linear chain

Shipped the same day, immediately after Phase 2. Given two candidate scopes
— a longer linear chain, or an intermediate timer event — a research pass
found timers are **not** a small next step: `TimerScheduler` claims and
fires due timers via a global `SKIP LOCKED` poll that writes straight to
Postgres, completely bypassing `internal/partition`'s Kafka consumer-group
ownership, with no optimistic-concurrency guard against a timer-driven
resume racing a Kafka-driven job completion on the same instance. Presented
this tradeoff to the user directly (small-and-safe vs. big-and-genuinely-new-risk)
rather than picking silently; the user chose the linear chain.

Generalized `internal/partition.ValidateShape`'s linear-chain validator
from exactly one service task to **any length** chain (walking from the
start event through tasks with exactly one outgoing flow each, until
reaching the end event) — confirmed via research, then verified by
re-reading `job_service.go`/`runtime.go` directly, that neither
`service.CompleteJob` nor `runtime.ExecuteExecution` has any assumption
about chain length, so **this was the entire code change**: no other file
in `internal/partition`, and nothing in `internal/service`/`internal/runtime`,
needed to change.

**Why this shape specifically, not just "the next construct"**: Phase 2's
Command Gateway fix (waiting on `JobCompleted` correlated by `job_id`
instead of `ProcessCompleted`) was built defensively for exactly the case
where completing a job does *not* immediately finish the process — but had
never actually been exercised, since Phase 1's linear shape and Phase 2's
diamond both always complete in exactly one hop from the last job
completion (`gateway.go`'s own comment admitted this explicitly). A 2-task
chain is the smallest shape that finally exercises "wait for the *right*
job's `JobCompleted`, and the process is genuinely still running
afterward" for real, closing a real gap in this session's own test
coverage rather than adding a new BPMN construct for its own sake.

**Verification**: 4 new unit tests in `internal/partition/validate_test.go`
(3-task chain passes; a mid-chain branch, a loop back to an earlier task,
and a disconnected extra task are all rejected) — though this session's
local Windows Application Control policy blocked running this package's
compiled test binary even via the usual project-local-path workaround (a
recurrence of a previously-documented flaky issue), so these were verified
indirectly via the equivalent e2e coverage instead of a local `go test`
run. New e2e suite `examples/tests/29_partition_chain.test.ts` (4 cases,
including the one this phase exists for: asserting the process is still
`active` — not `completed` — immediately after only the first of two jobs
finishes). Full 28-suite Docker regression green on the first attempt,
including Phase 1 and Phase 2's suites completely unchanged.

**How to apply going forward**: timers remain the natural §9.3 next
candidate but need their own dedicated design pass first — specifically,
how to give timer-driven resumes the same partition-ownership guarantee
Kafka-driven commands get today (e.g. partition-scoped timer claiming
inside `internal/partition.Processor`, rather than the current global
`TimerScheduler` poll), not just a `ValidateShape` relaxation. Extending
diamond branches to sub-chains (combining Phase 2 and Phase 3) is a
smaller, independent option if timers are deferred further.

## 1. Why this exists, and why not sooner

The current engine (`internal/runtime`, `internal/repository`) treats a
Postgres row as truth and does read-modify-write inside one ACID transaction
per engine step. That model is correct, simple to reason about, and — per
`[[project_grpc_wake_listen_notify]]` — the *app tier* already scales
horizontally against one Postgres primary. The one thing it cannot do is
scale the *write* path past a single Postgres primary's throughput ceiling.

Real Zeebe solves this with partitioning: each process instance is owned by
exactly one partition, each partition has its own Raft-replicated event log,
and there is no shared central database at all. This document describes the
closest equivalent buildable on top of Kafka (a proven, off-the-shelf
replicated commit log) instead of a custom Raft implementation.

**This is a new engine, not a migration.** The write path, the definition of
"a transaction," and the consistency guarantees callers get all change.

## 2. Non-negotiable constraint

GoFlow's entire value proposition is API compatibility with both Camunda 7
(`/engine-rest/...`) and Camunda 8 (`/v2/...` + the Zeebe gRPC gateway). This
design assumes **the wire contracts do not change** — a client of either API
today should not need to change a single line to talk to this engine
tomorrow. Everything below is scoped by that constraint.

## 3. The core shift

| | Today | Event-sourced |
|---|---|---|
| Source of truth | A Postgres row | An ordered, immutable event log |
| Unit of consistency | One SQL transaction | One appended event (or batch) |
| Current state | Read directly | A projection, derived by replaying events |
| Write path | Read-modify-write in Postgres | Validate command → append event(s) |
| Ordering guarantee | Row locks / `SKIP LOCKED` | Single-writer-per-partition, in log order |

## 4. Partitioning model (mirrors Zeebe)

```
processInstanceKey → hash(key) % N  →  one of N Kafka partitions
```

Every event for a given instance lands in the same partition, in order, for
the instance's entire lifetime. Exactly one consumer (the "partition owner")
processes a given partition at a time — the same single-writer-per-partition
property Zeebe relies on for correctness without distributed locking. `N` is
chosen up front for a target throughput ceiling (order of 12–24 to start) and
is the axis this architecture actually scales along — more partitions, more
partition-owner processes, more throughput, no shared bottleneck.

## 5. Components

```
                         ┌─────────────────────┐
   REST /engine-rest,    │   Command Gateway    │   (today's API layer,
   /v2, gRPC clients ───▶│  (routes by instance  │    mostly unchanged
                         │   key to a partition) │    shape)
                         └──────────┬───────────┘
                                    │ produce (command/event)
                                    ▼
                    ┌───────────────────────────────┐
                    │   Kafka: goflow.process-events  │  N partitions,
                    │   (keyed by processInstanceKey) │  keyed for ordering
                    └───────────────┬─────────────────┘
                                    │ consume (in order, per partition)
                    ┌───────────────┴────────────────┐
                    │                                 │
          ┌─────────▼─────────┐             ┌─────────▼─────────┐
          │ Partition Processor │   ...      │ Partition Processor │
          │  (owns partitions    │            │  (owns partitions    │
          │   e.g. 0-5)           │            │   e.g. 6-11)          │
          │  runs internal/engine │            │  runs internal/engine │
          │  + internal/common    │            │  + internal/common    │
          │  (BPMN/FEEL, reused)  │            │  (BPMN/FEEL, reused)  │
          └─────────┬─────────┘             └─────────┬─────────┘
                    │ produce resulting domain events (same topic)
                    └───────────────┬─────────────────┘
                                    │ consume (separate consumer group)
                         ┌──────────▼───────────┐
                         │  Read-Model Projector  │
                         │  (upserts Postgres —   │
                         │  process_instances,    │
                         │  jobs, tasks, historic_*)│
                         └──────────┬────────────┘
                                    ▼
                    History / Audit / Analytics / Token-replay
                    (existing read-side code, unchanged — see §7)
```

1. **Command Gateway** — today's REST/gRPC handlers, adapted to resolve the
   target partition from `processInstanceKey` and either forward synchronously
   to that partition's owner (closer to Zeebe's own gateway→broker model) or
   append a command event and bridge the async result back to the waiting
   HTTP/gRPC call (the existing gRPC `ActivateJobs` long-poll already does a
   version of this async-to-sync bridge — same pattern, reused).

2. **Partition Processor** — the actual engine. One process (or process group)
   owns a fixed set of partitions, consumes events for them in order, and runs
   the *same* BPMN interpretation logic that exists today
   (`internal/engine`'s graph walker, `internal/common`'s FEEL/CEL evaluation)
   — these are already decoupled from persistence and can move largely as-is.
   What changes is the persistence step: instead of `tx.Commit()` against
   Postgres, the processor appends the resulting domain events
   (`JobCreated`, `ExecutionMoved`, `IncidentRaised`, `TimerFired`, ...) to the
   log and updates its own in-memory state for the instances it owns.

3. **Event Log** — Kafka, or Redpanda (Kafka-wire-compatible, single binary,
   no ZooKeeper/KRaft cluster to operate — worth serious consideration for a
   from-scratch Go-shop build). One partitioned stream carries every event
   type for an instance, not one topic per event type — ordering guarantees
   the engine logic depends on require it.

4. **Read-Model Projector** — a separate consumer group materializing
   Postgres tables shaped like today's `process_instances` / `jobs` / `tasks`
   / `historic_*`. This is the single biggest cost-saver in the whole design:
   the entire history, audit-trail, token-replay, and analytics feature set
   (a large amount of real, working code) keeps working almost unchanged,
   fed by projection instead of direct writes.

5. **Snapshotting** — periodic aggregate-state checkpoints per instance
   (analogous to Zeebe's own snapshotting, or Kafka Streams' state stores) so
   a partition-owner restart or rebalance doesn't require replaying an
   instance's full history from the beginning.

6. **Idempotency / exactly-once** — a transactional Kafka producer
   (`enable.idempotence` + transactions) around each processor's
   append-then-apply step, so a crash-and-replay never double-creates a job or
   double-fires a timer. Projector upserts are keyed by event offset for the
   same reason.

## 6. Consistency model — the actual hard part

Today, `GET /process-instance/:id` immediately after
`POST .../complete` is guaranteed fresh — same transaction boundary,
synchronous. In this design the Postgres read-model is asynchronously
projected, so an immediate read *could* be briefly stale (typically
single-digit milliseconds, not zero). Two options, not mutually exclusive:

- **Accept eventual consistency** on the read-model and document it — simplest,
  but a behavior change for any caller relying on strict read-after-write.
- **Read-your-writes for hot instances** — route a read for a just-modified
  instance to its owning partition processor's in-memory state instead of the
  projected table for a short grace window. More compatible, real added
  complexity.

This is the one place where "the API contract doesn't change" (§2) is hardest
to fully honor and needs an explicit decision before implementation starts.

## 7. What's reused vs. rebuilt

**Reused largely as-is:** BPMN graph parsing/interpretation
(`internal/engine`), FEEL/CEL evaluation (`internal/common`), REST/gRPC
handler shapes, the entire historic/audit/analytics *read* surface once fed
by projections, the React frontend (no changes needed if the API contracts
hold).

**Rebuilt:** the write path in `internal/runtime`/`internal/repository`
(Postgres transactional mutation → event-sourced command handling), job/timer
dispatch (`SKIP LOCKED` polling → partition-owned in-memory scheduling), and
the definition of "a transaction" as the unit of consistency throughout the
codebase.

## 8. Technology choices to decide before starting

- **Kafka vs. Redpanda** — Redpanda is Kafka-wire-compatible, ships as a
  single binary, and avoids a separate ZooKeeper/KRaft operational burden;
  worth strongly evaluating given this is a from-scratch build with no
  existing Kafka investment to protect.
- **Go client library** — `segmentio/kafka-go` (pure Go, no cgo) vs.
  `confluent-kafka-go` (cgo, wraps librdkafka, more mature transactional
  producer support). Transactional support is load-bearing here (§5.6), so
  this needs a spike, not an assumption.
- **Event schema format** — Protobuf (already used for the gRPC gateway,
  compact, versionable, schema-registry-friendly) is recommended for the
  event log specifically, independent of the REST APIs' JSON.

## 9. Migration path (no big-bang cutover)

1. **Shadow mode** — stand up Kafka/Redpanda + the Read-Model Projector only.
   Dual-write: the *existing* Postgres-transactional code path also emits
   events (outbox pattern) alongside its normal writes. Verify the projector
   reconstructs identical state to live Postgres. No behavior change for any
   caller yet — this phase exists purely to validate the event schema and
   projector correctness before anything depends on them.
2. **First vertical slice** — build the Command Gateway + Partition Processor
   for one narrow concern (e.g. just the service-task/job lifecycle), running
   behind a flag for a subset of process keys, side-by-side with the legacy
   engine for everything else.
3. **Expand slice by slice** — timers, incidents, message correlation, DMN —
   until the legacy transactional runtime is no longer on the write path for
   anything.
4. **Cutover** — retire dual-write; Kafka becomes the sole source of truth;
   Postgres becomes a pure read-model, same role the historic-archive tables
   play today, just populated by projection instead of direct insert.

Each phase leaves a working system in place — no extended freeze on the
current engine.

## 10. Sizing (order of magnitude, not a committed estimate)

This is materially larger than any feature shipped in this project so far.
Realistically a multi-month effort even scoped to phase 1–2 above (new infra,
one new write-path slice, full parity testing against the existing e2e
suite). It should be tracked and resourced as its own initiative, not a
single roadmap line item.

## 11. Open questions

- ~~Transactional Kafka producer support (§5.6, §8)~~ — **resolved**:
  `segmentio/kafka-go` has no transactional/idempotent producer support
  (confirmed against kafka-go's own issue tracker), and adopting one would
  require `confluent-kafka-go`/librdkafka, which needs cgo and conflicts with
  this repo's `CGO_ENABLED=0` build. Phase 1 sidesteps the question entirely
  by using Postgres transaction atomicity as its idempotency boundary instead
  of a transactional producer — see "Phase 1 — what's actually running
  today" above.
- ~~Is eventual consistency on reads acceptable~~ — **resolved for Phase 1**:
  accepted, per an explicit user decision when scoping Phase 1's first slice.
- Self-hosted Kafka/Redpanda, or a managed service (MSK, Confluent Cloud)? —
  still open; irrelevant until a slice sees real production traffic.
- What's the actual target throughput number driving this? Worth confirming
  it's genuinely past what Postgres + Citus sharding (the incremental path,
  see `README.md`'s Comparison section) can reach — this whole design is only
  worth it if the number justifies it. Still open.
- Permanent parallel "v2 engine" behind a flag long-term, or a planned full
  cutover with the legacy engine eventually deleted? Still open, and still
  not worth answering until a slice sees real use.
