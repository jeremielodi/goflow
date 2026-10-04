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

**Wired events** (see `internal/eventlog/proto/events.proto` for exact
shapes) — 21 total, mirroring every generic transition already
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
- `JobRetriesSet` (Phase 5 gap-closing pass).
- **`TaskCreated`/`TaskClaimed`/`TaskUnclaimed`/`TaskCompleted`/
  `TaskCancelled`** (added 2026-07-05, closing a real, previously-unnoticed
  systematic gap: job lifecycle was fully instrumented here but *user-task*
  lifecycle had zero coverage at all — not a missing call site, the proto
  schema itself had no `Task*` message type. The only prior task
  observability was the older, entirely separate
  `internal/events.TaskEventDispatcher` (SSE/WebSocket/webhook push), which
  never touches this Kafka event log. Field shapes mirror that dispatcher's
  5 transitions exactly. `TaskCreated`/`TaskClaimed`/`TaskUnclaimed`/
  `TaskCancelled` publish from `internal/api/task_controller.go` (the
  layer these repository mutators are called from — publishing inside
  `internal/repository/task_repository.go` itself isn't possible, that
  package can't import `internal/eventlog` without an import cycle, since
  `eventlog.Consumer` already imports `internal/repository` for its own
  projection writes); `TaskCompleted` publishes from
  `internal/service/task_service.go`'s two real completion paths (the
  `TaskRepository.CompleteTask` method it does NOT go through is dead code,
  never registered as a route).
- ~35 call sites total wired, all following the identical
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

## Phase 4 — business rule task (DMN) in the linear chain

Shipped the same day, immediately after Phase 3. Phase 3's own "how to
apply" flagged two candidates (timers, or combining Phase 2+3 into
sub-chain diamond branches); research for this phase found a **better**
third option and used it instead of either, without needing to ask —
unlike Phase 3, there was no real tradeoff to weigh here, since a business
rule task beat both flagged candidates on every axis (safety, value, and
alignment with the design doc's own §9.3 "DMN" item).

Confirmed by reading `internal/runtime/executor_business_rule_task.go`
directly: a business rule task evaluates its DMN decision **synchronously
inline** — one Postgres read for the stored decision table plus a pure
`dmn.EvaluateDecisionTable` call, both inside the *same* transaction that
advances `current_element_id` via `UpdateExecutionNodeTx`, then the walk
continues in the same `ExecuteExecution` call. No job row, no external
worker, no wait state — architecturally identical to the exclusive-gateway
case Phase 2 already made redelivery-safe, not the service-task case that
needed the `status`-based fix. Since `UpdateExecutionNodeTx` never touches
`executions.status`, a business rule task never moves `status` away from
`'active'` — Phase 1/2's existing `status == 'active'` redelivery check in
`internal/partition/processor.go` already covers it correctly, with no new
stranding case. This meant the **entire diff was one file**:
`internal/partition/validate.go`'s linear-chain bucketing now accepts
`BusinessRuleTaskType` nodes mixed with `ServiceTaskType` nodes in any
order (the diamond shape's two branches deliberately stay
service-task-only, per this package's running discipline of never
combining two generalizations in one phase) — zero changes to
`internal/service`, `internal/runtime`, or `internal/partition/processor.go`/
`gateway.go`, the fourth phase in a row where the legacy-engine-reuse
architecture needed nothing beyond the validator.

**Verification**: 6 new `internal/partition/validate_test.go` cases total
(2 new for this phase: a chain mixing a business rule task and a service
task passes; a diamond with a business rule task in place of one branch's
service task is rejected) — this time the local test binary ran cleanly
(the Windows Application Control block from Phase 3 didn't recur). New
e2e suite `examples/tests/30_partition_dmn.test.ts` (4 cases: valid flip,
two different order amounts routing to two different DMN rules — proving
the evaluation genuinely re-runs rather than being cached from the first
call — and a regression check that Phases 1-3's fixtures still work). Full
29-suite Docker regression green on the first attempt.

**How to apply going forward**: timers are still the one candidate that
needs a dedicated design pass (partition-scoped claiming) before it can be
folded in safely — that hasn't changed. Extending diamond branches to
sub-chains, or to also allow business rule tasks per branch, remain small,
independent, low-risk options if a fifth phase is wanted before tackling
timers.

## Phase 5 — closing the FailJob gap

Shipped the same day, after the user asked "is there other phases non
completed" and an audit turned up a real, pre-existing gap rather than
another BPMN-shape candidate: `service.FailJob` was called **unconditionally**
from both the REST failure endpoint and the gRPC `FailJob` RPC — neither had
the `partition.LookupJobOwner` gate `CompleteJob`'s call sites already had.
A job belonging to a partitioned instance that failed bypassed Kafka/
partition ownership entirely and wrote straight to Postgres via the legacy
path. Unlike Phases 1-4, this wasn't a missing BPMN construct — it was a
hole in what had already shipped, only covering the happy path.

Research (cross-checked by reading `job_service.go`'s `FailJob` and both
call sites directly) confirmed the same reuse pattern held a fifth time:
`FailJob`'s three outcomes (retry-reset, permanent-fail-with-incident,
permanent-fail-with-boundary-trigger) all write to the same shared tables
Phase 1 already established (`jobs`, `executions`, and now `incidents` too
— confirmed write-path-agnostic, no changes needed to incident list/resolve
endpoints), and the boundary-trigger case resumes `runtime.ExecuteExecution`
exactly like `CompleteJob`'s non-terminal case, through an already-generic
`ErrorBoundaryEventType` dispatch case. `FailJob` also already publishes a
`JobFailed` event unconditionally on every outcome (wired since Phase 0),
giving the Command Gateway's bridge the same kind of confirmation signal
`JobCompleted` already provided for completions.

**Because shipping the boundary-trigger branch without ever letting a real
shape exercise it would be untested code** (a mistake avoided in every
prior phase), this phase both gated `FailJob` and added error-boundary-event
support to `ValidateShape` — scoped narrowly: only in the linear chain
(not the diamond), only attached to a service task (not a business rule
task — its executor doesn't raise a BPMN-catchable error), at most one per
task, flowing directly to the chain's shared end event (no sub-chain from
the boundary). `internal/partition/processor.go`'s new `handleFailJob`
reuses the **exact existing** `resumeIfStranded` helper for redelivery
safety — `service.FailJob` has no `ErrJobInvalidState`-style sentinel for
"already applied" the way `CompleteJob` does, so the processor checks
`jobs.status` itself before ever calling it, then falls back to the same
generic status-based resume check Phase 2 already built.

**Explicitly deferred, not fixed**: `POST /engine-rest/job/:id/retries`
(resetting retries) and `ResolveIncident` are the same class of gap —
ungated, no `LookupJobOwner` check — but lower risk (neither moves an
execution's graph position, they just make a job fetchable again) and kept
out of this phase's scope to stay bounded, the same discipline every prior
phase applied to combining generalizations.

**Verification**: 6 new `internal/partition/validate_test.go` cases (12
total in the package) — the local test binary hit the same flaky Windows
Application Control block Phase 3 saw (intermittent, not every run), so
these were verified via the equivalent e2e coverage instead of insisting on
a local `go test` run this time. New e2e suite
`examples/tests/31_partition_failjob.test.ts` (5 cases: a matching error
code triggering the boundary path with zero incidents created, an
unmatched error code creating a real incident verified via the completely
unmodified incident REST endpoint, a retry-reset that lets the job
complete normally afterward, and a regression check across Phases 1-3's
fixtures). Full 30-suite Docker regression green on the first attempt.
Manually curled the live incident list afterward and confirmed the
partitioned-path-created incident row is byte-for-byte the same shape as
legacy-created ones sitting right next to it in the same response.

**How to apply going forward**: the retry-setting/incident-resolution gap
above is the next-smallest closeable item if another "close a gap" phase
is wanted; timers remain the one item needing a dedicated design pass
before any phase attempts it.

## Gap-closing pass: retry-setting and incident-resolution

Shipped the same day, right after Phase 5, when the user asked to "fix
remaining known gaps." Since that phrase covered two very different things
— this small, well-understood gap and timers' genuine architectural risk —
asked which to do via `AskUserQuestion`; the user chose both, this one
first.

`POST /engine-rest/job/:id/retries` (REST) and the gRPC `ResolveIncident`
RPC had the identical ungated-bypass pattern `FailJob` had before Phase 5:
no `partition.LookupJobOwner`/`LookupIncidentOwner` check before writing
directly to Postgres. Closed exactly like every prior gate: extracted the
inline controller/RPC logic into `internal/service/incident_service.go`
(`SetJobRetries`, `ResolveIncident` — a new file, since no incident-focused
service file existed yet), added two new commands
(`SetJobRetriesCommand`, `ResolveIncidentCommand`) and a new
unconditionally-published confirmation event (`JobRetriesSet` — unlike the
already-existing `IncidentResolved`, which only fires when an open
incident actually existed, `SetJobRetries` needed a signal that always
fires so the Command Gateway's bridge always has something to wait on),
and gated both call sites.

**The one genuine simplification found here, not present in any
Start/Complete/Fail handler**: neither operation ever triggers a follow-up
`ExecuteExecution` walk — neither moves an execution's graph position at
all, just flips `jobs`/`incidents` row state. That means **no
redelivery-stranding case exists** for either new processor handler
(`handleSetJobRetries`, `handleResolveIncident`) — a redelivered command is
simply safe to re-apply as-is, no `status` check, no `resumeIfStranded`
call, unlike every handler since Phase 1. The accumulated infrastructure
(Command Gateway bridge pattern, `Waiter.Cancel()`-based retry loop) still
applied unchanged; only the "what happens on redelivery" question got
simpler, not harder, for these two.

**Verification**: extended `examples/tests/31_partition_failjob.test.ts`
with 2 more cases — `[F]` the REST retries endpoint resolving an incident
and letting the job complete again, `[G]` gRPC `ResolveIncident` against a
partitioned instance's incident using a real `zeebe-node` client (mirroring
suite 24's existing legacy-path test for the same RPC). Full 30-suite
Docker regression green on the first attempt.

**Status after this pass**: every write-path-affecting endpoint
(start, complete, fail, set-retries, resolve-incident) is now gated for
partitioned instances. Timers are the one remaining item, and still need
their own dedicated design pass — see the next section.

## Phase 6 — timers, safely

Shipped the same day, immediately after the gap-closing pass, when the
user asked to fix *all* remaining known gaps — including the one flagged
since Phase 3 as needing real design work, not a quick patch. Research
(reading `internal/service/timer_scheduler.go` and
`internal/runtime/executor_timer.go` in full) found the fix smaller and
cleaner than originally feared, for two reasons:

1. **Claiming and the dangerous write are already separate steps.**
   `EngineRepository.ClaimDueTimers` (marking `is_triggered=true`) is its
   own atomic statement that never touches `executions` — only
   `resumeExecution` (called afterward) does. So claiming stays exactly as
   it is today, globally, for every instance; the fix only gates what
   happens *after* a timer is claimed. No need to replicate Kafka's
   partition-hash function outside of Kafka at all — the race this session
   worried about since Phase 3 turned out to be avoidable by construction,
   not by hand-rolling partition-aware claiming.
2. **Timers have no caller waiting for a synchronous response.**
   Every other command in this series needed a produce-then-wait bridge
   because an HTTP/gRPC caller was blocked on the result.
   `TimerScheduler`'s background loop isn't — it produces a
   `TimerFiredCommand` and moves on to the next due timer, with
   confirmation happening asynchronously inside the Partition Processor.
   No `Waiter` retry-loop needed for this command type, the only one in
   the whole series without one.

**The one real design problem, solved with dependency inversion**: the
dangerous write lives in `internal/service` (`timer_scheduler.go`), but
`internal/partition` (which needs to redirect it) already imports
`internal/service` — so `internal/service` cannot import
`internal/partition` back, the same constraint that shaped every prior
phase's "callers branch themselves" design. Confirmed
`timer_scheduler.go` had zero compile-time dependency on
`internal/runtime`, only an injected `resumer` callback that fires *after*
the dangerous write already happened — too late to intercept. Fixed with a
`TimerPartitionRouter` interface defined *in* `internal/service` (where
it's consumed), implemented *in* `internal/partition` (which is allowed to
import `internal/service`), wired only in `main.go` — the standard Go
pattern for a lower layer calling into a higher one without an import
cycle, and the first time this whole effort needed it (every prior gate
was a brand-new caller, never a restructure of existing legacy code).

**Also confirmed safe to leave untouched**: cycle-timer rescheduling only
touches `timer_jobs`, never `executions` — no race, so it runs
unconditionally regardless of write path (extracted into
`scheduleNextCycleIfNeeded`, called from both branches of `triggerTimer`).
`resumeExecution`'s "cancel any `created` tasks" step — extracted into the
new shared `service.ResumeTimerExecution`, reused by both the legacy path
and the Partition Processor — is a no-op today for partitioned instances
regardless, since no shape `ValidateShape` accepts yet includes user
tasks.

**Redelivery safety**: `handleTimerFired` mirrors every prior handler's
pattern — `executeIntermediateTimer` sets `executions.status = 'waiting'`
when creating the timer job (confirmed by reading the executor, the same
convention `executeServiceTask` uses for jobs), so "status is still
`'waiting'`" is the correct "safe to resume" signal for a redelivered
command.

**Known, pre-existing risk, not introduced or worsened here**: if
producing a `TimerFiredCommand` fails after `ClaimDueTimers` already
marked the row triggered, that timer is stranded forever — but this is the
*exact same* risk the legacy path already has today (a `resumeExecution`
failure after claiming was already silently logged-and-dropped, no retry,
no dead-letter, confirmed by reading `processDueTimers`). Deliberately not
fixed here, to keep the new path's failure behavior consistent with the
old one rather than quietly stricter.

**Scope, matching every prior phase's discipline**: exactly one new shape
— `start -> intermediate timer event -> end`, no task at all, mirroring
`examples/tests/bpmn/26_short_timer.bpmn`. Timers mixed with tasks, more
than one timer in a chain, and boundary timers are all explicitly rejected
this pass — validated with dedicated unit tests, not just implied by
absence.

**Verification**: 4 new `internal/partition/validate_test.go` cases (26
total in the package now — local `go test` ran cleanly this time) plus
`internal/service`'s existing test suite re-run clean after the
`timer_scheduler.go` refactor. New e2e suite
`examples/tests/32_partition_timer.test.ts` (4 cases). Given
`timer_scheduler.go` is the first piece of *existing* legacy code
restructured anywhere in this whole effort (every prior phase only ever
added new call sites), the full regression was run **without**
`--no-timers` for the first time since this work started — all 33 suites
green, including suites 03/04/06's real timer/boundary-timer coverage on
the unmodified legacy path.

**Status after this pass**: every previously-identified gap is closed. See
the next section, though — auditing again afterward found two more.

## Gap-closing pass 2: Terminate/Cancel and Modify

Asked "is there another phase to cover? any gap or bug?" after Phase 6
shipped. Rather than re-reading the deferred-features list, audited the
codebase directly for any remaining direct-Postgres write skipping the
`LookupJobOwner`/`LookupIncidentOwner`/`LookupInstanceOwner` gate every
other write path already has. Found two real, previously-unflagged ones —
not BPMN construct coverage this time, but the same "bypasses partition
ownership" class of bug as everything above, on operations that apply to
*any* process instance regardless of shape (unlike message/signal/gateway
constructs, which can never reach a partitioned instance because
`ValidateShape` already rejects those definitions at flip-time):

1. **`service.TerminateProcessInstance`**, shared by REST `DELETE
   /process-instance/:id` and gRPC `CancelProcessInstance` — wrote
   directly to `executions`/`jobs`/`timer_jobs` with zero write-path
   check. A partitioned instance could be terminated out from under the
   Partition Processor at any time, racing its writes.
2. **`ModifyProcessInstance`** (REST v2 and gRPC) — called
   `runtime.ModifyInstance` directly, moving an execution's graph
   position. Same bypass.

**Terminate**: closed the same way as every prior gate — new
`TerminateCommand`, a Partition Processor handler that reuses
`service.TerminateProcessInstance` verbatim (it's already a single
self-contained operation: transaction + cascade cleanup + archive, not
something to reimplement), gated both call sites via the existing
`partition.LookupInstanceOwner`. The Command Gateway's `Terminate` waits on
the **existing** `ProcessTerminated` event (already published
unconditionally at the end of `TerminateProcessInstance`, already wired
into `Consumer`'s waiter-notify switch since Phase 0 shadow mode — no new
event type needed) with a single `Await`, not a retry loop: unlike
`JobCompleted`/`JobFailed` (which can fire repeatedly for different jobs on
the same instance), `ProcessTerminated` fires exactly once ever per
instance, the same reasoning `StartProcess`'s single `Await` on
`ProcessStarted` already relies on. Redelivery safety is keyed off "does
the row still exist" rather than a status check — a prior attempt's
success already archived-and-deleted the instance's live rows, so a second
`FindByID` genuinely returns `service.ErrProcessInstanceNotFound`, treated
as a safe no-op.

**Modify — rejected outright for partitioned instances, not implemented
through Kafka**: `ModifyProcessInstance` supports arbitrary source/target
element pairs and multi-token moves — routing that through the Command
Gateway would mean either restricting it to the narrow validated shapes
(what does "move a token" even mean inside a processor handler that
assumes today's fixed shapes?) or a much bigger effort disproportionate to
how rarely this operation is used. The gRPC handler already scopes itself
down for unrelated reasons ("exactly one activateInstruction... multi-token
modification is not supported over gRPC"), so an explicit scope rejection
for partitioned instances is consistent with precedent, not a new kind of
limitation. Both call sites now check `partition.LookupInstanceOwner`
first and return a clear error (REST: 422 `MODIFICATION_FAILED`; gRPC:
`codes.FailedPrecondition`) instead of calling `runtime.ModifyInstance` —
no new proto/command/processor code needed for this half of the pass.

**Verification**: new e2e suite
`examples/tests/33_partition_terminate_modify.test.ts` (5 cases: REST
terminate, gRPC cancel, REST v2 modify rejected, gRPC modify rejected,
legacy terminate regression). First full run surfaced 2 failures, both
test bugs rather than implementation bugs (checked expected instance state
against `'running'` instead of the actual `'active'` value the history
endpoint uses; the gRPC `ModifyProcessInstance` call omitted zeebe-node's
required `variableInstructions: []` field, crashing client-side before the
request even reached the server) — fixed both, then two full Docker
regressions green (32 suites, `--no-timers`; nothing in this pass touches
timer code, unlike Phase 6).

**Status after this pass**: every instance-level operation (start,
complete, fail, set-retries, resolve-incident, timers, terminate) is now
gated for partitioned instances; modification is explicitly out of scope
rather than silently unsafe. Remaining scope is entirely about BPMN
construct coverage the partitioned engine doesn't support yet
(parallel/inclusive gateways, multi-instance, message/signal correlation,
boundary timers, timers mixed with tasks, and modification if ever
prioritized) — no more known write-path bypass gaps.

## Phase 7 — parallel gateway (AND fork/join)

Second of 5 items from a user-directed sequential list ("fix all of them,
one at a time" — see `architecture/citus-distributed-storage-design.md`
section 6 for item 1). Presented via `AskUserQuestion` as one of three
candidate next shapes (the others: user tasks in the linear chain, which
would need a brand-new Kafka command type since no "complete a user task"
command exists yet; boundary timer events on chain tasks, which risks
combining two prior phases' concerns in one). Parallel gateway was chosen
as lowest-risk: it mirrors Phase 2's diamond shape but forks
unconditionally to every branch (AND) instead of picking one via a
condition (XOR).

**Turned out to be architecturally free, same as Phase 4** — zero changes
to `gateway.go` or `processor.go`. The reasoning: `runtime.ExecuteExecution`
already handles both the fork (creating N child executions and walking
each to its first job, all inside one call) and the join (counting arrived
branches via `gateway_state`, resuming the parent once the last one
arrives) generically, regardless of who's calling it — this is the exact
same shared code the legacy/Citus paths use, including the
`ParallelGatewayExecutor` join-counting logic fixed for a real bug the same
day (see `architecture/citus-distributed-storage-design.md` section 6).
`handleStartProcess`'s `rt.ExecuteExecution` walk reaches the fork and
creates+advances every branch in one shot; each branch's own job
completion goes through the already-generic `handleCompleteJob` →
`rt.ExecuteExecution(result.ExecutionID)` call, which resolves to that one
child and, on the last arrival, drives the join and resumes the parent —
no new command type, no new redelivery logic, since the existing
"status == 'active' means the walk hasn't resumed yet" contract is
per-execution, not shape-specific.

**The one real question this raised — sibling-branch concurrency — turned
out to be a non-issue given how Kafka partitioning already works here**:
every command for one process instance hashes to the same Kafka partition
and is processed strictly serially by that partition's single owning
goroutine. Two sibling branches' job completions are never actually
concurrent at the write layer, even if their underlying jobs complete at
the same wall-clock moment — the exact class of race the legacy/Citus path
needs `FOR UPDATE` locking to guard against simply can't happen on the
partitioned path.

**Only change needed**: `internal/partition/validate.go` — a new
`validateParallelShape` (structurally similar to `validateDiamondShape`)
recognizing start → fork parallelGateway (N≥2 unconditional outgoing
flows) → N distinct service tasks → join parallelGateway → shared end
event. Rejects a conditioned fork flow (AND semantics, a condition would
be silently ignored at runtime), business rule tasks or boundary
events/timers in a branch (same restriction as the diamond, not combining
generalizations), and any branch that skips the join.

**Verification**: 8 new unit tests in `validate_test.go` (2 passing shapes,
6 rejections). New e2e suite `examples/tests/34_partition_parallel.test.ts`
(5 cases: flip-to-partitioned, reject-conditioned-flow, both-branches-run,
join-works-regardless-of-completion-order, Phase 1/2 regression check).
Full 33-suite regression green on both a fresh plain stack and a fresh
Citus-backed stack. One flaky failure observed on the very first run
immediately after a container restart (`timed out after 5s waiting for
ProcessStarted` — a Kafka consumer-group rebalance-lag artifact of this
host, not a code bug; passed cleanly on immediate retry and on both full
regressions afterward).

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
