# GoFlow v2 — Event-Sourced, Kafka-Backed Engine (Design Scope)

Status: **Phase 0 (shadow mode) shipped on the `dev` branch, 2026-07-04.**
The rest of this document (Phases 1+, the real partition-owning write path)
remains scoped but not committed — Phase 0 exists specifically to validate
the event backbone in isolation before committing to anything further. It is
not an incremental change to the current engine's actual write path — see
`README.md`'s "Long-term / architectural" section for why. Building past
Phase 0 needs a separate go/no-go decision; this exists so that decision can
be made with a concrete plan (and now, a working shadow pipe) in hand instead
of in the abstract.

## Phase 0 — what's actually running today

The existing Postgres-transactional engine is **unchanged and remains the
only write path.** After each of 7 core transitions commits, a domain event
is *also* published (fire-and-forget) to a Redpanda topic, partitioned by
`processInstanceId`. A minimal in-memory consumer proves the pipe is
durable, ordered per instance, and well-formed — nothing yet depends on it.

**Wired events** (see `internal/eventlog/proto/events.proto` for exact
shapes): `ProcessStarted`, `JobCreated`, `JobCompleted`, `JobFailed`,
`IncidentResolved`, `ProcessTerminated`, `ProcessCompleted`. Hook points:
`service.StartProcessInstance`, `runtime.executeServiceTask`,
`service.CompleteJob` (both branches), `service.FailJob` (both branches),
`api.IncidentController.SetJobRetries`, `service.TerminateProcessInstance`,
`runtime.executeEndEvent`.

**Deferred** (not wired, tracked here rather than silently dropped): parallel
gateway fork/join, multi-instance progress, message/signal correlation,
timers, variable-update-only transitions.

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

**Verification**: new e2e suite `examples/tests/26_eventlog_shadow.test.ts`
(clean completion, permanent-failure-with-incident-resolution, and
termination flows, each polling the dev-only debug endpoint
`GET /engine-rest/eventlog/recent` — a 500-entry in-memory ring buffer over
the shadow consumer). Full 26-suite Docker regression green.

**How to apply**: Phase 1 (an actual partition-owning write path) is a much
larger step than Phase 0 and still needs the open questions in §11 answered
first — target throughput, managed-vs-self-hosted Kafka, and the
eventual-consistency tradeoff in §6 — none of that was resolved by shipping
Phase 0, which only proves the transport, not the architecture change.

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

## 11. Open questions (need answers before implementation starts)

- Self-hosted Kafka/Redpanda, or a managed service (MSK, Confluent Cloud)?
- Is eventual consistency on reads acceptable to real API consumers, or is
  read-your-writes (§6) a hard requirement?
- What's the actual target throughput number driving this? Worth confirming
  it's genuinely past what Postgres + Citus sharding (the incremental path,
  see `README.md`'s Comparison section) can reach — this whole design is only
  worth it if the number justifies it.
- Permanent parallel "v2 engine" behind a flag long-term, or a planned full
  cutover with the legacy engine eventually deleted?
