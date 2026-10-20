# Partitioned write path (Camunda 8 style)

Status: shipped 2026-10-20. Replaces the per-definition, shape-restricted
"partitioned write path" of July 2026 (phases 1–7 in
[`event-sourced-kafka-design.md`](event-sourced-kafka-design.md), kept as
history).

## What it is

Camunda 8 runs every process instance on one partition. The partition's
leader is the only broker that writes the instance, and it applies the
instance's commands one after the other. GoFlow can now run the same way:

```
WRITE_PATH=partitioned   # default: direct
KAFKA_PARTITIONS=12      # fixed once instances exist
COMMAND_TIMEOUT=30s      # how long a request waits for its command
```

In partitioned mode:

- every command that changes a process instance goes to a Kafka partition
  of `goflow.partition-commands`. The command's key picks the partition:
  the instance id, or the correlation key for a message, or the name for a
  signal;
- the GoFlow replicas form one Kafka consumer group. Kafka gives each
  partition to exactly one replica, which runs its commands in order, one
  at a time. When a replica stops or a new one joins, the partition moves;
- the caller (HTTP handler, gRPC handler, worker) waits for the outcome on
  `goflow.partition-replies`, as the Zeebe gateway waits for a broker. A
  handler's error comes back unchanged: same message, and `errors.Is`
  still matches the sentinel, so a 404 stays a 404 and a 409 stays a 409.

There is no restriction on the BPMN: every construct the engine supports
runs partitioned. The full integration regression passes in both modes.

## The pieces

| Package | Role |
|---|---|
| `internal/cmdbus` | The commands: `Register`, `Do` (wait for the outcome), `Post` (don't wait), `Execute` (what a partition owner runs). In direct mode `Do`/`Post` run the handler in place. Arguments and results go through JSON in both modes, so a handler that works in direct mode works partitioned. |
| `internal/runtime/commands.go` | The engine's commands: start an instance, complete or fail a job, set retries, resolve an incident, cancel, complete a user task, set variables, modify an instance, publish a message, broadcast a signal, fire a timer start event, re-check conditions, deliver a kept message. |
| `internal/service/timer_scheduler.go` | `FireTimer`. Any replica may claim a due timer, but the timer fires on the partition of its instance. |
| `internal/api/instance_commands.go` | The Camunda 7 style endpoints: edit variables, suspend, end, stop, correlate a message. |
| `internal/partition` | The Kafka transport (`Transport`), the partition owner (`Processor`) and `Describe` (the topology). |

Every caller goes through `cmdbus` in both modes. Direct mode therefore
runs exactly the same handler code, without Kafka.

## The order of commands

The owner of a partition reads its commands in order and runs each one to
completion before reading the next. A command is acknowledged to Kafka
(the offset is committed) once it has run and its reply is sent. When a
partition moves to another replica, the old owner finishes the command it
is running before it lets go. Kafka hands the partition over only when
the old owner's goroutine has returned, so two replicas never run
commands of the same partition at the same time.

Postgres remains the state, as in direct mode, and the row locks stay in
place. A few effects cross instances: a called process finishing resumes
its caller, a BPMN error or escalation is handed up, an intermediate
signal throw resumes other instances. These effects run inside the
command that causes them, under those locks, rather than as separate
commands. Zeebe sends such effects as commands to the other partition;
here they are transactional instead.

## When a replica dies mid-command

A replica can die after a command's changes were committed but before it
was acknowledged. The command is then delivered again, to the next owner.
The first command read after taking over a partition is marked as
possibly redelivered (`cmdbus.Redelivered`). A handler that finds its
change already applied does not fail. It completes what the dead owner
left unfinished instead: it re-runs the instance's executions left
`active`, because at rest an execution is `waiting` or finished, so
`active` means a run that was cut short.

| Command | "Already applied" signal |
|---|---|
| Start an instance | its id exists (the id is chosen before sending) |
| Complete a job | the job is no longer locked |
| Fail a job | the job is no longer locked |
| Complete a user task | the task is completed |
| Cancel an instance | the instance is gone |
| Fire a timer | the execution no longer waits (`ErrStaleTimer`) |

Messages with a `messageId` are deduplicated, as in Camunda 8. A message
published without one, a signal, or a modification can be applied twice
if its owner dies between committing and acknowledging it.

A command that fails because the database is out of reach is retried on
the same partition until it works. This covers a connection refused or
reset, a server shutting down, a replica promoted away (read-only), a
deadlock or a serialization failure. While it retries, the partition's
later commands wait, which keeps them in order. Any other error is an
answer and goes back to the caller.

## Background loops on every replica

- **Timers.** Every replica's scheduler claims due timers (`SKIP LOCKED`)
  and sends each one to its instance's partition. If the send fails, the
  timer is armed again for the next poll.
- **Conditional events and kept messages.** Every replica watches for
  them, but a replica only sends the commands for keys whose partition it
  owns (`cmdbus.OwnedHere`). Each condition re-check or kept-message
  delivery is therefore sent once.

## Topology

`GET /v2/topology` (Camunda 8 REST) and the gRPC `Topology` RPC report the
consumer group as Zeebe reports its brokers: one broker per replica,
leading the partitions it owns (numbered from 1). In direct mode the
cluster is the replica that answers, with one partition. The response
also says which write path is running (`writePath`).

## What it doesn't change

- Job activation (`fetchAndLock`, `ActivateJobs`) and user-task claim or
  unclaim stay direct. They only lock or assign a row, which Postgres
  already arbitrates with `SKIP LOCKED` and conditional updates.
- The event log (`goflow.process-events`, shadow mode, projector) is
  independent of the write path.
- Direct mode is unchanged, and is still the default.
