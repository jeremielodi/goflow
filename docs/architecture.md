# Architecture

## Key Design Decisions

- **NodeExecutor pattern** — each BPMN node type has its own `executor_<type>.go` file, all private methods on `*Runtime`
- **PostgreSQL SKIP LOCKED** — timers and jobs use `SELECT … FOR UPDATE SKIP LOCKED` for horizontal scaling without a queue broker
- **Graph cache** — parsed process graphs are stored as JSONB in `process_definitions.parsed_graph`, rebuilt only on deploy
- **Event subscriptions** — message/signal/EBG waiting is implemented via an `event_subscriptions` table; correlation is a SQL query
- **Call Activity** — child instance stores `parent_instance_id` + `parent_execution_id`; end event handler resumes parent

## Partitioned Write Path

Like Camunda 8, GoFlow can run every process instance on one partition: `WRITE_PATH=partitioned` (default `direct`). Full design: [`architecture/partitioned-write-path.md`](../architecture/partitioned-write-path.md).

- **Every command, every BPMN construct** — start, job completion/failure/BPMN error, retries, incident resolution, cancellation, user-task completion, variables, modification, message publication (on the partition of its correlation key), signal broadcast, timers, conditional events. No shape restriction.
- **How** — the command goes to the Kafka partition of its process instance (`goflow.partition-commands`, `KAFKA_PARTITIONS`, 12 by default, fixed once instances exist). The GoFlow instances form one consumer group: each partition is owned by exactly one of them, which runs its commands one at a time and sends the outcome back (`goflow.partition-replies`). The same handler code runs in direct mode, without Kafka.
- **Failover** — when an instance stops, its partitions move to the others (measured: ~7 s after a `docker kill`, no instance lost — `examples/tests/drill_partition_failover.ts`). A command left half-applied by a dying owner is delivered again and completed from where it stopped.
- **Topology** — `GET /v2/topology` and the gRPC `Topology` RPC show which instance leads which partition.
- **Requires** `KAFKA_BROKERS` (the bundled Redpanda). Postgres (or Citus) stays the state; the full regression passes in both modes, on one instance and on three behind the load balancer, and on Citus.
