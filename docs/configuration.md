# Environment Variables

Every variable can come either from the process environment or from a `.env` file (optional — real environment variables always win, and the Docker image no longer contains a `.env`). `docker-compose.yaml` reads secrets from `.env` next to it: `JWT_SECRET_KEY` is required, `POSTGRES_PASSWORD` defaults to `goflow123`.

```bash
# Database
DB_HOST=postgres
DB_PORT=5432
DB_USER=goflow
DB_PASS=goflow123
DB_SSLMODE=disable   # or require / verify-ca / verify-full
DB_NAME=goflow

# JWT — required; the server refuses to start without it, and refuses
# known sample values when GOFLOW_ENV=production. Generate with:
#   openssl rand -base64 48
JWT_SECRET_KEY=<random value>

# Superuser (seeded on first boot)
SUPER_USER_NAME=admin
SUPER_USER_EMAIL=admin@goflow.com
SUPER_USER_PASSWORD=admin123

# OIDC (optional) — set all three to accept bearer tokens from an external
# identity provider alongside the built-in JWT login; leave unset to disable
# OIDC_ISSUER=https://your-idp.example.com/realms/goflow
# OIDC_AUDIENCE=goflow-api
# OIDC_JWKS_URL=https://your-idp.example.com/realms/goflow/protocol/openid-connect/certs

# Kafka (Redpanda): event log, and the partitioned write path
# leave KAFKA_BROKERS unset to disable both
# KAFKA_BROKERS=redpanda:9092
# KAFKA_REPLICATION_FACTOR=1   # replicas per partition, applied when topics are created
# WRITE_PATH=direct            # or partitioned (Camunda 8 style, see above)
# KAFKA_PARTITIONS=12          # fixed once instances exist
# COMMAND_TIMEOUT=30s          # how long a request waits for its command

# Observability — bearer token required on GET /metrics (unset = open)
# METRICS_TOKEN=<random value>

# gRPC gateway (port 26500): every call needs credentials, as on REST —
# "authorization: Bearer <GoFlow JWT or OIDC token>" or "Basic <e-mail:password>".
# Invalid credentials are always refused (UNAUTHENTICATED). true lets a call
# with NO credentials run as the first super_admin: trusted local networks only.
# GRPC_ALLOW_ANONYMOUS=false

# Retention — automatically delete instances that finished more than N days
# ago (unset or 0 = keep everything forever); see "Operations" below
# RETENTION_DAYS=365
# RETENTION_INTERVAL=1h
# RETENTION_BATCH_SIZE=500

# FEEL expressions ("=" prefix) and DMN tables: shadow (default: legacy
# FEEL-to-CEL path decides, native FEEL runs alongside and mismatches are
# logged), feel (native interpreter decides), cel (legacy only)
# FEEL_MODE=shadow

# Connection pool per GoFlow instance (keep instances × this under
# Postgres's max_connections, or put PgBouncer in front)
# DB_MAX_OPEN_CONNS=80

# Finished instances: archived in the background (async, default) or in the
# call that finishes them (sync)
# ARCHIVE_MODE=async

# Several instances (set by docker-compose.scale.yaml, see Operations):
# relay task events between instances for SSE/WebSocket clients
# TASK_EVENTS_FANOUT=true
# with PgBouncer in front of Postgres: where session-level work (LISTEN,
# advisory locks) reaches Postgres directly
# DB_DIRECT_HOST=postgres
# DB_DIRECT_PORT=5432
# read replica for history and analytics (unset = the primary)
# DB_READ_HOST=postgres-replica
# DB_READ_PORT=5432

# High-availability cluster (deploy/ha, see Operations): passwords of the
# Patroni nodes, the same on every node
# PG_SUPERUSER_PASSWORD=<random value>
# PG_STANDBY_PASSWORD=<random value>
# PG_ADMIN_PASSWORD=<random value>

# Citus-backed distributed storage (optional — see below)
# leave unset (the default) to run on a single plain Postgres instance;
# only set when using docker-compose.citus.yml, which sets both automatically
# CITUS_ENABLED=true
# CITUS_WORKERS=citus-worker-1:5432,citus-worker-2:5432
```

**Trying the Citus-backed deployment:**

```bash
docker compose -f docker-compose.yaml -f docker-compose.citus.yml up -d --build --wait
```

Brings up a Citus coordinator + 2 workers instead of the single Postgres instance; the plain `docker compose up` is unaffected. The per-instance tables — `process_instances`, `executions`, `tasks`, `variables`, `scope_variables`, `gateway_state`, `multi_instance_executions`, `incidents`, `jobs`, `timer_jobs` — are sharded over the workers, all colocated on the process instance, so an instance's rows live on one worker. Config tables are reference tables (copied to every node); the others stay on the coordinator as Citus local tables. Jobs and timers are claimed without `FOR UPDATE` (Citus forbids it across shards): candidates are read, then each is claimed by an update on its own shard that only succeeds while it is still free. See [`architecture/citus-distributed-storage-design.md`](../architecture/citus-distributed-storage-design.md).
