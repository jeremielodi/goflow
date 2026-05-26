┌──────────────────────────────────────────────────────────────────┐
│                         Your goflow Engine                        │
├──────────────────────────────────────────────────────────────────┤
│  1. Start transaction in PostgreSQL                               │
│  2. Update runtime tables (executions, variables, tasks)          │
│  3. Insert audit event into "outbox" table (same transaction)     │
│  4. Commit transaction                                            │
└──────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────┐
│               Outbox Publisher (background worker)                │
├──────────────────────────────────────────────────────────────────┤
│  5. Poll outbox table for unprocessed events                      │
│  6. Publish each event to Kafka topic ("process-events")          │
│  7. Mark event as processed / delete row                          │
└──────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────┐
│                      Kafka Cluster                               │
├──────────────────────────────────────────────────────────────────┤
│  8. Append events to compacted topic(s)                          │
│  9. Retain for configured period (e.g., 30 days)                 │
│ 10. Tiered storage to S3 for long‑term compliance (7+ years)     │
└──────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
           ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
           │   Operate   │  │  Tasklist   │  │   Audit     │
           │ (Process    │  │ (User tasks)│  │ (History)   │
           │ monitoring) │  │             │  │             │
           └─────────────┘  └─────────────┘  └─────────────┘