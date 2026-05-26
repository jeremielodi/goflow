Yes — technically, building a lightweight Camunda-like workflow engine in Go can be a very good idea depending on your real requirements.

But there is a major difference between:

building a workflow orchestration engine for your company/project
vs
building a full Camunda replacement

Those are very different levels of complexity.

When it is a GOOD idea

It is a good idea if:

You mainly need:
BPMN execution
Human tasks
Approval workflows
Timers
Service tasks
Forms
Notifications
REST API
Simple process monitoring
Your workflows are:
internal business processes
document approvals
procurement
HR workflows
ticket systems
field operations
You already control:
frontend
infrastructure
deployment
You want:
no vendor lock-in
lower infrastructure costs
simpler deployment
better customization

For your use case (approval/document systems), this is VERY realistic.

Why Go is an excellent choice

Go is actually one of the best languages for workflow engines.

Advantages:

goroutines → ideal for process execution
channels → event orchestration
low memory usage
excellent concurrency
easy deployment
static binary
high performance
easy Docker deployment
excellent for microservices

Camunda itself uses:

Java
threads
job executors
async queues

Go can do this extremely well.

The IMPORTANT reality

You should NOT try to rebuild ALL Camunda 8 features.

Camunda 8 is huge.

It includes:

distributed execution
Zeebe broker
partitioning
clustering
retries
exporters
Elasticsearch/OpenSearch
Operate
Optimize
Tasklist
identity
multi-tenancy
scalable workers
message correlation
streaming engine
FEEL engine
incident management
backpressure
event sourcing

That took years and large teams.

Better strategy

Instead:

Build a "Focused BPMN Engine"

This is MUCH smarter.

You can support:

Phase 1
BPMN import
Start events
End events
Sequence flows
Exclusive gateway
User tasks
Service tasks
Variables
REST API
Phase 2
Parallel gateway
Timers
Message events
Forms
Attachments
Notifications
Phase 3
Process monitoring
Retry policies
Job queue
Multi-user assignment
Audit logs
Phase 4
Distributed workers
Horizontal scaling
Event streaming

This becomes VERY powerful already.

Reusing the Camunda Modeler

This is actually the smartest part.

You can absolutely use:

Camunda Modeler

because BPMN is a standard XML format.

Your Go engine can:

Load .bpmn XML
Parse nodes
Build execution graph
Execute workflow

This is exactly what Camunda does internally.

Go libraries for BPMN parsing exist, or you can build your own parser using:

encoding/xml
graph structures
state machines
Architecture recommendation

A good architecture:

Frontend
   ↓
API Gateway
   ↓
Workflow Engine
   ├── BPMN Parser
   ├── Execution Engine
   ├── Job Scheduler
   ├── Timer Service
   ├── Task Manager
   └── Event Bus
   ↓
DuckDB/PostgreSQL