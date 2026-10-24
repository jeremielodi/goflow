# Using GoFlow from a Java application

A Java application on another machine can drive GoFlow in three ways:

| Way | Library | Port | Use it for |
|---|---|---|---|
| [Plain REST](#1-plain-rest) | none (`java.net.http`) + Jackson | 8080 | Everything: deploying, starting instances, user tasks, workers, messages, history, search. |
| [Camunda 7 External Task Client](#2-camunda-7-external-task-client) | `org.camunda.bpm:camunda-external-task-client` | 8080 | Workers (service tasks). The library a Camunda 7 worker already uses. |
| [Camunda 8 Java client](#3-camunda-8-java-client-grpc) | `io.camunda:zeebe-client-java` or `io.camunda:camunda-client-java` | 26500 | Workers, starting instances, messages, signals, over gRPC. User tasks still go through REST. |

They can be mixed. A typical application starts instances and handles user tasks over REST, and runs its workers with one of the two clients.

Each section below comes from [examples/java-client](../examples/java-client), a Maven project with one runnable class per way. They drive the same process ([java-order.bpmn](../examples/java-client/src/main/resources/java-order.bpmn)): a user task `approveOrder` for the group `sales`, a service task of type `charge-card`, then a wait for the message `PaymentConfirmed`, correlated on `orderId`.

## Tested versions

Tested on 2026-10-24 against GoFlow `main`, on Java 18 (the examples compile for Java 17):

| Library | Version | Result |
|---|---|---|
| Jackson + `java.net.http` | 2.17.2 | Every call in `RestExample` works. |
| `camunda-external-task-client` | 7.24.0 | Fetch-and-lock with long polling, `complete`, `handleFailure` with a retry; Basic and JWT authentication. |
| `zeebe-client-java` | 8.3.22 | Topology, deployment, job worker, instance creation, message publication. |
| `camunda-client-java` | 8.8.44 | Over gRPC: the same commands, plus `BroadcastSignal`, job worker in polling and streaming mode. **Over REST it does not work** (see [below](#3-camunda-8-java-client-grpc)). |

Not tested: Spring Zeebe / the Camunda Spring Boot starter, TLS, OAuth.

## Run the examples

```bash
cd examples/java-client
export GOFLOW_URL=http://goflow-host:8080      # default http://127.0.0.1:8080
export GOFLOW_GRPC=goflow-host:26500           # default 127.0.0.1:26500
export GOFLOW_USER=admin@goflow.com GOFLOW_PASSWORD=admin123

./mvnw -q compile exec:java -Dexec.mainClass=com.goflow.examples.RestExample     # Windows: mvnw.cmd
./mvnw -q compile exec:java -Dexec.mainClass=com.goflow.examples.ExternalTaskWorkerExample
./mvnw -q compile exec:java -Dexec.mainClass=com.goflow.examples.ZeebeClientExample
```

`mvnw` is the Maven Wrapper: it downloads Maven 3.9.9 on first use, so only a JDK 17+ is needed. Each example deploys the BPMN, runs one order to its end, checks the result at every step, and ends with `… OK`. A failed check stops it with `CHECK FAILED: …`.

Integration suite 54 runs all of them as part of `run_all.ts` (see [Integration Tests](testing.md)), so these examples stay true to GoFlow.

## Authentication

Every call needs a GoFlow user. There are three ways to send it.

- **JWT** (REST and gRPC). `POST /auth/login` with `{"email", "password"}` returns `access_token` and `refresh_token` (snake_case). Send `Authorization: Bearer <access_token>`. The access token lives **15 minutes** and the refresh token 7 days, so a long-running client must call `POST /auth/refresh` with `{"refresh_token": "…"}` before the access token expires. [GoFlowRestClient](../examples/java-client/src/main/java/com/goflow/examples/GoFlowRestClient.java) does that: it refreshes 3 minutes early, logs in again when the refresh token is gone, and retries once on a `401`. Share one instance across threads.
- **HTTP Basic** (REST only). The GoFlow e-mail and password, on `/engine-rest` and `/v2` alike. Simplest for a worker: nothing expires.
- **gRPC**: the same credentials in the `authorization` metadata header: `Bearer <access_token>` (a GoFlow JWT, or an OIDC token when OIDC is configured) or `Basic <base64(email:password)>`. A call without valid credentials fails with `UNAUTHENTICATED`.

> **Security.** The gRPC gateway (port 26500) has no TLS. Keep it off networks you don't trust, or put it behind a TLS proxy that only your applications can reach; Docker Compose publishes it by default, so remove the `26500:26500` line if you don't use gRPC. `GRPC_ALLOW_ANONYMOUS=true` lets calls with **no** credentials run as the first super_admin (the gateway's former default): only on a trusted local network. Invalid credentials are refused either way.

Permissions are per process: a user needs `VIEW` to see an instance, `START` to start one, `MANAGE` to modify one (see [process permissions](api.md#process-permissions)). Use a dedicated GoFlow user for each application rather than the admin.

## 1. Plain REST

Only Jackson is needed:

```xml
<dependency>
  <groupId>com.fasterxml.jackson.core</groupId>
  <artifactId>jackson-databind</artifactId>
  <version>2.17.2</version>
</dependency>
```

The snippets use [GoFlowRestClient](../examples/java-client/src/main/java/com/goflow/examples/GoFlowRestClient.java) (around 200 lines: token handling, JSON, multipart upload); copy it or write your own. The full flow is in [RestExample](../examples/java-client/src/main/java/com/goflow/examples/RestExample.java).

```java
GoFlowRestClient goflow = new GoFlowRestClient("http://goflow-host:8080", "app@acme.com", "secret");

// Deploy (multipart: "resources" = the file, "deployment-name")
goflow.deploy("java-order.bpmn", bpmnBytes, "Java examples");

// Start by process key, with a business key and typed variables
String instanceId = goflow.post("/engine-rest/process-definition/key/JavaOrder/start", Map.of(
        "businessKey", orderId,
        "variables", Map.of(
                "orderId", Map.of("value", orderId, "type", "String"),
                "amount",  Map.of("value", 120, "type", "Integer"))))
    .path("id").asText();
```

Instance IDs are UUID strings.

### User tasks

```java
JsonNode tasks = goflow.get("/engine-rest/task" + query(
        "processInstanceId", instanceId,
        "taskDefinitionKey", "approveOrder",
        "candidateGroup", "sales"));
String taskId = tasks.get(0).path("id").asText();

goflow.post("/engine-rest/task/" + taskId + "/claim", Map.of("userId", "alice"));
goflow.post("/engine-rest/task/" + taskId + "/complete", Map.of(
        "variables", Map.of("approved", Map.of("value", true, "type", "Boolean"))));
```

A task shows up a moment after the instance reaches it, so poll. Every filter of the task query (`assignee`, `unassigned=true`, `nameLike`, sorting, paging…) is listed in the [API reference](api.md#camunda-7-task-api-engine-resttask).

### Workers over REST

```java
JsonNode jobs = goflow.post("/engine-rest/external-task/fetchAndLock", Map.of(
        "workerId", "billing-1",
        "maxTasks", 10,
        "asyncResponseTimeout", 10000,          // wait up to 10 s for a job (long polling)
        "topics", List.of(Map.of("topicName", "charge-card", "lockDuration", 30000))));

for (JsonNode job : jobs) {
    int amount = job.path("variables").path("amount").path("value").asInt();
    // ... do the work ...
    goflow.post("/engine-rest/external-task/" + job.path("id").asText() + "/complete", Map.of(
            "workerId", "billing-1",
            "variables", Map.of("chargeId", Map.of("value", "CH-1", "type", "String"))));
}
```

On failure, `POST …/{id}/failure` with `{"workerId", "errorMessage", "retries", "retryTimeout"}`. `retries` is what remains after this failure: `0` raises an incident. A BPMN error goes to `…/bpmnError`. In practice the [Camunda 7 client](#2-camunda-7-external-task-client) does all of this for you.

### Messages

```java
goflow.post("/engine-rest/message", Map.of(
        "messageName", "PaymentConfirmed",
        "correlationKeys", Map.of("orderId", orderId),   // plain values, not {"value", "type"}
        "variables", Map.of("paidAt", "2026-10-24T10:00:00Z")));
```

`correlationKeys` takes **plain values**. Camunda 7's typed form (`{"orderId": {"value": "…", "type": "String"}}`) doesn't match anything in GoFlow and answers `404 no process instance found waiting for this message`. A `404` also means the token hasn't reached the catch event yet: retry. `POST /v2/messages/publication` (`name`, `correlationKey`, `variables`, `timeToLive`) works the Camunda 8 way and buffers the message for `timeToLive` ms instead.

### History and search

```java
goflow.get("/engine-rest/history/process-instance/" + instanceId);            // "state": "completed"
goflow.get("/engine-rest/history/process-instance" + query(
        "processDefinitionKey", "JavaOrder", "businessKey", orderId, "state", "completed"));
goflow.get("/engine-rest/history/task" + query("processInstanceId", instanceId, "finished", "true"));
goflow.get("/engine-rest/process-instance/" + instanceId + "/variables");     // also after the end
```

A finished instance is still found by these calls: GoFlow moves it to its history tables about half a second after it ends, and they read both. The full list of filters is in the [API reference](api.md).

## 2. Camunda 7 External Task Client

```xml
<dependency>
  <groupId>org.camunda.bpm</groupId>
  <artifactId>camunda-external-task-client</artifactId>
  <version>7.24.0</version>
</dependency>
<!-- Required: the client loads JAXB at startup, and the JDK has no JAXB since Java 11.
     Without it: NoClassDefFoundError jakarta/xml/bind/JAXBException. -->
<dependency>
  <groupId>org.glassfish.jaxb</groupId>
  <artifactId>jaxb-runtime</artifactId>
  <version>4.0.5</version>
</dependency>
```

Point `baseUrl` at GoFlow's `/engine-rest` and add authentication. HTTP Basic is built in:

```java
ExternalTaskClient client = ExternalTaskClient.create()
        .baseUrl("http://goflow-host:8080/engine-rest")
        .workerId("billing-1")
        .maxTasks(5)
        .asyncResponseTimeout(10_000)                                   // long polling
        .addInterceptor(new BasicAuthProvider("worker@acme.com", "secret"))
        .build();
```

With a JWT instead, add the header yourself, from a token source that refreshes it:

```java
        .addInterceptor(request -> request.addHeader("Authorization", "Bearer " + goflow.accessToken()))
```

Then subscribe to a topic: the service task's `zeebe:taskDefinition type` or `camunda:topic`.

```java
client.subscribe("charge-card")
        .lockDuration(30_000)
        .variables("orderId", "amount")                                 // fetch only these
        .handler((task, service) -> {
            String orderId = task.getVariable("orderId");
            try {
                String chargeId = billing.charge(orderId, task.<Integer>getVariable("amount"));
                service.complete(task, Map.of("chargeId", chargeId));
            } catch (TemporaryFailure e) {
                // retries = what remains after this failure (0 raises an incident); retryTimeout in ms
                service.handleFailure(task, e.getMessage(), null, 2, 10_000);
            }
        })
        .open();
```

Stop with `client.stop()`. The whole worker is in [ExternalTaskWorkerExample](../examples/java-client/src/main/java/com/goflow/examples/ExternalTaskWorkerExample.java), which fails each job once and then completes it. Set `GOFLOW_WORKER_AUTH=jwt` to run it with a JWT instead of Basic.

## 3. Camunda 8 Java client (gRPC)

GoFlow's gateway speaks the Zeebe gRPC protocol of the 8.3 generation. Either client works over gRPC:

```xml
<!-- the 8.3 client -->
<dependency>
  <groupId>io.camunda</groupId>
  <artifactId>zeebe-client-java</artifactId>
  <version>8.3.22</version>
</dependency>
<!-- or the current client: gRPC only, see below -->
<dependency>
  <groupId>io.camunda</groupId>
  <artifactId>camunda-client-java</artifactId>
  <version>8.8.44</version>
</dependency>
```

### zeebe-client-java 8.3

```java
CredentialsProvider jwt = new CredentialsProvider() {
    @Override
    public void applyCredentials(Metadata headers) {                    // io.grpc.Metadata
        headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                "Bearer " + goflow.accessToken());
    }
    @Override
    public boolean shouldRetryRequest(Throwable t) {
        return t instanceof StatusRuntimeException e && e.getStatus().getCode() == Status.Code.UNAVAILABLE;
    }
};

try (ZeebeClient zeebe = ZeebeClient.newClientBuilder()
        .gatewayAddress("goflow-host:26500")
        .usePlaintext()                                                 // the gateway has no TLS
        .credentialsProvider(jwt)
        .build()) {

    zeebe.newDeployResourceCommand().addResourceFromClasspath("java-order.bpmn").send().join();

    JobWorker worker = zeebe.newWorker()
            .jobType("charge-card")
            .handler((client, job) -> client.newCompleteCommand(job.getKey())
                    .variables(Map.of("chargeId", "CH-" + job.getVariablesAsMap().get("orderId")))
                    .send().join())
            .open();

    long instanceKey = zeebe.newCreateInstanceCommand()
            .bpmnProcessId("JavaOrder").latestVersion()
            .variables(Map.of("orderId", orderId, "amount", 50))
            .send().join()
            .getProcessInstanceKey();

    zeebe.newPublishMessageCommand()
            .messageName("PaymentConfirmed")
            .correlationKey(orderId)                  // matched against zeebe:subscription correlationKey
            .send().join();
}
```

The whole flow is in [ZeebeClientExample](../examples/java-client/src/main/java/com/goflow/examples/ZeebeClientExample.java).

### camunda-client-java 8.8

The current client defaults to Camunda 8's REST API for many commands. That doesn't work against GoFlow: its `/v2` responses are simplified (keys are UUID strings, for one), the client can't parse them, and every command fails with `MalformedResponseException`. Force gRPC:

```java
CamundaClient client = CamundaClient.newClientBuilder()
        .grpcAddress(URI.create("http://goflow-host:26500"))
        .restAddress(URI.create("http://goflow-host:8080"))
        .preferRestOverGrpc(false)                                      // required with GoFlow
        .credentialsProvider(new CredentialsProvider() {
            public void applyCredentials(CredentialsApplier applier) {
                applier.put("Authorization", "Bearer " + goflow.accessToken());
            }
            public boolean shouldRetryRequest(StatusCode status) { return false; }
        })
        .build();
```

HTTP Basic works too, with the client's own provider, and needs no token refresh:

```java
        .credentialsProvider(CredentialsProvider.newBasicAuthCredentialsProviderBuilder()
                .username("worker@acme.com").password("secret").build())
```

With that, the commands above work under their 8.8 names (`io.camunda.client…`), and so does `newBroadcastSignalCommand()`. Job workers work in both modes, polling and `.streamEnabled(true)`. Commands that exist only in Camunda 8's REST API (user tasks, searches) are not available this way: use GoFlow's REST for them.

### What the gRPC side can't do

- **User tasks are not jobs** in GoFlow. Find and complete them over REST: `POST /v2/user-tasks/search`, then `POST /v2/user-tasks/{userTaskKey}/assignment` and `…/completion`.
- **gRPC keys and REST IDs don't match.** gRPC hands out numbers (`instanceKey = 109`); REST uses UUIDs, and no endpoint converts one into the other. To find over REST an instance created over gRPC, look for it by a business variable: the example lists the open `approveOrder` tasks and keeps the one whose instance has the right `orderId` (`/v2/variables/search`). When an application needs both, it is simpler to start its instances over REST, with a `businessKey`, and use gRPC for the workers.
- RPCs of newer clients that GoFlow doesn't have yet: `MigrateProcessInstance`, `UpdateJobTimeout`.

A harmless `SEVERE … RejectedExecutionException` can show up when the client closes while a worker's long poll is still open.

## Pitfalls

| Symptom | Cause |
|---|---|
| `401` after about 15 minutes | Access token expired: refresh it (`POST /auth/refresh`), or use HTTP Basic on REST. |
| `NoClassDefFoundError: jakarta/xml/bind/JAXBException` | Camunda 7 client without `jaxb-runtime`. |
| `MalformedResponseException` from `camunda-client-java` | REST mode: add `.preferRestOverGrpc(false)`. |
| Message `404 no process instance found waiting` | Typed `correlationKeys` (send plain values), or the token isn't at the catch event yet (retry). |
| gRPC `UNAUTHENTICATED` | No `authorization` header, a wrong password, or an expired or foreign token. |
| gRPC `NOT_FOUND` or `you do not have START access` | Valid credentials, but the user lacks the permission on that process. |
| A filter seems ignored | A date in another format than RFC 3339 is ignored; an unknown `/v2` `state` too (see [API conventions](api.md#conventions)). |
| `Cannot POST /v2/deployments` | Deploy with `POST /v2/resources/deployments`, `POST /engine-rest/deployment/create`, or gRPC. |
