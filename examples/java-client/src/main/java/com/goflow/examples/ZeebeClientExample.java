package com.goflow.examples;

import com.fasterxml.jackson.databind.JsonNode;
import io.camunda.zeebe.client.CredentialsProvider;
import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ProcessInstanceEvent;
import io.camunda.zeebe.client.api.response.Topology;
import io.camunda.zeebe.client.api.worker.JobWorker;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.goflow.examples.Examples.check;
import static com.goflow.examples.Examples.waitFor;

/**
 * The Camunda 8 Java client (zeebe-client-java 8.3) against GoFlow's gRPC
 * gateway on port 26500: topology, deployment, a job worker, instance
 * creation and message publication.
 *
 * GoFlow user tasks are not jobs, so the user task is completed over REST
 * (/v2/user-tasks). gRPC keys are numbers and REST ids are UUIDs, with no
 * mapping between them over the API: the task is found by its instance's
 * orderId variable.
 */
public class ZeebeClientExample {

    public static void main(String[] args) {
        GoFlowRestClient goflow = GoFlowRestClient.fromEnv();
        String gateway = GoFlowRestClient.env("GOFLOW_GRPC", "127.0.0.1:26500");

        // Send GoFlow's JWT as gRPC metadata: the gateway refuses calls without
        // valid credentials (UNAUTHENTICATED), and applies this user's
        // per-process permissions.
        CredentialsProvider jwt = new CredentialsProvider() {
            @Override
            public void applyCredentials(Metadata headers) {
                headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                        "Bearer " + goflow.accessToken());
            }

            @Override
            public boolean shouldRetryRequest(Throwable throwable) {
                return throwable instanceof StatusRuntimeException e
                        && e.getStatus().getCode() == Status.Code.UNAVAILABLE;
            }
        };

        AtomicInteger charged = new AtomicInteger();
        try (ZeebeClient zeebe = ZeebeClient.newClientBuilder()
                .gatewayAddress(gateway)
                .usePlaintext() // GoFlow's gateway has no TLS: put one in front (see docs/java-client.md)
                .credentialsProvider(jwt)
                .defaultRequestTimeout(Duration.ofSeconds(15))
                .build()) {

            Topology topology = zeebe.newTopologyRequest().send().join();
            check(!topology.getBrokers().isEmpty(), "topology: " + topology.getBrokers().size() + " broker(s), gateway "
                    + topology.getGatewayVersion());

            long processKey = zeebe.newDeployResourceCommand()
                    .addResourceFromClasspath("java-order.bpmn")
                    .send().join()
                    .getProcesses().get(0).getProcessDefinitionKey();
            check(processKey > 0, "deployed JavaOrder, processDefinitionKey " + processKey);

            try (JobWorker worker = zeebe.newWorker()
                    .jobType("charge-card")
                    .handler((client, job) -> {
                        Map<String, Object> vars = job.getVariablesAsMap();
                        client.newCompleteCommand(job.getKey())
                                .variables(Map.of("chargeId", "CH-" + vars.get("orderId")))
                                .send().join();
                        charged.incrementAndGet();
                        System.out.println("  · charged " + vars.get("orderId") + " (" + vars.get("amount") + ")");
                    })
                    .timeout(Duration.ofSeconds(30))
                    .open()) {

                String orderId = "ORDER-" + UUID.randomUUID().toString().substring(0, 8);
                ProcessInstanceEvent instance = zeebe.newCreateInstanceCommand()
                        .bpmnProcessId("JavaOrder")
                        .latestVersion()
                        .variables(Map.of("orderId", orderId, "amount", 50))
                        .send().join();
                check(instance.getProcessInstanceKey() > 0, "created instance key " + instance.getProcessInstanceKey());

                // The user task, over REST.
                JsonNode task = waitFor("approveOrder task", Duration.ofSeconds(10), () -> findTask(goflow, orderId));
                goflow.post("/v2/user-tasks/" + task.path("userTaskKey").asText() + "/assignment", Map.of("assignee", "bob"));
                goflow.post("/v2/user-tasks/" + task.path("userTaskKey").asText() + "/completion", Map.of("variables", Map.of()));
                String instanceId = task.path("processInstanceKey").asText();

                waitFor("charge-card job", Duration.ofSeconds(20),
                        () -> charged.get() > 0 ? Optional.of(true) : Optional.empty());

                // Correlates through the message's zeebe:subscription correlationKey (=orderId).
                waitFor("payment message", Duration.ofSeconds(10), () -> {
                    zeebe.newPublishMessageCommand()
                            .messageName("PaymentConfirmed")
                            .correlationKey(orderId)
                            .variables(Map.of("paidAt", "2026-10-24T10:00:00Z"))
                            .send().join();
                    return completed(goflow, instanceId) ? Optional.of(true) : Optional.empty();
                });
                waitFor("instance completed", Duration.ofSeconds(15),
                        () -> completed(goflow, instanceId) ? Optional.of(true) : Optional.empty());
                check(true, "instance completed");
            }
        }
        System.out.println("ZeebeClientExample OK");
    }

    /** The open approveOrder task of the instance whose orderId is the given one. */
    private static Optional<JsonNode> findTask(GoFlowRestClient goflow, String orderId) {
        JsonNode tasks = goflow.post("/v2/user-tasks/search",
                Map.of("filter", Map.of("elementId", "approveOrder", "state", "CREATED"))).path("items");
        for (JsonNode t : tasks) {
            JsonNode vars = goflow.post("/v2/variables/search", Map.of("filter", Map.of(
                    "processInstanceKey", t.path("processInstanceKey").asText(), "name", "orderId"))).path("items");
            if (vars.size() == 1 && orderId.equals(vars.get(0).path("value").asText())) {
                return Optional.of(t);
            }
        }
        return Optional.empty();
    }

    private static boolean completed(GoFlowRestClient goflow, String instanceId) {
        return List.of("COMPLETED").contains(goflow.get("/v2/process-instances/" + instanceId).path("state").asText());
    }
}
