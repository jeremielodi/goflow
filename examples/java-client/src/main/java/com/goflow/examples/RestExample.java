package com.goflow.examples;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.goflow.examples.Examples.check;
import static com.goflow.examples.Examples.waitFor;
import static com.goflow.examples.GoFlowRestClient.query;

/**
 * One order through the JavaOrder process, with nothing but REST calls:
 * deploy, start, find and complete the user task, act as the "charge-card"
 * worker, correlate the message, then read the history.
 */
public class RestExample {

    public static void main(String[] args) {
        GoFlowRestClient goflow = GoFlowRestClient.fromEnv();
        String orderId = "ORDER-" + UUID.randomUUID().toString().substring(0, 8);
        String workerId = "java-rest-worker";

        // 1. Deploy the BPMN (same file every run: GoFlow keeps one version per change).
        JsonNode deployment = goflow.deploy("java-order.bpmn", Examples.resource("java-order.bpmn"), "Java examples");
        check(deployment.has("id"), "deployed java-order.bpmn");

        // 2. Start an instance by process key, with a business key and typed variables.
        JsonNode started = goflow.post("/engine-rest/process-definition/key/JavaOrder/start", Map.of(
                "businessKey", orderId,
                "variables", Map.of(
                        "orderId", Map.of("value", orderId, "type", "String"),
                        "amount", Map.of("value", 120, "type", "Integer"))));
        String instanceId = started.path("id").asText();
        check(!instanceId.isEmpty(), "started instance " + instanceId + " (businessKey " + orderId + ")");

        // 3. Find the open user task: Camunda 7 task query, filtered.
        JsonNode task = waitFor("approveOrder task", Duration.ofSeconds(10), () -> first(goflow.get(
                "/engine-rest/task" + query(
                        "processInstanceId", instanceId,
                        "taskDefinitionKey", "approveOrder",
                        "candidateGroup", "sales"))));
        String taskId = task.path("id").asText();
        check("Approve Order".equals(task.path("name").asText()), "found task " + taskId);

        // 4. Claim it, then complete it with a variable.
        goflow.post("/engine-rest/task/" + taskId + "/claim", Map.of("userId", "alice"));
        JsonNode mine = goflow.get("/engine-rest/task" + query("assignee", "alice", "processInstanceId", instanceId));
        check(mine.size() == 1, "task is now alice's");
        goflow.post("/engine-rest/task/" + taskId + "/complete", Map.of(
                "variables", Map.of("approved", Map.of("value", true, "type", "Boolean"))));

        // 5. Be the "charge-card" worker: fetch-and-lock, then complete.
        JsonNode job = waitFor("charge-card job", Duration.ofSeconds(10), () -> {
            for (JsonNode j : goflow.post("/engine-rest/external-task/fetchAndLock", Map.of(
                    "workerId", workerId,
                    "maxTasks", 10,
                    "topics", List.of(Map.of("topicName", "charge-card", "lockDuration", 30000))))) {
                if (instanceId.equals(j.path("processInstanceId").asText())) {
                    return Optional.of(j);
                }
            }
            return Optional.empty();
        });
        check(job.path("variables").path("amount").path("value").asInt() == 120, "job carries amount=120");
        goflow.post("/engine-rest/external-task/" + job.path("id").asText() + "/complete", Map.of(
                "workerId", workerId,
                "variables", Map.of("chargeId", Map.of("value", "CH-" + orderId, "type", "String"))));

        // 6. Correlate the message the process now waits for (plain values in correlationKeys).
        waitFor("message correlation", Duration.ofSeconds(10), () -> {
            try {
                return Optional.of(goflow.post("/engine-rest/message", Map.of(
                        "messageName", "PaymentConfirmed",
                        "correlationKeys", Map.of("orderId", orderId),
                        "variables", Map.of("paidAt", "2026-10-24T10:00:00Z"))));
            } catch (GoFlowRestClient.GoFlowException e) {
                return Optional.empty(); // the token hasn't reached the catch event yet
            }
        });

        // 7. History: the instance is completed (served from the archive once archived).
        waitFor("instance completed", Duration.ofSeconds(15), () -> {
            JsonNode h = goflow.get("/engine-rest/history/process-instance/" + instanceId);
            return "completed".equals(h.path("state").asText()) ? Optional.of(h) : Optional.empty();
        });
        check(true, "instance completed");

        JsonNode byBusinessKey = goflow.get("/engine-rest/history/process-instance" + query(
                "processDefinitionKey", "JavaOrder", "businessKey", orderId, "state", "completed"));
        check(byBusinessKey.size() == 1, "history search by businessKey finds it");

        JsonNode doneTasks = goflow.get("/engine-rest/history/task" + query(
                "processInstanceId", instanceId, "finished", "true"));
        check(doneTasks.size() == 1 && "alice".equals(doneTasks.get(0).path("assignee").asText()),
                "history: one finished task, by alice");

        JsonNode vars = goflow.get("/engine-rest/process-instance/" + instanceId + "/variables");
        check(vars.has("chargeId") && vars.has("paidAt") && vars.has("approved"),
                "final variables: " + vars.fieldNames().next() + ", … (" + vars.size() + " in all)");

        System.out.println("RestExample OK");
    }

    private static Optional<JsonNode> first(JsonNode array) {
        return array != null && array.isArray() && array.size() > 0 ? Optional.of(array.get(0)) : Optional.empty();
    }
}
