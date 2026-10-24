package com.goflow.examples;

import org.camunda.community.rest.client.api.DeploymentApi;
import org.camunda.community.rest.client.api.ExternalTaskApi;
import org.camunda.community.rest.client.api.HistoricProcessInstanceApi;
import org.camunda.community.rest.client.api.MessageApi;
import org.camunda.community.rest.client.api.ProcessDefinitionApi;
import org.camunda.community.rest.client.api.ProcessInstanceApi;
import org.camunda.community.rest.client.api.TaskApi;
import org.camunda.community.rest.client.dto.CompleteExternalTaskDto;
import org.camunda.community.rest.client.dto.CompleteTaskDto;
import org.camunda.community.rest.client.dto.CorrelationMessageDto;
import org.camunda.community.rest.client.dto.FetchExternalTaskTopicDto;
import org.camunda.community.rest.client.dto.FetchExternalTasksDto;
import org.camunda.community.rest.client.dto.HistoricProcessInstanceDto;
import org.camunda.community.rest.client.dto.LockedExternalTaskDto;
import org.camunda.community.rest.client.dto.StartProcessInstanceDto;
import org.camunda.community.rest.client.dto.TaskQueryDto;
import org.camunda.community.rest.client.dto.TaskWithAttachmentAndCommentDto;
import org.camunda.community.rest.client.dto.UserIdDto;
import org.camunda.community.rest.client.dto.VariableValueDto;
import org.camunda.community.rest.client.invoker.ApiClient;
import org.camunda.community.rest.client.invoker.ApiException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.goflow.examples.Examples.check;
import static com.goflow.examples.Examples.waitFor;

/**
 * The Camunda 7 REST client generated from Camunda's OpenAPI spec
 * (org.camunda.community:camunda-engine-rest-client-openapi-java): plain
 * Java, typed DTOs, the whole /engine-rest API. Point its base path at
 * GoFlow's /engine-rest and authenticate with HTTP Basic.
 */
public class Camunda7OpenApiExample {

    public static void main(String[] args) throws Exception {
        String url = GoFlowRestClient.env("GOFLOW_URL", "http://127.0.0.1:8080");
        String user = GoFlowRestClient.env("GOFLOW_USER", "admin@goflow.com");
        String password = GoFlowRestClient.env("GOFLOW_PASSWORD", "admin123");

        ApiClient client = new ApiClient();
        client.setBasePath(url + "/engine-rest");
        client.addDefaultHeader("Authorization", "Basic " + Base64.getEncoder()
                .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)));

        String orderId = "ORDER-" + UUID.randomUUID().toString().substring(0, 8);

        // 1. Deploy (multipart; the client names the file part "data").
        File bpmn = File.createTempFile("java-order", ".bpmn");
        bpmn.deleteOnExit();
        Files.write(bpmn.toPath(), Examples.resource("java-order.bpmn"));
        var deployment = new DeploymentApi(client).createDeployment(
                null, "java-examples", false, false, "Java examples", null, bpmn);
        check(deployment.getId() != null, "deployed " + deployment.getId());

        // 2. Start by key, with a business key and typed variables.
        var started = new ProcessDefinitionApi(client).startProcessInstanceByKey("JavaOrder",
                new StartProcessInstanceDto()
                        .businessKey(orderId)
                        .variables(Map.of(
                                "orderId", typed(orderId, "String"),
                                "amount", typed(120, "Integer"))));
        String instanceId = started.getId();
        check(instanceId != null && orderId.equals(started.getBusinessKey()), "started " + instanceId);

        // 3. Query the user task, claim it, complete it.
        TaskApi tasks = new TaskApi(client);
        TaskWithAttachmentAndCommentDto task = waitFor("approveOrder task", Duration.ofSeconds(10), () -> {
            List<TaskWithAttachmentAndCommentDto> found = call(() -> tasks.queryTasks(0, 10, new TaskQueryDto()
                    .processInstanceId(instanceId)
                    .taskDefinitionKey("approveOrder")
                    .candidateGroup("sales")));
            return found.isEmpty() ? Optional.empty() : Optional.of(found.get(0));
        });
        check("Approve Order".equals(task.getName()), "found task " + task.getId());
        tasks.claim(task.getId(), new UserIdDto().userId("alice"));
        tasks.complete(task.getId(), new CompleteTaskDto().variables(Map.of("approved", typed(true, "Boolean"))));
        check(true, "task claimed and completed");

        // 4. Be the charge-card worker.
        ExternalTaskApi externalTasks = new ExternalTaskApi(client);
        LockedExternalTaskDto job = waitFor("charge-card job", Duration.ofSeconds(10), () ->
                call(() -> externalTasks.fetchAndLock(new FetchExternalTasksDto()
                        .workerId("java-openapi-worker")
                        .maxTasks(10)
                        .topics(List.of(new FetchExternalTaskTopicDto().topicName("charge-card").lockDuration(30_000L)))))
                        .stream().filter(j -> instanceId.equals(j.getProcessInstanceId())).findFirst());
        check(Integer.valueOf(120).equals(((Number) job.getVariables().get("amount").getValue()).intValue()),
                "job carries amount=120");
        externalTasks.completeExternalTaskResource(job.getId(), new CompleteExternalTaskDto()
                .workerId("java-openapi-worker")
                .variables(Map.of("chargeId", typed("CH-" + orderId, "String"))));

        // 5. Correlate the message, Camunda 7 style: typed correlation keys and process variables.
        MessageApi messages = new MessageApi(client);
        waitFor("message correlation", Duration.ofSeconds(10), () -> {
            try {
                messages.deliverMessage(new CorrelationMessageDto()
                        .messageName("PaymentConfirmed")
                        .correlationKeys(Map.of("orderId", typed(orderId, "String")))
                        .processVariables(Map.of("paidAt", typed("2026-10-24T10:00:00Z", "String"))));
                return Optional.of(true);
            } catch (ApiException e) {
                if (e.getCode() == 400 && String.valueOf(e.getResponseBody()).contains("MismatchingMessageCorrelation")) {
                    return Optional.empty(); // the token hasn't reached the catch event yet
                }
                throw new IllegalStateException("message: HTTP " + e.getCode() + " " + e.getMessage() + " " + e.getResponseBody(), e);
            }
        });

        // 6. History and variables.
        HistoricProcessInstanceApi history = new HistoricProcessInstanceApi(client);
        waitFor("instance completed", Duration.ofSeconds(15), () -> {
            var h = call(() -> history.getHistoricProcessInstance(instanceId));
            return h.getState() == HistoricProcessInstanceDto.StateEnum.COMPLETED ? Optional.of(h) : Optional.empty();
        });
        check(true, "instance completed");

        Map<String, VariableValueDto> vars = new ProcessInstanceApi(client).getProcessInstanceVariables(instanceId, true);
        check(("CH-" + orderId).equals(vars.get("chargeId").getValue())
                        && "2026-10-24T10:00:00Z".equals(vars.get("paidAt").getValue()),
                "variables: chargeId from the worker, paidAt from the message");

        System.out.println("Camunda7OpenApiExample OK");
    }

    private static VariableValueDto typed(Object value, String type) {
        return new VariableValueDto().value(value).type(type);
    }

    interface ApiCall<T> {
        T run() throws ApiException;
    }

    /** Runs an API call, rethrowing its checked ApiException with the response body. */
    private static <T> T call(ApiCall<T> c) {
        try {
            return c.run();
        } catch (ApiException e) {
            throw new IllegalStateException("HTTP " + e.getCode() + ": " + e.getResponseBody(), e);
        }
    }
}
