package com.goflow.examples.spring;

import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.RepositoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.TaskService;
import org.camunda.bpm.engine.externaltask.LockedExternalTask;
import org.camunda.bpm.engine.history.HistoricProcessInstance;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.task.Task;
import org.camunda.community.rest.EnableCamundaRestClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * A Camunda 7 application written against the engine's Java API
 * (RuntimeService, TaskService, ...), run against a remote GoFlow: the
 * community starter implements those interfaces with REST calls to
 * feign.client.config.default.url, authenticated with HTTP Basic (see
 * application.properties).
 */
@SpringBootApplication
@EnableCamundaRestClient
public class Camunda7JavaApiExample implements CommandLineRunner {

    private final RepositoryService repositoryService;
    private final RuntimeService runtimeService;
    private final TaskService taskService;
    private final ExternalTaskService externalTaskService;
    private final HistoryService historyService;

    public Camunda7JavaApiExample(@Qualifier("remote") RepositoryService repositoryService,
                                  @Qualifier("remote") RuntimeService runtimeService,
                                  @Qualifier("remote") TaskService taskService,
                                  @Qualifier("remote") ExternalTaskService externalTaskService,
                                  @Qualifier("remote") HistoryService historyService) {
        this.repositoryService = repositoryService;
        this.runtimeService = runtimeService;
        this.taskService = taskService;
        this.externalTaskService = externalTaskService;
        this.historyService = historyService;
    }

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(Camunda7JavaApiExample.class, args)));
    }

    @Override
    public void run(String... args) {
        String orderId = "ORDER-" + UUID.randomUUID().toString().substring(0, 8);

        // 1. Deploy.
        var deployment = repositoryService.createDeployment()
                .name("Java examples (Spring)")
                .addClasspathResource("java-order.bpmn")
                .deploy();
        check(deployment.getId() != null, "deployed " + deployment.getId());
        var definition = repositoryService.createProcessDefinitionQuery()
                .processDefinitionKey("JavaOrder")
                .latestVersion()
                .singleResult();
        check(definition != null && "JavaOrder".equals(definition.getKey()) && !definition.isSuspended(),
                "definition " + definition.getId() + ", version " + definition.getVersion());

        // 2. Start by key with a business key; plain Java values become typed variables.
        ProcessInstance instance = runtimeService.startProcessInstanceByKey("JavaOrder", orderId,
                Map.of("orderId", orderId, "amount", 120));
        String instanceId = instance.getId();
        check(orderId.equals(instance.getBusinessKey()), "started " + instanceId);

        // 3. Task query, claim, complete.
        Task task = waitFor("approveOrder task", () -> taskService.createTaskQuery()
                .processInstanceId(instanceId)
                .taskDefinitionKey("approveOrder")
                .taskCandidateGroup("sales")
                .singleResult());
        check("Approve Order".equals(task.getName()), "found task " + task.getId());
        taskService.claim(task.getId(), "alice");
        taskService.complete(task.getId(), Map.of("approved", true));
        check(true, "task claimed and completed");

        // 4. The charge-card job, through ExternalTaskService.
        LockedExternalTask job = waitFor("charge-card job", () -> externalTaskService
                .fetchAndLock(10, "java-spring-worker")
                .topic("charge-card", 30_000)
                .execute().stream()
                .filter(j -> instanceId.equals(j.getProcessInstanceId()))
                .findFirst().orElse(null));
        check(Integer.valueOf(120).equals(((Number) job.getVariables().get("amount")).intValue()), "job carries amount=120");
        externalTaskService.complete(job.getId(), "java-spring-worker", Map.of("chargeId", "CH-" + orderId));

        Map<String, Object> vars = waitFor("chargeId", () -> {
            Map<String, Object> v = runtimeService.getVariables(instanceId);
            return v.containsKey("chargeId") ? v : null;
        });
        check(("CH-" + orderId).equals(vars.get("chargeId")), "chargeId set by the worker");

        // 5. Message correlation on a process variable.
        waitFor("message correlation", () -> {
            try {
                runtimeService.createMessageCorrelation("PaymentConfirmed")
                        .processInstanceVariableEquals("orderId", orderId)
                        .setVariable("paidAt", "2026-10-24T10:00:00Z")
                        .correlate();
                return true;
            } catch (RuntimeException e) {
                System.out.println("  · correlation not yet possible: " + e.getMessage());
                return null;
            }
        });

        // 6. History.
        HistoricProcessInstance done = waitFor("completed instance", () -> {
            HistoricProcessInstance h = historyService.createHistoricProcessInstanceQuery()
                    .processInstanceId(instanceId)
                    .singleResult();
            return h != null && h.getEndTime() != null ? h : null;
        });
        check("COMPLETED".equals(done.getState()), "history: state " + done.getState());
        // (The starter doesn't implement createHistoricTaskInstanceQuery():
        // it throws kotlin.NotImplementedError on the client side.)

        System.out.println("Camunda7JavaApiExample OK");
    }

    static void check(boolean ok, String message) {
        if (!ok) {
            throw new IllegalStateException("CHECK FAILED: " + message);
        }
        System.out.println("  ✓ " + message);
    }

    /** Polls until the supplier returns non-null, for at most 15 s. */
    static <T> T waitFor(String what, Supplier<T> probe) {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(15));
        while (Instant.now().isBefore(deadline)) {
            T v = probe.get();
            if (v != null) {
                return v;
            }
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new IllegalStateException("timed out waiting for " + what);
    }
}
