package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
)

// maxAsyncResponseTimeout caps how long a fetchAndLock long poll waits.
const maxAsyncResponseTimeout = 60 * time.Second

// FetchAndLock handles POST /engine-rest/external-task/fetchAndLock, as in
// Camunda 7: Camunda's LockedExternalTaskDto fields, the topic's variables
// filter, and long polling with asyncResponseTimeout.
func (ctrl *ExternalTaskController) FetchAndLock(c *fiber.Ctx) error {
	var req FetchAndLockRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "invalid request body",
			"error":   err.Error(),
		})
	}
	if req.MaxTasks <= 0 {
		req.MaxTasks = 10
	}
	topics := make([]repository.TopicConfig, len(req.Topics))
	for i, topic := range req.Topics {
		topics[i] = repository.TopicConfig{TopicName: topic.TopicName, LockDuration: topic.LockDuration}
	}

	wait := time.Duration(req.AsyncResponseTimeout) * time.Millisecond
	if wait > maxAsyncResponseTimeout {
		wait = maxAsyncResponseTimeout
	}
	deadline := time.Now().Add(wait)
	var topicJobs []repository.TopicJobs
	for {
		// Taken before looking, so a job created meanwhile still wakes us.
		var wake <-chan struct{}
		if len(topics) > 0 {
			wake = repository.JobWakeChannel(topics[0].TopicName)
		}
		var err error
		topicJobs, err = ctrl.lockJobs(topics, req.WorkerId, req.MaxTasks)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"title": "Database Error", "message": "failed to fetch jobs", "error": err.Error()})
		}
		if countJobs(topicJobs) > 0 || time.Now().After(deadline) {
			break
		}
		// Woken as soon as a job of the first topic is created on any
		// GoFlow instance; the other topics at the next poll.
		select {
		case <-wake:
		case <-time.After(500 * time.Millisecond):
		}
	}
	for _, topicJob := range topicJobs {
		for _, job := range topicJob.Jobs {
			service.LogAuditErr("job locked", ctrl.auditService.LogJobLocked(job.ID, job.ProcessInstanceID, req.WorkerId))
		}
	}

	lockDuration := map[string]int{}
	wanted := map[string][]string{}
	for _, t := range req.Topics {
		lockDuration[t.TopicName] = t.LockDuration
		// An empty list means all of them, as GoFlow clients have always sent.
		if len(t.Variables) > 0 {
			wanted[t.TopicName] = t.Variables
		}
	}
	instances := ctrl.instanceInfo(topicJobs)
	engineRepo := repository.NewEngineRepository(ctrl.db, ctrl.dispatcher)
	response := []FetchAndLockResponse{}
	for _, topicJob := range topicJobs {
		for _, job := range topicJob.Jobs {
			rawVars := make(map[string]interface{})
			if len(job.Payload) > 0 {
				if err := json.Unmarshal(job.Payload, &rawVars); err != nil {
					log.Printf("Warning: failed to parse job payload for job %s: %v", job.ID, err)
				}
			}
			if names, ok := wanted[topicJob.TopicName]; ok {
				keep := make(map[string]interface{}, len(names))
				for _, n := range names {
					if v, ok := rawVars[n]; ok {
						keep[n] = v
					}
				}
				rawVars = keep
			}

			var headers map[string]string
			if graph, gerr := engineRepo.GetProcessGraphByInstanceID(job.ProcessInstanceID); gerr == nil && graph != nil {
				if n, ok := graph.Nodes[job.CurrentElementID]; ok {
					headers = n.TaskHeaders
				}
			}
			expires := time.Now().Add(time.Duration(lockDuration[topicJob.TopicName]) * time.Millisecond)
			info := instances[job.ProcessInstanceID]
			response = append(response, FetchAndLockResponse{
				ID:                   job.ID.String(),
				TopicName:            topicJob.TopicName,
				ProcessInstanceID:    job.ProcessInstanceID.String(),
				Variables:            formatVariablesForResponse(rawVars),
				Retries:              job.Retries,
				WorkerId:             req.WorkerId,
				Headers:              headers,
				ActivityID:           job.CurrentElementID,
				ActivityInstanceID:   job.ExecutionID.String(),
				ExecutionID:          job.ExecutionID.String(),
				ProcessDefinitionID:  info.DefinitionID.String(),
				ProcessDefinitionKey: info.ProcessKey,
				BusinessKey:          info.BusinessKey,
				TenantID:             info.TenantID,
				LockExpirationTime:   c7Time(&expires),
				CreateTime:           c7Time(&job.CreatedAt),
			})
		}
	}
	return c.JSON(response)
}

func countJobs(topicJobs []repository.TopicJobs) int {
	n := 0
	for _, t := range topicJobs {
		n += len(t.Jobs)
	}
	return n
}

// lockJobs locks up to max jobs of the topics for workerID.
func (ctrl *ExternalTaskController) lockJobs(topics []repository.TopicConfig, workerID string, max int) ([]repository.TopicJobs, error) {
	tx, err := ctrl.db.Beginx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	topicJobs, err := ctrl.externalTaskRepo.FetchAndLockJobsTx(tx, topics, workerID, max)
	if err != nil {
		return nil, err
	}
	return topicJobs, tx.Commit()
}

type jobInstanceInfo struct {
	ID           uuid.UUID `db:"id"`
	DefinitionID uuid.UUID `db:"process_definition_id"`
	ProcessKey   string    `db:"process_key"`
	TenantID     *string   `db:"tenant_id"`
	BusinessKey  *string   `db:"business_key"`
}

// instanceInfo reads, for the jobs' instances, what Camunda 7 returns
// with each task: definition, business key, tenant.
func (ctrl *ExternalTaskController) instanceInfo(topicJobs []repository.TopicJobs) map[uuid.UUID]jobInstanceInfo {
	var ids []string
	for _, t := range topicJobs {
		for _, j := range t.Jobs {
			ids = append(ids, j.ProcessInstanceID.String())
		}
	}
	out := map[uuid.UUID]jobInstanceInfo{}
	if len(ids) == 0 {
		return out
	}
	var rows []jobInstanceInfo
	if err := ctrl.db.Select(&rows, `
		SELECT p.id, p.process_definition_id, d.process_key, p.tenant_id, p.business_key
		FROM public.process_instances p
		JOIN public.process_definitions d ON d.id = p.process_definition_id
		WHERE p.id = ANY($1::uuid[])`, ids); err != nil {
		log.Printf("fetchAndLock: reading the instances of the locked tasks: %v", err)
		return out
	}
	for _, r := range rows {
		out[r.ID] = r
	}
	return out
}

// lockHeldBy answers Camunda 7's errors when the worker doesn't hold the
// task's lock: 404 for an unknown task, 500 otherwise.
func (ctrl *ExternalTaskController) lockHeldBy(c *fiber.Ctx, jobID uuid.UUID, workerID string) (bool, error) {
	owner, err := ctrl.externalTaskRepo.LockOwner(jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, c7Error(c, fiber.StatusNotFound, "RestException", fmt.Sprintf("External task with id %s does not exist", jobID))
	}
	if err != nil {
		return false, c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}
	if owner != workerID {
		return false, c7Error(c, fiber.StatusInternalServerError, "BadUserRequestException",
			fmt.Sprintf("External Task %s cannot be handled by worker '%s'. It is locked by worker '%s'.", jobID, workerID, owner))
	}
	return true, nil
}

// HandleBpmnError handles POST /engine-rest/external-task/:id/bpmnError
// {"workerId", "errorCode", "errorMessage", "variables"}: the BPMN error
// is caught by the nearest matching error boundary or error event
// sub-process (or a calling process); with none, an incident is raised.
func (ctrl *ExternalTaskController) HandleBpmnError(c *fiber.Ctx) error {
	jobID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", "invalid external task id")
	}
	var body struct {
		WorkerID     string                `json:"workerId"`
		ErrorCode    string                `json:"errorCode"`
		ErrorMessage string                `json:"errorMessage"`
		Variables    map[string]c7Variable `json:"variables"`
	}
	if err := json.Unmarshal(c.Body(), &body); err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
	}
	if body.ErrorCode == "" {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", "errorCode is required")
	}
	if ok, err := ctrl.lockHeldBy(c, jobID, body.WorkerID); !ok {
		return err
	}
	vars, err := fromC7Variables(body.Variables)
	if err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
	}
	out, err := runtime.FailJob(c.Context(), ctrl.db, runtime.FailJobArgs{
		JobID:        jobID,
		ErrorCode:    body.ErrorCode,
		ErrorMessage: body.ErrorMessage,
		Variables:    vars,
		Source:       "bpmnError",
	})
	if err != nil {
		return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}
	if out.ContinueError != "" {
		return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", "error caught, but continuing the process failed: "+out.ContinueError)
	}
	return c.SendStatus(fiber.StatusNoContent)
}

// ExtendLock handles POST /engine-rest/external-task/:id/extendLock
// {"workerId", "newDuration" (ms)}.
func (ctrl *ExternalTaskController) ExtendLock(c *fiber.Ctx) error {
	jobID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", "invalid external task id")
	}
	var body struct {
		WorkerID    string `json:"workerId"`
		NewDuration int64  `json:"newDuration"`
	}
	if err := json.Unmarshal(c.Body(), &body); err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
	}
	if body.NewDuration <= 0 {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", "newDuration must be positive")
	}
	if ok, err := ctrl.lockHeldBy(c, jobID, body.WorkerID); !ok {
		return err
	}
	until := time.Now().Add(time.Duration(body.NewDuration) * time.Millisecond)
	if err := ctrl.externalTaskRepo.ExtendLock(jobID, body.WorkerID, until); err != nil {
		return c7Error(c, fiber.StatusInternalServerError, "BadUserRequestException", err.Error())
	}
	return c.SendStatus(fiber.StatusNoContent)
}

// Unlock handles POST /engine-rest/external-task/:id/unlock: the task can
// be fetched again right away.
func (ctrl *ExternalTaskController) Unlock(c *fiber.Ctx) error {
	jobID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", "invalid external task id")
	}
	if _, err := ctrl.externalTaskRepo.LockOwner(jobID); errors.Is(err, sql.ErrNoRows) {
		return c7Error(c, fiber.StatusNotFound, "RestException", fmt.Sprintf("External task with id %s does not exist", jobID))
	}
	if err := ctrl.externalTaskRepo.Unlock(jobID); err != nil && !errors.Is(err, repository.ErrJobNotLockedBy) {
		return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}
	return c.SendStatus(fiber.StatusNoContent)
}
