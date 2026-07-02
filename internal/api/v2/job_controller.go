package v2

import (
	"encoding/json"
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jmoiron/sqlx"
)

type JobController struct {
	db               *sqlx.DB
	dispatcher       *events.TaskEventDispatcher
	externalTaskRepo *repository.ExternalTaskRepository
	auditService     *service.AuditService
}

func NewJobController(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *JobController {
	return &JobController{
		db:               db,
		dispatcher:       dispatcher,
		externalTaskRepo: repository.NewExternalTaskRepository(db),
		auditService:     service.NewAuditService(db),
	}
}

// ActivateJobsRequest matches Camunda 8's job activation body. Unlike the
// Camunda 7 fetchAndLock endpoint (which can poll several topics at once),
// Zeebe activates jobs of a single type per request.
type ActivateJobsRequest struct {
	Type              string `json:"type"`
	Worker            string `json:"worker"`
	Timeout           int    `json:"timeout"` // milliseconds
	MaxJobsToActivate int    `json:"maxJobsToActivate"`
}

type v2ActivatedJob struct {
	JobKey             string                 `json:"jobKey"`
	Type               string                 `json:"type"`
	ProcessInstanceKey string                 `json:"processInstanceKey"`
	Retries            int                    `json:"retries"`
	Worker             string                 `json:"worker"`
	Variables          map[string]interface{} `json:"variables"`
	CustomHeaders      map[string]string      `json:"customHeaders,omitempty"`
}

// ActivateJobs handles POST /v2/jobs/activation.
func (jc *JobController) ActivateJobs(c *fiber.Ctx) error {
	var req ActivateJobsRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title":  "INVALID_ARGUMENT",
			"detail": err.Error(),
		})
	}
	if req.Type == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title":  "INVALID_ARGUMENT",
			"detail": "type is required",
		})
	}
	if req.MaxJobsToActivate <= 0 {
		req.MaxJobsToActivate = 10
	}

	tx, err := jc.db.Beginx()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}
	defer tx.Rollback()

	topicJobs, err := jc.externalTaskRepo.FetchAndLockJobsTx(tx, []repository.TopicConfig{
		{TopicName: req.Type, LockDuration: req.Timeout},
	}, req.Worker, req.MaxJobsToActivate)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}

	if err := tx.Commit(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}
	for _, topicJob := range topicJobs {
		for _, job := range topicJob.Jobs {
			service.LogAuditErr("job locked", jc.auditService.LogJobLocked(job.ID, job.ProcessInstanceID, req.Worker))
		}
	}

	engineRepo := repository.NewEngineRepository(jc.db, jc.dispatcher)
	jobs := []v2ActivatedJob{}
	for _, topicJob := range topicJobs {
		for _, job := range topicJob.Jobs {
			vars := make(map[string]interface{})
			if len(job.Payload) > 0 {
				if err := json.Unmarshal(job.Payload, &vars); err != nil {
					log.Printf("Warning: failed to parse job payload for job %s: %v", job.ID, err)
				}
			}

			var headers map[string]string
			if graph, gerr := engineRepo.GetProcessGraphByInstanceID(job.ProcessInstanceID); gerr == nil && graph != nil {
				if n, ok := graph.Nodes[job.CurrentElementID]; ok {
					headers = n.TaskHeaders
				}
			}

			jobs = append(jobs, v2ActivatedJob{
				JobKey:             job.ID.String(),
				Type:               topicJob.TopicName,
				ProcessInstanceKey: job.ProcessInstanceID.String(),
				Retries:            job.Retries,
				Worker:             req.Worker,
				Variables:          vars,
				CustomHeaders:      headers,
			})
		}
	}

	return c.JSON(fiber.Map{"jobs": jobs})
}
