package api

import (
	"encoding/json"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type IncidentController struct {
	db           *sqlx.DB
	incidentRepo *repository.IncidentRepository
	extTaskRepo  *repository.ExternalTaskRepository
	zeebeKeys    *repository.ZeebeKeyRepository
}

func NewIncidentController(db *sqlx.DB) *IncidentController {
	return &IncidentController{
		db:           db,
		incidentRepo: repository.NewIncidentRepository(db),
		extTaskRepo:  repository.NewExternalTaskRepository(db),
		zeebeKeys:    repository.NewZeebeKeyRepository(db),
	}
}

// withZeebeKey adds a zeebeKey field (assigned lazily via the same
// zeebe_keys table jobs/instances already use) to an incident's JSON
// representation, additively — no existing field changes shape. This is
// the only way a gRPC caller can discover an incident's int64 key today,
// since GoFlow has no gRPC-side incident listing to mint one on
// (real Zeebe's own Gateway protocol has none either — real clients learn
// incident keys from Operate/exporters, outside the Gateway).
func (ctrl *IncidentController) withZeebeKey(inc *models.Incident) (map[string]interface{}, error) {
	zeebeKey, err := ctrl.zeebeKeys.GetOrAssignKey("incident", inc.ID)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(inc)
	if err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	m["zeebeKey"] = zeebeKey
	return m, nil
}

// GET /engine-rest/incident
func (ctrl *IncidentController) ListIncidents(c *fiber.Ctx) error {
	f := repository.IncidentFilter{}

	if raw := c.Query("processInstanceId"); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"message": "invalid processInstanceId"})
		}
		f.ProcessInstanceID = &id
	}
	if s := c.Query("state"); s != "" {
		f.State = &s
	}
	if t := c.Query("incidentType"); t != "" {
		f.IncidentType = &t
	}
	if a := c.Query("activityId"); a != "" {
		f.ActivityID = &a
	}

	incidents, err := ctrl.incidentRepo.List(f)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to list incidents", "error": err.Error()})
	}
	result := make([]map[string]interface{}, len(incidents))
	for i := range incidents {
		m, err := ctrl.withZeebeKey(&incidents[i])
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"message": err.Error()})
		}
		result[i] = m
	}
	return c.JSON(result)
}

// GET /engine-rest/incident/:id
func (ctrl *IncidentController) GetIncident(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"message": "invalid incident id"})
	}
	inc, err := ctrl.incidentRepo.GetByID(id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": err.Error()})
	}
	if inc == nil {
		return c.Status(404).JSON(fiber.Map{"message": "incident not found"})
	}
	m, err := ctrl.withZeebeKey(inc)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": err.Error()})
	}
	return c.JSON(m)
}

// DELETE /engine-rest/incident/:id
func (ctrl *IncidentController) DeleteIncident(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"message": "invalid incident id"})
	}
	if err := ctrl.incidentRepo.Delete(id); err != nil {
		return c.Status(500).JSON(fiber.Map{"message": err.Error()})
	}
	return c.SendStatus(204)
}

// GET /engine-rest/process-instance/:id/incidents
func (ctrl *IncidentController) GetByProcessInstance(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"message": "invalid process instance id"})
	}
	f := repository.IncidentFilter{ProcessInstanceID: &id}
	incidents, err := ctrl.incidentRepo.List(f)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": err.Error()})
	}
	result := make([]map[string]interface{}, len(incidents))
	for i := range incidents {
		m, err := ctrl.withZeebeKey(&incidents[i])
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"message": err.Error()})
		}
		result[i] = m
	}
	return c.JSON(result)
}

// POST /engine-rest/job/:id/retries   {"retries": N}
// Sets retries on a failed job so it can be picked up again, and resolves any open incident.
func (ctrl *IncidentController) SetJobRetries(c *fiber.Ctx) error {
	jobID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"message": "invalid job id"})
	}

	var body struct {
		Retries int `json:"retries"`
	}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"message": "invalid request body"})
	}
	if body.Retries <= 0 {
		return c.Status(400).JSON(fiber.Map{"message": "retries must be > 0"})
	}

	tx, err := ctrl.db.Beginx()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to begin transaction"})
	}
	defer tx.Rollback()

	// Reset job to pending with new retries
	if _, err := tx.Exec(`
		UPDATE public.jobs
		SET status = 'pending', retries = $1, locked_by = NULL, locked_until = NULL, updated_at = NOW()
		WHERE id = $2
	`, body.Retries, jobID); err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to update job retries", "error": err.Error()})
	}

	// Resolve any open incident for this job
	if err := ctrl.incidentRepo.ResolveByJobID(tx, jobID); err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to resolve incident", "error": err.Error()})
	}

	if err := tx.Commit(); err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to commit"})
	}

	return c.JSON(fiber.Map{"message": "retries set, incident resolved"})
}
