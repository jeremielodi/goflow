package v2

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type FlownodeInstanceController struct {
	instanceRepo *repository.ProcessInstanceRepository
}

func NewFlownodeInstanceController(db *sqlx.DB) *FlownodeInstanceController {
	return &FlownodeInstanceController{instanceRepo: repository.NewProcessInstanceRepository(db)}
}

// SearchFlownodeInstancesRequest matches the (simplified) Camunda 8 search body.
type SearchFlownodeInstancesRequest struct {
	Filter struct {
		ProcessInstanceKey string `json:"processInstanceKey"`
	} `json:"filter"`
}

// flownodeInstanceState maps an execution's (status, is_active) pair to the
// Camunda 8 flow-node-instance state naming. Completed executions are never
// deleted (only marked is_active=false), so this doubles as flow-node
// instance history, not just the currently active tokens.
func flownodeInstanceState(status string, isActive bool) string {
	if isActive {
		return "ACTIVE"
	}
	switch status {
	case "completed":
		return "COMPLETED"
	case "terminated":
		return "TERMINATED"
	default:
		return "COMPLETED"
	}
}

// SearchFlownodeInstances handles POST /v2/flownode-instances/search.
func (fc *FlownodeInstanceController) SearchFlownodeInstances(c *fiber.Ctx) error {
	var req SearchFlownodeInstancesRequest
	_ = c.BodyParser(&req)

	if req.Filter.ProcessInstanceKey == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title": "INVALID_ARGUMENT", "detail": "filter.processInstanceKey is required",
		})
	}
	instanceID, err := uuid.Parse(req.Filter.ProcessInstanceKey)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title": "INVALID_ARGUMENT", "detail": "invalid processInstanceKey",
		})
	}

	executions, err := fc.instanceRepo.FindExecutionsByInstance(instanceID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}

	items := make([]fiber.Map, 0, len(executions))
	for _, e := range executions {
		var endDate interface{}
		if !e.IsActive {
			endDate = e.UpdatedAt
		}
		items = append(items, fiber.Map{
			"flowNodeInstanceKey": e.ID,
			"processInstanceKey":  e.ProcessInstanceID,
			"elementId":           e.CurrentElementID,
			"state":               flownodeInstanceState(e.Status, e.IsActive),
			"startDate":           e.CreatedAt,
			"endDate":             endDate,
		})
	}

	return c.JSON(fiber.Map{"items": items})
}
