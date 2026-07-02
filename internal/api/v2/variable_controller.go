package v2

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type VariableController struct {
	instanceRepo *repository.ProcessInstanceRepository
}

func NewVariableController(db *sqlx.DB) *VariableController {
	return &VariableController{instanceRepo: repository.NewProcessInstanceRepository(db)}
}

// SearchVariablesRequest matches the (simplified) Camunda 8 search body.
type SearchVariablesRequest struct {
	Filter struct {
		ProcessInstanceKey string `json:"processInstanceKey"`
		Name                string `json:"name"`
	} `json:"filter"`
}

// SearchVariables handles POST /v2/variables/search. GoFlow stores all of a
// process instance's variables as one JSONB blob rather than one row per
// variable, so this flattens that blob into individual name/value entries
// to match the Camunda 8 response shape.
func (vc *VariableController) SearchVariables(c *fiber.Ctx) error {
	var req SearchVariablesRequest
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

	vars, err := vc.instanceRepo.GetVariables(instanceID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}

	items := make([]fiber.Map, 0, len(vars))
	for name, value := range vars {
		if req.Filter.Name != "" && name != req.Filter.Name {
			continue
		}
		items = append(items, fiber.Map{
			"name":                name,
			"value":               value,
			"processInstanceKey": instanceID,
		})
	}

	return c.JSON(fiber.Map{"items": items})
}
