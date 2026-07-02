package v2

import (
	"encoding/json"

	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/dmn"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type DecisionController struct {
	dmnRepo *repository.DMNRepository
}

func NewDecisionController(db *sqlx.DB) *DecisionController {
	return &DecisionController{dmnRepo: repository.NewDMNRepository(db)}
}

// EvaluateDecisionRequest matches Camunda 8's decision evaluation body.
type EvaluateDecisionRequest struct {
	DecisionId string                 `json:"decisionId"`
	Variables  map[string]interface{} `json:"variables"`
}

// EvaluateDecision handles POST /v2/decisions/:key/evaluation — evaluates a
// deployed DMN decision directly, outside of any process instance.
func (dc *DecisionController) EvaluateDecision(c *fiber.Ctx) error {
	decisionKey := c.Params("key")
	if decisionKey == "" {
		var req EvaluateDecisionRequest
		_ = c.BodyParser(&req)
		decisionKey = req.DecisionId
	}
	if decisionKey == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title":  "INVALID_ARGUMENT",
			"detail": "decision key is required",
		})
	}

	var req EvaluateDecisionRequest
	_ = c.BodyParser(&req)

	decision, err := dc.dmnRepo.FindLatestDecisionByKey(decisionKey)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"title":  "NOT_FOUND",
			"detail": "no decision found for id " + decisionKey,
		})
	}

	var parsed dmn.Decision
	if err := json.Unmarshal(decision.ParsedTable, &parsed); err != nil || parsed.DecisionTable == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"title":  "INTERNAL_ERROR",
			"detail": "decision has no evaluable decision table",
		})
	}

	result, err := dmn.EvaluateDecisionTable(parsed.DecisionTable, req.Variables)
	if err != nil {
		return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
			"title":  "EVALUATION_FAILED",
			"detail": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"decisionKey":     decision.DecisionKey,
		"decisionVersion": decision.Version,
		"output":          result,
	})
}
