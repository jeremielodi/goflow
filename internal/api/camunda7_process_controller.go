package api

import (
	"encoding/json"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/pkg/common"
)

// Camunda7ProcessController serves Camunda 7's process definition
// endpoints — get a definition by key, start an instance by key or by id —
// with Camunda 7's request and response shapes.
type Camunda7ProcessController struct {
	db   *sqlx.DB
	repo *repository.ProcessRepository
}

func NewCamunda7ProcessController(db *sqlx.DB) *Camunda7ProcessController {
	return &Camunda7ProcessController{db: db, repo: repository.NewProcessRepository(db)}
}

// definition resolves the definition a request names: :id, or the latest
// version of :key (for :tenant when given).
func (pc *Camunda7ProcessController) definition(c *fiber.Ctx) (models.ProcessDefinition, error) {
	if id := c.Params("id"); id != "" {
		defID, err := uuid.Parse(id)
		if err != nil {
			return models.ProcessDefinition{}, err
		}
		return pc.repo.FindProcessDefinitionByID(defID)
	}
	key := c.Params("key")
	if tenant := c.Params("tenant"); tenant != "" {
		var defID uuid.UUID
		if err := pc.db.Get(&defID, `
			SELECT id FROM public.process_definitions
			WHERE process_key = $1 AND tenant_id = $2
			ORDER BY version DESC LIMIT 1`, key, tenant); err != nil {
			return models.ProcessDefinition{}, err
		}
		return pc.repo.FindProcessDefinitionByID(defID)
	}
	return pc.repo.FindLatestProcessDefinitionByKey(key)
}

func (pc *Camunda7ProcessController) notFound(c *fiber.Ctx) error {
	what := c.Params("key")
	if what == "" {
		what = c.Params("id")
	}
	return c7Error(c, fiber.StatusNotFound, "RestException", fmt.Sprintf("No matching process definition with key or id %s", what))
}

// c7Definition is Camunda 7's ProcessDefinitionDto.
func c7Definition(d models.ProcessDefinition) fiber.Map {
	return fiber.Map{
		"id":                  d.ID,
		"key":                 d.ProcessKey,
		"category":            "http://bpmn.io/schema/bpmn",
		"description":         nil,
		"name":                d.ProcessName,
		"version":             d.Version,
		"resource":            d.ProcessKey + ".bpmn",
		"deploymentId":        d.DeploymentID,
		"diagram":             nil,
		"suspended":           !d.IsActive,
		"tenantId":            d.TenantID,
		"versionTag":          nil,
		"historyTimeToLive":   nil,
		"startableInTasklist": true,
	}
}

// GetDefinition handles GET /engine-rest/process-definition/key/:key
// (and .../tenant-id/:tenant).
func (pc *Camunda7ProcessController) GetDefinition(c *fiber.Ctx) error {
	def, err := pc.definition(c)
	if err != nil {
		return pc.notFound(c)
	}
	if !common.TenantMatches(def.TenantID, common.GetTenantId(c)) {
		return pc.notFound(c)
	}
	if userID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
		if allowed, permErr := service.CanAccessProcess(pc.db, userID, def.ProcessKey, "VIEW"); permErr == nil && !allowed {
			return pc.notFound(c)
		}
	}
	return c.JSON(c7Definition(def))
}

// Start handles POST /engine-rest/process-definition/key/:key/start,
// .../key/:key/tenant-id/:tenant/start, .../:id/start and the
// submit-form variants: {"variables": {name: {value, type}},
// "businessKey", "withVariablesInReturn"}. The answer is Camunda 7's
// ProcessInstanceWithVariablesDto, plus GoFlow's own fields
// (processInstanceId, executionId, processKey, version, definitionId).
func (pc *Camunda7ProcessController) Start(c *fiber.Ctx) error {
	var body struct {
		Variables             map[string]json.RawMessage `json:"variables"`
		BusinessKey           string                     `json:"businessKey"`
		WithVariablesInReturn bool                       `json:"withVariablesInReturn"`
	}
	if len(c.Body()) > 0 {
		if err := json.Unmarshal(c.Body(), &body); err != nil {
			return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
		}
	}
	vars, err := startVariables(body.Variables)
	if err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
	}

	def, err := pc.definition(c)
	if err != nil {
		return pc.notFound(c)
	}
	if userID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
		if allowed, permErr := service.CanAccessProcess(pc.db, userID, def.ProcessKey, "START"); permErr == nil && !allowed {
			return c7Error(c, fiber.StatusForbidden, "AuthorizationException", "you do not have START access to this process")
		}
	}
	tenantID := common.InstanceTenant(def.TenantID, common.GetTenantId(c))

	instanceID, execID, err := runtime.StartInstanceWithBusinessKey(c.Context(), def.ID, tenantID, vars, body.BusinessKey)
	if err != nil {
		return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}

	resp := fiber.Map{
		// Camunda 7
		"id":             instanceID,
		"definitionId":   def.ID,
		"businessKey":    nilIfBlank(body.BusinessKey),
		"caseInstanceId": nil,
		"ended":          false,
		"suspended":      false,
		"tenantId":       tenantID,
		"links":          []interface{}{},
		// GoFlow
		"processInstanceId": instanceID,
		"executionId":       execID,
		"processKey":        def.ProcessKey,
		"version":           def.Version,
	}
	if inst, err := repository.NewProcessInstanceRepository(pc.db).FindByID(instanceID); err != nil || inst == nil {
		resp["ended"] = true // finished (and archived) while starting
	}
	if body.WithVariablesInReturn {
		current, _ := repository.VariablesForExecution(pc.db, instanceID, uuid.Nil)
		if len(current) == 0 {
			current = vars
		}
		resp["variables"] = toC7Variables(current)
	}
	return c.JSON(resp)
}

// startVariables reads start variables given typed ({value, type}, as
// Camunda 7 sends them) or plain.
func startVariables(raw map[string]json.RawMessage) (map[string]interface{}, error) {
	typed := map[string]c7Variable{}
	plain := map[string]interface{}{}
	for name, r := range raw {
		var v c7Variable
		var probe map[string]json.RawMessage
		if json.Unmarshal(r, &probe) == nil {
			if _, hasValue := probe["value"]; hasValue {
				if err := json.Unmarshal(r, &v); err != nil {
					return nil, fmt.Errorf("variable %s: %w", name, err)
				}
				typed[name] = v
				continue
			}
		}
		var value interface{}
		if err := json.Unmarshal(r, &value); err != nil {
			return nil, fmt.Errorf("variable %s: %w", name, err)
		}
		plain[name] = value
	}
	out, err := fromC7Variables(typed)
	if err != nil {
		return nil, err
	}
	for k, v := range plain {
		out[k] = v
	}
	return out, nil
}

func nilIfBlank(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
