// Package v2 exposes GoFlow's engine through the Camunda 8 "Orchestration
// Cluster" REST API shapes (POST /v2/process-instances, /v2/jobs/activation,
// /v2/user-tasks/..., etc.), in parallel with the existing Camunda 7 style
// /engine-rest/... surface. It reuses the same repositories/services as the
// v1 controllers in internal/api — this package only translates request and
// response shapes, it does not duplicate engine logic.
package v2

import (
	"strings"

	"errors"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

type ProcessInstanceController struct {
	db                  *sqlx.DB
	dispatcher          *events.TaskEventDispatcher
	processRepo         *repository.ProcessRepository
	processInstanceRepo *repository.ProcessInstanceRepository
	auditService        *service.AuditService
}

func NewProcessInstanceController(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *ProcessInstanceController {
	return &ProcessInstanceController{
		db:                  db,
		dispatcher:          dispatcher,
		processRepo:         repository.NewProcessRepository(db),
		processInstanceRepo: repository.NewProcessInstanceRepository(db),
		auditService:        service.NewAuditService(db),
	}
}

// processInstanceState maps GoFlow's internal status to the uppercase state
// names used by the Camunda 8 REST API.
func processInstanceState(status string) string {
	switch status {
	case "active", "running":
		return "ACTIVE"
	case "completed":
		return "COMPLETED"
	case "terminated":
		return "TERMINATED"
	case "suspended":
		return "SUSPENDED"
	default:
		return strings.ToUpper(status)
	}
}

func toV2ProcessInstance(inst *models.ProcessInstance) fiber.Map {
	return fiber.Map{
		"processInstanceKey":       inst.ID,
		"processDefinitionId":      inst.ProcessKey,
		"processDefinitionKey":     inst.ProcessDefinitionID,
		"processDefinitionVersion": inst.Version,
		"state":                    processInstanceState(inst.Status),
		"startDate":                inst.StartedAt,
		"endDate":                  inst.EndedAt,
	}
}

// CreateProcessInstanceRequest matches Camunda 8's CreateProcessInstance body.
type CreateProcessInstanceRequest struct {
	ProcessDefinitionId string                 `json:"processDefinitionId"`
	Variables           map[string]interface{} `json:"variables"`
}

// CreateProcessInstance handles POST /v2/process-instances.
func (pc *ProcessInstanceController) CreateProcessInstance(c *fiber.Ctx) error {
	var req CreateProcessInstanceRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title":  "INVALID_ARGUMENT",
			"detail": err.Error(),
		})
	}
	if req.ProcessDefinitionId == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title":  "INVALID_ARGUMENT",
			"detail": "processDefinitionId is required",
		})
	}

	def, err := pc.processRepo.FindLatestProcessDefinitionByKey(req.ProcessDefinitionId)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"title":  "NOT_FOUND",
			"detail": "no process definition found for id " + req.ProcessDefinitionId,
		})
	}

	if userID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
		if allowed, permErr := service.CanAccessProcess(pc.db, userID, req.ProcessDefinitionId, "START"); permErr == nil && !allowed {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"title": "FORBIDDEN", "detail": "you do not have START access to this process",
			})
		}
	}

	tenantID := common.InstanceTenant(def.TenantID, common.GetTenantId(c))

	instanceID, _, err := runtime.StartInstance(c.Context(), def.ID, tenantID, req.Variables)
	if errors.Is(err, runtime.ErrExecutionFailed) {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"title":  "INTERNAL_ERROR",
			"detail": "failed to execute process: " + err.Error(),
		})
	}
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"processInstanceKey":       instanceID,
		"processDefinitionId":      def.ProcessKey,
		"processDefinitionKey":     def.ID,
		"processDefinitionVersion": def.Version,
	})
}

// GetProcessInstance handles GET /v2/process-instances/:id.
func (pc *ProcessInstanceController) GetProcessInstance(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title":  "INVALID_ARGUMENT",
			"detail": "invalid process instance key",
		})
	}

	callerTenant := common.GetTenantId(c)

	userID, userErr := uuid.Parse(common.GetUserUuid(c))

	inst, err := pc.processInstanceRepo.FindByID(instanceID)
	if err != nil || inst == nil {
		// Fall back to history for completed/terminated instances.
		hist, histErr := pc.processInstanceRepo.FindHistoricByID(instanceID)
		if histErr != nil || hist == nil || !common.TenantMatches(hist.TenantID, callerTenant) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"title":  "NOT_FOUND",
				"detail": "process instance not found",
			})
		}
		if userErr == nil {
			if allowed, permErr := service.CanAccessProcess(pc.db, userID, hist.ProcessKey, "VIEW"); permErr == nil && !allowed {
				return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"title": "NOT_FOUND", "detail": "process instance not found"})
			}
		}
		return c.JSON(toV2ProcessInstance(hist))
	}

	if !common.TenantMatches(inst.TenantID, callerTenant) {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"title":  "NOT_FOUND",
			"detail": "process instance not found",
		})
	}

	if userErr == nil {
		if allowed, permErr := service.CanAccessProcess(pc.db, userID, inst.ProcessKey, "VIEW"); permErr == nil && !allowed {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"title": "NOT_FOUND", "detail": "process instance not found"})
		}
	}

	return c.JSON(toV2ProcessInstance(inst))
}

// SearchProcessInstancesRequest matches the (simplified) Camunda 8 search body.
type SearchProcessInstancesRequest struct {
	Filter struct {
		ProcessDefinitionId string `json:"processDefinitionId"`
		State               string `json:"state"`
	} `json:"filter"`
}

// SearchProcessInstances handles POST /v2/process-instances/search.
func (pc *ProcessInstanceController) SearchProcessInstances(c *fiber.Ctx) error {
	var req SearchProcessInstancesRequest
	// A missing/empty body means "no filter" — ignore parse errors on empty body.
	_ = c.BodyParser(&req)

	status := ""
	switch strings.ToUpper(req.Filter.State) {
	case "ACTIVE":
		status = "active"
	case "COMPLETED":
		status = "completed"
	case "TERMINATED":
		status = "terminated"
	case "SUSPENDED":
		status = "suspended"
	}

	instances, err := pc.processInstanceRepo.FindAll(status, req.Filter.ProcessDefinitionId, common.GetTenantId(c))
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}

	userID, userErr := uuid.Parse(common.GetUserUuid(c))
	canView := func(processKey string) bool {
		if userErr != nil {
			return true
		}
		allowed, permErr := service.CanAccessProcess(pc.db, userID, processKey, "VIEW")
		return permErr != nil || allowed
	}

	items := make([]fiber.Map, 0, len(instances))
	for i := range instances {
		if !canView(instances[i].ProcessKey) {
			continue
		}
		items = append(items, toV2ProcessInstance(&instances[i]))
	}

	// The live table only ever holds still-running instances now — once
	// completed/terminated, an instance is archived (see archive_service.go).
	// Merge in matching historic rows so a search for completed/terminated
	// instances (or an unfiltered search) still finds them.
	if status == "" || status == "completed" || status == "terminated" {
		archiveRepo := repository.NewHistoricArchiveRepository(pc.db)
		historicRows, histErr := archiveRepo.FindAllProcessInstances(repository.HistoricProcessInstanceFilter{
			ProcessKey: req.Filter.ProcessDefinitionId,
			State:      status,
		})
		if histErr == nil {
			for _, h := range historicRows {
				if !canView(h.ProcessDefinitionKey) {
					continue
				}
				ended := h.EndedAt
				hi := models.ProcessInstance{
					ID:                  h.ID,
					ProcessDefinitionID: h.ProcessDefinitionID,
					Status:              h.State,
					StartedBy:           h.StartedBy,
					StartedAt:           h.StartedAt,
					EndedAt:             &ended,
					ProcessKey:          h.ProcessDefinitionKey,
					TenantID:            h.TenantID,
				}
				items = append(items, toV2ProcessInstance(&hi))
			}
		}
	}

	return c.JSON(fiber.Map{"items": items})
}

// ModifyProcessInstanceRequest matches the (simplified) Camunda 8 modification body.
type ModifyProcessInstanceRequest struct {
	MoveInstructions []struct {
		SourceElementId string `json:"sourceElementId"`
		TargetElementId string `json:"targetElementId"`
	} `json:"moveInstructions"`
}

// ModifyProcessInstance handles POST /v2/process-instances/:id/modification —
// moves one or more running tokens from a source element to a target
// element, cancelling the source execution (and any open task/job attached
// to it) and creating a fresh one at the target, preserving process
// variables (see internal/runtime/modification.go).
func (pc *ProcessInstanceController) ModifyProcessInstance(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title": "INVALID_ARGUMENT", "detail": "invalid process instance key",
		})
	}

	var req ModifyProcessInstanceRequest
	if err := c.BodyParser(&req); err != nil || len(req.MoveInstructions) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title": "INVALID_ARGUMENT", "detail": "at least one moveInstruction is required",
		})
	}

	if _, err := pc.processInstanceRepo.FindByID(instanceID); err != nil || !pc.canManage(c, instanceID) {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"title": "NOT_FOUND", "detail": "process instance not found or not active",
		})
	}

	moves := make([]runtime.MoveInstruction, len(req.MoveInstructions))
	for i, m := range req.MoveInstructions {
		moves[i] = runtime.MoveInstruction{SourceElementID: m.SourceElementId, TargetElementID: m.TargetElementId}
	}

	newExecutionIDs, err := runtime.Modify(c.Context(), instanceID, moves)
	if err != nil {
		return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
			"title": "MODIFICATION_FAILED", "detail": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"processInstanceKey": instanceID,
		"newExecutionIds":    newExecutionIDs,
	})
}

// canManage reports whether the caller may change instanceID's tokens
// (MANAGE on its process). Like the v1 checkInstanceAccess, it fails open
// without an authenticated user: the route-level auth already covers that.
func (pc *ProcessInstanceController) canManage(c *fiber.Ctx, instanceID uuid.UUID) bool {
	userID, err := uuid.Parse(common.GetUserUuid(c))
	if err != nil {
		return true
	}
	allowed, _ := service.CanAccessProcessInstance(pc.db, userID, instanceID, "MANAGE")
	return allowed
}

// StepBackRequest names the token to send back, by executionId or by the
// element it waits at (elementId), and how many wait states back (steps,
// default 1). dryRun only answers where the token would go.
type StepBackRequest struct {
	ExecutionId string `json:"executionId"`
	ElementId   string `json:"elementId"`
	Steps       int    `json:"steps"`
	DryRun      bool   `json:"dryRun"`
}

// StepBack handles POST /v2/process-instances/:id/step-back — the
// "previous" button: sends a token back to the wait state it went through
// before (see internal/runtime/step_back.go). Variables are kept as they
// are; the element is run again, with a new task or job.
func (pc *ProcessInstanceController) StepBack(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title": "INVALID_ARGUMENT", "detail": "invalid process instance key",
		})
	}
	var req StepBackRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title": "INVALID_ARGUMENT", "detail": "invalid body",
		})
	}
	args := runtime.StepBackArgs{InstanceID: instanceID, ElementID: req.ElementId, Steps: req.Steps, DryRun: req.DryRun}
	if req.ExecutionId != "" {
		if args.ExecutionID, err = uuid.Parse(req.ExecutionId); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"title": "INVALID_ARGUMENT", "detail": "invalid executionId",
			})
		}
	}
	if userID, err := uuid.Parse(common.GetUserUuid(c)); err == nil {
		args.UserID = &userID
	}

	if _, err := pc.processInstanceRepo.FindByID(instanceID); err != nil || !pc.canManage(c, instanceID) {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"title": "NOT_FOUND", "detail": "process instance not found or not active",
		})
	}

	res, err := runtime.StepBack(c.Context(), args)
	switch {
	case errors.Is(err, runtime.ErrStepBackRefused):
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"title": "STEP_BACK_REFUSED", "detail": err.Error(),
		})
	case errors.Is(err, service.ErrProcessInstanceNotFound):
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"title": "NOT_FOUND", "detail": err.Error(),
		})
	case err != nil && !res.Applied:
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"title": "STEP_BACK_FAILED", "detail": err.Error(),
		})
	}
	return c.JSON(res)
}
