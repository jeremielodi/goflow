package api

import (
	"errors"
	"io"
	"mime/multipart"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

type ProcessDefinitionController struct {
	db           *sqlx.DB
	util         common.Util
	processRepo  *repository.ProcessRepository
	dispatcher   *events.TaskEventDispatcher
	auditService *service.AuditService
}

func NewProcessDefinitionController(db *sqlx.DB, rootDirPath *string) *ProcessDefinitionController {
	return &ProcessDefinitionController{
		db:           db,
		util:         *common.NewUtil(rootDirPath),
		processRepo:  repository.NewProcessRepository(db),
		auditService: service.NewAuditService(db),
	}
}

func (ctrl *ProcessDefinitionController) SetDispatcher(d *events.TaskEventDispatcher) {
	ctrl.dispatcher = d
}

type ProcessDefinition struct {
	ID         string
	Key        string
	Name       string
	BPMN       string
	TenantID   *string
	EngineType string
	Graph      *engine.ProcessGraph
	CreatedAt  time.Time
}

func (processDef ProcessDefinitionController) DeployBPMN(c *fiber.Ctx) error {
	// 1. Parse multipart form.
	form, err := c.MultipartForm()
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "not a valid multipart request"})
	}

	// 2. Get uploaded files (supports Camunda 7 dynamic field & Camunda 8 "resources").
	var fileHeaders []*multipart.FileHeader
	if headers, ok := form.File["resources"]; ok && len(headers) > 0 {
		fileHeaders = headers
	} else {
		for _, headers := range form.File {
			if len(headers) > 0 {
				fileHeaders = headers
				break
			}
		}
	}
	if len(fileHeaders) == 0 {
		return c.Status(400).JSON(fiber.Map{
			"error":   "no BPMN file uploaded",
			"details": "expected field 'resources' or a file field",
		})
	}

	// 3. Extract deployment metadata.
	deploymentName := c.FormValue("deployment-name")
	if deploymentName == "" {
		deploymentName = fileHeaders[0].Filename
	}
	tenantID := c.FormValue("tenant-id") // optional; defaults to the caller's own tenant
	if tenantID == "" {
		tenantID = common.GetTenantId(c)
	}

	// 4. Read each uploaded file into a RawResource.
	resources := make([]service.RawResource, 0, len(fileHeaders))
	for _, fh := range fileHeaders {
		file, err := fh.Open()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "failed to open file " + fh.Filename})
		}
		fileBytes, err := io.ReadAll(file)
		file.Close()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "failed to read file " + fh.Filename})
		}
		resources = append(resources, service.RawResource{Filename: fh.Filename, Content: fileBytes})
	}

	// 5. Deploy (shared with the gRPC DeployResource RPC).
	deployerID, _ := uuid.Parse(common.GetUserUuid(c))
	result, err := service.DeployResources(processDef.db, deploymentName, tenantID, resources, deployerID)
	if err != nil {
		if errors.Is(err, service.ErrDeployPermissionDenied) {
			return c.Status(403).JSON(fiber.Map{"error": err.Error()})
		}
		if errors.Is(err, service.ErrInvalidResource) {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// 6. Return Camunda-style response.
	type ProcessDefInfo struct {
		ID           string `json:"id"`
		Key          string `json:"key"`
		Version      int    `json:"version"`
		ResourceName string `json:"resourceName"`
	}
	type DecisionInfo struct {
		ID           string `json:"id"`
		Key          string `json:"key"`
		Version      int    `json:"version"`
		ResourceName string `json:"resourceName"`
	}
	type FormInfo struct {
		ID           string `json:"id"`
		Key          string `json:"key"`
		Version      int    `json:"version"`
		ResourceName string `json:"resourceName"`
	}
	deployedProcesses := make(map[string]ProcessDefInfo)
	for _, d := range result.ProcessDefinitions {
		deployedProcesses[d.Key] = ProcessDefInfo{ID: d.ID.String(), Key: d.Key, Version: d.Version, ResourceName: d.ResourceName}
	}
	deployedDecisions := make(map[string]DecisionInfo)
	for _, d := range result.Decisions {
		deployedDecisions[d.Key] = DecisionInfo{ID: d.ID.String(), Key: d.Key, Version: d.Version, ResourceName: d.ResourceName}
	}
	deployedForms := make(map[string]FormInfo)
	for _, d := range result.Forms {
		deployedForms[d.Key] = FormInfo{ID: d.ID.String(), Key: d.Key, Version: d.Version, ResourceName: d.ResourceName}
	}

	return c.Status(200).JSON(fiber.Map{
		"id":                         result.DeploymentID,
		"name":                       result.DeploymentName,
		"deployedAt":                 time.Now().Format(time.RFC3339),
		"deployedProcessDefinitions": deployedProcesses,
		"deployedDecisions":          deployedDecisions,
		"deployedForms":              deployedForms,
	})
}

// GET /engine-rest/process-definition
// Query params: key, latestVersion (bool)
func (ctrl *ProcessDefinitionController) ListDefinitions(c *fiber.Ctx) error {
	key := c.Query("key")
	latestOnly := c.Query("latestVersion") == "true"
	tenantID := common.GetTenantId(c)

	var defs []models.ProcessDefinition
	var err error

	if key != "" && latestOnly {
		def, e := ctrl.processRepo.FindLatestProcessDefinitionByKey(key)
		if e != nil || (tenantID != "" && def.TenantID != nil && *def.TenantID != tenantID) {
			return c.Status(404).JSON(fiber.Map{"message": "not found"})
		}
		defs = []models.ProcessDefinition{def}
	} else if key != "" {
		defs, err = ctrl.processRepo.ListProcessDefinitionsByKey(key, tenantID)
	} else if latestOnly {
		defs, err = ctrl.processRepo.ListLatestProcessDefinitions(tenantID)
	} else {
		defs, err = ctrl.processRepo.ListProcessDefinitions(tenantID)
	}

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": err.Error()})
	}

	if userID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
		visible := make([]models.ProcessDefinition, 0, len(defs))
		for _, def := range defs {
			if allowed, _ := service.CanAccessProcess(ctrl.db, userID, def.ProcessKey, "VIEW"); allowed {
				visible = append(visible, def)
			}
		}
		defs = visible
	}

	// A nil slice serializes as JSON null, which crashes the frontend list pages.
	if defs == nil {
		defs = []models.ProcessDefinition{}
	}
	return c.JSON(defs)
}

// GET /engine-rest/process-definition/:id
func (ctrl *ProcessDefinitionController) GetDefinition(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"message": "invalid id"})
	}
	def, err := ctrl.processRepo.FindProcessDefinitionByID(id)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"message": "not found"})
	}

	if userID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
		if allowed, permErr := service.CanAccessProcess(ctrl.db, userID, def.ProcessKey, "VIEW"); permErr == nil && !allowed {
			return c.Status(404).JSON(fiber.Map{"message": "not found"})
		}
	}

	return c.JSON(def)
}

// TokenCount is one BPMN node's live count of active tokens, aggregated
// across every currently-running instance of a process key — the "where
// are running instances currently sitting" view (Camunda Operate/Cockpit
// calls this a token/activity count overlay). It's a snapshot, not a
// historical heatmap: only live instances (running or suspended) and their
// live executions (active, or waiting on a timer/message/external event —
// which is exactly "stopped at a node" from a user's point of view) are
// counted; completed/terminated instances and executions are excluded.
type TokenCount struct {
	ElementID string `db:"element_id" json:"elementId"`
	Count     int    `db:"count" json:"count"`
}

// GET /engine-rest/process-definition/key/:key/token-counts
func (ctrl *ProcessDefinitionController) GetActiveTokenCounts(c *fiber.Ctx) error {
	key := c.Params("key")

	if userID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
		if allowed, permErr := service.CanAccessProcess(ctrl.db, userID, key, "VIEW"); permErr == nil && !allowed {
			return c.Status(404).JSON(fiber.Map{"message": "not found"})
		}
	}

	var counts []TokenCount
	// Cross-instance aggregate by process_key, not a single process_instance_id
	// — a deliberate scatter-gather on Citus, same accepted tradeoff as the
	// analytics endpoints (see architecture/citus-distributed-storage-design.md
	// section 6): infrequent dashboard query, not a hot routing path.
	// process_key lives on process_definitions, not process_instances —
	// process_definitions is a Citus reference table (replicated to every
	// node), so this join stays cheap even under Citus.
	err := ctrl.db.Select(&counts, `
		SELECT e.current_element_id AS element_id, COUNT(*) AS count
		FROM executions e
		JOIN process_instances pi ON pi.id = e.process_instance_id
		JOIN process_definitions pd ON pd.id = pi.process_definition_id
		WHERE pd.process_key = $1
		  AND pi.status IN ('running', 'suspended')
		  AND e.status IN ('active', 'waiting')
		GROUP BY e.current_element_id
	`, key)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to load token counts", "error": err.Error()})
	}
	if counts == nil {
		counts = []TokenCount{}
	}

	return c.JSON(counts)
}

// InstancePosition is one (instance, node) pair — a live token's current
// position. An instance with more than one active token (parallel gateway,
// multi-instance) appears once per token, not once per instance.
type InstancePosition struct {
	ProcessInstanceID string `db:"process_instance_id" json:"processInstanceId"`
	ElementID         string `db:"element_id" json:"elementId"`
}

// GET /engine-rest/process-definition/key/:key/instance-positions
// Per-instance breakdown of the same live snapshot GetActiveTokenCounts
// aggregates — lets the frontend show "where is this specific instance
// currently sitting" in a list, without opening the full replay view.
func (ctrl *ProcessDefinitionController) GetInstancePositions(c *fiber.Ctx) error {
	key := c.Params("key")

	if userID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
		if allowed, permErr := service.CanAccessProcess(ctrl.db, userID, key, "VIEW"); permErr == nil && !allowed {
			return c.Status(404).JSON(fiber.Map{"message": "not found"})
		}
	}

	var positions []InstancePosition
	err := ctrl.db.Select(&positions, `
		SELECT e.process_instance_id AS process_instance_id, e.current_element_id AS element_id
		FROM executions e
		JOIN process_instances pi ON pi.id = e.process_instance_id
		JOIN process_definitions pd ON pd.id = pi.process_definition_id
		WHERE pd.process_key = $1
		  AND pi.status IN ('running', 'suspended')
		  AND e.status IN ('active', 'waiting')
	`, key)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to load instance positions", "error": err.Error()})
	}
	if positions == nil {
		positions = []InstancePosition{}
	}

	return c.JSON(positions)
}

