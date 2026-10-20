package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/pkg/common"
)

// Camunda7TaskController serves Camunda 7's task REST API
// (/engine-rest/task/...) with its paths, query parameters, response
// shapes and status codes, so a Camunda 7 application can drive GoFlow's
// user tasks without changes.
type Camunda7TaskController struct {
	db         *sqlx.DB
	dispatcher *events.TaskEventDispatcher
	repo       *repository.TaskRepository
	engineRepo *repository.EngineRepository
	audit      *service.AuditService
}

func NewCamunda7TaskController(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *Camunda7TaskController {
	return &Camunda7TaskController{
		db:         db,
		dispatcher: dispatcher,
		repo:       repository.NewTaskRepository(db, dispatcher),
		engineRepo: repository.NewEngineRepository(db, dispatcher),
		audit:      service.NewAuditService(db),
	}
}

// camunda7Time is Camunda 7's date format ("2013-01-23T13:42:42.000+0200").
const camunda7Time = "2006-01-02T15:04:05.000-0700"

func c7Time(t *time.Time) *string {
	if t == nil || t.IsZero() {
		return nil
	}
	s := t.Format(camunda7Time)
	return &s
}

// c7Error is Camunda 7's error body.
func c7Error(c *fiber.Ctx, status int, typ, message string) error {
	return c.Status(status).JSON(fiber.Map{"type": typ, "message": message})
}

func c7NotFound(c *fiber.Ctx, id string) error {
	return c7Error(c, fiber.StatusNotFound, "InvalidRequestException", fmt.Sprintf("No matching task with id %s", id))
}

// c7Task is Camunda 7's TaskDto.
type c7Task struct {
	ID                  string  `json:"id"`
	Name                *string `json:"name"`
	Assignee            *string `json:"assignee"`
	Owner               *string `json:"owner"`
	Created             *string `json:"created"`
	LastUpdated         *string `json:"lastUpdated"`
	Due                 *string `json:"due"`
	FollowUp            *string `json:"followUp"`
	DelegationState     *string `json:"delegationState"`
	Description         *string `json:"description"`
	ExecutionID         *string `json:"executionId"`
	ParentTaskID        *string `json:"parentTaskId"`
	Priority            int     `json:"priority"`
	ProcessDefinitionID string  `json:"processDefinitionId"`
	ProcessInstanceID   string  `json:"processInstanceId"`
	TaskDefinitionKey   string  `json:"taskDefinitionKey"`
	CaseExecutionID     *string `json:"caseExecutionId"`
	CaseInstanceID      *string `json:"caseInstanceId"`
	CaseDefinitionID    *string `json:"caseDefinitionId"`
	Suspended           bool    `json:"suspended"`
	FormKey             *string `json:"formKey"`
	CamundaFormRef      *string `json:"camundaFormRef"`
	TenantID            *string `json:"tenantId"`
}

type c7TaskRow struct {
	ID                  uuid.UUID  `db:"id"`
	Name                *string    `db:"task_name"`
	Assignee            *string    `db:"assignee"`
	CreatedAt           time.Time  `db:"created_at"`
	UpdatedAt           *time.Time `db:"updated_at"`
	ExecutionID         *uuid.UUID `db:"execution_id"`
	Priority            int        `db:"priority"`
	ProcessInstanceID   uuid.UUID  `db:"process_instance_id"`
	TaskDefinitionKey   string     `db:"task_definition_key"`
	ProcessDefinitionID uuid.UUID  `db:"process_definition_id"`
	TenantID            *string    `db:"tenant_id"`
	InstanceStatus      string     `db:"instance_status"`
}

func (tc *Camunda7TaskController) dto(r c7TaskRow) c7Task {
	t := c7Task{
		ID:                  r.ID.String(),
		Name:                r.Name,
		Assignee:            r.Assignee,
		Created:             c7Time(&r.CreatedAt),
		LastUpdated:         c7Time(r.UpdatedAt),
		Priority:            r.Priority,
		ProcessDefinitionID: r.ProcessDefinitionID.String(),
		ProcessInstanceID:   r.ProcessInstanceID.String(),
		TaskDefinitionKey:   r.TaskDefinitionKey,
		Suspended:           r.InstanceStatus == "suspended",
		TenantID:            r.TenantID,
	}
	if r.ExecutionID != nil {
		s := r.ExecutionID.String()
		t.ExecutionID = &s
	}
	if graph, err := tc.engineRepo.GetProcessGraphByInstanceID(r.ProcessInstanceID); err == nil && graph != nil {
		if n, ok := graph.Nodes[r.TaskDefinitionKey]; ok && n.FormKey != "" {
			k := n.FormKey
			t.FormKey = &k
		}
	}
	return t
}

// c7TaskQuery is the task query of Camunda 7, as query parameters (GET)
// or a JSON body (POST). Unsupported criteria are ignored.
type c7TaskQuery struct {
	ProcessInstanceID    string   `json:"processInstanceId"`
	ProcessInstanceIDIn  []string `json:"processInstanceIdIn"`
	ProcessDefinitionID  string   `json:"processDefinitionId"`
	ProcessDefinitionKey string   `json:"processDefinitionKey"`
	ExecutionID          string   `json:"executionId"`
	TaskID               string   `json:"taskId"`
	TaskIDIn             []string `json:"taskIdIn"`
	TaskDefinitionKey    string   `json:"taskDefinitionKey"`
	TaskDefinitionKeyIn  []string `json:"taskDefinitionKeyIn"`
	Name                 string   `json:"name"`
	NameLike             string   `json:"nameLike"`
	Assignee             string   `json:"assignee"`
	AssigneeLike         string   `json:"assigneeLike"`
	Assigned             bool     `json:"assigned"`
	Unassigned           bool     `json:"unassigned"`
	CandidateGroup       string   `json:"candidateGroup"`
	TenantIDIn           []string `json:"tenantIdIn"`
	Active               bool     `json:"active"`
	Suspended            bool     `json:"suspended"`
	SortBy               string   `json:"sortBy"`
	SortOrder            string   `json:"sortOrder"`
	Sorting              []struct {
		SortBy    string `json:"sortBy"`
		SortOrder string `json:"sortOrder"`
	} `json:"sorting"`
}

func (tc *Camunda7TaskController) parseQuery(c *fiber.Ctx) (c7TaskQuery, error) {
	var q c7TaskQuery
	if c.Method() == fiber.MethodPost && len(c.Body()) > 0 {
		if err := json.Unmarshal(c.Body(), &q); err != nil {
			return q, err
		}
		return q, nil
	}
	list := func(name string) []string {
		if v := c.Query(name); v != "" {
			return strings.Split(v, ",")
		}
		return nil
	}
	yes := func(name string) bool { return c.Query(name) == "true" }
	q = c7TaskQuery{
		ProcessInstanceID:    c.Query("processInstanceId"),
		ProcessInstanceIDIn:  list("processInstanceIdIn"),
		ProcessDefinitionID:  c.Query("processDefinitionId"),
		ProcessDefinitionKey: c.Query("processDefinitionKey"),
		ExecutionID:          c.Query("executionId"),
		TaskID:               c.Query("taskId"),
		TaskIDIn:             list("taskIdIn"),
		TaskDefinitionKey:    c.Query("taskDefinitionKey"),
		TaskDefinitionKeyIn:  list("taskDefinitionKeyIn"),
		Name:                 c.Query("name"),
		NameLike:             c.Query("nameLike"),
		Assignee:             c.Query("assignee"),
		AssigneeLike:         c.Query("assigneeLike"),
		Assigned:             yes("assigned"),
		Unassigned:           yes("unassigned"),
		CandidateGroup:       c.Query("candidateGroup"),
		TenantIDIn:           list("tenantIdIn"),
		Active:               yes("active"),
		Suspended:            yes("suspended"),
		SortBy:               c.Query("sortBy"),
		SortOrder:            c.Query("sortOrder"),
	}
	return q, nil
}

var c7SortColumns = map[string]string{
	"id": "t.id", "name": "t.task_name", "assignee": "t.assignee", "created": "t.created_at",
	"priority": "t.priority", "processInstanceId": "t.process_instance_id",
	"taskDefinitionKey": "t.task_definition_key", "lastUpdated": "t.updated_at",
}

// find runs the query over the open tasks the caller may see.
func (tc *Camunda7TaskController) find(c *fiber.Ctx, q c7TaskQuery) ([]c7TaskRow, error) {
	where := []string{"t.status NOT IN ('completed', 'cancelled')"}
	var args []interface{}
	arg := func(v interface{}) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	eq := func(col, v string) {
		if v != "" {
			where = append(where, col+" = "+arg(v))
		}
	}
	in := func(col string, vs []string) {
		if len(vs) > 0 {
			where = append(where, col+" = ANY("+arg(vs)+")")
		}
	}
	eq("t.process_instance_id::text", q.ProcessInstanceID)
	in("t.process_instance_id::text", q.ProcessInstanceIDIn)
	eq("p.process_definition_id::text", q.ProcessDefinitionID)
	eq("d.process_key", q.ProcessDefinitionKey)
	eq("t.execution_id::text", q.ExecutionID)
	eq("t.id::text", q.TaskID)
	in("t.id::text", q.TaskIDIn)
	eq("t.task_definition_key", q.TaskDefinitionKey)
	in("t.task_definition_key", q.TaskDefinitionKeyIn)
	eq("t.task_name", q.Name)
	eq("t.assignee", q.Assignee)
	eq("t.candidate_group", q.CandidateGroup)
	in("p.tenant_id", q.TenantIDIn)
	if q.NameLike != "" {
		where = append(where, "t.task_name LIKE "+arg(q.NameLike))
	}
	if q.AssigneeLike != "" {
		where = append(where, "t.assignee LIKE "+arg(q.AssigneeLike))
	}
	if q.Assigned {
		where = append(where, "t.assignee IS NOT NULL")
	}
	if q.Unassigned {
		where = append(where, "t.assignee IS NULL")
	}
	if q.Suspended {
		where = append(where, "p.status = 'suspended'")
	}
	if q.Active {
		where = append(where, "p.status <> 'suspended'")
	}
	if tenant := common.GetTenantId(c); tenant != "" {
		where = append(where, "p.tenant_id = "+arg(tenant))
	}

	order := []string{}
	sorts := q.Sorting
	if q.SortBy != "" {
		sorts = append(sorts, struct {
			SortBy    string `json:"sortBy"`
			SortOrder string `json:"sortOrder"`
		}{q.SortBy, q.SortOrder})
	}
	for _, s := range sorts {
		if col, ok := c7SortColumns[s.SortBy]; ok {
			dir := "ASC"
			if strings.EqualFold(s.SortOrder, "desc") {
				dir = "DESC"
			}
			order = append(order, col+" "+dir)
		}
	}
	order = append(order, "t.created_at ASC", "t.id ASC")

	var rows []c7TaskRow
	if err := tc.db.Select(&rows, `
		SELECT t.id, t.task_name, t.assignee, t.created_at, t.updated_at, t.execution_id, t.priority,
		       t.process_instance_id, t.task_definition_key, p.process_definition_id, p.tenant_id,
		       p.status AS instance_status
		FROM public.tasks t
		JOIN public.process_instances p ON p.id = t.process_instance_id
		JOIN public.process_definitions d ON d.id = p.process_definition_id
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY `+strings.Join(order, ", "), args...); err != nil {
		return nil, err
	}

	// Per-row VIEW access, as GET /engine-rest/tasks.
	userID, err := uuid.Parse(common.GetUserUuid(c))
	if err != nil {
		return rows, nil
	}
	visible := rows[:0]
	for _, r := range rows {
		if allowed, _ := service.CanAccessProcessInstance(tc.db, userID, r.ProcessInstanceID, "VIEW"); allowed {
			visible = append(visible, r)
		}
	}
	return visible, nil
}

// List handles GET and POST /engine-rest/task (firstResult, maxResults).
func (tc *Camunda7TaskController) List(c *fiber.Ctx) error {
	q, err := tc.parseQuery(c)
	if err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
	}
	rows, err := tc.find(c, q)
	if err != nil {
		return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}
	first, _ := strconv.Atoi(c.Query("firstResult"))
	max, err := strconv.Atoi(c.Query("maxResults"))
	if err != nil || max < 0 {
		max = len(rows)
	}
	if first > len(rows) {
		first = len(rows)
	}
	end := first + max
	if end > len(rows) {
		end = len(rows)
	}
	out := make([]c7Task, 0, end-first)
	for _, r := range rows[first:end] {
		out = append(out, tc.dto(r))
	}
	return c.JSON(out)
}

// Count handles GET and POST /engine-rest/task/count.
func (tc *Camunda7TaskController) Count(c *fiber.Ctx) error {
	q, err := tc.parseQuery(c)
	if err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
	}
	rows, err := tc.find(c, q)
	if err != nil {
		return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}
	return c.JSON(fiber.Map{"count": len(rows)})
}

// task loads an open task the caller may see, answering 404 otherwise.
func (tc *Camunda7TaskController) task(c *fiber.Ctx) (c7TaskRow, bool, error) {
	id := c.Params("id")
	rows, err := tc.find(c, c7TaskQuery{TaskID: id})
	if err != nil {
		return c7TaskRow{}, false, c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}
	if len(rows) == 0 {
		return c7TaskRow{}, false, c7NotFound(c, id)
	}
	return rows[0], true, nil
}

// Get handles GET /engine-rest/task/:id.
func (tc *Camunda7TaskController) Get(c *fiber.Ctx) error {
	t, ok, err := tc.task(c)
	if !ok {
		return err
	}
	return c.JSON(tc.dto(t))
}

// Claim handles POST /engine-rest/task/:id/claim {"userId": …}.
func (tc *Camunda7TaskController) Claim(c *fiber.Ctx) error {
	var body struct {
		UserID string `json:"userId"`
	}
	_ = json.Unmarshal(c.Body(), &body)
	if body.UserID == "" {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", "userId is required")
	}
	t, ok, err := tc.task(c)
	if !ok {
		return err
	}
	if t.Assignee != nil && *t.Assignee != "" && *t.Assignee != body.UserID {
		return c7Error(c, fiber.StatusInternalServerError, "TaskAlreadyClaimedException",
			fmt.Sprintf("Task '%s' is already claimed by someone else.", t.ID))
	}
	if err := tc.assign(t, body.UserID); err != nil {
		return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}
	return c.SendStatus(fiber.StatusNoContent)
}

// Unclaim handles POST /engine-rest/task/:id/unclaim.
func (tc *Camunda7TaskController) Unclaim(c *fiber.Ctx) error {
	t, ok, err := tc.task(c)
	if !ok {
		return err
	}
	if err := tc.assign(t, ""); err != nil {
		return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}
	return c.SendStatus(fiber.StatusNoContent)
}

// SetAssignee handles POST /engine-rest/task/:id/assignee {"userId": …}:
// assigns whoever holds it (null: unassigns).
func (tc *Camunda7TaskController) SetAssignee(c *fiber.Ctx) error {
	var body struct {
		UserID *string `json:"userId"`
	}
	_ = json.Unmarshal(c.Body(), &body)
	t, ok, err := tc.task(c)
	if !ok {
		return err
	}
	user := ""
	if body.UserID != nil {
		user = *body.UserID
	}
	if err := tc.assign(t, user); err != nil {
		return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}
	return c.SendStatus(fiber.StatusNoContent)
}

// assign sets the task's assignee ("" unassigns), with its audit and
// event log entries.
func (tc *Camunda7TaskController) assign(t c7TaskRow, user string) error {
	if user == "" {
		if t.Assignee == nil {
			return nil
		}
		if _, err := tc.repo.UnClaimTask(t.ID); err != nil {
			return err
		}
		service.LogAuditErr("task unclaimed", tc.audit.LogTaskUnclaimed(t.ID, t.ProcessInstanceID))
		if p := eventlog.Global(); p != nil {
			p.TaskUnclaimed(t.ProcessInstanceID.String(), t.ID.String())
		}
		return nil
	}
	if t.Assignee != nil && *t.Assignee == user {
		return nil
	}
	if _, err := tc.repo.ClaimTask(t.ID, user); err != nil {
		return err
	}
	service.LogAuditErr("task claimed", tc.audit.LogTaskClaimed(t.ID, t.ProcessInstanceID, user))
	if p := eventlog.Global(); p != nil {
		p.TaskClaimed(t.ProcessInstanceID.String(), t.ID.String(), user)
	}
	return nil
}

// Complete handles POST /engine-rest/task/:id/complete and
// /submit-form: {"variables": {name: {value, type}}, "withVariablesInReturn"}.
func (tc *Camunda7TaskController) Complete(c *fiber.Ctx) error {
	var body struct {
		Variables             map[string]c7Variable `json:"variables"`
		WithVariablesInReturn bool                  `json:"withVariablesInReturn"`
	}
	if len(c.Body()) > 0 {
		if err := json.Unmarshal(c.Body(), &body); err != nil {
			return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
		}
	}
	t, ok, err := tc.task(c)
	if !ok {
		return err
	}
	vars, err := fromC7Variables(body.Variables)
	if err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
	}
	var after map[string]interface{}
	if body.WithVariablesInReturn {
		after, _ = repository.VariablesForExecution(tc.db, t.ProcessInstanceID, execOf(t))
		for k, v := range vars {
			after[k] = v
		}
	}
	if err := runtime.CompleteUserTask(c.Context(), tc.db, t.ID, vars); err != nil {
		if errors.Is(err, runtime.ErrUserTaskNotFound) {
			return c7NotFound(c, t.ID.String())
		}
		return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}
	if body.WithVariablesInReturn {
		return c.JSON(toC7Variables(after))
	}
	return c.SendStatus(fiber.StatusNoContent)
}

func execOf(t c7TaskRow) uuid.UUID {
	if t.ExecutionID != nil {
		return *t.ExecutionID
	}
	return uuid.Nil
}

// Variables handles GET /engine-rest/task/:id/variables and
// /form-variables: the variables visible from the task.
func (tc *Camunda7TaskController) Variables(c *fiber.Ctx) error {
	t, ok, err := tc.task(c)
	if !ok {
		return err
	}
	vars, err := repository.VariablesForExecution(tc.db, t.ProcessInstanceID, execOf(t))
	if err != nil {
		return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}
	if names := c.Query("variableNames"); names != "" {
		keep := map[string]interface{}{}
		for _, n := range strings.Split(names, ",") {
			if v, ok := vars[n]; ok {
				keep[n] = v
			}
		}
		vars = keep
	}
	return c.JSON(toC7Variables(vars))
}

// Variable handles GET /engine-rest/task/:id/variables/:name.
func (tc *Camunda7TaskController) Variable(c *fiber.Ctx) error {
	t, ok, err := tc.task(c)
	if !ok {
		return err
	}
	vars, err := repository.VariablesForExecution(tc.db, t.ProcessInstanceID, execOf(t))
	if err != nil {
		return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}
	name := c.Params("name")
	v, found := vars[name]
	if !found {
		return c7Error(c, fiber.StatusNotFound, "InvalidRequestException",
			fmt.Sprintf("task variable with name %s does not exist", name))
	}
	return c.JSON(toC7Variable(v))
}

// ModifyVariables handles POST /engine-rest/task/:id/variables
// {"modifications": {...}}: sets them on the task's scope.
func (tc *Camunda7TaskController) ModifyVariables(c *fiber.Ctx) error {
	var body struct {
		Modifications map[string]c7Variable `json:"modifications"`
		Deletions     []string              `json:"deletions"`
	}
	if err := json.Unmarshal(c.Body(), &body); err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
	}
	if len(body.Deletions) > 0 {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", "deleting task variables is not supported")
	}
	return tc.setVariables(c, body.Modifications)
}

// PutVariable handles PUT /engine-rest/task/:id/variables/:name {value, type}.
func (tc *Camunda7TaskController) PutVariable(c *fiber.Ctx) error {
	var v c7Variable
	if err := json.Unmarshal(c.Body(), &v); err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
	}
	return tc.setVariables(c, map[string]c7Variable{c.Params("name"): v})
}

func (tc *Camunda7TaskController) setVariables(c *fiber.Ctx, in map[string]c7Variable) error {
	t, ok, err := tc.task(c)
	if !ok {
		return err
	}
	vars, err := fromC7Variables(in)
	if err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
	}
	if len(vars) > 0 {
		if err := runtime.SetVariables(c.Context(), runtime.SetVariablesArgs{
			InstanceID: t.ProcessInstanceID, ScopeID: execOf(t), Variables: vars, Source: "task",
		}); err != nil {
			return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
		}
	}
	return c.SendStatus(fiber.StatusNoContent)
}

// Form handles GET /engine-rest/task/:id/form.
func (tc *Camunda7TaskController) Form(c *fiber.Ctx) error {
	t, ok, err := tc.task(c)
	if !ok {
		return err
	}
	return c.JSON(fiber.Map{"key": tc.dto(t).FormKey, "contextPath": nil})
}

// c7Variable is a Camunda 7 typed variable.
type c7Variable struct {
	Value     interface{}            `json:"value"`
	Type      string                 `json:"type,omitempty"`
	ValueInfo map[string]interface{} `json:"valueInfo,omitempty"`
}

// fromC7Variables reads typed variables: a "Json" (or "Object" in JSON)
// value given as a string is parsed.
func fromC7Variables(in map[string]c7Variable) (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(in))
	for name, v := range in {
		value := v.Value
		if s, isString := value.(string); isString && (strings.EqualFold(v.Type, "json") || strings.EqualFold(v.Type, "object")) {
			var parsed interface{}
			if err := json.Unmarshal([]byte(s), &parsed); err != nil {
				return nil, fmt.Errorf("variable %s: invalid JSON: %w", name, err)
			}
			value = parsed
		}
		out[name] = value
	}
	return out, nil
}

func toC7Variables(vars map[string]interface{}) map[string]c7Variable {
	out := make(map[string]c7Variable, len(vars))
	for k, v := range vars {
		out[k] = toC7Variable(v)
	}
	return out
}

// toC7Variable types a value the way Camunda 7 does: whole numbers as
// Integer or Long, objects and arrays as Json (the value a JSON string).
func toC7Variable(v interface{}) c7Variable {
	switch x := v.(type) {
	case nil:
		return c7Variable{Value: nil, Type: "Null", ValueInfo: map[string]interface{}{}}
	case string:
		return c7Variable{Value: x, Type: "String", ValueInfo: map[string]interface{}{}}
	case bool:
		return c7Variable{Value: x, Type: "Boolean", ValueInfo: map[string]interface{}{}}
	case float64:
		if x == math.Trunc(x) && math.Abs(x) < 1<<53 {
			if x >= math.MinInt32 && x <= math.MaxInt32 {
				return c7Variable{Value: int64(x), Type: "Integer", ValueInfo: map[string]interface{}{}}
			}
			return c7Variable{Value: int64(x), Type: "Long", ValueInfo: map[string]interface{}{}}
		}
		return c7Variable{Value: x, Type: "Double", ValueInfo: map[string]interface{}{}}
	case map[string]interface{}, []interface{}:
		raw, _ := json.Marshal(x)
		return c7Variable{Value: string(raw), Type: "Json", ValueInfo: map[string]interface{}{}}
	default:
		return c7Variable{Value: x, Type: "Object", ValueInfo: map[string]interface{}{}}
	}
}
