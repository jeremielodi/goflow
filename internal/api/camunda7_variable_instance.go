package api

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/lib/pq"
)

// c7VariableInstance is Camunda 7's VariableInstanceDto. GoFlow keeps a
// process instance's variables in one row, at the instance's scope, so
// executionId and activityInstanceId are the process instance's own id (as
// for a Camunda 7 process-level variable) and id is "<instance>:<name>".
type c7VariableInstance struct {
	ID                  string                 `json:"id"`
	Name                string                 `json:"name"`
	Type                string                 `json:"type"`
	Value               interface{}            `json:"value"`
	ValueInfo           map[string]interface{} `json:"valueInfo"`
	ProcessDefinitionID string                 `json:"processDefinitionId"`
	ProcessInstanceID   string                 `json:"processInstanceId"`
	ExecutionID         string                 `json:"executionId"`
	ActivityInstanceID  string                 `json:"activityInstanceId"`
	CaseInstanceID      *string                `json:"caseInstanceId"`
	CaseExecutionID     *string                `json:"caseExecutionId"`
	TaskID              *string                `json:"taskId"`
	BatchID             *string                `json:"batchId"`
	TenantID            *string                `json:"tenantId"`
	ErrorMessage        *string                `json:"errorMessage"`
}

// c7VariableQuery holds the VariableInstanceQueryDto fields GoFlow serves.
type c7VariableQuery struct {
	VariableName        string   `json:"variableName"`
	VariableNameIn      []string `json:"variableNameIn"`
	VariableNameLike    string   `json:"variableNameLike"`
	ProcessInstanceIDIn []string `json:"processInstanceIdIn"`
	ExecutionIDIn       []string `json:"executionIdIn"`
	TenantIDIn          []string `json:"tenantIdIn"`
}

func (tc *Camunda7TaskController) parseVariableQuery(c *fiber.Ctx) (c7VariableQuery, error) {
	var q c7VariableQuery
	if c.Method() == fiber.MethodPost && len(c.Body()) > 0 {
		return q, json.Unmarshal(c.Body(), &q)
	}
	list := func(name string) []string {
		if v := c.Query(name); v != "" {
			return strings.Split(v, ",")
		}
		return nil
	}
	return c7VariableQuery{
		VariableName:        c.Query("variableName"),
		VariableNameIn:      list("variableNameIn"),
		VariableNameLike:    c.Query("variableNameLike"),
		ProcessInstanceIDIn: list("processInstanceIdIn"),
		ExecutionIDIn:       list("executionIdIn"),
		TenantIDIn:          list("tenantIdIn"),
	}, nil
}

// findVariables runs the query over the running instances the caller may
// see, ordered by instance then name.
func (tc *Camunda7TaskController) findVariables(c *fiber.Ctx, q c7VariableQuery) ([]c7VariableInstance, error) {
	// An execution id is a process instance id here (see c7VariableInstance).
	instances := append(append([]string{}, q.ProcessInstanceIDIn...), q.ExecutionIDIn...)
	where := []string{"p.status IN ('running', 'suspended')"}
	var args []interface{}
	if len(instances) > 0 {
		args = append(args, pq.Array(instances))
		where = append(where, "v.process_instance_id::text = ANY($"+strconv.Itoa(len(args))+")")
	}
	if len(q.TenantIDIn) > 0 {
		args = append(args, pq.Array(q.TenantIDIn))
		where = append(where, "p.tenant_id = ANY($"+strconv.Itoa(len(args))+")")
	}
	if tenant := common.GetTenantId(c); tenant != "" {
		args = append(args, tenant)
		where = append(where, "p.tenant_id = $"+strconv.Itoa(len(args)))
	}
	var rows []struct {
		ProcessInstanceID   uuid.UUID `db:"process_instance_id"`
		ProcessDefinitionID uuid.UUID `db:"process_definition_id"`
		TenantID            *string   `db:"tenant_id"`
		Data                []byte    `db:"data"`
	}
	if err := tc.db.Select(&rows, `
		SELECT v.process_instance_id, p.process_definition_id, p.tenant_id, v.data
		FROM public.variables v
		JOIN public.process_instances p ON p.id = v.process_instance_id
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY v.process_instance_id`, args...); err != nil {
		return nil, err
	}

	names := map[string]bool{}
	for _, n := range q.VariableNameIn {
		names[n] = true
	}
	if q.VariableName != "" {
		names[q.VariableName] = true
	}
	userID, userErr := uuid.Parse(common.GetUserUuid(c))

	out := []c7VariableInstance{}
	for _, r := range rows {
		if userErr == nil {
			if allowed, _ := service.CanAccessProcessInstance(tc.db, userID, r.ProcessInstanceID, "VIEW"); !allowed {
				continue
			}
		}
		var vars map[string]interface{}
		if err := json.Unmarshal(r.Data, &vars); err != nil {
			continue
		}
		keys := make([]string, 0, len(vars))
		for k := range vars {
			if (len(names) == 0 || names[k]) && likeMatch(k, q.VariableNameLike) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		instanceID := r.ProcessInstanceID.String()
		for _, k := range keys {
			v := toC7Variable(vars[k])
			out = append(out, c7VariableInstance{
				ID:                  instanceID + ":" + k,
				Name:                k,
				Type:                v.Type,
				Value:               v.Value,
				ValueInfo:           v.ValueInfo,
				ProcessDefinitionID: r.ProcessDefinitionID.String(),
				ProcessInstanceID:   instanceID,
				ExecutionID:         instanceID,
				ActivityInstanceID:  instanceID,
				TenantID:            r.TenantID,
			})
		}
	}
	return out, nil
}

// likeMatch is SQL LIKE on a name ("%" any run, "_" one character); an
// empty pattern matches everything.
func likeMatch(s, pattern string) bool {
	return pattern == "" || like(s, pattern)
}

func like(s, pattern string) bool {
	if pattern == "" {
		return s == ""
	}
	if pattern[0] == '%' {
		for i := 0; i <= len(s); i++ {
			if like(s[i:], pattern[1:]) {
				return true
			}
		}
		return false
	}
	if s == "" {
		return false
	}
	if pattern[0] == '_' || pattern[0] == s[0] {
		return like(s[1:], pattern[1:])
	}
	return false
}

// VariableInstances handles GET and POST /engine-rest/variable-instance
// (firstResult, maxResults), what Camunda 7's RuntimeService.getVariables
// calls when it runs over REST.
func (tc *Camunda7TaskController) VariableInstances(c *fiber.Ctx) error {
	q, err := tc.parseVariableQuery(c)
	if err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
	}
	vars, err := tc.findVariables(c, q)
	if err != nil {
		return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}
	first, _ := strconv.Atoi(c.Query("firstResult"))
	max, err := strconv.Atoi(c.Query("maxResults"))
	if err != nil || max < 0 {
		max = len(vars)
	}
	if first > len(vars) {
		first = len(vars)
	}
	end := first + max
	if end > len(vars) {
		end = len(vars)
	}
	return c.JSON(vars[first:end])
}

// VariableInstanceCount handles GET and POST /engine-rest/variable-instance/count.
func (tc *Camunda7TaskController) VariableInstanceCount(c *fiber.Ctx) error {
	q, err := tc.parseVariableQuery(c)
	if err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
	}
	vars, err := tc.findVariables(c, q)
	if err != nil {
		return c7Error(c, fiber.StatusInternalServerError, "ProcessEngineException", err.Error())
	}
	return c.JSON(fiber.Map{"count": len(vars)})
}
