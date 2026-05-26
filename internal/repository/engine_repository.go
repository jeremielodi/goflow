package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/pkg/database"
	"github.com/jmoiron/sqlx"
)

// Custom errors
var (
	ErrInvalidOutgoingFlows = errors.New("node has invalid number of outgoing flows")
)

type EngineRepository struct {
	db *sqlx.DB
}

func NewEngineRepository(db *sqlx.DB) *EngineRepository {
	return &EngineRepository{db: db}
}

// GetActiveExecution retrieves an active execution by ID
func (r *EngineRepository) GetActiveExecution(ctx context.Context, execID uuid.UUID) (*Execution, error) {
	var exec Execution
	query := `
		SELECT id, process_instance_id, current_element_id, status, is_active, created_at, updated_at
		FROM public.executions
		WHERE id = $1 AND is_active = true
	`
	err := r.db.GetContext(ctx, &exec, query, execID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &exec, nil
}

// GetProcessVariables retrieves variables for a process instance
func (r *EngineRepository) GetProcessVariables(instanceID uuid.UUID) (map[string]interface{}, error) {
	var data []byte
	err := r.db.Get(&data, `
		SELECT data FROM public.variables
		WHERE process_instance_id = $1
	`, instanceID)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	variables := make(map[string]interface{})
	if len(data) > 0 {
		if err := json.Unmarshal(data, &variables); err != nil {
			return nil, err
		}
	}
	return variables, nil
}

// UpdateExecutionNode updates the current node of an execution
func (r *EngineRepository) UpdateExecutionNodeTx(tx *sqlx.Tx, execID uuid.UUID, nodeID string) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Update("public.executions",
		[]database.QueryParameter{
			{Key: "current_element_id", Value: nodeID},
			{Key: "updated_at", Value: time.Now()},
		},
		database.QueryParameter{Key: "id", Value: execID},
	)
	return err
}

// UpdateExecutionStatus updates the status of an execution
func (r *EngineRepository) UpdateExecutionStatusTx(tx *sqlx.Tx, execID uuid.UUID, status string) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Update("public.executions",
		[]database.QueryParameter{
			{Key: "status", Value: status},
			{Key: "updated_at", Value: time.Now()},
		},
		database.QueryParameter{Key: "id", Value: execID},
	)
	return err
}

// UpdateExecutionStatusAndNode updates both status and current node
func (r *EngineRepository) UpdateExecutionStatusAndNodeTx(tx *sqlx.Tx, execID uuid.UUID, status, nodeID string) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Update("public.executions",
		[]database.QueryParameter{
			{Key: "status", Value: status},
			{Key: "current_element_id", Value: nodeID},
			{Key: "updated_at", Value: time.Now()},
		},
		database.QueryParameter{Key: "id", Value: execID},
	)
	return err
}

// CompleteExecution marks an execution as completed
func (r *EngineRepository) CompleteExecutionTx(tx *sqlx.Tx, execID uuid.UUID) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Update("public.executions",
		[]database.QueryParameter{
			{Key: "status", Value: "completed"},
			{Key: "is_active", Value: false},
			{Key: "updated_at", Value: time.Now()},
		},
		database.QueryParameter{Key: "id", Value: execID},
	)
	return err
}

// CreateJob creates a new service task job
func (r *EngineRepository) CreateJobTx(tx *sqlx.Tx, jobID, processInstanceID, executionID uuid.UUID, jobType string, payload []byte) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Insert("public.jobs", []database.QueryParameter{
		{Key: "id", Value: jobID},
		{Key: "process_instance_id", Value: processInstanceID},
		{Key: "execution_id", Value: executionID},
		{Key: "job_type", Value: jobType},
		{Key: "status", Value: "pending"},
		{Key: "payload", Value: payload},
		{Key: "retries", Value: 3},
		{Key: "created_at", Value: time.Now()},
		{Key: "updated_at", Value: time.Now()},
	})
	return err
}

// CreateUserTask creates a new user task
func (r *EngineRepository) CreateUserTaskTx(tx *sqlx.Tx, task *UserTask) error {
	adapter := database.NewDabaseAdapter(r.db)

	params := []database.QueryParameter{
		{Key: "id", Value: task.ID},
		{Key: "process_instance_id", Value: task.ProcessInstanceID},
		{Key: "execution_id", Value: task.ExecutionID},
		{Key: "task_definition_key", Value: task.TaskDefinitionKey},
		{Key: "status", Value: "created"},
		{Key: "created_at", Value: time.Now()},
		{Key: "updated_at", Value: time.Now()},
	}

	if task.TaskName != nil {
		params = append(params, database.QueryParameter{Key: "task_name", Value: *task.TaskName})
	}
	if task.Assignee != nil {
		params = append(params, database.QueryParameter{Key: "assignee", Value: *task.Assignee})
	}
	if task.CandidateGroup != nil {
		params = append(params, database.QueryParameter{Key: "candidate_group", Value: *task.CandidateGroup})
	}

	_, err := adapter.Insert("public.tasks", params)
	return err
}

// UpdateProcessInstanceStatus updates the status of a process instance
func (r *EngineRepository) UpdateProcessInstanceStatusTx(tx *sqlx.Tx, instanceID uuid.UUID, status string, endedAt *time.Time) error {
	adapter := database.NewDabaseAdapter(r.db)

	params := []database.QueryParameter{
		{Key: "status", Value: status},
	}

	if endedAt != nil {
		params = append(params, database.QueryParameter{Key: "ended_at", Value: *endedAt})
	}

	_, err := adapter.Update("public.process_instances",
		params,
		database.QueryParameter{Key: "id", Value: instanceID},
	)
	return err
}

// CompleteProcessInstance marks a process instance as completed
func (r *EngineRepository) CompleteProcessInstanceTx(tx *sqlx.Tx, instanceID uuid.UUID) error {
	return r.UpdateProcessInstanceStatusTx(tx, instanceID, "completed", &[]time.Time{time.Now()}[0])
}

// UpdateProcessVariables updates variables for a process instance
func (r *EngineRepository) UpdateProcessVariablesTx(tx *sqlx.Tx, instanceID uuid.UUID, variables map[string]interface{}) error {
	jsonData, err := json.Marshal(variables)
	if err != nil {
		return err
	}

	adapter := database.NewDabaseAdapter(r.db)

	// Check if variables exist
	var exists bool
	err = tx.QueryRow(`
		SELECT EXISTS(SELECT 1 FROM public.variables WHERE process_instance_id = $1)
	`, instanceID).Scan(&exists)
	if err != nil {
		return err
	}

	if exists {
		_, err = adapter.Update("public.variables",
			[]database.QueryParameter{
				{Key: "data", Value: jsonData},
				{Key: "updated_at", Value: time.Now()},
			},
			database.QueryParameter{Key: "process_instance_id", Value: instanceID},
		)
	} else {
		_, err = adapter.Insert("public.variables", []database.QueryParameter{
			{Key: "id", Value: uuid.New()},
			{Key: "process_instance_id", Value: instanceID},
			{Key: "data", Value: jsonData},
			{Key: "updated_at", Value: time.Now()},
		})
	}
	return err
}

// MergeProcessVariables merges new variables with existing ones
func (r *EngineRepository) MergeProcessVariablesTx(tx *sqlx.Tx, instanceID uuid.UUID, newVars map[string]interface{}) error {
	// Get existing variables
	existingVars, err := r.GetProcessVariables(instanceID)
	if err != nil {
		return err
	}

	// Merge
	for k, v := range newVars {
		existingVars[k] = v
	}

	// Save merged variables
	return r.UpdateProcessVariablesTx(tx, instanceID, existingVars)
}

// GetProcessDefinitionByInstanceID retrieves the process definition for a process instance
func (r *EngineRepository) GetProcessDefinitionByInstanceID(instanceID uuid.UUID) (*ProcessDefinition, error) {
	var def ProcessDefinition
	query := `
		SELECT pd.id, pd.process_key, pd.version, pd.parsed_graph, pd.created_at
		FROM public.process_definitions pd
		JOIN public.process_instances pi ON pi.process_definition_id = pd.id
		WHERE pi.id = $1
	`
	err := r.db.Get(&def, query, instanceID)
	if err != nil {
		return nil, err
	}
	return &def, nil
}

// GetProcessGraphByInstanceID retrieves just the process graph for a process instance
func (r *EngineRepository) GetProcessGraphByInstanceID(instanceID uuid.UUID) (*engine.ProcessGraph, error) {
	var graphJSON []byte
	query := `
		SELECT pd.parsed_graph
		FROM public.process_definitions pd
		JOIN public.process_instances pi ON pi.process_definition_id = pd.id
		WHERE pi.id = $1
	`
	err := r.db.Get(&graphJSON, query, instanceID)
	if err != nil {
		return nil, err
	}

	var graph engine.ProcessGraph
	if err := json.Unmarshal(graphJSON, &graph); err != nil {
		return nil, err
	}
	return &graph, nil
}

// GetNodeByID retrieves a node from the graph by ID
func (r *EngineRepository) GetNodeByID(graph *engine.ProcessGraph, nodeID string) (*engine.Node, error) {
	node, ok := graph.Nodes[nodeID]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return node, nil
}

// GetProcessInstanceIDByExecutionID retrieves the process instance ID for an execution
func (r *EngineRepository) GetProcessInstanceIDByExecutionID(execID uuid.UUID) (uuid.UUID, error) {
	var instanceID uuid.UUID
	err := r.db.Get(&instanceID, `
		SELECT process_instance_id FROM public.executions WHERE id = $1
	`, execID)
	return instanceID, err
}

// CreateExecution creates a new execution
func (r *EngineRepository) CreateExecutionTx(tx *sqlx.Tx, exec *Execution) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Insert("public.executions", []database.QueryParameter{
		{Key: "id", Value: exec.ID},
		{Key: "process_instance_id", Value: exec.ProcessInstanceID},
		{Key: "current_element_id", Value: exec.CurrentElementID},
		{Key: "status", Value: exec.Status},
		{Key: "is_active", Value: exec.IsActive},
		{Key: "created_at", Value: time.Now()},
		{Key: "updated_at", Value: time.Now()},
	})
	return err
}

// GetOutgoingFlows returns the outgoing flows from a node
func (r *EngineRepository) GetOutgoingFlows(node *engine.Node) []engine.Flow {
	return node.Outgoing
}

// HasMultipleOutgoingFlows checks if a node has multiple outgoing flows
func (r *EngineRepository) HasMultipleOutgoingFlows(node *engine.Node) bool {
	return len(node.Outgoing) > 1
}

// HasSingleOutgoingFlow checks if a node has a single outgoing flow
func (r *EngineRepository) HasSingleOutgoingFlow(node *engine.Node) bool {
	return len(node.Outgoing) == 1
}

// GetSingleOutgoingFlow returns the single outgoing flow from a node
func (r *EngineRepository) GetSingleOutgoingFlow(node *engine.Node) (*engine.Flow, error) {
	if len(node.Outgoing) != 1 {
		return nil, ErrInvalidOutgoingFlows
	}
	return &node.Outgoing[0], nil
}

// UserTask represents a user task for creation
type UserTask struct {
	ID                uuid.UUID
	ProcessInstanceID uuid.UUID
	ExecutionID       uuid.UUID
	TaskDefinitionKey string
	TaskName          *string
	Assignee          *string
	CandidateGroup    *string
}

// ProcessDefinition represents a process definition
type ProcessDefinition struct {
	ID          uuid.UUID       `db:"id"`
	ProcessKey  string          `db:"process_key"`
	Version     int             `db:"version"`
	ParsedGraph json.RawMessage `db:"parsed_graph"`
	CreatedAt   time.Time       `db:"created_at"`
}
