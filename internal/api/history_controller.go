package api

import (
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type HistoryController struct {
	db *sqlx.DB
}

func NewHistoryController(db *sqlx.DB) *HistoryController {
	return &HistoryController{db: db}
}

// HistoricProcessInstanceResponse follows the Camunda 7 schema.
type HistoricProcessInstanceResponse struct {
	ID                  string     `json:"id"`
	ProcessDefinitionID string     `json:"processDefinitionId"`
	ProcessDefinitionKey string    `json:"processDefinitionKey"`
	BusinessKey         *string    `json:"businessKey"`
	StartTime           *time.Time `json:"startTime"`
	EndTime             *time.Time `json:"endTime"`
	DurationInMillis    *int64     `json:"durationInMillis"`
	State               string     `json:"state"`
	StartUserID         *string    `json:"startUserId"`
}

// HistoricActivityInstanceResponse follows the Camunda 7 schema.
type HistoricActivityInstanceResponse struct {
	ID                  string     `json:"id"`
	ActivityID          string     `json:"activityId"`
	ActivityType        string     `json:"activityType"`
	ProcessInstanceID   string     `json:"processInstanceId"`
	ProcessDefinitionID string     `json:"processDefinitionId"`
	StartTime           *time.Time `json:"startTime"`
	EndTime             *time.Time `json:"endTime"`
	DurationInMillis    *int64     `json:"durationInMillis"`
	Canceled            bool       `json:"canceled"`
	CompleteScope       bool       `json:"completeScope"`
}

// ListHistoricProcessInstances handles GET /engine-rest/history/process-instance
func (hc *HistoryController) ListHistoricProcessInstances(c *fiber.Ctx) error {
	processKey := c.Query("processDefinitionKey")
	state := c.Query("state")        // active, completed, suspended
	startedAfter := c.Query("startedAfter")
	startedBefore := c.Query("startedBefore")
	finishedAfter := c.Query("finishedAfter")
	firstResult := c.QueryInt("firstResult", 0)
	maxResults := c.QueryInt("maxResults", 100)
	if maxResults > 1000 {
		maxResults = 1000
	}

	query := `
		SELECT pi.id, pi.process_definition_id, pd.process_key,
		       pi.started_at, pi.ended_at, pi.status
		FROM process_instances pi
		JOIN process_definitions pd ON pd.id = pi.process_definition_id
		WHERE 1=1
	`
	args := []interface{}{}
	idx := 1

	if processKey != "" {
		query += ` AND pd.process_key = $` + itoa(idx)
		args = append(args, processKey)
		idx++
	}
	if state != "" {
		// Map Camunda state names to internal status values
		dbState := state
		switch state {
		case "active":
			dbState = "running"
		case "completed":
			dbState = "completed"
		case "suspended":
			dbState = "suspended"
		case "terminated":
			dbState = "terminated"
		}
		query += ` AND pi.status = $` + itoa(idx)
		args = append(args, dbState)
		idx++
	}
	if startedAfter != "" {
		if t, err := time.Parse(time.RFC3339, startedAfter); err == nil {
			query += ` AND pi.started_at > $` + itoa(idx)
			args = append(args, t)
			idx++
		}
	}
	if startedBefore != "" {
		if t, err := time.Parse(time.RFC3339, startedBefore); err == nil {
			query += ` AND pi.started_at < $` + itoa(idx)
			args = append(args, t)
			idx++
		}
	}
	if finishedAfter != "" {
		if t, err := time.Parse(time.RFC3339, finishedAfter); err == nil {
			query += ` AND pi.ended_at > $` + itoa(idx)
			args = append(args, t)
			idx++
		}
	}

	query += ` ORDER BY pi.started_at DESC`
	query += ` LIMIT $` + itoa(idx) + ` OFFSET $` + itoa(idx+1)
	args = append(args, maxResults, firstResult)

	type row struct {
		ID                  uuid.UUID  `db:"id"`
		ProcessDefinitionID uuid.UUID  `db:"process_definition_id"`
		ProcessKey          string     `db:"process_key"`
		StartedAt           *time.Time `db:"started_at"`
		EndedAt             *time.Time `db:"ended_at"`
		Status              string     `db:"status"`
	}
	var rows []row
	if err := hc.db.Select(&rows, query, args...); err != nil {
		return c.Status(500).JSON(fiber.Map{"message": err.Error()})
	}

	resp := make([]HistoricProcessInstanceResponse, len(rows))
	for i, r := range rows {
		var dur *int64
		if r.StartedAt != nil && r.EndedAt != nil {
			d := r.EndedAt.Sub(*r.StartedAt).Milliseconds()
			dur = &d
		}
		state := r.Status
		if state == "running" {
			state = "active"
		}
		resp[i] = HistoricProcessInstanceResponse{
			ID:                  r.ID.String(),
			ProcessDefinitionID: r.ProcessDefinitionID.String(),
			ProcessDefinitionKey: r.ProcessKey,
			StartTime:           r.StartedAt,
			EndTime:             r.EndedAt,
			DurationInMillis:    dur,
			State:               state,
		}
	}
	return c.JSON(resp)
}

// GetHistoricProcessInstance handles GET /engine-rest/history/process-instance/:id
func (hc *HistoryController) GetHistoricProcessInstance(c *fiber.Ctx) error {
	id := c.Params("id")

	type row struct {
		ID                  uuid.UUID  `db:"id"`
		ProcessDefinitionID uuid.UUID  `db:"process_definition_id"`
		ProcessKey          string     `db:"process_key"`
		StartedAt           *time.Time `db:"started_at"`
		EndedAt             *time.Time `db:"ended_at"`
		Status              string     `db:"status"`
	}
	var r row
	err := hc.db.Get(&r, `
		SELECT pi.id, pi.process_definition_id, pd.process_key,
		       pi.started_at, pi.ended_at, pi.status
		FROM process_instances pi
		JOIN process_definitions pd ON pd.id = pi.process_definition_id
		WHERE pi.id = $1
	`, id)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"message": "process instance not found"})
	}

	var dur *int64
	if r.StartedAt != nil && r.EndedAt != nil {
		d := r.EndedAt.Sub(*r.StartedAt).Milliseconds()
		dur = &d
	}
	state := r.Status
	if state == "running" {
		state = "active"
	}
	return c.JSON(HistoricProcessInstanceResponse{
		ID:                  r.ID.String(),
		ProcessDefinitionID: r.ProcessDefinitionID.String(),
		ProcessDefinitionKey: r.ProcessKey,
		StartTime:           r.StartedAt,
		EndTime:             r.EndedAt,
		DurationInMillis:    dur,
		State:               state,
	})
}

// ListHistoricActivityInstances handles GET /engine-rest/history/activity-instance
func (hc *HistoryController) ListHistoricActivityInstances(c *fiber.Ctx) error {
	processInstanceID := c.Query("processInstanceId")
	firstResult := c.QueryInt("firstResult", 0)
	maxResults := c.QueryInt("maxResults", 100)
	if maxResults > 1000 {
		maxResults = 1000
	}

	query := `
		SELECT e.id, e.current_element_id, e.process_instance_id,
		       pi.process_definition_id, e.status,
		       e.created_at, e.updated_at
		FROM executions e
		JOIN process_instances pi ON pi.id = e.process_instance_id
		WHERE 1=1
	`
	args := []interface{}{}
	idx := 1
	if processInstanceID != "" {
		query += ` AND e.process_instance_id = $` + itoa(idx)
		args = append(args, processInstanceID)
		idx++
	}
	query += ` ORDER BY e.created_at ASC`
	query += ` LIMIT $` + itoa(idx) + ` OFFSET $` + itoa(idx+1)
	args = append(args, maxResults, firstResult)

	type row struct {
		ID                  uuid.UUID  `db:"id"`
		CurrentElementID    string     `db:"current_element_id"`
		ProcessInstanceID   uuid.UUID  `db:"process_instance_id"`
		ProcessDefinitionID uuid.UUID  `db:"process_definition_id"`
		Status              string     `db:"status"`
		CreatedAt           time.Time  `db:"created_at"`
		UpdatedAt           time.Time  `db:"updated_at"`
	}
	var rows []row
	if err := hc.db.Select(&rows, query, args...); err != nil {
		return c.Status(500).JSON(fiber.Map{"message": err.Error()})
	}

	resp := make([]HistoricActivityInstanceResponse, len(rows))
	for i, r := range rows {
		completed := r.Status == "completed"
		terminal := completed || r.Status == "terminated"

		// EndTime/DurationInMillis only make sense once the execution has
		// actually reached a terminal state — a still-active/waiting token
		// (e.g. a user task not yet completed) must report a nil EndTime,
		// otherwise every consumer that filters "active" activities by
		// `!endTime` (e.g. the frontend's InstanceDetail page) sees nothing.
		var endTime *time.Time
		var dur *int64
		if terminal {
			t := r.UpdatedAt
			endTime = &t
			d := r.UpdatedAt.Sub(r.CreatedAt).Milliseconds()
			dur = &d
		}

		resp[i] = HistoricActivityInstanceResponse{
			ID:                  r.ID.String(),
			ActivityID:          r.CurrentElementID,
			ActivityType:        "unknown",
			ProcessInstanceID:   r.ProcessInstanceID.String(),
			ProcessDefinitionID: r.ProcessDefinitionID.String(),
			StartTime:           &r.CreatedAt,
			EndTime:             endTime,
			DurationInMillis:    dur,
			CompleteScope:       completed,
		}
	}
	return c.JSON(resp)
}

func itoa(n int) string { return strconv.Itoa(n) }
