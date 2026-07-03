package api

import (
	"sort"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

type HistoryController struct {
	db          *sqlx.DB
	archiveRepo *repository.HistoricArchiveRepository
}

func NewHistoryController(db *sqlx.DB) *HistoryController {
	return &HistoryController{db: db, archiveRepo: repository.NewHistoricArchiveRepository(db)}
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

// ListHistoricProcessInstances handles GET /engine-rest/history/process-instance.
// Merges still-live instances (running/suspended, and any not-yet-archived
// completed/terminated ones) with the historic archive (completed/terminated
// instances that have already been archived — see archive_service.go) in Go,
// rather than a SQL UNION: pagination/sorting across two tables is simpler
// to get right this way than a hand-rolled UNION with LIMIT/OFFSET.
func (hc *HistoryController) ListHistoricProcessInstances(c *fiber.Ctx) error {
	processKey := c.Query("processDefinitionKey")
	state := c.Query("state") // active, completed, suspended, terminated
	startedAfter := c.Query("startedAfter")
	startedBefore := c.Query("startedBefore")
	finishedAfter := c.Query("finishedAfter")
	firstResult := c.QueryInt("firstResult", 0)
	maxResults := c.QueryInt("maxResults", 100)
	if maxResults > 1000 {
		maxResults = 1000
	}

	var startedAfterT, startedBeforeT, finishedAfterT *time.Time
	if t, err := time.Parse(time.RFC3339, startedAfter); err == nil {
		startedAfterT = &t
	}
	if t, err := time.Parse(time.RFC3339, startedBefore); err == nil {
		startedBeforeT = &t
	}
	if t, err := time.Parse(time.RFC3339, finishedAfter); err == nil {
		finishedAfterT = &t
	}

	// Map Camunda state names to internal status values for the live query.
	dbState := state
	switch state {
	case "active":
		dbState = "running"
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
	if dbState != "" {
		query += ` AND pi.status = $` + itoa(idx)
		args = append(args, dbState)
		idx++
	}
	if startedAfterT != nil {
		query += ` AND pi.started_at > $` + itoa(idx)
		args = append(args, *startedAfterT)
		idx++
	}
	if startedBeforeT != nil {
		query += ` AND pi.started_at < $` + itoa(idx)
		args = append(args, *startedBeforeT)
		idx++
	}
	if finishedAfterT != nil {
		query += ` AND pi.ended_at > $` + itoa(idx)
		args = append(args, *finishedAfterT)
		idx++
	}

	type row struct {
		ID                  uuid.UUID  `db:"id"`
		ProcessDefinitionID uuid.UUID  `db:"process_definition_id"`
		ProcessKey          string     `db:"process_key"`
		StartedAt           *time.Time `db:"started_at"`
		EndedAt             *time.Time `db:"ended_at"`
		Status              string     `db:"status"`
	}
	var liveRows []row
	if err := hc.db.Select(&liveRows, query, args...); err != nil {
		return c.Status(500).JSON(fiber.Map{"message": err.Error()})
	}

	resp := make([]HistoricProcessInstanceResponse, 0, len(liveRows))
	for _, r := range liveRows {
		var dur *int64
		if r.StartedAt != nil && r.EndedAt != nil {
			d := r.EndedAt.Sub(*r.StartedAt).Milliseconds()
			dur = &d
		}
		s := r.Status
		if s == "running" {
			s = "active"
		}
		resp = append(resp, HistoricProcessInstanceResponse{
			ID:                   r.ID.String(),
			ProcessDefinitionID:  r.ProcessDefinitionID.String(),
			ProcessDefinitionKey: r.ProcessKey,
			StartTime:            r.StartedAt,
			EndTime:              r.EndedAt,
			DurationInMillis:     dur,
			State:                s,
		})
	}

	// Only query the archive when the requested state (if any) could
	// include a finished instance.
	if state == "" || state == "completed" || state == "terminated" {
		historicRows, err := hc.archiveRepo.FindAllProcessInstances(repository.HistoricProcessInstanceFilter{
			ProcessKey:    processKey,
			State:         state, // "" = no filter, archive only ever holds completed/terminated anyway
			StartedAfter:  startedAfterT,
			StartedBefore: startedBeforeT,
			FinishedAfter: finishedAfterT,
		})
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"message": err.Error()})
		}
		for _, h := range historicRows {
			ended := h.EndedAt
			d := ended.Sub(h.StartedAt).Milliseconds()
			resp = append(resp, HistoricProcessInstanceResponse{
				ID:                   h.ID.String(),
				ProcessDefinitionID:  h.ProcessDefinitionID.String(),
				ProcessDefinitionKey: h.ProcessDefinitionKey,
				StartTime:            &h.StartedAt,
				EndTime:              &ended,
				DurationInMillis:     &d,
				State:                h.State,
			})
		}
	}

	if userID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
		visible := resp[:0]
		for _, r := range resp {
			if allowed, _ := service.CanAccessProcess(hc.db, userID, r.ProcessDefinitionKey, "VIEW"); allowed {
				visible = append(visible, r)
			}
		}
		resp = visible
	}

	sort.Slice(resp, func(i, j int) bool {
		if resp[i].StartTime == nil || resp[j].StartTime == nil {
			return false
		}
		return resp[i].StartTime.After(*resp[j].StartTime)
	})

	// Paginate the merged, sorted result in Go.
	if firstResult >= len(resp) {
		return c.JSON([]HistoricProcessInstanceResponse{})
	}
	end := firstResult + maxResults
	if end > len(resp) {
		end = len(resp)
	}
	return c.JSON(resp[firstResult:end])
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
	userID, userErr := uuid.Parse(common.GetUserUuid(c))

	if err != nil {
		// Not live — fall back to the historic archive.
		instanceID, parseErr := uuid.Parse(id)
		if parseErr == nil {
			if h, histErr := hc.archiveRepo.FindProcessInstanceByID(instanceID); histErr == nil && h != nil {
				if userErr == nil {
					if allowed, permErr := service.CanAccessProcess(hc.db, userID, h.ProcessDefinitionKey, "VIEW"); permErr == nil && !allowed {
						return c.Status(404).JSON(fiber.Map{"message": "process instance not found"})
					}
				}
				ended := h.EndedAt
				d := ended.Sub(h.StartedAt).Milliseconds()
				return c.JSON(HistoricProcessInstanceResponse{
					ID:                   h.ID.String(),
					ProcessDefinitionID:  h.ProcessDefinitionID.String(),
					ProcessDefinitionKey: h.ProcessDefinitionKey,
					StartTime:            &h.StartedAt,
					EndTime:              &ended,
					DurationInMillis:     &d,
					State:                h.State,
				})
			}
		}
		return c.Status(404).JSON(fiber.Map{"message": "process instance not found"})
	}

	if userErr == nil {
		if allowed, permErr := service.CanAccessProcess(hc.db, userID, r.ProcessKey, "VIEW"); permErr == nil && !allowed {
			return c.Status(404).JSON(fiber.Map{"message": "process instance not found"})
		}
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

	// The live `executions` table only ever holds still-running instances —
	// once an instance is archived (see archive_service.go), its executions
	// are gone. Fall back to historic_activity_instances (one row per
	// element visited) when a specific instance was requested and nothing
	// came back live.
	if len(resp) == 0 && processInstanceID != "" {
		if instanceID, parseErr := uuid.Parse(processInstanceID); parseErr == nil {
			hist, histErr := hc.archiveRepo.FindProcessInstanceByID(instanceID)
			if histErr == nil && hist != nil {
				activityRows, actErr := hc.archiveRepo.FindActivityInstancesByProcessInstance(instanceID)
				if actErr == nil {
					for _, a := range activityRows {
						if a.ElementID == nil || *a.ElementID == "" {
							continue
						}
						t := a.OccurredAt
						resp = append(resp, HistoricActivityInstanceResponse{
							ID:                  instanceID.String() + "-" + t.String(),
							ActivityID:          *a.ElementID,
							ActivityType:        "unknown",
							ProcessInstanceID:   instanceID.String(),
							ProcessDefinitionID: hist.ProcessDefinitionID.String(),
							StartTime:           &t,
							EndTime:             &t,
							DurationInMillis:    nil,
							Canceled:            a.Action == "TASK_CANCELLED",
							CompleteScope:       a.Action == "EXECUTION_COMPLETED",
						})
					}
				}
			}
		}
	}

	return c.JSON(resp)
}

func itoa(n int) string { return strconv.Itoa(n) }
