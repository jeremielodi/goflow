// service/token_history_service.go
package service

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
)

// TokenHistoryStep is a single, human-readable point in a process instance's
// timeline, derived from an audit_logs row. Unlike GET /engine-rest/history/
// activity-instance (which reads the live executions table and therefore can
// only ever show a token's *current* position, since executions are mutated
// in place rather than inserted per hop), this is built from audit_logs —
// the only table that keeps one row per transition — so it can reconstruct
// the full path a token took, including repeat visits to the same node.
type TokenHistoryStep struct {
	Timestamp   time.Time  `json:"timestamp"`
	Action      string     `json:"action"`
	ElementId   string     `json:"elementId,omitempty"`
	ElementName string     `json:"elementName,omitempty"`
	ExecutionId *uuid.UUID `json:"executionId,omitempty"`
	TaskId      *uuid.UUID `json:"taskId,omitempty"`
	Detail      string     `json:"detail,omitempty"`
}

// BuildTokenHistory turns a process instance's ordered audit_logs rows into
// a readable timeline. Steps are emitted in the same order as the input logs
// (already ORDER BY created_at ASC) — no de-duplication, so a BPMN loop that
// revisits the same element produces one step per visit, not one collapsed
// entry.
func BuildTokenHistory(logs []models.AuditLog, tasks []models.Task) []TokenHistoryStep {
	taskByID := make(map[uuid.UUID]models.Task, len(tasks))
	for _, t := range tasks {
		taskByID[t.ID] = t
	}

	steps := make([]TokenHistoryStep, 0, len(logs))
	for _, log := range logs {
		var newData map[string]interface{}
		if len(log.NewData) > 0 {
			_ = json.Unmarshal(log.NewData, &newData)
		}

		step := TokenHistoryStep{
			Timestamp: log.CreatedAt,
			Action:    string(log.Action),
			TaskId:    log.TaskID,
		}

		if execID, ok := stringField(newData, "executionId"); ok {
			if parsed, err := uuid.Parse(execID); err == nil {
				step.ExecutionId = &parsed
			}
		}

		switch log.Action {
		case models.ActionExecutionCreated:
			step.ElementId, _ = stringField(newData, "currentElementId")
			step.Detail = fmt.Sprintf("Token created at %s", step.ElementId)

		case models.ActionExecutionMoved:
			fromNode, _ := stringField(newData, "fromNode")
			toNode, _ := stringField(newData, "toNode")
			step.ElementId = toNode
			step.Detail = fmt.Sprintf("%s → %s", fromNode, toNode)

		case models.ActionExecutionWaited:
			nodeID, _ := stringField(newData, "nodeId")
			waitType, _ := stringField(newData, "waitType")
			step.ElementId = nodeID
			step.Detail = fmt.Sprintf("Waiting at %s (%s)", nodeID, waitType)

		case models.ActionExecutionCompleted:
			step.ElementId, _ = stringField(newData, "nodeId")
			step.Detail = fmt.Sprintf("Token reached %s", step.ElementId)

		case models.ActionTaskCreated:
			applyTaskElement(&step, taskByID)
			taskName, _ := stringField(newData, "taskName")
			step.Detail = fmt.Sprintf("Task %q created", taskName)

		case models.ActionTaskClaimed:
			applyTaskElement(&step, taskByID)
			claimedBy, _ := stringField(newData, "claimedBy")
			step.Detail = fmt.Sprintf("Claimed by %s", claimedBy)

		case models.ActionTaskUnclaimed:
			applyTaskElement(&step, taskByID)
			step.Detail = "Task unclaimed"

		case models.ActionTaskCompleted:
			applyTaskElement(&step, taskByID)
			step.Detail = "Task completed"

		case models.ActionTaskCancelled:
			applyTaskElement(&step, taskByID)
			reason, _ := stringField(newData, "reason")
			step.Detail = fmt.Sprintf("Task cancelled (%s)", reason)

		case models.ActionGatewayForked:
			step.ElementId, _ = stringField(newData, "gatewayId")
			step.Detail = "Gateway forked"

		case models.ActionGatewayJoined:
			step.ElementId, _ = stringField(newData, "gatewayId")
			step.Detail = "Gateway joined"

		case models.ActionProcessStarted:
			step.Detail = "Process instance started"
		case models.ActionProcessCompleted:
			step.Detail = "Process instance completed"
		case models.ActionProcessTerminated:
			reason, _ := stringField(newData, "reason")
			step.Detail = fmt.Sprintf("Process instance terminated (%s)", reason)
		case models.ActionProcessSuspended:
			step.Detail = "Process instance suspended"
		case models.ActionProcessResumed:
			step.Detail = "Process instance resumed"

		case models.ActionJobCreated:
			step.Detail = "Job created"
		case models.ActionJobLocked:
			lockOwner, _ := stringField(newData, "lockOwner")
			step.Detail = fmt.Sprintf("Job locked by %s", lockOwner)
		case models.ActionJobCompleted:
			step.Detail = "Job completed"
		case models.ActionJobFailed:
			errMsg, _ := stringField(newData, "errorMessage")
			step.Detail = fmt.Sprintf("Job failed (%s)", errMsg)

		case models.ActionVariablesUpdated:
			step.Detail = "Variables updated"
		case models.ActionVariablesMerged:
			source, _ := stringField(newData, "source")
			step.Detail = fmt.Sprintf("Variables merged (%s)", source)

		case models.ActionTimerCreated:
			step.Detail = "Timer scheduled"
		case models.ActionTimerTriggered:
			step.Detail = "Timer triggered"
		case models.ActionTimerCancelled:
			step.Detail = "Timer cancelled"

		case models.ActionMessageCorrelated:
			messageName, _ := stringField(newData, "messageName")
			step.Detail = fmt.Sprintf("Message %q correlated", messageName)

		case models.ActionSignalBroadcast:
			signalName, _ := stringField(newData, "signalName")
			step.Detail = fmt.Sprintf("Signal %q broadcast", signalName)
		}

		steps = append(steps, step)
	}

	return steps
}

func applyTaskElement(step *TokenHistoryStep, taskByID map[uuid.UUID]models.Task) {
	if step.TaskId == nil {
		return
	}
	if task, ok := taskByID[*step.TaskId]; ok {
		step.ElementId = task.TaskDefinitionKey
		step.ElementName = task.TaskName
		// Task-related audit entries never carried an executionId in their
		// newData payload, but the task row itself knows which execution it
		// belongs to — needed so the frontend can group replay steps by
		// token (executionId) rather than flattening every concurrent
		// branch into one ambiguous timeline.
		step.ExecutionId = task.ExecutionID
	}
}

func stringField(data map[string]interface{}, key string) (string, bool) {
	if data == nil {
		return "", false
	}
	v, ok := data[key].(string)
	return v, ok
}
