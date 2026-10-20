package api

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/jeremielodi/goflow/internal/cmdbus"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
)

// Commands of the Camunda 7 style instance endpoints, run like the
// engine's (see package cmdbus): directly, or on the owner of the
// instance's partition.
const (
	cmdEditVariables    = "api.EditVariables"
	cmdSuspendInstance  = "api.SuspendInstance"
	cmdEndInstance      = "api.EndInstance"
	cmdStopInstance     = "api.StopInstance"
	cmdCorrelateMessage = "api.CorrelateMessage"
)

type instanceCommands struct {
	db         *sqlx.DB
	dispatcher *events.TaskEventDispatcher
	repo       *repository.ProcessInstanceRepository
	audit      *service.AuditService
}

func registerInstanceCommands(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) {
	ic := &instanceCommands{
		db:         db,
		dispatcher: dispatcher,
		repo:       repository.NewProcessInstanceRepository(db),
		audit:      service.NewAuditService(db),
	}
	registerCmd(cmdEditVariables, ic.editVariables)
	registerCmd(cmdSuspendInstance, ic.suspend)
	registerCmd(cmdEndInstance, ic.end)
	registerCmd(cmdStopInstance, ic.stop)
	registerCmd(cmdCorrelateMessage, ic.correlate)
}

func registerCmd[A any, R any](op string, h func(ctx context.Context, args A) (R, error)) {
	cmdbus.Register(op, func(ctx context.Context, raw json.RawMessage) (any, error) {
		var args A
		if err := json.Unmarshal(raw, &args); err != nil {
			return nil, fmt.Errorf("%s: decode arguments: %w", op, err)
		}
		return h(ctx, args)
	})
}

// editVariablesArgs: Clear empties them first, then Unset are removed and
// Set merged in. AuditAll records the whole variable set before and after
// (one variable's change otherwise).
type editVariablesArgs struct {
	InstanceID uuid.UUID              `json:"instanceId"`
	Set        map[string]interface{} `json:"set,omitempty"`
	Unset      []string               `json:"unset,omitempty"`
	Clear      bool                   `json:"clear,omitempty"`
	AuditAll   bool                   `json:"auditAll,omitempty"`
}

func (ic *instanceCommands) editVariables(_ context.Context, a editVariablesArgs) (struct{}, error) {
	existing := map[string]interface{}{}
	if !a.Clear {
		vars, err := ic.repo.GetVariables(a.InstanceID)
		if err != nil {
			return struct{}{}, err
		}
		existing = vars
	}
	old := make(map[string]interface{}, len(existing))
	for k, v := range existing {
		old[k] = v
	}
	for _, k := range a.Unset {
		delete(existing, k)
	}
	for k, v := range a.Set {
		existing[k] = v
	}
	if err := ic.repo.UpsertVariables(a.InstanceID, existing); err != nil {
		return struct{}{}, err
	}
	switch {
	case len(a.Set) == 0:
	case a.AuditAll:
		service.LogAuditErr("variables updated", ic.audit.LogVariablesUpdated(a.InstanceID, old, existing))
	default:
		before, after := map[string]interface{}{}, map[string]interface{}{}
		for k, v := range a.Set {
			before[k], after[k] = old[k], v
		}
		service.LogAuditErr("variables updated", ic.audit.LogVariablesUpdated(a.InstanceID, before, after))
	}
	return struct{}{}, nil
}

type suspendArgs struct {
	InstanceID uuid.UUID `json:"instanceId"`
	Suspended  bool      `json:"suspended"`
}

func (ic *instanceCommands) suspend(_ context.Context, a suspendArgs) (struct{}, error) {
	status := "running"
	if a.Suspended {
		status = "suspended"
	}
	if err := ic.repo.UpdateStatus(a.InstanceID, status); err != nil {
		return struct{}{}, err
	}
	if a.Suspended {
		service.LogAuditErr("process suspended", ic.audit.LogProcessSuspended(a.InstanceID))
	} else {
		service.LogAuditErr("process resumed", ic.audit.LogProcessResumed(a.InstanceID))
	}
	return struct{}{}, nil
}

type endArgs struct {
	InstanceID uuid.UUID              `json:"instanceId"`
	Variables  map[string]interface{} `json:"variables"`
}

func (ic *instanceCommands) end(_ context.Context, a endArgs) (struct{}, error) {
	if err := ic.repo.UpdateStatus(a.InstanceID, "completed"); err != nil {
		return struct{}{}, err
	}
	if err := ic.mergeVariables(a.InstanceID, a.Variables); err != nil {
		return struct{}{}, err
	}
	if err := service.ArchiveFinishedInstance(ic.db, a.InstanceID, nil); err != nil {
		fmt.Printf("Warning: failed to archive ended process instance %s: %v\n", a.InstanceID, err)
	}
	return struct{}{}, nil
}

type stopArgs struct {
	InstanceID uuid.UUID              `json:"instanceId"`
	Variables  map[string]interface{} `json:"variables"`
}

func (ic *instanceCommands) stop(_ context.Context, a stopArgs) (struct{}, error) {
	if err := ic.mergeVariables(a.InstanceID, a.Variables); err != nil {
		return struct{}{}, err
	}
	return struct{}{}, ic.repo.UpdateStatus(a.InstanceID, "terminated")
}

func (ic *instanceCommands) mergeVariables(instanceID uuid.UUID, vars map[string]interface{}) error {
	existing, err := ic.repo.GetVariables(instanceID)
	if err != nil {
		return err
	}
	for k, v := range vars {
		existing[k] = v
	}
	return ic.repo.UpsertVariables(instanceID, existing)
}

type correlateArgs struct {
	MessageName     string                 `json:"messageName"`
	CorrelationKeys map[string]interface{} `json:"correlationKeys,omitempty"`
	Variables       map[string]interface{} `json:"variables,omitempty"`
}

type correlateOutcome struct {
	ProcessInstanceID uuid.UUID `json:"processInstanceId"`
	ExecutionID       uuid.UUID `json:"executionId"`
}

func (ic *instanceCommands) correlate(_ context.Context, a correlateArgs) (correlateOutcome, error) {
	mc := &MessageController{db: ic.db, dispatcher: ic.dispatcher, auditService: ic.audit}
	instanceID, execID, err := mc.correlate(a.MessageName, a.CorrelationKeys, a.Variables)
	return correlateOutcome{ProcessInstanceID: instanceID, ExecutionID: execID}, err
}
