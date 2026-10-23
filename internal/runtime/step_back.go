package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/cmdbus"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
)

// ErrStepBackRefused is returned when a token can't be stepped back; the
// wrapped message says why, for the user.
var ErrStepBackRefused = errors.New("step back refused")

const CmdStepBack = "StepBack"

const maxStepBackSteps = 50

// StepBackArgs names the token to send back: by execution, or by the
// element it waits at when only one token waits there.
type StepBackArgs struct {
	InstanceID  uuid.UUID  `json:"instanceId"`
	ExecutionID uuid.UUID  `json:"executionId"`
	ElementID   string     `json:"elementId"`
	Steps       int        `json:"steps"`
	DryRun      bool       `json:"dryRun"`
	UserID      *uuid.UUID `json:"userId,omitempty"`
	// RequestID identifies the step back in its audit entry, so a command
	// delivered again after a crash doesn't step the token back twice.
	RequestID uuid.UUID `json:"requestId"`
}

type StepBackResult struct {
	ExecutionID    uuid.UUID   `json:"executionId"`
	FromElementID  string      `json:"fromElementId"`
	ToElementID    string      `json:"toElementId"`
	ToElementName  string      `json:"toElementName,omitempty"`
	Applied        bool        `json:"applied"`
	CancelledTasks []uuid.UUID `json:"cancelledTaskIds,omitempty"`
}

// StepBack sends a token back to the wait state it went through `Steps`
// steps ago (the "previous" button). With DryRun it only says where the
// token would go.
func StepBack(ctx context.Context, args StepBackArgs) (StepBackResult, error) {
	var out StepBackResult
	if args.RequestID == uuid.Nil {
		args.RequestID = uuid.New()
	}
	err := cmdbus.Do(ctx, cmdbus.Key(args.InstanceID), CmdStepBack, args, &out)
	return out, err
}

func (c *commands) stepBack(ctx context.Context, a StepBackArgs) (StepBackResult, error) {
	if cmdbus.Redelivered(ctx) && !a.DryRun {
		var applied bool
		if err := c.db.Get(&applied, `
			SELECT EXISTS (SELECT 1 FROM public.audit_logs
			WHERE process_instance_id = $1 AND action = 'EXECUTION_MOVED' AND new_data->>'requestId' = $2)
		`, a.InstanceID, a.RequestID.String()); err != nil {
			return StepBackResult{}, err
		}
		if applied {
			return StepBackResult{ExecutionID: a.ExecutionID, Applied: true}, c.resumeStranded(ctx, a.InstanceID)
		}
	}
	graph, err := repository.NewEngineRepository(c.db, c.dispatcher).GetProcessGraphByInstanceID(a.InstanceID)
	if err != nil {
		return StepBackResult{}, fmt.Errorf("%w: %s", service.ErrProcessInstanceNotFound, a.InstanceID)
	}
	return NewRuntime(graph, c.db, c.dispatcher).StepBack(ctx, a)
}

func refuse(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrStepBackRefused, fmt.Sprintf(format, args...))
}

// StepBack moves the token in place: the same execution keeps its parent,
// branch and variables, its current work is cancelled (see
// CancelExecutionWorkTx) and it runs again from the earlier element, which
// creates a fresh task or job there. Variables are not rolled back.
//
// Refused when the step would cross a parallel or inclusive gateway, leave
// the token's sub-process, or touch a multi-instance activity: those need
// several tokens moved together, which a step back doesn't do.
func (r *Runtime) StepBack(ctx context.Context, a StepBackArgs) (StepBackResult, error) {
	steps := a.Steps
	if steps <= 0 {
		steps = 1
	}
	if steps > maxStepBackSteps {
		return StepBackResult{}, refuse("at most %d steps at once", maxStepBackSteps)
	}

	inst, err := repository.NewProcessInstanceRepository(r.DB).FindByID(a.InstanceID)
	if err != nil || inst == nil {
		return StepBackResult{}, fmt.Errorf("%w: %s", service.ErrProcessInstanceNotFound, a.InstanceID)
	}
	if inst.Status != "running" {
		return StepBackResult{}, refuse("the process instance is %s, not running", inst.Status)
	}

	exec, err := r.stepBackSource(a)
	if err != nil {
		return StepBackResult{}, err
	}
	node := r.Graph.Nodes[exec.CurrentElementID]
	if node == nil {
		return StepBackResult{}, refuse("element %q is not in the process", exec.CurrentElementID)
	}
	switch {
	case node.MultiInstance != nil:
		return StepBackResult{}, refuse("%q is a multi-instance activity", node.ID)
	case node.Type == engine.SubProcessType || node.Type == engine.CallActivityType || node.Type == engine.EventSubProcessType:
		return StepBackResult{}, refuse("step back the token inside %q instead", node.ID)
	}
	if inner, err := lookupInnerInstance(r.DB, a.InstanceID, exec.ID); err != nil {
		return StepBackResult{}, err
	} else if inner != nil {
		return StepBackResult{}, refuse("the token is an instance of multi-instance activity %q", inner.ActivityID)
	}
	incidents := repository.NewIncidentRepository(r.DB)
	hasIncident := incidents.HasOpenIncidentOnExecution(a.InstanceID, exec.ID)
	if !service.IsStepBackTarget(node) && !hasIncident {
		return StepBackResult{}, refuse("the token is not waiting at %q", node.ID)
	}

	logs, err := repository.NewAuditRepository(r.DB).GetProcessAuditLogs(a.InstanceID)
	if err != nil {
		return StepBackResult{}, fmt.Errorf("failed to load the token history: %w", err)
	}
	path := service.StepBackPath(logs, r.Graph, exec.ID, node.ParentScope)
	// The path ends at the current element when it is a wait state; an
	// element stopped by an incident (a failing script, say) isn't in it.
	// An empty path is a token that never moved (a parallel branch still at
	// its first element).
	if n := len(path); n > 0 && path[n-1] == node.ID {
		path = path[:n-1]
	} else if n > 0 && service.IsStepBackTarget(node) {
		return StepBackResult{}, refuse("the history of the token at %q is incomplete", node.ID)
	}
	if len(path) < steps {
		if len(path) == 0 {
			return StepBackResult{}, refuse("no earlier step to go back to from %q (start of the process, sub-process or parallel branch)", node.ID)
		}
		return StepBackResult{}, refuse("only %d earlier step(s) to go back to from %q", len(path), node.ID)
	}
	target := r.Graph.Nodes[path[len(path)-steps]]

	res := StepBackResult{
		ExecutionID:   exec.ID,
		FromElementID: node.ID,
		ToElementID:   target.ID,
		ToElementName: target.Name,
	}
	if a.DryRun {
		return res, nil
	}

	engineRepo := repository.NewEngineRepository(r.DB, r.dispatcher)
	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return res, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	locked, err := engineRepo.LockActivityExecutionTx(tx, a.InstanceID, exec.ID)
	if err != nil {
		return res, fmt.Errorf("failed to lock the token: %w", err)
	}
	if locked.Status != "waiting" || locked.CurrentElementID != node.ID {
		return res, refuse("the token left %q meanwhile", node.ID)
	}
	if res.CancelledTasks, _, err = engineRepo.CancelExecutionWorkTx(tx, a.InstanceID, exec.ID); err != nil {
		return res, fmt.Errorf("failed to cancel the work at %q: %w", node.ID, err)
	}
	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, a.InstanceID, exec.ID, "active", target.ID); err != nil {
		return res, fmt.Errorf("failed to move the token: %w", err)
	}
	if err := r.auditService.LogExecutionSteppedBackTx(tx, exec.ID, a.InstanceID, node.ID, target.ID, a.UserID, a.RequestID); err != nil {
		return res, fmt.Errorf("failed to record the step back: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return res, fmt.Errorf("failed to commit the step back: %w", err)
	}
	res.Applied = true

	for _, taskID := range res.CancelledTasks {
		service.LogAuditErr("task cancelled", r.auditService.LogTaskCancelled(taskID, a.InstanceID, "step back"))
	}
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(a.InstanceID.String(), exec.ID.String(), node.ID, target.ID)
	}

	if err := r.ExecuteExecution(ctx, exec.ID); err != nil {
		return res, fmt.Errorf("failed to resume the token at %q: %w", target.ID, err)
	}
	return res, nil
}

// stepBackSource finds the token to step back among the instance's waiting
// tokens, armed boundary triggers and compensation handlers aside (their
// path IDs are "boundary:…" and "compensation:…").
func (r *Runtime) stepBackSource(a StepBackArgs) (*repository.Execution, error) {
	executions, err := repository.NewProcessInstanceRepository(r.DB).GetActiveExecutions(a.InstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to load active executions: %w", err)
	}
	var matched []repository.Execution
	for _, e := range executions {
		if e.Status != "waiting" || (e.PathID != nil && strings.Contains(*e.PathID, ":")) {
			continue
		}
		if (a.ExecutionID != uuid.Nil && e.ID == a.ExecutionID) ||
			(a.ExecutionID == uuid.Nil && a.ElementID != "" && e.CurrentElementID == a.ElementID) {
			matched = append(matched, e)
		}
	}
	switch {
	case a.ExecutionID == uuid.Nil && a.ElementID == "":
		return nil, refuse("executionId or elementId is required")
	case len(matched) == 0:
		return nil, refuse("no waiting token found")
	case len(matched) > 1:
		return nil, refuse("%d tokens wait at %q, give the executionId", len(matched), a.ElementID)
	}
	return &matched[0], nil
}
