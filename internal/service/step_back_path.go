package service

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/models"
)

// IsStepBackTarget reports whether a token can be stepped back to n: only
// the elements where a token actually stops (a task waiting for a worker or
// a user, a catch event, an event-based gateway, a sub-process or call
// activity run again from its start). Pass-through elements (gateways,
// script and business rule tasks, throw events) are skipped: stepping back
// to one would only run straight forward again.
func IsStepBackTarget(n *engine.Node) bool {
	if n == nil || n.MultiInstance != nil {
		return false
	}
	switch n.Type {
	case engine.UserTaskType, engine.ServiceTaskType,
		engine.IntermediateCatchEventType, engine.IntermediateTimerEventType,
		engine.IntermediateMessageCatchEventType, engine.IntermediateSignalCatchEventType,
		engine.IntermediateConditionalCatchEventType, engine.EventBasedGatewayType,
		engine.SubProcessType, engine.CallActivityType:
		return true
	}
	return false
}

// StepBackPath returns, oldest first, the wait states one execution went
// through, as far back as the token can be stepped back. It reads the
// execution's EXECUTION_MOVED audit entries:
//   - only elements of the execution's own scope (parentScope) that are
//     step-back targets are kept, consecutive repeats collapsed;
//   - passing a parallel or inclusive gateway starts the path over, since a
//     token can't be stepped back across a fork or a join;
//   - an earlier step back (newData.stepBack) pops the path back to its
//     target, so "previous" pressed twice goes back two steps instead of
//     bouncing between the last two.
func StepBackPath(logs []models.AuditLog, graph *engine.ProcessGraph, execID uuid.UUID, scope string) []string {
	var path []string
	isBarrier := func(id string) bool {
		n := graph.Nodes[id]
		return n != nil && (n.Type == engine.ParallelGatewayType || n.Type == engine.InclusiveGatewayType)
	}
	keep := func(id string) {
		n := graph.Nodes[id]
		if !IsStepBackTarget(n) || n.ParentScope != scope {
			return
		}
		if len(path) > 0 && path[len(path)-1] == id {
			return
		}
		path = append(path, id)
	}
	first := true
	for _, l := range logs {
		if l.Action != models.ActionExecutionMoved {
			continue
		}
		var d struct {
			ExecutionID string `json:"executionId"`
			FromNode    string `json:"fromNode"`
			ToNode      string `json:"toNode"`
			StepBack    bool   `json:"stepBack"`
		}
		if json.Unmarshal(l.NewData, &d) != nil || d.ExecutionID != execID.String() {
			continue
		}
		// An execution created right at an element (a parallel branch)
		// has no entry for arriving there: its first move starts from it.
		if first {
			first = false
			keep(d.FromNode)
		}
		if d.StepBack {
			for i := len(path) - 2; i >= 0; i-- {
				if path[i] == d.ToNode {
					path = path[:i+1]
					break
				}
			}
			continue
		}
		if isBarrier(d.FromNode) {
			path = nil
		}
		if isBarrier(d.ToNode) {
			path = nil
			continue
		}
		keep(d.ToNode)
	}
	return path
}
