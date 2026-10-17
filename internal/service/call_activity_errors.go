package service

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jmoiron/sqlx"
)

// CallerLink is the call activity that started a process instance.
type CallerLink struct {
	InstanceID uuid.UUID          // the calling instance
	ExecID     uuid.UUID          // its execution waiting on the call activity
	Node       *engine.Node       // the call activity
	Graph      *engine.ProcessGraph
}

// CallerOf returns the call activity that started instanceID, or nil for an
// instance nobody called (or whose caller has moved on).
func CallerOf(q sqlx.Queryer, instanceID uuid.UUID) *CallerLink {
	var parentInstanceID, parentExecID *uuid.UUID
	if err := q.QueryRowx(`SELECT parent_instance_id, parent_execution_id FROM public.process_instances WHERE id = $1`, instanceID).Scan(&parentInstanceID, &parentExecID); err != nil || parentInstanceID == nil || parentExecID == nil {
		return nil
	}
	var at struct {
		Node   string `db:"current_element_id"`
		Status string `db:"status"`
	}
	if err := sqlx.Get(q, &at, `SELECT current_element_id, status FROM public.executions WHERE process_instance_id = $1 AND id = $2`, *parentInstanceID, *parentExecID); err != nil || at.Status != "waiting" {
		return nil
	}
	var graphJSON []byte
	if err := sqlx.Get(q, &graphJSON, `
		SELECT d.parsed_graph FROM public.process_instances p
		JOIN public.process_definitions d ON d.id = p.process_definition_id
		WHERE p.id = $1
	`, *parentInstanceID); err != nil {
		return nil
	}
	var graph engine.ProcessGraph
	if err := json.Unmarshal(graphJSON, &graph); err != nil {
		return nil
	}
	node, ok := graph.Nodes[at.Node]
	if !ok || node.Type != engine.CallActivityType {
		return nil
	}
	return &CallerLink{InstanceID: *parentInstanceID, ExecID: *parentExecID, Node: node, Graph: &graph}
}

// CallerCatchesError tells whether BPMN error errorCode, uncaught in
// instanceID, is caught by the call activity that started it or anything
// above it — the calling processes, up the chain — as in Camunda 8.
func CallerCatchesError(q sqlx.Queryer, instanceID uuid.UUID, errorCode string) bool {
	for depth := 0; depth < 64; depth++ {
		caller := CallerOf(q, instanceID)
		if caller == nil {
			return false
		}
		if catcher, _ := caller.Graph.FindErrorCatcher(caller.Node.ID, errorCode); catcher != nil {
			return true
		}
		instanceID = caller.InstanceID
	}
	return false
}

// TerminateOrphanedCalledInstances terminates the running instances
// started by a call activity of parentInstanceID that is no longer
// waiting for them — interrupted by a boundary event, an error, an
// interrupting event sub-process, a terminate end event, a completed
// multi-instance loop — as Camunda 8 terminates a called process with its
// call activity. With parentInstanceID nil, it sweeps every instance whose
// calling execution has gone or moved on (run every second as a safety
// net; the runtime calls it right away after an interruption).
func TerminateOrphanedCalledInstances(db *sqlx.DB, dispatcher *events.TaskEventDispatcher, parentInstanceID *uuid.UUID) {
	var children []uuid.UUID
	var err error
	if parentInstanceID != nil {
		err = db.Select(&children, `
			SELECT id FROM public.process_instances
			WHERE parent_instance_id = $1 AND status = 'running'
		`, *parentInstanceID)
	} else {
		err = db.Select(&children, `
			SELECT c.id FROM public.process_instances c
			LEFT JOIN public.executions e ON e.process_instance_id = c.parent_instance_id AND e.id = c.parent_execution_id
			WHERE c.status = 'running' AND c.parent_execution_id IS NOT NULL
			  AND (e.id IS NULL OR e.status <> 'waiting')
			LIMIT 100
		`)
	}
	if err != nil {
		return
	}
	for _, child := range children {
		if CallerOf(db, child) != nil {
			continue // its call activity still waits for it
		}
		if err := TerminateProcessInstance(db, dispatcher, child, "its call activity was interrupted"); err != nil && !errors.Is(err, ErrProcessInstanceNotFound) {
			log.Printf("terminating called instance %s: %v", child, err)
		}
	}
}
