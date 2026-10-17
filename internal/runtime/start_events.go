package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jmoiron/sqlx"
)

// FireStartTimer fires a timer start event (see service.ArmStartTimers):
// it arms the cycle's next occurrence, then starts an instance of the
// process at that start event. Set on the TimerScheduler in main.
func FireStartTimer(ctx context.Context, db *sqlx.DB, dispatcher *events.TaskEventDispatcher, job models.TimerJob) error {
	var p service.TimerStartPayload
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		return fmt.Errorf("timer start payload: %w", err)
	}
	def, err := repository.NewProcessRepository(db).FindProcessDefinitionByID(p.ProcessDefinitionID)
	if err != nil {
		return fmt.Errorf("process definition %s: %w", p.ProcessDefinitionID, err)
	}
	var graph engine.ProcessGraph
	if err := json.Unmarshal(def.ParsedGraph, &graph); err != nil {
		return err
	}
	node, ok := graph.Nodes[p.StartEventID]
	if !ok {
		return fmt.Errorf("start event %s not found in %s", p.StartEventID, p.ProcessKey)
	}

	// The next occurrence first: a failing instance must not stop a cycle.
	if err := armNextStartTimer(db, node, job.DueAt, p); err != nil {
		log.Printf("timer start %s/%s: arming the next occurrence: %v", p.ProcessKey, p.StartEventID, err)
	}

	var tenant *string
	if def.TenantID != nil && *def.TenantID != "" {
		tenant = def.TenantID
	}
	_, execID, err := service.StartProcessInstanceAt(db, &graph, def.ID, tenant, nil, node.ID)
	if err != nil {
		return err
	}
	return NewRuntime(&graph, db, dispatcher).ExecuteExecution(ctx, execID)
}

// armNextStartTimer arms a cycle's occurrence after the one due at dueAt
// (a TIMESTAMP holding local time). Occurrences missed while the engine
// was down are skipped rather than fired in a burst.
func armNextStartTimer(db *sqlx.DB, node *engine.Node, dueAt time.Time, p service.TimerStartPayload) error {
	t, err := service.ParseStartTimer(node)
	if err != nil {
		return err
	}
	prev := time.Date(dueAt.Year(), dueAt.Month(), dueAt.Day(), dueAt.Hour(), dueAt.Minute(), dueAt.Second(), dueAt.Nanosecond(), time.Local)
	fired := p.Fired + 1
	next, ok := t.Next(prev, fired)
	for ok && next.Before(time.Now().Add(-time.Minute)) {
		fired++
		next, ok = t.Next(next, fired)
	}
	if !ok {
		return nil
	}
	p.Fired = fired
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := service.InsertStartTimerTx(tx, next, p); err != nil {
		return err
	}
	return tx.Commit()
}

// startBySignal starts an instance of every process (latest version) whose
// top-level signal start event waits for this signal, with the signal's
// variables, as in Camunda 8.
func startBySignal(ctx context.Context, db *sqlx.DB, dispatcher *events.TaskEventDispatcher, name string, variables map[string]interface{}) ([]uuid.UUID, []string) {
	var targets []messageStartTarget
	if err := db.Select(&targets, `
		SELECT d.id, d.process_key, n.key AS start_node_id
		FROM process_definitions d
		JOIN LATERAL jsonb_each(d.parsed_graph -> 'Nodes') n ON true
		WHERE d.version = (SELECT max(version) FROM process_definitions WHERE process_key = d.process_key)
		  AND n.value ->> 'Type' = $1
		  AND n.value ->> 'signalRef' = $2
		  AND COALESCE(n.value ->> 'parentScope', '') = ''
	`, string(engine.SignalStartEventType), name); err != nil {
		return nil, []string{err.Error()}
	}
	var started []uuid.UUID
	var errs []string
	for _, t := range targets {
		def, err := repository.NewProcessRepository(db).FindProcessDefinitionByID(t.DefinitionID)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		var graph engine.ProcessGraph
		if err := json.Unmarshal(def.ParsedGraph, &graph); err != nil {
			errs = append(errs, err.Error())
			continue
		}
		var tenant *string
		if def.TenantID != nil && *def.TenantID != "" {
			tenant = def.TenantID
		}
		instanceID, execID, err := service.StartProcessInstanceAt(db, &graph, def.ID, tenant, variables, t.StartNodeID)
		if err != nil {
			errs = append(errs, fmt.Sprintf("starting %s: %v", t.ProcessKey, err))
			continue
		}
		started = append(started, instanceID)
		if err := NewRuntime(&graph, db, dispatcher).ExecuteExecution(ctx, execID); err != nil {
			errs = append(errs, fmt.Sprintf("running %s: %v", t.ProcessKey, err))
		}
	}
	return started, errs
}
