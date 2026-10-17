package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jmoiron/sqlx"
)

// Conditional events (Camunda 7 style; Camunda 8 has none). A token
// waiting on a condition — an intermediate conditional catch event, or the
// trigger of a conditional boundary event or event sub-process — holds a
// 'conditional' event subscription whose event_name is the condition. The
// conditional watcher re-evaluates them against the instance's variables
// every conditionalPollInterval and resumes the ones that became true.
//
// A subscription fires on a false -> true transition: message_payload
// remembers the last evaluation ({"last": true|false}), so a
// non-interrupting conditional event, re-armed with last = true right after
// firing, doesn't fire again until its condition has been false in between.

const conditionalPollInterval = time.Second

// insertConditionalSubscriptionTx makes execID wait for condition; when it
// becomes true the watcher resumes execID on targetNodeID. lastTrue marks
// the condition as currently true (for a re-armed trigger).
func insertConditionalSubscriptionTx(tx *sqlx.Tx, instanceID, execID uuid.UUID, condition, targetNodeID string, lastTrue bool) error {
	var payload interface{}
	if lastTrue {
		payload = `{"last": true}`
	}
	_, err := tx.Exec(`
		INSERT INTO event_subscriptions
		  (process_instance_id, execution_id, event_type, event_name, target_element_id, message_payload)
		VALUES ($1, $2, 'conditional', $3, $4, $5)
	`, instanceID, execID, condition, targetNodeID, payload)
	return err
}

// executeConditionalCatch handles an intermediate conditional catch event:
// a condition already true lets the token through; otherwise it waits.
func (r *Runtime) executeConditionalCatch(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, variables map[string]interface{}) (passed bool, err error) {
	ok, evalErr := common.EvaluateCondition(node.Condition, variables)
	if evalErr != nil {
		// A condition on variables not set yet is simply not true yet.
		log.Printf("conditional event %s: %v — waiting", node.ID, evalErr)
	}
	if ok {
		return true, nil
	}
	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	if err := insertConditionalSubscriptionTx(tx, exec.ProcessInstanceID, exec.ID, node.Condition, node.ID, false); err != nil {
		return false, fmt.Errorf("conditional event %s: %w", node.ID, err)
	}
	if err := engineRepo.UpdateExecutionStatusTx(tx, exec.ProcessInstanceID, exec.ID, "waiting"); err != nil {
		return false, fmt.Errorf("conditional event %s: %w", node.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	service.LogAuditErr("execution waited", r.auditService.LogExecutionWaited(exec.ID, exec.ProcessInstanceID, node.ID, "conditional"))
	return false, nil
}

// StartConditionalWatcher runs the conditional watcher until ctx is done.
// Several GoFlow instances can run it: subscriptions are claimed with
// FOR UPDATE SKIP LOCKED.
func StartConditionalWatcher(ctx context.Context, db *sqlx.DB, dispatcher *events.TaskEventDispatcher) {
	go func() {
		ticker := time.NewTicker(conditionalPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := checkConditions(db, dispatcher); err != nil {
					log.Printf("conditional watcher: %v", err)
				}
			}
		}
	}()
}

type conditionalSubscription struct {
	ID                uuid.UUID `db:"id"`
	ProcessInstanceID uuid.UUID `db:"process_instance_id"`
	ExecutionID       uuid.UUID `db:"execution_id"`
	Condition         string    `db:"event_name"`
	TargetElementID   string    `db:"target_element_id"`
	Payload           []byte    `db:"message_payload"`
}

var (
	conditionErrorsLogged   = map[string]bool{}
	conditionErrorsLoggedMu sync.Mutex
)

func checkConditions(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var subs []conditionalSubscription
	if err := tx.Select(&subs, `
		SELECT id, process_instance_id, execution_id, event_name, COALESCE(target_element_id, '') AS target_element_id, message_payload
		FROM event_subscriptions
		WHERE event_type = 'conditional' AND consumed_at IS NULL AND execution_id IS NOT NULL
		ORDER BY created_at
		LIMIT 500
		FOR UPDATE SKIP LOCKED
	`); err != nil {
		return err
	}
	if len(subs) == 0 {
		return nil
	}

	engineRepo := repository.NewEngineRepository(db, dispatcher)
	varsByInstance := map[uuid.UUID]map[string]interface{}{}
	var fired []conditionalSubscription
	for _, s := range subs {
		vars, ok := varsByInstance[s.ProcessInstanceID]
		if !ok {
			vars, _ = engineRepo.GetProcessVariables(s.ProcessInstanceID)
			varsByInstance[s.ProcessInstanceID] = vars
		}
		now, evalErr := common.EvaluateCondition(s.Condition, vars)
		if evalErr != nil {
			logConditionErrorOnce(s.Condition, evalErr)
			now = false
		}

		var state struct {
			Last *bool `json:"last"`
		}
		if len(s.Payload) > 0 {
			_ = json.Unmarshal(s.Payload, &state)
		}
		switch {
		case now && (state.Last == nil || !*state.Last):
			if _, err := tx.Exec(`UPDATE event_subscriptions SET consumed_at = NOW() WHERE id = $1`, s.ID); err != nil {
				return err
			}
			fired = append(fired, s)
		case state.Last == nil || *state.Last != now:
			b, _ := json.Marshal(map[string]bool{"last": now})
			if _, err := tx.Exec(`UPDATE event_subscriptions SET message_payload = $2 WHERE id = $1`, s.ID, b); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	for _, s := range fired {
		exec, err := engineRepo.GetExecutionByID(s.ExecutionID)
		if err != nil || exec == nil || exec.Status != "waiting" {
			continue // its token moved on or was cancelled meanwhile
		}
		if err := repository.ResumeWaitingExecution(db, s.ProcessInstanceID, s.ExecutionID, s.TargetElementID); err != nil {
			log.Printf("conditional watcher: resuming %s: %v", s.ExecutionID, err)
			continue
		}
		if err := ResumeExecution(db, s.ExecutionID, dispatcher); err != nil {
			log.Printf("conditional watcher: continuing %s: %v", s.ExecutionID, err)
		}
	}
	return nil
}

func logConditionErrorOnce(condition string, err error) {
	conditionErrorsLoggedMu.Lock()
	defer conditionErrorsLoggedMu.Unlock()
	if conditionErrorsLogged[condition] {
		return
	}
	if len(conditionErrorsLogged) > 1000 {
		conditionErrorsLogged = map[string]bool{}
	}
	conditionErrorsLogged[condition] = true
	log.Printf("conditional watcher: %q can't be evaluated yet (%v) — treated as false", condition, err)
}
