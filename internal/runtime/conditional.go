package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/cmdbus"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
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

// conditionalPollInterval is the safety-net sweep: variable writes trigger
// an immediate check (see StartConditionalWatcher), so this only matters
// for conditions on time and notifications a dropped listener missed.
const conditionalPollInterval = 10 * time.Second

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

// variablesChannel is the NOTIFY channel a database trigger signals every
// variable write on, with the instance id (migration 004).
const variablesChannel = "goflow_variables"

// StartConditionalWatcher runs the conditional watcher until ctx is done.
// Conditions are evaluated as soon as a variable of their instance changes
// (listenerConnStr: a dedicated LISTEN connection on variablesChannel), as
// in Camunda 7, and also every conditionalPollInterval — conditions on
// time (now()) and anything a dropped listener missed. Several GoFlow
// instances can run it: subscriptions are claimed with FOR UPDATE SKIP
// LOCKED.
func StartConditionalWatcher(ctx context.Context, db *sqlx.DB, dispatcher *events.TaskEventDispatcher, listenerConnStr string) {
	changed := make(chan uuid.UUID, 4096)
	if listenerConnStr != "" {
		listener := pq.NewListener(listenerConnStr, 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
			if err != nil {
				log.Printf("conditional watcher listener: %v", err)
			}
		})
		if err := listener.Listen(variablesChannel); err != nil {
			log.Printf("conditional watcher: LISTEN %s: %v (polling only)", variablesChannel, err)
			listener.Close()
		} else {
			go func() {
				for n := range listener.Notify {
					if n == nil {
						continue // reconnected; the poll covers the gap
					}
					if id, err := uuid.Parse(n.Extra); err == nil {
						select {
						case changed <- id:
						default: // flooded: the poll will catch up
						}
					}
				}
			}()
		}
	}

	go func() {
		ticker := time.NewTicker(conditionalPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := recheckConditions(ctx, db, dispatcher, nil); err != nil {
					log.Printf("conditional watcher: %v", err)
				}
			case id := <-changed:
				// Writes come in bursts: gather the instances touched in
				// the next few milliseconds and check them together.
				ids := map[uuid.UUID]bool{id: true}
				gather := time.After(20 * time.Millisecond)
			collect:
				for {
					select {
					case more := <-changed:
						ids[more] = true
					case <-gather:
						break collect
					}
				}
				list := make([]uuid.UUID, 0, len(ids))
				for i := range ids {
					list = append(list, i)
				}
				if err := recheckConditions(ctx, db, dispatcher, list); err != nil {
					log.Printf("conditional watcher: %v", err)
				}
			}
		}
	}()
}

// recheckConditions re-evaluates the conditions of the given instances
// (all with nil). Partitioned, each instance's check is a command on its
// partition, issued by the replica owning it only.
func recheckConditions(ctx context.Context, db *sqlx.DB, dispatcher *events.TaskEventDispatcher, instances []uuid.UUID) error {
	if !cmdbus.Partitioned() {
		return checkConditions(db, dispatcher, instances)
	}
	if instances == nil {
		if err := db.Select(&instances, `
			SELECT DISTINCT process_instance_id FROM event_subscriptions
			WHERE event_type = 'conditional' AND consumed_at IS NULL`); err != nil {
			return err
		}
	}
	for _, id := range instances {
		if !cmdbus.OwnedHere(id) {
			continue
		}
		if err := cmdbus.Post(ctx, cmdbus.Key(id), CmdCheckConditions, checkConditionsArgs{InstanceID: id}); err != nil {
			return err
		}
	}
	return nil
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

// checkConditions evaluates the open conditional subscriptions — of the
// given instances only, or of all with instances nil.
func checkConditions(db *sqlx.DB, dispatcher *events.TaskEventDispatcher, instances []uuid.UUID) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var only interface{}
	if instances != nil {
		ids := make([]string, len(instances))
		for i, id := range instances {
			ids[i] = id.String()
		}
		only = pq.Array(ids)
	}
	var subs []conditionalSubscription
	if err := tx.Select(&subs, `
		SELECT id, process_instance_id, execution_id, event_name, COALESCE(target_element_id, '') AS target_element_id, message_payload
		FROM event_subscriptions
		WHERE event_type = 'conditional' AND consumed_at IS NULL AND execution_id IS NOT NULL
		  AND ($1::uuid[] IS NULL OR process_instance_id = ANY($1::uuid[]))
		ORDER BY created_at
		LIMIT 500
		FOR UPDATE SKIP LOCKED
	`, only); err != nil {
		return err
	}
	if len(subs) == 0 {
		return nil
	}

	engineRepo := repository.NewEngineRepository(db, dispatcher)
	varsByScope := map[uuid.UUID]map[string]interface{}{}
	var fired []conditionalSubscription
	for _, s := range subs {
		// Evaluated in the scope of the waiting element.
		scope := repository.VariableScopeOf(tx, s.ProcessInstanceID, s.ExecutionID)
		vars, ok := varsByScope[scope]
		if !ok {
			vars, _ = repository.VariablesForExecution(tx, s.ProcessInstanceID, scope)
			varsByScope[scope] = vars
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
