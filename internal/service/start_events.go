package service

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/feel"
	"github.com/jeremielodi/goflow/internal/timer"
	"github.com/jmoiron/sqlx"
)

// Timer start events, as in Camunda 8: deploying a process arms the timer
// start events of its new version and cancels those of older versions;
// deleting the latest version arms the previous one again. Each armed
// timer is a timer_jobs row (event_type "timerStart", no process
// instance) fired by the TimerScheduler, which hands it to the runtime
// (runtime.FireStartTimer) to start the instance and arm the next
// occurrence of a cycle.

// TimerStartEventType is timer_jobs.event_type for a timer start event.
const TimerStartEventType = "timerStart"

// TimerStartPayload is a timer start job's payload.
type TimerStartPayload struct {
	ProcessKey          string    `json:"processKey"`
	ProcessDefinitionID uuid.UUID `json:"processDefinitionId"`
	StartEventID        string    `json:"startEventId"`
	// Fired counts the firings of a cycle so far.
	Fired int `json:"fired"`
}

// ParseStartTimer parses a timer start event's definition. Its FEEL
// expressions are evaluated without variables (an instance doesn't exist
// yet), as in Camunda 8.
func ParseStartTimer(node *engine.Node) (*timer.Timer, error) {
	return timer.ParseXML(node.TimerDefinition, func(expr string) (string, error) {
		v, err := feel.Evaluate(expr, map[string]interface{}{})
		if err != nil {
			return "", err
		}
		s, ok := v.(string)
		if !ok {
			return "", fmt.Errorf("evaluates to %v, not a date, duration or cycle", v)
		}
		return s, nil
	})
}

// ArmStartTimers cancels the armed timer start events of processKey and
// arms those of definition defID (its latest version).
func ArmStartTimers(db *sqlx.DB, processKey string, defID uuid.UUID, graph *engine.ProcessGraph) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := disarmStartTimersTx(tx, processKey); err != nil {
		return err
	}
	now := time.Now()
	for _, node := range graph.Nodes {
		if node.Type != engine.StartTimerEventType || node.ParentScope != "" {
			continue
		}
		t, err := ParseStartTimer(node)
		if err != nil {
			return fmt.Errorf("timer start event %s: %w", node.ID, err)
		}
		due, ok := t.First(now)
		if !ok {
			continue
		}
		if err := InsertStartTimerTx(tx, due, TimerStartPayload{ProcessKey: processKey, ProcessDefinitionID: defID, StartEventID: node.ID}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func disarmStartTimersTx(tx *sqlx.Tx, processKey string) error {
	_, err := tx.Exec(`
		DELETE FROM timer_jobs
		WHERE event_type = $1 AND is_triggered = false AND payload ->> 'processKey' = $2
	`, TimerStartEventType, processKey)
	return err
}

// InsertStartTimerTx arms one timer start occurrence.
func InsertStartTimerTx(tx *sqlx.Tx, due time.Time, p TimerStartPayload) error {
	payload, _ := json.Marshal(p)
	_, err := tx.Exec(`
		INSERT INTO timer_jobs (id, process_instance_id, execution_id, event_type, due_at, payload, is_triggered, created_at)
		VALUES ($1, $2, NULL, $3, $4, $5, false, NOW())
	`, uuid.New(), uuid.Nil, TimerStartEventType, due.In(time.Local), payload)
	return err
}

// RearmStartEvents arms the timer start events of processKey's latest
// remaining version, or just disarms them if none is left — after a
// version was deleted.
func RearmStartEvents(db *sqlx.DB, processKey string) {
	var def struct {
		ID          uuid.UUID       `db:"id"`
		ParsedGraph json.RawMessage `db:"parsed_graph"`
	}
	err := db.Get(&def, `
		SELECT id, parsed_graph FROM process_definitions WHERE process_key = $1 ORDER BY version DESC LIMIT 1
	`, processKey)
	if err != nil {
		tx, txErr := db.Beginx()
		if txErr == nil {
			if disarmStartTimersTx(tx, processKey) == nil {
				tx.Commit()
			}
			tx.Rollback()
		}
		return
	}
	var graph engine.ProcessGraph
	if err := json.Unmarshal(def.ParsedGraph, &graph); err != nil {
		log.Printf("re-arming start events of %s: %v", processKey, err)
		return
	}
	if err := ArmStartTimers(db, processKey, def.ID, &graph); err != nil {
		log.Printf("re-arming start events of %s: %v", processKey, err)
	}
}
