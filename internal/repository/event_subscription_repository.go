package repository

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// EventSubscription represents a row in event_subscriptions.
type EventSubscription struct {
	ID                uuid.UUID  `db:"id"`
	ProcessInstanceID uuid.UUID  `db:"process_instance_id"`
	ExecutionID       uuid.UUID  `db:"execution_id"`
	EventType         string     `db:"event_type"`
	EventName         string     `db:"event_name"`
	CorrelationKey    *string    `db:"correlation_key"`
	TargetElementID   *string    `db:"target_element_id"`
	CreatedAt         time.Time  `db:"created_at"`
	ConsumedAt        *time.Time `db:"consumed_at"`
}

type EventSubscriptionRepository struct {
	db *sqlx.DB
}

func NewEventSubscriptionRepository(db *sqlx.DB) *EventSubscriptionRepository {
	return &EventSubscriptionRepository{db: db}
}

// FindByMessageName returns the first unconsumed message subscription matching the
// given message name and optional correlation key filter.
// If correlationVarName + correlationVarValue are provided, we additionally
// filter by matching a process variable.
func (r *EventSubscriptionRepository) FindByMessageName(
	messageName string,
	correlationVarName string,
	correlationVarValue string,
) ([]EventSubscription, error) {
	var subs []EventSubscription

	if correlationVarName != "" && correlationVarValue != "" {
		// Filter by process variable value
		err := r.db.Select(&subs, `
			SELECT es.id, es.process_instance_id, es.execution_id, es.event_type,
			       es.event_name, es.correlation_key, es.target_element_id,
			       es.created_at, es.consumed_at
			FROM event_subscriptions es
			JOIN variables v ON v.process_instance_id = es.process_instance_id
			WHERE es.event_type = 'message'
			  AND es.event_name = $1
			  AND es.consumed_at IS NULL
			  AND v.data->>$2 = $3
			ORDER BY es.created_at ASC
		`, messageName, correlationVarName, correlationVarValue)
		return subs, err
	}

	err := r.db.Select(&subs, `
		SELECT id, process_instance_id, execution_id, event_type,
		       event_name, correlation_key, target_element_id,
		       created_at, consumed_at
		FROM event_subscriptions
		WHERE event_type = 'message'
		  AND event_name = $1
		  AND consumed_at IS NULL
		ORDER BY created_at ASC
	`, messageName)
	return subs, err
}

// FindBySignalName returns all unconsumed signal subscriptions for the given signal name.
func (r *EventSubscriptionRepository) FindBySignalName(signalName string) ([]EventSubscription, error) {
	var subs []EventSubscription
	err := r.db.Select(&subs, `
		SELECT id, process_instance_id, execution_id, event_type,
		       event_name, correlation_key, target_element_id,
		       created_at, consumed_at
		FROM event_subscriptions
		WHERE event_type = 'signal'
		  AND event_name = $1
		  AND consumed_at IS NULL
		ORDER BY created_at ASC
	`, signalName)
	return subs, err
}

// Consume marks a subscription as consumed.
func (r *EventSubscriptionRepository) Consume(id uuid.UUID) error {
	now := time.Now()
	_, err := r.db.Exec(`
		UPDATE event_subscriptions SET consumed_at = $1 WHERE id = $2
	`, now, id)
	return err
}

// CancelSiblingSubscriptions marks all other unconsumed subscriptions for the same
// execution as consumed (used by Event-based Gateway when one event fires).
func (r *EventSubscriptionRepository) CancelSiblingSubscriptions(executionID uuid.UUID, exceptID uuid.UUID) error {
	_, err := r.db.Exec(`
		UPDATE event_subscriptions
		SET consumed_at = NOW()
		WHERE execution_id = $1
		  AND id != $2
		  AND consumed_at IS NULL
	`, executionID, exceptID)
	return err
}

// ResumeExecution reactivates a waiting execution and advances it to targetElementID.
// If targetElementID is empty, the execution stays at its current node.
func ResumeWaitingExecution(db *sqlx.DB, executionID uuid.UUID, targetElementID string) error {
	var err error
	if targetElementID != "" {
		_, err = db.Exec(`
			UPDATE executions
			SET current_element_id = $1, status = 'active', is_active = true, updated_at = NOW()
			WHERE id = $2
		`, targetElementID, executionID)
	} else {
		_, err = db.Exec(`
			UPDATE executions
			SET status = 'active', is_active = true, updated_at = NOW()
			WHERE id = $1
		`, executionID)
	}
	return err
}

// FindByProcessInstance returns all subscriptions for a process instance.
func (r *EventSubscriptionRepository) FindByProcessInstance(processInstanceID uuid.UUID) ([]EventSubscription, error) {
	var subs []EventSubscription
	err := r.db.Select(&subs, `
		SELECT id, process_instance_id, execution_id, event_type,
		       event_name, correlation_key, target_element_id,
		       created_at, consumed_at
		FROM event_subscriptions
		WHERE process_instance_id = $1
		ORDER BY created_at ASC
	`, processInstanceID)
	return subs, err
}

// GetSubscription returns a single subscription by ID (nil if not found).
func (r *EventSubscriptionRepository) GetSubscription(id uuid.UUID) (*EventSubscription, error) {
	var sub EventSubscription
	err := r.db.Get(&sub, `
		SELECT id, process_instance_id, execution_id, event_type,
		       event_name, correlation_key, target_element_id,
		       created_at, consumed_at
		FROM event_subscriptions WHERE id = $1
	`, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &sub, err
}
