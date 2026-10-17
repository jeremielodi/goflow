package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/feel"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jmoiron/sqlx"
)

// Camunda 8 message semantics, used by the gRPC PublishMessage RPC and the
// /v2 publication endpoint (the Camunda 7 style /engine-rest/message keeps
// its own semantics in internal/api).
//
//   - A message catch (intermediate event, receive task, boundary event,
//     event sub-process, event-based gateway branch) subscribes with a
//     correlation key: its message's zeebe:subscription correlationKey
//     expression, evaluated when the subscription opens.
//   - A message goes at most once to each process (BPMN process id, all
//     versions): to its oldest open subscription with the same name and
//     correlation key, or else — if the process has a message start event
//     for that name — to a new instance (one active instance per
//     correlation key).
//   - With a time to live, the message is also kept until it expires
//     (published_messages) and goes to the processes it hasn't gone to yet
//     as subscriptions open or start conditions allow. A messageId
//     deduplicates publications while the first one is alive.

// ErrMessageAlreadyPublished is returned for a messageId already published
// and not yet expired.
var ErrMessageAlreadyPublished = errors.New("a message with this id is already published")

// PublishedMessage is a message to publish.
type PublishedMessage struct {
	Name           string
	CorrelationKey string
	Variables      map[string]interface{}
	TimeToLive     time.Duration
	MessageID      string
	TenantID       string
}

// correlationKeyValue renders a correlation key value the way Camunda 8
// does: strings as is, integral numbers without a fraction.
func correlationKeyValue(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		if x == math.Trunc(x) && math.Abs(x) < 1e15 {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	}
	return fmt.Sprint(v)
}

// messageCorrelationKey evaluates a message catch node's correlation key
// expression against the instance's variables ("" when it has none).
func (r *Runtime) messageCorrelationKey(node *engine.Node, instanceID uuid.UUID) string {
	if strings.TrimSpace(node.MessageCorrelationKey) == "" {
		return ""
	}
	vars, err := repository.NewEngineRepository(r.DB, r.dispatcher).GetProcessVariables(instanceID)
	if err != nil {
		log.Printf("message %q: loading variables for its correlation key: %v", node.MessageRef, err)
		return ""
	}
	v, err := feel.Evaluate(strings.TrimPrefix(strings.TrimSpace(node.MessageCorrelationKey), "="), vars)
	if err != nil {
		log.Printf("message %q: correlation key %q: %v", node.MessageRef, node.MessageCorrelationKey, err)
		return ""
	}
	return correlationKeyValue(v)
}

// insertMessageSubscriptionTx opens a message subscription for execID.
// targetNodeID "" means "the catch node's outgoing flow"; otherwise the
// execution jumps there (event-based gateway branches, boundary and
// event sub-process triggers).
func insertMessageSubscriptionTx(tx *sqlx.Tx, instanceID, execID uuid.UUID, messageName, correlationKey, targetNodeID string) error {
	var target interface{}
	if targetNodeID != "" {
		target = targetNodeID
	}
	_, err := tx.Exec(`
		INSERT INTO event_subscriptions
		  (process_instance_id, execution_id, event_type, event_name, correlation_key, target_element_id)
		VALUES ($1, $2, 'message', $3, $4, $5)
	`, instanceID, execID, messageName, correlationKey, target)
	return err
}

// PublishResult is what publishing a message did.
type PublishResult struct {
	// MessageID identifies the publication (the kept message's id when it
	// has a time to live).
	MessageID uuid.UUID
	// Correlated are the waiting instances it correlated to, Started the
	// instances its message start events started.
	Correlated []uuid.UUID
	Started    []uuid.UUID
}

// PublishMessage publishes a message with Camunda 8 semantics (see above).
// Finding nothing waiting is not an error: with a time to live the message
// waits, without one it is dropped, as in Camunda 8.
func PublishMessage(ctx context.Context, db *sqlx.DB, dispatcher *events.TaskEventDispatcher, msg PublishedMessage) (PublishResult, error) {
	res := PublishResult{MessageID: uuid.New()}
	if msg.Name == "" {
		return res, fmt.Errorf("message name is required")
	}
	if msg.MessageID != "" {
		var exists bool
		if err := db.Get(&exists, `
			SELECT EXISTS (SELECT 1 FROM published_messages
			               WHERE name = $1 AND message_id = $2 AND expires_at > NOW())
		`, msg.Name, msg.MessageID); err != nil {
			return res, err
		}
		if exists {
			return res, ErrMessageAlreadyPublished
		}
	}

	// Keep it first when it has a time to live, so a subscription opening
	// while this call runs still finds it (the sweep catches that case).
	var storedID *uuid.UUID
	if msg.TimeToLive > 0 {
		vars, _ := json.Marshal(nonNil(msg.Variables))
		if _, err := db.Exec(`
			INSERT INTO published_messages (id, name, correlation_key, message_id, variables, tenant_id, expires_at)
			VALUES ($1, $2, $3, NULLIF($4, ''), $5, NULLIF($6, ''), NOW() + make_interval(secs => $7))
		`, res.MessageID, msg.Name, msg.CorrelationKey, msg.MessageID, vars, msg.TenantID, msg.TimeToLive.Seconds()); err != nil {
			return res, err
		}
		storedID = &res.MessageID
	}

	done := map[string]bool{}
	var err error
	if res.Correlated, err = correlateToSubscriptions(ctx, db, dispatcher, msg.Name, msg.CorrelationKey, msg.Variables, storedID, "", done); err != nil {
		return res, err
	}
	res.Started, err = startByMessage(ctx, db, dispatcher, msg, storedID, done)
	return res, err
}

func nonNil(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return map[string]interface{}{}
	}
	return m
}

type openMessageSubscription struct {
	ID                uuid.UUID `db:"id"`
	ProcessInstanceID uuid.UUID `db:"process_instance_id"`
	ExecutionID       uuid.UUID `db:"execution_id"`
	TargetElementID   *string   `db:"target_element_id"`
	ProcessKey        string    `db:"process_key"`
}

// correlateToSubscriptions correlates a message to the open subscriptions
// with its name and correlation key: the oldest one of each process (BPMN
// process id, all versions), at most once per process. done holds the
// processes it already went to (filled in); a kept message (storedID) also
// skips those recorded in published_message_correlations; onlyProcess, if
// set, limits it to that process.
//
// A subscription without a correlation key expression also matches a
// non-empty key equal to its instance's "correlationKey" variable: what
// GoFlow's gRPC and /v2 publish did before correlation keys existed, kept
// so processes deployed for it keep working.
func correlateToSubscriptions(ctx context.Context, db *sqlx.DB, dispatcher *events.TaskEventDispatcher, name, key string, vars map[string]interface{}, storedID *uuid.UUID, onlyProcess string, done map[string]bool) ([]uuid.UUID, error) {
	var subs []openMessageSubscription
	if err := db.Select(&subs, `
		SELECT s.id, s.process_instance_id, s.execution_id, s.target_element_id, d.process_key
		FROM event_subscriptions s
		JOIN process_instances p ON p.id = s.process_instance_id
		JOIN process_definitions d ON d.id = p.process_definition_id
		WHERE s.event_type = 'message' AND s.event_name = $1 AND s.consumed_at IS NULL
		  AND (COALESCE(s.correlation_key, '') = $2
		       OR ($2 <> '' AND COALESCE(s.correlation_key, '') = '' AND EXISTS (
		             SELECT 1 FROM variables v
		             WHERE v.process_instance_id = s.process_instance_id AND v.data ->> 'correlationKey' = $2)))
		  AND ($3 = '' OR d.process_key = $3)
		  AND ($4::uuid IS NULL OR NOT EXISTS (
		        SELECT 1 FROM published_message_correlations c
		        WHERE c.published_message_id = $4 AND c.process_key = d.process_key))
		ORDER BY d.process_key, s.created_at
	`, name, key, onlyProcess, storedID); err != nil {
		return nil, err
	}
	var correlated []uuid.UUID
	for _, s := range subs {
		// Oldest first within a process: the next one only if an earlier
		// one was consumed by someone else meanwhile.
		if done[s.ProcessKey] {
			continue
		}
		ok, err := correlateSubscription(ctx, db, dispatcher, s, name, vars, storedID)
		if err != nil {
			log.Printf("message %q: correlating to instance %s: %v", name, s.ProcessInstanceID, err)
		}
		if ok {
			done[s.ProcessKey] = true
			correlated = append(correlated, s.ProcessInstanceID)
		}
	}
	return correlated, nil
}

// recordCorrelationTx records that a kept message went to a process; false
// when it already had (another publisher or sweep got there first).
func recordCorrelationTx(tx *sqlx.Tx, storedID *uuid.UUID, processKey string, instanceID uuid.UUID) (bool, error) {
	if storedID == nil {
		return true, nil
	}
	res, err := tx.Exec(`
		INSERT INTO published_message_correlations (published_message_id, process_key, process_instance_id)
		VALUES ($1, $2, $3) ON CONFLICT DO NOTHING
	`, *storedID, processKey, instanceID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// correlateSubscription consumes one subscription and moves its execution
// on: to target_element_id when set, otherwise along the catch node's
// outgoing flow. False when another publisher consumed it first.
func correlateSubscription(ctx context.Context, db *sqlx.DB, dispatcher *events.TaskEventDispatcher, s openMessageSubscription, name string, vars map[string]interface{}, storedID *uuid.UUID) (bool, error) {
	engineRepo := repository.NewEngineRepository(db, dispatcher)
	tx, err := db.Beginx()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var claimed uuid.UUID
	if err := tx.Get(&claimed, `
		UPDATE event_subscriptions SET consumed_at = NOW()
		WHERE id = $1 AND consumed_at IS NULL RETURNING id
	`, s.ID); err != nil {
		return false, nil // consumed meanwhile
	}
	if ok, err := recordCorrelationTx(tx, storedID, s.ProcessKey, s.ProcessInstanceID); err != nil || !ok {
		return false, err
	}

	var fromNode string
	if err := tx.Get(&fromNode, `SELECT current_element_id FROM executions WHERE process_instance_id = $1 AND id = $2`, s.ProcessInstanceID, s.ExecutionID); err != nil {
		return false, fmt.Errorf("execution %s not found: %w", s.ExecutionID, err)
	}
	target := ""
	if s.TargetElementID != nil && *s.TargetElementID != "" {
		target = *s.TargetElementID
		// An event-based gateway branch, a boundary or event sub-process
		// trigger: the other events it was waiting for no longer apply.
		if _, err := tx.Exec(`
			UPDATE event_subscriptions SET consumed_at = NOW()
			WHERE execution_id = $1 AND id <> $2 AND consumed_at IS NULL AND event_type <> 'compensable'
		`, s.ExecutionID, s.ID); err != nil {
			return false, err
		}
		if _, err := tx.Exec(`
			DELETE FROM timer_jobs WHERE process_instance_id = $1 AND execution_id = $2 AND is_triggered = false
		`, s.ProcessInstanceID, s.ExecutionID); err != nil {
			return false, err
		}
	} else if graph, err := engineRepo.GetProcessGraphByInstanceID(s.ProcessInstanceID); err == nil {
		if catchNode, ok := graph.Nodes[fromNode]; ok && len(catchNode.Outgoing) > 0 {
			target = catchNode.Outgoing[0].TargetRef
		}
	}

	if len(vars) > 0 {
		var data []byte
		current := map[string]interface{}{}
		if err := tx.Get(&data, `SELECT data FROM variables WHERE process_instance_id = $1 FOR UPDATE`, s.ProcessInstanceID); err == nil && len(data) > 0 {
			_ = json.Unmarshal(data, &current)
		}
		for k, v := range vars {
			current[k] = v
		}
		if err := engineRepo.UpdateProcessVariablesTx(tx, s.ProcessInstanceID, current); err != nil {
			return false, err
		}
	}
	if target != "" {
		if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, s.ProcessInstanceID, s.ExecutionID, "active", target); err != nil {
			return false, err
		}
	} else if err := engineRepo.UpdateExecutionStatusTx(tx, s.ProcessInstanceID, s.ExecutionID, "active"); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}

	audit := service.NewAuditService(db)
	service.LogAuditErr("message correlated", audit.LogMessageCorrelated(s.ProcessInstanceID, name))
	if target != "" {
		service.LogAuditErr("execution moved", audit.LogExecutionMoved(s.ExecutionID, s.ProcessInstanceID, fromNode, target, vars))
	}
	if p := eventlog.Global(); p != nil {
		p.MessageCorrelated(s.ProcessInstanceID.String(), name)
		if target != "" {
			p.ExecutionMoved(s.ProcessInstanceID.String(), s.ExecutionID.String(), fromNode, target)
		}
	}
	// Correlated even if running on from there fails (that surfaces as an
	// incident or in the log, like any other resume).
	return true, ResumeExecution(db, s.ExecutionID, dispatcher)
}

type messageStartTarget struct {
	DefinitionID uuid.UUID `db:"id"`
	ProcessKey   string    `db:"process_key"`
	StartNodeID  string    `db:"start_node_id"`
}

// startByMessage starts an instance of every process (latest version)
// whose top-level message start event waits for this message, except the
// processes it already went to (done, and for a kept message the recorded
// correlations) and those with an instance started with the same
// correlation key still active — as in Camunda 8.
func startByMessage(ctx context.Context, db *sqlx.DB, dispatcher *events.TaskEventDispatcher, msg PublishedMessage, storedID *uuid.UUID, done map[string]bool) ([]uuid.UUID, error) {
	var targets []messageStartTarget
	if err := db.Select(&targets, `
		SELECT d.id, d.process_key, n.key AS start_node_id
		FROM process_definitions d
		JOIN LATERAL jsonb_each(d.parsed_graph -> 'Nodes') n ON true
		WHERE d.version = (SELECT max(version) FROM process_definitions WHERE process_key = d.process_key)
		  AND n.value ->> 'Type' = 'messageStartEvent'
		  AND n.value ->> 'messageRef' = $1
		  AND COALESCE(n.value ->> 'parentScope', '') = ''
		  AND ($2::uuid IS NULL OR NOT EXISTS (
		        SELECT 1 FROM published_message_correlations c
		        WHERE c.published_message_id = $2 AND c.process_key = d.process_key))
	`, msg.Name, storedID); err != nil {
		return nil, err
	}
	var started []uuid.UUID
	for _, t := range targets {
		if done[t.ProcessKey] {
			continue
		}
		instanceID, err := startOneByMessage(ctx, db, dispatcher, t, msg, storedID)
		if err != nil {
			log.Printf("message %q: starting %s: %v", msg.Name, t.ProcessKey, err)
		}
		if instanceID != uuid.Nil {
			done[t.ProcessKey] = true
			started = append(started, instanceID)
		}
	}
	return started, nil
}

// startOneByMessage starts one instance at a message start event; uuid.Nil
// when an instance started with the same correlation key is still active
// or the kept message already went to this process.
func startOneByMessage(ctx context.Context, db *sqlx.DB, dispatcher *events.TaskEventDispatcher, t messageStartTarget, msg PublishedMessage, storedID *uuid.UUID) (uuid.UUID, error) {
	def, err := repository.NewProcessRepository(db).FindProcessDefinitionByID(t.DefinitionID)
	if err != nil {
		return uuid.Nil, err
	}
	var graph engine.ProcessGraph
	if err := json.Unmarshal(def.ParsedGraph, &graph); err != nil {
		return uuid.Nil, err
	}
	var tenant *string
	if msg.TenantID != "" {
		tenant = &msg.TenantID
	}

	// One active instance per (process, correlation key): the check and
	// the marker row are serialized by an advisory lock held until this
	// transaction ends, across app instances too.
	tx, err := db.Beginx()
	if err != nil {
		return uuid.Nil, err
	}
	defer tx.Rollback()
	if msg.CorrelationKey != "" {
		if _, err := tx.Exec(`SELECT pg_advisory_xact_lock(hashtext('message_start:' || $1 || ':' || $2))`, t.ProcessKey, msg.CorrelationKey); err != nil {
			return uuid.Nil, err
		}
		var active bool
		if err := tx.Get(&active, `
			SELECT EXISTS (SELECT 1 FROM event_subscriptions s
			               JOIN process_instances p ON p.id = s.process_instance_id
			               WHERE s.event_type = 'message_start' AND s.event_name = $1
			                 AND s.correlation_key = $2 AND s.consumed_at IS NULL
			                 AND p.ended_at IS NULL)
		`, t.ProcessKey, msg.CorrelationKey); err != nil {
			return uuid.Nil, err
		}
		if active {
			return uuid.Nil, nil
		}
	}
	// Claimed before starting, so a concurrent sweep can't start it twice;
	// the instance id is filled in once known.
	if ok, err := recordCorrelationTx(tx, storedID, t.ProcessKey, uuid.Nil); err != nil || !ok {
		return uuid.Nil, err
	}
	instanceID, execID, err := service.StartProcessInstanceAt(db, &graph, def.ID, tenant, msg.Variables, t.StartNodeID)
	if err != nil {
		return uuid.Nil, err
	}
	if msg.CorrelationKey != "" {
		if _, err := tx.Exec(`
			INSERT INTO event_subscriptions (process_instance_id, execution_id, event_type, event_name, correlation_key)
			VALUES ($1, $2, 'message_start', $3, $4)
		`, instanceID, execID, t.ProcessKey, msg.CorrelationKey); err != nil {
			return instanceID, err
		}
	}
	if storedID != nil {
		if _, err := tx.Exec(`
			UPDATE published_message_correlations SET process_instance_id = $3
			WHERE published_message_id = $1 AND process_key = $2
		`, *storedID, t.ProcessKey, instanceID); err != nil {
			return instanceID, err
		}
	}
	if err := tx.Commit(); err != nil {
		return instanceID, err
	}
	rt := NewRuntime(&graph, db, dispatcher)
	return instanceID, rt.ExecuteExecution(ctx, execID)
}

// StartMessageSweeper runs sweepPublishedMessages every second until ctx
// is done. Safe on several app instances: subscriptions and correlations
// are claimed atomically.
func StartMessageSweeper(ctx context.Context, db *sqlx.DB, dispatcher *events.TaskEventDispatcher) {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := sweepPublishedMessages(db, dispatcher); err != nil {
					log.Printf("message sweeper: %v", err)
				}
			}
		}
	}()
}

type keptMessage struct {
	ID             uuid.UUID `db:"id"`
	Name           string    `db:"name"`
	CorrelationKey string    `db:"correlation_key"`
	Variables      []byte    `db:"variables"`
	TenantID       *string   `db:"tenant_id"`
	ProcessKey     string    `db:"process_key"`
}

func (m keptMessage) published() PublishedMessage {
	var vars map[string]interface{}
	_ = json.Unmarshal(m.Variables, &vars)
	msg := PublishedMessage{Name: m.Name, CorrelationKey: m.CorrelationKey, Variables: vars}
	if m.TenantID != nil {
		msg.TenantID = *m.TenantID
	}
	return msg
}

// sweepPublishedMessages drops expired kept messages, correlates the
// others to subscriptions opened since they were published, and starts
// instances of processes they haven't gone to yet (a process deployed
// later, or one whose instance for the same correlation key has ended).
func sweepPublishedMessages(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) error {
	if _, err := db.Exec(`DELETE FROM published_messages WHERE expires_at <= NOW()`); err != nil {
		return err
	}
	var live int
	if err := db.Get(&live, `SELECT COUNT(*) FROM published_messages`); err != nil || live == 0 {
		return err
	}

	ctx := context.Background()
	var toSubscriptions []keptMessage
	if err := db.Select(&toSubscriptions, `
		SELECT DISTINCT ON (m.id, d.process_key)
		       m.id, m.name, m.correlation_key, m.variables, m.tenant_id, d.process_key
		FROM published_messages m
		JOIN event_subscriptions s
		  ON s.event_type = 'message' AND s.event_name = m.name AND s.consumed_at IS NULL
		 AND (COALESCE(s.correlation_key, '') = m.correlation_key
		      OR (m.correlation_key <> '' AND COALESCE(s.correlation_key, '') = '' AND EXISTS (
		            SELECT 1 FROM variables v
		            WHERE v.process_instance_id = s.process_instance_id AND v.data ->> 'correlationKey' = m.correlation_key)))
		JOIN process_instances p ON p.id = s.process_instance_id
		JOIN process_definitions d ON d.id = p.process_definition_id
		WHERE m.expires_at > NOW()
		  AND NOT EXISTS (SELECT 1 FROM published_message_correlations c
		                  WHERE c.published_message_id = m.id AND c.process_key = d.process_key)
		ORDER BY m.id, d.process_key
		LIMIT 200
	`); err != nil {
		return err
	}
	for _, m := range toSubscriptions {
		id := m.ID
		msg := m.published()
		if _, err := correlateToSubscriptions(ctx, db, dispatcher, msg.Name, msg.CorrelationKey, msg.Variables, &id, m.ProcessKey, map[string]bool{}); err != nil {
			log.Printf("published message %s: %v", m.ID, err)
		}
	}

	var toStarts []keptMessage
	if err := db.Select(&toStarts, `
		SELECT DISTINCT m.id, m.name, m.correlation_key, m.variables, m.tenant_id, '' AS process_key
		FROM published_messages m
		JOIN process_definitions d
		  ON d.version = (SELECT max(version) FROM process_definitions WHERE process_key = d.process_key)
		JOIN LATERAL jsonb_each(d.parsed_graph -> 'Nodes') n ON true
		WHERE m.expires_at > NOW()
		  AND n.value ->> 'Type' = 'messageStartEvent'
		  AND n.value ->> 'messageRef' = m.name
		  AND COALESCE(n.value ->> 'parentScope', '') = ''
		  AND NOT EXISTS (SELECT 1 FROM published_message_correlations c
		                  WHERE c.published_message_id = m.id AND c.process_key = d.process_key)
		LIMIT 200
	`); err != nil {
		return err
	}
	for _, m := range toStarts {
		id := m.ID
		if _, err := startByMessage(ctx, db, dispatcher, m.published(), &id, map[string]bool{}); err != nil {
			log.Printf("published message %s: %v", m.ID, err)
		}
	}
	return nil
}
