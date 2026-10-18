package repository

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// TaskEventsChannel is the NOTIFY channel task events are relayed on
// between GoFlow instances sharing a database.
const TaskEventsChannel = "goflow_task_events"

// Postgres caps a NOTIFY payload at 8000 bytes.
const maxNotifyPayload = 7900

type relayedTaskEvent struct {
	Origin string            `json:"origin"`
	Event  *events.TaskEvent `json:"event"`
}

// taskEventFanout publishes this instance's task events with NOTIFY.
type taskEventFanout struct {
	db     *sqlx.DB
	origin string
}

func (f *taskEventFanout) Publish(event *events.TaskEvent) {
	payload, err := json.Marshal(relayedTaskEvent{Origin: f.origin, Event: event})
	if err == nil && len(payload) > maxNotifyPayload {
		// Too big with its variables: relay the event without them.
		trimmed := *event
		trimmed.Variables = nil
		payload, err = json.Marshal(relayedTaskEvent{Origin: f.origin, Event: &trimmed})
	}
	if err != nil || len(payload) > maxNotifyPayload {
		return
	}
	if _, err := f.db.Exec(`SELECT pg_notify($1, $2)`, TaskEventsChannel, string(payload)); err != nil {
		log.Printf("task event fanout: %v", err)
	}
}

// StartTaskEventFanout relays task events between the GoFlow instances
// sharing this database: the events dispatched here are published with
// NOTIFY, and those the other instances publish are delivered to this
// instance's viewers (SSE, WebSocket — see events.TaskEventDispatcher).
// Needed as soon as several instances run behind a load balancer.
func StartTaskEventFanout(ctx context.Context, db *sqlx.DB, connStr string, dispatcher *events.TaskEventDispatcher) error {
	origin := uuid.New().String()
	listener := pq.NewListener(connStr, 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf("task event fanout listener: %v", err)
		}
	})
	if err := listener.Listen(TaskEventsChannel); err != nil {
		listener.Close()
		return err
	}
	dispatcher.SetFanout(&taskEventFanout{db: db, origin: origin})
	go func() {
		defer listener.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case n := <-listener.Notify:
				if n == nil {
					continue // reconnected: events sent meanwhile are lost
				}
				var relayed relayedTaskEvent
				if err := json.Unmarshal([]byte(n.Extra), &relayed); err != nil || relayed.Event == nil || relayed.Origin == origin {
					continue
				}
				dispatcher.DispatchRemote(context.Background(), relayed.Event)
			}
		}
	}()
	return nil
}
