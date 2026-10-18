// internal/events/listener.go
package events

import (
	"context"
	"fmt"
)

// TaskEventListener interface pour les listeners de tâches
type TaskEventListener interface {
	OnTaskEvent(ctx context.Context, event *TaskEvent) error
}

// Fanout relays the events a GoFlow instance dispatches to the other
// instances sharing its database (see repository.StartTaskEventFanout).
type Fanout interface {
	Publish(event *TaskEvent)
}

// TaskEventDispatcher distribue les événements aux listeners.
//
// With several GoFlow instances behind a load balancer, a client watching
// task events (SSE, WebSocket) is connected to one of them while the event
// may happen on another: viewers (RegisterViewer) also get the events the
// other instances relay through the fanout. Other listeners (Register —
// webhooks) only get the events of their own instance, so each fires once.
type TaskEventDispatcher struct {
	listeners []TaskEventListener
	viewers   []TaskEventListener
	fanout    Fanout
}

func NewTaskEventDispatcher() *TaskEventDispatcher {
	return &TaskEventDispatcher{
		listeners: make([]TaskEventListener, 0),
	}
}

// Register adds a listener for the events of this instance only.
func (d *TaskEventDispatcher) Register(listener TaskEventListener) {
	d.listeners = append(d.listeners, listener)
}

// RegisterViewer adds a listener for the events of every instance.
func (d *TaskEventDispatcher) RegisterViewer(listener TaskEventListener) {
	d.viewers = append(d.viewers, listener)
}

// SetFanout relays this instance's events to the other instances.
func (d *TaskEventDispatcher) SetFanout(f Fanout) {
	d.fanout = f
}

func (d *TaskEventDispatcher) Dispatch(ctx context.Context, event *TaskEvent) {
	d.deliver(ctx, event, d.listeners)
	d.deliver(ctx, event, d.viewers)
	if d.fanout != nil {
		d.fanout.Publish(event)
	}
}

// DispatchRemote delivers an event another instance relayed: viewers only.
func (d *TaskEventDispatcher) DispatchRemote(ctx context.Context, event *TaskEvent) {
	d.deliver(ctx, event, d.viewers)
}

func (d *TaskEventDispatcher) deliver(ctx context.Context, event *TaskEvent, to []TaskEventListener) {
	for _, listener := range to {
		go func(l TaskEventListener) {
			if err := l.OnTaskEvent(ctx, event); err != nil {
				// Log error but don't block other listeners
				fmt.Printf("Error dispatching event to listener: %v\n", err)
			}
		}(listener)
	}
}
