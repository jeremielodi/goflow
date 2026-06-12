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

// TaskEventDispatcher distribue les événements aux listeners
type TaskEventDispatcher struct {
	listeners []TaskEventListener
}

func NewTaskEventDispatcher() *TaskEventDispatcher {
	return &TaskEventDispatcher{
		listeners: make([]TaskEventListener, 0),
	}
}

func (d *TaskEventDispatcher) Register(listener TaskEventListener) {
	d.listeners = append(d.listeners, listener)
}

func (d *TaskEventDispatcher) Dispatch(ctx context.Context, event *TaskEvent) {
	for _, listener := range d.listeners {
		go func(l TaskEventListener) {
			if err := l.OnTaskEvent(ctx, event); err != nil {
				// Log error but don't block other listeners
				fmt.Printf("Error dispatching event to listener: %v\n", err)
			}
		}(listener)
	}
}
