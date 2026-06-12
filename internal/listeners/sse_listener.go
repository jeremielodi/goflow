// internal/listeners/sse_listener.go
package listeners

import (
	"bufio"
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/events"
)

type SSEClient struct {
	ID         string
	Channel    chan []byte
	TaskFilter string
}

type SSEListener struct {
	clients   map[string]*SSEClient
	clientsMu sync.RWMutex
}

func NewSSEListener() *SSEListener {
	return &SSEListener{
		clients: make(map[string]*SSEClient),
	}
}

func (l *SSEListener) AddClient(client *SSEClient) {
	l.clientsMu.Lock()
	defer l.clientsMu.Unlock()
	l.clients[client.ID] = client
}

func (l *SSEListener) RemoveClient(clientID string) {
	l.clientsMu.Lock()
	defer l.clientsMu.Unlock()
	if client, exists := l.clients[clientID]; exists {
		close(client.Channel)
		delete(l.clients, clientID)
	}
}

func (l *SSEListener) OnTaskEvent(ctx context.Context, event *events.TaskEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	l.clientsMu.RLock()
	defer l.clientsMu.RUnlock()

	for _, client := range l.clients {
		if client.TaskFilter != "*" && client.TaskFilter != event.TaskID {
			continue
		}

		select {
		case client.Channel <- payload:
		default:
		}
	}

	return nil
}

// SSEHandler handles SSE connections
func (l *SSEListener) SSEHandler(c *fiber.Ctx) error {
	clientID := c.Query("clientId")
	if clientID == "" {
		clientID = uuid.New().String()
	}

	taskFilter := c.Query("taskFilter", "*")

	// Set SSE headers BEFORE SetBodyStreamWriter
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")
	c.Set("X-Accel-Buffering", "no") // Disable nginx buffering

	// Capture the done channel BEFORE entering SetBodyStreamWriter
	// fasthttp's RequestCtx implements context.Context
	ctxDone := c.Context().Done()

	// Use SetBodyStreamWriter for proper streaming with flush support
	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		client := &SSEClient{
			ID:         clientID,
			Channel:    make(chan []byte, 100),
			TaskFilter: taskFilter,
		}

		l.AddClient(client)
		defer l.RemoveClient(client.ID)

		// Helper to write and flush SSE events
		writeEvent := func(eventName, data string) bool {
			_, err := w.WriteString("event: " + eventName + "\n")
			if err != nil {
				return false
			}
			_, err = w.WriteString("data: " + data + "\n\n")
			if err != nil {
				return false
			}
			err = w.Flush()
			return err == nil
		}

		// Send initial connection message immediately
		if !writeEvent("connected", clientID) {
			return
		}

		// Create a ticker for keep-alive ping
		pingTicker := time.NewTicker(30 * time.Second)
		defer pingTicker.Stop()

		// Listen for events
		for {
			select {
			case message, ok := <-client.Channel:
				if !ok {
					return // Channel closed
				}
				if !writeEvent("task", string(message)) {
					return // Client disconnected
				}

			case <-pingTicker.C:
				if !writeEvent("ping", "keep-alive") {
					return // Client disconnected
				}

			case <-ctxDone:
				return // Request cancelled
			}
		}
	})

	return nil
}
