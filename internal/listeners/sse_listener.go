// internal/listeners/sse_listener.go
package listeners

import (
	"bufio"
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/events"
)

type SSEClient struct {
	ID                 string
	Channel            chan []byte
	TaskFilter         string   // Filter by task ID ("*" for all)
	ProcessKeys        []string // Filter by process keys (empty = all)
	ExcludeProcessKeys []string // Exclude specific process keys
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

// shouldFilterEvent checks if the event should be sent to the client
func (l *SSEListener) shouldFilterEvent(client *SSEClient, event *events.TaskEvent) bool {
	// Check task filter
	if client.TaskFilter != "*" && client.TaskFilter != event.TaskID {
		return true // Filter out
	}

	// Check process keys include filter
	if len(client.ProcessKeys) > 0 {
		found := false
		for _, pk := range client.ProcessKeys {
			if event.ProcessKey == pk {
				found = true
				break
			}
		}
		if !found {
			return true // Filter out - process key not in allowed list
		}
	}

	// Check process keys exclude filter
	for _, epk := range client.ExcludeProcessKeys {
		if event.ProcessKey == epk {
			return true // Filter out - process key is excluded
		}
	}

	return false // Keep the event
}

func (l *SSEListener) OnTaskEvent(ctx context.Context, event *events.TaskEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	l.clientsMu.RLock()
	defer l.clientsMu.RUnlock()

	for _, client := range l.clients {
		if l.shouldFilterEvent(client, event) {
			continue
		}

		select {
		case client.Channel <- payload:
		default:
			// Channel full, skip
		}
	}

	return nil
}

// parseCommaSeparated parses a comma-separated string into a slice of strings
func parseCommaSeparated(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// getMultipleQueryValues gets all values for a query parameter
func getMultipleQueryValues(c *fiber.Ctx, key string) []string {
	values := make([]string, 0)

	// Get raw query string and parse manually for multiple values
	rawQuery := string(c.Context().QueryArgs().QueryString())
	if rawQuery == "" {
		return values
	}

	// Parse query string
	pairs := strings.Split(rawQuery, "&")
	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			k, err := url.QueryUnescape(kv[0])
			if err != nil {
				k = kv[0]
			}
			if k == key {
				v, err := url.QueryUnescape(kv[1])
				if err != nil {
					v = kv[1]
				}
				values = append(values, v)
			}
		}
	}

	return values
}

// SSEHandler handles SSE connections
func (l *SSEListener) SSEHandler(c *fiber.Ctx) error {

	c.Set("X-Accel-Buffering", "no")
	c.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	c.Set("Content-Type", "text/event-stream; charset=utf-8")
	c.Set("Connection", "keep-alive")
	c.Set("Keep-Alive", "timeout=60")

	// Timeout de connexion (5 minutes)
	_, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()

	clientID := c.Query("clientId")
	if clientID == "" {
		clientID = uuid.New().String()
	}

	taskFilter := c.Query("taskFilter", "*")

	// Parse process keys from query parameters
	var processKeys []string

	// Check for comma-separated processKeys parameter
	if processKeysParam := c.Query("processKeys"); processKeysParam != "" {
		processKeys = parseCommaSeparated(processKeysParam)
	} else {
		// Check for multiple processKey parameters
		processKeys = getMultipleQueryValues(c, "processKey")
	}

	// Parse exclude process keys
	var excludeProcessKeys []string
	if excludeParam := c.Query("excludeProcessKeys"); excludeParam != "" {
		excludeProcessKeys = parseCommaSeparated(excludeParam)
	} else {
		excludeProcessKeys = getMultipleQueryValues(c, "excludeProcessKey")
	}
	// Set SSE headers BEFORE SetBodyStreamWriter
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")
	c.Set("X-Accel-Buffering", "no") // Disable nginx buffering

	// Capture the done channel BEFORE entering SetBodyStreamWriter
	ctxDone := c.Context().Done()

	// Use SetBodyStreamWriter for proper streaming with flush support
	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		client := &SSEClient{
			ID:                 clientID,
			Channel:            make(chan []byte, 100),
			TaskFilter:         taskFilter,
			ProcessKeys:        processKeys,
			ExcludeProcessKeys: excludeProcessKeys,
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

		// Send initial connection message with filter info
		connInfo := map[string]interface{}{
			"clientId":           clientID,
			"taskFilter":         taskFilter,
			"processKeys":        processKeys,
			"excludeProcessKeys": excludeProcessKeys,
		}
		connInfoJSON, _ := json.Marshal(connInfo)
		if !writeEvent("connected", string(connInfoJSON)) {
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
