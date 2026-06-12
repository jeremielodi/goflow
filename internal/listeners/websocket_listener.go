// internal/listeners/websocket_listener.go
package listeners

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/events"
)

type WebSocketClient struct {
	ID         string
	Conn       *websocket.Conn
	TaskFilter string
	Send       chan []byte
}

type WebSocketListener struct {
	clients   map[string]*WebSocketClient
	clientsMu sync.RWMutex
}

func NewWebSocketListener() *WebSocketListener {
	return &WebSocketListener{
		clients: make(map[string]*WebSocketClient),
	}
}

func (l *WebSocketListener) AddClient(client *WebSocketClient) {
	l.clientsMu.Lock()
	defer l.clientsMu.Unlock()
	l.clients[client.ID] = client
	go l.handleClient(client)
}

func (l *WebSocketListener) RemoveClient(clientID string) {
	l.clientsMu.Lock()
	defer l.clientsMu.Unlock()
	if client, exists := l.clients[clientID]; exists {
		close(client.Send)
		delete(l.clients, clientID)
	}
}

func (l *WebSocketListener) handleClient(client *WebSocketClient) {
	defer func() {
		l.RemoveClient(client.ID)
		client.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-client.Send:
			if !ok {
				return
			}
			if err := client.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}
		}
	}
}

func (l *WebSocketListener) OnTaskEvent(ctx context.Context, event *events.TaskEvent) error {
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
		case client.Send <- payload:
		default:
		}
	}

	return nil
}

// WebSocketHandler gère les connexions WebSocket
func (l *WebSocketListener) WebSocketHandler(c *fiber.Ctx) error {
	if websocket.IsWebSocketUpgrade(c) {
		return c.Next()
	}
	return fiber.ErrUpgradeRequired
}

func (l *WebSocketListener) HandleWebSocket(c *websocket.Conn) {
	clientID := c.Query("clientId")
	if clientID == "" {
		clientID = uuid.New().String()
	}

	taskFilter := c.Query("taskFilter", "*")

	client := &WebSocketClient{
		ID:         clientID,
		Conn:       c,
		TaskFilter: taskFilter,
		Send:       make(chan []byte, 100),
	}

	l.AddClient(client)

	// Send initial message
	client.Send <- []byte(`{"type":"connected","clientId":"` + clientID + `"}`)

	// Keep connection alive
	<-make(chan struct{})
}
