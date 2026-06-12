// internal/listeners/webhook_listener.go
package listeners

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jeremielodi/goflow/internal/events"
)

type WebhookConfig struct {
	URL        string
	Headers    map[string]string
	Timeout    time.Duration
	RetryCount int
	RetryDelay time.Duration
}

type WebhookListener struct {
	configs map[string][]WebhookConfig // taskId -> webhook configs
	client  *http.Client
}

func NewWebhookListener() *WebhookListener {
	return &WebhookListener{
		configs: make(map[string][]WebhookConfig),
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// RegisterWebhook enregistre un webhook pour une tâche spécifique
func (l *WebhookListener) RegisterWebhook(taskID string, config WebhookConfig) {
	if _, exists := l.configs[taskID]; !exists {
		l.configs[taskID] = make([]WebhookConfig, 0)
	}
	l.configs[taskID] = append(l.configs[taskID], config)
}

// RegisterGlobalWebhook enregistre un webhook pour toutes les tâches
func (l *WebhookListener) RegisterGlobalWebhook(config WebhookConfig) {
	l.RegisterWebhook("*", config)
}

func (l *WebhookListener) OnTaskEvent(ctx context.Context, event *events.TaskEvent) error {
	// Find webhooks for this specific task and global webhooks
	webhooks := make([]WebhookConfig, 0)

	// Add task-specific webhooks
	if configs, exists := l.configs[event.TaskID]; exists {
		webhooks = append(webhooks, configs...)
	}

	// Add global webhooks
	if globalConfigs, exists := l.configs["*"]; exists {
		webhooks = append(webhooks, globalConfigs...)
	}

	// Send webhook for each configuration
	for _, config := range webhooks {
		go l.sendWebhook(config, event)
	}

	return nil
}

func (l *WebhookListener) sendWebhook(config WebhookConfig, event *events.TaskEvent) {
	payload, err := json.Marshal(event)
	if err != nil {
		fmt.Printf("Error marshaling webhook payload: %v\n", err)
		return
	}

	req, err := http.NewRequest("POST", config.URL, bytes.NewBuffer(payload))
	if err != nil {
		fmt.Printf("Error creating webhook request: %v\n", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	for key, value := range config.Headers {
		req.Header.Set(key, value)
	}

	// Retry logic
	var resp *http.Response
	for i := 0; i <= config.RetryCount; i++ {
		resp, err = l.client.Do(req)
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			resp.Body.Close()
			return
		}

		if resp != nil {
			resp.Body.Close()
		}

		if i < config.RetryCount {
			time.Sleep(config.RetryDelay)
		}
	}

	if err != nil {
		fmt.Printf("Webhook failed after %d retries: %v\n", config.RetryCount, err)
	}
}
