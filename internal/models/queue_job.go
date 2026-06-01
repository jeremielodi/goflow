// internal/models/queue_job.go
package models

import (
	"fmt"
	"time"
)

// QueueJobStatus defines the status of a queue job
type QueueJobStatus string

const (
	QueueJobStatusPending    QueueJobStatus = "pending"
	QueueJobStatusProcessing QueueJobStatus = "processing"
	QueueJobStatusCompleted  QueueJobStatus = "completed"
	QueueJobStatusFailed     QueueJobStatus = "failed"
	QueueJobStatusDead       QueueJobStatus = "dead"
	QueueJobStatusScheduled  QueueJobStatus = "scheduled"
	QueueJobStatusCancelled  QueueJobStatus = "cancelled"
	QueueJobStatusPaused     QueueJobStatus = "paused"
)

// QueueJobPriority defines job priority levels
type QueueJobPriority int

const (
	PriorityLow    QueueJobPriority = 1
	PriorityNormal QueueJobPriority = 5
	PriorityHigh   QueueJobPriority = 8
	PriorityUrgent QueueJobPriority = 10
)

// QueueJobPayload represents the payload data for a job
type QueueJobPayload struct {
	// Type identifies the kind of payload (e.g., "email", "webhook", "process")
	Type string `json:"type"`

	// Data contains the actual payload data (generic)
	Data interface{} `json:"data,omitempty"`

	// ProcessKey is the BPMN process key/ID to start
	ProcessKey string `json:"process_key,omitempty"`

	// Variables are the process variables to pass
	Variables map[string]interface{} `json:"variables,omitempty"`

	// Version for schema evolution
	Version int `json:"version,omitempty"`

	// ContentType specifies the encoding (json, protobuf, etc.)
	ContentType string `json:"content_type,omitempty"`

	// Size in bytes (for monitoring)
	Size int64 `json:"size,omitempty"`

	// Checksum for data integrity
	Checksum string `json:"checksum,omitempty"`

	// Compression method used (none, gzip, zstd)
	Compression string `json:"compression,omitempty"`
}

// QueueJob represents a job in the queue system
type QueueJob struct {
	// Core fields
	ID       string           `json:"id"`
	JobType  string           `json:"job_type"`
	Status   QueueJobStatus   `json:"status"`
	Payload  QueueJobPayload  `json:"payload"`
	Priority QueueJobPriority `json:"priority"`

	// Timestamps
	CreatedAt   time.Time  `json:"created_at"`
	ScheduledAt time.Time  `json:"scheduled_at"`
	ProcessedAt *time.Time `json:"processed_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	FailedAt    *time.Time `json:"failed_at,omitempty"`
	CancelledAt *time.Time `json:"cancelled_at,omitempty"`

	// Retry handling
	RetryCount  int        `json:"retry_count"`
	MaxRetries  int        `json:"max_retries"`
	NextRetryAt *time.Time `json:"next_retry_at,omitempty"`
	LastError   *string    `json:"last_error,omitempty"`

	// Timeout in seconds (0 = no timeout)
	TimeoutSeconds int `json:"timeout_seconds"`

	// Rate limiting
	RateLimitKey    string `json:"rate_limit_key,omitempty"`
	RateLimitCount  int    `json:"rate_limit_count,omitempty"`
	RateLimitWindow int    `json:"rate_limit_window,omitempty"`

	// Metadata
	Metadata map[string]interface{} `json:"metadata,omitempty"`

	// Tracing
	TraceID string `json:"trace_id,omitempty"`
	SpanID  string `json:"span_id,omitempty"`

	// Ownership (which worker is processing)
	WorkerID   string     `json:"worker_id,omitempty"`
	LeaseUntil *time.Time `json:"lease_until,omitempty"`
}

// NewJob creates a new job with default values
func NewJob(jobType string, payload interface{}) *QueueJob {
	return &QueueJob{
		ID:       "",
		JobType:  jobType,
		Status:   QueueJobStatusPending,
		Priority: PriorityNormal,
		Payload: QueueJobPayload{
			Type:        "generic",
			Data:        payload,
			Version:     1,
			ContentType: "application/json",
		},
		MaxRetries:     3,
		TimeoutSeconds: 30,
		Metadata:       make(map[string]interface{}),
		CreatedAt:      time.Now(),
		ScheduledAt:    time.Now(),
	}
}

// NewProcessJob creates a job for starting a BPMN process
func NewProcessJob(processKey string, variables map[string]interface{}) *QueueJob {
	return &QueueJob{
		ID:       "",
		JobType:  "process.start",
		Status:   QueueJobStatusPending,
		Priority: PriorityNormal,
		Payload: QueueJobPayload{
			Type:       "process",
			ProcessKey: processKey,
			Variables:  variables,
			Version:    1,
		},
		MaxRetries:     3,
		TimeoutSeconds: 60,
		Metadata:       make(map[string]interface{}),
		CreatedAt:      time.Now(),
		ScheduledAt:    time.Now(),
	}
}

// NewJobWithPayload creates a job with a fully specified payload
func NewJobWithPayload(jobType string, payload QueueJobPayload) *QueueJob {
	return &QueueJob{
		ID:             "",
		JobType:        jobType,
		Status:         QueueJobStatusPending,
		Priority:       PriorityNormal,
		Payload:        payload,
		MaxRetries:     3,
		TimeoutSeconds: 30,
		Metadata:       make(map[string]interface{}),
		CreatedAt:      time.Now(),
		ScheduledAt:    time.Now(),
	}
}

// EmailPayload represents email job data
type EmailPayload struct {
	To          []string `json:"to"`
	Cc          []string `json:"cc,omitempty"`
	Bcc         []string `json:"bcc,omitempty"`
	Subject     string   `json:"subject"`
	Body        string   `json:"body"`
	HTML        bool     `json:"html"`
	From        string   `json:"from,omitempty"`
	ReplyTo     string   `json:"reply_to,omitempty"`
	Attachments []struct {
		Name    string `json:"name"`
		Content string `json:"content"` // base64 encoded
		Type    string `json:"type"`
	} `json:"attachments,omitempty"`
}

// NewEmailJob creates a job for sending emails
func NewEmailJob(email EmailPayload) *QueueJob {
	job := NewJob("email.send", email)
	job.Payload.Type = "email"
	job.Payload.ContentType = "application/json"
	return job
}

// WebhookPayload represents webhook job data
type WebhookPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"` // GET, POST, PUT, DELETE
	Headers map[string]string `json:"headers,omitempty"`
	Body    interface{}       `json:"body,omitempty"`
	Retry   struct {
		Count    int `json:"count"`
		Interval int `json:"interval"` // seconds
	} `json:"retry,omitempty"`
}

// NewWebhookJob creates a job for sending webhooks
func NewWebhookJob(webhook WebhookPayload) *QueueJob {
	job := NewJob("webhook.send", webhook)
	job.Payload.Type = "webhook"
	job.Payload.ContentType = "application/json"
	return job
}

// DataProcessingPayload represents data processing job data
type DataProcessingPayload struct {
	Operation string                 `json:"operation"` // transform, validate, enrich, aggregate
	Source    string                 `json:"source"`    // s3://, gcs://, etc.
	Target    string                 `json:"target,omitempty"`
	Filters   map[string]interface{} `json:"filters,omitempty"`
	Fields    []string               `json:"fields,omitempty"`
	BatchSize int                    `json:"batch_size,omitempty"`
}

// NewDataProcessingJob creates a job for data processing
func NewDataProcessingJob(processing DataProcessingPayload) *QueueJob {
	job := NewJob("data.process", processing)
	job.Payload.Type = "data_processing"
	job.Payload.ContentType = "application/json"
	return job
}

// FilePayload represents file operation job data
type FilePayload struct {
	Operation   string            `json:"operation"` // upload, download, convert, compress
	Source      string            `json:"source"`
	Destination string            `json:"destination"`
	Format      string            `json:"format,omitempty"`
	Quality     int               `json:"quality,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// NewFileJob creates a job for file operations
func NewFileJob(file FilePayload) *QueueJob {
	job := NewJob("file.operation", file)
	job.Payload.Type = "file"
	job.Payload.ContentType = "application/json"
	return job
}

// NotificationPayload represents push notification job data
type NotificationPayload struct {
	Title    string            `json:"title"`
	Body     string            `json:"body"`
	UserIDs  []string          `json:"user_ids"`
	Platform []string          `json:"platform"` // ios, android, web
	Data     map[string]string `json:"data,omitempty"`
	Image    string            `json:"image,omitempty"`
	Sound    string            `json:"sound,omitempty"`
	Badge    int               `json:"badge,omitempty"`
}

// NewNotificationJob creates a job for sending push notifications
func NewNotificationJob(notification NotificationPayload) *QueueJob {
	job := NewJob("notification.push", notification)
	job.Payload.Type = "notification"
	job.Payload.ContentType = "application/json"
	return job
}

// Validate checks if the job has all required fields
func (j *QueueJob) Validate() error {
	if j.JobType == "" {
		return fmt.Errorf("job type is required")
	}

	if j.Payload.ProcessKey == "" && j.Payload.Data == nil {
		return fmt.Errorf("job payload requires either process_key or data")
	}

	if j.MaxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative")
	}

	if j.TimeoutSeconds < 0 {
		return fmt.Errorf("timeout seconds cannot be negative")
	}

	if j.Priority < PriorityLow || j.Priority > PriorityUrgent {
		return fmt.Errorf("invalid priority: %d", j.Priority)
	}

	return nil
}

// IsRetryable returns whether the job can be retried
func (j *QueueJob) IsRetryable() bool {
	return j.RetryCount < j.MaxRetries
}

// IsTimedOut returns whether the job has timed out
func (j *QueueJob) IsTimedOut() bool {
	if j.TimeoutSeconds == 0 {
		return false
	}
	if j.ProcessedAt == nil {
		return false
	}
	return time.Since(*j.ProcessedAt) > time.Duration(j.TimeoutSeconds)*time.Second
}

// GetBackoffDuration calculates the next retry backoff duration
func (j *QueueJob) GetBackoffDuration(baseDelay time.Duration, maxDelay time.Duration) time.Duration {
	if j.RetryCount == 0 {
		return 0
	}

	// Exponential backoff: 1s, 2s, 4s, 8s, 16s, etc.
	delay := baseDelay * time.Duration(1<<uint(j.RetryCount-1))
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}
