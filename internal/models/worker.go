package models

import "time"

type WorkerPool interface {
	GetMetrics() WorkerMetrics
	IsHealthy() bool
}

type WorkerMetrics struct {
	JobsSubmitted int64
	JobsProcessed int64
	JobsFailed    int64
	JobsRetried   int64
	AverageTime   time.Duration
	QueueLength   int
	ActiveWorkers int
}
