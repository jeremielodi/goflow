// internal/worker/pool.go
package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jmoiron/sqlx"
)

// Job represents a unit of work
type Job struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"`
	ProcessKey  string                 `json:"processKey,omitempty"`
	ExecutionID uuid.UUID              `json:"executionId,omitempty"`
	TaskID      uuid.UUID              `json:"taskId,omitempty"`
	Variables   map[string]interface{} `json:"variables,omitempty"`
	CreatedAt   time.Time              `json:"createdAt"`
	RetryCount  int                    `json:"retryCount"`
	MaxRetries  int                    `json:"maxRetries"`
}

// JobResult represents the result of processing a job
type JobResult struct {
	JobID       string
	Success     bool
	Error       error
	ProcessedAt time.Time
}

// WorkerPool manages a pool of workers
type WorkerPool struct {
	name         string
	workerCount  int
	jobQueue     chan Job
	resultQueue  chan JobResult
	wg           sync.WaitGroup
	stopChan     chan struct{}
	db           *sqlx.DB
	processCache sync.Map
	metrics      *PoolMetrics
	dispatcher   *events.TaskEventDispatcher
}

// PoolMetrics tracks worker pool performance
type PoolMetrics struct {
	mu            sync.RWMutex
	JobsSubmitted int64
	JobsProcessed int64
	JobsFailed    int64
	JobsRetried   int64
	AverageTime   time.Duration
	QueueLength   int
	ActiveWorkers int
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(db *sqlx.DB, name string, workerCount int, queueSize int, dispatcher *events.TaskEventDispatcher) *WorkerPool {
	if workerCount <= 0 {
		workerCount = 10
	}
	if queueSize <= 0 {
		queueSize = 1000
	}

	return &WorkerPool{
		name:        name,
		workerCount: workerCount,
		jobQueue:    make(chan Job, queueSize),
		resultQueue: make(chan JobResult, queueSize),
		stopChan:    make(chan struct{}),
		db:          db,
		metrics:     &PoolMetrics{},
		dispatcher:  dispatcher,
	}
}

// Start initializes and starts the worker pool
func (p *WorkerPool) Start() {
	log.Printf("🚀 Starting worker pool '%s' with %d workers, queue size: %d", p.name, p.workerCount, cap(p.jobQueue))

	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	go p.collectMetrics()
}

// Stop gracefully stops the worker pool
func (p *WorkerPool) Stop() {
	log.Printf("🛑 Stopping worker pool '%s'...", p.name)
	close(p.stopChan)
	close(p.jobQueue)
	p.wg.Wait()
	close(p.resultQueue)
	log.Printf("✅ Worker pool '%s' stopped", p.name)
}

// Submit submits a job to the worker pool
func (p *WorkerPool) Submit(job Job) error {
	job.ID = uuid.New().String()
	job.CreatedAt = time.Now()
	job.MaxRetries = 3

	select {
	case p.jobQueue <- job:
		p.updateMetricsSubmitted()
		return nil
	default:
		return fmt.Errorf("job queue is full, please try again later")
	}
}

// worker processes jobs from the queue
func (p *WorkerPool) worker(id int) {
	defer p.wg.Done()
	// log.Printf("👷 Worker %d started in pool '%s'", id, p.name)

	for job := range p.jobQueue {
		p.updateActiveWorker(true)
		startTime := time.Now()

		result := p.processJob(job)
		result.JobID = job.ID
		result.ProcessedAt = time.Now()

		duration := time.Since(startTime)

		if result.Success {
			p.updateMetricsProcessed(true, duration)
		} else {
			p.updateMetricsProcessed(false, duration)
			log.Printf("❌ Job %s failed: %v", job.ID, result.Error)
		}

		if !result.Success && job.RetryCount < job.MaxRetries {
			job.RetryCount++
			log.Printf("🔄 Retrying job %s (attempt %d/%d)", job.ID, job.RetryCount, job.MaxRetries)
			p.updateMetricsRetried()

			go func() {
				time.Sleep(time.Duration(job.RetryCount) * time.Second)
				select {
				case p.jobQueue <- job:
					p.updateMetricsSubmitted()
				default:
					log.Printf("❌ Failed to requeue job %s", job.ID)
				}
			}()
		}

		select {
		case p.resultQueue <- result:
		default:
		}

		p.updateActiveWorker(false)
	}

	// log.Printf("👷 Worker %d stopped in pool '%s'", id, p.name)
}

// processJob processes a single job
func (p *WorkerPool) processJob(job Job) JobResult {
	switch job.Type {
	case "start_process":
		return p.processStartProcess(job)
	default:
		return JobResult{
			Success: false,
			Error:   fmt.Errorf("unknown job type: %s", job.Type),
		}
	}
}

// processStartProcess handles process start jobs
func (p *WorkerPool) processStartProcess(job Job) JobResult {
	engineRepo := repository.NewEngineRepository(p.db, p.dispatcher)

	// Get process definition
	procDef, err := engineRepo.FindLatestProcessDefinitionByKey(job.ProcessKey)
	if err != nil {
		log.Printf("❌ Failed to find process definition: %v", err)
		return JobResult{Success: false, Error: fmt.Errorf("process definition not found: %w", err)}
	}

	// Build graph from BPMN
	graph, err := engine.BuildGraphForProcess([]byte(procDef.BpmnXML), job.ProcessKey)
	if err != nil {
		log.Printf("❌ Failed to build graph: %v", err)
		return JobResult{Success: false, Error: fmt.Errorf("failed to build graph: %w", err)}
	}

	// Find start node
	var startNodeID string
	for _, node := range graph.Nodes {
		if node.Type == engine.StartEventType {
			startNodeID = node.ID
			break
		}
	}
	if startNodeID == "" {
		log.Printf("❌ No start event found for process %s", job.ProcessKey)
		return JobResult{Success: false, Error: fmt.Errorf("no start event found")}
	}

	// Start transaction
	tx, err := p.db.Beginx()
	if err != nil {
		log.Printf("❌ Failed to begin transaction: %v", err)
		return JobResult{Success: false, Error: err}
	}
	defer tx.Rollback()

	// Create process instance
	instanceID := uuid.New()
	now := time.Now()
	err = engineRepo.CreateProcessInstanceTx(tx, instanceID, procDef.ID, now)
	if err != nil {
		log.Printf("❌ Failed to create process instance: %v", err)
		return JobResult{Success: false, Error: fmt.Errorf("failed to create process instance: %w", err)}
	}

	// Save variables
	if len(job.Variables) > 0 {
		err = engineRepo.CreateVariablesTx(tx, instanceID, job.Variables, now)
		if err != nil {
			log.Printf("❌ Failed to save variables: %v", err)
			return JobResult{Success: false, Error: fmt.Errorf("failed to save variables: %w", err)}
		}
	}

	// Create initial execution
	execID := uuid.New()
	err = engineRepo.CreateExecutionTx2(tx, execID, instanceID, startNodeID, now)
	if err != nil {
		log.Printf("❌ Failed to create execution: %v", err)
		return JobResult{Success: false, Error: fmt.Errorf("failed to create execution: %w", err)}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("❌ Failed to commit transaction: %v", err)
		return JobResult{Success: false, Error: fmt.Errorf("failed to commit: %w", err)}
	}

	// Execute asynchronously
	rt := runtime.NewRuntime(graph, p.db, p.dispatcher)
	go func() {
		if err := rt.ExecuteExecution(context.Background(), execID); err != nil {
			log.Printf("❌ Execution error for instance %s: %v", instanceID, err)
		} else {
			log.Printf("✅ Process instance %s started successfully", instanceID)
		}
	}()

	log.Printf("✅ Job %s submitted successfully for process %s", job.ID, job.ProcessKey)
	return JobResult{Success: true, Error: nil}
}

// updateMetricsProcessed updates processed metrics
func (p *WorkerPool) updateMetricsProcessed(success bool, duration time.Duration) {
	p.metrics.mu.Lock()
	defer p.metrics.mu.Unlock()

	p.metrics.JobsProcessed++

	if !success {
		p.metrics.JobsFailed++
	}

	if p.metrics.AverageTime == 0 {
		p.metrics.AverageTime = duration
	} else {
		p.metrics.AverageTime = (p.metrics.AverageTime + duration) / 2
	}
}

// updateMetricsSubmitted updates submitted metrics
func (p *WorkerPool) updateMetricsSubmitted() {
	p.metrics.mu.Lock()
	defer p.metrics.mu.Unlock()
	p.metrics.JobsSubmitted++
}

// updateMetricsRetried updates retried metrics
func (p *WorkerPool) updateMetricsRetried() {
	p.metrics.mu.Lock()
	defer p.metrics.mu.Unlock()
	p.metrics.JobsRetried++
}

// updateActiveWorker updates active worker count
func (p *WorkerPool) updateActiveWorker(active bool) {
	p.metrics.mu.Lock()
	defer p.metrics.mu.Unlock()
	if active {
		p.metrics.ActiveWorkers++
	} else {
		p.metrics.ActiveWorkers--
	}
	p.metrics.QueueLength = len(p.jobQueue)
}

// collectMetrics collects pool metrics
func (p *WorkerPool) collectMetrics() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopChan:
			return
		case <-ticker.C:
			p.logMetrics()
		}
	}
}

// logMetrics logs current metrics
func (p *WorkerPool) logMetrics() {
	p.metrics.mu.RLock()
	defer p.metrics.mu.RUnlock()
	// // log.Printf("📊 Worker Pool '%s': Submitted=%d, Processed=%d, Failed=%d, Retried=%d, Queue=%d, Workers=%d",
	// 	p.name, p.metrics.JobsSubmitted, p.metrics.JobsProcessed, p.metrics.JobsFailed,
	// 	p.metrics.JobsRetried, p.metrics.QueueLength, p.metrics.ActiveWorkers)
}

// GetMetrics returns current metrics
func (p *WorkerPool) GetMetrics() PoolMetrics {
	p.metrics.mu.RLock()
	defer p.metrics.mu.RUnlock()
	return *p.metrics
}

// GetQueueLength returns current queue length
func (p *WorkerPool) GetQueueLength() int {
	p.metrics.mu.RLock()
	defer p.metrics.mu.RUnlock()
	return p.metrics.QueueLength
}

// IsHealthy returns true if pool is healthy
func (p *WorkerPool) IsHealthy() bool {
	p.metrics.mu.RLock()
	defer p.metrics.mu.RUnlock()
	queueUsage := float64(p.metrics.QueueLength) / 1000.0
	return queueUsage < 0.8
}
