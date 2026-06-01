package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/api"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/internal/worker"
	"github.com/jeremielodi/goflow/pkg/authentication"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jeremielodi/goflow/pkg/database"
	"github.com/jeremielodi/goflow/pkg/middleware"
	"github.com/jmoiron/sqlx"
)

func permissions(db *sqlx.DB, code string) fiber.Handler {
	return middleware.PermissionMiddleware(db, code)
}

func main() {

	time.Local = time.UTC
	// Get the root directory path of the application
	rootDirPath, pathErr := os.Getwd()
	if pathErr != nil {
		log.Fatalln(pathErr)
	}

	// Initialize utility functions using the root directory
	var util = *common.NewUtil(&rootDirPath)

	// Initialize a new Postgres database connection
	db, err := database.NewPostgres(util)
	if err != nil {
		log.Fatalln("Postgres: ", err)
	}
	resumer := runtime.CreateResumer(db)

	// ============================================================
	// CREATE WORKER POOL
	// ============================================================

	// Create worker pool for processing requests asynchronously
	// Parameters: db, pool name, number of workers, queue size
	workerPool := worker.NewWorkerPool(db, "GoFlow-Main", 25, 1000)
	workerPool.Start()
	defer workerPool.Stop()

	// Optional: Monitor worker pool health
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if !workerPool.IsHealthy() {
				log.Printf("⚠️ Worker pool is under heavy load! Queue length: %d", workerPool.GetQueueLength())
			}
			// Log metrics periodically
			metrics := workerPool.GetMetrics()
			log.Printf("📊 Worker Pool Metrics: Submitted=%d, Processed=%d, Failed=%d, Queue=%d, Workers=%d",
				metrics.JobsSubmitted, metrics.JobsProcessed, metrics.JobsFailed,
				metrics.QueueLength, metrics.ActiveWorkers)
		}
	}()

	taskService := service.NewTaskService(db)
	taskService.SetResumeExecutor(func(ctx context.Context, graph *engine.ProcessGraph, execID uuid.UUID) error {
		rt := runtime.NewRuntime(graph, db)
		return rt.ExecuteExecution(ctx, execID)
	})

	// Pass worker pool to controllers that need it
	processDefinitionCtrl := api.NewProcessDefinitionController(db, &rootDirPath)
	processInstanceCtrl := api.NewProcessInstanceController(db, taskService, workerPool) // Pass worker pool
	taskCtrl := api.NewTaskController(db, &rootDirPath)

	externalTaskCtrl := api.NewExternalTaskController(db)
	auditController := api.NewAuditController(db)

	multiInstanceController := api.NewMultiInstanceController(db)
	// timer-----------------------------------------

	userController := api.NewUserController(db, &authentication.JWTService{})

	timerController := api.NewTimerController(db)

	timerScheduler := service.NewTimerScheduler(db, resumer)
	go timerScheduler.Start(context.Background())
	defer timerScheduler.Stop()
	// </ timer-----------------------------------------

	app := fiber.New()

	app.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"title": "Information", "message": "Goflow server is running successfully"})
	})

	// Initialize role and action controllers
	roleCtrl := api.NewRoleController(db)
	actionCtrl := api.NewActionController(db)

	// Role routes (protected with permissions)
	app.Post("/roles", permissions(db, "CAN_EDIT_ROLE"), roleCtrl.CreateRole)
	app.Get("/roles", roleCtrl.ListRoles)
	app.Get("/roles/:id", roleCtrl.GetRole)
	app.Put("/roles/:id", permissions(db, "CAN_EDIT_ROLE"), roleCtrl.UpdateRole)
	app.Delete("/roles/:id", permissions(db, "CAN_EDIT_ROLE"), roleCtrl.DeleteRole)

	// User role assignment
	app.Post("/users/:userId/roles/:roleId", permissions(db, "CAN_MANGE_USER"), roleCtrl.AssignRoleToUser)
	app.Delete("/users/:userId/roles/:roleId", permissions(db, "CAN_MANGE_USER"), roleCtrl.RemoveRoleFromUser)
	app.Get("/users/:userId/roles", roleCtrl.GetUserRoles)

	// Action routes
	app.Get("/actions", actionCtrl.ListActions)
	app.Post("/roles/:roleId/actions/:actionId", permissions(db, "CAN_EDIT_ROLE"), actionCtrl.AssignActionToRole)
	app.Delete("/roles/:roleId/actions/:actionId", permissions(db, "CAN_EDIT_ROLE"), actionCtrl.RemoveActionFromRole)
	app.Get("/roles/:roleId/actions", actionCtrl.GetRoleActions)

	// User routes (protected by your auth middleware)
	app.Post("/users", permissions(db, "CAN_MANGE_USER"), userController.CreateUser)

	app.Get("/users", permissions(db, "CAN_MANGE_USER"), userController.ListUsers)         // Protected - needs auth
	app.Get("/users/me", userController.GetCurrentUser)                                    // Protected - gets current user
	app.Get("/users/:id", userController.GetUser)                                          // Protected
	app.Get("/users/email/:email", userController.GetUserByEmail)                          // Protected
	app.Put("/users/:id", userController.UpdateUser)                                       // Protected - own profile only
	app.Put("/users/:id/password", userController.UpdatePassword)                          // Protected - own password only
	app.Delete("/users/:id", permissions(db, "CAN_MANGE_USER"), userController.DeleteUser) // Protected - admin only

	// Access control routes
	app.Get("/users/:id/access/:resource", userController.CheckAccess)
	app.Get("/users/:id/permission/:actionId", userController.CheckPermission)

	// C8
	app.Post("/engine-rest/v2/deployments", permissions(db, "CAN_MANAGE_DEPLOY_PROCESS"), processDefinitionCtrl.DeployBPMN)
	app.Post("/engine-rest/v2/process-definitions/:key/start", processInstanceCtrl.StartProcess)

	// C7
	app.Post("/engine-rest/v2/deployment/create", permissions(db, "CAN_MANAGE_DEPLOY_PROCESS"), processDefinitionCtrl.DeployBPMN)
	app.Post("/engine-rest/deployment/create", permissions(db, "CAN_MANAGE_DEPLOY_PROCESS"), processDefinitionCtrl.DeployBPMN)
	app.Post("/engine-rest/external-task/fetchAndLock", permissions(db, "CAN_READ_JOBS"), externalTaskCtrl.FetchAndLock)
	app.Post("/engine-rest/external-task/:id/complete", externalTaskCtrl.CompleteTask)
	app.Post("/engine-rest/external-task/:id/failure", externalTaskCtrl.HandleFailure)

	app.Get("/tasks", permissions(db, "CAN_READ_TASKS"), taskCtrl.GetTasks)
	app.Post("/tasks/:id/complete", processInstanceCtrl.CompleteTask)
	app.Get("/jobs", permissions(db, "CAN_READ_JOBS"), externalTaskCtrl.GetJobs)

	// Audit endpoints
	app.Get("/audit/process/:processId", auditController.GetProcessAuditLogs)
	app.Get("/audit/task/:taskId", auditController.GetTaskAuditLogs)
	app.Post("/audit/date-range", auditController.GetAuditLogsByDateRange)

	app.Get("/timers", timerController.GetTimers)
	app.Get("/timers/:id", timerController.GetTimerByID)
	app.Get("/multi-instance/execution/:instanceId", multiInstanceController.GetByProcessInstance)

	app.Post("/async/process-definitions/:key/start", processInstanceCtrl.StartProcessAsync)

	// Worker pool metrics endpoint (for monitoring)
	app.Get("/metrics/pool", func(c *fiber.Ctx) error {
		metrics := workerPool.GetMetrics()
		return c.JSON(fiber.Map{
			"name":            "GoFlow-Main",
			"jobs_submitted":  metrics.JobsSubmitted,
			"jobs_processed":  metrics.JobsProcessed,
			"jobs_failed":     metrics.JobsFailed,
			"jobs_retried":    metrics.JobsRetried,
			"average_time_ms": metrics.AverageTime.Milliseconds(),
			"queue_length":    metrics.QueueLength,
			"active_workers":  metrics.ActiveWorkers,
			"queue_capacity":  1000,
			"healthy":         workerPool.IsHealthy(),
		})
	})

	// Add to main.go
	app.Post("/metrics/pool/reset", func(c *fiber.Ctx) error {
		// This would require adding a Reset() method to worker pool
		// For now, just return a message
		return c.JSON(fiber.Map{
			"message": "Restart server to reset metrics",
			"note":    "Metrics are not resettable without restart",
		})
	})

	log.Println("🚀 GoFlow server starting on :8080")
	log.Printf("   Worker Pool: 10 workers, queue size 1000")
	log.Printf("   Timer Scheduler: running")
	log.Println("   Ready to accept requests")

	app.Listen(":8080")
}
