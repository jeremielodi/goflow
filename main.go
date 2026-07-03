package main

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/api"
	apiv2 "github.com/jeremielodi/goflow/internal/api/v2"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/listeners"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/internal/worker"
	"github.com/jeremielodi/goflow/internal/zeebegrpc"
	"github.com/jeremielodi/goflow/internal/zeebegrpc/pb"
	"github.com/jeremielodi/goflow/pkg/authentication"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jeremielodi/goflow/pkg/database"
	"github.com/jeremielodi/goflow/pkg/middleware"
	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc"
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

	// Create task event dispatcher
	taskEventDispatcher := events.NewTaskEventDispatcher()

	// Create SSE listener for real-time events
	sseListener := listeners.NewSSEListener()
	taskEventDispatcher.Register(sseListener)

	// Create WebSocket listener
	wsListener := listeners.NewWebSocketListener()
	taskEventDispatcher.Register(wsListener)

	// Create Webhook listener (optional - configure as needed)
	webhookListener := listeners.NewWebhookListener()
	// Register global webhook (uncomment to enable)
	// webhookListener.RegisterGlobalWebhook(listeners.WebhookConfig{
	// 	URL:        "https://your-client.com/webhook/task-events",
	// 	Timeout:    5 * time.Second,
	// 	RetryCount: 3,
	// 	RetryDelay: 1 * time.Second,
	// 	Headers: map[string]string{
	// 		"X-API-Key": "your-api-key",
	// 	},
	// })
	taskEventDispatcher.Register(webhookListener)

	resumer := runtime.CreateResumer(db, taskEventDispatcher)

	// Create default data
	if err := service.CreateDefaultData(db, util); err != nil {
		log.Fatalf("Failed to create superuser: %v", err)
	}

	// ============================================================
	// TASK EVENT DISPATCHER & LISTENERS
	// ============================================================

	log.Println("✅ Task event listeners initialized (SSE, WebSocket, Webhook)")

	// ============================================================
	// CREATE WORKER POOL
	// ============================================================

	// Create worker pool for processing requests asynchronously
	// Parameters: db, pool name, number of workers, queue size
	workerPool := worker.NewWorkerPool(db, "GoFlow-Main", 25, 1000, taskEventDispatcher)
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
		}
	}()

	taskService := service.NewTaskService(db, taskEventDispatcher)
	taskService.SetResumeExecutor(func(ctx context.Context, graph *engine.ProcessGraph, execID uuid.UUID) error {
		rt := runtime.NewRuntime(graph, db, taskEventDispatcher)
		return rt.ExecuteExecution(ctx, execID)
	})

	// ============================================================
	// INITIALIZE CONTROLLERS
	// ============================================================

	processDefinitionCtrl := api.NewProcessDefinitionController(db, &rootDirPath)
	processDefinitionCtrl.SetDispatcher(taskEventDispatcher)
	processInstanceCtrl := api.NewProcessInstanceController(db, taskService, workerPool, taskEventDispatcher)
	taskCtrl := api.NewTaskController(db, &rootDirPath, taskEventDispatcher) // Pass dispatcher

	externalTaskCtrl := api.NewExternalTaskController(db, taskEventDispatcher)
	auditController := api.NewAuditController(db)

	multiInstanceController := api.NewMultiInstanceController(db)

	jwcService := authentication.NewJWTService(db, util.DotEnvVariable("goflow_secret_key"))
	userController := api.NewUserController(db, jwcService)

	// OIDC is entirely optional: NewOIDCService returns nil unless both
	// OIDC_ISSUER and OIDC_JWKS_URL are configured, in which case
	// CombinedAuthMiddleware accepts bearer tokens from that provider
	// alongside (not instead of) the internal JWT/password login.
	oidcService := authentication.NewOIDCService(
		util.DotEnvVariable("OIDC_ISSUER"),
		util.DotEnvVariable("OIDC_AUDIENCE"),
		util.DotEnvVariable("OIDC_JWKS_URL"),
	)
	if oidcService != nil {
		log.Println("✅ OIDC authentication enabled")
	}

	timerController := api.NewTimerController(db)
	incidentCtrl := api.NewIncidentController(db)

	historicTaskCtrl := api.NewHistoricTaskController(db)
	processPermissionCtrl := api.NewProcessPermissionController(db)

	messageCtrl := api.NewMessageController(db, taskEventDispatcher)
	signalCtrl := api.NewSignalController(db, taskEventDispatcher)
	historyCtrl := api.NewHistoryController(db)

	// Camunda 8 REST API ("/v2/...") — same engine, translated request/response shapes.
	v2ProcessInstanceCtrl := apiv2.NewProcessInstanceController(db, taskEventDispatcher)
	v2JobCtrl := apiv2.NewJobController(db, taskEventDispatcher)
	v2UserTaskCtrl := apiv2.NewUserTaskController(db, taskEventDispatcher)
	v2MessageCtrl := apiv2.NewMessageController(messageCtrl)
	v2SignalCtrl := apiv2.NewSignalController(signalCtrl)
	v2DecisionCtrl := apiv2.NewDecisionController(db)
	v2FormCtrl := apiv2.NewFormController(db)
	v2IncidentCtrl := apiv2.NewIncidentController(db)
	v2FlownodeInstanceCtrl := apiv2.NewFlownodeInstanceController(db)
	v2VariableCtrl := apiv2.NewVariableController(db)

	timerScheduler := service.NewTimerScheduler(db, resumer, taskEventDispatcher)
	go timerScheduler.Start(context.Background())
	defer timerScheduler.Stop()

	// ============================================================
	// SETUP FIBER APP
	// ============================================================

	app := fiber.New()

	// Health check endpoints
	app.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"title": "Information", "message": "Goflow server is running successfully"})
	})

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"title": "Information", "message": "Goflow server is running successfully"})
	})

	// ============================================================
	// EVENT STREAMING ENDPOINTS
	// ============================================================

	// SSE endpoint for task events (requires authentication)
	app.Get("/events/tasks", sseListener.SSEHandler)

	// WebSocket endpoint for task events
	app.Use("/ws/tasks", func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})
	app.Get("/ws/tasks", websocket.New(wsListener.HandleWebSocket))

	log.Println("✅ Event streaming endpoints initialized (SSE: /events/tasks, WebSocket: /ws/tasks)")

	// ============================================================
	// AUTHENTICATION MIDDLEWARE
	// ============================================================

	app.Use(middleware.CombinedAuthMiddleware(db, jwcService, oidcService, util))

	// ============================================================
	// ROLE & ACTION ROUTES
	// ============================================================

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

	// ============================================================
	// USER ROUTES
	// ============================================================

	app.Post("/users", permissions(db, "CAN_MANGE_USER"), userController.CreateUser)
	app.Post("/auth/login", userController.Login)
	app.Post("/auth/refresh", userController.RefreshToken)
	app.Post("/auth/logout", userController.Logout)
	app.Get("/users", permissions(db, "CAN_MANGE_USER"), userController.ListUsers)
	app.Get("/users/me", userController.GetCurrentUser)
	app.Get("/users/:id", userController.GetUser)
	app.Get("/users/email/:email", userController.GetUserByEmail)
	app.Put("/users/:id", userController.UpdateUser)
	app.Put("/users/:id/password", userController.UpdatePassword)
	app.Delete("/users/:id", permissions(db, "CAN_MANGE_USER"), userController.DeleteUser)

	// Access control routes
	app.Get("/users/:id/access/:resource", userController.CheckAccess)
	app.Get("/users/:id/permission/:actionId", userController.CheckPermission)

	// ============================================================
	// BPMN ENDPOINTS (Camunda Compatible)
	// ============================================================

	// Camunda 8 compatible
	// DeployBPMN is authorized in-handler, per process key, not at the route
	// level: deploying a brand-new key still requires the global
	// CAN_MANAGE_DEPLOY_PROCESS action, but redeploying a new version of an
	// EXISTING, restricted key only requires MANAGE on that specific key
	// (see process_authorization_service.go) — a per-resource grant can
	// satisfy that without the global action.
	app.Post("/engine-rest/v2/deployments", processDefinitionCtrl.DeployBPMN)
	app.Post("/engine-rest/v2/process-definitions/:key/start", processInstanceCtrl.StartProcess)
	app.Get("/engine-rest/process-definition", processDefinitionCtrl.ListDefinitions)
	app.Get("/engine-rest/process-definition/:id", processDefinitionCtrl.GetDefinition)
	app.Post("/engine-rest/process-definition/:id/start", processDefinitionCtrl.StartByDefinitionID)

	// Per-resource process permissions (see process_authorization_service.go) —
	// each handler itself requires MANAGE on :key, so no route-level gate here.
	app.Get("/engine-rest/process-definition/key/:key/permissions", processPermissionCtrl.ListPermissions)
	app.Post("/engine-rest/process-definition/key/:key/permissions", processPermissionCtrl.GrantPermission)
	app.Delete("/engine-rest/process-definition/key/:key/permissions/:id", processPermissionCtrl.RevokePermission)

	// Camunda 7 compatible
	app.Get("/engine-rest/engine", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"title": "Information", "message": "Goflow server is running successfully"})
	})

	app.Post("/engine-rest/v2/deployment/create", processDefinitionCtrl.DeployBPMN)
	app.Post("/engine-rest/deployment/create", processDefinitionCtrl.DeployBPMN)
	app.Post("/engine-rest/external-task/fetchAndLock", permissions(db, "CAN_READ_JOBS"), externalTaskCtrl.FetchAndLock)
	app.Post("/engine-rest/external-task/:id/complete", externalTaskCtrl.CompleteTask)
	app.Post("/engine-rest/external-task/:id/failure", externalTaskCtrl.HandleFailure)

	// ============================================================
	// TASK ROUTES
	// ============================================================

	app.Get("/tasks", permissions(db, "CAN_READ_TASKS"), taskCtrl.GetTasks)
	app.Get("/tasks/:id", permissions(db, "CAN_READ_TASKS"), taskCtrl.GetTask)
	app.Post("/tasks/:id/complete", processInstanceCtrl.CompleteTask)
	app.Get("/engine-rest/tasks", permissions(db, "CAN_READ_TASKS"), taskCtrl.GetTasks)
	app.Post("/engine-rest/tasks/:id/claim", taskCtrl.Claim)
	app.Post("/engine-rest/tasks/:id/unclaim", taskCtrl.UnClaim)
	app.Post("/engine-rest/tasks/:id/complete", processInstanceCtrl.CompleteTask)

	// ============================================================
	// JOB ROUTES
	// ============================================================

	app.Get("/jobs", permissions(db, "CAN_READ_JOBS"), externalTaskCtrl.GetJobs)
	app.Get("/jobs/:id", permissions(db, "CAN_READ_JOBS"), externalTaskCtrl.GetJob)
	app.Get("/engine-rest/jobs/:id", permissions(db, "CAN_READ_JOBS"), externalTaskCtrl.GetJob)
	app.Post("/engine-rest/job/:id/retries", incidentCtrl.SetJobRetries)

	// ============================================================
	// INCIDENT ROUTES
	// ============================================================

	app.Get("/engine-rest/incident", incidentCtrl.ListIncidents)
	app.Get("/engine-rest/incident/:id", incidentCtrl.GetIncident)
	app.Delete("/engine-rest/incident/:id", incidentCtrl.DeleteIncident)
	app.Get("/engine-rest/process-instance/:id/incidents", incidentCtrl.GetByProcessInstance)

	// ============================================================
	// HISTORY ROUTES
	// ============================================================

	app.Get("/engine-rest/history/task/count", historicTaskCtrl.GetHistoricTaskCount)
	app.Get("/engine-rest/history/task/:id", historicTaskCtrl.GetHistoricTask)
	app.Get("/engine-rest/history/task", historicTaskCtrl.GetHistoricTasks)

	// Full historic process/activity instance endpoints (Camunda 7 compatible)
	app.Get("/engine-rest/history/process-instance", historyCtrl.ListHistoricProcessInstances)
	app.Get("/engine-rest/history/process-instance/:id", historyCtrl.GetHistoricProcessInstance)
	app.Get("/engine-rest/history/activity-instance", historyCtrl.ListHistoricActivityInstances)

	// ============================================================
	// MESSAGE & SIGNAL ROUTES
	// ============================================================

	app.Post("/engine-rest/message", messageCtrl.CorrelateMessage)
	app.Post("/engine-rest/signal", signalCtrl.BroadcastSignal)

	// ============================================================
	// CAMUNDA 8 REST API ("/v2/...")
	// ============================================================

	app.Post("/v2/resources/deployments", processDefinitionCtrl.DeployBPMN)

	app.Post("/v2/process-instances", v2ProcessInstanceCtrl.CreateProcessInstance)
	app.Get("/v2/process-instances/:id", v2ProcessInstanceCtrl.GetProcessInstance)
	app.Post("/v2/process-instances/search", v2ProcessInstanceCtrl.SearchProcessInstances)

	app.Post("/v2/jobs/activation", permissions(db, "CAN_READ_JOBS"), v2JobCtrl.ActivateJobs)
	app.Post("/v2/jobs/:id/completion", externalTaskCtrl.CompleteTask)
	app.Post("/v2/jobs/:id/failure", externalTaskCtrl.HandleFailure)
	app.Post("/v2/jobs/:id/error", externalTaskCtrl.HandleFailure)

	app.Post("/v2/user-tasks/:id/completion", processInstanceCtrl.CompleteTask)
	app.Post("/v2/user-tasks/:id/assignment", taskCtrl.Claim)
	app.Delete("/v2/user-tasks/:id/assignee", taskCtrl.UnClaim)
	app.Post("/v2/user-tasks/search", permissions(db, "CAN_READ_TASKS"), v2UserTaskCtrl.SearchUserTasks)

	app.Post("/v2/messages/publication", v2MessageCtrl.PublishMessage)
	app.Post("/v2/signals/broadcast", v2SignalCtrl.BroadcastSignal)
	app.Post("/v2/decisions/:key/evaluation", v2DecisionCtrl.EvaluateDecision)
	app.Get("/v2/forms/:formKey", v2FormCtrl.GetForm)

	app.Post("/v2/incidents/search", v2IncidentCtrl.SearchIncidents)
	app.Post("/v2/flownode-instances/search", v2FlownodeInstanceCtrl.SearchFlownodeInstances)
	app.Post("/v2/variables/search", v2VariableCtrl.SearchVariables)
	app.Post("/v2/process-instances/:id/modification", v2ProcessInstanceCtrl.ModifyProcessInstance)

	// ============================================================
	// AUDIT ROUTES
	// ============================================================

	app.Get("/engine-rest/audit/process/:processId", auditController.GetProcessAuditLogs)
	app.Get("/engine-rest/audit/process/:processId/token-history", auditController.GetTokenHistory)
	app.Get("/engine-rest/audit/task/:taskId", auditController.GetTaskAuditLogs)
	app.Post("/engine-rest/audit/date-range", auditController.GetAuditLogsByDateRange)

	// ============================================================
	// TIMER & MULTI-INSTANCE ROUTES
	// ============================================================

	app.Get("/timers", timerController.GetTimers)
	app.Get("/timers/:id", timerController.GetTimerByID)
	app.Get("/multi-instance/execution/:instanceId", multiInstanceController.GetByProcessInstance)

	// ============================================================
	// ASYNC PROCESS START
	// ============================================================

	app.Post("/async/process-definitions/:key/start", processInstanceCtrl.StartProcessAsync)

	// ============================================================
	// PROCESS INSTANCE VARIABLE ROUTES
	// ============================================================

	app.Get("/engine-rest/process-instance/:id/variables", processInstanceCtrl.GetProcessVariables)
	app.Get("/engine-rest/process-instance/:id/variables/:varName", processInstanceCtrl.GetProcessVariable)
	app.Post("/engine-rest/process-instance/:id/variables", processInstanceCtrl.SetProcessVariables)
	app.Put("/engine-rest/process-instance/:id/variables/:varName", processInstanceCtrl.SetProcessVariable)
	app.Delete("/engine-rest/process-instance/:id/variables/:varName", processInstanceCtrl.DeleteProcessVariable)
	app.Delete("/engine-rest/process-instance/:id/variables", processInstanceCtrl.DeleteProcessVariables)

	// ============================================================
	// PROCESS INSTANCE STATE MANAGEMENT
	// ============================================================

	app.Put("/engine-rest/process-instance/:id/suspended", processInstanceCtrl.SuspendProcess)
	app.Delete("/engine-rest/process-instance/:id", processInstanceCtrl.TerminateProcess)
	app.Post("/engine-rest/process-instance/:id/end", processInstanceCtrl.EndProcess)
	app.Get("/engine-rest/process-instance/:id/state", processInstanceCtrl.GetProcessState)
	app.Post("/engine-rest/process-instance/:id/stop", processInstanceCtrl.StopProcessWithReason)

	// Historic process instance
	app.Get("/engine-rest/history/process-instance/:id", processInstanceCtrl.GetHistoricProcessInstance)

	// Process instance queries
	app.Get("/engine-rest/process-instance/:id", processInstanceCtrl.GetProcessInstance)
	app.Get("/engine-rest/process-instance", processInstanceCtrl.GetProcessInstanceList)

	// ============================================================
	// METRICS ENDPOINTS
	// ============================================================

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

	app.Post("/metrics/pool/reset", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"message": "Restart server to reset metrics",
			"note":    "Metrics are not resettable without restart",
		})
	})

	// ============================================================
	// gRPC GATEWAY (Zeebe protocol subset — see internal/zeebegrpc)
	// ============================================================

	anonymousUserID, err := zeebegrpc.ResolveAnonymousUserID(db)
	if err != nil {
		log.Printf("⚠️ gRPC gateway: could not resolve a super_admin fallback identity, gRPC calls without a bearer token will be rejected: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(zeebegrpc.UnaryAuthInterceptor(jwcService, anonymousUserID)),
		grpc.StreamInterceptor(zeebegrpc.StreamAuthInterceptor(jwcService, anonymousUserID)),
	)
	pb.RegisterGatewayServer(grpcServer, zeebegrpc.NewServer(db, taskEventDispatcher))

	grpcLis, err := net.Listen("tcp", ":26500")
	if err != nil {
		log.Printf("⚠️ gRPC gateway: failed to listen on :26500: %v", err)
	} else {
		go func() {
			log.Println("🚀 gRPC gateway (Zeebe protocol subset) listening on :26500")
			if err := grpcServer.Serve(grpcLis); err != nil {
				log.Printf("gRPC server stopped: %v", err)
			}
		}()
	}

	// ============================================================
	// START SERVER
	// ============================================================

	log.Println("🚀 GoFlow server starting on :8080")
	log.Printf("   Worker Pool: 10 workers, queue size 1000")
	log.Printf("   Timer Scheduler: running")
	log.Printf("   Event Streams: SSE (/events/tasks), WebSocket (/ws/tasks)")
	log.Println("   Ready to accept requests")

	app.Listen(":8080")
}
