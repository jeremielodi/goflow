package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/api"
	apiv2 "github.com/jeremielodi/goflow/internal/api/v2"
	"github.com/jeremielodi/goflow/internal/citus"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/listeners"
	"github.com/jeremielodi/goflow/internal/metrics"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/partition"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/retention"
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

// knownDevJWTSecrets are placeholder values that have shipped in this repo's
// sample config; starting in production with one of them would let anyone
// who has read the repo forge tokens.
var knownDevJWTSecrets = map[string]bool{
	"change_me":                              true,
	"change-me-in-production":                true,
	"goflow_dev_secret_change_in_production": true,
}

// resolveJWTSecret reads JWT_SECRET_KEY (falling back to the legacy
// goflow_secret_key name) and refuses to start without one: an empty HMAC
// key is silently accepted by jwt/v4, which makes every token forgeable.
func resolveJWTSecret(util common.Util) string {
	secret := util.DotEnvVariable("JWT_SECRET_KEY")
	if secret == "" {
		secret = util.DotEnvVariable("goflow_secret_key")
		if secret != "" {
			log.Println("⚠️ goflow_secret_key is deprecated — rename it to JWT_SECRET_KEY")
		}
	}
	if secret == "" {
		log.Fatalln("JWT_SECRET_KEY is not set — refusing to start with an empty token-signing key")
	}
	if util.DotEnvVariable("GOFLOW_ENV") == "production" && knownDevJWTSecrets[secret] {
		log.Fatalln("JWT_SECRET_KEY is a sample placeholder value — set a real secret for production (e.g. `openssl rand -base64 48`)")
	}
	if len(secret) < 32 {
		log.Println("⚠️ JWT_SECRET_KEY is shorter than 32 characters — use a longer random value (e.g. `openssl rand -base64 48`)")
	}
	return secret
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

	// Optional Citus-backed distributed-storage deployment (see
	// architecture/citus-distributed-storage-design.md and
	// docker-compose.citus.yml) — Phase 1 (reference tables) + Phase 2
	// (the per-instance colocation group). If CITUS_ENABLED isn't set (the
	// default, plain-Postgres path), this is a complete no-op.
	if util.DotEnvVariable("CITUS_ENABLED") == "true" {
		workersEnv := util.DotEnvVariable("CITUS_WORKERS")
		var workers []string
		if workersEnv != "" {
			workers = strings.Split(workersEnv, ",")
		}
		if err := citus.EnsureReferenceTables(db, workers); err != nil {
			log.Fatalln("citus: failed to ensure reference tables: ", err)
		}
		if err := citus.EnsureDistributedTables(db); err != nil {
			log.Fatalln("citus: failed to ensure distributed tables: ", err)
		}
		log.Println("✅ Citus reference + distributed tables ensured (workers:", workersEnv, ")")
	}

	// Cross-instance job wake-ups: a dedicated LISTEN connection means a job
	// created on one GoFlow instance wakes gRPC ActivateJobs long-polls on
	// every instance sharing this database, not just the one that created
	// it — required for the app tier to scale horizontally without
	// degrading gRPC job dispatch latency down to the fallback poll.
	if _, err := repository.StartJobWakeListener(database.ConnStringForListener(util)); err != nil {
		log.Fatalln("job wake listener: ", err)
	}

	// Phase 0 shadow-mode event log (see
	// architecture/event-sourced-kafka-design.md) — optional. If
	// KAFKA_BROKERS isn't set (e.g. local `go run` outside Docker),
	// eventlog.Global() stays nil and every publish call site becomes a
	// silent no-op; nothing about the engine's real write path depends on
	// this being configured.
	var eventLogConsumer *eventlog.Consumer
	if brokersEnv := util.DotEnvVariable("KAFKA_BROKERS"); brokersEnv != "" {
		brokers := strings.Split(brokersEnv, ",")
		eventlog.SetGlobalProducer(eventlog.NewProducer(brokers))
		eventLogConsumer = eventlog.NewConsumer(brokers, "goflow-eventlog-shadow", db)
		go eventLogConsumer.Run(context.Background())
		log.Println("✅ Event log shadow mode started (brokers:", brokersEnv, ")")
	} else {
		log.Println("⏭️ KAFKA_BROKERS not set — event log shadow mode disabled")
	}

	// Create task event dispatcher
	taskEventDispatcher := events.NewTaskEventDispatcher()

	// Phase 1 partitioned write path (see
	// architecture/event-sourced-kafka-design.md) — reuses the same
	// KAFKA_BROKERS config as Phase 0 above; if that wasn't set, this
	// stays fully disabled too (eventlog.GlobalCommand()/GlobalConsumer()
	// stay nil, and every process definition either already defaults to
	// write_path='legacy' or was never flippable in the first place since
	// PATCH .../write-path itself needs this running to be meaningful).
	var timerRouter service.TimerPartitionRouter
	if eventLogConsumer != nil {
		brokers := strings.Split(util.DotEnvVariable("KAFKA_BROKERS"), ",")
		const partitionCount = 12
		replicationFactor := 1
		if v := util.DotEnvVariable("KAFKA_REPLICATION_FACTOR"); v != "" {
			rf, err := strconv.Atoi(v)
			if err != nil || rf < 1 {
				log.Fatalf("KAFKA_REPLICATION_FACTOR must be a whole number >= 1, got %q", v)
			}
			replicationFactor = rf
		}
		if err := eventlog.EnsureTopics(brokers, partitionCount, replicationFactor); err != nil {
			log.Fatalln("partition processor: failed to ensure Kafka topics: ", err)
		}
		eventlog.SetGlobalCommandProducer(eventlog.NewCommandProducer(brokers))
		eventlog.SetGlobalConsumer(eventLogConsumer)

		processor, err := partition.NewProcessor(brokers, db, taskEventDispatcher)
		if err != nil {
			log.Fatalln("partition processor: ", err)
		}
		go processor.Run(context.Background())
		log.Println("✅ Partition processor started (partitions:", partitionCount, ")")

		// Phase 6 (see architecture/event-sourced-kafka-design.md): routes
		// a due timer belonging to a partitioned-write-path instance
		// through the Command Gateway instead of TimerScheduler's own
		// direct resume write — see service.TimerPartitionRouter's doc
		// comment for why this needs a dependency-inversion interface
		// rather than a plain function call.
		timerRouter = partition.NewTimerRouter(db)
	}

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

	// ============================================================
	// METRICS & RETENTION
	// ============================================================

	metrics.Register(db)
	metrics.RegisterGaugeFunc("goflow_worker_pool_queue_length", "Requests waiting in the async worker pool queue.",
		func() float64 { return float64(workerPool.GetMetrics().QueueLength) })
	metrics.RegisterGaugeFunc("goflow_worker_pool_active_workers", "Async worker pool workers currently busy.",
		func() float64 { return float64(workerPool.GetMetrics().ActiveWorkers) })
	metrics.RegisterGaugeFunc("goflow_worker_pool_jobs_failed", "Async worker pool requests that failed since startup.",
		func() float64 { return float64(workerPool.GetMetrics().JobsFailed) })

	retentionCfg, err := retention.ConfigFromEnv(util.DotEnvVariable)
	if err != nil {
		log.Fatalln("retention:", err)
	}
	if retentionCfg.Enabled() {
		retention.Start(context.Background(), db, retentionCfg)
		log.Printf("✅ Retention purge enabled: finished instances older than %d days, every %s", retentionCfg.Days, retentionCfg.Interval)
	} else {
		log.Println("⏭️ RETENTION_DAYS not set — finished instances are kept forever")
	}

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

	jwcService := authentication.NewJWTService(db, resolveJWTSecret(util))
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
	analyticsCtrl := api.NewAnalyticsController(db)
	eventLogCtrl := api.NewEventLogController(eventLogConsumer, db)

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

	timerScheduler := service.NewTimerScheduler(db, resumer, taskEventDispatcher, timerRouter)
	timerScheduler.SetStartTimerHandler(func(ctx context.Context, job models.TimerJob) error {
		return runtime.FireStartTimer(ctx, db, taskEventDispatcher, job)
	})
	go timerScheduler.Start(context.Background())
	runtime.StartConditionalWatcher(context.Background(), db, taskEventDispatcher)
	runtime.StartMessageSweeper(context.Background(), db, taskEventDispatcher)
	defer timerScheduler.Stop()

	// ============================================================
	// SETUP FIBER APP
	// ============================================================

	app := fiber.New()
	app.Use(metrics.Middleware())

	// Health check endpoints
	app.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"title": "Information", "message": "Goflow server is running successfully"})
	})

	app.Get("/health", func(c *fiber.Ctx) error {
		ctx, cancel := context.WithTimeout(c.Context(), 2*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"title": "Error", "message": "database unreachable", "database": "down"})
		}
		return c.JSON(fiber.Map{"title": "Information", "message": "Goflow server is running successfully", "database": "ok"})
	})

	// Prometheus metrics. Registered before the auth middleware below so a
	// scraper needs no user account; protect it with METRICS_TOKEN instead.
	metricsToken := util.DotEnvVariable("METRICS_TOKEN")
	if metricsToken == "" && util.DotEnvVariable("GOFLOW_ENV") == "production" {
		log.Println("⚠️ METRICS_TOKEN not set — /metrics is readable without authentication")
	}
	app.Get("/metrics", metrics.Handler(metricsToken))

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

	// Role routes (protected with permissions). Read routes require either
	// admin permission (CAN_EDIT_ROLE or CAN_MANAGE_USER) rather than being
	// open to any authenticated caller — these expose the permission
	// system's own structure (which roles exist, who can do what).
	adminRead := middleware.RequireAnyPermission(db, "CAN_EDIT_ROLE", "CAN_MANAGE_USER")
	app.Post("/roles", permissions(db, "CAN_EDIT_ROLE"), roleCtrl.CreateRole)
	app.Get("/roles", adminRead, roleCtrl.ListRoles)
	app.Get("/roles/:id", adminRead, roleCtrl.GetRole)
	app.Put("/roles/:id", permissions(db, "CAN_EDIT_ROLE"), roleCtrl.UpdateRole)
	app.Delete("/roles/:id", permissions(db, "CAN_EDIT_ROLE"), roleCtrl.DeleteRole)

	// User role assignment
	app.Post("/users/:userId/roles/:roleId", permissions(db, "CAN_MANAGE_USER"), roleCtrl.AssignRoleToUser)
	app.Delete("/users/:userId/roles/:roleId", permissions(db, "CAN_MANAGE_USER"), roleCtrl.RemoveRoleFromUser)
	app.Get("/users/:userId/roles", adminRead, roleCtrl.GetUserRoles)

	// Action routes
	app.Get("/actions", actionCtrl.ListActions)
	app.Post("/roles/:roleId/actions/:actionId", permissions(db, "CAN_EDIT_ROLE"), actionCtrl.AssignActionToRole)
	app.Delete("/roles/:roleId/actions/:actionId", permissions(db, "CAN_EDIT_ROLE"), actionCtrl.RemoveActionFromRole)
	app.Get("/roles/:roleId/actions", adminRead, actionCtrl.GetRoleActions)

	// ============================================================
	// USER ROUTES
	// ============================================================

	app.Post("/users", permissions(db, "CAN_MANAGE_USER"), userController.CreateUser)
	app.Post("/auth/login", userController.Login)
	app.Post("/auth/refresh", userController.RefreshToken)
	app.Post("/auth/logout", userController.Logout)
	app.Get("/users", permissions(db, "CAN_MANAGE_USER"), userController.ListUsers)
	app.Get("/users/me", userController.GetCurrentUser)
	app.Get("/users/:id", userController.GetUser)
	app.Get("/users/email/:email", userController.GetUserByEmail)
	app.Put("/users/:id", userController.UpdateUser)
	app.Put("/users/:id/password", userController.UpdatePassword)
	app.Put("/users/:id/reset-password", permissions(db, "CAN_RESET_PASSWORD"), userController.AdminResetPassword)
	app.Delete("/users/:id", permissions(db, "CAN_MANAGE_USER"), userController.DeleteUser)

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
	// Phase 1 (see architecture/event-sourced-kafka-design.md): flips a
	// definition onto the Kafka-partitioned write path, gated by a
	// deploy-time BPMN-shape validator.
	app.Patch("/engine-rest/process-definition/:id/write-path", processDefinitionCtrl.SetWritePath)

	// Per-resource process permissions (see process_authorization_service.go) —
	// each handler itself requires MANAGE on :key, so no route-level gate here.
	app.Get("/engine-rest/process-definition/key/:key/permissions", processPermissionCtrl.ListPermissions)
	app.Post("/engine-rest/process-definition/key/:key/permissions", processPermissionCtrl.GrantPermission)
	app.Delete("/engine-rest/process-definition/key/:key/permissions/:id", processPermissionCtrl.RevokePermission)

	// Live per-node active-token counts across every running instance of a
	// key — powers the diagram overlay on the frontend's definition detail page.
	app.Get("/engine-rest/process-definition/key/:key/token-counts", processDefinitionCtrl.GetActiveTokenCounts)
	// Per-instance breakdown of the same snapshot — powers the "current
	// node" column on the frontend's instance list, without needing the
	// full replay view.
	app.Get("/engine-rest/process-definition/key/:key/instance-positions", processDefinitionCtrl.GetInstancePositions)

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
	// ANALYTICS ROUTES
	// ============================================================

	app.Get("/engine-rest/analytics/process-stats", analyticsCtrl.GetProcessStats)

	// ============================================================
	// EVENT LOG (Phase 0 shadow-mode debug/verification surface — see
	// architecture/event-sourced-kafka-design.md; not a real API surface)
	// ============================================================

	app.Get("/engine-rest/eventlog/recent", eventLogCtrl.GetRecent)
	app.Get("/engine-rest/eventlog/projected", eventLogCtrl.GetProjected)

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
