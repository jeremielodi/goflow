package main

import (
	"context"
	"log"
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/api"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jeremielodi/goflow/pkg/database"
)

func main() {

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

	taskService := service.NewTaskService(db)
	taskService.SetResumeExecutor(func(ctx context.Context, graph *engine.ProcessGraph, execID uuid.UUID) error {
		rt := runtime.NewRuntime(graph, db)
		return rt.ExecuteExecution(ctx, execID)
	})

	processDefinitionCtrl := api.NewProcessDefinitionController(db, &rootDirPath)
	processInstanceCtrl := api.NewProcessInstanceController(db, taskService)
	taskCtrl := api.NewTaskController(db, &rootDirPath)

	externalTaskCtrl := api.NewExternalTaskController(db)
	auditController := api.NewAuditController(db)

	// timer-----------------------------------------
	timerScheduler := service.NewTimerScheduler(db, resumer)
	go timerScheduler.Start(context.Background())
	defer timerScheduler.Stop()
	// </ timer-----------------------------------------
	app := fiber.New()

	app.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"title": "Information", "message": "Goflow server is running successfully"})
	})
	// C8
	app.Post("/engine-rest/v2/deployments", processDefinitionCtrl.DeployBPMN)
	app.Post("/engine-rest/v2/process-definitions/:key/start", processInstanceCtrl.StartProcess)

	// C7
	app.Post("/engine-rest/v2/deployment/create", processDefinitionCtrl.DeployBPMN)
	app.Post("/engine-rest/deployment/create", processDefinitionCtrl.DeployBPMN)
	app.Post("/engine-rest/external-task/fetchAndLock", externalTaskCtrl.FetchAndLock)
	app.Post("/engine-rest/external-task/:id/complete", externalTaskCtrl.CompleteTask)
	app.Post("/engine-rest/external-task/:id/failure", externalTaskCtrl.HandleFailure)

	app.Get("/tasks", taskCtrl.GetTasks)
	app.Post("/tasks/:id/complete", processInstanceCtrl.CompleteTask)
	app.Get("/jobs", externalTaskCtrl.GetJobs)

	// Audit endpoints
	app.Get("/audit/process/:processId", auditController.GetProcessAuditLogs)
	app.Get("/audit/task/:taskId", auditController.GetTaskAuditLogs)
	app.Post("/audit/date-range", auditController.GetAuditLogsByDateRange)

	app.Listen(":8080")
}
