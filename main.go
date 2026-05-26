package main

import (
	"log"
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/api"
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
	processDefinitionCtrl := api.NewProcessDefinitionController(db, &rootDirPath)
	processInstanceCtrl := api.NewProcessInstanceController(db)
	taskCtrl := api.NewTaskController(db, &rootDirPath)
	externalTaskCtrl := api.NewExternalTaskController(db)
	app := fiber.New()

	app.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"title": "Information", "message": "Goflow server is running successfully"})
	})
	// C8
	app.Post("/engine-rest/v2/deployments", processDefinitionCtrl.DeployBPMN)
	app.Post("/engine-rest/v2/process-definitions/:key/start", processInstanceCtrl.StartProcess)

	// C7
	app.Post("/engine-rest/deployment/create", processDefinitionCtrl.DeployBPMN)
	app.Post("/engine-rest/external-task/fetchAndLock", externalTaskCtrl.FetchAndLock)
	app.Post("/engine-rest/external-task/:id/complete", externalTaskCtrl.CompleteTask)
	app.Post("/engine-rest/external-task/:id/failure", externalTaskCtrl.HandleFailure)

	app.Get("/tasks", taskCtrl.GetTasks)
	app.Post("/tasks/:id/complete", processInstanceCtrl.CompleteTask)

	app.Listen(":8080")
}
