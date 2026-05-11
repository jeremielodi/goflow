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

	app := fiber.New()

	// Deploy BPMN
	app.Post("/engine-rest/v2/deployments", processDefinitionCtrl.DeployBPMN)

	app.Listen(":8080")
}
