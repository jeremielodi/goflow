package v2

import (
	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type FormController struct {
	formRepo *repository.FormRepository
}

func NewFormController(db *sqlx.DB) *FormController {
	return &FormController{formRepo: repository.NewFormRepository(db)}
}

// GetForm handles GET /v2/forms/:formKey — serves the latest version of a
// linked form definition (a .form JSON resource deployed alongside a BPMN
// file and referenced from a userTask via zeebe:formDefinition formId).
func (fc *FormController) GetForm(c *fiber.Ctx) error {
	formKey := c.Params("formKey")

	form, err := fc.formRepo.FindLatestFormByFormId(formKey)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"title":  "NOT_FOUND",
			"detail": "no form found for id " + formKey,
		})
	}

	return c.JSON(fiber.Map{
		"formId":  form.FormId,
		"version": form.Version,
		"schema":  form.Schema,
	})
}
