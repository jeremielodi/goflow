package v2

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type IncidentController struct {
	incidentRepo *repository.IncidentRepository
}

func NewIncidentController(db *sqlx.DB) *IncidentController {
	return &IncidentController{incidentRepo: repository.NewIncidentRepository(db)}
}

// SearchIncidentsRequest matches the (simplified) Camunda 8 search body.
type SearchIncidentsRequest struct {
	Filter struct {
		ProcessInstanceKey string `json:"processInstanceKey"`
		State               string `json:"state"`
		ErrorType           string `json:"errorType"`
		ElementId           string `json:"elementId"`
	} `json:"filter"`
}

// incidentState maps GoFlow's incident state to the Camunda 8 naming.
func incidentState(state string) string {
	switch state {
	case "open":
		return "ACTIVE"
	case "resolved":
		return "RESOLVED"
	case "deleted":
		return "DELETED"
	default:
		return state
	}
}

// SearchIncidents handles POST /v2/incidents/search.
func (ic *IncidentController) SearchIncidents(c *fiber.Ctx) error {
	var req SearchIncidentsRequest
	_ = c.BodyParser(&req)

	f := repository.IncidentFilter{}
	if req.Filter.ProcessInstanceKey != "" {
		id, err := uuid.Parse(req.Filter.ProcessInstanceKey)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"title": "INVALID_ARGUMENT", "detail": "invalid processInstanceKey",
			})
		}
		f.ProcessInstanceID = &id
	}
	if req.Filter.ErrorType != "" {
		f.IncidentType = &req.Filter.ErrorType
	}
	if req.Filter.ElementId != "" {
		f.ActivityID = &req.Filter.ElementId
	}
	if req.Filter.State != "" {
		state := ""
		switch req.Filter.State {
		case "ACTIVE":
			state = "open"
		case "RESOLVED":
			state = "resolved"
		case "DELETED":
			state = "deleted"
		}
		if state != "" {
			f.State = &state
		}
	}

	incidents, err := ic.incidentRepo.List(f)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}

	items := make([]fiber.Map, 0, len(incidents))
	for _, inc := range incidents {
		items = append(items, fiber.Map{
			"incidentKey":        inc.ID,
			"processInstanceKey": inc.ProcessInstanceID,
			"jobKey":             inc.JobID,
			"errorType":          inc.IncidentType,
			"errorMessage":       inc.ErrorMessage.String,
			"elementId":          inc.ActivityID,
			"state":              incidentState(inc.State),
			"creationTime":       inc.CreatedAt,
		})
	}

	return c.JSON(fiber.Map{"items": items})
}
