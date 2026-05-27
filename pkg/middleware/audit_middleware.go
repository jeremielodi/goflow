// middleware/audit_middleware.go
package middleware

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type AuditMiddleware struct {
	auditRepo *repository.AuditRepository
}

func NewAuditMiddleware(db *sqlx.DB) *AuditMiddleware {
	return &AuditMiddleware{
		auditRepo: repository.NewAuditRepository(db),
	}
}

// APIAuditMiddleware logs API requests and responses
func (m *AuditMiddleware) APIAuditMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()

		// Read request body
		var requestBody []byte
		if c.Request().Body() != nil {
			requestBody = bytes.Clone(c.Request().Body())
		}

		// Process request
		err := c.Next()

		// Calculate duration
		duration := time.Since(start)

		// Log API call
		auditLog := &models.AuditLog{
			ID:        uuid.New(),
			Action:    models.AuditAction("API_CALL"),
			CreatedAt: time.Now(),
		}

		logData := map[string]interface{}{
			"method":    c.Method(),
			"path":      c.Path(),
			"status":    c.Response().StatusCode(),
			"duration":  duration.Milliseconds(),
			"ip":        c.IP(),
			"userAgent": c.Get("User-Agent"),
		}

		if len(requestBody) > 0 && len(requestBody) < 10000 { // Don't log huge bodies
			logData["requestBody"] = string(requestBody)
		}

		if err != nil {
			logData["error"] = err.Error()
		}

		logDataJSON, _ := json.Marshal(logData)
		auditLog.NewData = logDataJSON

		// Async log to avoid blocking
		go m.auditRepo.CreateAuditLog(auditLog)

		return err
	}
}
