package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type GatewayState struct {
	ID                uuid.UUID       `db:"id"`
	ProcessInstanceID uuid.UUID       `db:"process_instance_id"`
	GatewayID         string          `db:"gateway_id"`
	ExpectedIncoming  int             `db:"expected_incoming"`
	ReceivedIncoming  int             `db:"received_incoming"`
	JoinedFlows       json.RawMessage `db:"joined_flows"`
	Status            string          `db:"status"`
	CreatedAt         time.Time       `db:"created_at"`
	CompletedAt       *time.Time      `db:"completed_at"`
}
