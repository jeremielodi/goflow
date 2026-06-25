package models

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
)

type Incident struct {
	ID                uuid.UUID      `db:"id" json:"id"`
	ProcessInstanceID uuid.UUID      `db:"process_instance_id" json:"processInstanceId"`
	JobID             *uuid.UUID     `db:"job_id" json:"jobId,omitempty"`
	IncidentType      string         `db:"incident_type" json:"incidentType"`
	ActivityID        string         `db:"activity_id" json:"activityId"`
	ErrorMessage      sql.NullString `db:"error_message" json:"errorMessage,omitempty"`
	ErrorCode         sql.NullString `db:"error_code" json:"errorCode,omitempty"`
	State             string         `db:"state" json:"state"`
	CreatedAt         time.Time      `db:"created_at" json:"createdAt"`
	ResolvedAt        sql.NullTime   `db:"resolved_at" json:"resolvedAt,omitempty"`
}

type IncidentCreate struct {
	ProcessInstanceID uuid.UUID
	JobID             *uuid.UUID
	IncidentType      string
	ActivityID        string
	ErrorMessage      string
	ErrorCode         string
}
