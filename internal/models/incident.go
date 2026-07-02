package models

import (
	"database/sql"
	"encoding/json"
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

// MarshalJSON flattens the sql.Null* fields to plain JSON values (or omits
// them) instead of serializing Go's internal {String,Valid}/{Time,Valid}
// struct shape, which API consumers (the frontend, external clients) have no
// reason to know about.
func (i Incident) MarshalJSON() ([]byte, error) {
	type Alias Incident
	aux := struct {
		Alias
		ErrorMessage string     `json:"errorMessage,omitempty"`
		ErrorCode    string     `json:"errorCode,omitempty"`
		ResolvedAt   *time.Time `json:"resolvedAt,omitempty"`
	}{Alias: Alias(i)}

	if i.ErrorMessage.Valid {
		aux.ErrorMessage = i.ErrorMessage.String
	}
	if i.ErrorCode.Valid {
		aux.ErrorCode = i.ErrorCode.String
	}
	if i.ResolvedAt.Valid {
		t := i.ResolvedAt.Time
		aux.ResolvedAt = &t
	}
	return json.Marshal(aux)
}

type IncidentCreate struct {
	ProcessInstanceID uuid.UUID
	JobID             *uuid.UUID
	IncidentType      string
	ActivityID        string
	ErrorMessage      string
	ErrorCode         string
}
