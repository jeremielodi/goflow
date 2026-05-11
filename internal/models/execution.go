package models

type Execution struct {
	ID                string
	ProcessInstanceID string
	CurrentElementID  string
	Status            string
}
