package models

type Task struct {
	ID                string
	ProcessInstanceID string
	TaskDefinitionKey string
	Name              string
	Status            string // open | completed
	Assignee          string
	CandidateGroup    string
}
