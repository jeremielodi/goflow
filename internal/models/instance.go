package models

type ProcessInstance struct {
	ID         string
	ProcessKey string
	Variables  map[string]interface{}
	Status     string // running | waiting | completed
}
