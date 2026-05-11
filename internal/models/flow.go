package models

type Flow struct {
	ID        string
	SourceRef string
	TargetRef string
	Condition string
}
