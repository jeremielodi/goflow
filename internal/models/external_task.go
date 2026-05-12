package models

type CamundaVariable struct {
	Value interface{} `json:"value"`
	Type  string      `json:"type"`
}

type ExternalTask struct {
	ID        string                     `json:"id"`
	TopicName string                     `json:"topicName"`
	Variables map[string]CamundaVariable `json:"variables"`
	Retries   int                        `json:"retries"`
	WorkerId  string                     `json:"workerId"`
}
type FetchAndLockResponse []ExternalTask
