// internal/parser/detect.go
package parser

import (
	"strings"
)

type EngineType string

const (
	EngineCamunda7 EngineType = "camunda7"
	EngineCamunda8 EngineType = "camunda8"
	EngineUnknown  EngineType = "unknown"
)

// DetectEngine inspects the BPMN XML for namespace declarations.
func DetectEngine(xmlData []byte) EngineType {
	// Simple string search – reliable enough for extension detection
	if strings.Contains(string(xmlData), "xmlns:camunda=\"http://camunda.org/schema/1.0/BPMN\"") {
		return EngineCamunda7
	}
	if strings.Contains(string(xmlData), "xmlns:zeebe=\"http://camunda.org/schema/zeebe/1.0\"") {
		return EngineCamunda8
	}
	// Fallback: look for <camunda:...> or <zeebe:...> elements
	if strings.Contains(string(xmlData), "<camunda:") {
		return EngineCamunda7
	}
	if strings.Contains(string(xmlData), "<zeebe:") {
		return EngineCamunda8
	}
	return EngineUnknown
}
