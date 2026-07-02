package common

import "github.com/jeremielodi/goflow/internal/engine"

// EvaluateIOMappings evaluates each mapping's Source expression against vars
// and returns a new map of Target -> evaluated value. Used for zeebe:ioMapping
// on both input (activity entry) and output (activity completion) — the two
// only differ in which variable map they're evaluated against.
func EvaluateIOMappings(mappings []engine.IOMapping, vars map[string]interface{}) map[string]interface{} {
	if len(mappings) == 0 {
		return nil
	}
	result := make(map[string]interface{}, len(mappings))
	for _, m := range mappings {
		value, err := EvaluateValue(m.Source, vars)
		if err != nil {
			continue
		}
		result[m.Target] = value
	}
	return result
}
