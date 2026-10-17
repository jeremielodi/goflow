package runtime

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/feel"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/timer"
)

// resolveTimer parses a node's raw timer definition for an instance:
// FEEL expressions (a value starting with "=") are evaluated against the
// instance's variables when the timer is armed, as in Camunda 8.
func (r *Runtime) resolveTimer(timerDefinition string, instanceID uuid.UUID) (*timer.Timer, error) {
	return timer.ParseXML(timerDefinition, func(expr string) (string, error) {
		vars, err := repository.NewEngineRepository(r.DB, r.dispatcher).GetProcessVariables(instanceID)
		if err != nil {
			return "", err
		}
		v, err := feel.Evaluate(expr, vars)
		if err != nil {
			return "", err
		}
		s, ok := v.(string)
		if !ok {
			return "", fmt.Errorf("evaluates to %v, not a date, duration or cycle", v)
		}
		return s, nil
	})
}
