package runtime

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/feel"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/timer"
	"github.com/jmoiron/sqlx"
)

// resolveTimer parses a node's raw timer definition for an instance:
// FEEL expressions (a value starting with "=") are evaluated against the
// instance's variables when the timer is armed, as in Camunda 8.
func (r *Runtime) resolveTimer(q sqlx.Queryer, timerDefinition string, instanceID, execID uuid.UUID) (*timer.Timer, error) {
	return timer.ParseXML(timerDefinition, func(expr string) (string, error) {
		vars, err := repository.VariablesForExecution(q, instanceID, repository.VariableScopeOf(q, instanceID, execID))
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
