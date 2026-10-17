package repository

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// Variable scopes, as in Camunda 8. An instance's variables live in nested
// scopes: the instance itself (the variables table) and local scopes held
// by executions (scope_variables, keyed by the execution's id) — an
// embedded sub-process, a multi-instance body and each of its iterations,
// an activity with input mappings.
//
//   - Reading from an execution sees its own scope and every enclosing one
//     (walking parent_execution_id), the innermost value winning.
//   - Setting variables from an execution propagates each one outwards: it
//     updates the nearest scope that already has it, or else the instance
//     scope.
//   - Setting local variables puts them in one scope only.
//
// The tree is walked in Go from one per-instance query — no recursive SQL
// (Citus-safe; see architecture/citus-distributed-storage-design.md).

// scopeChain returns execID and its ancestors, innermost first.
func scopeChain(q sqlx.Queryer, instanceID, execID uuid.UUID) ([]uuid.UUID, error) {
	if execID == uuid.Nil {
		return nil, nil
	}
	rows, err := q.Queryx(`SELECT id, parent_execution_id FROM public.executions WHERE process_instance_id = $1`, instanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	parent := map[uuid.UUID]*uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		var p *uuid.UUID
		if err := rows.Scan(&id, &p); err != nil {
			return nil, err
		}
		parent[id] = p
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var chain []uuid.UUID
	seen := map[uuid.UUID]bool{}
	for id := &execID; id != nil && !seen[*id]; id = parent[*id] {
		seen[*id] = true
		chain = append(chain, *id)
	}
	return chain, nil
}

// VariableScopeOf returns the execution whose scopes execID reads and
// writes variables through: itself, except a boundary event's trigger
// execution (which has no parent), which goes through the activity it is
// attached to.
func VariableScopeOf(q sqlx.Queryer, instanceID, execID uuid.UUID) uuid.UUID {
	var pathID *string
	if err := sqlx.Get(q, &pathID, `SELECT path_id FROM public.executions WHERE process_instance_id = $1 AND id = $2`, instanceID, execID); err != nil {
		return execID
	}
	if activity, ok := ParseBoundaryTriggerPathID(pathID); ok {
		return activity
	}
	return execID
}

// localScopes loads an instance's local scopes, by scope id.
func localScopes(q sqlx.Queryer, instanceID uuid.UUID, forUpdate bool) (map[uuid.UUID]map[string]interface{}, error) {
	query := `SELECT scope_id, data FROM public.scope_variables WHERE process_instance_id = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	rows, err := q.Queryx(query, instanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	scopes := map[uuid.UUID]map[string]interface{}{}
	for rows.Next() {
		var id uuid.UUID
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, err
		}
		vars := map[string]interface{}{}
		if len(data) > 0 {
			if err := json.Unmarshal(data, &vars); err != nil {
				return nil, err
			}
		}
		scopes[id] = vars
	}
	return scopes, rows.Err()
}

func rootVariables(q sqlx.Queryer, instanceID uuid.UUID, forUpdate bool) (map[string]interface{}, error) {
	query := `SELECT data FROM public.variables WHERE process_instance_id = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var data []byte
	if err := sqlx.Get(q, &data, query, instanceID); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	vars := map[string]interface{}{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &vars); err != nil {
			return nil, err
		}
	}
	return vars, nil
}

// VariablesForExecution returns the variables execID sees: the instance's,
// overlaid by each enclosing local scope from the outermost in. uuid.Nil
// returns the instance scope only.
func VariablesForExecution(q sqlx.Queryer, instanceID, execID uuid.UUID) (map[string]interface{}, error) {
	vars, err := rootVariables(q, instanceID, false)
	if err != nil {
		return nil, err
	}
	scopes, err := localScopes(q, instanceID, false)
	if err != nil || len(scopes) == 0 {
		return vars, err
	}
	chain, err := scopeChain(q, instanceID, execID)
	if err != nil {
		return nil, err
	}
	for i := len(chain) - 1; i >= 0; i-- {
		for k, v := range scopes[chain[i]] {
			vars[k] = v
		}
	}
	return vars, nil
}

// LocalVariables returns the variables local to one scope.
func LocalVariables(q sqlx.Queryer, instanceID, scopeID uuid.UUID) (map[string]interface{}, error) {
	var data []byte
	err := sqlx.Get(q, &data, `SELECT data FROM public.scope_variables WHERE process_instance_id = $1 AND scope_id = $2`, instanceID, scopeID)
	vars := map[string]interface{}{}
	if err == sql.ErrNoRows {
		return vars, nil
	}
	if err != nil {
		return nil, err
	}
	return vars, json.Unmarshal(data, &vars)
}

// SetLocalVariablesTx merges vars into scopeID's local scope.
func SetLocalVariablesTx(tx *sqlx.Tx, instanceID, scopeID uuid.UUID, vars map[string]interface{}) error {
	if len(vars) == 0 {
		return nil
	}
	data, err := json.Marshal(vars)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`
		INSERT INTO public.scope_variables (process_instance_id, scope_id, data, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (process_instance_id, scope_id)
		DO UPDATE SET data = public.scope_variables.data || EXCLUDED.data, updated_at = NOW()
	`, instanceID, scopeID, data)
	return err
}

// SetVariablesTx sets vars from execID's scope outwards: each one updates
// the nearest scope that already has it, or the instance scope. uuid.Nil
// sets them all on the instance scope.
func SetVariablesTx(tx *sqlx.Tx, instanceID, execID uuid.UUID, vars map[string]interface{}) error {
	if len(vars) == 0 {
		return nil
	}
	root, err := rootVariables(tx, instanceID, true)
	if err != nil {
		return err
	}
	scopes, err := localScopes(tx, instanceID, true)
	if err != nil {
		return err
	}
	var chain []uuid.UUID
	if len(scopes) > 0 {
		if chain, err = scopeChain(tx, instanceID, execID); err != nil {
			return err
		}
	}
	touched := map[uuid.UUID]map[string]interface{}{}
	rootChanged := false
	for name, value := range vars {
		placed := false
		for _, scopeID := range chain {
			if local, ok := scopes[scopeID]; ok {
				if _, has := local[name]; has {
					if touched[scopeID] == nil {
						touched[scopeID] = map[string]interface{}{}
					}
					touched[scopeID][name] = value
					placed = true
					break
				}
			}
		}
		if !placed {
			root[name] = value
			rootChanged = true
		}
	}
	for scopeID, changed := range touched {
		if err := SetLocalVariablesTx(tx, instanceID, scopeID, changed); err != nil {
			return err
		}
	}
	if rootChanged {
		data, err := json.Marshal(root)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`
			INSERT INTO public.variables (id, process_instance_id, data, updated_at)
			VALUES ($1, $2, $3, NOW())
			ON CONFLICT (process_instance_id) DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()
		`, uuid.New(), instanceID, data); err != nil {
			return fmt.Errorf("instance variables: %w", err)
		}
	}
	return nil
}

// ClearLocalVariablesTx drops scopes when the executions holding them
// leave their element.
func ClearLocalVariablesTx(tx *sqlx.Tx, instanceID uuid.UUID, scopeIDs ...uuid.UUID) error {
	for _, id := range scopeIDs {
		if _, err := tx.Exec(`DELETE FROM public.scope_variables WHERE process_instance_id = $1 AND scope_id = $2`, instanceID, id); err != nil {
			return err
		}
	}
	return nil
}
