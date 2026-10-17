package common

import "testing"

func TestFEELModes(t *testing.T) {
	vars := map[string]interface{}{"amount": 250.0, "price": 0.1}
	defer setFEELMode(feelModeShadow)

	// FEEL-only syntax: CEL rejects it, the native interpreter doesn't.
	const ifExpr = `= if amount > 100 then "high" else "low"`

	setFEELMode(feelModeShadow)
	if _, err := EvaluateValue(ifExpr, vars); err == nil {
		t.Error("shadow mode must still return the CEL result (an error here)")
	}

	setFEELMode(feelModeFEEL)
	if v, err := EvaluateValue(ifExpr, vars); err != nil || v != "high" {
		t.Errorf("feel mode: got %v, %v; want \"high\"", v, err)
	}
	if ok, err := EvaluateCondition(`= price * 3 = 0.3`, vars); err != nil || !ok {
		t.Errorf("feel mode: exact decimals: got %v, %v", ok, err)
	}
	// Camunda 7 expressions keep the CEL path in every mode.
	if ok, err := EvaluateCondition(`${amount > 100}`, vars); err != nil || !ok {
		t.Errorf("feel mode: ${...} condition: got %v, %v", ok, err)
	}
	// Literals that aren't expressions are untouched.
	if v, err := EvaluateValue("sales,marketing", vars); err != nil || v != "sales,marketing" {
		t.Errorf("literal: got %v, %v", v, err)
	}

	setFEELMode(feelModeCEL)
	if _, err := EvaluateValue(ifExpr, vars); err == nil {
		t.Error("cel mode must use CEL (an error here)")
	}
}

func TestSameValue(t *testing.T) {
	if !sameValue(int64(3), 3.0) || !sameValue([]interface{}{int64(1)}, []interface{}{1.0}) {
		t.Error("numbers of different Go types must compare equal")
	}
	if sameValue("a", "b") || sameValue(1.0, "1") {
		t.Error("different values must differ")
	}
}
