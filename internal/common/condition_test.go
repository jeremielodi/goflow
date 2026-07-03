package common

import "testing"

// Regression: mixing a Go int and a float64 variable in the same arithmetic
// expression used to throw a CEL "no matching overload for '_+_'" runtime
// error, because CEL infers each DynType variable's concrete type from the
// Go value at Eval time and int+double has no arithmetic overload. total
// arrives as a plain Go int (e.g. set by application code), tax as a
// float64 (e.g. decoded from JSON) — a realistic mix, not a contrived one.
func TestEvaluateCondition_MixedIntFloatArithmetic(t *testing.T) {
	vars := map[string]interface{}{
		"total": 80,
		"tax":   25.5,
	}
	ok, err := EvaluateCondition("total + tax > 100", vars)
	if err != nil {
		t.Fatalf("unexpected error evaluating mixed int/float arithmetic: %v", err)
	}
	if !ok {
		t.Errorf("expected total + tax > 100 (80 + 25.5 = 105.5) to be true")
	}
}

// Regression: convertFEELToCEL used to run its "and"/"or"/"="/"not" keyword
// substitutions across the whole expression string including quoted
// literals, so a value like "cash and carry" was silently corrupted into
// "cash && carry" before comparison — making an otherwise-true condition
// evaluate to false.
func TestEvaluateCondition_StringLiteralContainingKeywords(t *testing.T) {
	vars := map[string]interface{}{
		"paymentMethod": "cash and carry",
	}
	ok, err := EvaluateCondition(`paymentMethod = "cash and carry"`, vars)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Errorf(`expected paymentMethod = "cash and carry" to be true when paymentMethod is exactly "cash and carry"`)
	}
}

// The single-quote FEEL form must still be normalized to CEL's double
// quotes, including when its content contains logical keywords.
func TestEvaluateCondition_SingleQuotedStringLiteralContainingKeywords(t *testing.T) {
	vars := map[string]interface{}{
		"status": "gold or platinum",
	}
	ok, err := EvaluateCondition(`status = 'gold or platinum'`, vars)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Errorf("expected single-quoted literal with 'or' inside to survive unmodified")
	}
}

func TestEvaluateCondition_EmptyIsAlwaysTrue(t *testing.T) {
	ok, err := EvaluateCondition("", map[string]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Errorf("expected empty condition to be true")
	}
}

func TestEvaluateCondition_LogicalOperatorsStillWork(t *testing.T) {
	vars := map[string]interface{}{"status": "gold", "amount": 500}
	ok, err := EvaluateCondition(`status = "gold" and amount > 100`, vars)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Errorf("expected the logical 'and' outside quotes to still convert to CEL's &&")
	}
}
