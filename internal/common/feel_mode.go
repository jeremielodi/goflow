package common

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/google/cel-go/common/types/ref"
	"github.com/jeremielodi/goflow/internal/feel"
	"github.com/jeremielodi/goflow/internal/metrics"
)

// FEEL expressions (the "=" prefix, Camunda 8 style) can be evaluated by
// the native FEEL interpreter (internal/feel) instead of the historical
// FEEL-to-CEL rewriting. FEEL_MODE picks:
//
//	cel    — CEL only, the historical behavior.
//	shadow — (default) both run; the CEL result is still the one used, and
//	         any disagreement is logged and counted
//	         (goflow_feel_shadow_mismatches_total), so the switch to "feel"
//	         can be validated against real traffic first.
//	feel   — the native interpreter's result is used.
//
// Camunda 7 "${...}" expressions and unprefixed conditions always take the
// CEL path: they aren't FEEL.
const (
	feelModeCEL    = "cel"
	feelModeShadow = "shadow"
	feelModeFEEL   = "feel"
)

var (
	feelModeOnce  sync.Once
	feelModeValue string
)

func feelMode() string {
	feelModeOnce.Do(func() {
		switch m := strings.ToLower(strings.TrimSpace(os.Getenv("FEEL_MODE"))); m {
		case feelModeCEL, feelModeFEEL:
			feelModeValue = m
		case "", feelModeShadow:
			feelModeValue = feelModeShadow
		default:
			log.Printf("⚠️ unknown FEEL_MODE %q — using %q", m, feelModeShadow)
			feelModeValue = feelModeShadow
		}
		log.Printf("FEEL expressions: mode %q", feelModeValue)
	})
	return feelModeValue
}

// feelSource returns the FEEL expression behind a "=" prefixed value, and
// whether it is one.
func feelSource(expr string) (string, bool) {
	trimmed := strings.TrimSpace(expr)
	if !strings.HasPrefix(trimmed, "=") {
		return "", false
	}
	return strings.TrimSpace(trimmed[1:]), true
}

// shadowCompare logs and counts a disagreement between the CEL result
// (the one used) and the native FEEL result for the same expression.
func shadowCompare(kind, expr string, celResult interface{}, celErr error, feelResult interface{}, feelErr error) {
	switch {
	case celErr != nil && feelErr != nil:
		return // both reject it
	case celErr == nil && feelErr == nil && sameValue(celResult, feelResult):
		return
	}
	metrics.FeelShadowMismatches.WithLabelValues(kind).Inc()
	if !firstMismatch(expr) {
		return
	}
	describe := func(v interface{}, err error) string {
		if err != nil {
			return "error: " + err.Error()
		}
		return fmt.Sprintf("%#v", v)
	}
	log.Printf("FEEL shadow mismatch (%s) for %q — CEL (used): %s; native FEEL: %s",
		kind, expr, describe(celResult, celErr), describe(feelResult, feelErr))
}

var (
	loggedMismatchesMu sync.Mutex
	loggedMismatches   = map[string]bool{}
)

// firstMismatch reports whether expr's mismatch hasn't been logged yet, so
// a hot expression doesn't flood the log (the metric still counts all).
func firstMismatch(expr string) bool {
	loggedMismatchesMu.Lock()
	defer loggedMismatchesMu.Unlock()
	if loggedMismatches[expr] {
		return false
	}
	if len(loggedMismatches) >= 1000 {
		loggedMismatches = map[string]bool{}
	}
	loggedMismatches[expr] = true
	return true
}

// sameValue compares results from the two engines: numbers by value
// whatever their Go type, lists and maps element-wise.
func sameValue(a, b interface{}) bool {
	return reflect.DeepEqual(normalize(a), normalize(b))
}

func normalize(v interface{}) interface{} {
	if rv, ok := v.(ref.Val); ok {
		v = rv.Value()
	}
	switch x := v.(type) {
	case nil, bool, string:
		return x
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case uint64:
		return float64(x)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		out := make([]interface{}, rv.Len())
		for i := range out {
			out[i] = normalize(rv.Index(i).Interface())
		}
		return out
	case reflect.Map:
		out := make(map[string]interface{}, rv.Len())
		for _, k := range rv.MapKeys() {
			out[fmt.Sprint(normalize(k.Interface()))] = normalize(rv.MapIndex(k).Interface())
		}
		return out
	}
	return fmt.Sprint(v)
}

// evaluateFEELCondition runs a "=" condition according to FEEL_MODE. used
// is false when the CEL path must run (mode cel, or the value isn't FEEL).
func evaluateFEELCondition(expr string, variables map[string]interface{}, celEval func() (bool, error)) (result bool, err error, used bool) {
	src, isFEEL := feelSource(expr)
	if !isFEEL {
		return false, nil, false
	}
	switch feelMode() {
	case feelModeFEEL:
		ok, err := feel.EvaluateBool(src, variables)
		return ok, err, true
	case feelModeShadow:
		celResult, celErr := celEval()
		feelResult, feelErr := feel.EvaluateBool(src, variables)
		shadowCompare("condition", expr, celResult, celErr, feelResult, feelErr)
		return celResult, celErr, true
	}
	return false, nil, false
}

// evaluateFEELValue is evaluateFEELCondition for a value expression.
func evaluateFEELValue(expr string, variables map[string]interface{}, celEval func() (interface{}, error)) (result interface{}, err error, used bool) {
	src, isFEEL := feelSource(expr)
	if !isFEEL {
		return nil, nil, false
	}
	switch feelMode() {
	case feelModeFEEL:
		v, err := feel.Evaluate(src, variables)
		return v, err, true
	case feelModeShadow:
		celResult, celErr := celEval()
		feelResult, feelErr := feel.Evaluate(src, variables)
		shadowCompare("value", expr, celResult, celErr, feelResult, feelErr)
		return celResult, celErr, true
	}
	return nil, nil, false
}

// setFEELMode forces the mode, for tests.
func setFEELMode(m string) {
	feelModeOnce.Do(func() {})
	feelModeValue = m
}

// EvaluateUnaryTests runs a DMN input entry (unary tests, always FEEL)
// against input according to FEEL_MODE: legacy is the historical matcher,
// used in modes cel and shadow.
func EvaluateUnaryTests(tests string, input interface{}, variables map[string]interface{}, legacy func() (bool, error)) (bool, error) {
	if strings.TrimSpace(tests) == "" {
		tests = "-"
	}
	switch feelMode() {
	case feelModeFEEL:
		return feel.EvaluateUnaryTests(tests, input, variables)
	case feelModeShadow:
		legacyResult, legacyErr := legacy()
		feelResult, feelErr := feel.EvaluateUnaryTests(tests, input, variables)
		shadowCompare("unaryTest", tests, legacyResult, legacyErr, feelResult, feelErr)
		return legacyResult, legacyErr
	}
	return legacy()
}
