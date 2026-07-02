package dmn

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/jeremielodi/goflow/internal/common"
)

// EvaluateDecisionTable evaluates a decision table against variables.
// Returns a single value/map for UNIQUE, FIRST and ANY hit policies, or a
// slice of them for COLLECT. A table with no matching rule returns nil.
func EvaluateDecisionTable(table *DecisionTable, variables map[string]interface{}) (interface{}, error) {
	if table == nil {
		return nil, fmt.Errorf("decision table is nil")
	}

	// Each input column's expression is evaluated once against the case
	// variables — every rule's input entry is then a unary test against
	// that single concrete value.
	inputValues := make([]interface{}, len(table.Inputs))
	for i, in := range table.Inputs {
		// Unlike Camunda 7 attributes, a DMN <text> body is always a FEEL
		// expression (no "=" convention needed there) — force expression
		// evaluation rather than EvaluateValue's literal/expression guess.
		val, err := common.EvaluateValue("="+strings.TrimSpace(in.InputExpression.Text), variables)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate input %q: %w", in.Label, err)
		}
		inputValues[i] = val
	}

	hitPolicy := strings.ToUpper(strings.TrimSpace(table.HitPolicy))
	if hitPolicy == "" {
		hitPolicy = "UNIQUE"
	}

	var matches []map[string]interface{}
	for _, rule := range table.Rules {
		matched := true
		for i := range table.Inputs {
			var test string
			if i < len(rule.InputEntries) {
				test = rule.InputEntries[i].Text
			}
			ok, err := matchesUnaryTest(test, inputValues[i])
			if err != nil {
				return nil, fmt.Errorf("rule %s: %w", rule.ID, err)
			}
			if !ok {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}

		output, err := evaluateOutputs(table.Outputs, rule.OutputEntries, variables)
		if err != nil {
			return nil, fmt.Errorf("rule %s: %w", rule.ID, err)
		}
		matches = append(matches, output)

		if hitPolicy != "COLLECT" {
			break // UNIQUE/FIRST/ANY: stop at the first match
		}
	}

	if len(matches) == 0 {
		return nil, nil
	}

	if hitPolicy == "COLLECT" {
		results := make([]interface{}, len(matches))
		for i, m := range matches {
			results[i] = singleOrMap(table.Outputs, m)
		}
		return results, nil
	}

	return singleOrMap(table.Outputs, matches[0]), nil
}

func singleOrMap(outputs []Output, values map[string]interface{}) interface{} {
	if len(outputs) == 1 {
		return values[outputKey(outputs[0], 0)]
	}
	result := make(map[string]interface{}, len(values))
	for k, v := range values {
		result[k] = v
	}
	return result
}

func outputKey(o Output, index int) string {
	if o.Name != "" {
		return o.Name
	}
	if o.Label != "" {
		return o.Label
	}
	return fmt.Sprintf("output%d", index+1)
}

func evaluateOutputs(outputs []Output, entries []Entry, variables map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(outputs))
	for i, out := range outputs {
		var text string
		if i < len(entries) {
			text = strings.TrimSpace(entries[i].Text)
		}
		value, err := evaluateOutputEntry(text, variables)
		if err != nil {
			return nil, err
		}
		result[outputKey(out, i)] = value
	}
	return result, nil
}

// evaluateOutputEntry evaluates a DMN output entry — a FEEL literal
// expression, unlike input entries which are unary tests. Bare
// true/false/number/"string" literals are handled directly; anything else
// falls back to full FEEL/CEL evaluation (e.g. a variable reference).
func evaluateOutputEntry(text string, variables map[string]interface{}) (interface{}, error) {
	if text == "" {
		return nil, nil
	}
	switch text {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	if n, err := strconv.ParseFloat(text, 64); err == nil {
		return n, nil
	}
	if len(text) >= 2 && strings.HasPrefix(text, `"`) && strings.HasSuffix(text, `"`) {
		return text[1 : len(text)-1], nil
	}
	return common.EvaluateValue("="+text, variables)
}

var rangeRe = regexp.MustCompile(`^([\[(])\s*(.+?)\s*\.\.\s*(.+?)\s*([\])])$`)

// matchesUnaryTest checks a DMN unary test (e.g. `< 1000`, `[100..500]`,
// `"gold","platinum"`, or `-` for wildcard) against a concrete value.
func matchesUnaryTest(test string, value interface{}) (bool, error) {
	test = strings.TrimSpace(test)
	if test == "" || test == "-" {
		return true, nil
	}

	for _, part := range splitTopLevel(test, ',') {
		ok, err := matchesSingleUnaryTest(strings.TrimSpace(part), value)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func matchesSingleUnaryTest(test string, value interface{}) (bool, error) {
	if m := rangeRe.FindStringSubmatch(test); m != nil {
		lowIncl, highIncl := m[1] == "[", m[4] == "]"
		low, err := parseLiteral(m[2])
		if err != nil {
			return false, err
		}
		high, err := parseLiteral(m[3])
		if err != nil {
			return false, err
		}
		cmpLow, err := compare(value, low)
		if err != nil {
			return false, err
		}
		cmpHigh, err := compare(value, high)
		if err != nil {
			return false, err
		}
		return (cmpLow > 0 || (lowIncl && cmpLow == 0)) && (cmpHigh < 0 || (highIncl && cmpHigh == 0)), nil
	}

	for _, op := range []string{"<=", ">=", "<", ">"} {
		if strings.HasPrefix(test, op) {
			operand, err := parseLiteral(strings.TrimSpace(test[len(op):]))
			if err != nil {
				return false, err
			}
			cmp, err := compare(value, operand)
			if err != nil {
				return false, err
			}
			switch op {
			case "<=":
				return cmp <= 0, nil
			case ">=":
				return cmp >= 0, nil
			case "<":
				return cmp < 0, nil
			case ">":
				return cmp > 0, nil
			}
		}
	}

	literal, err := parseLiteral(test)
	if err != nil {
		return false, err
	}
	cmp, err := compare(value, literal)
	if err != nil {
		return false, err
	}
	return cmp == 0, nil
}

// parseLiteral parses a DMN literal: a quoted string, a number, or (relaxed)
// a bare word treated as a string literal.
func parseLiteral(s string) (interface{}, error) {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && (strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) || strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) {
		return s[1 : len(s)-1], nil
	}
	if n, err := strconv.ParseFloat(s, 64); err == nil {
		return n, nil
	}
	return s, nil
}

func compare(a, b interface{}) (int, error) {
	if af, aok := toFloat(a); aok {
		if bf, bok := toFloat(b); bok {
			switch {
			case af < bf:
				return -1, nil
			case af > bf:
				return 1, nil
			default:
				return 0, nil
			}
		}
	}
	return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b)), nil
}

func toFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// splitTopLevel splits s on sep, ignoring separators inside single/double
// quoted substrings (DMN unary test lists like `"gold","platinum"`).
func splitTopLevel(s string, sep byte) []string {
	var parts []string
	var buf strings.Builder
	inQuotes := false
	var quoteChar byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inQuotes {
			buf.WriteByte(c)
			if c == quoteChar {
				inQuotes = false
			}
			continue
		}
		if c == '"' || c == '\'' {
			inQuotes = true
			quoteChar = c
			buf.WriteByte(c)
			continue
		}
		if c == sep {
			parts = append(parts, buf.String())
			buf.Reset()
			continue
		}
		buf.WriteByte(c)
	}
	parts = append(parts, buf.String())
	return parts
}
