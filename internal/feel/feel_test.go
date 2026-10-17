package feel

import (
	"reflect"
	"strings"
	"testing"
)

var testVars = map[string]interface{}{
	"amount":   250.0,
	"price":    0.1,
	"count":    3,
	"name":     "Alice",
	"approved": true,
	"tags":     []interface{}{"gold", "vip"},
	"order": map[string]interface{}{
		"id":    "O-1",
		"total": 99.5,
		"items": []interface{}{
			map[string]interface{}{"sku": "A", "qty": 2.0, "price": 10.0},
			map[string]interface{}{"sku": "B", "qty": 1.0, "price": 25.0},
		},
	},
	"nums": []interface{}{5.0, 1.0, 4.0},
}

func TestEvaluate(t *testing.T) {
	cases := []struct {
		expr string
		want interface{}
	}{
		// literals and arithmetic, on exact decimals
		{`1 + 2 * 3`, 7.0},
		{`(1 + 2) * 3`, 9.0},
		{`0.1 + 0.2 = 0.3`, true},
		{`price * 3 = 0.3`, true},
		{`10 / 4`, 2.5},
		{`2 ** 10`, 1024.0},
		{`-amount`, -250.0},
		{`1 / 0`, nil},
		{`"a" + "b"`, "ab"},
		{`1 + "a"`, nil},
		// comparisons and logic
		{`amount > 100`, true},
		{`amount >= 250 and approved`, true},
		{`amount < 100 or name = "Alice"`, true},
		{`name != "Bob"`, true},
		{`missing = null`, true},
		{`missing > 5`, nil},
		{`false and missing`, false},
		{`true and missing`, nil},
		{`true or missing`, true},
		{`not(approved)`, false},
		{`amount between 100 and 300`, true},
		// in / unary tests
		{`name in ("Alice", "Bob")`, true},
		{`amount in [1..100]`, false},
		{`amount in (200..300]`, true},
		{`amount in > 200`, true},
		{`"gold" in tags`, true},
		{`count in (< 2, > 2)`, true},
		// if / for / quantifiers
		{`if amount > 100 then "high" else "low"`, "high"},
		{`if missing then 1 else 2`, 2.0},
		{`for x in nums return x * 2`, []interface{}{10.0, 2.0, 8.0}},
		{`for i in 1..3 return i`, []interface{}{1.0, 2.0, 3.0}},
		{`some x in nums satisfies x > 4`, true},
		{`every x in nums satisfies x > 4`, false},
		// lists, contexts, paths, filters
		{`[1, 2, 3][2]`, 2.0},
		{`[1, 2, 3][-1]`, 3.0},
		{`nums[item > 2]`, []interface{}{5.0, 4.0}},
		{`order.items[qty > 1].sku`, []interface{}{"A"}},
		{`order.items.sku`, []interface{}{"A", "B"}},
		{`order.total`, 99.5},
		{`{a: 1, b: a + 1}.b`, 2.0},
		{`sum(for i in order.items return i.qty * i.price)`, 45.0},
		// built-ins
		{`string length(name)`, 5.0},
		{`upper case(name)`, "ALICE"},
		{`substring("foobar", 3)`, "obar"},
		{`substring("foobar", 3, 3)`, "oba"},
		{`substring(string: "foobar", start position: 2, length: 2)`, "oo"},
		{`contains(name, "lic")`, true},
		{`starts with(name, "Al")`, true},
		{`matches("abc123", "[a-z]+[0-9]+")`, true},
		{`replace("a-b-c", "-", "+")`, "a+b+c"},
		{`split("a,b,c", ",")`, []interface{}{"a", "b", "c"}},
		{`string join(["a", "b"], "-")`, "a-b"},
		{`list contains(tags, "vip")`, true},
		{`count(nums)`, 3.0},
		{`min(nums)`, 1.0},
		{`max(5, 9, 2)`, 9.0},
		{`mean([1, 2, 3, 4])`, 2.5},
		{`append([1], 2, 3)`, []interface{}{1.0, 2.0, 3.0}},
		{`distinct values([1, 2, 1])`, []interface{}{1.0, 2.0}},
		{`sort([3, 1, 2])`, []interface{}{1.0, 2.0, 3.0}},
		{`index of([1, 2, 1], 1)`, []interface{}{1.0, 3.0}},
		{`decimal(1.255, 2)`, 1.26},
		{`decimal(2.5, 0)`, 2.0},
		{`floor(-1.5)`, -2.0},
		{`ceiling(1.2)`, 2.0},
		{`modulo(-5, 3)`, 1.0},
		{`abs(-3)`, 3.0},
		{`number("12.5") + 1`, 13.5},
		{`string(12.50)`, "12.5"},
		{`is defined(missing)`, false},
		{`get value({a: 1}, "a")`, 1.0},
		// temporal
		{`date("2024-01-31") + duration("P1D")`, "2024-02-01"},
		{`date("2024-03-01") > date("2024-02-28")`, true},
		{`date("2024-01-15").month`, 1.0},
		{`string(duration("PT90M"))`, "PT1H30M"},
		{`date("2024-03-01") - date("2024-02-01") = duration("P29D")`, true},
		{`@"2024-05-01" + duration("P1M")`, "2024-06-01"},
	}
	for _, c := range cases {
		got, err := Evaluate(c.expr, testVars)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", c.expr, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s = %#v, want %#v", c.expr, got, c.want)
		}
	}
}

func TestEvaluateErrors(t *testing.T) {
	for _, expr := range []string{
		`1 +`,
		`foo bar(1)`,
		`unknownFunction(1)`,
		`"unterminated`,
		`if true then 1`,
	} {
		if _, err := Evaluate(expr, testVars); err == nil {
			t.Errorf("%s: expected an error", expr)
		}
	}
}

func TestEvaluateBool(t *testing.T) {
	if ok, err := EvaluateBool(`amount > 100`, testVars); err != nil || !ok {
		t.Errorf("amount > 100 = %v, %v", ok, err)
	}
	if _, err := EvaluateBool(`missing`, testVars); err == nil || !strings.Contains(err.Error(), "null") {
		t.Errorf("a null condition should be an error, got %v", err)
	}
	if _, err := EvaluateBool(`amount + 1`, testVars); err == nil {
		t.Error("a numeric condition should be an error")
	}
}

func TestEvaluateUnaryTests(t *testing.T) {
	cases := []struct {
		tests string
		input interface{}
		want  bool
	}{
		{`-`, 42.0, true},
		{`< 10`, 5.0, true},
		{`< 10`, 15.0, false},
		{`[1..10]`, 10.0, true},
		{`(1..10)`, 10.0, false},
		{`"gold","silver"`, "silver", true},
		{`"gold","silver"`, "bronze", false},
		{`not("gold")`, "silver", true},
		{`not("gold")`, "gold", false},
		{`>= 100, < 0`, -1.0, true},
		{`? > amount`, 300.0, true},
		{`amount`, 250.0, true},
	}
	for _, c := range cases {
		got, err := EvaluateUnaryTests(c.tests, c.input, testVars)
		if err != nil {
			t.Errorf("%s with %v: unexpected error: %v", c.tests, c.input, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s with %v = %v, want %v", c.tests, c.input, got, c.want)
		}
	}
}
