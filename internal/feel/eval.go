package feel

import (
	"fmt"
	"math"
	"math/big"

	"sort"
	"strconv"
	"time"
)

// value is a FEEL value: nil (null), bool, string, *big.Rat (number),
// []value (list), map[string]value (context), Date, time.Time (date and
// time), DTDuration, YMDuration or *rangeValue.
type value = interface{}

// Date is a FEEL date (no time of day).
type Date struct{ time.Time }

// DTDuration is a FEEL days-and-time duration.
type DTDuration time.Duration

// YMDuration is a FEEL years-and-months duration, in months.
type YMDuration int

type rangeValue struct {
	lo, hi             value
	loClosed, hiClosed bool
}

// scope is a chain of variable maps; lookups fall through to the parent.
type scope struct {
	vars   map[string]value
	parent *scope
	// input is the value under test in a unary test ("?"); hasInput tells
	// whether one is set.
	input    value
	hasInput bool
}

func (s *scope) lookup(name string) value {
	for c := s; c != nil; c = c.parent {
		if v, ok := c.vars[name]; ok {
			return v
		}
		if name == "?" && c.hasInput {
			return c.input
		}
	}
	return nil
}

func (s *scope) child(vars map[string]value) *scope {
	return &scope{vars: vars, parent: s, input: s.input, hasInput: s.hasInput}
}

func (s *scope) testing(input value) *scope {
	return &scope{vars: map[string]value{}, parent: s, input: input, hasInput: true}
}

func eval(n node, s *scope) (value, error) {
	switch n := n.(type) {
	case *litNode:
		return n.v, nil
	case *nameNode:
		return s.lookup(n.name), nil
	case *negNode:
		x, err := eval(n.x, s)
		if err != nil {
			return nil, err
		}
		switch v := x.(type) {
		case *big.Rat:
			return new(big.Rat).Neg(v), nil
		case DTDuration:
			return -v, nil
		case YMDuration:
			return -v, nil
		}
		return nil, nil
	case *binNode:
		return evalBinary(n, s)
	case *betweenNode:
		x, err := eval(n.x, s)
		if err != nil {
			return nil, err
		}
		lo, err := eval(n.lo, s)
		if err != nil {
			return nil, err
		}
		hi, err := eval(n.hi, s)
		if err != nil {
			return nil, err
		}
		return and3(compareOp(">=", x, lo), compareOp("<=", x, hi)), nil
	case *inNode:
		x, err := eval(n.x, s)
		if err != nil {
			return nil, err
		}
		return anyTest(n.tests, x, s)
	case *ifNode:
		c, err := eval(n.cond, s)
		if err != nil {
			return nil, err
		}
		if c == true {
			return eval(n.then, s)
		}
		return eval(n.els, s)
	case *forNode:
		var out []value
		err := iterate(n.iters, s, func(inner *scope) error {
			v, err := eval(n.body, inner)
			out = append(out, v)
			return err
		})
		if out == nil {
			out = []value{}
		}
		return out, err
	case *quantNode:
		result := value(n.every)
		err := iterate(n.iters, s, func(inner *scope) error {
			v, err := eval(n.cond, inner)
			if err != nil {
				return err
			}
			if n.every {
				result = and3(result, v)
			} else {
				result = or3(result, v)
			}
			return nil
		})
		return result, err
	case *listNode:
		out := make([]value, 0, len(n.items))
		for _, item := range n.items {
			v, err := eval(item, s)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case *contextNode:
		ctx := make(map[string]value, len(n.keys))
		// Later entries can refer to earlier ones.
		inner := s.child(ctx)
		for i, k := range n.keys {
			v, err := eval(n.vals[i], inner)
			if err != nil {
				return nil, err
			}
			ctx[k] = v
		}
		return ctx, nil
	case *rangeNode:
		lo, err := eval(n.lo, s)
		if err != nil {
			return nil, err
		}
		hi, err := eval(n.hi, s)
		if err != nil {
			return nil, err
		}
		return &rangeValue{lo, hi, n.loClosed, n.hiClosed}, nil
	case *pathNode:
		x, err := eval(n.x, s)
		if err != nil {
			return nil, err
		}
		return path(x, n.name), nil
	case *filterNode:
		return evalFilter(n, s)
	case *callNode:
		return evalCall(n, s)
	case *cmpTestNode, *anyTestNode:
		// A unary test used as an expression tests the "?" input.
		return anyTest([]node{n}, s.lookup("?"), s)
	case *notTestNode:
		if s.hasInput {
			ok, err := anyTest(n.tests, s.input, s)
			if err != nil {
				return nil, err
			}
			return not3(ok), nil
		}
		if len(n.tests) != 1 {
			return nil, fmt.Errorf("not() takes one argument, got %d", len(n.tests))
		}
		v, err := eval(n.tests[0], s)
		if err != nil {
			return nil, err
		}
		return not3(v), nil
	}
	return nil, fmt.Errorf("cannot evaluate %T", n)
}

// iterate runs fn once per combination of the iterators' values (nested,
// first iterator outermost), each in a scope binding the iterator names.
func iterate(iters []iterator, s *scope, fn func(*scope) error) error {
	if len(iters) == 0 {
		return fn(s)
	}
	coll, err := eval(iters[0].in, s)
	if err != nil {
		return err
	}
	items, err := iterationValues(coll)
	if err != nil {
		return err
	}
	for _, item := range items {
		inner := s.child(map[string]value{iters[0].name: item})
		if err := iterate(iters[1:], inner, fn); err != nil {
			return err
		}
	}
	return nil
}

// iterationValues returns what a for/some/every iterates over: a list's
// items, or the integers of a numeric range.
func iterationValues(coll value) ([]value, error) {
	switch c := coll.(type) {
	case []value:
		return c, nil
	case *rangeValue:
		lo, okLo := c.lo.(*big.Rat)
		hi, okHi := c.hi.(*big.Rat)
		if !okLo || !okHi || !lo.IsInt() || !hi.IsInt() {
			return nil, fmt.Errorf("can only iterate over integer ranges")
		}
		a, b := lo.Num().Int64(), hi.Num().Int64()
		if !c.loClosed {
			a++
		}
		if !c.hiClosed {
			b--
		}
		if b-a > 100000 || a-b > 100000 {
			return nil, fmt.Errorf("range too large to iterate")
		}
		var out []value
		if a <= b {
			for i := a; i <= b; i++ {
				out = append(out, new(big.Rat).SetInt64(i))
			}
		} else {
			for i := a; i >= b; i-- {
				out = append(out, new(big.Rat).SetInt64(i))
			}
		}
		return out, nil
	case nil:
		return nil, nil
	}
	return []value{coll}, nil
}

func evalBinary(n *binNode, s *scope) (value, error) {
	l, err := eval(n.l, s)
	if err != nil {
		return nil, err
	}
	// and/or short-circuit on a decided left side.
	if n.op == "and" && l == false {
		return false, nil
	}
	if n.op == "or" && l == true {
		return true, nil
	}
	r, err := eval(n.r, s)
	if err != nil {
		return nil, err
	}
	switch n.op {
	case "and":
		return and3(l, r), nil
	case "or":
		return or3(l, r), nil
	case "=", "!=", "<", "<=", ">", ">=":
		return compareOp(n.op, l, r), nil
	}
	return arithmetic(n.op, l, r), nil
}

func toBool3(v value) (bool, bool) { b, ok := v.(bool); return b, ok }

func and3(a, b value) value {
	x, okX := toBool3(a)
	y, okY := toBool3(b)
	switch {
	case okX && !x, okY && !y:
		return false
	case okX && okY:
		return true
	}
	return nil
}

func or3(a, b value) value {
	x, okX := toBool3(a)
	y, okY := toBool3(b)
	switch {
	case okX && x, okY && y:
		return true
	case okX && okY:
		return false
	}
	return nil
}

func not3(v value) value {
	if b, ok := v.(bool); ok {
		return !b
	}
	return nil
}

// compareOp applies a comparison with FEEL semantics: = and != work on any
// values (null equals only null); ordering comparisons need two values of
// the same orderable type and are null otherwise.
func compareOp(op string, l, r value) value {
	if op == "=" || op == "!=" {
		eq, ok := equal(l, r)
		if !ok {
			return nil
		}
		if op == "!=" {
			return !eq
		}
		return eq
	}
	c, ok := order(l, r)
	if !ok {
		return nil
	}
	switch op {
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	default:
		return c >= 0
	}
}

// equal reports whether l and r are equal; ok is false when they are of
// incomparable types (which FEEL treats as null).
func equal(l, r value) (eq bool, ok bool) {
	if l == nil || r == nil {
		return l == nil && r == nil, true
	}
	switch a := l.(type) {
	case []value:
		b, isList := r.([]value)
		if !isList {
			return false, false
		}
		if len(a) != len(b) {
			return false, true
		}
		for i := range a {
			if e, ok := equal(a[i], b[i]); !ok || !e {
				return false, ok
			}
		}
		return true, true
	case map[string]value:
		b, isCtx := r.(map[string]value)
		if !isCtx {
			return false, false
		}
		if len(a) != len(b) {
			return false, true
		}
		for k, av := range a {
			bv, present := b[k]
			if !present {
				return false, true
			}
			if e, ok := equal(av, bv); !ok || !e {
				return false, ok
			}
		}
		return true, true
	case bool:
		b, isBool := r.(bool)
		return a == b, isBool
	}
	c, ok := order(l, r)
	return c == 0, ok
}

// order compares two values of the same orderable type.
func order(l, r value) (int, bool) {
	switch a := l.(type) {
	case *big.Rat:
		if b, ok := r.(*big.Rat); ok {
			return a.Cmp(b), true
		}
	case string:
		if b, ok := r.(string); ok {
			switch {
			case a < b:
				return -1, true
			case a > b:
				return 1, true
			}
			return 0, true
		}
	case Date:
		if b, ok := r.(Date); ok {
			return a.Compare(b.Time), true
		}
	case time.Time:
		if b, ok := r.(time.Time); ok {
			return a.Compare(b), true
		}
	case DTDuration:
		if b, ok := r.(DTDuration); ok {
			return cmpInt(int64(a), int64(b)), true
		}
	case YMDuration:
		if b, ok := r.(YMDuration); ok {
			return cmpInt(int64(a), int64(b)), true
		}
	}
	return 0, false
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// arithmetic applies + - * / ** with FEEL semantics; null when the
// operand types don't support the operation (or on division by zero).
func arithmetic(op string, l, r value) value {
	switch a := l.(type) {
	case *big.Rat:
		b, ok := r.(*big.Rat)
		if !ok {
			if d, isDT := r.(DTDuration); isDT && op == "*" {
				return DTDuration(ratTimesInt64(a, int64(d)))
			}
			return nil
		}
		switch op {
		case "+":
			return new(big.Rat).Add(a, b)
		case "-":
			return new(big.Rat).Sub(a, b)
		case "*":
			return new(big.Rat).Mul(a, b)
		case "/":
			if b.Sign() == 0 {
				return nil
			}
			return new(big.Rat).Quo(a, b)
		case "**":
			return power(a, b)
		}
	case string:
		if b, ok := r.(string); ok && op == "+" {
			return a + b
		}
	case Date:
		return temporalArithmetic(op, a.Time, r, true)
	case time.Time:
		return temporalArithmetic(op, a, r, false)
	case DTDuration:
		switch b := r.(type) {
		case DTDuration:
			switch op {
			case "+":
				return a + b
			case "-":
				return a - b
			}
		case *big.Rat:
			switch op {
			case "*":
				return DTDuration(ratTimesInt64(b, int64(a)))
			case "/":
				if b.Sign() == 0 {
					return nil
				}
				return DTDuration(ratTimesInt64(new(big.Rat).Inv(b), int64(a)))
			}
		case Date:
			if op == "+" {
				return Date{b.Add(time.Duration(a))}
			}
		case time.Time:
			if op == "+" {
				return b.Add(time.Duration(a))
			}
		}
	case YMDuration:
		switch b := r.(type) {
		case YMDuration:
			switch op {
			case "+":
				return a + b
			case "-":
				return a - b
			}
		case Date:
			if op == "+" {
				return Date{b.AddDate(0, int(a), 0)}
			}
		case time.Time:
			if op == "+" {
				return b.AddDate(0, int(a), 0)
			}
		}
	}
	return nil
}

func temporalArithmetic(op string, t time.Time, r value, isDate bool) value {
	wrap := func(x time.Time) value {
		if isDate {
			return Date{x}
		}
		return x
	}
	switch b := r.(type) {
	case DTDuration:
		switch op {
		case "+":
			return wrap(t.Add(time.Duration(b)))
		case "-":
			return wrap(t.Add(-time.Duration(b)))
		}
	case YMDuration:
		switch op {
		case "+":
			return wrap(t.AddDate(0, int(b), 0))
		case "-":
			return wrap(t.AddDate(0, -int(b), 0))
		}
	case Date:
		if op == "-" {
			return DTDuration(t.Sub(b.Time))
		}
	case time.Time:
		if op == "-" {
			return DTDuration(t.Sub(b))
		}
	}
	return nil
}

func ratTimesInt64(r *big.Rat, n int64) int64 {
	f, _ := new(big.Rat).Mul(r, new(big.Rat).SetInt64(n)).Float64()
	return int64(math.Round(f))
}

func power(base, exp *big.Rat) value {
	if exp.IsInt() && exp.Num().IsInt64() {
		e := exp.Num().Int64()
		if e > 10000 || e < -10000 {
			return nil
		}
		result := new(big.Rat).SetInt64(1)
		b := new(big.Rat).Set(base)
		neg := e < 0
		if neg {
			e = -e
		}
		for ; e > 0; e-- {
			result.Mul(result, b)
		}
		if neg {
			if result.Sign() == 0 {
				return nil
			}
			result.Inv(result)
		}
		return result
	}
	bf, _ := base.Float64()
	ef, _ := exp.Float64()
	return ratFromFloat(math.Pow(bf, ef))
}

func ratFromFloat(f float64) value {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	r, ok := new(big.Rat).SetString(strconv.FormatFloat(f, 'g', -1, 64))
	if !ok {
		return nil
	}
	return r
}

// path reads a context entry, maps a path over a list of contexts, or
// reads a temporal component (date.year, ...).
func path(x value, name string) value {
	switch v := x.(type) {
	case map[string]value:
		return v[name]
	case []value:
		out := make([]value, 0, len(v))
		for _, item := range v {
			out = append(out, path(item, name))
		}
		return out
	case Date:
		return temporalComponent(v.Time, name)
	case time.Time:
		return temporalComponent(v, name)
	}
	return nil
}

func temporalComponent(t time.Time, name string) value {
	n := func(i int) value { return new(big.Rat).SetInt64(int64(i)) }
	switch name {
	case "year":
		return n(t.Year())
	case "month":
		return n(int(t.Month()))
	case "day":
		return n(t.Day())
	case "hour":
		return n(t.Hour())
	case "minute":
		return n(t.Minute())
	case "second":
		return n(t.Second())
	case "weekday":
		wd := int(t.Weekday())
		if wd == 0 {
			wd = 7
		}
		return n(wd)
	}
	return nil
}

// evalFilter indexes a list (a number, 1-based, negative from the end) or
// filters it (a boolean evaluated per item, with the item bound to "item"
// and, for a context item, its entries bound by name).
func evalFilter(n *filterNode, s *scope) (value, error) {
	x, err := eval(n.x, s)
	if err != nil {
		return nil, err
	}
	list, isList := x.([]value)
	if !isList {
		if x == nil {
			return nil, nil
		}
		list = []value{x}
	}
	if idx, err := eval(n.cond, s); err == nil {
		if r, ok := idx.(*big.Rat); ok {
			if !r.IsInt() {
				return nil, nil
			}
			i := int(r.Num().Int64())
			switch {
			case i > 0 && i <= len(list):
				return list[i-1], nil
			case i < 0 && -i <= len(list):
				return list[len(list)+i], nil
			}
			return nil, nil
		}
	}
	out := []value{}
	for _, item := range list {
		vars := map[string]value{"item": item}
		if ctx, ok := item.(map[string]value); ok {
			for k, v := range ctx {
				vars[k] = v
			}
			vars["item"] = item
		}
		keep, err := eval(n.cond, s.child(vars))
		if err != nil {
			return nil, err
		}
		if keep == true {
			out = append(out, item)
		}
	}
	return out, nil
}

// anyTest reports whether x passes at least one of the unary tests.
func anyTest(tests []node, x value, s *scope) (value, error) {
	var result value = false
	for _, t := range tests {
		ok, err := unaryTest(t, x, s)
		if err != nil {
			return nil, err
		}
		result = or3(result, ok)
		if result == true {
			return true, nil
		}
	}
	return result, nil
}

func unaryTest(t node, x value, s *scope) (value, error) {
	ts := s.testing(x)
	switch t := t.(type) {
	case *anyTestNode:
		return true, nil
	case *cmpTestNode:
		r, err := eval(t.x, ts)
		if err != nil {
			return nil, err
		}
		return compareOp(t.op, x, r), nil
	case *notTestNode:
		ok, err := anyTest(t.tests, x, ts)
		if err != nil {
			return nil, err
		}
		return not3(ok), nil
	}
	v, err := eval(t, ts)
	if err != nil {
		return nil, err
	}
	switch tv := v.(type) {
	case *rangeValue:
		return inRange(x, tv), nil
	case []value:
		if xs, isList := x.([]value); isList {
			eq, _ := equal(xs, tv)
			return eq, nil
		}
		for _, item := range tv {
			if eq, ok := equal(x, item); ok && eq {
				return true, nil
			}
		}
		return false, nil
	case bool:
		// A boolean test built on "?" (e.g. "? > 5") is the verdict itself;
		// otherwise it's a value compared for equality.
		if usesInput(t) {
			return tv, nil
		}
	}
	eq, ok := equal(x, v)
	if !ok {
		return nil, nil
	}
	return eq, nil
}

func inRange(x value, rv *rangeValue) value {
	var lo, hi value = true, true
	if rv.lo != nil {
		if rv.loClosed {
			lo = compareOp(">=", x, rv.lo)
		} else {
			lo = compareOp(">", x, rv.lo)
		}
	}
	if rv.hi != nil {
		if rv.hiClosed {
			hi = compareOp("<=", x, rv.hi)
		} else {
			hi = compareOp("<", x, rv.hi)
		}
	}
	return and3(lo, hi)
}

// sortValues sorts numbers, strings or temporal values in place, ascending;
// false when the values aren't mutually orderable.
func sortValues(items []value) bool {
	ok := true
	sort.SliceStable(items, func(i, j int) bool {
		c, comparable := order(items[i], items[j])
		if !comparable {
			ok = false
		}
		return c < 0
	})
	return ok
}

// usesInput reports whether an expression refers to the "?" input.
func usesInput(n node) bool {
	any := func(nodes ...node) bool {
		for _, c := range nodes {
			if c != nil && usesInput(c) {
				return true
			}
		}
		return false
	}
	switch n := n.(type) {
	case *nameNode:
		return n.name == "?"
	case *negNode:
		return any(n.x)
	case *binNode:
		return any(n.l, n.r)
	case *betweenNode:
		return any(n.x, n.lo, n.hi)
	case *inNode:
		return any(n.x) || any(n.tests...)
	case *ifNode:
		return any(n.cond, n.then, n.els)
	case *forNode:
		for _, it := range n.iters {
			if any(it.in) {
				return true
			}
		}
		return any(n.body)
	case *quantNode:
		for _, it := range n.iters {
			if any(it.in) {
				return true
			}
		}
		return any(n.cond)
	case *listNode:
		return any(n.items...)
	case *contextNode:
		return any(n.vals...)
	case *rangeNode:
		return any(n.lo, n.hi)
	case *pathNode:
		return any(n.x)
	case *filterNode:
		return any(n.x, n.cond)
	case *callNode:
		for _, a := range n.named {
			if any(a) {
				return true
			}
		}
		return any(n.args...)
	case *cmpTestNode:
		return true
	case *notTestNode:
		return any(n.tests...)
	}
	return false
}
