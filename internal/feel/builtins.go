package feel

import (
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

type builtin struct {
	params []string // parameter names, for named arguments
	fn     func(args []value) (value, error)
}

var builtins map[string]builtin

// multiWordFunctions are built-in function names containing spaces, which
// the lexer splits into several name tokens.
var multiWordFunctions = map[string]bool{}

func init() {
	num := func(i int) value { return new(big.Rat).SetInt64(int64(i)) }
	str := func(v value) (string, bool) { s, ok := v.(string); return s, ok }
	rat := func(v value) (*big.Rat, bool) { r, ok := v.(*big.Rat); return r, ok }
	intArg := func(v value) (int, bool) {
		r, ok := v.(*big.Rat)
		if !ok || !r.IsInt() || !r.Num().IsInt64() {
			return 0, false
		}
		return int(r.Num().Int64()), true
	}
	// listArgs accepts f(list) or f(a, b, c).
	listArgs := func(args []value) []value {
		if len(args) == 1 {
			if l, ok := args[0].([]value); ok {
				return l
			}
		}
		return args
	}
	strFn := func(f func(string) value) func([]value) (value, error) {
		return func(args []value) (value, error) {
			if s, ok := str(args[0]); ok {
				return f(s), nil
			}
			return nil, nil
		}
	}
	str2 := func(f func(a, b string) value) func([]value) (value, error) {
		return func(args []value) (value, error) {
			a, okA := str(args[0])
			b, okB := str(args[1])
			if !okA || !okB {
				return nil, nil
			}
			return f(a, b), nil
		}
	}
	numFn := func(f func(*big.Rat) value) func([]value) (value, error) {
		return func(args []value) (value, error) {
			if r, ok := rat(args[0]); ok {
				return f(r), nil
			}
			return nil, nil
		}
	}

	builtins = map[string]builtin{
		// Conversion
		"string": {[]string{"from"}, func(a []value) (value, error) { return toString(a[0]), nil }},
		"number": {[]string{"from"}, func(a []value) (value, error) {
			s, ok := str(a[0])
			if !ok {
				if r, isNum := rat(a[0]); isNum {
					return r, nil
				}
				return nil, nil
			}
			r, ok := new(big.Rat).SetString(strings.TrimSpace(s))
			if !ok {
				return nil, nil
			}
			return r, nil
		}},
		"date": {[]string{"from"}, func(a []value) (value, error) {
			switch v := a[0].(type) {
			case Date:
				return v, nil
			case time.Time:
				y, m, d := v.Date()
				return Date{time.Date(y, m, d, 0, 0, 0, 0, time.UTC)}, nil
			case string:
				t, err := time.Parse("2006-01-02", v)
				if err != nil {
					return nil, nil
				}
				return Date{t}, nil
			}
			if len(a) == 3 {
				y, okY := intArg(a[0])
				m, okM := intArg(a[1])
				d, okD := intArg(a[2])
				if okY && okM && okD {
					return Date{time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)}, nil
				}
			}
			return nil, nil
		}},
		"date and time": {[]string{"from"}, func(a []value) (value, error) {
			switch v := a[0].(type) {
			case time.Time:
				return v, nil
			case Date:
				return v.Time, nil
			case string:
				if t, ok := parseDateTime(v); ok {
					return t, nil
				}
			}
			return nil, nil
		}},
		"duration": {[]string{"from"}, func(a []value) (value, error) {
			if s, ok := str(a[0]); ok {
				if d, err := parseDuration(s); err == nil {
					return d, nil
				}
			}
			return nil, nil
		}},
		"now":   {nil, func([]value) (value, error) { return time.Now().UTC(), nil }},
		"today": {nil, func([]value) (value, error) { return Date{time.Now().UTC().Truncate(24 * time.Hour)}, nil }},

		// Boolean / null
		"is defined": {[]string{"value"}, func(a []value) (value, error) { return a[0] != nil, nil }},

		// String
		"string length": {[]string{"string"}, strFn(func(s string) value { return num(utf8.RuneCountInString(s)) })},
		"upper case":    {[]string{"string"}, strFn(func(s string) value { return strings.ToUpper(s) })},
		"lower case":    {[]string{"string"}, strFn(func(s string) value { return strings.ToLower(s) })},
		"trim":          {[]string{"string"}, strFn(func(s string) value { return strings.TrimSpace(s) })},
		"contains":      {[]string{"string", "match"}, str2(func(a, b string) value { return strings.Contains(a, b) })},
		"starts with":   {[]string{"string", "match"}, str2(func(a, b string) value { return strings.HasPrefix(a, b) })},
		"ends with":     {[]string{"string", "match"}, str2(func(a, b string) value { return strings.HasSuffix(a, b) })},
		"substring before": {[]string{"string", "match"}, str2(func(a, b string) value {
			if i := strings.Index(a, b); i >= 0 {
				return a[:i]
			}
			return ""
		})},
		"substring after": {[]string{"string", "match"}, str2(func(a, b string) value {
			if i := strings.Index(a, b); i >= 0 {
				return a[i+len(b):]
			}
			return ""
		})},
		"substring": {[]string{"string", "start position", "length"}, func(a []value) (value, error) {
			s, ok := str(a[0])
			start, okStart := intArg(a[1])
			if !ok || !okStart {
				return nil, nil
			}
			r := []rune(s)
			// 1-based; negative counts from the end.
			if start < 0 {
				start = len(r) + start + 1
			}
			if start < 1 {
				start = 1
			}
			if start > len(r) {
				return "", nil
			}
			end := len(r)
			if len(a) > 2 && a[2] != nil {
				l, okL := intArg(a[2])
				if !okL {
					return nil, nil
				}
				if start-1+l < end {
					end = start - 1 + l
				}
			}
			return string(r[start-1 : end]), nil
		}},
		"matches": {[]string{"input", "pattern", "flags"}, func(a []value) (value, error) {
			s, okS := str(a[0])
			p, okP := str(a[1])
			if !okS || !okP {
				return nil, nil
			}
			re, err := regexp.Compile(regexFlags(a) + p)
			if err != nil {
				return nil, nil
			}
			return re.MatchString(s), nil
		}},
		"replace": {[]string{"input", "pattern", "replacement", "flags"}, func(a []value) (value, error) {
			s, okS := str(a[0])
			p, okP := str(a[1])
			rep, okR := str(a[2])
			if !okS || !okP || !okR {
				return nil, nil
			}
			flags := ""
			if len(a) > 3 {
				flags = regexFlags([]value{nil, nil, a[3]})
			}
			re, err := regexp.Compile(flags + p)
			if err != nil {
				return nil, nil
			}
			return re.ReplaceAllString(s, rep), nil
		}},
		"split": {[]string{"string", "delimiter"}, func(a []value) (value, error) {
			s, okS := str(a[0])
			d, okD := str(a[1])
			if !okS || !okD {
				return nil, nil
			}
			re, err := regexp.Compile(d)
			if err != nil {
				return nil, nil
			}
			parts := re.Split(s, -1)
			out := make([]value, len(parts))
			for i, p := range parts {
				out[i] = p
			}
			return out, nil
		}},
		"string join": {[]string{"list", "delimiter"}, func(a []value) (value, error) {
			l, ok := a[0].([]value)
			if !ok {
				return nil, nil
			}
			delim := ""
			if len(a) > 1 && a[1] != nil {
				if d, okD := str(a[1]); okD {
					delim = d
				}
			}
			parts := make([]string, 0, len(l))
			for _, item := range l {
				if item == nil {
					continue
				}
				s, isStr := item.(string)
				if !isStr {
					return nil, nil
				}
				parts = append(parts, s)
			}
			return strings.Join(parts, delim), nil
		}},

		// List
		"list contains": {[]string{"list", "element"}, func(a []value) (value, error) {
			l, ok := a[0].([]value)
			if !ok {
				return nil, nil
			}
			for _, item := range l {
				if eq, ok := equal(item, a[1]); ok && eq {
					return true, nil
				}
			}
			return false, nil
		}},
		"count": {[]string{"list"}, func(a []value) (value, error) { return num(len(listArgs(a))), nil }},
		"is empty": {[]string{"list"}, func(a []value) (value, error) {
			l, ok := a[0].([]value)
			if !ok {
				return nil, nil
			}
			return len(l) == 0, nil
		}},
		"min": {[]string{"list"}, func(a []value) (value, error) { return extreme(listArgs(a), -1), nil }},
		"max": {[]string{"list"}, func(a []value) (value, error) { return extreme(listArgs(a), 1), nil }},
		"sum": {[]string{"list"}, func(a []value) (value, error) {
			total := new(big.Rat)
			for _, v := range listArgs(a) {
				r, ok := rat(v)
				if !ok {
					return nil, nil
				}
				total.Add(total, r)
			}
			return total, nil
		}},
		"product": {[]string{"list"}, func(a []value) (value, error) {
			items := listArgs(a)
			if len(items) == 0 {
				return nil, nil
			}
			total := new(big.Rat).SetInt64(1)
			for _, v := range items {
				r, ok := rat(v)
				if !ok {
					return nil, nil
				}
				total.Mul(total, r)
			}
			return total, nil
		}},
		"mean": {[]string{"list"}, func(a []value) (value, error) {
			items := listArgs(a)
			if len(items) == 0 {
				return nil, nil
			}
			total := new(big.Rat)
			for _, v := range items {
				r, ok := rat(v)
				if !ok {
					return nil, nil
				}
				total.Add(total, r)
			}
			return total.Quo(total, new(big.Rat).SetInt64(int64(len(items)))), nil
		}},
		"all": {[]string{"list"}, func(a []value) (value, error) {
			var result value = true
			for _, v := range listArgs(a) {
				result = and3(result, v)
			}
			return result, nil
		}},
		"any": {[]string{"list"}, func(a []value) (value, error) {
			var result value = false
			for _, v := range listArgs(a) {
				result = or3(result, v)
			}
			return result, nil
		}},
		"append": {[]string{"list"}, func(a []value) (value, error) {
			l, ok := a[0].([]value)
			if !ok {
				return nil, nil
			}
			out := append(append([]value{}, l...), a[1:]...)
			return out, nil
		}},
		"concatenate": {[]string{"lists"}, func(a []value) (value, error) {
			out := []value{}
			for _, v := range a {
				l, ok := v.([]value)
				if !ok {
					return nil, nil
				}
				out = append(out, l...)
			}
			return out, nil
		}},
		"index of": {[]string{"list", "match"}, func(a []value) (value, error) {
			l, ok := a[0].([]value)
			if !ok {
				return nil, nil
			}
			out := []value{}
			for i, item := range l {
				if eq, ok := equal(item, a[1]); ok && eq {
					out = append(out, num(i+1))
				}
			}
			return out, nil
		}},
		"distinct values": {[]string{"list"}, func(a []value) (value, error) {
			l, ok := a[0].([]value)
			if !ok {
				return nil, nil
			}
			out := []value{}
			for _, item := range l {
				dup := false
				for _, seen := range out {
					if eq, ok := equal(item, seen); ok && eq {
						dup = true
						break
					}
				}
				if !dup {
					out = append(out, item)
				}
			}
			return out, nil
		}},
		"flatten": {[]string{"list"}, func(a []value) (value, error) {
			l, ok := a[0].([]value)
			if !ok {
				return nil, nil
			}
			return flatten(l), nil
		}},
		"reverse": {[]string{"list"}, func(a []value) (value, error) {
			l, ok := a[0].([]value)
			if !ok {
				return nil, nil
			}
			out := make([]value, len(l))
			for i, item := range l {
				out[len(l)-1-i] = item
			}
			return out, nil
		}},
		"sort": {[]string{"list"}, func(a []value) (value, error) {
			l, ok := a[0].([]value)
			if !ok {
				return nil, nil
			}
			out := append([]value{}, l...)
			if !sortValues(out) {
				return nil, nil
			}
			return out, nil
		}},
		"sublist": {[]string{"list", "start position", "length"}, func(a []value) (value, error) {
			l, ok := a[0].([]value)
			start, okStart := intArg(a[1])
			if !ok || !okStart {
				return nil, nil
			}
			if start < 0 {
				start = len(l) + start + 1
			}
			if start < 1 || start > len(l) {
				return nil, nil
			}
			end := len(l)
			if len(a) > 2 && a[2] != nil {
				n, okN := intArg(a[2])
				if !okN {
					return nil, nil
				}
				if start-1+n < end {
					end = start - 1 + n
				}
			}
			return append([]value{}, l[start-1:end]...), nil
		}},
		"remove": {[]string{"list", "position"}, func(a []value) (value, error) {
			l, ok := a[0].([]value)
			pos, okPos := intArg(a[1])
			if !ok || !okPos || pos == 0 || pos > len(l) || -pos > len(l) {
				return nil, nil
			}
			if pos < 0 {
				pos = len(l) + pos + 1
			}
			out := append(append([]value{}, l[:pos-1]...), l[pos:]...)
			return out, nil
		}},

		// Numeric
		"decimal": {[]string{"n", "scale"}, func(a []value) (value, error) {
			r, ok := rat(a[0])
			scale, okS := intArg(a[1])
			if !ok || !okS {
				return nil, nil
			}
			return roundHalfEven(r, scale), nil
		}},
		"floor": {[]string{"n"}, numFn(func(r *big.Rat) value { return floorRat(r) })},
		"ceiling": {[]string{"n"}, numFn(func(r *big.Rat) value {
			f := floorRat(r)
			if f.Cmp(r) != 0 {
				f.Add(f, big.NewRat(1, 1))
			}
			return f
		})},
		"abs": {[]string{"n"}, numFn(func(r *big.Rat) value { return new(big.Rat).Abs(r) })},
		"modulo": {[]string{"dividend", "divisor"}, func(a []value) (value, error) {
			x, okX := rat(a[0])
			y, okY := rat(a[1])
			if !okX || !okY || y.Sign() == 0 {
				return nil, nil
			}
			// FEEL: the result takes the sign of the divisor.
			q := floorRat(new(big.Rat).Quo(x, y))
			return new(big.Rat).Sub(x, q.Mul(q, y)), nil
		}},
		"sqrt": {[]string{"number"}, numFn(func(r *big.Rat) value {
			if r.Sign() < 0 {
				return nil
			}
			f, _ := r.Float64()
			return ratFromFloat(math.Sqrt(f))
		})},
		"odd": {[]string{"number"}, numFn(func(r *big.Rat) value {
			return r.IsInt() && new(big.Int).Abs(r.Num()).Bit(0) == 1
		})},
		"even": {[]string{"number"}, numFn(func(r *big.Rat) value {
			return r.IsInt() && r.Num().Bit(0) == 0
		})},

		// Context
		"get value": {[]string{"m", "key"}, func(a []value) (value, error) {
			ctx, ok := a[0].(map[string]value)
			k, okK := str(a[1])
			if !ok || !okK {
				return nil, nil
			}
			return ctx[k], nil
		}},
		"get entries": {[]string{"m"}, func(a []value) (value, error) {
			ctx, ok := a[0].(map[string]value)
			if !ok {
				return nil, nil
			}
			keys := make([]string, 0, len(ctx))
			for k := range ctx {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			out := make([]value, 0, len(keys))
			for _, k := range keys {
				out = append(out, map[string]value{"key": k, "value": ctx[k]})
			}
			return out, nil
		}},
	}

	for name := range builtins {
		if strings.Contains(name, " ") {
			multiWordFunctions[name] = true
		}
	}
}

func regexFlags(a []value) string {
	if len(a) < 3 || a[2] == nil {
		return ""
	}
	f, ok := a[2].(string)
	if !ok {
		return ""
	}
	flags := ""
	for _, c := range f {
		switch c {
		case 'i', 's', 'm':
			flags += string(c)
		}
	}
	if flags == "" {
		return ""
	}
	return "(?" + flags + ")"
}

func evalCall(n *callNode, s *scope) (value, error) {
	b, ok := builtins[n.fn]
	if !ok {
		return nil, fmt.Errorf("unknown function %q", n.fn)
	}
	args := make([]value, 0, len(n.args))
	for _, a := range n.args {
		v, err := eval(a, s)
		if err != nil {
			return nil, err
		}
		args = append(args, v)
	}
	if len(n.named) > 0 {
		if len(args) > 0 {
			return nil, fmt.Errorf("%s(): mixing positional and named arguments", n.fn)
		}
		byPos := make([]value, len(b.params))
		used := 0
		for i, p := range b.params {
			if a, present := n.named[p]; present {
				v, err := eval(a, s)
				if err != nil {
					return nil, err
				}
				byPos[i] = v
				used = i + 1
			}
		}
		if len(n.named) != countPresent(n.named, b.params) {
			return nil, fmt.Errorf("%s(): unknown named argument", n.fn)
		}
		args = byPos[:used]
	}
	// Missing trailing arguments are null; functions check what they need.
	for len(args) < len(b.params) {
		args = append(args, nil)
	}
	return b.fn(args)
}

func countPresent(named map[string]node, params []string) int {
	n := 0
	for _, p := range params {
		if _, ok := named[p]; ok {
			n++
		}
	}
	return n
}

func extreme(items []value, sign int) value {
	if len(items) == 0 {
		return nil
	}
	best := items[0]
	for _, v := range items[1:] {
		c, ok := order(v, best)
		if !ok {
			return nil
		}
		if c*sign > 0 {
			best = v
		}
	}
	return best
}

func flatten(l []value) []value {
	out := []value{}
	for _, item := range l {
		if inner, ok := item.([]value); ok {
			out = append(out, flatten(inner)...)
		} else {
			out = append(out, item)
		}
	}
	return out
}

func floorRat(r *big.Rat) *big.Rat {
	q := new(big.Int)
	m := new(big.Int)
	q.DivMod(r.Num(), r.Denom(), m) // Euclidean: floors for a positive denominator
	return new(big.Rat).SetInt(q)
}

// roundHalfEven rounds r to scale decimal places, ties to even (FEEL's
// decimal()).
func roundHalfEven(r *big.Rat, scale int) *big.Rat {
	factor := new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(abs(scale))), nil))
	scaled := new(big.Rat).Set(r)
	if scale >= 0 {
		scaled.Mul(scaled, factor)
	} else {
		scaled.Quo(scaled, factor)
	}
	f := floorRat(scaled)
	diff := new(big.Rat).Sub(scaled, f)
	switch diff.Cmp(big.NewRat(1, 2)) {
	case 1:
		f.Add(f, big.NewRat(1, 1))
	case 0:
		if f.Num().Bit(0) == 1 {
			f.Add(f, big.NewRat(1, 1))
		}
	}
	if scale >= 0 {
		return f.Quo(f, factor)
	}
	return f.Mul(f, factor)
}

func abs(i int) int {
	if i < 0 {
		return -i
	}
	return i
}
