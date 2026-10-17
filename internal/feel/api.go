package feel

import (
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	cacheMu sync.RWMutex
	cache   = map[string]node{}
)

const maxCachedExpressions = 5000

func parseCached(expr string) (node, error) {
	cacheMu.RLock()
	n, ok := cache[expr]
	cacheMu.RUnlock()
	if ok {
		return n, nil
	}
	n, err := parseExpression(expr)
	if err != nil {
		return nil, err
	}
	cacheMu.Lock()
	if len(cache) >= maxCachedExpressions {
		cache = map[string]node{}
	}
	cache[expr] = n
	cacheMu.Unlock()
	return n, nil
}

// Evaluate evaluates a FEEL expression (without a leading "=") against
// vars and returns the result as a plain Go value: float64 for numbers,
// string, bool, nil, []interface{}, map[string]interface{}, and ISO 8601
// strings for dates, times and durations.
func Evaluate(expr string, vars map[string]interface{}) (result interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			result, err = nil, fmt.Errorf("FEEL evaluation of %q panicked: %v", expr, r)
		}
	}()
	n, err := parseCached(expr)
	if err != nil {
		return nil, fmt.Errorf("FEEL syntax error in %q: %w", expr, err)
	}
	v, err := eval(n, rootScope(vars))
	if err != nil {
		return nil, fmt.Errorf("FEEL evaluation error in %q: %w", expr, err)
	}
	return FromFEEL(v), nil
}

// EvaluateBool evaluates a FEEL condition; anything but a boolean result
// (null included) is an error, as in Camunda 8.
func EvaluateBool(expr string, vars map[string]interface{}) (bool, error) {
	v, err := Evaluate(expr, vars)
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("FEEL condition %q evaluated to %v, not a boolean", expr, describe(v))
	}
	return b, nil
}

// EvaluateUnaryTests reports whether input passes the DMN unary tests
// (e.g. "< 10", "[1..5]", "\"gold\",\"silver\"", "-", "not(\"x\")").
func EvaluateUnaryTests(tests string, input interface{}, vars map[string]interface{}) (result bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			result, err = false, fmt.Errorf("FEEL unary tests %q panicked: %v", tests, r)
		}
	}()
	parsed, err := parseUnaryTests(tests)
	if err != nil {
		return false, fmt.Errorf("FEEL syntax error in unary tests %q: %w", tests, err)
	}
	v, err := anyTest(parsed, ToFEEL(input), rootScope(vars))
	if err != nil {
		return false, err
	}
	return v == true, nil
}

func rootScope(vars map[string]interface{}) *scope {
	converted := make(map[string]value, len(vars))
	for k, v := range vars {
		converted[k] = ToFEEL(v)
	}
	return &scope{vars: converted}
}

func describe(v interface{}) string {
	if v == nil {
		return "null"
	}
	return fmt.Sprintf("%v (%T)", v, v)
}

// ToFEEL converts a Go value (typically decoded from JSON) to a FEEL value.
// Numbers become exact decimals through their shortest decimal form, so
// the float64 0.1 is the decimal 0.1.
func ToFEEL(v interface{}) value {
	switch x := v.(type) {
	case nil:
		return nil
	case bool, string, Date, time.Time, DTDuration, YMDuration, *big.Rat:
		return x
	case float64:
		return ratFromFloat(x)
	case float32:
		return ratFromFloat(float64(x))
	case int:
		return new(big.Rat).SetInt64(int64(x))
	case int8:
		return new(big.Rat).SetInt64(int64(x))
	case int16:
		return new(big.Rat).SetInt64(int64(x))
	case int32:
		return new(big.Rat).SetInt64(int64(x))
	case int64:
		return new(big.Rat).SetInt64(x)
	case uint:
		return new(big.Rat).SetUint64(uint64(x))
	case uint32:
		return new(big.Rat).SetUint64(uint64(x))
	case uint64:
		return new(big.Rat).SetUint64(x)
	case json.Number:
		if r, ok := new(big.Rat).SetString(x.String()); ok {
			return r
		}
		return nil
	case []interface{}:
		out := make([]value, len(x))
		for i, item := range x {
			out[i] = ToFEEL(item)
		}
		return out
	case map[string]interface{}:
		out := make(map[string]value, len(x))
		for k, item := range x {
			out[k] = ToFEEL(item)
		}
		return out
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		out := make([]value, rv.Len())
		for i := range out {
			out[i] = ToFEEL(rv.Index(i).Interface())
		}
		return out
	case reflect.Map:
		if rv.Type().Key().Kind() == reflect.String {
			out := make(map[string]value, rv.Len())
			for _, k := range rv.MapKeys() {
				out[k.String()] = ToFEEL(rv.MapIndex(k).Interface())
			}
			return out
		}
	case reflect.Ptr:
		if rv.IsNil() {
			return nil
		}
		return ToFEEL(rv.Elem().Interface())
	}
	return fmt.Sprint(v)
}

// FromFEEL converts a FEEL value to a plain Go value that marshals to JSON
// the way process variables are stored.
func FromFEEL(v value) interface{} {
	switch x := v.(type) {
	case *big.Rat:
		f, _ := x.Float64()
		return f
	case []value:
		out := make([]interface{}, len(x))
		for i, item := range x {
			out[i] = FromFEEL(item)
		}
		return out
	case map[string]value:
		out := make(map[string]interface{}, len(x))
		for k, item := range x {
			out[k] = FromFEEL(item)
		}
		return out
	case Date, time.Time, DTDuration, YMDuration:
		return toString(x)
	case *rangeValue:
		return nil
	}
	return v
}

// toString renders a value the way FEEL's string() does.
func toString(v value) value {
	switch x := v.(type) {
	case nil:
		return nil
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	case *big.Rat:
		return formatRat(x)
	case Date:
		return x.Format("2006-01-02")
	case time.Time:
		return x.Format(time.RFC3339Nano)
	case DTDuration:
		return formatDTDuration(time.Duration(x))
	case YMDuration:
		return formatYMDuration(int(x))
	case []value, map[string]value:
		b, err := json.Marshal(FromFEEL(x))
		if err != nil {
			return nil
		}
		return string(b)
	}
	return fmt.Sprint(v)
}

func formatRat(r *big.Rat) string {
	if r.IsInt() {
		return r.Num().String()
	}
	s := r.FloatString(20)
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}

func parseTemporal(s string) (value, error) {
	if d, err := parseDuration(s); err == nil {
		return d, nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return Date{t}, nil
	}
	if t, ok := parseDateTime(s); ok {
		return t, nil
	}
	return nil, fmt.Errorf("invalid temporal literal @%q", s)
}

func parseDateTime(s string) (time.Time, bool) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// parseDuration parses an ISO 8601 duration: years/months ("P1Y2M") give a
// YMDuration, days/hours/minutes/seconds ("P1DT2H30M") a DTDuration.
func parseDuration(s string) (value, error) {
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	if !strings.HasPrefix(s, "P") || len(s) < 2 {
		return nil, fmt.Errorf("not a duration")
	}
	s = s[1:]
	datePart, timePart, hasTime := strings.Cut(s, "T")
	var months int
	var d time.Duration
	ym, dt := false, false

	read := func(part string, units map[byte]func(float64)) error {
		num := ""
		for i := 0; i < len(part); i++ {
			c := part[i]
			if (c >= '0' && c <= '9') || c == '.' {
				num += string(c)
				continue
			}
			f, err := strconv.ParseFloat(num, 64)
			if err != nil || num == "" {
				return fmt.Errorf("bad duration")
			}
			apply, ok := units[c]
			if !ok {
				return fmt.Errorf("bad duration unit %q", c)
			}
			apply(f)
			num = ""
		}
		if num != "" {
			return fmt.Errorf("bad duration")
		}
		return nil
	}
	if err := read(datePart, map[byte]func(float64){
		'Y': func(f float64) { months += int(f) * 12; ym = true },
		'M': func(f float64) { months += int(f); ym = true },
		'W': func(f float64) { d += time.Duration(f * 7 * 24 * float64(time.Hour)); dt = true },
		'D': func(f float64) { d += time.Duration(f * 24 * float64(time.Hour)); dt = true },
	}); err != nil {
		return nil, err
	}
	if hasTime {
		if err := read(timePart, map[byte]func(float64){
			'H': func(f float64) { d += time.Duration(f * float64(time.Hour)); dt = true },
			'M': func(f float64) { d += time.Duration(f * float64(time.Minute)); dt = true },
			'S': func(f float64) { d += time.Duration(f * float64(time.Second)); dt = true },
		}); err != nil {
			return nil, err
		}
	}
	switch {
	case ym && dt:
		return nil, fmt.Errorf("a duration mixes years/months with days/time")
	case ym:
		if neg {
			months = -months
		}
		return YMDuration(months), nil
	case dt:
		if neg {
			d = -d
		}
		return DTDuration(d), nil
	}
	return nil, fmt.Errorf("empty duration")
}

func formatDTDuration(d time.Duration) string {
	if d == 0 {
		return "PT0S"
	}
	sign := ""
	if d < 0 {
		sign, d = "-", -d
	}
	days := d / (24 * time.Hour)
	d -= days * 24 * time.Hour
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	secs := d.Seconds()

	var b strings.Builder
	b.WriteString(sign + "P")
	if days > 0 {
		fmt.Fprintf(&b, "%dD", days)
	}
	if h > 0 || m > 0 || secs > 0 {
		b.WriteString("T")
		if h > 0 {
			fmt.Fprintf(&b, "%dH", h)
		}
		if m > 0 {
			fmt.Fprintf(&b, "%dM", m)
		}
		if secs > 0 {
			b.WriteString(strconv.FormatFloat(secs, 'f', -1, 64) + "S")
		}
	}
	return b.String()
}

func formatYMDuration(months int) string {
	sign := ""
	if months < 0 {
		sign, months = "-", -months
	}
	y, m := months/12, months%12
	switch {
	case y > 0 && m > 0:
		return fmt.Sprintf("%sP%dY%dM", sign, y, m)
	case y > 0:
		return fmt.Sprintf("%sP%dY", sign, y)
	}
	return fmt.Sprintf("%sP%dM", sign, m)
}
