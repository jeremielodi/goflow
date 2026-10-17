// Package timer parses BPMN timer values the way Camunda 8 accepts them and
// computes when they fire:
//
//   - timeDuration: an ISO 8601 duration, PnYnMnWnDTnHnMnS (P1D, PT30S,
//     P1Y2M, PT1.5S…);
//   - timeDate: an ISO 8601 date-time — with an offset (2026-10-01T12:00:00Z,
//     …+02:00), a zone id (2026-10-01T12:00:00@Europe/Paris or
//     …[Europe/Paris]) or neither (local time) — or a plain date;
//   - timeCycle: an ISO 8601 repeating interval, R[n]/duration,
//     R[n]/start/duration or R[n]/duration/end (no n: forever), or a cron
//     expression (seconds field optional; see cron.go).
//
// A value starting with "=" is a FEEL expression; callers evaluate it first
// and pass the resulting string (internal/feel renders dates and durations
// as ISO 8601 strings).
package timer

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Duration is an ISO 8601 duration: calendar years, months and days are
// added as calendar units (a month is not 30 days), the rest as time.
type Duration struct {
	Years, Months, Days int
	Time                time.Duration
}

var durationRe = regexp.MustCompile(`^(-)?P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:[.,]\d+)?)S)?)?$`)

// ParseDuration parses an ISO 8601 duration.
func ParseDuration(s string) (Duration, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	m := durationRe.FindStringSubmatch(s)
	if m == nil || s == "P" || strings.HasSuffix(s, "T") {
		return Duration{}, fmt.Errorf("invalid ISO 8601 duration %q", s)
	}
	atoi := func(x string) int {
		n, _ := strconv.Atoi(x)
		return n
	}
	d := Duration{
		Years:  atoi(m[2]),
		Months: atoi(m[3]),
		Days:   atoi(m[4])*7 + atoi(m[5]),
		Time:   time.Duration(atoi(m[6]))*time.Hour + time.Duration(atoi(m[7]))*time.Minute,
	}
	if m[8] != "" {
		secs, _ := strconv.ParseFloat(strings.Replace(m[8], ",", ".", 1), 64)
		d.Time += time.Duration(math.Round(secs * float64(time.Second)))
	}
	if m[1] == "-" {
		d = Duration{-d.Years, -d.Months, -d.Days, -d.Time}
	}
	return d, nil
}

// AddTo returns t plus the duration.
func (d Duration) AddTo(t time.Time) time.Time {
	return t.AddDate(d.Years, d.Months, d.Days).Add(d.Time)
}

// IsZero is true for a duration of nothing.
func (d Duration) IsZero() bool { return d == Duration{} }

var zoneSuffix = regexp.MustCompile(`^(.*?)(?:@([A-Za-z_]+(?:/[A-Za-z_+\-0-9]+)*)|\[([A-Za-z_]+(?:/[A-Za-z_+\-0-9]+)*)\])$`)

// ParseDate parses an ISO 8601 date-time (see the package doc).
func ParseDate(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	loc := time.Local
	if m := zoneSuffix.FindStringSubmatch(s); m != nil {
		name := m[2]
		if name == "" {
			name = m[3]
		}
		l, err := time.LoadLocation(name)
		if err != nil {
			return time.Time{}, fmt.Errorf("unknown time zone %q: %w", name, err)
		}
		s, loc = m[1], l
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04Z07:00"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	for _, layout := range []string{"2006-01-02T15:04:05.999999999", "2006-01-02T15:04", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, s, loc); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid ISO 8601 date-time %q", s)
}

// Kind is which of the three BPMN timer definitions a Timer comes from.
type Kind string

const (
	KindDuration Kind = "duration"
	KindDate     Kind = "date"
	KindCycle    Kind = "cycle"
)

// Timer is a parsed timer value.
type Timer struct {
	Kind Kind
	Raw  string

	duration Duration  // duration, and a cycle's interval
	date     time.Time // date, and a cycle's explicit start
	end      time.Time // a cycle's end (zero: none)
	// Repetitions of a cycle: -1 forever. Camunda counts R3 as three
	// firings.
	Repetitions int
	cron        *Cron
}

// Parse parses a timer value of the given kind.
func Parse(kind Kind, value string) (*Timer, error) {
	value = strings.TrimSpace(value)
	t := &Timer{Kind: kind, Raw: value, Repetitions: 1}
	switch kind {
	case KindDuration:
		d, err := ParseDuration(value)
		if err != nil {
			return nil, err
		}
		t.duration = d
	case KindDate:
		d, err := ParseDate(value)
		if err != nil {
			return nil, err
		}
		t.date = d
	case KindCycle:
		if !strings.HasPrefix(strings.ToUpper(value), "R") || !strings.Contains(value, "/") {
			c, err := ParseCron(value)
			if err != nil {
				return nil, fmt.Errorf("invalid timer cycle %q: neither an ISO 8601 repeating interval nor a cron expression: %w", value, err)
			}
			t.cron, t.Repetitions = c, -1
			return t, nil
		}
		parts := strings.Split(value, "/")
		if len(parts) < 2 || len(parts) > 3 {
			return nil, fmt.Errorf("invalid timer cycle %q", value)
		}
		if n := strings.TrimPrefix(strings.ToUpper(parts[0]), "R"); n == "" {
			t.Repetitions = -1
		} else {
			r, err := strconv.Atoi(n)
			if err != nil || r < 0 {
				return nil, fmt.Errorf("invalid repetition count in timer cycle %q", value)
			}
			t.Repetitions = r
		}
		var err error
		switch {
		case len(parts) == 2:
			t.duration, err = ParseDuration(parts[1])
		case strings.HasPrefix(strings.ToUpper(parts[1]), "P"):
			if t.duration, err = ParseDuration(parts[1]); err == nil {
				t.end, err = ParseDate(parts[2])
			}
		default:
			if t.date, err = ParseDate(parts[1]); err == nil {
				t.duration, err = ParseDuration(parts[2])
			}
		}
		if err != nil {
			return nil, fmt.Errorf("invalid timer cycle %q: %w", value, err)
		}
		if t.duration.AddTo(time.Unix(0, 0)).Sub(time.Unix(0, 0)) <= 0 {
			return nil, fmt.Errorf("invalid timer cycle %q: the interval must be positive", value)
		}
	default:
		return nil, fmt.Errorf("unknown timer kind %q", kind)
	}
	return t, nil
}

// ParseXML parses a node's raw timerEventDefinition XML (the form the
// engine stores), after resolving a FEEL expression with eval (nil: a
// FEEL expression is an error).
func ParseXML(raw string, eval func(expr string) (string, error)) (*Timer, error) {
	for _, k := range []struct {
		tag  string
		kind Kind
	}{{"timeDuration", KindDuration}, {"timeDate", KindDate}, {"timeCycle", KindCycle}} {
		re := regexp.MustCompile(`<(?:\w+:)?` + k.tag + `\b[^>]*>([\s\S]*?)</(?:\w+:)?` + k.tag + `>`)
		m := re.FindStringSubmatch(raw)
		if m == nil {
			continue
		}
		value := strings.TrimSpace(xmlUnescape(m[1]))
		if strings.HasPrefix(value, "=") {
			if eval == nil {
				return nil, fmt.Errorf("timer expression %q needs variables", value)
			}
			v, err := eval(strings.TrimPrefix(value, "="))
			if err != nil {
				return nil, fmt.Errorf("timer expression %q: %w", value, err)
			}
			value = v
		}
		return Parse(k.kind, value)
	}
	return nil, fmt.Errorf("no timeDuration, timeDate or timeCycle in timer definition")
}

func xmlUnescape(s string) string {
	return strings.NewReplacer("&lt;", "<", "&gt;", ">", "&quot;", `"`, "&apos;", "'", "&amp;", "&").Replace(s)
}

// First returns when the timer first fires for something armed at now.
// ok is false when it never fires (a cycle with zero repetitions or past
// its end).
func (t *Timer) First(now time.Time) (time.Time, bool) {
	switch t.Kind {
	case KindDuration:
		return t.duration.AddTo(now), true
	case KindDate:
		return t.date, true
	}
	if t.Repetitions == 0 {
		return time.Time{}, false
	}
	if t.cron != nil {
		return t.cron.Next(now)
	}
	var due time.Time
	if !t.date.IsZero() {
		// An explicit start fires at the start, then every interval.
		due = t.date
	} else {
		due = t.duration.AddTo(now)
	}
	if !t.end.IsZero() && due.After(t.end) {
		return time.Time{}, false
	}
	return due, true
}

// Next returns when a cycle fires again after firing at prev, which was
// its fired-th firing (1 for the first). ok is false once it is done.
func (t *Timer) Next(prev time.Time, fired int) (time.Time, bool) {
	if t.Kind != KindCycle || (t.Repetitions >= 0 && fired >= t.Repetitions) {
		return time.Time{}, false
	}
	if t.cron != nil {
		return t.cron.Next(prev)
	}
	due := t.duration.AddTo(prev)
	if !t.end.IsZero() && due.After(t.end) {
		return time.Time{}, false
	}
	return due, true
}

// IsCycle is true for a repeating timer.
func (t *Timer) IsCycle() bool { return t.Kind == KindCycle }
