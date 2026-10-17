package timer

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Cron is a cron expression in the Spring syntax Camunda 8 accepts:
//
//	second minute hour day-of-month month day-of-week
//
// The second field may be left out (five fields: it is 0). Fields take *,
// ?, numbers, names (JAN–DEC, SUN–SAT), lists (a,b), ranges (a-b) and steps
// (*/n, a/n, a-b/n). Day of week is 0–7, 0 and 7 being Sunday. Day of
// month also takes L (last day), L-n (n days before it), nW (weekday
// nearest to day n) and LW (last weekday); day of week takes dL (last
// such day of the month) and d#n (n-th such day). Macros: @yearly
// (@annually), @monthly, @weekly, @daily (@midnight), @hourly.
//
// When both day fields are restricted a day must match both, as in Spring.
type Cron struct {
	Raw                         string
	second, minute, hour, month uint64 // bit sets
	dom                         uint64 // plain days of month 1–31
	domAny                      bool
	domLast                     []int  // L / L-n: offsets before the last day
	domWeekday                  []int  // nW
	domLastWeekday              bool   // LW
	dow                         uint64 // plain weekdays 0–6
	dowAny                      bool
	dowLast                     []int    // dL
	dowNth                      [][2]int // d#n
	loc                         *time.Location
}

var cronMacros = map[string]string{
	"@yearly": "0 0 0 1 1 *", "@annually": "0 0 0 1 1 *", "@monthly": "0 0 0 1 * *",
	"@weekly": "0 0 0 * * 0", "@daily": "0 0 0 * * *", "@midnight": "0 0 0 * * *", "@hourly": "0 0 * * * *",
}

var monthNames = map[string]int{"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
var dayNames = map[string]int{"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6}

// ParseCron parses a cron expression, evaluated in the local time zone.
func ParseCron(expr string) (*Cron, error) {
	raw := strings.TrimSpace(expr)
	s := raw
	if m, ok := cronMacros[strings.ToLower(s)]; ok {
		s = m
	}
	f := strings.Fields(strings.ToUpper(s))
	if len(f) == 5 {
		f = append([]string{"0"}, f...)
	}
	if len(f) != 6 {
		return nil, fmt.Errorf("cron expression %q: want 5 or 6 fields, got %d", raw, len(f))
	}
	c := &Cron{Raw: raw, loc: time.Local}
	var err error
	if c.second, err = parseCronField(f[0], 0, 59, nil); err != nil {
		return nil, fmt.Errorf("cron %q, seconds: %w", raw, err)
	}
	if c.minute, err = parseCronField(f[1], 0, 59, nil); err != nil {
		return nil, fmt.Errorf("cron %q, minutes: %w", raw, err)
	}
	if c.hour, err = parseCronField(f[2], 0, 23, nil); err != nil {
		return nil, fmt.Errorf("cron %q, hours: %w", raw, err)
	}
	if err = c.parseDom(f[3]); err != nil {
		return nil, fmt.Errorf("cron %q, day of month: %w", raw, err)
	}
	if c.month, err = parseCronField(f[4], 1, 12, monthNames); err != nil {
		return nil, fmt.Errorf("cron %q, month: %w", raw, err)
	}
	if err = c.parseDow(f[5]); err != nil {
		return nil, fmt.Errorf("cron %q, day of week: %w", raw, err)
	}
	return c, nil
}

func parseCronField(field string, min, max int, names map[string]int) (uint64, error) {
	var bits uint64
	for _, part := range strings.Split(field, ",") {
		b, err := parseCronPart(part, min, max, names)
		if err != nil {
			return 0, err
		}
		bits |= b
	}
	return bits, nil
}

func parseCronPart(part string, min, max int, names map[string]int) (uint64, error) {
	step := 1
	if i := strings.Index(part, "/"); i >= 0 {
		n, err := strconv.Atoi(part[i+1:])
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid step in %q", part)
		}
		step, part = n, part[:i]
	}
	lo, hi := min, max
	switch {
	case part == "*" || part == "?":
	case strings.Contains(part, "-"):
		ends := strings.SplitN(part, "-", 2)
		var err error
		if lo, err = cronValue(ends[0], names); err != nil {
			return 0, err
		}
		if hi, err = cronValue(ends[1], names); err != nil {
			return 0, err
		}
	default:
		v, err := cronValue(part, names)
		if err != nil {
			return 0, err
		}
		lo = v
		if step == 1 {
			hi = v
		}
	}
	if lo < min || hi > max || lo > hi {
		return 0, fmt.Errorf("%q is out of range %d-%d", part, min, max)
	}
	var bits uint64
	for v := lo; v <= hi; v += step {
		bits |= 1 << uint(v)
	}
	return bits, nil
}

func cronValue(s string, names map[string]int) (int, error) {
	if v, ok := names[s]; ok {
		return v, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", s)
	}
	return v, nil
}

func (c *Cron) parseDom(field string) error {
	if field == "*" || field == "?" {
		c.domAny = true
		return nil
	}
	for _, part := range strings.Split(field, ",") {
		switch {
		case part == "LW":
			c.domLastWeekday = true
		case part == "L":
			c.domLast = append(c.domLast, 0)
		case strings.HasPrefix(part, "L-"):
			n, err := strconv.Atoi(part[2:])
			if err != nil || n < 0 || n > 30 {
				return fmt.Errorf("invalid %q", part)
			}
			c.domLast = append(c.domLast, n)
		case strings.HasSuffix(part, "W"):
			n, err := strconv.Atoi(strings.TrimSuffix(part, "W"))
			if err != nil || n < 1 || n > 31 {
				return fmt.Errorf("invalid %q", part)
			}
			c.domWeekday = append(c.domWeekday, n)
		default:
			b, err := parseCronPart(part, 1, 31, nil)
			if err != nil {
				return err
			}
			c.dom |= b
		}
	}
	return nil
}

func (c *Cron) parseDow(field string) error {
	if field == "*" || field == "?" {
		c.dowAny = true
		return nil
	}
	for _, part := range strings.Split(field, ",") {
		switch {
		case strings.Contains(part, "#"):
			x := strings.SplitN(part, "#", 2)
			d, err := cronValue(x[0], dayNames)
			n, err2 := strconv.Atoi(x[1])
			if err != nil || err2 != nil || d < 0 || d > 7 || n < 1 || n > 5 {
				return fmt.Errorf("invalid %q", part)
			}
			c.dowNth = append(c.dowNth, [2]int{d % 7, n})
		case len(part) > 1 && strings.HasSuffix(part, "L"):
			d, err := cronValue(strings.TrimSuffix(part, "L"), dayNames)
			if err != nil || d < 0 || d > 7 {
				return fmt.Errorf("invalid %q", part)
			}
			c.dowLast = append(c.dowLast, d%7)
		default:
			b, err := parseCronPart(part, 0, 7, dayNames)
			if err != nil {
				return err
			}
			if b&(1<<7) != 0 { // 7 is Sunday too
				b = b&^(1<<7) | 1
			}
			c.dow |= b
		}
	}
	return nil
}

func daysIn(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func (c *Cron) dayMatches(t time.Time) bool {
	day, last := t.Day(), daysIn(t.Year(), t.Month())
	domOK := c.domAny || c.dom&(1<<uint(day)) != 0
	for _, n := range c.domLast {
		domOK = domOK || day == last-n
	}
	weekday := func(target int) int { // nearest weekday to target, same month
		d := time.Date(t.Year(), t.Month(), target, 0, 0, 0, 0, t.Location())
		switch d.Weekday() {
		case time.Saturday:
			if target == 1 {
				return 3
			}
			return target - 1
		case time.Sunday:
			if target == last {
				return target - 2
			}
			return target + 1
		}
		return target
	}
	for _, n := range c.domWeekday {
		if n <= last {
			domOK = domOK || day == weekday(n)
		}
	}
	if c.domLastWeekday {
		domOK = domOK || day == weekday(last)
	}

	wd := int(t.Weekday())
	dowOK := c.dowAny || c.dow&(1<<uint(wd)) != 0
	for _, d := range c.dowLast {
		dowOK = dowOK || (wd == d && day+7 > last)
	}
	for _, x := range c.dowNth {
		dowOK = dowOK || (wd == x[0] && (day-1)/7+1 == x[1])
	}
	return domOK && dowOK
}

// Next returns the first matching time strictly after t; ok is false if
// none comes within five years.
func (c *Cron) Next(t time.Time) (time.Time, bool) {
	t = t.In(c.loc).Truncate(time.Second).Add(time.Second)
	limit := t.AddDate(5, 0, 0)
	for t.Before(limit) {
		if c.month&(1<<uint(t.Month())) == 0 {
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, c.loc)
			continue
		}
		if !c.dayMatches(t) {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, c.loc)
			continue
		}
		if c.hour&(1<<uint(t.Hour())) == 0 {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, c.loc)
			continue
		}
		if c.minute&(1<<uint(t.Minute())) == 0 {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()+1, 0, 0, c.loc)
			continue
		}
		if c.second&(1<<uint(t.Second())) == 0 {
			t = t.Add(time.Second)
			continue
		}
		return t, true
	}
	return time.Time{}, false
}
