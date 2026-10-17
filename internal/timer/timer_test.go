package timer

import (
	"testing"
	"time"
)

func mustDate(t *testing.T, s string) time.Time {
	t.Helper()
	d, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestParseDuration(t *testing.T) {
	base := mustDate(t, "2026-01-31T10:00:00Z")
	cases := map[string]string{
		"PT30S":   "2026-01-31T10:00:30Z",
		"PT1.5S":  "2026-01-31T10:00:01.5Z",
		"PT1H30M": "2026-01-31T11:30:00Z",
		"P1D":     "2026-02-01T10:00:00Z",
		"P2W":     "2026-02-14T10:00:00Z",
		"P1Y":     "2027-01-31T10:00:00Z",
		"P1DT2H":  "2026-02-01T12:00:00Z",
		"P1M":     "2026-03-03T10:00:00Z", // Go's AddDate normalizes Feb 31
		"-PT1H":   "2026-01-31T09:00:00Z",
	}
	for in, want := range cases {
		d, err := ParseDuration(in)
		if err != nil {
			t.Errorf("%s: %v", in, err)
			continue
		}
		got := d.AddTo(base)
		w, _ := time.Parse(time.RFC3339Nano, want)
		if !got.Equal(w) {
			t.Errorf("%s: got %s, want %s", in, got, want)
		}
	}
	for _, bad := range []string{"", "P", "PT", "1H", "PT1X", "P1H"} {
		if _, err := ParseDuration(bad); err == nil {
			t.Errorf("%q should not parse", bad)
		}
	}
}

func TestParseDate(t *testing.T) {
	paris, _ := time.LoadLocation("Europe/Paris")
	cases := map[string]time.Time{
		"2026-10-01T12:00:00Z":              time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC),
		"2026-10-01T12:00:00+02:00":         time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC),
		"2026-10-01T12:00:00@Europe/Paris":  time.Date(2026, 10, 1, 12, 0, 0, 0, paris),
		"2026-10-01T12:00:00[Europe/Paris]": time.Date(2026, 10, 1, 12, 0, 0, 0, paris),
		"2026-10-01T12:00:00":               time.Date(2026, 10, 1, 12, 0, 0, 0, time.Local),
		"2026-10-01":                        time.Date(2026, 10, 1, 0, 0, 0, 0, time.Local),
	}
	for in, want := range cases {
		got, err := ParseDate(in)
		if err != nil || !got.Equal(want) {
			t.Errorf("%s: got %s (%v), want %s", in, got, err, want)
		}
	}
	if _, err := ParseDate("tomorrow"); err == nil {
		t.Error("tomorrow should not parse")
	}
}

func TestCycles(t *testing.T) {
	now := mustDate(t, "2026-01-01T00:00:00Z")

	c, err := Parse(KindCycle, "R3/PT10S")
	if err != nil {
		t.Fatal(err)
	}
	first, ok := c.First(now)
	if !ok || !first.Equal(now.Add(10*time.Second)) {
		t.Fatalf("first: %s %v", first, ok)
	}
	second, ok := c.Next(first, 1)
	third, ok2 := c.Next(second, 2)
	_, ok3 := c.Next(third, 3)
	if !ok || !ok2 || ok3 || !third.Equal(now.Add(30*time.Second)) {
		t.Fatalf("R3 should fire three times: %s %s %v %v %v", second, third, ok, ok2, ok3)
	}

	forever, _ := Parse(KindCycle, "R/P1D")
	if _, ok := forever.Next(now, 1000); !ok {
		t.Error("R/P1D repeats forever")
	}

	start, _ := Parse(KindCycle, "R2/2026-03-01T08:00:00Z/PT1H")
	f, _ := start.First(now)
	n, _ := start.Next(f, 1)
	if !f.Equal(mustDate(t, "2026-03-01T08:00:00Z")) || !n.Equal(mustDate(t, "2026-03-01T09:00:00Z")) {
		t.Errorf("explicit start: %s %s", f, n)
	}

	ending, _ := Parse(KindCycle, "R/PT1H/2026-01-01T02:30:00Z")
	f, _ = ending.First(now)
	n, ok = ending.Next(f, 1)
	_, ok2 = ending.Next(n, 2)
	if !ok || ok2 {
		t.Errorf("cycle with end: %s %v %v", n, ok, ok2)
	}

	for _, bad := range []string{"R/PT0S", "Rx/PT1S", "R/", "every hour"} {
		if _, err := Parse(KindCycle, bad); err == nil {
			t.Errorf("%q should not parse", bad)
		}
	}
}

func TestCron(t *testing.T) {
	time.Local = time.UTC
	from := mustDate(t, "2026-01-30T10:15:30Z") // a Friday
	cases := []struct{ expr, want string }{
		{"0 0 * * * *", "2026-01-30T11:00:00Z"},
		{"*/10 * * * * *", "2026-01-30T10:15:40Z"},
		{"0 30 9 * * MON-FRI", "2026-02-02T09:30:00Z"},
		{"0 0 12 1 * ?", "2026-02-01T12:00:00Z"},
		{"0 0 0 L * *", "2026-01-31T00:00:00Z"},
		{"0 0 0 L-1 * *", "2026-02-27T00:00:00Z"},
		{"0 0 0 ? * 5L", "2026-02-27T00:00:00Z"}, // last Friday of February
		{"0 0 0 ? * MON#2", "2026-02-09T00:00:00Z"},
		{"0 0 0 1W * *", "2026-02-02T00:00:00Z"}, // Feb 1 2026 is a Sunday
		{"0 0 0 LW * *", "2026-02-27T00:00:00Z"}, // Feb 28 2026 is a Saturday
		{"0 0 0 29 2 *", "2028-02-29T00:00:00Z"},
		{"30 9 * * 7", "2026-02-01T09:30:00Z"}, // five fields, 7 = Sunday
		{"@daily", "2026-01-31T00:00:00Z"},
		{"0 0 0 1 JAN,JUL *", "2026-07-01T00:00:00Z"},
	}
	for _, c := range cases {
		cr, err := ParseCron(c.expr)
		if err != nil {
			t.Errorf("%s: %v", c.expr, err)
			continue
		}
		got, ok := cr.Next(from)
		if !ok || !got.Equal(mustDate(t, c.want)) {
			t.Errorf("%s: got %s, want %s", c.expr, got, c.want)
		}
	}
	for _, bad := range []string{"* * *", "61 * * * * *", "0 0 0 32 * *", "0 0 0 * 13 *", "0 0 0 * * MON#6"} {
		if _, err := ParseCron(bad); err == nil {
			t.Errorf("%q should not parse", bad)
		}
	}
	if _, ok := mustCron(t, "0 0 0 30 2 *").Next(from); ok {
		t.Error("February 30th never comes")
	}
}

func mustCron(t *testing.T, s string) *Cron {
	c, err := ParseCron(s)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestParseXML(t *testing.T) {
	tm, err := ParseXML(`<bpmn:timerEventDefinition><bpmn:timeCycle xsi:type="bpmn:tFormalExpression">R/PT5S</bpmn:timeCycle></bpmn:timerEventDefinition>`, nil)
	if err != nil || tm.Kind != KindCycle || tm.Repetitions != -1 {
		t.Fatalf("prefixed timeCycle: %+v %v", tm, err)
	}
	tm, err = ParseXML(`<timeDuration>=delay</timeDuration>`, func(expr string) (string, error) { return "PT2M", nil })
	if err != nil || tm.Kind != KindDuration {
		t.Fatalf("FEEL duration: %+v %v", tm, err)
	}
	if _, err := ParseXML(`<timeDuration>=delay</timeDuration>`, nil); err == nil {
		t.Error("a FEEL expression without an evaluator is an error")
	}
}
