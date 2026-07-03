package api

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type AnalyticsController struct {
	analyticsRepo *repository.AnalyticsRepository
}

func NewAnalyticsController(db *sqlx.DB) *AnalyticsController {
	return &AnalyticsController{analyticsRepo: repository.NewAnalyticsRepository(db)}
}

type processKeyStats struct {
	ProcessKey        string   `json:"processKey"`
	RunningCount      int      `json:"runningCount"`
	CompletedCount    int      `json:"completedCount"`
	TerminatedCount   int      `json:"terminatedCount"`
	AvgDurationMillis *float64 `json:"avgDurationMillis,omitempty"`
	P50DurationMillis *float64 `json:"p50DurationMillis,omitempty"`
	P95DurationMillis *float64 `json:"p95DurationMillis,omitempty"`
	IncidentCount     int      `json:"incidentCount"`
	IncidentRate      float64  `json:"incidentRate"`
}

type throughputPoint struct {
	Date       string `json:"date"`
	Started    int    `json:"started"`
	Completed  int    `json:"completed"`
	Terminated int    `json:"terminated"`
}

// GET /engine-rest/analytics/process-stats
// Query params: processKey, from, to (RFC3339 dates), days (default 30,
// used when from/to are omitted).
func (ctrl *AnalyticsController) GetProcessStats(c *fiber.Ctx) error {
	processKey := c.Query("processKey")

	to := time.Now()
	from := to.AddDate(0, 0, -30)
	if daysStr := c.Query("days"); daysStr != "" {
		if days, err := time.ParseDuration(daysStr + "h"); err == nil {
			from = to.Add(-days * 24)
		}
	}
	if fromStr := c.Query("from"); fromStr != "" {
		if t, err := time.Parse(time.RFC3339, fromStr); err == nil {
			from = t
		}
	}
	if toStr := c.Query("to"); toStr != "" {
		if t, err := time.Parse(time.RFC3339, toStr); err == nil {
			to = t
		}
	}

	byKey := make(map[string]*processKeyStats)
	// durationSourceCount tracks, per process key, how many instances the
	// currently-stored duration percentiles were computed from — so when
	// both 'completed' and 'terminated' rows exist, whichever has more
	// instances wins as the representative sample (not which was seen
	// first while iterating the result set).
	durationSourceCount := make(map[string]int)
	getOrCreate := func(key string) *processKeyStats {
		if s, ok := byKey[key]; ok {
			return s
		}
		s := &processKeyStats{ProcessKey: key}
		byKey[key] = s
		return s
	}

	liveCounts, err := ctrl.analyticsRepo.GetLiveStatusCounts(processKey)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to load live status counts", "error": err.Error()})
	}
	for _, lc := range liveCounts {
		s := getOrCreate(lc.ProcessKey)
		switch lc.Status {
		case "running", "suspended":
			s.RunningCount += lc.Count
		}
	}

	historicStats, err := ctrl.analyticsRepo.GetHistoricStatusStats(processKey, &from, &to)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to load historic status stats", "error": err.Error()})
	}
	for _, hs := range historicStats {
		s := getOrCreate(hs.ProcessKey)
		switch hs.State {
		case "completed":
			s.CompletedCount += hs.Count
		case "terminated":
			s.TerminatedCount += hs.Count
		}
		// Surface duration stats from whichever state (completed vs
		// terminated) has more instances — good enough for a summary
		// view; a caller wanting a per-state breakdown can filter by
		// processKey and inspect the raw historic rows directly.
		if hs.AvgDurationMillis != nil && hs.Count > durationSourceCount[hs.ProcessKey] {
			s.AvgDurationMillis = hs.AvgDurationMillis
			s.P50DurationMillis = hs.P50DurationMillis
			s.P95DurationMillis = hs.P95DurationMillis
			durationSourceCount[hs.ProcessKey] = hs.Count
		}
	}

	incidentCounts, err := ctrl.analyticsRepo.GetIncidentCounts(processKey)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to load incident counts", "error": err.Error()})
	}
	for _, ic := range incidentCounts {
		s := getOrCreate(ic.ProcessKey)
		s.IncidentCount = ic.Count
	}

	byProcessKey := make([]processKeyStats, 0, len(byKey))
	for _, s := range byKey {
		total := s.RunningCount + s.CompletedCount + s.TerminatedCount
		if total > 0 {
			s.IncidentRate = float64(s.IncidentCount) / float64(total)
		}
		byProcessKey = append(byProcessKey, *s)
	}

	started, err := ctrl.analyticsRepo.GetStartedPerDay(processKey, from, to)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to load throughput (started)", "error": err.Error()})
	}
	finished, err := ctrl.analyticsRepo.GetFinishedPerDay(processKey, from, to)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to load throughput (finished)", "error": err.Error()})
	}

	throughput := make([]throughputPoint, 0)
	for d := truncateToDay(from); !d.After(to); d = d.AddDate(0, 0, 1) {
		dateKey := d.Format("2006-01-02")
		point := throughputPoint{Date: dateKey}
		point.Started = started[dateKey]
		if byState, ok := finished[dateKey]; ok {
			point.Completed = byState["completed"]
			point.Terminated = byState["terminated"]
		}
		throughput = append(throughput, point)
	}

	return c.JSON(fiber.Map{
		"byProcessKey": byProcessKey,
		"throughput":   throughput,
	})
}

func truncateToDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}
