package common

import (
	"time"
)

type Period struct {
	Key          string
	TranslateKey string
	Limit        func() (time.Time, time.Time)
}

type PeriodService struct {
	Timestamp time.Time
	Periods   map[string]Period
}

func NewPeriodService(clientTimestamp time.Time) *PeriodService {
	ps := &PeriodService{}
	ps.Timestamp = clientTimestamp

	ps.Periods = map[string]Period{
		"today": {
			Key:          "today",
			TranslateKey: "PERIODS.TODAY",
			Limit:        ps.calculatePeriodLimit("date", 0),
		},
		"week": {
			Key:          "week",
			TranslateKey: "PERIODS.THIS_WEEK",
			Limit:        ps.calculatePeriodLimit("week", 0),
		},
		"month": {
			Key:          "month",
			TranslateKey: "PERIODS.THIS_MONTH",
			Limit:        ps.calculatePeriodLimit("month", 0),
		},
		"year": {
			Key:          "year",
			TranslateKey: "PERIODS.THIS_YEAR",
			Limit:        ps.calculatePeriodLimit("year", 0),
		},
		"yesterday": {
			Key:          "yesterday",
			TranslateKey: "PERIODS.YESTERDAY",
			Limit:        ps.calculatePeriodLimit("date", -1),
		},
		"lastWeek": {
			Key:          "lastWeek",
			TranslateKey: "PERIODS.LAST_WEEK",
			Limit:        ps.calculatePeriodLimit("week", -1),
		},
		"lastMonth": {
			Key:          "lastMonth",
			TranslateKey: "PERIODS.LAST_MONTH",
			Limit:        ps.calculatePeriodLimit("month", -1),
		},
		"lastYear": {
			Key:          "lastYear",
			TranslateKey: "PERIODS.LAST_YEAR",
			Limit:        ps.calculatePeriodLimit("year", -1),
		},
		"allTime": {
			Key:          "allTime",
			TranslateKey: "PERIODS.ALL_TIME",
			Limit:        func() (time.Time, time.Time) { return time.Time{}, time.Time{} },
		},
		"custom": {
			Key:          "custom",
			TranslateKey: "PERIODS.CUSTOM",
			Limit:        func() (time.Time, time.Time) { return time.Time{}, time.Time{} },
		},
	}
	return ps
}

func (ps *PeriodService) calculatePeriodLimit(periodKey string, modifier int) func() (time.Time, time.Time) {
	return func() (time.Time, time.Time) {
		var start, end time.Time
		switch periodKey {
		case "date":
			start = ps.Timestamp.AddDate(0, 0, modifier).Truncate(24 * time.Hour)
			end = start.Add(24 * time.Hour).Add(-1)
		case "week":
			weekday := ps.Timestamp.Weekday()
			start := ps.Timestamp.AddDate(0, 0, -int(weekday+time.Weekday(modifier)))
			end = start.AddDate(0, 0, 6)
		case "month":
			var month = ps.Timestamp.Month()
			start = time.Date(ps.Timestamp.Year(), month+time.Month(modifier), 1, 0, 0, 0, 0, ps.Timestamp.Location())
			// Get the last day of the month
			end = start.AddDate(0, 1, 0).Add(-time.Nanosecond)
		case "year":
			start = time.Date(ps.Timestamp.Year()+modifier, 1, 1, 0, 0, 0, 0, ps.Timestamp.Location())
			// Get the last day of the year
			end = time.Date(ps.Timestamp.Year(), 12, 31, 0, 0, 0, 0, ps.Timestamp.Location())

		}
		return start, end
	}
}

func (ps *PeriodService) LookupPeriod(key string) Period {
	return ps.Periods[key]
}
