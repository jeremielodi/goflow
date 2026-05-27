package engine

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jeremielodi/goflow/internal/models"
)

// ParseTimerDefinition parses BPMN timer definition from XML
func ParseTimerDefinition(timerXML string) (*models.TimerDefinition, error) {
	timerDef := &models.TimerDefinition{}

	// Parse timeDuration
	if strings.Contains(timerXML, "timeDuration") {
		re := regexp.MustCompile(`<timeDuration>(.*?)</timeDuration>`)
		matches := re.FindStringSubmatch(timerXML)
		if len(matches) > 1 {
			timerDef.TimeDuration = matches[1]
			timerDef.Duration = matches[1]
		}
	}

	// Parse timeDate
	if strings.Contains(timerXML, "timeDate") {
		re := regexp.MustCompile(`<timeDate>(.*?)</timeDate>`)
		matches := re.FindStringSubmatch(timerXML)
		if len(matches) > 1 {
			timerDef.TimeDate = matches[1]
		}
	}

	// Parse timeCycle
	if strings.Contains(timerXML, "timeCycle") {
		re := regexp.MustCompile(`<timeCycle>(.*?)</timeCycle>`)
		matches := re.FindStringSubmatch(timerXML)
		if len(matches) > 1 {
			timerDef.TimeCycle = matches[1]
			timerDef.Cycle = matches[1]
		}
	}

	return timerDef, nil
}

// CalculateDueTime calculates when the timer should fire
func CalculateDueTime(timerDef *models.TimerDefinition, startTime time.Time) (time.Time, error) {
	// Priority: timeDate > timeDuration > timeCycle
	if timerDef.TimeDate != "" {
		// Parse fixed date/time
		dueTime, err := time.Parse(time.RFC3339, timerDef.TimeDate)
		if err != nil {
			// Try other common formats
			dueTime, err = time.Parse("2006-01-02T15:04:05", timerDef.TimeDate)
			if err != nil {
				return time.Time{}, fmt.Errorf("failed to parse timeDate: %w", err)
			}
		}
		return dueTime, nil
	}

	if timerDef.TimeDuration != "" || timerDef.Duration != "" {
		duration := timerDef.TimeDuration
		if duration == "" {
			duration = timerDef.Duration
		}
		// Parse ISO 8601 duration
		durationMs, err := parseISODuration(duration)
		if err != nil {
			return time.Time{}, err
		}
		return startTime.Add(time.Duration(durationMs) * time.Millisecond), nil
	}

	if timerDef.TimeCycle != "" || timerDef.Cycle != "" {
		// For first occurrence, parse the cycle
		cycle := timerDef.TimeCycle
		if cycle == "" {
			cycle = timerDef.Cycle
		}
		durationMs, err := parseISODuration(cycle)
		if err != nil {
			return time.Time{}, err
		}
		return startTime.Add(time.Duration(durationMs) * time.Millisecond), nil
	}

	return time.Time{}, fmt.Errorf("no valid timer definition found")
}

// parseISODuration parses ISO 8601 duration format
// Examples: PT1M (1 minute), PT1H (1 hour), PT1D (1 day), PT30S (30 seconds)
func parseISODuration(durationStr string) (int64, error) {
	durationStr = strings.TrimSpace(durationStr)

	// Remove 'PT' prefix
	if !strings.HasPrefix(durationStr, "PT") {
		return 0, fmt.Errorf("invalid duration format, expected PT...")
	}
	durationStr = durationStr[2:]

	var totalMs int64 = 0

	// Parse days
	if strings.Contains(durationStr, "D") {
		parts := strings.Split(durationStr, "D")
		days, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, err
		}
		totalMs += days * 24 * 60 * 60 * 1000
		durationStr = parts[1]
	}

	// Parse time part (after T)
	if strings.Contains(durationStr, "T") {
		parts := strings.Split(durationStr, "T")
		durationStr = parts[1]
	}

	// Parse hours
	if strings.Contains(durationStr, "H") {
		parts := strings.Split(durationStr, "H")
		hours, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, err
		}
		totalMs += hours * 60 * 60 * 1000
		durationStr = parts[1]
	}

	// Parse minutes
	if strings.Contains(durationStr, "M") && !strings.Contains(durationStr, "MS") {
		parts := strings.Split(durationStr, "M")
		minutes, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, err
		}
		totalMs += minutes * 60 * 1000
		durationStr = parts[1]
	}

	// Parse seconds
	if strings.Contains(durationStr, "S") {
		parts := strings.Split(durationStr, "S")
		seconds, err := strconv.ParseFloat(parts[0], 64)
		if err != nil {
			return 0, err
		}
		totalMs += int64(seconds * 1000)
	}

	return totalMs, nil
}
