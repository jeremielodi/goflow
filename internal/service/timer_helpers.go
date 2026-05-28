// internal/service/timer_helpers.go
package service

import (
	"fmt"
	"regexp"
	"time"
)

// extractCycleString extracts the timeCycle value from timer definition XML
func extractCycleString(timerDef string) string {
	re := regexp.MustCompile(`<timeCycle>(.*?)</timeCycle>`)
	matches := re.FindStringSubmatch(timerDef)
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
}

// formatDuration formats a duration as ISO 8601 string
func formatDuration(d time.Duration) string {
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	result := "PT"
	if hours > 0 {
		result += fmt.Sprintf("%dH", hours)
	}
	if minutes > 0 {
		result += fmt.Sprintf("%dM", minutes)
	}
	if seconds > 0 {
		result += fmt.Sprintf("%dS", seconds)
	}
	return result
}
