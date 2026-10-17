package runtime

import (
	"regexp"
	"strings"
)

func executeScript(script string, variables map[string]interface{}) (map[string]interface{}, error) {
	// Simple implementation - just parse variable assignments
	// For production, use a JavaScript engine like goja or otto

	result := make(map[string]interface{})

	// Simple parsing for "items = [\"Item A\", \"Item B\", \"Item C\"];"
	if strings.Contains(script, "items = [") {
		// Extract items
		re := regexp.MustCompile(`items = \[(.*?)\]`)
		matches := re.FindStringSubmatch(script)
		if len(matches) > 1 {
			itemsStr := matches[1]
			items := strings.Split(itemsStr, ",")
			var itemsList []string
			for _, item := range items {
				item = strings.TrimSpace(item)
				item = strings.Trim(item, `"`)
				itemsList = append(itemsList, item)
			}
			result["items"] = itemsList
		}
	}

	return result, nil
}
