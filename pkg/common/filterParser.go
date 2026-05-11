package common

import (
	"fmt"
	"strings"
	"time"
)

type FilterParser struct {
	statements          []string
	parameters          []interface{}
	filters             map[string]interface{}
	tableAlias          string
	limitKey            string
	order               string
	parseUuids          bool
	autoParseStatements bool
	group               string
}

const (
	DEFAULT_LIMIT_KEY        = ""
	DEFAULT_UUID_PARTIAL_KEY = "uuid"
)

// NewFilterParser constructs a new FilterParser instance
func NewFilterParser(filters map[string]interface{}, options map[string]interface{}) *FilterParser {
	fp := &FilterParser{
		statements:          []string{},
		parameters:          []interface{}{},
		filters:             filters,
		tableAlias:          getString(options, "tableAlias", ""),
		limitKey:            getLimitKey(options, "limitKey", DEFAULT_LIMIT_KEY),
		order:               "",
		parseUuids:          getBool(options, "parseUuids", true),
		autoParseStatements: getBool(options, "autoParseStatements", false),
		group:               "",
	}
	return fp
}

// Helper function to get string from options with a default value
func getString(options map[string]interface{}, key string, defaultValue string) string {
	if value, exists := options[key]; exists {
		return value.(string)
	}
	return defaultValue
}

// Helper function to get string from options with a default value
func getLimitKey(options map[string]interface{}, key string, defaultValue string) string {
	if _, exists := options[key]; exists {
		return key
	}
	return defaultValue
}

// Helper function to get bool from options with a default value
func getBool(options map[string]interface{}, key string, defaultValue bool) bool {
	if value, exists := options[key]; exists {
		return value.(bool)
	}
	return defaultValue
}

// fullText filters by text value
func (fp *FilterParser) FullText(filterKey string, columnAlias string, tableAlias string) {
	if columnAlias == "" {
		columnAlias = filterKey
	}
	if tableAlias == "" {
		tableAlias = fp.tableAlias
	}
	tableString := fp.formatTableAlias(tableAlias)

	if value, exists := fp.filters[filterKey]; exists {
		searchString := fmt.Sprintf("%%%s%%", value)
		preparedStatement := fmt.Sprintf("LOWER(%s%s) LIKE ?", tableString, columnAlias)
		fp.addFilter(preparedStatement, searchString)
		delete(fp.filters, filterKey)
	}
}

// period filters by a period
func (fp *FilterParser) Period(filterKey string, columnAlias string, tableAlias string) {
	if columnAlias == "" {
		columnAlias = filterKey
	}
	if tableAlias == "" {
		tableAlias = fp.tableAlias
	}
	tableString := fp.formatTableAlias(tableAlias)

	if value, exists := fp.filters[filterKey]; exists {

		periodService := NewPeriodService(time.Now()) // Or use a specific timestamp if provided
		targetPeriod := periodService.LookupPeriod(value.(string))

		if targetPeriod.Key == "allTime" || targetPeriod.Key == "custom" {
			delete(fp.filters, filterKey)
			return
		}

		periodFromStatement := fmt.Sprintf("DATE(%s%s) >= DATE(?)", tableString, columnAlias)
		periodToStatement := fmt.Sprintf("DATE(%s%s) <= DATE(?)", tableString, columnAlias)
		today := periodService.LookupPeriod("today")
		periodStart, periodEnd := today.Limit()
		fp.addFilter(periodFromStatement, periodStart)
		fp.addFilter(periodToStatement, periodEnd)
		delete(fp.filters, filterKey)
	}
}

// dateFrom filters by a date from
func (fp *FilterParser) DateFrom(filterKey string, columnAlias string, tableAlias string) {
	if columnAlias == "" {
		columnAlias = filterKey
	}
	if tableAlias == "" {
		tableAlias = fp.tableAlias
	}
	tableString := fp.formatTableAlias(tableAlias)

	if value, exists := fp.filters[filterKey]; exists {
		preparedStatement := fmt.Sprintf("DATE(%s%s) >= DATE(?)", tableString, columnAlias)
		fp.addFilter(preparedStatement, value)
		delete(fp.filters, filterKey)
	}
}

// dateTo filters by a date to
func (fp *FilterParser) DateTo(filterKey string, columnAlias string, tableAlias string) {
	if columnAlias == "" {
		columnAlias = filterKey
	}
	if tableAlias == "" {
		tableAlias = fp.tableAlias
	}
	tableString := fp.formatTableAlias(tableAlias)

	if value, exists := fp.filters[filterKey]; exists {
		preparedStatement := fmt.Sprintf("DATE(%s%s) <= DATE(?)", tableString, columnAlias)
		fp.addFilter(preparedStatement, value)
		delete(fp.filters, filterKey)
	}
}

// equals filters for equality
func (fp *FilterParser) Equals(filterKey string, columnAlias string, tableAlias string, isArray bool) {
	if columnAlias == "" {
		columnAlias = filterKey
	}
	if tableAlias == "" {
		tableAlias = fp.tableAlias
	}
	tableString := fp.formatTableAlias(tableAlias)

	if value, exists := fp.filters[filterKey]; exists {
		valueString := "?"
		var preparedStatement string
		if isArray {
			preparedStatement = fmt.Sprintf("%s%s IN (%s)", tableString, columnAlias, valueString)
		} else {
			preparedStatement = fmt.Sprintf("%s%s = %s", tableString, columnAlias, valueString)
		}
		fp.addFilter(preparedStatement, value)
		delete(fp.filters, filterKey)
	}
}

// custom allows for custom SQL
func (fp *FilterParser) Custom(filterKey string, preparedStatement string, preparedValue interface{}) {
	if value, exists := fp.filters[filterKey]; exists {
		searchValue := preparedValue
		if searchValue == nil || searchValue == "" {
			searchValue = value
		}
		fp.statements = append(fp.statements, preparedStatement)
		if values, ok := searchValue.([]interface{}); ok {
			fp.parameters = append(fp.parameters, values...)
		} else {
			fp.parameters = append(fp.parameters, searchValue)
		}
		delete(fp.filters, filterKey)
	}
}

// setOrder sets the SQL ordering
func (fp *FilterParser) SetOrder(orderString string) {
	fp.order = orderString
}

// setGroup sets the SQL groups
func (fp *FilterParser) SetGroup(groupString string) {
	fp.group = groupString
}

// applyQuery applies the filters to the SQL query
func (fp *FilterParser) ApplyQuery(sql string) (string, []interface{}) {
	limitCondition := fp.parseLimit()
	if fp.autoParseStatements {
		fp.parseDefaultFilters()
	}
	conditionStatements := fp.parseStatements()
	order := fp.order
	group := fp.group
	sql1 := fmt.Sprintf("%s WHERE %s %s %s %s", sql, conditionStatements, group, order, limitCondition)
	var i = 1
	for {
		var iindex = strings.Index(sql1, "?")
		if iindex != -1 {
			sql1 = strings.Replace(sql1, "?", fmt.Sprintf("$%v", i), 1)
		} else {
			break
		}
		i++
	}
	return sql1, fp.parameters

}

// parameters returns the parameters for the query
func (fp *FilterParser) Parameters() []interface{} {
	return fp.parameters
}

// formatTableAlias formats the table alias
func (fp *FilterParser) formatTableAlias(table string) string {
	if table != "" {
		return table + "."
	}
	return ""
}

// addFilter populates the statements and parameters
func (fp *FilterParser) addFilter(statement string, parameter interface{}) {
	fp.statements = append(fp.statements, statement)
	fp.parameters = append(fp.parameters, parameter)
}

// parseDefaultFilters parses any default filters
func (fp *FilterParser) parseDefaultFilters() {
	reservedKeywords := []string{"limit", "detailed"}
	for _, key := range reservedKeywords {
		delete(fp.filters, key)
	}
	for key, value := range fp.filters {
		valueString := "?"
		tableString := fp.formatTableAlias(fp.tableAlias)
		if fp.parseUuids && strings.Contains(key, DEFAULT_UUID_PARTIAL_KEY) {
			valueString = "HUID(?)"
		}
		fp.addFilter(fmt.Sprintf("%s%s = %s", tableString, key, valueString), value)
	}
}

// parseStatements parses the filter statements
func (fp *FilterParser) parseStatements() string {
	if len(fp.statements) == 0 {
		return "TRUE" // Default condition
	}
	return strings.Join(fp.statements, " AND ")
}

// parseLimit parses the limit
func (fp *FilterParser) parseLimit() string {
	limitString := ""
	if fp.filters[fp.limitKey] != nil {
		limitString = fmt.Sprintf("LIMIT %v", fp.filters[fp.limitKey])
	}
	return limitString
}
