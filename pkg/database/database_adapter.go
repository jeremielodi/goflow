package database

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

type DataBaseAdapter struct {
	db *sqlx.DB
}

func NewDabaseAdapter(db *sqlx.DB) *DataBaseAdapter {
	return &DataBaseAdapter{
		db: db,
	}
}

/**
 *
 * Fun Insert
 * @param {*} tableName the table name to inserte into
 * @param {*} jsonData defferent values to insert
 */
func (adaptor DataBaseAdapter) Insert(tableName string, parameters []QueryParameter) (sql.Result, error) {
	sql := adaptor.FormatInsertArrayVsalues(tableName, parameters)

	return adaptor.db.DB.Exec(sql, adaptor.QueryParamsValues(parameters)...)
}

func (adaptor DataBaseAdapter) InsertSlice(tableName string, rows []map[string]interface{}) ([]sql.Result, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("NO_DATA_FOUND")
	}

	// Prepare query columns based on first row keys
	var q1 []QueryParameter
	for key, val := range rows[0] {
		q1 = append(q1, QueryParameter{
			Key:   key,
			Value: val,
		})
	}

	// Generate insert statement
	query := adaptor.FormatInsertByColName(tableName, q1)

	numCols := len(q1)
	maxParams := 65535
	maxRowsPerInsert := maxParams / numCols

	var results []sql.Result

	for start := 0; start < len(rows); start += maxRowsPerInsert {
		end := start + maxRowsPerInsert
		if end > len(rows) {
			end = len(rows)
		}

		batch := rows[start:end]

		// Execute insert for this batch
		result, err := adaptor.db.NamedExec(query, batch)
		if err != nil {
			return results, err
		}

		results = append(results, result)
	}

	return results, nil
}

func (adaptor DataBaseAdapter) Delete(tableName string, condition []QueryParameter) (sql.Result, error) {
	sql := adaptor.FormatDeleteWithoutParameters(tableName, condition)
	params := condition
	return adaptor.db.DB.Exec(sql, adaptor.QueryParamsValues(params)...)
}

func (adaptor DataBaseAdapter) QueryParamsToMap(params []QueryParameter) map[string]interface{} {
	maps := map[string]interface{}{}
	for _, item := range params {
		maps[item.Key] = item.Value
	}
	return maps
}

func (adaptor DataBaseAdapter) QueryParamsValues(params []QueryParameter) []any {
	maps := []any{}
	for _, item := range params {
		maps = append(maps, item.Value)
	}
	return maps
}

func (db *DataBaseAdapter) FormatDelete(tableName string, conditionsParam []QueryParameter) string {
	// Build WHERE clause
	whereSql := ""
	for j, cond := range conditionsParam {
		if j != 0 {
			whereSql += " AND "
		}
		whereSql += fmt.Sprintf("%s=:%s", cond.Key, cond.Key)
	}

	// Final SQL string
	return fmt.Sprintf(`DELETE FROM  %s WHERE %s`, tableName, whereSql)

}
func (db DataBaseAdapter) FormatDeleteWithoutParameters(tableName string, conditionsParam []QueryParameter) string {

	// Build WHERE clause
	whereSql := ""
	for j, cond := range conditionsParam {
		if j != 0 && (len(conditionsParam)-1 < j) {
			whereSql += " AND "
		}
		whereSql += fmt.Sprintf("%s=$%d", cond.Key, j+1)
	}

	// Final SQL string
	return fmt.Sprintf(`DELETE FROM  %s WHERE %s`, tableName, whereSql)

}

func (db DataBaseAdapter) FormatInsert(tableName string, parameters []QueryParameter) string {
	colSql := ""
	valSql := ""
	var i int = 0
	for _, item := range parameters {
		k := item.Key
		if i != 0 {
			colSql += ", "
			valSql += ", "
		}
		colSql += k

		valSql += fmt.Sprintf(":%s", k)

		i++
	}

	sql := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, tableName, colSql, valSql)
	return sql
}

func (db DataBaseAdapter) FormatInsertArrayVsalues(tableName string, parameters []QueryParameter) string {
	colSql := ""
	valSql := ""
	var i int = 0
	for _, item := range parameters {
		k := item.Key
		if i != 0 {
			colSql += ", "
			valSql += ", "
		}
		colSql += k
		valSql += fmt.Sprintf("$%v", i+1)
		i++
	}

	sql := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, tableName, colSql, valSql)
	return sql
}
func (db DataBaseAdapter) FormatInsertByColName(tableName string, parameters []QueryParameter) string {
	colSql := ""
	valSql := ""
	var i int = 0
	for _, item := range parameters {
		k := item.Key
		if i != 0 {
			colSql += ", "
			valSql += ", "
		}
		colSql += k
		valSql += fmt.Sprintf(":%v", k)
		i++
	}

	sql := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, tableName, colSql, valSql)
	return sql
}
func (adaptor DataBaseAdapter) FormatUpdate(tableName string, parameters []QueryParameter, conditionsParam []QueryParameter) string {
	colSql := ""

	// Build SET clause
	for i, item := range parameters {
		k := item.Key
		if i != 0 {
			colSql += ", "
		}

		if !item.IsBinary {
			colSql += fmt.Sprintf("%s=:%s", k, k)
		}
	}

	// Build WHERE clause
	whereSql := ""
	for j, cond := range conditionsParam {
		if j != 0 {
			whereSql += " AND "
		}
		whereSql += fmt.Sprintf("%s=:%s", cond.Key, cond.Key)
	}

	// Final SQL string
	return fmt.Sprintf(`UPDATE %s SET %s WHERE %s`, tableName, colSql, whereSql)
}

func (adaptor DataBaseAdapter) FormatUpdate2(tableName string, parameters []QueryParameter, idParam QueryParameter) string {
	colSql := ""

	var i int = 0
	for _, item := range parameters {
		k := item.Key
		if i != 0 {
			colSql += ", "
		}
		if item.IsBinary {
			colSql += fmt.Sprintf("%s=HUID(?)", k)
		}
		if !item.IsBinary {
			colSql += fmt.Sprintf("%s=$%v", k, i+1)
		}

		i++
	}

	if idParam.IsBinary {
		return fmt.Sprintf(`UPDATE %s SET %s WHERE %s=HUID(?)`, tableName, colSql, idParam.Key)
	}

	return fmt.Sprintf(`UPDATE %s SET %s WHERE %s=$%v`, tableName, colSql, idParam.Key, i+1)

}

/**
 *
 * Fun Insert
 * @param {*} tableName the table name to inserte into
 * @param {*} parameters defferent values to insert
 */
func (adaptor DataBaseAdapter) Update(tableName string, parameters []QueryParameter, KeyParam QueryParameter) (sql.Result, error) {
	sql := adaptor.FormatUpdate2(tableName, parameters, KeyParam)
	parameters = append(parameters, KeyParam)
	var mapParams = adaptor.QueryParamsValues(parameters)
	return adaptor.db.DB.Exec(sql, mapParams...)
}

func (adaptor DataBaseAdapter) BatchInsert(tableName string, rows [][]QueryParameter) (sql.Result, error) {
	if len(rows) == 0 || tableName == "" {
		return nil, fmt.Errorf("tableName and rows cannot be empty")
	}

	columns := []string{}
	if len(rows[0]) > 0 {
		for _, param := range rows[0] {
			columns = append(columns, param.Key)
		}
	}

	placeholders := ""
	values := []any{}
	for i, row := range rows {
		rowPlaceholders := ""
		for j, param := range row {
			if j > 0 {
				rowPlaceholders += ", "
			}
			rowPlaceholders += "?"
			values = append(values, param.Value)
		}
		if i > 0 {
			placeholders += ", "
		}
		placeholders += fmt.Sprintf("(%s)", rowPlaceholders)
	}

	query := fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s`, tableName, strings.Join(columns, ", "), placeholders)
	return adaptor.db.DB.Exec(query, values...)
}
