package database

import (
	"database/sql"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
)

type QueryParameter struct {
	Key      string
	Value    any
	IsBinary bool `default:"false"`
}
type Query struct {
	SQL      string
	JsonData []QueryParameter
}

type Transactions struct {
	Adaptor DataBaseAdapter
	Queries []Query
}

func NewTransaction(db *sqlx.DB) *Transactions {

	return &Transactions{
		Adaptor: *NewDabaseAdapter(db),
		Queries: []Query{},
	}
}

func (t *Transactions) AddQuery(sql string, jsonData []QueryParameter) string {
	t.Queries = append(t.Queries, Query{SQL: sql, JsonData: jsonData})
	return sql
}

func (t *Transactions) AddInsertQuery(tableName string, jsonData []QueryParameter) string {
	sql := t.Adaptor.FormatInsert(tableName, jsonData)
	t.Queries = append(t.Queries, Query{SQL: sql, JsonData: jsonData})
	return sql
}

func (t *Transactions) AddDeleteQuery(tableName string, param []QueryParameter) string {
	sql := t.Adaptor.FormatDelete(tableName, param)
	jsonData := []QueryParameter{}
	jsonData = append(jsonData, param...)

	t.Queries = append(t.Queries, Query{SQL: sql, JsonData: jsonData})
	return sql
}

func (t *Transactions) AddUpdateQuery(tableName string, jsonData []QueryParameter, KeyParam []QueryParameter) string {
	sql := t.Adaptor.FormatUpdate(tableName, jsonData, KeyParam)
	jsonData = append(jsonData, KeyParam...)
	t.Queries = append(t.Queries, Query{SQL: sql, JsonData: jsonData})
	return sql
}

func (t *Transactions) Execute() ([]sql.Result, bool, error) {
	complete := true
	results := []sql.Result{}
	var _error error = nil
	tx, err := t.Adaptor.db.Beginx()
	if err != nil {
		complete = false
		log.Println("Failed to begin transaction:", err)
	}
	for _, query := range t.Queries {

		params := t.Adaptor.QueryParamsToMap(query.JsonData)
		result, err := tx.NamedExec(query.SQL, params)
		if err != nil {
			complete = false
			_error = err
			break
		}
		results = append(results, result)
		time.Sleep(100 * time.Microsecond)
	}
	if complete {
		// Commit the transaction
		if err := tx.Commit(); err != nil {
			log.Println("Failed to commit transaction:", err)
		}
	} else {
		tx.Rollback()

	}
	return results, complete, _error
}
