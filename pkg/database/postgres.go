package database

import (
	"fmt"

	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

func NewPostgres(util common.Util) (*sqlx.DB, error) {
	env := util.DotEnvVariable
	DB_HOST := env("DB_HOST")
	DB_NAME := env("DB_NAME")
	DB_USER := env("DB_USER")
	DB_PASSWORD := env("DB_PASS")
	DB_PORT := env("DB_PORT")
	return sqlx.Connect("postgres", fmt.Sprintf(
		"host=%s  port=%v user=%s password=%s dbname=%s sslmode=disable",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME,
	))
}

func NewPostgres2(util common.Util) (*sqlx.DB, error) {
	env := util.DotEnvVariable
	DB_HOST := env("DB_HOST")
	DB_NAME := env("DB_NAME")
	DB_USER := env("DB_USER")
	DB_PASSWORD := env("DB_PASS")
	DB_PORT := env("DB_PORT")
	return sqlx.Open("postgres", fmt.Sprintf(
		"host=%s  port=%v user=%s password=%s dbname=%s sslmode=disable",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME,
	))
}
