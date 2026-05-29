package database

import (
	"fmt"
	"time"

	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// Connection pool configuration
type PoolConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// Default pool configuration for production
var DefaultPoolConfig = PoolConfig{
	MaxOpenConns:    100,              // Maximum number of open connections
	MaxIdleConns:    50,               // Maximum number of idle connections
	ConnMaxLifetime: 1 * time.Hour,    // Maximum lifetime of a connection
	ConnMaxIdleTime: 10 * time.Minute, // Maximum idle time before closing
}

// Development pool configuration (lower resources)
var DevPoolConfig = PoolConfig{
	MaxOpenConns:    20,
	MaxIdleConns:    10,
	ConnMaxLifetime: 30 * time.Minute,
	ConnMaxIdleTime: 5 * time.Minute,
}

// High-performance pool configuration
var HighPerfPoolConfig = PoolConfig{
	MaxOpenConns:    100,
	MaxIdleConns:    50,
	ConnMaxLifetime: 2 * time.Hour,
	ConnMaxIdleTime: 30 * time.Minute,
}

// NewPostgres creates a new PostgreSQL connection with connection pooling
func NewPostgres(util common.Util) (*sqlx.DB, error) {
	env := util.DotEnvVariable

	DB_HOST := env("DB_HOST")
	DB_NAME := env("DB_NAME")
	DB_USER := env("DB_USER")
	DB_PASSWORD := env("DB_PASS")
	DB_PORT := env("DB_PORT")

	// Build connection string with additional performance parameters
	connStr := fmt.Sprintf(
		"host=%s port=%v user=%s password=%s dbname=%s sslmode=disable "+
			"connect_timeout=10 "+
			"statement_timeout=30000 "+
			"idle_in_transaction_session_timeout=60000",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME,
	)

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Apply connection pool configuration
	configureConnectionPool(db, DefaultPoolConfig)

	// Verify connection is alive
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

// NewPostgresWithPool creates a PostgreSQL connection with custom pool configuration
func NewPostgresWithPool(util common.Util, config PoolConfig) (*sqlx.DB, error) {
	env := util.DotEnvVariable

	DB_HOST := env("DB_HOST")
	DB_NAME := env("DB_NAME")
	DB_USER := env("DB_USER")
	DB_PASSWORD := env("DB_PASS")
	DB_PORT := env("DB_PORT")

	connStr := fmt.Sprintf(
		"host=%s port=%v user=%s password=%s dbname=%s sslmode=disable "+
			"connect_timeout=10",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME,
	)

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	configureConnectionPool(db, config)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

// NewPostgresDev creates a development database connection with lower resource usage
func NewPostgresDev(util common.Util) (*sqlx.DB, error) {
	env := util.DotEnvVariable

	DB_HOST := env("DB_HOST")
	DB_NAME := env("DB_NAME")
	DB_USER := env("DB_USER")
	DB_PASSWORD := env("DB_PASS")
	DB_PORT := env("DB_PORT")

	connStr := fmt.Sprintf(
		"host=%s port=%v user=%s password=%s dbname=%s sslmode=disable",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME,
	)

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	configureConnectionPool(db, DevPoolConfig)

	return db, nil
}

// NewPostgresHighPerf creates a high-performance database connection
func NewPostgresHighPerf(util common.Util) (*sqlx.DB, error) {
	env := util.DotEnvVariable

	DB_HOST := env("DB_HOST")
	DB_NAME := env("DB_NAME")
	DB_USER := env("DB_USER")
	DB_PASSWORD := env("DB_PASS")
	DB_PORT := env("DB_PORT")

	connStr := fmt.Sprintf(
		"host=%s port=%v user=%s password=%s dbname=%s sslmode=disable "+
			"connect_timeout=5 "+
			"statement_timeout=60000 "+
			"pool_max_conns=100",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME,
	)

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	configureConnectionPool(db, HighPerfPoolConfig)

	return db, nil
}

// configureConnectionPool applies connection pool settings to the database
func configureConnectionPool(db *sqlx.DB, config PoolConfig) {
	// Set maximum number of open connections to the database
	db.SetMaxOpenConns(config.MaxOpenConns)

	// Set maximum number of idle connections in the pool
	db.SetMaxIdleConns(config.MaxIdleConns)

	// Set maximum lifetime of a connection
	db.SetConnMaxLifetime(config.ConnMaxLifetime)

	// Set maximum idle time before closing idle connections
	db.SetConnMaxIdleTime(config.ConnMaxIdleTime)

	// Log pool configuration
	fmt.Printf("📊 Database Connection Pool configured:\n")
	fmt.Printf("   Max Open Connections: %d\n", config.MaxOpenConns)
	fmt.Printf("   Max Idle Connections: %d\n", config.MaxIdleConns)
	fmt.Printf("   Conn Max Lifetime: %v\n", config.ConnMaxLifetime)
	fmt.Printf("   Conn Max Idle Time: %v\n", config.ConnMaxIdleTime)
}

// NewPostgres2 creates a new PostgreSQL connection using Open (not Connect)
// Deprecated: Use NewPostgres instead
func NewPostgres2(util common.Util) (*sqlx.DB, error) {
	env := util.DotEnvVariable

	DB_HOST := env("DB_HOST")
	DB_NAME := env("DB_NAME")
	DB_USER := env("DB_USER")
	DB_PASSWORD := env("DB_PASS")
	DB_PORT := env("DB_PORT")

	connStr := fmt.Sprintf(
		"host=%s port=%v user=%s password=%s dbname=%s sslmode=disable",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME,
	)

	db, err := sqlx.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Apply connection pool configuration
	configureConnectionPool(db, DefaultPoolConfig)

	// Ping to verify connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

// GetPoolStats returns current connection pool statistics for monitoring
func GetPoolStats(db *sqlx.DB) map[string]interface{} {
	stats := db.Stats()
	return map[string]interface{}{
		"max_open_connections": stats.MaxOpenConnections,
		"open_connections":     stats.OpenConnections,
		"in_use":               stats.InUse,
		"idle":                 stats.Idle,
		"wait_count":           stats.WaitCount,
		"wait_duration":        stats.WaitDuration.String(),
		"max_idle_closed":      stats.MaxIdleClosed,
		"max_lifetime_closed":  stats.MaxLifetimeClosed,
	}
}

// HealthCheck performs a simple database health check
func HealthCheck(db *sqlx.DB) error {
	return db.Ping()
}
