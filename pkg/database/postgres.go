package database

import (
	"fmt"
	"strconv"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // the "pgx" driver
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // LISTEN (pq.Listener) and pq helpers
)

// Connection pool configuration
type PoolConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// Default pool configuration for production. MaxOpenConns must stay well
// below Postgres's max_connections, multiplied by the number of GoFlow
// instances: past it Postgres refuses new connections ("too many clients
// already") and requests fail. Override with DB_MAX_OPEN_CONNS and
// DB_MAX_IDLE_CONNS.
var DefaultPoolConfig = PoolConfig{
	MaxOpenConns:    80,               // Maximum number of open connections
	MaxIdleConns:    40,               // Maximum number of idle connections
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
		"host=%s port=%v user=%s password=%s dbname=%s sslmode=%s "+
			"connect_timeout=10 "+
			"statement_timeout=30000 "+
			"idle_in_transaction_session_timeout=60000",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, sslMode(env),
	)

	db, err := sqlx.Connect(driverName, connStr+pgxOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Apply connection pool configuration
	configureConnectionPool(db, poolConfigFromEnv(env, DefaultPoolConfig))

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
		"host=%s port=%v user=%s password=%s dbname=%s sslmode=%s "+
			"connect_timeout=10",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, sslMode(env),
	)

	db, err := sqlx.Connect(driverName, connStr+pgxOptions)
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
		"host=%s port=%v user=%s password=%s dbname=%s sslmode=%s",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, sslMode(env),
	)

	db, err := sqlx.Connect(driverName, connStr+pgxOptions)
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
		"host=%s port=%v user=%s password=%s dbname=%s sslmode=%s "+
			"connect_timeout=5 "+
			"statement_timeout=60000 "+
			"pool_max_conns=100",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, sslMode(env),
	)

	db, err := sqlx.Connect(driverName, connStr+pgxOptions)
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
		"host=%s port=%v user=%s password=%s dbname=%s sslmode=%s",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, sslMode(env),
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

// ConnStringForListener builds a connection string for a dedicated
// LISTEN/NOTIFY connection. This is intentionally separate from the pooled
// *sqlx.DB used everywhere else: LISTEN state (which channels a session is
// subscribed to) lives on one specific backend connection, so it can't be
// shared with a connection pool that hands out and recycles connections
// across callers — lib/pq's pq.Listener manages its own dedicated,
// auto-reconnecting connection for exactly this reason.
func ConnStringForListener(util common.Util) string {
	return directConnString(util)
}

// directConnString reaches Postgres itself, bypassing a connection pooler
// such as PgBouncer: DB_DIRECT_HOST / DB_DIRECT_PORT when set, otherwise
// DB_HOST / DB_PORT (no pooler: the same database). Session-level features
// need it — LISTEN, and advisory locks held across transactions — since a
// pooler in transaction mode hands each transaction to any server
// connection.
func directConnString(util common.Util) string {
	env := util.DotEnvVariable
	host, port := env("DB_DIRECT_HOST"), env("DB_DIRECT_PORT")
	if host == "" {
		host = env("DB_HOST")
	}
	if port == "" {
		port = env("DB_PORT")
	}
	return fmt.Sprintf(
		"host=%s port=%v user=%s password=%s dbname=%s sslmode=%s",
		host, port, env("DB_USER"), env("DB_PASS"), env("DB_NAME"), sslMode(env),
	)
}

// NewDirectPostgres opens a small pool reaching Postgres directly (see
// directConnString), for session-level work: the advisory locks of the
// archiver and the retention job.
func NewDirectPostgres(util common.Util) (*sqlx.DB, error) {
	db, err := sqlx.Connect(driverName, directConnString(util)+" connect_timeout=10"+pgxOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database directly: %w", err)
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
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

// sslMode returns DB_SSLMODE (disable, require, verify-ca, verify-full),
// defaulting to "disable" so existing deployments keep their behavior.
func sslMode(env func(string) string) string {
	if mode := env("DB_SSLMODE"); mode != "" {
		return mode
	}
	return "disable"
}

// poolConfigFromEnv applies DB_MAX_OPEN_CONNS / DB_MAX_IDLE_CONNS over base.
func poolConfigFromEnv(env func(string) string, base PoolConfig) PoolConfig {
	if n, err := strconv.Atoi(env("DB_MAX_OPEN_CONNS")); err == nil && n > 0 {
		base.MaxOpenConns = n
	}
	if n, err := strconv.Atoi(env("DB_MAX_IDLE_CONNS")); err == nil && n >= 0 {
		base.MaxIdleConns = n
	}
	if base.MaxIdleConns > base.MaxOpenConns {
		base.MaxIdleConns = base.MaxOpenConns
	}
	return base
}

// NewReadPostgres opens a pool on a read replica (DB_READ_HOST,
// DB_READ_PORT; same credentials), for read-only, lag-tolerant queries —
// history, analytics — so they don't load the primary.
// nil when DB_READ_HOST isn't set: those queries then read the primary.
func NewReadPostgres(util common.Util) (*sqlx.DB, error) {
	env := util.DotEnvVariable
	host := env("DB_READ_HOST")
	if host == "" {
		return nil, nil
	}
	port := env("DB_READ_PORT")
	if port == "" {
		port = "5432"
	}
	connStr := fmt.Sprintf(
		"host=%s port=%v user=%s password=%s dbname=%s sslmode=%s connect_timeout=10 statement_timeout=60000",
		host, port, env("DB_USER"), env("DB_PASS"), env("DB_NAME"), sslMode(env),
	)
	db, err := sqlx.Connect(driverName, connStr+pgxOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the read replica: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	return db, nil
}

// driverName is the database/sql driver of every pool: pgx. lib/pq sent
// each parameterized query in two round trips (prepare, then execute),
// which a connection pooler in transaction mode (PgBouncer) may route to
// two different server connections — "unnamed prepared statement does
// not exist". pq.Listener (LISTEN) keeps using lib/pq on its own
// connection.
const driverName = "pgx"

// pgxOptions: describe a statement once per connection, then send each
// execution in one round trip with the unnamed statement — safe through
// PgBouncer, unlike cached named statements.
const pgxOptions = " default_query_exec_mode=cache_describe"
