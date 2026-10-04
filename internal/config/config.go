// pkg/config/config.go
package config

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all configuration for the application
type Config struct {
	// Server configuration
	ServerPort   string
	QueueWorkers int

	// Queue configuration
	QueueMaxRetries             int
	QueueRetryBackoffBase       time.Duration
	QueueRetryMaxBackoff        time.Duration
	QueueRetrySchedulerInterval time.Duration
	QueueStatsUpdateInterval    time.Duration
	QueueCleanupOlderThan       time.Duration
}

// LoadConfig loads configuration from environment variables and .env file
func LoadConfig() *Config {
	// Load .env file if it exists (ignore error if not found)
	if err := godotenv.Load(); err != nil {
		log.Println("⚠️ No .env file found, using environment variables")
	}

	return &Config{
		// Server configuration
		ServerPort:   getEnv("SERVER_PORT", "8080"),
		QueueWorkers: getEnvAsInt("QUEUE_WORKERS", 10),

		// Queue configuration
		QueueMaxRetries:             getEnvAsInt("QUEUE_MAX_RETRIES", 3),
		QueueRetryBackoffBase:       getEnvAsDuration("QUEUE_RETRY_BACKOFF_BASE", 1*time.Second),
		QueueRetryMaxBackoff:        getEnvAsDuration("QUEUE_RETRY_MAX_BACKOFF", 5*time.Minute),
		QueueRetrySchedulerInterval: getEnvAsDuration("QUEUE_RETRY_SCHEDULER_INTERVAL", 5*time.Second),
		QueueStatsUpdateInterval:    getEnvAsDuration("QUEUE_STATS_UPDATE_INTERVAL", 30*time.Second),
		QueueCleanupOlderThan:       getEnvAsDuration("QUEUE_CLEANUP_OLDER_THAN", 24*time.Hour),
	}
}

// Helper functions for environment variables

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvAsInt gets an environment variable as an integer or returns a default value
func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
		log.Printf("⚠️ Invalid integer for %s: %s, using default %d", key, value, defaultValue)
	}
	return defaultValue
}

// getEnvAsBool gets an environment variable as a boolean or returns a default value
func getEnvAsBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
		log.Printf("⚠️ Invalid boolean for %s: %s, using default %v", key, value, defaultValue)
	}
	return defaultValue
}

// getEnvAsDuration gets an environment variable as a duration or returns a default value
func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if durationVal, err := time.ParseDuration(value); err == nil {
			return durationVal
		}
		log.Printf("⚠️ Invalid duration for %s: %s, using default %v", key, value, defaultValue)
	}
	return defaultValue
}

// getEnvAsFloat64 gets an environment variable as a float64 or returns a default value
func getEnvAsFloat64(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
		log.Printf("⚠️ Invalid float for %s: %s, using default %f", key, value, defaultValue)
	}
	return defaultValue
}

// GetQueueOptions returns a map of queue options for convenience
func (c *Config) GetQueueOptions() map[string]interface{} {
	return map[string]interface{}{
		"max_retries":           c.QueueMaxRetries,
		"retry_backoff_base":    c.QueueRetryBackoffBase,
		"retry_max_backoff":     c.QueueRetryMaxBackoff,
		"scheduler_interval":    c.QueueRetrySchedulerInterval,
		"stats_update_interval": c.QueueStatsUpdateInterval,
		"cleanup_older_than":    c.QueueCleanupOlderThan,
	}
}
