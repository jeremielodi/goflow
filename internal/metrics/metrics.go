// Package metrics exposes GoFlow's Prometheus metrics on GET /metrics:
// Go runtime/process metrics (default collectors), HTTP request counts and
// latencies, the database connection pool, engine state read from Postgres
// at scrape time (running instances, pending jobs, open incidents...),
// table sizes for the tables that grow without bound, and retention purges.
package metrics

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "goflow_http_requests_total",
		Help: "HTTP requests handled, by method, route pattern and status code.",
	}, []string{"method", "route", "status"})

	httpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "goflow_http_request_duration_seconds",
		Help:    "HTTP request latency, by method and route pattern.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})

	// RetentionDeletedRows is incremented by the retention purge
	// (internal/retention) for every row it deletes, labeled by table.
	// FeelShadowMismatches counts FEEL expressions where the native FEEL
	// interpreter and the CEL path disagreed, in FEEL_MODE=shadow
	// (internal/common), by kind ("condition" or "value").
	FeelShadowMismatches = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "goflow_feel_shadow_mismatches_total",
		Help: "FEEL expressions on which the native interpreter and the CEL path disagreed (FEEL_MODE=shadow).",
	}, []string{"kind"})

	RetentionDeletedRows = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "goflow_retention_deleted_rows_total",
		Help: "Rows deleted by the retention purge, by table.",
	}, []string{"table"})
)

// Register installs every GoFlow collector on the default registry, which
// already carries the Go runtime and process collectors.
func Register(db *sqlx.DB) {
	prometheus.MustRegister(
		httpRequests,
		httpDuration,
		RetentionDeletedRows,
		FeelShadowMismatches,
		collectors.NewDBStatsCollector(db.DB, "goflow"),
		&engineCollector{db: db},
	)
}

// RegisterGaugeFunc registers a gauge whose value is read at scrape time.
func RegisterGaugeFunc(name, help string, fn func() float64) {
	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, fn))
}

// Middleware records request count and latency. It labels by the matched
// route pattern (e.g. /engine-rest/tasks/:id), never the raw path, so IDs
// in URLs can't blow up label cardinality.
func Middleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()

		route := c.Route().Path
		if route == "" || route == "/" && c.Path() != "/" {
			route = "unmatched"
		}
		status := c.Response().StatusCode()
		if fe, ok := err.(*fiber.Error); ok {
			status = fe.Code
		}
		// Fiber's strings point into buffers it reuses for the next request;
		// Prometheus keeps label values forever, so they must be copies.
		method := strings.Clone(c.Method())
		route = strings.Clone(route)
		httpRequests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
		httpDuration.WithLabelValues(method, route).Observe(time.Since(start).Seconds())
		return err
	}
}

// Handler serves the Prometheus exposition format. When token is non-empty,
// requests must carry "Authorization: Bearer <token>".
func Handler(token string) fiber.Handler {
	h := adaptor.HTTPHandler(promhttp.Handler())
	return func(c *fiber.Ctx) error {
		if token != "" && c.Get(fiber.HeaderAuthorization) != "Bearer "+token {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		return h(c)
	}
}

// engineCollector reads engine state from Postgres on every scrape. All
// queries hit live tables that stay small (finished instances are archived
// out of them), plus catalog lookups for sizes, so a scrape stays cheap.
type engineCollector struct {
	db *sqlx.DB
}

var (
	descInstances = prometheus.NewDesc("goflow_process_instances",
		"Live process instances, by status.", []string{"status"}, nil)
	descJobs = prometheus.NewDesc("goflow_jobs",
		"Jobs in the live jobs table, by status.", []string{"status"}, nil)
	descTasks = prometheus.NewDesc("goflow_user_tasks",
		"Live user tasks, by status.", []string{"status"}, nil)
	descIncidentsOpen = prometheus.NewDesc("goflow_incidents_open",
		"Open incidents.", nil, nil)
	descTimersPending = prometheus.NewDesc("goflow_timers_pending",
		"Timers not yet triggered.", nil, nil)
	descTimersOverdue = prometheus.NewDesc("goflow_timers_overdue",
		"Timers past their due time by more than one minute and still not triggered.", nil, nil)
	descTableBytes = prometheus.NewDesc("goflow_table_size_bytes",
		"Total on-disk size (table + indexes + TOAST) of tables that grow over time.", []string{"table"}, nil)
	descTableRows = prometheus.NewDesc("goflow_table_rows_estimate",
		"Planner row-count estimate (pg_class.reltuples) of tables that grow over time.", []string{"table"}, nil)
	descScrapeError = prometheus.NewDesc("goflow_metrics_scrape_error",
		"1 if the last engine-state query failed, else 0.", nil, nil)
)

// growingTables are the tables nothing trims except the retention purge.
var growingTables = []string{
	"audit_logs",
	"historic_process_instances",
	"historic_activity_instances",
	"historic_tasks",
	"historic_variables",
	"historic_incidents",
	"eventlog_projected_instances",
	"eventlog_projected_jobs",
}

func (c *engineCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descInstances, descJobs, descTasks, descIncidentsOpen,
		descTimersPending, descTimersOverdue, descTableBytes, descTableRows, descScrapeError} {
		ch <- d
	}
}

func (c *engineCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	failed := false
	fail := func(what string, err error) {
		failed = true
		log.Printf("metrics: %s query failed: %v", what, err)
	}

	type statusCount struct {
		Status string  `db:"status"`
		Count  float64 `db:"count"`
	}
	grouped := func(desc *prometheus.Desc, what, query string) {
		var rows []statusCount
		if err := c.db.SelectContext(ctx, &rows, query); err != nil {
			fail(what, err)
			return
		}
		for _, r := range rows {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, r.Count, r.Status)
		}
	}
	grouped(descInstances, "process instances", `SELECT status::text AS status, count(*) AS count FROM process_instances GROUP BY status`)
	grouped(descJobs, "jobs", `SELECT status, count(*) AS count FROM jobs GROUP BY status`)
	grouped(descTasks, "tasks", `SELECT status::text AS status, count(*) AS count FROM tasks GROUP BY status`)

	single := func(desc *prometheus.Desc, what, query string) {
		var v float64
		if err := c.db.GetContext(ctx, &v, query); err != nil {
			fail(what, err)
			return
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v)
	}
	single(descIncidentsOpen, "incidents", `SELECT count(*) FROM incidents WHERE state = 'open'`)
	single(descTimersPending, "timers", `SELECT count(*) FROM timer_jobs WHERE NOT is_triggered`)
	single(descTimersOverdue, "overdue timers", `SELECT count(*) FROM timer_jobs WHERE NOT is_triggered AND due_at < NOW() - INTERVAL '1 minute'`)

	type tableStat struct {
		Name  string  `db:"name"`
		Bytes float64 `db:"bytes"`
		Rows  float64 `db:"rows"`
	}
	var stats []tableStat
	if err := c.db.SelectContext(ctx, &stats, `
		SELECT c.relname AS name,
		       pg_total_relation_size(c.oid)::float8 AS bytes,
		       GREATEST(c.reltuples, 0)::float8 AS rows
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname = ANY($1)`,
		pq.Array(growingTables)); err != nil {
		fail("table sizes", err)
	} else {
		for _, s := range stats {
			ch <- prometheus.MustNewConstMetric(descTableBytes, prometheus.GaugeValue, s.Bytes, s.Name)
			ch <- prometheus.MustNewConstMetric(descTableRows, prometheus.GaugeValue, s.Rows, s.Name)
		}
	}

	scrapeErr := 0.0
	if failed {
		scrapeErr = 1
	}
	ch <- prometheus.MustNewConstMetric(descScrapeError, prometheus.GaugeValue, scrapeErr)
}
