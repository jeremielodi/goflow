// Command loadtest measures how many process instances per second a GoFlow
// server sustains. It deploys a minimal process (start -> service task ->
// end), runs job workers that complete its jobs, and starts instances at
// increasing rates (open loop: a slow server doesn't slow the generator
// down), reporting per step the start latency, errors, job throughput and
// end-to-end time. It stops at the first step the server can't sustain.
//
//	go run ./cmd/loadtest -url http://127.0.0.1:8080 -email admin@goflow.com -password ... \
//	    -rates 10,25,50,100,200 -step 30s
//
// Run it against a non-production server: it creates real instances.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const processKey = "LoadTestProcess"
const jobTopic = "loadtest_job"

const bpmn = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:camunda="http://camunda.org/schema/1.0/bpmn" id="LoadTest" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="` + processKey + `" isExecutable="true">
    <bpmn:startEvent id="lt_start"/>
    <bpmn:sequenceFlow id="lt1" sourceRef="lt_start" targetRef="lt_job"/>
    <bpmn:serviceTask id="lt_job" camunda:type="external" camunda:topic="` + jobTopic + `"/>
    <bpmn:sequenceFlow id="lt2" sourceRef="lt_job" targetRef="lt_end"/>
    <bpmn:endEvent id="lt_end"/>
  </bpmn:process>
</bpmn:definitions>`

type config struct {
	url, email, password string
	rates                []int
	step                 time.Duration
	workers              int
	maxInFlight          int
	stopP95              time.Duration
	stopErrorRate        float64
}

type client struct {
	base  string
	token string
	http  *http.Client
}

func main() {
	cfg := parseFlags()
	c := &client{base: strings.TrimRight(cfg.url, "/"), http: &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.maxInFlight + cfg.workers,
			MaxIdleConnsPerHost: cfg.maxInFlight + cfg.workers,
		},
	}}

	if err := c.login(cfg.email, cfg.password); err != nil {
		fail("login: %v", err)
	}
	if err := c.deploy(); err != nil {
		fail("deploy: %v", err)
	}
	fmt.Printf("Deployed %s. Workers: %d, max in-flight starts: %d, step: %s\n\n", processKey, cfg.workers, cfg.maxInFlight, cfg.step)

	run := newRun()
	stopWorkers := make(chan struct{})
	var workersDone sync.WaitGroup
	for i := 0; i < cfg.workers; i++ {
		workersDone.Add(1)
		go func(id int) {
			defer workersDone.Done()
			c.worker(fmt.Sprintf("loadtest-%d", id), run, stopWorkers)
		}(i)
	}

	fmt.Printf("%8s %9s %8s %7s %9s %9s %9s %9s %9s %8s\n",
		"target/s", "started/s", "errors", "err %", "p50", "p95", "p99", "jobs/s", "e2e p95", "backlog")
	var results []stepResult
	for _, rate := range cfg.rates {
		r := c.runStep(rate, cfg, run)
		results = append(results, r)
		fmt.Println(r.row())
		if reason := r.unsustainable(cfg); reason != "" {
			fmt.Printf("\nStopped: %s at %d starts/s.\n", reason, rate)
			break
		}
	}
	close(stopWorkers)
	workersDone.Wait()

	best := 0
	for _, r := range results {
		if r.unsustainable(cfg) == "" {
			best = r.target
		}
	}
	fmt.Printf("\nHighest sustained rate: %d process instances/s end to end (p95 start latency <= %s, errors <= %.1f%%, jobs completed as fast as instances start).\n",
		best, cfg.stopP95, cfg.stopErrorRate*100)
}

func parseFlags() config {
	var cfg config
	var rates string
	flag.StringVar(&cfg.url, "url", "http://127.0.0.1:8080", "GoFlow base URL")
	flag.StringVar(&cfg.email, "email", os.Getenv("LOADTEST_EMAIL"), "login email (or LOADTEST_EMAIL)")
	flag.StringVar(&cfg.password, "password", os.Getenv("LOADTEST_PASSWORD"), "login password (or LOADTEST_PASSWORD)")
	flag.StringVar(&rates, "rates", "10,25,50,100,200,400", "comma-separated start rates (instances/s), one step each")
	flag.DurationVar(&cfg.step, "step", 30*time.Second, "duration of each rate step")
	flag.IntVar(&cfg.workers, "workers", 16, "concurrent job workers")
	flag.IntVar(&cfg.maxInFlight, "max-in-flight", 500, "max concurrent start requests; starts beyond it count as errors")
	flag.DurationVar(&cfg.stopP95, "stop-p95", time.Second, "stop when the p95 start latency exceeds this")
	flag.Float64Var(&cfg.stopErrorRate, "stop-errors", 0.01, "stop when the error rate exceeds this fraction")
	flag.Parse()
	if cfg.email == "" || cfg.password == "" {
		fail("-email and -password (or LOADTEST_EMAIL / LOADTEST_PASSWORD) are required")
	}
	for _, s := range strings.Split(rates, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n <= 0 {
			fail("invalid rate %q", s)
		}
		cfg.rates = append(cfg.rates, n)
	}
	return cfg
}

func fail(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "loadtest: "+format+"\n", args...)
	os.Exit(1)
}

// run tracks instances across steps: when each was started, and job
// completions.
type run struct {
	started   sync.Map // instance ID -> time.Time
	completed atomic.Int64
	mu        sync.Mutex
	e2e       []time.Duration
}

func newRun() *run { return &run{} }

func (r *run) takeE2E() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.e2e
	r.e2e = nil
	return out
}

type stepResult struct {
	target        int
	attempted     int64
	ok            int64
	errors        int64
	errorCodes    map[string]int64
	latencies     []time.Duration
	e2e           []time.Duration
	jobsCompleted int64
	backlog       int64
	duration      time.Duration
}

func (c *client) runStep(rate int, cfg config, rn *run) stepResult {
	res := stepResult{target: rate, errorCodes: map[string]int64{}, duration: cfg.step}
	var mu sync.Mutex
	var wg sync.WaitGroup
	inFlight := make(chan struct{}, cfg.maxInFlight)
	completedBefore := rn.completed.Load()

	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	deadline := time.Now().Add(cfg.step)
	for now := range ticker.C {
		if now.After(deadline) {
			break
		}
		res.attempted++
		select {
		case inFlight <- struct{}{}:
		default:
			mu.Lock()
			res.errors++
			res.errorCodes["client saturated"]++
			mu.Unlock()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-inFlight }()
			start := time.Now()
			id, code, err := c.start()
			lat := time.Since(start)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				res.errors++
				key := code
				if key == "" {
					key = "network"
				}
				res.errorCodes[key]++
				return
			}
			res.ok++
			res.latencies = append(res.latencies, lat)
			rn.started.Store(id, start)
		}()
	}
	ticker.Stop()
	wg.Wait()
	// Let the workers drain what this step started.
	time.Sleep(2 * time.Second)

	res.jobsCompleted = rn.completed.Load() - completedBefore
	res.e2e = rn.takeE2E()
	pending := int64(0)
	rn.started.Range(func(_, _ interface{}) bool { pending++; return true })
	res.backlog = pending
	return res
}

func (r stepResult) row() string {
	sort.Slice(r.latencies, func(i, j int) bool { return r.latencies[i] < r.latencies[j] })
	sort.Slice(r.e2e, func(i, j int) bool { return r.e2e[i] < r.e2e[j] })
	secs := r.duration.Seconds()
	return fmt.Sprintf("%8d %9.1f %8d %6.2f%% %9s %9s %9s %9.1f %9s %8d%s",
		r.target, float64(r.ok)/secs, r.errors, r.errorRate()*100,
		pct(r.latencies, 0.50), pct(r.latencies, 0.95), pct(r.latencies, 0.99),
		float64(r.jobsCompleted)/secs, pct(r.e2e, 0.95), r.backlog, r.codes())
}

func (r stepResult) codes() string {
	if len(r.errorCodes) == 0 {
		return ""
	}
	parts := []string{}
	for code, n := range r.errorCodes {
		parts = append(parts, fmt.Sprintf("%s×%d", code, n))
	}
	sort.Strings(parts)
	return "  [" + strings.Join(parts, ", ") + "]"
}

func (r stepResult) errorRate() float64 {
	if r.attempted == 0 {
		return 0
	}
	return float64(r.errors) / float64(r.attempted)
}

// unsustainable says why the server didn't keep up with this step, or "".
func (r stepResult) unsustainable(cfg config) string {
	sort.Slice(r.latencies, func(i, j int) bool { return r.latencies[i] < r.latencies[j] })
	switch {
	case r.errorRate() > cfg.stopErrorRate:
		return fmt.Sprintf("error rate %.1f%%", r.errorRate()*100)
	case pctDur(r.latencies, 0.95) > cfg.stopP95:
		return fmt.Sprintf("p95 start latency %s", pct(r.latencies, 0.95))
	case r.ok > 0 && float64(r.jobsCompleted) < 0.9*float64(r.ok):
		return fmt.Sprintf("jobs completed at %.1f/s for %.1f/s started (backlog %d): workers or engine not keeping up",
			float64(r.jobsCompleted)/r.duration.Seconds(), float64(r.ok)/r.duration.Seconds(), r.backlog)
	}
	return ""
}

func pctDur(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	return d[int(float64(len(d)-1)*p)]
}

func pct(d []time.Duration, p float64) string {
	if len(d) == 0 {
		return "-"
	}
	return pctDur(d, p).Round(time.Millisecond).String()
}

// ---- HTTP calls ----

func (c *client) do(method, path string, body io.Reader, contentType string) (*http.Response, error) {
	req, err := http.NewRequest(method, c.base+path, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return c.http.Do(req)
}

func (c *client) postJSON(path string, in, out interface{}) (int, error) {
	b, err := json.Marshal(in)
	if err != nil {
		return 0, err
	}
	resp, err := c.do(http.MethodPost, path, bytes.NewReader(b), "application/json")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(data)))
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decoding response: %w", err)
		}
	}
	return resp.StatusCode, nil
}

func truncate(s string) string {
	if len(s) > 200 {
		return s[:200] + "…"
	}
	return s
}

func (c *client) login(email, password string) error {
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if _, err := c.postJSON("/auth/login", map[string]string{"email": email, "password": password}, &out); err != nil {
		return err
	}
	if out.AccessToken == "" {
		return fmt.Errorf("no access_token in the login response")
	}
	c.token = out.AccessToken
	return nil
}

func (c *client) deploy() error {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	part, err := w.CreateFormFile("resources", "loadtest.bpmn")
	if err != nil {
		return err
	}
	part.Write([]byte(bpmn))
	w.WriteField("deployment-name", "Load test")
	w.Close()
	resp, err := c.do(http.MethodPost, "/engine-rest/deployment/create", &body, w.FormDataContentType())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(data)))
	}
	return nil
}

// start starts one instance; code is the HTTP status on failure.
func (c *client) start() (id, code string, err error) {
	var out map[string]interface{}
	status, err := c.postJSON("/engine-rest/v2/process-definitions/"+processKey+"/start", map[string]interface{}{"variables": map[string]interface{}{}}, &out)
	if err != nil {
		if status != 0 {
			return "", strconv.Itoa(status), err
		}
		return "", "", err
	}
	for _, k := range []string{"processInstanceId", "id", "processInstanceKey"} {
		if s, ok := out[k].(string); ok && s != "" {
			return s, "", nil
		}
	}
	return "", "no id", fmt.Errorf("no instance id in the start response")
}

// worker fetches and completes the load-test process's jobs until stop is
// closed.
func (c *client) worker(id string, rn *run, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		var jobs []struct {
			ID                string `json:"id"`
			ProcessInstanceID string `json:"processInstanceId"`
		}
		_, err := c.postJSON("/engine-rest/external-task/fetchAndLock", map[string]interface{}{
			"workerId": id,
			"maxTasks": 50,
			"topics":   []map[string]interface{}{{"topicName": jobTopic, "lockDuration": 60000}},
		}, &jobs)
		if err != nil || len(jobs) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		for _, j := range jobs {
			if _, err := c.postJSON("/engine-rest/external-task/"+j.ID+"/complete", map[string]interface{}{
				"workerId": id, "variables": map[string]interface{}{},
			}, nil); err != nil {
				continue
			}
			rn.completed.Add(1)
			if v, ok := rn.started.LoadAndDelete(j.ProcessInstanceID); ok {
				rn.mu.Lock()
				rn.e2e = append(rn.e2e, time.Since(v.(time.Time)))
				rn.mu.Unlock()
			}
		}
	}
}
