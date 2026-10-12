package retention

import (
	"testing"
	"time"
)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestConfigFromEnv_DefaultsDisabled(t *testing.T) {
	cfg, err := ConfigFromEnv(envOf(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Enabled() || cfg.Interval != time.Hour || cfg.BatchSize != 500 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestConfigFromEnv_Valid(t *testing.T) {
	cfg, err := ConfigFromEnv(envOf(map[string]string{
		"RETENTION_DAYS": "90", "RETENTION_INTERVAL": "30m", "RETENTION_BATCH_SIZE": "200",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Enabled() || cfg.Days != 90 || cfg.Interval != 30*time.Minute || cfg.BatchSize != 200 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestConfigFromEnv_ZeroDaysDisables(t *testing.T) {
	cfg, err := ConfigFromEnv(envOf(map[string]string{"RETENTION_DAYS": "0"}))
	if err != nil || cfg.Enabled() {
		t.Fatalf("RETENTION_DAYS=0 should disable without error: %+v, %v", cfg, err)
	}
}

func TestConfigFromEnv_RejectsInvalid(t *testing.T) {
	for _, env := range []map[string]string{
		{"RETENTION_DAYS": "-1"},
		{"RETENTION_DAYS": "90d"},
		{"RETENTION_DAYS": "abc"},
		{"RETENTION_INTERVAL": "10s"},
		{"RETENTION_INTERVAL": "hourly"},
		{"RETENTION_BATCH_SIZE": "0"},
		{"RETENTION_BATCH_SIZE": "100000"},
	} {
		if _, err := ConfigFromEnv(envOf(env)); err == nil {
			t.Errorf("expected an error for %v", env)
		}
	}
}
