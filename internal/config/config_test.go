package config_test

import (
	"strings"
	"testing"
	"time"

	"pulse/internal/config"
)

func TestConfig_Defaults(t *testing.T) {
	// Empty lookup function simulates no environment variables set
	emptyLookup := func(key string) (string, bool) {
		return "", false
	}

	cfg, err := config.LoadWithLookup(emptyLookup)
	if err != nil {
		t.Fatalf("expected successful default load, got error: %v", err)
	}

	if cfg.AppAddr != config.DefaultAppAddr {
		t.Errorf("AppAddr = %q, expected %q", cfg.AppAddr, config.DefaultAppAddr)
	}
	if cfg.DatabaseURL != config.DefaultDatabaseURL {
		t.Errorf("DatabaseURL = %q, expected %q", cfg.DatabaseURL, config.DefaultDatabaseURL)
	}
	if cfg.WorkerCount != config.DefaultWorkerCount {
		t.Errorf("WorkerCount = %d, expected %d", cfg.WorkerCount, config.DefaultWorkerCount)
	}
	if cfg.QueueCapacity != config.DefaultQueueCapacity {
		t.Errorf("QueueCapacity = %d, expected %d", cfg.QueueCapacity, config.DefaultQueueCapacity)
	}
	if cfg.SchedulerPollInterval != config.DefaultSchedulerPollInterval {
		t.Errorf("SchedulerPollInterval = %s, expected %s", cfg.SchedulerPollInterval, config.DefaultSchedulerPollInterval)
	}
	if cfg.DefaultMonitorInterval != config.DefaultMonitorInterval {
		t.Errorf("DefaultMonitorInterval = %s, expected %s", cfg.DefaultMonitorInterval, config.DefaultMonitorInterval)
	}
	if cfg.DefaultCheckTimeout != config.DefaultCheckTimeout {
		t.Errorf("DefaultCheckTimeout = %s, expected %s", cfg.DefaultCheckTimeout, config.DefaultCheckTimeout)
	}
}

func TestConfig_ValidOverrides(t *testing.T) {
	env := map[string]string{
		"APP_ADDR":                 "127.0.0.1:9090",
		"DATABASE_URL":             "postgresql://custom_user:secret_pw@localhost:5433/custom_db?sslmode=require",
		"WORKER_COUNT":             "10",
		"QUEUE_CAPACITY":           "250",
		"SCHEDULER_POLL_INTERVAL":  "500ms",
		"DEFAULT_MONITOR_INTERVAL": "120s",
		"DEFAULT_CHECK_TIMEOUT":    "10s",
	}

	lookup := func(key string) (string, bool) {
		val, ok := env[key]
		return val, ok
	}

	cfg, err := config.LoadWithLookup(lookup)
	if err != nil {
		t.Fatalf("expected successful load with overrides, got error: %v", err)
	}

	if cfg.AppAddr != "127.0.0.1:9090" {
		t.Errorf("AppAddr = %q, expected 127.0.0.1:9090", cfg.AppAddr)
	}
	if cfg.DatabaseURL != "postgresql://custom_user:secret_pw@localhost:5433/custom_db?sslmode=require" {
		t.Errorf("DatabaseURL = %q, expected overridden URL", cfg.DatabaseURL)
	}
	if cfg.WorkerCount != 10 {
		t.Errorf("WorkerCount = %d, expected 10", cfg.WorkerCount)
	}
	if cfg.QueueCapacity != 250 {
		t.Errorf("QueueCapacity = %d, expected 250", cfg.QueueCapacity)
	}
	if cfg.SchedulerPollInterval != 500*time.Millisecond {
		t.Errorf("SchedulerPollInterval = %s, expected 500ms", cfg.SchedulerPollInterval)
	}
	if cfg.DefaultMonitorInterval != 120*time.Second {
		t.Errorf("DefaultMonitorInterval = %s, expected 120s", cfg.DefaultMonitorInterval)
	}
	if cfg.DefaultCheckTimeout != 10*time.Second {
		t.Errorf("DefaultCheckTimeout = %s, expected 10s", cfg.DefaultCheckTimeout)
	}
}

func TestConfig_InvalidValuesAndBounds(t *testing.T) {
	tests := []struct {
		name          string
		env           map[string]string
		wantErrSubstr string
	}{
		// APP_ADDR tests
		{
			name:          "empty APP_ADDR",
			env:           map[string]string{"APP_ADDR": ""},
			wantErrSubstr: "APP_ADDR",
		},
		{
			name:          "missing port in APP_ADDR",
			env:           map[string]string{"APP_ADDR": "127.0.0.1"},
			wantErrSubstr: "expected host:port",
		},
		{
			name:          "non-numeric port in APP_ADDR",
			env:           map[string]string{"APP_ADDR": "127.0.0.1:abc"},
			wantErrSubstr: "port must be between 1 and 65535",
		},
		{
			name:          "port 0 in APP_ADDR",
			env:           map[string]string{"APP_ADDR": "127.0.0.1:0"},
			wantErrSubstr: "port must be between 1 and 65535",
		},
		{
			name:          "port out of range in APP_ADDR",
			env:           map[string]string{"APP_ADDR": "127.0.0.1:70000"},
			wantErrSubstr: "port must be between 1 and 65535",
		},
		{
			name:          "missing host in APP_ADDR",
			env:           map[string]string{"APP_ADDR": ":8080"},
			wantErrSubstr: "host must not be empty",
		},

		// DATABASE_URL tests
		{
			name:          "empty DATABASE_URL",
			env:           map[string]string{"DATABASE_URL": ""},
			wantErrSubstr: "DATABASE_URL must not be empty",
		},
		{
			name:          "unsupported scheme DATABASE_URL",
			env:           map[string]string{"DATABASE_URL": "mysql://pulse:pulse@localhost:3306/pulse"},
			wantErrSubstr: "scheme must be postgres or postgresql",
		},
		{
			name:          "missing host DATABASE_URL",
			env:           map[string]string{"DATABASE_URL": "postgres:///pulse"},
			wantErrSubstr: "must include a host",
		},
		{
			name:          "invalid URL syntax in DATABASE_URL",
			env:           map[string]string{"DATABASE_URL": "://invalid-url"},
			wantErrSubstr: "invalid DATABASE_URL",
		},

		// WORKER_COUNT tests
		{
			name:          "non-integer WORKER_COUNT",
			env:           map[string]string{"WORKER_COUNT": "abc"},
			wantErrSubstr: "invalid WORKER_COUNT",
		},
		{
			name:          "zero WORKER_COUNT",
			env:           map[string]string{"WORKER_COUNT": "0"},
			wantErrSubstr: "WORKER_COUNT must be between",
		},
		{
			name:          "negative WORKER_COUNT",
			env:           map[string]string{"WORKER_COUNT": "-5"},
			wantErrSubstr: "WORKER_COUNT must be between",
		},
		{
			name:          "WORKER_COUNT exceeds upper bound (65)",
			env:           map[string]string{"WORKER_COUNT": "65"},
			wantErrSubstr: "WORKER_COUNT must be between",
		},

		// QUEUE_CAPACITY tests
		{
			name:          "non-integer QUEUE_CAPACITY",
			env:           map[string]string{"QUEUE_CAPACITY": "invalid"},
			wantErrSubstr: "invalid QUEUE_CAPACITY",
		},
		{
			name:          "zero QUEUE_CAPACITY",
			env:           map[string]string{"QUEUE_CAPACITY": "0"},
			wantErrSubstr: "QUEUE_CAPACITY must be between",
		},
		{
			name:          "negative QUEUE_CAPACITY",
			env:           map[string]string{"QUEUE_CAPACITY": "-10"},
			wantErrSubstr: "QUEUE_CAPACITY must be between",
		},
		{
			name:          "QUEUE_CAPACITY exceeds upper bound",
			env:           map[string]string{"QUEUE_CAPACITY": "10001"},
			wantErrSubstr: "QUEUE_CAPACITY must be between",
		},

		// SCHEDULER_POLL_INTERVAL tests
		{
			name:          "non-duration SCHEDULER_POLL_INTERVAL",
			env:           map[string]string{"SCHEDULER_POLL_INTERVAL": "fast"},
			wantErrSubstr: "invalid SCHEDULER_POLL_INTERVAL",
		},
		{
			name:          "SCHEDULER_POLL_INTERVAL below minimum (100ms)",
			env:           map[string]string{"SCHEDULER_POLL_INTERVAL": "50ms"},
			wantErrSubstr: "SCHEDULER_POLL_INTERVAL must be between",
		},
		{
			name:          "SCHEDULER_POLL_INTERVAL above maximum (1h)",
			env:           map[string]string{"SCHEDULER_POLL_INTERVAL": "2h"},
			wantErrSubstr: "SCHEDULER_POLL_INTERVAL must be between",
		},

		// DEFAULT_MONITOR_INTERVAL tests
		{
			name:          "non-duration DEFAULT_MONITOR_INTERVAL",
			env:           map[string]string{"DEFAULT_MONITOR_INTERVAL": "hourly"},
			wantErrSubstr: "invalid DEFAULT_MONITOR_INTERVAL",
		},
		{
			name:          "DEFAULT_MONITOR_INTERVAL below minimum (30s)",
			env:           map[string]string{"DEFAULT_MONITOR_INTERVAL": "29s"},
			wantErrSubstr: "DEFAULT_MONITOR_INTERVAL must be between",
		},
		{
			name:          "DEFAULT_MONITOR_INTERVAL above maximum (24h)",
			env:           map[string]string{"DEFAULT_MONITOR_INTERVAL": "25h"},
			wantErrSubstr: "DEFAULT_MONITOR_INTERVAL must be between",
		},

		// DEFAULT_CHECK_TIMEOUT tests
		{
			name:          "non-duration DEFAULT_CHECK_TIMEOUT",
			env:           map[string]string{"DEFAULT_CHECK_TIMEOUT": "quick"},
			wantErrSubstr: "invalid DEFAULT_CHECK_TIMEOUT",
		},
		{
			name:          "DEFAULT_CHECK_TIMEOUT below minimum (1s)",
			env:           map[string]string{"DEFAULT_CHECK_TIMEOUT": "500ms"},
			wantErrSubstr: "DEFAULT_CHECK_TIMEOUT must be between",
		},
		{
			name:          "DEFAULT_CHECK_TIMEOUT above maximum (30s)",
			env:           map[string]string{"DEFAULT_CHECK_TIMEOUT": "31s"},
			wantErrSubstr: "DEFAULT_CHECK_TIMEOUT must be between",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lookup := func(key string) (string, bool) {
				val, ok := tc.env[key]
				return val, ok
			}

			_, err := config.LoadWithLookup(lookup)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErrSubstr)
			}
			if !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Errorf("error %q does not contain expected substring %q", err.Error(), tc.wantErrSubstr)
			}
		})
	}
}

func TestConfig_RedactedDatabaseURL(t *testing.T) {
	cfg := &config.Config{
		DatabaseURL: "postgres://myuser:secretpassword@dbhost:5432/mydb?sslmode=disable&foo=bar",
	}

	redacted := cfg.RedactedDatabaseURL()
	if strings.Contains(redacted, "secretpassword") {
		t.Errorf("redacted URL contains secret password: %s", redacted)
	}
	if strings.Contains(redacted, "foo=bar") {
		t.Errorf("redacted URL contains query string: %s", redacted)
	}
	if !strings.Contains(redacted, "myuser") {
		t.Errorf("redacted URL should preserve user: %s", redacted)
	}
	if !strings.Contains(redacted, "dbhost:5432") {
		t.Errorf("redacted URL should preserve host and port: %s", redacted)
	}
}

func TestConfig_WorkerCountBounds(t *testing.T) {
	t.Run("default is 5", func(t *testing.T) {
		cfg, err := config.LoadWithLookup(func(string) (string, bool) { return "", false })
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if cfg.WorkerCount != 5 {
			t.Errorf("expected default WorkerCount = 5, got %d", cfg.WorkerCount)
		}
		if config.DefaultWorkerCount != 5 {
			t.Errorf("expected DefaultWorkerCount = 5, got %d", config.DefaultWorkerCount)
		}
	})

	t.Run("worker count 64 is accepted", func(t *testing.T) {
		cfg, err := config.LoadWithLookup(func(k string) (string, bool) {
			if k == "WORKER_COUNT" {
				return "64", true
			}
			return "", false
		})
		if err != nil {
			t.Fatalf("expected WORKER_COUNT=64 to be accepted, got error: %v", err)
		}
		if cfg.WorkerCount != 64 {
			t.Errorf("expected WorkerCount = 64, got %d", cfg.WorkerCount)
		}
	})

	t.Run("worker count 65 is rejected", func(t *testing.T) {
		_, err := config.LoadWithLookup(func(k string) (string, bool) {
			if k == "WORKER_COUNT" {
				return "65", true
			}
			return "", false
		})
		if err == nil {
			t.Fatal("expected WORKER_COUNT=65 to be rejected, got nil error")
		}
		if !strings.Contains(err.Error(), "WORKER_COUNT must be between") {
			t.Errorf("expected error containing 'WORKER_COUNT must be between', got: %v", err)
		}
	})
}
