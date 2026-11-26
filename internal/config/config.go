package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"
)

const (
	DefaultAppAddr               = "127.0.0.1:8080"
	DefaultDatabaseURL           = "postgres://pulse:pulse@localhost:5432/pulse?sslmode=disable"
	DefaultWorkerCount           = 5
	DefaultQueueCapacity         = 100
	DefaultSchedulerPollInterval = 1 * time.Second
	DefaultMonitorInterval       = 60 * time.Second
	DefaultCheckTimeout          = 5 * time.Second
	DefaultDBMaxConns            = 5

	MinWorkerCount           = 1
	MaxWorkerCount           = 64
	MinQueueCapacity         = 1
	MaxQueueCapacity         = 10000
	MinSchedulerPollInterval = 100 * time.Millisecond
	MaxSchedulerPollInterval = 1 * time.Hour
	MinMonitorInterval       = 30 * time.Second
	MaxMonitorInterval       = 24 * time.Hour
	MinCheckTimeout          = 1 * time.Second
	MaxCheckTimeout          = 30 * time.Second
	MinDBMaxConns            = 1
	MaxDBMaxConns            = 20

	MinAPIKeyLength = 32
)

// Config holds runtime configuration for the Pulse application.
type Config struct {
	AppAddr                string        `json:"app_addr"`
	DatabaseURL            string        `json:"database_url"`
	APIKey                 string        `json:"-"`
	WorkerCount            int           `json:"worker_count"`
	QueueCapacity          int           `json:"queue_capacity"`
	SchedulerPollInterval  time.Duration `json:"scheduler_poll_interval"`
	DefaultMonitorInterval time.Duration `json:"default_monitor_interval"`
	DefaultCheckTimeout    time.Duration `json:"default_check_timeout"`
	DBMaxConns             int           `json:"db_max_conns"`
}

// NewDefault returns a Config initialized with standard specification defaults.
func NewDefault() *Config {
	return &Config{
		AppAddr:                DefaultAppAddr,
		DatabaseURL:            DefaultDatabaseURL,
		WorkerCount:            DefaultWorkerCount,
		QueueCapacity:          DefaultQueueCapacity,
		SchedulerPollInterval:  DefaultSchedulerPollInterval,
		DefaultMonitorInterval: DefaultMonitorInterval,
		DefaultCheckTimeout:    DefaultCheckTimeout,
		DBMaxConns:             DefaultDBMaxConns,
	}
}

// Load loads configuration from environment variables, falling back to standard defaults.
func Load() (*Config, error) {
	return LoadWithLookup(os.LookupEnv)
}

// LoadWithLookup loads configuration using the provided environment lookup function.
func LoadWithLookup(lookup func(string) (string, bool)) (*Config, error) {
	cfg := NewDefault()

	if val, ok := lookup("APP_ADDR"); ok {
		cfg.AppAddr = val
	}
	if val, ok := lookup("DATABASE_URL"); ok {
		cfg.DatabaseURL = val
	}
	if val, ok := lookup("WORKER_COUNT"); ok {
		n, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("invalid WORKER_COUNT: %w", err)
		}
		cfg.WorkerCount = n
	}
	if val, ok := lookup("QUEUE_CAPACITY"); ok {
		n, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("invalid QUEUE_CAPACITY: %w", err)
		}
		cfg.QueueCapacity = n
	}
	if val, ok := lookup("SCHEDULER_POLL_INTERVAL"); ok {
		d, err := time.ParseDuration(val)
		if err != nil {
			return nil, fmt.Errorf("invalid SCHEDULER_POLL_INTERVAL: %w", err)
		}
		cfg.SchedulerPollInterval = d
	}
	if val, ok := lookup("DEFAULT_MONITOR_INTERVAL"); ok {
		d, err := time.ParseDuration(val)
		if err != nil {
			return nil, fmt.Errorf("invalid DEFAULT_MONITOR_INTERVAL: %w", err)
		}
		cfg.DefaultMonitorInterval = d
	}
	if val, ok := lookup("DEFAULT_CHECK_TIMEOUT"); ok {
		d, err := time.ParseDuration(val)
		if err != nil {
			return nil, fmt.Errorf("invalid DEFAULT_CHECK_TIMEOUT: %w", err)
		}
		cfg.DefaultCheckTimeout = d
	}
	if val, ok := lookup("DB_MAX_CONNS"); ok {
		n, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("invalid DB_MAX_CONNS: %w", err)
		}
		cfg.DBMaxConns = n
	}
	if val, ok := lookup("PULSE_API_KEY"); ok {
		cfg.APIKey = val
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return cfg, nil
}

// Validate checks all configuration values against sensible bounds and invariants.
func (c *Config) Validate() error {
	if c.AppAddr == "" {
		return errors.New("APP_ADDR must not be empty")
	}
	host, portStr, err := net.SplitHostPort(c.AppAddr)
	if err != nil {
		return fmt.Errorf("invalid APP_ADDR format (expected host:port): %w", err)
	}
	if host == "" {
		return errors.New("APP_ADDR host must not be empty")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("APP_ADDR port must be between 1 and 65535, got %q", portStr)
	}

	if c.DatabaseURL == "" {
		return errors.New("DATABASE_URL must not be empty")
	}
	u, err := url.Parse(c.DatabaseURL)
	if err != nil {
		return fmt.Errorf("invalid DATABASE_URL: %w", err)
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return fmt.Errorf("DATABASE_URL scheme must be postgres or postgresql, got %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("DATABASE_URL must include a host")
	}

	if c.WorkerCount < MinWorkerCount || c.WorkerCount > MaxWorkerCount {
		return fmt.Errorf("WORKER_COUNT must be between %d and %d, got %d", MinWorkerCount, MaxWorkerCount, c.WorkerCount)
	}

	if c.QueueCapacity < MinQueueCapacity || c.QueueCapacity > MaxQueueCapacity {
		return fmt.Errorf("QUEUE_CAPACITY must be between %d and %d, got %d", MinQueueCapacity, MaxQueueCapacity, c.QueueCapacity)
	}

	if c.SchedulerPollInterval < MinSchedulerPollInterval || c.SchedulerPollInterval > MaxSchedulerPollInterval {
		return fmt.Errorf("SCHEDULER_POLL_INTERVAL must be between %s and %s, got %s", MinSchedulerPollInterval, MaxSchedulerPollInterval, c.SchedulerPollInterval)
	}

	if c.DefaultMonitorInterval < MinMonitorInterval || c.DefaultMonitorInterval > MaxMonitorInterval {
		return fmt.Errorf("DEFAULT_MONITOR_INTERVAL must be between %s and %s, got %s", MinMonitorInterval, MaxMonitorInterval, c.DefaultMonitorInterval)
	}

	if c.DefaultCheckTimeout < MinCheckTimeout || c.DefaultCheckTimeout > MaxCheckTimeout {
		return fmt.Errorf("DEFAULT_CHECK_TIMEOUT must be between %s and %s, got %s", MinCheckTimeout, MaxCheckTimeout, c.DefaultCheckTimeout)
	}

	if c.DefaultCheckTimeout > c.DefaultMonitorInterval {
		return fmt.Errorf("DEFAULT_CHECK_TIMEOUT (%s) must not exceed DEFAULT_MONITOR_INTERVAL (%s)", c.DefaultCheckTimeout, c.DefaultMonitorInterval)
	}

	if c.DBMaxConns < MinDBMaxConns || c.DBMaxConns > MaxDBMaxConns {
		return fmt.Errorf("DB_MAX_CONNS must be between %d and %d, got %d", MinDBMaxConns, MaxDBMaxConns, c.DBMaxConns)
	}

	if c.APIKey == "" {
		return errors.New("PULSE_API_KEY must not be empty")
	}
	if len(c.APIKey) < MinAPIKeyLength {
		return fmt.Errorf("PULSE_API_KEY must be at least %d characters, got %d", MinAPIKeyLength, len(c.APIKey))
	}

	return nil
}

// RedactedDatabaseURL returns the database URL with password and query string redacted for safe logging.
func (c *Config) RedactedDatabaseURL() string {
	u, err := url.Parse(c.DatabaseURL)
	if err != nil {
		return "[unparseable database URL]"
	}
	redacted := u.Redacted()
	parsedRedacted, err := url.Parse(redacted)
	if err != nil {
		return redacted
	}
	parsedRedacted.RawQuery = ""
	return parsedRedacted.String()
}

// LogValue implements slog.LogValuer to ensure sensitive fields are sanitized when logged.
func (c *Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("app_addr", c.AppAddr),
		slog.String("database_url", c.RedactedDatabaseURL()),
		slog.Int("worker_count", c.WorkerCount),
		slog.Int("queue_capacity", c.QueueCapacity),
		slog.Duration("scheduler_poll_interval", c.SchedulerPollInterval),
		slog.Duration("default_monitor_interval", c.DefaultMonitorInterval),
		slog.Duration("default_check_timeout", c.DefaultCheckTimeout),
		slog.Int("db_max_conns", c.DBMaxConns),
	)
}
