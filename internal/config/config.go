package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultAppAddr               = "127.0.0.1:8080"
	DefaultDatabaseURL           = ""
	DefaultWorkerCount           = 5
	DefaultSchedulerPollInterval = 1 * time.Second
	DefaultDBMaxConns            = 5

	MinWorkerCount           = 1
	MaxWorkerCount           = 64
	MinSchedulerPollInterval = 100 * time.Millisecond
	MaxSchedulerPollInterval = 1 * time.Hour
	MinDBMaxConns            = 1
	MaxDBMaxConns            = 20

	MinAPIKeyLength = 32
)

// Config holds runtime configuration for the Pulse application.
type Config struct {
	AppAddr               string        `json:"app_addr"`
	DatabaseURL           string        `json:"database_url"`
	APIKey                string        `json:"-"`
	WorkerCount           int           `json:"worker_count"`
	SchedulerPollInterval time.Duration `json:"scheduler_poll_interval"`
	DBMaxConns            int           `json:"db_max_conns"`
}

// NewDefault returns a Config initialized with standard specification defaults.
func NewDefault() *Config {
	return &Config{
		AppAddr:               DefaultAppAddr,
		DatabaseURL:           DefaultDatabaseURL,
		WorkerCount:           DefaultWorkerCount,
		SchedulerPollInterval: DefaultSchedulerPollInterval,
		DBMaxConns:            DefaultDBMaxConns,
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
		cfg.AppAddr = strings.TrimSpace(val)
	}
	if val, ok := lookup("DATABASE_URL"); ok {
		cfg.DatabaseURL = strings.TrimSpace(val)
	}
	if val, ok := lookup("WORKER_COUNT"); ok {
		n, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil {
			return nil, fmt.Errorf("invalid WORKER_COUNT: %w", err)
		}
		cfg.WorkerCount = n
	}
	if val, ok := lookup("SCHEDULER_POLL_INTERVAL"); ok {
		d, err := time.ParseDuration(strings.TrimSpace(val))
		if err != nil {
			return nil, fmt.Errorf("invalid SCHEDULER_POLL_INTERVAL: %w", err)
		}
		cfg.SchedulerPollInterval = d
	}
	if val, ok := lookup("DB_MAX_CONNS"); ok {
		n, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil {
			return nil, fmt.Errorf("invalid DB_MAX_CONNS: %w", err)
		}
		cfg.DBMaxConns = n
	}
	if val, ok := lookup("PULSE_API_KEY"); ok {
		cfg.APIKey = strings.TrimSpace(val)
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

	if c.SchedulerPollInterval < MinSchedulerPollInterval || c.SchedulerPollInterval > MaxSchedulerPollInterval {
		return fmt.Errorf("SCHEDULER_POLL_INTERVAL must be between %s and %s, got %s", MinSchedulerPollInterval, MaxSchedulerPollInterval, c.SchedulerPollInterval)
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
		slog.Duration("scheduler_poll_interval", c.SchedulerPollInterval),
		slog.Int("db_max_conns", c.DBMaxConns),
	)
}
