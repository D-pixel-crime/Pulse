package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DefaultMinConns defines the minimum idle connections kept open in the pool.
	// Set to 0 so idle replicas do not reserve unnecessary connections.
	DefaultMinConns = 0
	// DefaultMaxConns defines the maximum concurrent connections allocated for the pool.
	DefaultMaxConns = 5
	// DefaultMaxConnLifetime defines the maximum time a connection may be reused.
	DefaultMaxConnLifetime = 1 * time.Hour
	// DefaultMaxConnIdleTime defines the maximum time an idle connection can remain open.
	DefaultMaxConnIdleTime = 30 * time.Minute
	// DefaultHealthCheckPeriod defines the background health check frequency for pool connections.
	DefaultHealthCheckPeriod = 1 * time.Minute
)

// DB wraps a pgxpool.Pool to provide lifecycle management and connectivity checks.
type DB struct {
	pool *pgxpool.Pool
}

// Option provides functional configuration for pgxpool.Config.
type Option func(*pgxpool.Config)

// WithMaxConns configures the maximum concurrent connections allocated for the pool.
func WithMaxConns(maxConns int32) Option {
	return func(cfg *pgxpool.Config) {
		if maxConns > 0 {
			cfg.MaxConns = maxConns
		}
	}
}

// NewPoolConfig parses the given database URL and applies sensible connection pool defaults.
func NewPoolConfig(databaseURL string, opts ...Option) (*pgxpool.Config, error) {
	if databaseURL == "" {
		return nil, fmt.Errorf("database URL must not be empty")
	}

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database configuration: %w", err)
	}

	cfg.MinConns = DefaultMinConns
	cfg.MaxConns = DefaultMaxConns
	cfg.MaxConnLifetime = DefaultMaxConnLifetime
	cfg.MaxConnIdleTime = DefaultMaxConnIdleTime
	cfg.HealthCheckPeriod = DefaultHealthCheckPeriod

	for _, opt := range opts {
		opt(cfg)
	}

	return cfg, nil
}

// Connect parses configuration, initializes a pgxpool.Pool with sensible limits,
// pings the database to verify connectivity, and returns a managed DB instance.
func Connect(ctx context.Context, databaseURL string, opts ...Option) (*DB, error) {
	cfg, err := NewPoolConfig(databaseURL, opts...)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database at startup: %w", err)
	}

	return &DB{pool: pool}, nil
}

// Pool returns the underlying *pgxpool.Pool.
func (d *DB) Pool() *pgxpool.Pool {
	if d == nil {
		return nil
	}
	return d.pool
}

// Ping verifies connectivity to the PostgreSQL database.
func (d *DB) Ping(ctx context.Context) error {
	if d == nil || d.pool == nil {
		return fmt.Errorf("database connection pool is nil")
	}
	return d.pool.Ping(ctx)
}

// Close cleanly terminates all database pool connections.
func (d *DB) Close() {
	if d != nil && d.pool != nil {
		d.pool.Close()
	}
}
