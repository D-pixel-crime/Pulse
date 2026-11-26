package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"pulse/internal/store"
)

const (
	// DefaultRetentionInterval defines the periodic interval between retention cleanup passes (approx. once every 24 hours).
	DefaultRetentionInterval = 24 * time.Hour
	// DefaultRetentionPeriod defines the fixed 90-day retention duration (3x max stats window of 30 days).
	DefaultRetentionPeriod = store.DefaultRetentionPeriod
	// DefaultRetentionBatchSize defines the maximum rows deleted per batch query.
	DefaultRetentionBatchSize = store.DefaultRetentionBatchSize
)

// RetentionStore defines the persistence contract required for check-history retention cleanup.
type RetentionStore interface {
	DeleteExpiredCheckResults(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}

// RetentionLoop manages periodic background deletion of expired check results.
type RetentionLoop struct {
	store     RetentionStore
	interval  time.Duration
	period    time.Duration
	batchSize int
	now       func() time.Time
	logger    *slog.Logger
}

// RetentionOption configures optional RetentionLoop properties.
type RetentionOption func(*RetentionLoop)

// WithLoopInterval sets the periodic execution interval (default 24h).
func WithLoopInterval(d time.Duration) RetentionOption {
	return func(r *RetentionLoop) {
		if d > 0 {
			r.interval = d
		}
	}
}

// WithLoopPeriod sets the expiration cutoff age (default 90 days).
func WithLoopPeriod(d time.Duration) RetentionOption {
	return func(r *RetentionLoop) {
		if d > 0 {
			r.period = d
		}
	}
}

// WithLoopBatchSize sets the batch limit for each DELETE pass (default 1000).
func WithLoopBatchSize(size int) RetentionOption {
	return func(r *RetentionLoop) {
		if size > 0 {
			if size > store.MaxRetentionBatchSize {
				size = store.MaxRetentionBatchSize
			}
			r.batchSize = size
		}
	}
}

// WithLoopNowFunc sets a custom clock function for deterministic tests.
func WithLoopNowFunc(now func() time.Time) RetentionOption {
	return func(r *RetentionLoop) {
		if now != nil {
			r.now = now
		}
	}
}

// WithLoopLogger sets a custom logger for the retention loop.
func WithLoopLogger(logger *slog.Logger) RetentionOption {
	return func(r *RetentionLoop) {
		if logger != nil {
			r.logger = logger
		}
	}
}

// NewRetentionLoop creates a RetentionLoop configured to purge check history older than 90 days.
func NewRetentionLoop(retStore RetentionStore, logger *slog.Logger, opts ...RetentionOption) (*RetentionLoop, error) {
	if retStore == nil {
		return nil, errors.New("retention store must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	r := &RetentionLoop{
		store:     retStore,
		interval:  DefaultRetentionInterval,
		period:    DefaultRetentionPeriod,
		batchSize: DefaultRetentionBatchSize,
		now:       time.Now,
		logger:    logger,
	}

	for _, opt := range opts {
		opt(r)
	}

	return r, nil
}

// Interval returns the configured retention pass interval.
func (r *RetentionLoop) Interval() time.Duration {
	return r.interval
}

// Period returns the configured retention cutoff period.
func (r *RetentionLoop) Period() time.Duration {
	return r.period
}

// BatchSize returns the configured retention batch size.
func (r *RetentionLoop) BatchSize() int {
	return r.batchSize
}

// Run executes the retention cleanup once immediately at startup, then periodically on
// every tick (approx. every 24 hours), until ctx is canceled.
func (r *RetentionLoop) Run(ctx context.Context) error {
	r.logger.Info("retention loop started",
		slog.Duration("interval", r.interval),
		slog.Duration("retention_period", r.period),
		slog.Int("batch_size", r.batchSize),
	)

	// Run cleanup once at startup immediately
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if _, err := r.RunPass(ctx); err != nil && ctx.Err() == nil {
		r.logger.Warn("startup retention cleanup pass encountered error", slog.String("error", err.Error()))
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("retention loop stopped")
			return nil
		case <-ticker.C:
			if _, err := r.RunPass(ctx); err != nil && ctx.Err() == nil {
				r.logger.Warn("scheduled retention cleanup pass encountered error", slog.String("error", err.Error()))
			}
		}
	}
}

// RunPass executes a single retention pass, deleting expired check results in bounded batches
// of at most batchSize until fewer than batchSize rows are returned.
// It stops immediately if ctx is canceled and returns the total number of deleted rows.
func (r *RetentionLoop) RunPass(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	cutoff := r.now().UTC().Add(-r.period)
	var totalDeleted int64

	for {
		if err := ctx.Err(); err != nil {
			return totalDeleted, err
		}

		deleted, err := r.store.DeleteExpiredCheckResults(ctx, cutoff, r.batchSize)
		if err != nil {
			return totalDeleted, err
		}

		totalDeleted += deleted
		if deleted < int64(r.batchSize) {
			break
		}
	}

	if totalDeleted > 0 {
		r.logger.Info("retention cleanup pass completed",
			slog.Int64("deleted_rows", totalDeleted),
			slog.Time("cutoff", cutoff),
		)
	}

	return totalDeleted, nil
}
