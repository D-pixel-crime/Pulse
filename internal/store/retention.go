package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// DefaultRetentionPeriod defines the fixed 90-day retention duration (3x max stats window of 30 days).
	DefaultRetentionPeriod = 90 * 24 * time.Hour
	// DefaultRetentionBatchSize defines the maximum rows deleted per retention batch query.
	DefaultRetentionBatchSize = 1000
	// MaxRetentionBatchSize caps the retention batch size to prevent unbounded deletes.
	MaxRetentionBatchSize = 1000
)

type retentionExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// DeleteExpiredCheckResults deletes at most limit (capped at MaxRetentionBatchSize = 1000)
// check_results older than the cutoff timestamp using a CTE with row-level locks
// and FOR UPDATE SKIP LOCKED to safely allow concurrent cleanup passes across multiple instances.
// It returns the number of rows deleted.
func (s *Store) DeleteExpiredCheckResults(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, errors.New("store pool must not be nil")
	}
	return s.deleteExpiredCheckResultsWithExecer(ctx, s.pool, cutoff, limit)
}

func (s *Store) deleteExpiredCheckResultsWithExecer(ctx context.Context, ex retentionExecer, cutoff time.Time, limit int) (int64, error) {
	if cutoff.IsZero() {
		cutoff = time.Now().Add(-DefaultRetentionPeriod)
	}
	cutoff = cutoff.UTC()

	if limit <= 0 {
		limit = DefaultRetentionBatchSize
	}
	if limit > MaxRetentionBatchSize {
		limit = MaxRetentionBatchSize
	}

	const deleteExpiredSQL = `
		WITH expired AS (
			SELECT id
			FROM check_results
			WHERE checked_at < $1
			ORDER BY checked_at ASC, id ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM check_results cr
		USING expired
		WHERE cr.id = expired.id
	`

	tag, err := ex.Exec(ctx, deleteExpiredSQL, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("failed to delete expired check results: %w", err)
	}

	return tag.RowsAffected(), nil
}
