package store

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"pulse/internal/model"
)

// RecordCheckParams holds a completed outbound HTTP check observation to persist
// along with the fencing claim token that authorized the check.
// The outbound HTTP request must already have completed before calling RecordCheckResult.
type RecordCheckParams struct {
	MonitorID    uuid.UUID
	ClaimToken   uuid.UUID
	CheckedAt    time.Time
	Success      bool
	HTTPStatus   *int
	LatencyMs    int64
	ErrorMessage *string
}

// RecordCheckOutput returns the persisted CheckResult and updated Monitor
// after the PostgreSQL transaction commits.
type RecordCheckOutput struct {
	CheckResult *model.CheckResult
	Monitor     *model.Monitor
}

type checkTx interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type checkQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// RecordCheckResult persists a completed check observation in check_results and updates
// the monitor's health state and next_check_at in a single short PostgreSQL transaction.
//
// If either SQL operation or commit fails, the transaction is rolled back so neither
// the check_results row nor the monitor state/next_check_at update is persisted,
// leaving the monitor due for a subsequent scheduler pass.
func (s *Store) RecordCheckResult(ctx context.Context, params RecordCheckParams) (*RecordCheckOutput, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("store pool must not be nil")
	}
	if err := validateRecordCheckParams(params); err != nil {
		return nil, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin check persistence transaction: %w", err)
	}

	return s.recordCheckResultTx(ctx, tx, params, nil)
}

func validateRecordCheckParams(params RecordCheckParams) error {
	if params.MonitorID == uuid.Nil {
		return errors.New("monitor_id must not be nil")
	}
	if params.ClaimToken == uuid.Nil {
		return fmt.Errorf("%w: claim_token must not be nil", ErrClaimLost)
	}
	if params.CheckedAt.IsZero() {
		return model.ErrZeroCheckedAt
	}
	if params.LatencyMs < 0 {
		return errors.New("latency_ms must not be negative")
	}
	return nil
}

func sanitizePersistedErrorMessage(msg *string) *string {
	if msg == nil {
		return nil
	}
	s := *msg
	if utf8.RuneCountInString(s) > model.MaxErrorMessageLength {
		runes := []rune(s)
		s = string(runes[:model.MaxErrorMessageLength-3]) + "..."
	}
	return &s
}

func (s *Store) recordCheckResultTx(
	ctx context.Context,
	tx checkTx,
	params RecordCheckParams,
	afterInsert func(context.Context, checkTx) error,
) (*RecordCheckOutput, error) {
	if err := validateRecordCheckParams(params); err != nil {
		return nil, err
	}

	committed := false
	defer func() {
		if !committed {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tx.Rollback(rollbackCtx)
		}
	}()

	const selectMonitorForUpdateSQL = `
		SELECT id, name, url, interval_seconds, timeout_seconds,
		       current_status, consecutive_failures,
		       last_checked_at, last_success_at, next_check_at,
		       created_at, updated_at,
		       claim_token, claim_expires_at, clock_timestamp()
		FROM monitors
		WHERE id = $1
		FOR UPDATE
	`

	var current model.Monitor
	var currentStatus string
	var currentClaimToken *uuid.UUID
	var currentClaimExpiresAt *time.Time
	var dbClock time.Time
	err := tx.QueryRow(ctx, selectMonitorForUpdateSQL, params.MonitorID).Scan(
		&current.ID, &current.Name, &current.URL, &current.IntervalSeconds, &current.TimeoutSeconds,
		&currentStatus, &current.ConsecutiveFailures,
		&current.LastCheckedAt, &current.LastSuccessAt, &current.NextCheckAt,
		&current.CreatedAt, &current.UpdatedAt,
		&currentClaimToken, &currentClaimExpiresAt, &dbClock,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to lock monitor for check persistence: %w", err)
	}
	current.CurrentStatus = model.Status(currentStatus)

	if currentClaimToken == nil {
		return nil, fmt.Errorf("%w: monitor has no active claim", ErrClaimLost)
	}
	if *currentClaimToken != params.ClaimToken {
		return nil, fmt.Errorf("%w: claim token mismatch", ErrClaimLost)
	}
	if currentClaimExpiresAt == nil || !currentClaimExpiresAt.After(dbClock) {
		return nil, fmt.Errorf("%w: claim expired", ErrClaimLost)
	}

	checkedAt := params.CheckedAt.UTC()

	nextState, err := model.TransitionMonitorHealth(
		current.CurrentStatus,
		current.ConsecutiveFailures,
		current.LastSuccessAt,
		params.Success,
		checkedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to compute monitor health transition: %w", err)
	}

	const insertCheckResultSQL = `
		INSERT INTO check_results (
			id, monitor_id, checked_at, success, http_status, latency_ms, error_message
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7
		)
		RETURNING id, monitor_id, checked_at, success, http_status, latency_ms, error_message
	`

	checkID := uuid.New()
	errMsg := sanitizePersistedErrorMessage(params.ErrorMessage)

	var persistedCheck model.CheckResult
	err = tx.QueryRow(ctx, insertCheckResultSQL,
		checkID,
		params.MonitorID,
		checkedAt,
		params.Success,
		params.HTTPStatus,
		params.LatencyMs,
		errMsg,
	).Scan(
		&persistedCheck.ID,
		&persistedCheck.MonitorID,
		&persistedCheck.CheckedAt,
		&persistedCheck.Success,
		&persistedCheck.HTTPStatus,
		&persistedCheck.LatencyMs,
		&persistedCheck.ErrorMessage,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert check_result: %w", err)
	}

	if afterInsert != nil {
		if err := afterInsert(ctx, tx); err != nil {
			return nil, err
		}
	}

	const updateMonitorHealthSQL = `
		WITH db_clock AS MATERIALIZED (
			SELECT clock_timestamp() AS current_time
		)
		UPDATE monitors m
		SET current_status = $2,
		    consecutive_failures = $3,
		    last_checked_at = $4,
		    last_success_at = $5,
		    next_check_at = db_clock.current_time + make_interval(secs => m.interval_seconds),
		    updated_at = db_clock.current_time,
		    claim_token = NULL,
		    claim_expires_at = NULL
		FROM db_clock
		WHERE m.id = $1
		  AND m.claim_token = $6
		  AND m.claim_expires_at > db_clock.current_time
		RETURNING m.id, m.name, m.url, m.interval_seconds, m.timeout_seconds,
		          m.current_status, m.consecutive_failures,
		          m.last_checked_at, m.last_success_at, m.next_check_at,
		          m.created_at, m.updated_at
	`

	var updated model.Monitor
	var updatedStatus string
	err = tx.QueryRow(ctx, updateMonitorHealthSQL,
		params.MonitorID,
		string(nextState.CurrentStatus),
		nextState.ConsecutiveFailures,
		nextState.LastCheckedAt,
		nextState.LastSuccessAt,
		params.ClaimToken,
	).Scan(
		&updated.ID, &updated.Name, &updated.URL, &updated.IntervalSeconds, &updated.TimeoutSeconds,
		&updatedStatus, &updated.ConsecutiveFailures,
		&updated.LastCheckedAt, &updated.LastSuccessAt, &updated.NextCheckAt,
		&updated.CreatedAt, &updated.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: claim token mismatched or expired before update", ErrClaimLost)
		}
		return nil, fmt.Errorf("failed to update monitor health state: %w", err)
	}
	updated.CurrentStatus = model.Status(updatedStatus)

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit check persistence transaction: %w", err)
	}
	committed = true

	return &RecordCheckOutput{
		CheckResult: &persistedCheck,
		Monitor:     &updated,
	}, nil
}

// ListChecks retrieves recent check results for a monitor ordered newest first (checked_at DESC).
// Returns ErrNotFound if the monitor does not exist.
func (s *Store) ListChecks(ctx context.Context, monitorID uuid.UUID, limit int) ([]*model.CheckResult, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("store pool must not be nil")
	}
	return s.listChecksWithQuerier(ctx, s.pool, monitorID, limit)
}

func (s *Store) listChecksWithQuerier(ctx context.Context, q checkQuerier, monitorID uuid.UUID, limit int) ([]*model.CheckResult, error) {
	if limit < model.MinCheckHistoryLimit || limit > model.MaxCheckHistoryLimit {
		return nil, fmt.Errorf("limit must be between %d and %d", model.MinCheckHistoryLimit, model.MaxCheckHistoryLimit)
	}

	const checkMonitorExistsSQL = `SELECT 1 FROM monitors WHERE id = $1`
	var exists int
	if err := q.QueryRow(ctx, checkMonitorExistsSQL, monitorID).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to verify monitor existence: %w", err)
	}

	const listChecksSQL = `
		SELECT id, monitor_id, checked_at, success, http_status, latency_ms, error_message
		FROM check_results
		WHERE monitor_id = $1
		ORDER BY checked_at DESC, id DESC
		LIMIT $2
	`

	rows, err := q.Query(ctx, listChecksSQL, monitorID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query check results: %w", err)
	}
	defer rows.Close()

	checks := make([]*model.CheckResult, 0)
	for rows.Next() {
		var c model.CheckResult
		if err := rows.Scan(
			&c.ID,
			&c.MonitorID,
			&c.CheckedAt,
			&c.Success,
			&c.HTTPStatus,
			&c.LatencyMs,
			&c.ErrorMessage,
		); err != nil {
			return nil, fmt.Errorf("failed to scan check result row: %w", err)
		}
		checks = append(checks, &c)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("check results rows iteration error: %w", err)
	}

	return checks, nil
}

// GetMonitorStats computes uptime availability and average latency for a monitor
// over a sliding window of hours (1 to 720).
// Returns ErrNotFound if the monitor does not exist.
// When total_checks is 0, both Availability and AverageLatencyMs are nil.
// When successful_checks is 0, AverageLatencyMs is nil.
func (s *Store) GetMonitorStats(ctx context.Context, monitorID uuid.UUID, hours int) (*model.MonitorStats, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("store pool must not be nil")
	}
	return s.getMonitorStatsWithQuerier(ctx, s.pool, monitorID, hours)
}

func (s *Store) getMonitorStatsWithQuerier(ctx context.Context, q checkQuerier, monitorID uuid.UUID, hours int) (*model.MonitorStats, error) {
	if hours < model.MinStatsWindowHours || hours > model.MaxStatsWindowHours {
		return nil, fmt.Errorf("hours must be between %d and %d", model.MinStatsWindowHours, model.MaxStatsWindowHours)
	}

	const statsSQL = `
		SELECT
			m.id,
			COUNT(cr.id) AS total_checks,
			COUNT(cr.id) FILTER (WHERE cr.success = true) AS successful_checks,
			COUNT(cr.id) FILTER (WHERE cr.success = false) AS failed_checks,
			(AVG(cr.latency_ms) FILTER (WHERE cr.success = true))::double precision AS average_latency_ms
		FROM monitors m
		LEFT JOIN check_results cr
			ON cr.monitor_id = m.id
			AND cr.checked_at >= NOW() - make_interval(hours => $2::integer)
		WHERE m.id = $1
		GROUP BY m.id
	`

	stats := &model.MonitorStats{
		WindowHours: hours,
	}
	err := q.QueryRow(ctx, statsSQL, monitorID, hours).Scan(
		&stats.MonitorID,
		&stats.TotalChecks,
		&stats.SuccessfulChecks,
		&stats.FailedChecks,
		&stats.AverageLatencyMs,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to query monitor stats: %w", err)
	}

	if stats.TotalChecks > 0 {
		availability := (float64(stats.SuccessfulChecks) / float64(stats.TotalChecks)) * 100.0
		stats.Availability = &availability
	}
	if stats.SuccessfulChecks == 0 {
		stats.AverageLatencyMs = nil
	}

	return stats, nil
}
