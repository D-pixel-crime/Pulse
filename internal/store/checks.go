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

// RecordCheckParams holds a completed outbound HTTP check observation to persist.
// The outbound HTTP request must already have completed before calling RecordCheckResult.
type RecordCheckParams struct {
	MonitorID    uuid.UUID
	CheckedAt    time.Time
	CompletedAt  time.Time
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
		       created_at, updated_at
		FROM monitors
		WHERE id = $1
		FOR UPDATE
	`

	var current model.Monitor
	var currentStatus string
	err := tx.QueryRow(ctx, selectMonitorForUpdateSQL, params.MonitorID).Scan(
		&current.ID, &current.Name, &current.URL, &current.IntervalSeconds, &current.TimeoutSeconds,
		&currentStatus, &current.ConsecutiveFailures,
		&current.LastCheckedAt, &current.LastSuccessAt, &current.NextCheckAt,
		&current.CreatedAt, &current.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to lock monitor for check persistence: %w", err)
	}
	current.CurrentStatus = model.Status(currentStatus)

	checkedAt := params.CheckedAt.UTC()
	completedAt := params.CompletedAt.UTC()
	if params.CompletedAt.IsZero() {
		completedAt = checkedAt
	}

	nextState, err := model.TransitionMonitorHealth(
		current.CurrentStatus,
		current.ConsecutiveFailures,
		current.LastSuccessAt,
		current.IntervalSeconds,
		params.Success,
		checkedAt,
		completedAt,
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
		UPDATE monitors
		SET current_status = $2,
		    consecutive_failures = $3,
		    last_checked_at = $4,
		    last_success_at = $5,
		    next_check_at = $6,
		    updated_at = $7
		WHERE id = $1
		RETURNING id, name, url, interval_seconds, timeout_seconds,
		          current_status, consecutive_failures,
		          last_checked_at, last_success_at, next_check_at,
		          created_at, updated_at
	`

	var updated model.Monitor
	var updatedStatus string
	err = tx.QueryRow(ctx, updateMonitorHealthSQL,
		params.MonitorID,
		string(nextState.CurrentStatus),
		nextState.ConsecutiveFailures,
		nextState.LastCheckedAt,
		nextState.LastSuccessAt,
		nextState.NextCheckAt,
		nextState.UpdatedAt,
	).Scan(
		&updated.ID, &updated.Name, &updated.URL, &updated.IntervalSeconds, &updated.TimeoutSeconds,
		&updatedStatus, &updated.ConsecutiveFailures,
		&updated.LastCheckedAt, &updated.LastSuccessAt, &updated.NextCheckAt,
		&updated.CreatedAt, &updated.UpdatedAt,
	)
	if err != nil {
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
