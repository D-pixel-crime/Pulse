package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"pulse/internal/model"
)

const (
	// DefaultDueBatchLimit is the default maximum number of due monitors returned per discovery query.
	DefaultDueBatchLimit = 100
	// MaxDueBatchLimit is the hard upper cap on due monitors returned per discovery query.
	MaxDueBatchLimit = 100
)

type rowScanner interface {
	Scan(dest ...any) error
}

func scanMonitor(row rowScanner) (*model.Monitor, error) {
	var m model.Monitor
	var currentStatus string

	if err := row.Scan(
		&m.ID, &m.Name, &m.URL, &m.IntervalSeconds, &m.TimeoutSeconds,
		&currentStatus, &m.ConsecutiveFailures,
		&m.LastCheckedAt, &m.LastSuccessAt, &m.NextCheckAt,
		&m.CreatedAt, &m.UpdatedAt,
	); err != nil {
		return nil, err
	}
	m.CurrentStatus = model.Status(currentStatus)
	return &m, nil
}

// Create inserts a new monitor into PostgreSQL using parameterized SQL.
// It sets creation invariants: current_status is UNKNOWN, consecutive_failures is 0,
// last_checked_at and last_success_at are null, and next_check_at equals created_at (due immediately).
func (s *Store) Create(ctx context.Context, m *model.Monitor) (*model.Monitor, error) {
	if m == nil {
		return nil, errors.New("monitor cannot be nil")
	}

	now := time.Now().UTC()
	m.InitializeCreationDefaults(now)

	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("monitor validation failed: %w", err)
	}

	query := `
		INSERT INTO monitors (
			id, name, url, interval_seconds, timeout_seconds,
			current_status, consecutive_failures,
			last_checked_at, last_success_at, next_check_at,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7,
			$8, $9, $10,
			$11, $12
		) RETURNING id, name, url, interval_seconds, timeout_seconds,
		            current_status, consecutive_failures,
		            last_checked_at, last_success_at, next_check_at,
		            created_at, updated_at
	`

	created, err := scanMonitor(s.pool.QueryRow(ctx, query,
		m.ID, m.Name, m.URL, m.IntervalSeconds, m.TimeoutSeconds,
		string(m.CurrentStatus), m.ConsecutiveFailures,
		m.LastCheckedAt, m.LastSuccessAt, m.NextCheckAt,
		m.CreatedAt, m.UpdatedAt,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to create monitor: %w", err)
	}

	return created, nil
}

// CreateMonitor is an alias for Create.
func (s *Store) CreateMonitor(ctx context.Context, m *model.Monitor) (*model.Monitor, error) {
	return s.Create(ctx, m)
}

// GetByID retrieves a single monitor by UUID using parameterized SQL.
// Maps pgx.ErrNoRows to ErrNotFound at the store boundary.
func (s *Store) GetByID(ctx context.Context, id uuid.UUID) (*model.Monitor, error) {
	query := `
		SELECT id, name, url, interval_seconds, timeout_seconds,
		       current_status, consecutive_failures,
		       last_checked_at, last_success_at, next_check_at,
		       created_at, updated_at
		FROM monitors
		WHERE id = $1
	`

	m, err := scanMonitor(s.pool.QueryRow(ctx, query, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to get monitor: %w", err)
	}

	return m, nil
}

// GetMonitorByID is an alias for GetByID.
func (s *Store) GetMonitorByID(ctx context.Context, id uuid.UUID) (*model.Monitor, error) {
	return s.GetByID(ctx, id)
}

// List retrieves all monitors ordered by created_at DESC.
func (s *Store) List(ctx context.Context) ([]*model.Monitor, error) {
	query := `
		SELECT id, name, url, interval_seconds, timeout_seconds,
		       current_status, consecutive_failures,
		       last_checked_at, last_success_at, next_check_at,
		       created_at, updated_at
		FROM monitors
		ORDER BY created_at DESC
	`

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query monitors: %w", err)
	}
	defer rows.Close()

	monitors := make([]*model.Monitor, 0)
	for rows.Next() {
		m, err := scanMonitor(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan monitor row: %w", err)
		}
		monitors = append(monitors, m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return monitors, nil
}

// ListMonitors is an alias for List.
func (s *Store) ListMonitors(ctx context.Context) ([]*model.Monitor, error) {
	return s.List(ctx)
}

// ListDueMonitors retrieves a bounded batch of monitors with next_check_at <= now,
// ordered by next_check_at ascending with id ascending as a deterministic tie-breaker.
// When limit is 0, DefaultDueBatchLimit (100) is used; values above MaxDueBatchLimit (100)
// are capped at MaxDueBatchLimit. It never modifies next_check_at during discovery.
func (s *Store) ListDueMonitors(ctx context.Context, now time.Time, limit int) ([]*model.Monitor, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("store pool must not be nil")
	}
	return s.listDueMonitorsWithQuerier(ctx, s.pool, now, limit)
}

func (s *Store) listDueMonitorsWithQuerier(ctx context.Context, q checkQuerier, now time.Time, limit int) ([]*model.Monitor, error) {
	if now.IsZero() {
		return nil, errors.New("current time must not be zero")
	}
	if limit < 0 {
		return nil, fmt.Errorf("limit must not be negative, got %d", limit)
	}
	if limit == 0 {
		limit = DefaultDueBatchLimit
	} else if limit > MaxDueBatchLimit {
		limit = MaxDueBatchLimit
	}

	const listDueMonitorsSQL = `
		SELECT id, name, url, interval_seconds, timeout_seconds,
		       current_status, consecutive_failures,
		       last_checked_at, last_success_at, next_check_at,
		       created_at, updated_at
		FROM monitors
		WHERE next_check_at <= $1
		ORDER BY next_check_at ASC, id ASC
		LIMIT $2
	`

	rows, err := q.Query(ctx, listDueMonitorsSQL, now.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query due monitors: %w", err)
	}
	defer rows.Close()

	monitors := make([]*model.Monitor, 0)
	for rows.Next() {
		m, err := scanMonitor(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan due monitor row: %w", err)
		}
		monitors = append(monitors, m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("due monitor rows iteration error: %w", err)
	}

	return monitors, nil
}

// Delete removes a monitor by UUID.
// If the monitor was not found, it returns ErrNotFound at the store boundary.
// Associated check_results are automatically removed by foreign key ON DELETE CASCADE.
func (s *Store) Delete(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM monitors WHERE id = $1`

	tag, err := s.pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete monitor: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

// DeleteMonitor is an alias for Delete.
func (s *Store) DeleteMonitor(ctx context.Context, id uuid.UUID) error {
	return s.Delete(ctx, id)
}
