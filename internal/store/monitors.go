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

	var created model.Monitor
	var currentStatus string

	err := s.pool.QueryRow(ctx, query,
		m.ID, m.Name, m.URL, m.IntervalSeconds, m.TimeoutSeconds,
		string(m.CurrentStatus), m.ConsecutiveFailures,
		m.LastCheckedAt, m.LastSuccessAt, m.NextCheckAt,
		m.CreatedAt, m.UpdatedAt,
	).Scan(
		&created.ID, &created.Name, &created.URL, &created.IntervalSeconds, &created.TimeoutSeconds,
		&currentStatus, &created.ConsecutiveFailures,
		&created.LastCheckedAt, &created.LastSuccessAt, &created.NextCheckAt,
		&created.CreatedAt, &created.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create monitor: %w", err)
	}
	created.CurrentStatus = model.Status(currentStatus)

	return &created, nil
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

	var m model.Monitor
	var currentStatus string

	err := s.pool.QueryRow(ctx, query, id).Scan(
		&m.ID, &m.Name, &m.URL, &m.IntervalSeconds, &m.TimeoutSeconds,
		&currentStatus, &m.ConsecutiveFailures,
		&m.LastCheckedAt, &m.LastSuccessAt, &m.NextCheckAt,
		&m.CreatedAt, &m.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to get monitor: %w", err)
	}
	m.CurrentStatus = model.Status(currentStatus)

	return &m, nil
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
		var m model.Monitor
		var currentStatus string

		if err := rows.Scan(
			&m.ID, &m.Name, &m.URL, &m.IntervalSeconds, &m.TimeoutSeconds,
			&currentStatus, &m.ConsecutiveFailures,
			&m.LastCheckedAt, &m.LastSuccessAt, &m.NextCheckAt,
			&m.CreatedAt, &m.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan monitor row: %w", err)
		}
		m.CurrentStatus = model.Status(currentStatus)
		monitors = append(monitors, &m)
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
