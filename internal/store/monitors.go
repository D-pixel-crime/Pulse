package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"pulse/internal/model"
)

const (
	// DefaultDueBatchLimit is the default maximum number of due monitors returned per discovery query.
	DefaultDueBatchLimit = 100
	// MaxDueBatchLimit is the hard upper cap on due monitors returned per discovery query.
	MaxDueBatchLimit = 100
	// DefaultClaimSafetyMarginSeconds is the fixed safety margin added to monitor timeout_seconds
	// when computing claim_expires_at so a crashed instance does not hold work indefinitely.
	DefaultClaimSafetyMarginSeconds = 30
	// DefaultClaimSafetyMargin is the duration equivalent of DefaultClaimSafetyMarginSeconds.
	DefaultClaimSafetyMargin = DefaultClaimSafetyMarginSeconds * time.Second
)

type checkExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

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

func scanClaimedMonitor(row rowScanner) (model.ClaimedMonitor, error) {
	var m model.Monitor
	var currentStatus string
	var claimToken uuid.UUID
	var claimExpiresAt time.Time

	if err := row.Scan(
		&m.ID, &m.Name, &m.URL, &m.IntervalSeconds, &m.TimeoutSeconds,
		&currentStatus, &m.ConsecutiveFailures,
		&m.LastCheckedAt, &m.LastSuccessAt, &m.NextCheckAt,
		&m.CreatedAt, &m.UpdatedAt,
		&claimToken, &claimExpiresAt,
	); err != nil {
		return model.ClaimedMonitor{}, err
	}
	m.CurrentStatus = model.Status(currentStatus)
	return model.ClaimedMonitor{
		Monitor:        &m,
		ClaimToken:     claimToken,
		ClaimExpiresAt: claimExpiresAt,
	}, nil
}

// Create inserts a new monitor into PostgreSQL using parameterized SQL.
// It sets creation invariants: current_status is UNKNOWN, consecutive_failures is 0,
// last_checked_at and last_success_at are null, and PostgreSQL statement_timestamp()
// generates identical created_at, updated_at, and next_check_at (due immediately).
func (s *Store) Create(ctx context.Context, m *model.Monitor) (*model.Monitor, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("store pool must not be nil")
	}
	return s.createWithQuerier(ctx, s.pool, m)
}

func (s *Store) createWithQuerier(ctx context.Context, q checkQuerier, m *model.Monitor) (*model.Monitor, error) {
	if m == nil {
		return nil, errors.New("monitor cannot be nil")
	}

	if m.ID == uuid.Nil {
		m.ID = uuid.New()
	}
	if m.IntervalSeconds == 0 {
		m.IntervalSeconds = model.DefaultIntervalSeconds
	}
	if m.TimeoutSeconds == 0 {
		m.TimeoutSeconds = model.DefaultTimeoutSeconds
	}
	m.CurrentStatus = model.StatusUnknown
	m.ConsecutiveFailures = 0
	m.LastCheckedAt = nil
	m.LastSuccessAt = nil

	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("monitor validation failed: %w", err)
	}

	const query = `
		WITH db_clock AS MATERIALIZED (
			SELECT statement_timestamp() AS current_time
		)
		INSERT INTO monitors (
			id, name, url, interval_seconds, timeout_seconds,
			current_status, consecutive_failures,
			last_checked_at, last_success_at, next_check_at,
			created_at, updated_at
		)
		SELECT
			$1, $2, $3, $4, $5,
			$6, $7,
			$8, $9, db_clock.current_time,
			db_clock.current_time, db_clock.current_time
		FROM db_clock
		RETURNING id, name, url, interval_seconds, timeout_seconds,
		          current_status, consecutive_failures,
		          last_checked_at, last_success_at, next_check_at,
		          created_at, updated_at
	`

	created, err := scanMonitor(q.QueryRow(ctx, query,
		m.ID, m.Name, m.URL, m.IntervalSeconds, m.TimeoutSeconds,
		string(m.CurrentStatus), m.ConsecutiveFailures,
		m.LastCheckedAt, m.LastSuccessAt,
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

// List retrieves a bounded page of monitors ordered by created_at DESC, id DESC
// using parameterized keyset pagination. When cursor is non-nil, only monitors created
// before the cursor (or with a smaller UUID tie-breaker for identical created_at timestamps)
// are returned. It queries limit + 1 items to determine if a subsequent page exists.
// Returns the page of monitors and a next_cursor string if further rows exist.
func (s *Store) List(ctx context.Context, limit int, cursor *model.MonitorCursor) ([]*model.Monitor, *string, error) {
	if s == nil || s.pool == nil {
		return nil, nil, errors.New("store pool must not be nil")
	}
	return s.listWithQuerier(ctx, s.pool, limit, cursor)
}

func (s *Store) listWithQuerier(ctx context.Context, q checkQuerier, limit int, cursor *model.MonitorCursor) ([]*model.Monitor, *string, error) {
	if limit < 0 {
		return nil, nil, fmt.Errorf("limit must not be negative, got %d", limit)
	}
	if limit == 0 {
		limit = model.DefaultMonitorListLimit
	} else if limit > model.MaxMonitorListLimit {
		limit = model.MaxMonitorListLimit
	}

	fetchLimit := limit + 1

	var (
		rows pgx.Rows
		err  error
	)

	if cursor == nil {
		const listMonitorsSQL = `
			SELECT id, name, url, interval_seconds, timeout_seconds,
			       current_status, consecutive_failures,
			       last_checked_at, last_success_at, next_check_at,
			       created_at, updated_at
			FROM monitors
			ORDER BY created_at DESC, id DESC
			LIMIT $1
		`
		rows, err = q.Query(ctx, listMonitorsSQL, fetchLimit)
	} else {
		const listMonitorsWithCursorSQL = `
			SELECT id, name, url, interval_seconds, timeout_seconds,
			       current_status, consecutive_failures,
			       last_checked_at, last_success_at, next_check_at,
			       created_at, updated_at
			FROM monitors
			WHERE (created_at < $1) OR (created_at = $1 AND id < $2)
			ORDER BY created_at DESC, id DESC
			LIMIT $3
		`
		rows, err = q.Query(ctx, listMonitorsWithCursorSQL, cursor.CreatedAt.UTC(), cursor.ID, fetchLimit)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query monitors: %w", err)
	}
	defer rows.Close()

	monitors := make([]*model.Monitor, 0, limit)
	for rows.Next() {
		m, err := scanMonitor(rows)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan monitor row: %w", err)
		}
		monitors = append(monitors, m)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("monitors rows iteration error: %w", err)
	}

	var nextCursor *string
	if len(monitors) > limit {
		last := monitors[limit-1]
		c := model.EncodeCursor(last.CreatedAt, last.ID)
		nextCursor = &c
		monitors = monitors[:limit]
	}

	return monitors, nextCursor, nil
}

// ListMonitors is an alias for List.
func (s *Store) ListMonitors(ctx context.Context, limit int, cursor *model.MonitorCursor) ([]*model.Monitor, *string, error) {
	return s.List(ctx, limit, cursor)
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

// ClaimDueMonitors atomically claims up to limit due, unclaimed (or lease-expired) monitors
// using PostgreSQL's statement_timestamp() as the shared clock authority and
// SELECT ... FOR UPDATE OF m SKIP LOCKED, ordered by next_check_at ASC, id ASC.
// Each claimed row receives a fresh UUID claim_token and a lease expiry of
// current_time + timeout_seconds + 30s without advancing next_check_at.
func (s *Store) ClaimDueMonitors(ctx context.Context, limit int) ([]model.ClaimedMonitor, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("store pool must not be nil")
	}
	return s.claimDueMonitorsWithQuerier(ctx, s.pool, limit)
}

func (s *Store) claimDueMonitorsWithQuerier(ctx context.Context, q checkQuerier, limit int) ([]model.ClaimedMonitor, error) {
	if limit < 0 {
		return nil, fmt.Errorf("limit must not be negative, got %d", limit)
	}
	if limit == 0 {
		limit = DefaultDueBatchLimit
	} else if limit > MaxDueBatchLimit {
		limit = MaxDueBatchLimit
	}

	const claimDueMonitorsSQL = `
		WITH db_clock AS MATERIALIZED (
			SELECT statement_timestamp() AS current_time
		),
		due AS (
			SELECT m.id, m.timeout_seconds, db_clock.current_time
			FROM monitors m
			CROSS JOIN db_clock
			WHERE m.next_check_at <= db_clock.current_time
			  AND (
			      m.claim_token IS NULL
			      OR m.claim_expires_at IS NULL
			      OR m.claim_expires_at <= db_clock.current_time
			  )
			ORDER BY m.next_check_at ASC, m.id ASC
			LIMIT $1
			FOR UPDATE OF m SKIP LOCKED
		)
		UPDATE monitors m
		SET claim_token = gen_random_uuid(),
		    claim_expires_at = due.current_time + make_interval(secs => (due.timeout_seconds + $2::integer))
		FROM due
		WHERE m.id = due.id
		RETURNING m.id, m.name, m.url, m.interval_seconds, m.timeout_seconds,
		          m.current_status, m.consecutive_failures,
		          m.last_checked_at, m.last_success_at, m.next_check_at,
		          m.created_at, m.updated_at,
		          m.claim_token, m.claim_expires_at
	`

	rows, err := q.Query(ctx, claimDueMonitorsSQL, limit, DefaultClaimSafetyMarginSeconds)
	if err != nil {
		return nil, fmt.Errorf("failed to claim due monitors: %w", err)
	}
	defer rows.Close()

	claimed := make([]model.ClaimedMonitor, 0)
	for rows.Next() {
		cm, err := scanClaimedMonitor(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan claimed monitor row: %w", err)
		}
		claimed = append(claimed, cm)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claimed monitor rows iteration error: %w", err)
	}

	// Preserve deterministic (next_check_at ASC, id ASC) ordering regardless of UPDATE RETURNING order.
	sort.Slice(claimed, func(i, j int) bool {
		mi, mj := claimed[i].Monitor, claimed[j].Monitor
		if !mi.NextCheckAt.Equal(mj.NextCheckAt) {
			return mi.NextCheckAt.Before(mj.NextCheckAt)
		}
		return bytes.Compare(mi.ID[:], mj.ID[:]) < 0
	})

	return claimed, nil
}

// ReleaseClaim clears the active claim on monitorID only if its stored claim_token
// still matches claimToken. It never advances next_check_at. Returns true when a
// matching claim was cleared, or false if the monitor was missing or fenced by another token.
func (s *Store) ReleaseClaim(ctx context.Context, monitorID, claimToken uuid.UUID) (bool, error) {
	if s == nil || s.pool == nil {
		return false, errors.New("store pool must not be nil")
	}
	return s.releaseClaimWithExecer(ctx, s.pool, monitorID, claimToken)
}

func (s *Store) releaseClaimWithExecer(ctx context.Context, ex checkExecer, monitorID, claimToken uuid.UUID) (bool, error) {
	if monitorID == uuid.Nil {
		return false, errors.New("monitor_id must not be nil")
	}
	if claimToken == uuid.Nil {
		return false, errors.New("claim_token must not be nil")
	}

	const releaseClaimSQL = `
		UPDATE monitors
		SET claim_token = NULL,
		    claim_expires_at = NULL
		WHERE id = $1
		  AND claim_token = $2
	`

	tag, err := ex.Exec(ctx, releaseClaimSQL, monitorID, claimToken)
	if err != nil {
		return false, fmt.Errorf("failed to release monitor claim: %w", err)
	}

	return tag.RowsAffected() > 0, nil
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
