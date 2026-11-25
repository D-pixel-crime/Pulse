package store

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"pulse/internal/model"
)

type fakeMonitorRows struct {
	pgx.Rows
	monitors []*model.Monitor
	idx      int
	closed   bool
	err      error
	scanErr  error
}

func (r *fakeMonitorRows) Close()     { r.closed = true }
func (r *fakeMonitorRows) Err() error { return r.err }

func (r *fakeMonitorRows) Next() bool {
	if r.idx >= len(r.monitors) {
		return false
	}
	r.idx++
	return true
}

func (r *fakeMonitorRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	return assignMonitorScan(dest, *r.monitors[r.idx-1])
}

func TestListDueMonitors_NilAndValidation(t *testing.T) {
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)

	var nilStore *Store
	if _, err := nilStore.ListDueMonitors(context.Background(), now, 10); err == nil {
		t.Fatal("expected error for nil store")
	}

	s := New(nil)
	if _, err := s.ListDueMonitors(context.Background(), now, 10); err == nil {
		t.Fatal("expected error for nil pool")
	}

	q := &fakeQuerier{}
	if _, err := s.listDueMonitorsWithQuerier(context.Background(), q, time.Time{}, 10); err == nil {
		t.Fatal("expected error for zero time")
	}
	if _, err := s.listDueMonitorsWithQuerier(context.Background(), q, now, -1); err == nil {
		t.Fatal("expected error for negative limit")
	}
	if len(q.calls) != 0 {
		t.Errorf("expected no SQL calls on validation error, got %d", len(q.calls))
	}
}

func TestListDueMonitorsWithQuerier_SQLAndErrorHandling(t *testing.T) {
	s := New(nil)
	loc := time.FixedZone("UTC+2", 2*3600)
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, loc)

	t.Run("parameterized SQL, default and capped batch size, UTC normalization", func(t *testing.T) {
		q := &fakeQuerier{
			queryFunc: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				if !strings.Contains(sql, "WHERE next_check_at <= $1") {
					t.Errorf("expected indexed WHERE next_check_at <= $1 predicate, got: %s", sql)
				}
				if !strings.Contains(sql, "ORDER BY next_check_at ASC, id ASC") {
					t.Errorf("expected deterministic ORDER BY next_check_at ASC, id ASC, got: %s", sql)
				}
				if !strings.Contains(sql, "LIMIT $2") {
					t.Errorf("expected parameterized LIMIT $2, got: %s", sql)
				}
				upperSQL := strings.ToUpper(strings.TrimSpace(sql))
				if !strings.HasPrefix(upperSQL, "SELECT ") || strings.Contains(upperSQL, "UPDATE MONITORS") || strings.Contains(upperSQL, "FOR UPDATE") {
					t.Errorf("discovery query must be a read-only SELECT without row mutation: %s", sql)
				}
				if len(args) != 2 {
					t.Fatalf("expected 2 SQL arguments, got %d", len(args))
				}
				argTime, ok := args[0].(time.Time)
				if !ok || !argTime.Equal(now.UTC()) || argTime.Location() != time.UTC {
					t.Errorf("expected $1 to be UTC time %v, got %v", now.UTC(), args[0])
				}
				return &fakeMonitorRows{}, nil
			},
		}

		// limit 0 -> DefaultDueBatchLimit (100)
		got, err := s.listDueMonitorsWithQuerier(context.Background(), q, now, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil || len(got) != 0 {
			t.Errorf("expected empty non-nil slice, got %v", got)
		}
		if q.calls[0].args[1].(int) != DefaultDueBatchLimit {
			t.Errorf("expected default limit %d, got %v", DefaultDueBatchLimit, q.calls[0].args[1])
		}

		// limit > MaxDueBatchLimit -> capped at MaxDueBatchLimit (100)
		_, err = s.listDueMonitorsWithQuerier(context.Background(), q, now, 500)
		if err != nil {
			t.Fatalf("unexpected error for large limit: %v", err)
		}
		if q.calls[1].args[1].(int) != MaxDueBatchLimit {
			t.Errorf("expected capped limit %d, got %v", MaxDueBatchLimit, q.calls[1].args[1])
		}
	})

	t.Run("propagates query, scan, and rows errors and closes rows", func(t *testing.T) {
		qErr := &fakeQuerier{
			queryFunc: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return nil, errors.New("db query failed")
			},
		}
		if _, err := s.listDueMonitorsWithQuerier(context.Background(), qErr, now, 10); err == nil || !strings.Contains(err.Error(), "db query failed") {
			t.Fatalf("expected query error propagation, got %v", err)
		}

		rowsScanErr := &fakeMonitorRows{
			monitors: []*model.Monitor{{ID: uuid.New()}},
			scanErr:  errors.New("scan failed"),
		}
		qScan := &fakeQuerier{
			queryFunc: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return rowsScanErr, nil
			},
		}
		if _, err := s.listDueMonitorsWithQuerier(context.Background(), qScan, now, 10); err == nil || !strings.Contains(err.Error(), "scan failed") {
			t.Fatalf("expected scan error propagation, got %v", err)
		}
		if !rowsScanErr.closed {
			t.Error("expected rows to be closed on scan error")
		}

		rowsIterErr := &fakeMonitorRows{
			err: errors.New("rows iteration failed"),
		}
		qIter := &fakeQuerier{
			queryFunc: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return rowsIterErr, nil
			},
		}
		if _, err := s.listDueMonitorsWithQuerier(context.Background(), qIter, now, 10); err == nil || !strings.Contains(err.Error(), "rows iteration failed") {
			t.Fatalf("expected rows.Err propagation, got %v", err)
		}
		if !rowsIterErr.closed {
			t.Error("expected rows to be closed on rows.Err")
		}
	})
}

func TestListDueMonitorsWithQuerier_SchedulingDataRules(t *testing.T) {
	s := New(nil)
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

	idLow := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	idHigh := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	idOverdue := uuid.MustParse("00000000-0000-0000-0000-000000000003")
	idAtNow := uuid.MustParse("00000000-0000-0000-0000-000000000004")
	idFuture := uuid.MustParse("00000000-0000-0000-0000-000000000005")

	// Newly created monitor must have next_check_at == created_at so it is due immediately.
	freshMonitor := &model.Monitor{
		Name:            "Fresh",
		URL:             "https://example.com/fresh",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
	}
	freshMonitor.InitializeCreationDefaults(now)
	if !freshMonitor.NextCheckAt.Equal(freshMonitor.CreatedAt) {
		t.Fatalf("expected fresh monitor next_check_at (%v) == created_at (%v)", freshMonitor.NextCheckAt, freshMonitor.CreatedAt)
	}

	records := []*model.Monitor{
		{
			ID:              idFuture,
			Name:            "Future",
			URL:             "https://example.com/future",
			IntervalSeconds: 60,
			TimeoutSeconds:  5,
			CurrentStatus:   model.StatusUp,
			NextCheckAt:     now.Add(1 * time.Second),
			CreatedAt:       now.Add(-5 * time.Minute),
			UpdatedAt:       now.Add(-1 * time.Minute),
		},
		{
			ID:              idHigh,
			Name:            "Tie High",
			URL:             "https://example.com/tie-high",
			IntervalSeconds: 60,
			TimeoutSeconds:  5,
			CurrentStatus:   model.StatusUp,
			NextCheckAt:     now.Add(-30 * time.Second),
			CreatedAt:       now.Add(-5 * time.Minute),
			UpdatedAt:       now.Add(-1 * time.Minute),
		},
		{
			ID:              idLow,
			Name:            "Tie Low",
			URL:             "https://example.com/tie-low",
			IntervalSeconds: 60,
			TimeoutSeconds:  5,
			CurrentStatus:   model.StatusUp,
			NextCheckAt:     now.Add(-30 * time.Second),
			CreatedAt:       now.Add(-5 * time.Minute),
			UpdatedAt:       now.Add(-1 * time.Minute),
		},
		{
			ID:              idOverdue,
			Name:            "Overdue Multi-Interval",
			URL:             "https://example.com/overdue",
			IntervalSeconds: 30,
			TimeoutSeconds:  5,
			CurrentStatus:   model.StatusDown,
			NextCheckAt:     now.Add(-10 * time.Minute),
			CreatedAt:       now.Add(-20 * time.Minute),
			UpdatedAt:       now.Add(-10 * time.Minute),
		},
		{
			ID:              idAtNow,
			Name:            "Exact Boundary",
			URL:             "https://example.com/boundary",
			IntervalSeconds: 60,
			TimeoutSeconds:  5,
			CurrentStatus:   model.StatusUnknown,
			NextCheckAt:     now,
			CreatedAt:       now,
			UpdatedAt:       now,
		},
	}

	originalNextCheck := make(map[uuid.UUID]time.Time, len(records))
	for _, m := range records {
		originalNextCheck[m.ID] = m.NextCheckAt
	}

	// Simulate SQL execution of WHERE next_check_at <= $1 ORDER BY next_check_at ASC, id ASC LIMIT $2.
	simQuerier := &fakeQuerier{
		queryFunc: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
			cutoff := args[0].(time.Time)
			lim := args[1].(int)

			var matched []*model.Monitor
			for _, m := range records {
				if !m.NextCheckAt.After(cutoff) {
					cp := *m
					matched = append(matched, &cp)
				}
			}
			sort.Slice(matched, func(i, j int) bool {
				if !matched[i].NextCheckAt.Equal(matched[j].NextCheckAt) {
					return matched[i].NextCheckAt.Before(matched[j].NextCheckAt)
				}
				return bytes.Compare(matched[i].ID[:], matched[j].ID[:]) < 0
			})
			if len(matched) > lim {
				matched = matched[:lim]
			}
			return &fakeMonitorRows{monitors: matched}, nil
		},
	}

	// 1. Due vs not-due and deterministic ordering (next_check_at ASC, id ASC).
	due, err := s.listDueMonitorsWithQuerier(context.Background(), simQuerier, now, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantOrder := []uuid.UUID{idOverdue, idLow, idHigh, idAtNow}
	if len(due) != len(wantOrder) {
		t.Fatalf("expected %d due monitors, got %d", len(wantOrder), len(due))
	}
	for i, wantID := range wantOrder {
		if due[i].ID != wantID {
			t.Errorf("due[%d].ID = %v, want %v", i, due[i].ID, wantID)
		}
	}

	// 2. Batch limit enforcement.
	limited, err := s.listDueMonitorsWithQuerier(context.Background(), simQuerier, now, 2)
	if err != nil {
		t.Fatalf("unexpected error with limit=2: %v", err)
	}
	if len(limited) != 2 || limited[0].ID != idOverdue || limited[1].ID != idLow {
		t.Errorf("unexpected limited batch: %+v", limited)
	}

	// 3. No mutation of next_check_at across repeated discovery passes; overdue selected once.
	dueAgain, err := s.listDueMonitorsWithQuerier(context.Background(), simQuerier, now, 100)
	if err != nil {
		t.Fatalf("unexpected error on second discovery: %v", err)
	}
	if len(dueAgain) != len(wantOrder) {
		t.Fatalf("expected identical due count %d on second discovery, got %d", len(wantOrder), len(dueAgain))
	}
	for _, m := range records {
		if !m.NextCheckAt.Equal(originalNextCheck[m.ID]) {
			t.Errorf("monitor %v next_check_at mutated during discovery: got %v, want %v", m.ID, m.NextCheckAt, originalNextCheck[m.ID])
		}
	}
}

type fakeClaimedMonitorRows struct {
	pgx.Rows
	claimed []model.ClaimedMonitor
	idx     int
	closed  bool
	err     error
	scanErr error
}

func (r *fakeClaimedMonitorRows) Close()     { r.closed = true }
func (r *fakeClaimedMonitorRows) Err() error { return r.err }

func (r *fakeClaimedMonitorRows) Next() bool {
	if r.idx >= len(r.claimed) {
		return false
	}
	r.idx++
	return true
}

func (r *fakeClaimedMonitorRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	cm := r.claimed[r.idx-1]
	if err := assignMonitorScan(dest, *cm.Monitor); err != nil {
		return err
	}
	*dest[12].(*uuid.UUID) = cm.ClaimToken
	*dest[13].(*time.Time) = cm.ClaimExpiresAt
	return nil
}

type fakeExecer struct {
	execFunc func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	calls    []fakeTxCall
}

func (f *fakeExecer) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	f.calls = append(f.calls, fakeTxCall{sql: sql, args: arguments})
	return f.execFunc(ctx, sql, arguments...)
}

func TestClaimDueMonitors_ValidationAndSQL(t *testing.T) {
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)

	var nilStore *Store
	if _, err := nilStore.ClaimDueMonitors(context.Background(), 5); err == nil {
		t.Fatal("expected error for nil store")
	}

	s := New(nil)
	if _, err := s.ClaimDueMonitors(context.Background(), 5); err == nil {
		t.Fatal("expected error for nil pool")
	}

	q := &fakeQuerier{}
	if _, err := s.claimDueMonitorsWithQuerier(context.Background(), q, -1); err == nil {
		t.Fatal("expected error for negative limit")
	}
	if len(q.calls) != 0 {
		t.Errorf("expected no SQL calls on validation error, got %d", len(q.calls))
	}

	idLate := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	idEarly := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	tokEarly := uuid.New()
	tokLate := uuid.New()

	qSQL := &fakeQuerier{
		queryFunc: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
			if !strings.Contains(sql, "db_clock AS MATERIALIZED") || !strings.Contains(sql, "statement_timestamp() AS current_time") {
				t.Errorf("expected materialized statement_timestamp() db_clock CTE in SQL: %s", sql)
			}
			if !strings.Contains(sql, "FOR UPDATE OF m SKIP LOCKED") {
				t.Errorf("expected FOR UPDATE OF m SKIP LOCKED in SQL: %s", sql)
			}
			if !strings.Contains(sql, "ORDER BY m.next_check_at ASC, m.id ASC") {
				t.Errorf("expected deterministic ordering in SQL: %s", sql)
			}
			if !strings.Contains(sql, "m.claim_token IS NULL") || !strings.Contains(sql, "m.claim_expires_at <= db_clock.current_time") {
				t.Errorf("expected claim availability predicate against db_clock in SQL: %s", sql)
			}
			if !strings.Contains(sql, "LIMIT $1") {
				t.Errorf("expected LIMIT $1 in SQL: %s", sql)
			}
			if strings.Contains(sql, "next_check_at =") {
				t.Errorf("claim SQL must never advance next_check_at: %s", sql)
			}
			if len(args) != 2 {
				t.Fatalf("expected 2 SQL args (limit, safetyMarginSeconds), got %d", len(args))
			}
			if args[1].(int) != DefaultClaimSafetyMarginSeconds {
				t.Errorf("expected safety margin %d, got %v", DefaultClaimSafetyMarginSeconds, args[1])
			}
			// Return intentionally out of order to verify deterministic Go sort.
			return &fakeClaimedMonitorRows{
				claimed: []model.ClaimedMonitor{
					{
						Monitor: &model.Monitor{
							ID:              idLate,
							NextCheckAt:     now,
							TimeoutSeconds:  5,
							IntervalSeconds: 60,
						},
						ClaimToken:     tokLate,
						ClaimExpiresAt: now.Add(35 * time.Second),
					},
					{
						Monitor: &model.Monitor{
							ID:              idEarly,
							NextCheckAt:     now.Add(-10 * time.Second),
							TimeoutSeconds:  5,
							IntervalSeconds: 60,
						},
						ClaimToken:     tokEarly,
						ClaimExpiresAt: now.Add(35 * time.Second),
					},
				},
			}, nil
		},
	}

	got, err := s.claimDueMonitorsWithQuerier(context.Background(), qSQL, 500)
	if err != nil {
		t.Fatalf("claimDueMonitorsWithQuerier failed: %v", err)
	}
	if qSQL.calls[0].args[0].(int) != MaxDueBatchLimit {
		t.Errorf("expected capped limit %d, got %v", MaxDueBatchLimit, qSQL.calls[0].args[0])
	}
	if len(got) != 2 || got[0].Monitor.ID != idEarly || got[1].Monitor.ID != idLate {
		t.Fatalf("expected deterministic ordering [early, late], got %+v", got)
	}
	if got[0].ClaimToken != tokEarly || got[1].ClaimToken != tokLate {
		t.Fatalf("expected matching claim tokens preserved")
	}
	if !got[0].Monitor.NextCheckAt.Equal(now.Add(-10 * time.Second)) {
		t.Errorf("expected next_check_at unchanged on claim, got %v", got[0].Monitor.NextCheckAt)
	}
}

func TestReleaseClaim_ValidationAndMatching(t *testing.T) {
	var nilStore *Store
	if _, err := nilStore.ReleaseClaim(context.Background(), uuid.New(), uuid.New()); err == nil {
		t.Fatal("expected error for nil store")
	}

	s := New(nil)
	if _, err := s.ReleaseClaim(context.Background(), uuid.New(), uuid.New()); err == nil {
		t.Fatal("expected error for nil pool")
	}

	ex := &fakeExecer{}
	if _, err := s.releaseClaimWithExecer(context.Background(), ex, uuid.Nil, uuid.New()); err == nil {
		t.Fatal("expected error for nil monitor_id")
	}
	if _, err := s.releaseClaimWithExecer(context.Background(), ex, uuid.New(), uuid.Nil); err == nil {
		t.Fatal("expected error for nil claim_token")
	}
	if len(ex.calls) != 0 {
		t.Fatalf("expected no SQL calls on invalid args, got %d", len(ex.calls))
	}

	monitorID := uuid.New()
	claimToken := uuid.New()
	activeToken := claimToken

	exSim := &fakeExecer{
		execFunc: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			if !strings.Contains(sql, "WHERE id = $1") || !strings.Contains(sql, "AND claim_token = $2") {
				t.Errorf("expected WHERE id = $1 AND claim_token = $2, got: %s", sql)
			}
			if strings.Contains(sql, "next_check_at") {
				t.Errorf("ReleaseClaim must not modify next_check_at: %s", sql)
			}
			if args[0].(uuid.UUID) == monitorID && args[1].(uuid.UUID) == activeToken {
				activeToken = uuid.Nil
				return pgconn.NewCommandTag("UPDATE 1"), nil
			}
			return pgconn.NewCommandTag("UPDATE 0"), nil
		},
	}

	// Wrong token does not release active claim.
	released, err := s.releaseClaimWithExecer(context.Background(), exSim, monitorID, uuid.New())
	if err != nil {
		t.Fatalf("unexpected error on wrong token: %v", err)
	}
	if released {
		t.Fatal("expected wrong token not to release claim")
	}
	if activeToken != claimToken {
		t.Fatal("expected active claim token to remain intact")
	}

	// Matching token releases claim.
	released, err = s.releaseClaimWithExecer(context.Background(), exSim, monitorID, claimToken)
	if err != nil {
		t.Fatalf("unexpected error on matching token: %v", err)
	}
	if !released {
		t.Fatal("expected matching token to release claim")
	}
	if activeToken != uuid.Nil {
		t.Fatal("expected active claim token to be cleared")
	}
}

func TestCreateMonitorWithQuerier_UsesDBClock(t *testing.T) {
	var nilStore *Store
	if _, err := nilStore.Create(context.Background(), &model.Monitor{Name: "X", URL: "https://example.com"}); err == nil {
		t.Fatal("expected error for nil store")
	}
	s := New(nil)
	if _, err := s.Create(context.Background(), &model.Monitor{Name: "X", URL: "https://example.com"}); err == nil {
		t.Fatal("expected error for nil pool")
	}

	dbTime := time.Date(2026, 11, 25, 16, 40, 0, 0, time.UTC)
	q := &fakeQuerier{
		queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
			if !strings.Contains(sql, "db_clock AS MATERIALIZED") || !strings.Contains(sql, "statement_timestamp() AS current_time") {
				t.Errorf("expected INSERT to use db_clock statement_timestamp() CTE, got: %s", sql)
			}
			if !strings.Contains(sql, "db_clock.current_time") {
				t.Errorf("expected next_check_at/created_at/updated_at set from db_clock.current_time, got: %s", sql)
			}
			if len(args) != 9 {
				t.Fatalf("expected 9 SQL args (no application timestamps), got %d: %v", len(args), args)
			}
			for i, a := range args {
				if _, isTime := a.(time.Time); isTime {
					t.Errorf("arg %d unexpectedly passed application time.Time: %v", i, a)
				}
			}
			return fakeRow{scanFunc: func(dest ...any) error {
				return assignMonitorScan(dest, model.Monitor{
					ID:                  args[0].(uuid.UUID),
					Name:                args[1].(string),
					URL:                 args[2].(string),
					IntervalSeconds:     args[3].(int),
					TimeoutSeconds:      args[4].(int),
					CurrentStatus:       model.Status(args[5].(string)),
					ConsecutiveFailures: args[6].(int),
					LastCheckedAt:       args[7].(*time.Time),
					LastSuccessAt:       args[8].(*time.Time),
					NextCheckAt:         dbTime,
					CreatedAt:           dbTime,
					UpdatedAt:           dbTime,
				})
			}}
		},
	}

	// Pass intentionally skewed client timestamps on input to verify DB timestamps override them.
	skewed := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	created, err := s.createWithQuerier(context.Background(), q, &model.Monitor{
		Name:        "DB Clock Create",
		URL:         "https://example.com/create",
		NextCheckAt: skewed,
		CreatedAt:   skewed,
		UpdatedAt:   skewed,
	})
	if err != nil {
		t.Fatalf("createWithQuerier failed: %v", err)
	}
	if !created.CreatedAt.Equal(dbTime) || !created.UpdatedAt.Equal(dbTime) || !created.NextCheckAt.Equal(dbTime) {
		t.Fatalf("expected DB-generated timestamps %v, got created=%v updated=%v next=%v", dbTime, created.CreatedAt, created.UpdatedAt, created.NextCheckAt)
	}
	if created.CurrentStatus != model.StatusUnknown || created.ConsecutiveFailures != 0 {
		t.Fatalf("unexpected initial status/failures: %s/%d", created.CurrentStatus, created.ConsecutiveFailures)
	}
	if created.LastCheckedAt != nil || created.LastSuccessAt != nil {
		t.Fatalf("expected nil last_checked_at/last_success_at")
	}
}
