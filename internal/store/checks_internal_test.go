package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"pulse/internal/model"
)

type fakeRow struct {
	scanFunc func(dest ...any) error
}

func (r fakeRow) Scan(dest ...any) error {
	return r.scanFunc(dest...)
}

type fakeTxCall struct {
	sql  string
	args []any
}

type fakeCheckTx struct {
	queryRowFunc   func(ctx context.Context, sql string, args ...any) pgx.Row
	commitErr      error
	calls          []fakeTxCall
	commitCount    int
	rollbackCnt    int
	rollbackCtxErr error
}

func (f *fakeCheckTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	f.calls = append(f.calls, fakeTxCall{sql: sql, args: args})
	return f.queryRowFunc(ctx, sql, args...)
}

func (f *fakeCheckTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (f *fakeCheckTx) Commit(ctx context.Context) error {
	f.commitCount++
	return f.commitErr
}

func (f *fakeCheckTx) Rollback(ctx context.Context) error {
	f.rollbackCnt++
	if ctx == nil {
		f.rollbackCtxErr = errors.New("nil rollback context")
	} else {
		f.rollbackCtxErr = ctx.Err()
	}
	return nil
}

func assignMonitorScan(dest []any, m model.Monitor) error {
	*dest[0].(*uuid.UUID) = m.ID
	*dest[1].(*string) = m.Name
	*dest[2].(*string) = m.URL
	*dest[3].(*int) = m.IntervalSeconds
	*dest[4].(*int) = m.TimeoutSeconds
	*dest[5].(*string) = string(m.CurrentStatus)
	*dest[6].(*int) = m.ConsecutiveFailures
	*dest[7].(**time.Time) = m.LastCheckedAt
	*dest[8].(**time.Time) = m.LastSuccessAt
	*dest[9].(*time.Time) = m.NextCheckAt
	*dest[10].(*time.Time) = m.CreatedAt
	*dest[11].(*time.Time) = m.UpdatedAt
	return nil
}

func assignMonitorForUpdateScan(dest []any, m model.Monitor, claimToken *uuid.UUID, claimExpiresAt *time.Time, dbNow time.Time) error {
	if err := assignMonitorScan(dest, m); err != nil {
		return err
	}
	*dest[12].(**uuid.UUID) = claimToken
	*dest[13].(**time.Time) = claimExpiresAt
	*dest[14].(*time.Time) = dbNow
	return nil
}

func TestRecordCheckResult_NilPoolAndInvalidParams(t *testing.T) {
	var nilStore *Store
	if _, err := nilStore.RecordCheckResult(context.Background(), RecordCheckParams{}); err == nil {
		t.Fatal("expected error for nil store")
	}

	s := New(nil)
	if _, err := s.RecordCheckResult(context.Background(), RecordCheckParams{
		MonitorID:  uuid.New(),
		ClaimToken: uuid.New(),
		CheckedAt:  time.Now().UTC(),
	}); err == nil {
		t.Fatal("expected error for nil pool")
	}

	if _, err := s.recordCheckResultTx(context.Background(), &fakeCheckTx{}, RecordCheckParams{
		MonitorID:  uuid.Nil,
		ClaimToken: uuid.New(),
		CheckedAt:  time.Now().UTC(),
	}, nil); err == nil {
		t.Fatal("expected error for nil monitor_id")
	}

	if _, err := s.recordCheckResultTx(context.Background(), &fakeCheckTx{}, RecordCheckParams{
		MonitorID:  uuid.New(),
		ClaimToken: uuid.Nil,
		CheckedAt:  time.Now().UTC(),
	}, nil); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("expected ErrClaimLost for nil claim_token, got %v", err)
	}

	if _, err := s.recordCheckResultTx(context.Background(), &fakeCheckTx{}, RecordCheckParams{
		MonitorID:  uuid.New(),
		ClaimToken: uuid.New(),
		CheckedAt:  time.Time{},
	}, nil); !errors.Is(err, model.ErrZeroCheckedAt) {
		t.Fatalf("expected ErrZeroCheckedAt, got %v", err)
	}

	if _, err := s.recordCheckResultTx(context.Background(), &fakeCheckTx{}, RecordCheckParams{
		MonitorID:  uuid.New(),
		ClaimToken: uuid.New(),
		CheckedAt:  time.Now().UTC(),
		LatencyMs:  -1,
	}, nil); err == nil {
		t.Fatal("expected error for negative latency_ms")
	}
}

func TestRecordCheckResultTx_RollsBackWhenMonitorNotFound(t *testing.T) {
	s := New(nil)
	tx := &fakeCheckTx{
		queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return fakeRow{scanFunc: func(dest ...any) error {
				return pgx.ErrNoRows
			}}
		},
	}

	_, err := s.recordCheckResultTx(context.Background(), tx, RecordCheckParams{
		MonitorID:  uuid.New(),
		ClaimToken: uuid.New(),
		CheckedAt:  time.Now().UTC(),
		Success:    true,
	}, nil)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if tx.commitCount != 0 {
		t.Errorf("expected 0 commits, got %d", tx.commitCount)
	}
	if tx.rollbackCnt != 1 {
		t.Errorf("expected 1 rollback, got %d", tx.rollbackCnt)
	}
	if len(tx.calls) != 1 {
		t.Errorf("expected only SELECT FOR UPDATE to run, got %d calls", len(tx.calls))
	}
}

func TestRecordCheckResultTx_RollsBackOnClaimLoss(t *testing.T) {
	s := New(nil)
	monitorID := uuid.New()
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	token := uuid.New()
	otherToken := uuid.New()
	activeExpiry := now.Add(35 * time.Second)
	expiredExpiry := now.Add(-1 * time.Second)

	cases := []struct {
		name           string
		storedToken    *uuid.UUID
		storedExpiry   *time.Time
		expectedErrSub string
	}{
		{
			name:           "missing claim on monitor row",
			storedToken:    nil,
			storedExpiry:   nil,
			expectedErrSub: "no active claim",
		},
		{
			name:           "stale mismatched claim token",
			storedToken:    &otherToken,
			storedExpiry:   &activeExpiry,
			expectedErrSub: "mismatch",
		},
		{
			name:           "expired claim lease",
			storedToken:    &token,
			storedExpiry:   &expiredExpiry,
			expectedErrSub: "expired",
		},
		{
			name:           "exact boundary claim expiry",
			storedToken:    &token,
			storedExpiry:   &now,
			expectedErrSub: "expired",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fakeCheckTx{
				queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
					if strings.Contains(sql, "SELECT id, name") {
						return fakeRow{scanFunc: func(dest ...any) error {
							return assignMonitorForUpdateScan(dest, model.Monitor{
								ID:              monitorID,
								Name:            "API",
								URL:             "https://example.com",
								IntervalSeconds: 60,
								TimeoutSeconds:  5,
								CurrentStatus:   model.StatusUnknown,
								NextCheckAt:     now,
								CreatedAt:       now,
								UpdatedAt:       now,
							}, tc.storedToken, tc.storedExpiry, now)
						}}
					}
					t.Fatalf("expected no INSERT or UPDATE after claim loss, got SQL: %s", sql)
					return nil
				},
			}

			_, err := s.recordCheckResultTx(context.Background(), tx, RecordCheckParams{
				MonitorID:  monitorID,
				ClaimToken: token,
				CheckedAt:  now,
				Success:    true,
				LatencyMs:  10,
			}, nil)
			if !errors.Is(err, ErrClaimLost) || !strings.Contains(err.Error(), tc.expectedErrSub) {
				t.Fatalf("expected ErrClaimLost (%s), got %v", tc.expectedErrSub, err)
			}
			if tx.commitCount != 0 {
				t.Errorf("expected 0 commits, got %d", tx.commitCount)
			}
			if tx.rollbackCnt != 1 {
				t.Errorf("expected 1 rollback, got %d", tx.rollbackCnt)
			}
			if len(tx.calls) != 1 {
				t.Errorf("expected only SELECT FOR UPDATE to run, got %d calls", len(tx.calls))
			}
		})
	}
}

func TestRecordCheckResultTx_RollsBackOnInsertFailure(t *testing.T) {
	s := New(nil)
	monitorID := uuid.New()
	claimToken := uuid.New()
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	expiry := now.Add(35 * time.Second)

	tx := &fakeCheckTx{
		queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
			if strings.Contains(sql, "SELECT id, name") {
				return fakeRow{scanFunc: func(dest ...any) error {
					return assignMonitorForUpdateScan(dest, model.Monitor{
						ID:              monitorID,
						Name:            "API",
						URL:             "https://example.com",
						IntervalSeconds: 60,
						TimeoutSeconds:  5,
						CurrentStatus:   model.StatusUnknown,
						NextCheckAt:     now,
						CreatedAt:       now,
						UpdatedAt:       now,
					}, &claimToken, &expiry, now)
				}}
			}
			if strings.Contains(sql, "INSERT INTO check_results") {
				return fakeRow{scanFunc: func(dest ...any) error {
					return errors.New("insert check_results failed")
				}}
			}
			t.Fatalf("unexpected SQL after insert failure: %s", sql)
			return nil
		},
	}

	_, err := s.recordCheckResultTx(context.Background(), tx, RecordCheckParams{
		MonitorID:  monitorID,
		ClaimToken: claimToken,
		CheckedAt:  now,
		Success:    true,
		LatencyMs:  50,
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "insert check_results failed") {
		t.Fatalf("expected insert failure error, got %v", err)
	}
	if tx.commitCount != 0 {
		t.Errorf("expected 0 commits, got %d", tx.commitCount)
	}
	if tx.rollbackCnt != 1 {
		t.Errorf("expected 1 rollback, got %d", tx.rollbackCnt)
	}
	if len(tx.calls) != 2 {
		t.Errorf("expected 2 SQL calls (SELECT + INSERT), got %d", len(tx.calls))
	}
}

func TestRecordCheckResultTx_RollsBackOnUpdateFailure(t *testing.T) {
	s := New(nil)
	monitorID := uuid.New()
	claimToken := uuid.New()
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	expiry := now.Add(35 * time.Second)
	statusCode := 503
	errText := "HTTP status 503 Service Unavailable"

	tx := &fakeCheckTx{
		queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
			if strings.Contains(sql, "SELECT id, name") {
				return fakeRow{scanFunc: func(dest ...any) error {
					return assignMonitorForUpdateScan(dest, model.Monitor{
						ID:              monitorID,
						Name:            "API",
						URL:             "https://example.com",
						IntervalSeconds: 60,
						TimeoutSeconds:  5,
						CurrentStatus:   model.StatusUnknown,
						NextCheckAt:     now,
						CreatedAt:       now,
						UpdatedAt:       now,
					}, &claimToken, &expiry, now)
				}}
			}
			if strings.Contains(sql, "INSERT INTO check_results") {
				return fakeRow{scanFunc: func(dest ...any) error {
					*dest[0].(*uuid.UUID) = args[0].(uuid.UUID)
					*dest[1].(*uuid.UUID) = args[1].(uuid.UUID)
					*dest[2].(*time.Time) = args[2].(time.Time)
					*dest[3].(*bool) = args[3].(bool)
					*dest[4].(**int) = args[4].(*int)
					*dest[5].(*int64) = args[5].(int64)
					*dest[6].(**string) = args[6].(*string)
					return nil
				}}
			}
			if strings.Contains(sql, "UPDATE monitors") {
				return fakeRow{scanFunc: func(dest ...any) error {
					return errors.New("update monitors failed")
				}}
			}
			t.Fatalf("unexpected SQL: %s", sql)
			return nil
		},
	}

	_, err := s.recordCheckResultTx(context.Background(), tx, RecordCheckParams{
		MonitorID:    monitorID,
		ClaimToken:   claimToken,
		CheckedAt:    now,
		Success:      false,
		HTTPStatus:   &statusCode,
		LatencyMs:    120,
		ErrorMessage: &errText,
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "update monitors failed") {
		t.Fatalf("expected update failure error, got %v", err)
	}
	if tx.commitCount != 0 {
		t.Errorf("expected 0 commits, got %d", tx.commitCount)
	}
	if tx.rollbackCnt != 1 {
		t.Errorf("expected 1 rollback, got %d", tx.rollbackCnt)
	}
	if len(tx.calls) != 3 {
		t.Errorf("expected 3 SQL calls (SELECT + INSERT + UPDATE), got %d", len(tx.calls))
	}
}

func TestRecordCheckResultTx_RollsBackWhenConditionalUpdateReturnsNoRows(t *testing.T) {
	s := New(nil)
	monitorID := uuid.New()
	claimToken := uuid.New()
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	expiry := now.Add(35 * time.Second)

	tx := &fakeCheckTx{
		queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
			if strings.Contains(sql, "SELECT id, name") {
				return fakeRow{scanFunc: func(dest ...any) error {
					return assignMonitorForUpdateScan(dest, model.Monitor{
						ID:              monitorID,
						Name:            "API",
						URL:             "https://example.com",
						IntervalSeconds: 60,
						TimeoutSeconds:  5,
						CurrentStatus:   model.StatusUnknown,
						NextCheckAt:     now,
						CreatedAt:       now,
						UpdatedAt:       now,
					}, &claimToken, &expiry, now)
				}}
			}
			if strings.Contains(sql, "INSERT INTO check_results") {
				return fakeRow{scanFunc: func(dest ...any) error {
					*dest[0].(*uuid.UUID) = args[0].(uuid.UUID)
					*dest[1].(*uuid.UUID) = args[1].(uuid.UUID)
					*dest[2].(*time.Time) = args[2].(time.Time)
					*dest[3].(*bool) = args[3].(bool)
					*dest[4].(**int) = args[4].(*int)
					*dest[5].(*int64) = args[5].(int64)
					*dest[6].(**string) = args[6].(*string)
					return nil
				}}
			}
			if strings.Contains(sql, "UPDATE monitors") {
				return fakeRow{scanFunc: func(dest ...any) error {
					return pgx.ErrNoRows
				}}
			}
			t.Fatalf("unexpected SQL: %s", sql)
			return nil
		},
	}

	_, err := s.recordCheckResultTx(context.Background(), tx, RecordCheckParams{
		MonitorID:  monitorID,
		ClaimToken: claimToken,
		CheckedAt:  now,
		Success:    true,
		LatencyMs:  10,
	}, nil)
	if !errors.Is(err, ErrClaimLost) {
		t.Fatalf("expected ErrClaimLost when conditional UPDATE matches 0 rows, got %v", err)
	}
	if tx.commitCount != 0 {
		t.Errorf("expected 0 commits, got %d", tx.commitCount)
	}
	if tx.rollbackCnt != 1 {
		t.Errorf("expected 1 rollback, got %d", tx.rollbackCnt)
	}
}

func TestRecordCheckResultTx_RollsBackOnCommitFailure(t *testing.T) {
	s := New(nil)
	monitorID := uuid.New()
	claimToken := uuid.New()
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	expiry := now.Add(35 * time.Second)

	tx := &fakeCheckTx{
		commitErr: errors.New("commit failed"),
		queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
			if strings.Contains(sql, "SELECT id, name") {
				return fakeRow{scanFunc: func(dest ...any) error {
					return assignMonitorForUpdateScan(dest, model.Monitor{
						ID:              monitorID,
						Name:            "API",
						URL:             "https://example.com",
						IntervalSeconds: 60,
						TimeoutSeconds:  5,
						CurrentStatus:   model.StatusUnknown,
						NextCheckAt:     now,
						CreatedAt:       now,
						UpdatedAt:       now,
					}, &claimToken, &expiry, now)
				}}
			}
			if strings.Contains(sql, "INSERT INTO check_results") {
				return fakeRow{scanFunc: func(dest ...any) error {
					*dest[0].(*uuid.UUID) = args[0].(uuid.UUID)
					*dest[1].(*uuid.UUID) = args[1].(uuid.UUID)
					*dest[2].(*time.Time) = args[2].(time.Time)
					*dest[3].(*bool) = args[3].(bool)
					*dest[4].(**int) = args[4].(*int)
					*dest[5].(*int64) = args[5].(int64)
					*dest[6].(**string) = args[6].(*string)
					return nil
				}}
			}
			if strings.Contains(sql, "UPDATE monitors") {
				return fakeRow{scanFunc: func(dest ...any) error {
					return assignMonitorScan(dest, model.Monitor{
						ID:                  monitorID,
						Name:                "API",
						URL:                 "https://example.com",
						IntervalSeconds:     60,
						TimeoutSeconds:      5,
						CurrentStatus:       model.Status(args[1].(string)),
						ConsecutiveFailures: args[2].(int),
						LastCheckedAt:       args[3].(*time.Time),
						LastSuccessAt:       args[4].(*time.Time),
						NextCheckAt:         now.Add(60 * time.Second),
						CreatedAt:           now,
						UpdatedAt:           now,
					})
				}}
			}
			t.Fatalf("unexpected SQL: %s", sql)
			return nil
		},
	}

	_, err := s.recordCheckResultTx(context.Background(), tx, RecordCheckParams{
		MonitorID:  monitorID,
		ClaimToken: claimToken,
		CheckedAt:  now,
		Success:    true,
		LatencyMs:  20,
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "commit failed") {
		t.Fatalf("expected commit error, got %v", err)
	}
	if tx.commitCount != 1 {
		t.Errorf("expected 1 commit attempt, got %d", tx.commitCount)
	}
	if tx.rollbackCnt != 1 {
		t.Errorf("expected 1 rollback after commit failure, got %d", tx.rollbackCnt)
	}
}

func TestRecordCheckResultTx_ParameterizedSQLAndCommit(t *testing.T) {
	s := New(nil)
	monitorID := uuid.New()
	claimToken := uuid.New()
	checkedAt := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	dbUpdateClock := checkedAt.Add(5 * time.Second)
	expiry := checkedAt.Add(35 * time.Second)
	statusCode := 200

	tx := &fakeCheckTx{
		queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
			if strings.Contains(sql, "SELECT id, name") {
				if !strings.Contains(sql, "clock_timestamp()") {
					t.Errorf("expected SELECT ... FOR UPDATE to use clock_timestamp(), got: %s", sql)
				}
				return fakeRow{scanFunc: func(dest ...any) error {
					return assignMonitorForUpdateScan(dest, model.Monitor{
						ID:                  monitorID,
						Name:                "API",
						URL:                 "https://example.com",
						IntervalSeconds:     90,
						TimeoutSeconds:      5,
						CurrentStatus:       model.StatusDown,
						ConsecutiveFailures: 3,
						NextCheckAt:         checkedAt,
						CreatedAt:           checkedAt.Add(-5 * time.Minute),
						UpdatedAt:           checkedAt.Add(-time.Minute),
					}, &claimToken, &expiry, checkedAt)
				}}
			}
			if strings.Contains(sql, "INSERT INTO check_results") {
				return fakeRow{scanFunc: func(dest ...any) error {
					*dest[0].(*uuid.UUID) = args[0].(uuid.UUID)
					*dest[1].(*uuid.UUID) = args[1].(uuid.UUID)
					*dest[2].(*time.Time) = args[2].(time.Time)
					*dest[3].(*bool) = args[3].(bool)
					*dest[4].(**int) = args[4].(*int)
					*dest[5].(*int64) = args[5].(int64)
					*dest[6].(**string) = args[6].(*string)
					return nil
				}}
			}
			if strings.Contains(sql, "UPDATE monitors") {
				if !strings.Contains(sql, "db_clock AS MATERIALIZED") || !strings.Contains(sql, "clock_timestamp() AS current_time") {
					t.Errorf("expected UPDATE monitors to use materialized clock_timestamp() CTE, got: %s", sql)
				}
				if !strings.Contains(sql, "next_check_at = db_clock.current_time + make_interval(secs => m.interval_seconds)") {
					t.Errorf("expected next_check_at computed from db_clock.current_time + interval_seconds, got: %s", sql)
				}
				if !strings.Contains(sql, "updated_at = db_clock.current_time") {
					t.Errorf("expected updated_at set to db_clock.current_time, got: %s", sql)
				}
				if !strings.Contains(sql, "claim_token = NULL") || !strings.Contains(sql, "claim_expires_at = NULL") {
					t.Errorf("expected UPDATE monitors to clear claim_token and claim_expires_at, got: %s", sql)
				}
				if !strings.Contains(sql, "AND m.claim_token = $6") {
					t.Errorf("expected UPDATE monitors to fence on m.claim_token = $6, got: %s", sql)
				}
				if !strings.Contains(sql, "AND m.claim_expires_at > db_clock.current_time") {
					t.Errorf("expected UPDATE monitors to verify m.claim_expires_at > db_clock.current_time, got: %s", sql)
				}
				if len(args) != 6 || args[5].(uuid.UUID) != claimToken {
					t.Errorf("expected 6 args with $6=claimToken %v, got %v", claimToken, args)
				}
				return fakeRow{scanFunc: func(dest ...any) error {
					return assignMonitorScan(dest, model.Monitor{
						ID:                  monitorID,
						Name:                "API",
						URL:                 "https://example.com",
						IntervalSeconds:     90,
						TimeoutSeconds:      5,
						CurrentStatus:       model.Status(args[1].(string)),
						ConsecutiveFailures: args[2].(int),
						LastCheckedAt:       args[3].(*time.Time),
						LastSuccessAt:       args[4].(*time.Time),
						NextCheckAt:         dbUpdateClock.Add(90 * time.Second),
						CreatedAt:           checkedAt.Add(-5 * time.Minute),
						UpdatedAt:           dbUpdateClock,
					})
				}}
			}
			t.Fatalf("unexpected SQL: %s", sql)
			return nil
		},
	}

	out, err := s.recordCheckResultTx(context.Background(), tx, RecordCheckParams{
		MonitorID:  monitorID,
		ClaimToken: claimToken,
		CheckedAt:  checkedAt,
		Success:    true,
		HTTPStatus: &statusCode,
		LatencyMs:  350,
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.commitCount != 1 {
		t.Errorf("expected 1 commit, got %d", tx.commitCount)
	}
	if tx.rollbackCnt != 0 {
		t.Errorf("expected 0 rollbacks on commit, got %d", tx.rollbackCnt)
	}
	if out.Monitor.CurrentStatus != model.StatusUp {
		t.Errorf("expected status UP, got %q", out.Monitor.CurrentStatus)
	}
	if out.Monitor.ConsecutiveFailures != 0 {
		t.Errorf("expected consecutive_failures 0, got %d", out.Monitor.ConsecutiveFailures)
	}
	if out.Monitor.LastCheckedAt == nil || !out.Monitor.LastCheckedAt.Equal(checkedAt) {
		t.Errorf("expected last_checked_at %v, got %v", checkedAt, out.Monitor.LastCheckedAt)
	}
	if out.Monitor.LastSuccessAt == nil || !out.Monitor.LastSuccessAt.Equal(checkedAt) {
		t.Errorf("expected last_success_at %v, got %v", checkedAt, out.Monitor.LastSuccessAt)
	}
	if !out.Monitor.UpdatedAt.Equal(dbUpdateClock) {
		t.Errorf("expected updated_at %v, got %v", dbUpdateClock, out.Monitor.UpdatedAt)
	}
	wantNext := dbUpdateClock.Add(90 * time.Second)
	if !out.Monitor.NextCheckAt.Equal(wantNext) {
		t.Errorf("expected next_check_at %v, got %v", wantNext, out.Monitor.NextCheckAt)
	}
	if out.CheckResult.MonitorID != monitorID || !out.CheckResult.Success || out.CheckResult.LatencyMs != 350 {
		t.Errorf("unexpected check result persisted: %+v", out.CheckResult)
	}
}

func TestRecordCheckResultTx_RollsBackAfterInsertHookAndUsesUncanceledRollbackCtx(t *testing.T) {
	s := New(nil)
	monitorID := uuid.New()
	claimToken := uuid.New()
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	expiry := now.Add(35 * time.Second)

	t.Run("rolls back before UPDATE when afterInsert hook fails", func(t *testing.T) {
		tx := &fakeCheckTx{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				if strings.Contains(sql, "SELECT id, name") {
					return fakeRow{scanFunc: func(dest ...any) error {
						return assignMonitorForUpdateScan(dest, model.Monitor{
							ID:              monitorID,
							Name:            "API",
							URL:             "https://example.com",
							IntervalSeconds: 60,
							TimeoutSeconds:  5,
							CurrentStatus:   model.StatusUnknown,
							NextCheckAt:     now,
							CreatedAt:       now,
							UpdatedAt:       now,
						}, &claimToken, &expiry, now)
					}}
				}
				if strings.Contains(sql, "INSERT INTO check_results") {
					return fakeRow{scanFunc: func(dest ...any) error {
						*dest[0].(*uuid.UUID) = args[0].(uuid.UUID)
						*dest[1].(*uuid.UUID) = args[1].(uuid.UUID)
						*dest[2].(*time.Time) = args[2].(time.Time)
						*dest[3].(*bool) = args[3].(bool)
						*dest[4].(**int) = args[4].(*int)
						*dest[5].(*int64) = args[5].(int64)
						*dest[6].(**string) = args[6].(*string)
						return nil
					}}
				}
				t.Fatalf("UPDATE monitors must not run after afterInsert failure, got SQL: %s", sql)
				return nil
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		_, err := s.recordCheckResultTx(ctx, tx, RecordCheckParams{
			MonitorID:  monitorID,
			ClaimToken: claimToken,
			CheckedAt:  now,
			Success:    true,
			LatencyMs:  10,
		}, func(ctx context.Context, _ checkTx) error {
			// Cancel caller context and return an error to verify Rollback still receives a live context.
			cancel()
			return errors.New("injected mid-tx failure")
		})
		if err == nil || !strings.Contains(err.Error(), "injected mid-tx failure") {
			t.Fatalf("expected injected hook error, got %v", err)
		}
		if tx.commitCount != 0 || tx.rollbackCnt != 1 {
			t.Fatalf("expected 0 commits and 1 rollback, got commits=%d rollbacks=%d", tx.commitCount, tx.rollbackCnt)
		}
		if tx.rollbackCtxErr != nil {
			t.Fatalf("expected Rollback to receive non-canceled context, got err=%v", tx.rollbackCtxErr)
		}
	})

	t.Run("truncates error message exceeding 1024 runes before insert", func(t *testing.T) {
		longErr := strings.Repeat("é", 1200)
		var capturedErr *string
		tx := &fakeCheckTx{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				if strings.Contains(sql, "SELECT id, name") {
					return fakeRow{scanFunc: func(dest ...any) error {
						return assignMonitorForUpdateScan(dest, model.Monitor{
							ID:              monitorID,
							Name:            "API",
							URL:             "https://example.com",
							IntervalSeconds: 60,
							TimeoutSeconds:  5,
							CurrentStatus:   model.StatusUnknown,
							NextCheckAt:     now,
							CreatedAt:       now,
							UpdatedAt:       now,
						}, &claimToken, &expiry, now)
					}}
				}
				if strings.Contains(sql, "INSERT INTO check_results") {
					capturedErr = args[6].(*string)
					return fakeRow{scanFunc: func(dest ...any) error {
						*dest[0].(*uuid.UUID) = args[0].(uuid.UUID)
						*dest[1].(*uuid.UUID) = args[1].(uuid.UUID)
						*dest[2].(*time.Time) = args[2].(time.Time)
						*dest[3].(*bool) = args[3].(bool)
						*dest[4].(**int) = args[4].(*int)
						*dest[5].(*int64) = args[5].(int64)
						*dest[6].(**string) = args[6].(*string)
						return nil
					}}
				}
				return fakeRow{scanFunc: func(dest ...any) error {
					return assignMonitorScan(dest, model.Monitor{
						ID:                  monitorID,
						Name:                "API",
						URL:                 "https://example.com",
						IntervalSeconds:     60,
						TimeoutSeconds:      5,
						CurrentStatus:       model.Status(args[1].(string)),
						ConsecutiveFailures: args[2].(int),
						LastCheckedAt:       args[3].(*time.Time),
						LastSuccessAt:       args[4].(*time.Time),
						NextCheckAt:         now.Add(60 * time.Second),
						CreatedAt:           now,
						UpdatedAt:           now,
					})
				}}
			},
		}

		out, err := s.recordCheckResultTx(context.Background(), tx, RecordCheckParams{
			MonitorID:    monitorID,
			ClaimToken:   claimToken,
			CheckedAt:    now,
			Success:      false,
			LatencyMs:    25,
			ErrorMessage: &longErr,
		}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if capturedErr == nil || len([]rune(*capturedErr)) != model.MaxErrorMessageLength {
			t.Fatalf("expected persisted error truncated to %d runes, got %d", model.MaxErrorMessageLength, len([]rune(*capturedErr)))
		}
		if !strings.HasSuffix(*capturedErr, "...") || out.CheckResult.ErrorMessage == nil {
			t.Errorf("expected '...' suffix on truncated error message")
		}
	})
}

type fakeRows struct {
	pgx.Rows
	rows      [][]any
	idx       int
	closed    bool
	err       error
	scanError error
}

func (r *fakeRows) Close()     { r.closed = true }
func (r *fakeRows) Err() error { return r.err }

func (r *fakeRows) Next() bool {
	if r.idx >= len(r.rows) {
		return false
	}
	r.idx++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.scanError != nil {
		return r.scanError
	}
	row := r.rows[r.idx-1]
	*dest[0].(*uuid.UUID) = row[0].(uuid.UUID)
	*dest[1].(*uuid.UUID) = row[1].(uuid.UUID)
	*dest[2].(*time.Time) = row[2].(time.Time)
	*dest[3].(*bool) = row[3].(bool)
	*dest[4].(**int) = row[4].(*int)
	*dest[5].(*int64) = row[5].(int64)
	*dest[6].(**string) = row[6].(*string)
	return nil
}

type fakeQuerier struct {
	queryRowFunc func(ctx context.Context, sql string, args ...any) pgx.Row
	queryFunc    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	calls        []fakeTxCall
}

func (q *fakeQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	q.calls = append(q.calls, fakeTxCall{sql: sql, args: args})
	return q.queryRowFunc(ctx, sql, args...)
}

func (q *fakeQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.calls = append(q.calls, fakeTxCall{sql: sql, args: args})
	return q.queryFunc(ctx, sql, args...)
}

func TestListChecks_NilAndBoundsValidation(t *testing.T) {
	var nilStore *Store
	if _, err := nilStore.ListChecks(context.Background(), uuid.New(), 50); err == nil {
		t.Fatal("expected error for nil store")
	}

	s := New(nil)
	if _, err := s.ListChecks(context.Background(), uuid.New(), 50); err == nil {
		t.Fatal("expected error for nil pool")
	}

	q := &fakeQuerier{}
	for _, invalidLimit := range []int{0, -1, 201} {
		if _, err := s.listChecksWithQuerier(context.Background(), q, uuid.New(), invalidLimit); err == nil {
			t.Errorf("expected error for invalid limit %d", invalidLimit)
		}
	}
	if len(q.calls) != 0 {
		t.Errorf("expected no SQL calls for invalid limits, got %d", len(q.calls))
	}
}

func TestListChecksWithQuerier_NotFoundEmptyAndOrdering(t *testing.T) {
	s := New(nil)
	monitorID := uuid.New()

	t.Run("unknown monitor returns ErrNotFound", func(t *testing.T) {
		q := &fakeQuerier{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error { return pgx.ErrNoRows }}
			},
		}

		_, err := s.listChecksWithQuerier(context.Background(), q, monitorID, 50)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
		if len(q.calls) != 1 {
			t.Errorf("expected only existence check query, got %d calls", len(q.calls))
		}
	})

	t.Run("existing monitor with empty history returns non-nil empty slice", func(t *testing.T) {
		q := &fakeQuerier{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					*dest[0].(*int) = 1
					return nil
				}}
			},
			queryFunc: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				if !strings.Contains(sql, "ORDER BY checked_at DESC") || !strings.Contains(sql, "LIMIT $2") {
					t.Errorf("unexpected SQL ordering/limit: %s", sql)
				}
				if args[0].(uuid.UUID) != monitorID || args[1].(int) != 50 {
					t.Errorf("unexpected args: %v", args)
				}
				return &fakeRows{}, nil
			},
		}

		checks, err := s.listChecksWithQuerier(context.Background(), q, monitorID, 50)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if checks == nil || len(checks) != 0 {
			t.Errorf("expected non-nil empty slice, got %v", checks)
		}
	})

	t.Run("scans rows and preserves order", func(t *testing.T) {
		t2 := time.Date(2026, 11, 25, 12, 5, 0, 0, time.UTC)
		t1 := time.Date(2026, 11, 25, 12, 4, 0, 0, time.UTC)
		status200 := 200
		status503 := 503
		err503 := "HTTP status 503 Service Unavailable"
		id2 := uuid.New()
		id1 := uuid.New()

		rows := &fakeRows{
			rows: [][]any{
				{id2, monitorID, t2, true, &status200, int64(42), (*string)(nil)},
				{id1, monitorID, t1, false, &status503, int64(85), &err503},
			},
		}

		q := &fakeQuerier{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					*dest[0].(*int) = 1
					return nil
				}}
			},
			queryFunc: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				if args[1].(int) != 2 {
					t.Errorf("expected limit 2, got %v", args[1])
				}
				return rows, nil
			},
		}

		checks, err := s.listChecksWithQuerier(context.Background(), q, monitorID, 2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !rows.closed {
			t.Errorf("expected rows to be closed")
		}
		if len(checks) != 2 {
			t.Fatalf("expected 2 checks, got %d", len(checks))
		}
		if checks[0].ID != id2 || !checks[0].Success || checks[0].ErrorMessage != nil {
			t.Errorf("unexpected first check: %+v", checks[0])
		}
		if checks[1].ID != id1 || checks[1].Success || checks[1].ErrorMessage == nil || *checks[1].ErrorMessage != err503 {
			t.Errorf("unexpected second check: %+v", checks[1])
		}
	})
}

func TestGetMonitorStats_NilAndBoundsValidation(t *testing.T) {
	var nilStore *Store
	if _, err := nilStore.GetMonitorStats(context.Background(), uuid.New(), 24); err == nil {
		t.Fatal("expected error for nil store")
	}

	s := New(nil)
	if _, err := s.GetMonitorStats(context.Background(), uuid.New(), 24); err == nil {
		t.Fatal("expected error for nil pool")
	}

	q := &fakeQuerier{}
	for _, invalidHours := range []int{0, -1, 721} {
		if _, err := s.getMonitorStatsWithQuerier(context.Background(), q, uuid.New(), invalidHours); err == nil {
			t.Errorf("expected error for invalid hours %d", invalidHours)
		}
	}
	if len(q.calls) != 0 {
		t.Errorf("expected no SQL calls for invalid hours, got %d", len(q.calls))
	}
}

func TestGetMonitorStatsWithQuerier_Cases(t *testing.T) {
	s := New(nil)
	monitorID := uuid.New()

	t.Run("unknown monitor returns ErrNotFound", func(t *testing.T) {
		q := &fakeQuerier{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error { return pgx.ErrNoRows }}
			},
		}

		_, err := s.getMonitorStatsWithQuerier(context.Background(), q, monitorID, 24)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("empty window returns nil availability and nil average_latency_ms", func(t *testing.T) {
		q := &fakeQuerier{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				if !strings.Contains(sql, "make_interval(hours => $2::integer)") {
					t.Errorf("expected bounded parameterized interval SQL, got: %s", sql)
				}
				if args[0].(uuid.UUID) != monitorID || args[1].(int) != 24 {
					t.Errorf("unexpected args: %v", args)
				}
				return fakeRow{scanFunc: func(dest ...any) error {
					*dest[0].(*uuid.UUID) = monitorID
					*dest[1].(*int64) = 0
					*dest[2].(*int64) = 0
					*dest[3].(*int64) = 0
					*dest[4].(**float64) = nil
					return nil
				}}
			},
		}

		stats, err := s.getMonitorStatsWithQuerier(context.Background(), q, monitorID, 24)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if stats.TotalChecks != 0 || stats.SuccessfulChecks != 0 || stats.FailedChecks != 0 {
			t.Errorf("expected zero counts, got %+v", stats)
		}
		if stats.Availability != nil {
			t.Errorf("expected nil availability for empty window, got %v", *stats.Availability)
		}
		if stats.AverageLatencyMs != nil {
			t.Errorf("expected nil average_latency_ms for empty window, got %v", *stats.AverageLatencyMs)
		}
	})

	t.Run("all failed checks returns 0 availability and nil average_latency_ms", func(t *testing.T) {
		q := &fakeQuerier{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					*dest[0].(*uuid.UUID) = monitorID
					*dest[1].(*int64) = 3
					*dest[2].(*int64) = 0
					*dest[3].(*int64) = 3
					*dest[4].(**float64) = nil
					return nil
				}}
			},
		}

		stats, err := s.getMonitorStatsWithQuerier(context.Background(), q, monitorID, 12)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if stats.WindowHours != 12 {
			t.Errorf("expected window_hours 12, got %d", stats.WindowHours)
		}
		if stats.Availability == nil || *stats.Availability != 0 {
			t.Errorf("expected availability 0.0, got %v", stats.Availability)
		}
		if stats.AverageLatencyMs != nil {
			t.Errorf("expected nil average_latency_ms when successful_checks == 0, got %v", *stats.AverageLatencyMs)
		}
	})

	t.Run("mixed results computes availability accurately", func(t *testing.T) {
		avg := 134.5
		q := &fakeQuerier{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					*dest[0].(*uuid.UUID) = monitorID
					*dest[1].(*int64) = 4
					*dest[2].(*int64) = 3
					*dest[3].(*int64) = 1
					*dest[4].(**float64) = &avg
					return nil
				}}
			},
		}

		stats, err := s.getMonitorStatsWithQuerier(context.Background(), q, monitorID, 48)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if stats.Availability == nil || *stats.Availability != 75.0 {
			t.Errorf("expected availability 75.0, got %v", stats.Availability)
		}
		if stats.AverageLatencyMs == nil || *stats.AverageLatencyMs != 134.5 {
			t.Errorf("expected average_latency_ms 134.5, got %v", stats.AverageLatencyMs)
		}
	})
}
