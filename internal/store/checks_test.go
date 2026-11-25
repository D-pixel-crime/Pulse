package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"pulse/internal/config"
	"pulse/internal/db"
	"pulse/internal/model"
)

func connectTestDB(t *testing.T) *db.DB {
	t.Helper()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = config.DefaultDatabaseURL
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	database, err := db.Connect(ctx, dbURL)
	if err != nil {
		t.Skipf("PostgreSQL unavailable at %s; DB-dependent test skipped (%v)", dbURL, err)
		return nil
	}
	t.Cleanup(database.Close)

	lockConn, err := database.Pool().Acquire(ctx)
	if err != nil {
		t.Fatalf("failed to acquire DB lock connection: %v", err)
	}
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_lock(70756973)`); err != nil {
		lockConn.Release()
		t.Fatalf("failed to acquire advisory lock: %v", err)
	}
	t.Cleanup(func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer unlockCancel()
		_, _ = lockConn.Exec(unlockCtx, `SELECT pg_advisory_unlock(70756973)`)
		lockConn.Release()
	})

	if err := database.RunSchema(ctx); err != nil {
		t.Fatalf("failed to run schema: %v", err)
	}

	return database
}

func assertTimeMicroEqual(t *testing.T, label string, got, want time.Time) {
	t.Helper()
	if got.Truncate(time.Microsecond).Sub(want.Truncate(time.Microsecond)).Abs() > time.Microsecond {
		t.Errorf("%s: got %v, want %v", label, got, want)
	}
}

func TestRecordCheckResult_LiveStateTransitionsAndPersistence(t *testing.T) {
	database := connectTestDB(t)
	if database == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := New(database.Pool())

	created, err := s.Create(ctx, &model.Monitor{
		Name:            "Transition Integration Monitor",
		URL:             "https://api.example.com/health",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
	})
	if err != nil {
		t.Fatalf("failed to create monitor: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = s.Delete(cleanupCtx, created.ID)
	})

	status500 := 500
	err500 := "HTTP status 500 Internal Server Error"
	status200 := 200

	claimForNextStep := func(makeDue bool) uuid.UUID {
		t.Helper()
		if makeDue {
			if _, execErr := database.Pool().Exec(ctx,
				`UPDATE monitors SET next_check_at = clock_timestamp() - INTERVAL '1 second' WHERE id = $1`,
				created.ID,
			); execErr != nil {
				t.Fatalf("failed to make monitor due for next check: %v", execErr)
			}
		}
		claimed, claimErr := s.ClaimDueMonitors(ctx, 100)
		if claimErr != nil {
			t.Fatalf("ClaimDueMonitors failed: %v", claimErr)
		}
		for _, cm := range claimed {
			if cm.Monitor != nil && cm.Monitor.ID == created.ID {
				return cm.ClaimToken
			}
		}
		t.Fatalf("expected monitor %v to be claimed", created.ID)
		return uuid.Nil
	}

	assertClaimCleared := func(label string) {
		t.Helper()
		var tok *uuid.UUID
		var exp *time.Time
		if qErr := database.Pool().QueryRow(ctx,
			`SELECT claim_token, claim_expires_at FROM monitors WHERE id = $1`, created.ID,
		).Scan(&tok, &exp); qErr != nil {
			t.Fatalf("%s: failed to inspect claim columns: %v", label, qErr)
		}
		if tok != nil || exp != nil {
			t.Errorf("%s: expected claim_token and claim_expires_at cleared, got tok=%v exp=%v", label, tok, exp)
		}
	}

	// 1. UNKNOWN -> failure 1 -> remains UNKNOWN
	var dbBefore, dbAfter time.Time
	if err := database.Pool().QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbBefore); err != nil {
		t.Fatalf("failed to query dbBefore: %v", err)
	}
	t1 := time.Now().UTC().Truncate(time.Microsecond)
	out1, err := s.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:    created.ID,
		ClaimToken:   claimForNextStep(false),
		CheckedAt:    t1,
		Success:      false,
		HTTPStatus:   &status500,
		LatencyMs:    150,
		ErrorMessage: &err500,
	})
	if err != nil {
		t.Fatalf("failure 1 failed: %v", err)
	}
	if err := database.Pool().QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbAfter); err != nil {
		t.Fatalf("failed to query dbAfter: %v", err)
	}
	assertClaimCleared("failure 1")
	if out1.Monitor.CurrentStatus != model.StatusUnknown || out1.Monitor.ConsecutiveFailures != 1 {
		t.Fatalf("failure 1: expected UNKNOWN/1, got %s/%d", out1.Monitor.CurrentStatus, out1.Monitor.ConsecutiveFailures)
	}
	if out1.Monitor.LastCheckedAt == nil {
		t.Fatal("failure 1: expected non-nil last_checked_at")
	}
	assertTimeMicroEqual(t, "failure 1 last_checked_at", *out1.Monitor.LastCheckedAt, t1)
	if out1.Monitor.LastSuccessAt != nil {
		t.Errorf("failure 1: expected nil last_success_at, got %v", out1.Monitor.LastSuccessAt)
	}
	if out1.Monitor.UpdatedAt.Before(dbBefore.Add(-time.Second)) || out1.Monitor.UpdatedAt.After(dbAfter.Add(time.Second)) {
		t.Errorf("failure 1 updated_at %v outside DB clock window [%v, %v]", out1.Monitor.UpdatedAt, dbBefore, dbAfter)
	}
	assertTimeMicroEqual(t, "failure 1 next_check_at = updated_at + interval", out1.Monitor.NextCheckAt, out1.Monitor.UpdatedAt.Add(60*time.Second))

	// 2. UNKNOWN -> failure 2 -> remains UNKNOWN
	t2 := t1.Add(60 * time.Second)
	out2, err := s.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:    created.ID,
		ClaimToken:   claimForNextStep(true),
		CheckedAt:    t2,
		Success:      false,
		HTTPStatus:   &status500,
		LatencyMs:    200,
		ErrorMessage: &err500,
	})
	if err != nil {
		t.Fatalf("failure 2 failed: %v", err)
	}
	assertClaimCleared("failure 2")
	if out2.Monitor.CurrentStatus != model.StatusUnknown || out2.Monitor.ConsecutiveFailures != 2 {
		t.Fatalf("failure 2: expected UNKNOWN/2, got %s/%d", out2.Monitor.CurrentStatus, out2.Monitor.ConsecutiveFailures)
	}

	// 3. UNKNOWN -> failure 3 -> transitions to DOWN
	t3 := t2.Add(60 * time.Second)
	out3, err := s.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:    created.ID,
		ClaimToken:   claimForNextStep(true),
		CheckedAt:    t3,
		Success:      false,
		HTTPStatus:   &status500,
		LatencyMs:    250,
		ErrorMessage: &err500,
	})
	if err != nil {
		t.Fatalf("failure 3 failed: %v", err)
	}
	assertClaimCleared("failure 3")
	if out3.Monitor.CurrentStatus != model.StatusDown || out3.Monitor.ConsecutiveFailures != 3 {
		t.Fatalf("failure 3: expected DOWN/3, got %s/%d", out3.Monitor.CurrentStatus, out3.Monitor.ConsecutiveFailures)
	}

	// 4. DOWN -> success -> transitions to UP and resets consecutive_failures
	t4 := t3.Add(60 * time.Second)
	out4, err := s.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:  created.ID,
		ClaimToken: claimForNextStep(true),
		CheckedAt:  t4,
		Success:    true,
		HTTPStatus: &status200,
		LatencyMs:  40,
	})
	if err != nil {
		t.Fatalf("success check failed: %v", err)
	}
	assertClaimCleared("success check")
	if out4.Monitor.CurrentStatus != model.StatusUp || out4.Monitor.ConsecutiveFailures != 0 {
		t.Fatalf("success check: expected UP/0, got %s/%d", out4.Monitor.CurrentStatus, out4.Monitor.ConsecutiveFailures)
	}
	if out4.Monitor.LastSuccessAt == nil {
		t.Fatal("success check: expected non-nil last_success_at")
	}
	assertTimeMicroEqual(t, "success check last_success_at", *out4.Monitor.LastSuccessAt, t4)

	// 5. UP -> failure 1 -> remains UP
	t5 := t4.Add(60 * time.Second)
	out5, err := s.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:    created.ID,
		ClaimToken:   claimForNextStep(true),
		CheckedAt:    t5,
		Success:      false,
		HTTPStatus:   &status500,
		LatencyMs:    300,
		ErrorMessage: &err500,
	})
	if err != nil {
		t.Fatalf("UP failure 1 failed: %v", err)
	}
	assertClaimCleared("UP failure 1")
	if out5.Monitor.CurrentStatus != model.StatusUp || out5.Monitor.ConsecutiveFailures != 1 {
		t.Fatalf("UP failure 1: expected UP/1, got %s/%d", out5.Monitor.CurrentStatus, out5.Monitor.ConsecutiveFailures)
	}
	assertTimeMicroEqual(t, "UP failure 1 preserved last_success_at", *out5.Monitor.LastSuccessAt, t4)

	// 6. UP -> failure 2 -> remains UP
	t6 := t5.Add(60 * time.Second)
	out6, err := s.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:    created.ID,
		ClaimToken:   claimForNextStep(true),
		CheckedAt:    t6,
		Success:      false,
		HTTPStatus:   &status500,
		LatencyMs:    310,
		ErrorMessage: &err500,
	})
	if err != nil {
		t.Fatalf("UP failure 2 failed: %v", err)
	}
	assertClaimCleared("UP failure 2")
	if out6.Monitor.CurrentStatus != model.StatusUp || out6.Monitor.ConsecutiveFailures != 2 {
		t.Fatalf("UP failure 2: expected UP/2, got %s/%d", out6.Monitor.CurrentStatus, out6.Monitor.ConsecutiveFailures)
	}
	assertTimeMicroEqual(t, "UP failure 2 preserved last_success_at", *out6.Monitor.LastSuccessAt, t4)

	// Verify 6 rows exist in check_results for this monitor
	var count int
	if err := database.Pool().QueryRow(ctx, "SELECT count(*) FROM check_results WHERE monitor_id = $1", created.ID).Scan(&count); err != nil {
		t.Fatalf("failed to count check_results: %v", err)
	}
	if count != 6 {
		t.Errorf("expected 6 check_results rows, got %d", count)
	}
}

func TestRecordCheckResult_LiveTransactionRollback(t *testing.T) {
	database := connectTestDB(t)
	if database == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := New(database.Pool())

	created, err := s.Create(ctx, &model.Monitor{
		Name:            "Rollback Integration Monitor",
		URL:             "https://api.example.com/rollback",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
	})
	if err != nil {
		t.Fatalf("failed to create monitor: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = s.Delete(cleanupCtx, created.ID)
	})

	// Test 1: Claim monitor, then INSERT check_results succeeds and a SQL failure aborts before UPDATE monitors.
	// Both the INSERT and monitor update must roll back, leaving next_check_at due and check_results empty.
	claimed, err := s.ClaimDueMonitors(ctx, 100)
	if err != nil {
		t.Fatalf("ClaimDueMonitors failed: %v", err)
	}
	var claimToken uuid.UUID
	for _, cm := range claimed {
		if cm.Monitor != nil && cm.Monitor.ID == created.ID {
			claimToken = cm.ClaimToken
			break
		}
	}
	if claimToken == uuid.Nil {
		t.Fatalf("expected monitor %v to be claimed", created.ID)
	}

	tx, err := database.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	status200 := 200
	checkedAt := time.Now().UTC().Truncate(time.Microsecond)

	_, err = s.recordCheckResultTx(ctx, tx, RecordCheckParams{
		MonitorID:  created.ID,
		ClaimToken: claimToken,
		CheckedAt:  checkedAt,
		Success:    true,
		HTTPStatus: &status200,
		LatencyMs:  100,
	}, func(ctx context.Context, tx checkTx) error {
		// Force a real PostgreSQL statement error after check_results INSERT inside the same transaction.
		_, execErr := tx.Exec(ctx, "SELECT 1 / 0")
		if execErr != nil {
			return execErr
		}
		return errors.New("forced failure after insert")
	})
	if err == nil {
		t.Fatal("expected transaction error after injected SQL failure, got nil")
	}

	var checkCount int
	if err := database.Pool().QueryRow(ctx, "SELECT count(*) FROM check_results WHERE monitor_id = $1", created.ID).Scan(&checkCount); err != nil {
		t.Fatalf("failed to query check_results count: %v", err)
	}
	if checkCount != 0 {
		t.Errorf("expected 0 check_results after rollback, got %d", checkCount)
	}

	persisted, err := s.GetByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("failed to refetch monitor: %v", err)
	}
	if persisted.CurrentStatus != model.StatusUnknown {
		t.Errorf("expected monitor status to remain UNKNOWN after rollback, got %q", persisted.CurrentStatus)
	}
	if persisted.ConsecutiveFailures != 0 {
		t.Errorf("expected consecutive_failures to remain 0 after rollback, got %d", persisted.ConsecutiveFailures)
	}
	if persisted.LastCheckedAt != nil {
		t.Errorf("expected last_checked_at to remain nil after rollback, got %v", persisted.LastCheckedAt)
	}
	if persisted.LastSuccessAt != nil {
		t.Errorf("expected last_success_at to remain nil after rollback, got %v", persisted.LastSuccessAt)
	}
	assertTimeMicroEqual(t, "next_check_at unchanged after rollback", persisted.NextCheckAt, created.NextCheckAt)

	// Test 2: Missing monitor ID returns ErrNotFound and inserts no check_results.
	missingID := uuid.New()
	_, err = s.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:  missingID,
		ClaimToken: uuid.New(),
		CheckedAt:  checkedAt,
		Success:    true,
		HTTPStatus: &status200,
		LatencyMs:  100,
	})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for unknown monitor ID, got %v", err)
	}
}

func TestRecordCheckResult_NextCheckAtUsesDBClockNotAppClock_LiveDB(t *testing.T) {
	database := connectTestDB(t)
	if database == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := New(database.Pool())

	created, err := s.Create(ctx, &model.Monitor{
		Name:            "DB Clock Schedule Monitor",
		URL:             "https://api.example.com/db-clock",
		IntervalSeconds: 45,
		TimeoutSeconds:  5,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	t.Cleanup(func() {
		cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cCancel()
		_ = s.Delete(cCtx, created.ID)
	})

	claimed, err := s.ClaimDueMonitors(ctx, 100)
	if err != nil {
		t.Fatalf("ClaimDueMonitors failed: %v", err)
	}
	var token uuid.UUID
	for _, cm := range claimed {
		if cm.Monitor != nil && cm.Monitor.ID == created.ID {
			token = cm.ClaimToken
			break
		}
	}
	if token == uuid.Nil {
		t.Fatalf("expected monitor %v to be claimed", created.ID)
	}

	var dbBefore, dbAfter time.Time
	if err := database.Pool().QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbBefore); err != nil {
		t.Fatalf("failed to query dbBefore: %v", err)
	}

	// Pass an intentionally skewed observation CheckedAt (years in the past).
	// Persistent scheduling timestamps (updated_at, next_check_at) must come from PostgreSQL clock_timestamp().
	skewedCheckedAt := time.Date(2005, 1, 2, 3, 4, 5, 0, time.UTC)
	status200 := 200
	out, err := s.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:  created.ID,
		ClaimToken: token,
		CheckedAt:  skewedCheckedAt,
		Success:    true,
		HTTPStatus: &status200,
		LatencyMs:  25,
	})
	if err != nil {
		t.Fatalf("RecordCheckResult failed: %v", err)
	}

	if err := database.Pool().QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbAfter); err != nil {
		t.Fatalf("failed to query dbAfter: %v", err)
	}

	if out.Monitor.LastCheckedAt == nil || !out.Monitor.LastCheckedAt.Equal(skewedCheckedAt) {
		t.Errorf("expected last_checked_at %v, got %v", skewedCheckedAt, out.Monitor.LastCheckedAt)
	}
	if out.Monitor.UpdatedAt.Before(dbBefore.Add(-time.Second)) || out.Monitor.UpdatedAt.After(dbAfter.Add(time.Second)) {
		t.Errorf("expected updated_at %v within PostgreSQL clock window [%v, %v]", out.Monitor.UpdatedAt, dbBefore, dbAfter)
	}
	wantNext := out.Monitor.UpdatedAt.Add(45 * time.Second)
	assertTimeMicroEqual(t, "next_check_at = updated_at + 45s", out.Monitor.NextCheckAt, wantNext)
}

func TestListChecksAndGetMonitorStats_LiveDB(t *testing.T) {
	database := connectTestDB(t)
	if database == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := New(database.Pool())

	// 1. Unknown monitor returns ErrNotFound for both endpoints.
	unknownID := uuid.New()
	if _, err := s.ListChecks(ctx, unknownID, 50); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for unknown monitor ListChecks, got %v", err)
	}
	if _, err := s.GetMonitorStats(ctx, unknownID, 24); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for unknown monitor GetMonitorStats, got %v", err)
	}

	// 2. Existing monitor with empty history/window.
	m1, err := s.Create(ctx, &model.Monitor{
		Name:            "History and Stats Live Monitor",
		URL:             "https://api.example.com/stats",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
	})
	if err != nil {
		t.Fatalf("failed to create monitor m1: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = s.Delete(cleanupCtx, m1.ID)
	})

	emptyChecks, err := s.ListChecks(ctx, m1.ID, 50)
	if err != nil {
		t.Fatalf("ListChecks on empty monitor failed: %v", err)
	}
	if emptyChecks == nil || len(emptyChecks) != 0 {
		t.Errorf("expected empty non-nil slice, got %v", emptyChecks)
	}

	emptyStats, err := s.GetMonitorStats(ctx, m1.ID, 24)
	if err != nil {
		t.Fatalf("GetMonitorStats on empty monitor failed: %v", err)
	}
	if emptyStats.TotalChecks != 0 || emptyStats.SuccessfulChecks != 0 || emptyStats.FailedChecks != 0 {
		t.Errorf("expected zero counts for empty window, got %+v", emptyStats)
	}
	if emptyStats.Availability != nil || emptyStats.AverageLatencyMs != nil {
		t.Errorf("expected nil availability and average_latency_ms for empty window, got %+v", emptyStats)
	}

	// 3. Seed mixed check_results across time windows:
	// - 30h ago: success, latency 500ms (outside 24h window, inside 48h window)
	// - 5h ago: failure, latency 4000ms (inside 24h window, outside 2h window)
	// - 30m ago: success, latency 100ms (inside 2h and 24h windows)
	// - 10m ago: failure, latency 5000ms (inside 2h and 24h windows)
	// - 5m ago: success, latency 200ms (inside 2h and 24h windows)
	now := time.Now().UTC().Truncate(time.Microsecond)
	status200 := 200
	status503 := 503
	err503 := "HTTP status 503 Service Unavailable"

	type seedRow struct {
		id         uuid.UUID
		checkedAt  time.Time
		success    bool
		httpStatus *int
		latencyMs  int64
		errMsg     *string
	}
	seeds := []seedRow{
		{uuid.New(), now.Add(-30 * time.Hour), true, &status200, 500, nil},
		{uuid.New(), now.Add(-5 * time.Hour), false, &status503, 4000, &err503},
		{uuid.New(), now.Add(-30 * time.Minute), true, &status200, 100, nil},
		{uuid.New(), now.Add(-10 * time.Minute), false, &status503, 5000, &err503},
		{uuid.New(), now.Add(-5 * time.Minute), true, &status200, 200, nil},
	}

	for _, row := range seeds {
		_, err := database.Pool().Exec(ctx,
			`INSERT INTO check_results (id, monitor_id, checked_at, success, http_status, latency_ms, error_message)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			row.id, m1.ID, row.checkedAt, row.success, row.httpStatus, row.latencyMs, row.errMsg,
		)
		if err != nil {
			t.Fatalf("failed to seed check_result: %v", err)
		}
	}

	// Verify newest-first ordering and limit enforcement.
	checksAll, err := s.ListChecks(ctx, m1.ID, 50)
	if err != nil {
		t.Fatalf("ListChecks failed: %v", err)
	}
	if len(checksAll) != 5 {
		t.Fatalf("expected 5 checks, got %d", len(checksAll))
	}
	for i := 0; i < len(checksAll); i++ {
		want := seeds[len(seeds)-1-i]
		if checksAll[i].ID != want.id {
			t.Errorf("checks[%d].ID = %v, want %v (newest first)", i, checksAll[i].ID, want.id)
		}
	}

	checksLimited, err := s.ListChecks(ctx, m1.ID, 2)
	if err != nil {
		t.Fatalf("ListChecks(limit=2) failed: %v", err)
	}
	if len(checksLimited) != 2 {
		t.Fatalf("expected 2 checks, got %d", len(checksLimited))
	}
	if checksLimited[0].ID != seeds[4].id || checksLimited[1].ID != seeds[3].id {
		t.Errorf("limit=2 did not return newest 2 checks: got %v, %v", checksLimited[0].ID, checksLimited[1].ID)
	}

	// Verify 2h window stats: 3 checks (2 success: 100ms, 200ms; 1 failure: 5000ms).
	stats2h, err := s.GetMonitorStats(ctx, m1.ID, 2)
	if err != nil {
		t.Fatalf("GetMonitorStats(2h) failed: %v", err)
	}
	if stats2h.TotalChecks != 3 || stats2h.SuccessfulChecks != 2 || stats2h.FailedChecks != 1 {
		t.Errorf("unexpected 2h counts: %+v", stats2h)
	}
	wantAvail2h := (2.0 / 3.0) * 100.0
	if stats2h.Availability == nil || (*stats2h.Availability-wantAvail2h) > 1e-9 || (wantAvail2h-*stats2h.Availability) > 1e-9 {
		t.Errorf("2h availability = %v, want %v", stats2h.Availability, wantAvail2h)
	}
	// Average latency must only include successful checks: (100 + 200) / 2 = 150.0 (excluding 5000ms failure).
	if stats2h.AverageLatencyMs == nil || *stats2h.AverageLatencyMs != 150.0 {
		t.Errorf("2h average_latency_ms = %v, want 150.0", stats2h.AverageLatencyMs)
	}

	// Verify 24h window stats: 4 checks (2 success: 100ms, 200ms; 2 failure: 4000ms, 5000ms).
	stats24h, err := s.GetMonitorStats(ctx, m1.ID, 24)
	if err != nil {
		t.Fatalf("GetMonitorStats(24h) failed: %v", err)
	}
	if stats24h.TotalChecks != 4 || stats24h.SuccessfulChecks != 2 || stats24h.FailedChecks != 2 {
		t.Errorf("unexpected 24h counts: %+v", stats24h)
	}
	if stats24h.Availability == nil || *stats24h.Availability != 50.0 {
		t.Errorf("24h availability = %v, want 50.0", stats24h.Availability)
	}
	if stats24h.AverageLatencyMs == nil || *stats24h.AverageLatencyMs != 150.0 {
		t.Errorf("24h average_latency_ms = %v, want 150.0", stats24h.AverageLatencyMs)
	}

	// Verify 48h window stats: 5 checks (3 success: 500, 100, 200 -> avg 800/3; 2 failures).
	stats48h, err := s.GetMonitorStats(ctx, m1.ID, 48)
	if err != nil {
		t.Fatalf("GetMonitorStats(48h) failed: %v", err)
	}
	if stats48h.TotalChecks != 5 || stats48h.SuccessfulChecks != 3 || stats48h.FailedChecks != 2 {
		t.Errorf("unexpected 48h counts: %+v", stats48h)
	}
	if stats48h.Availability == nil || *stats48h.Availability != 60.0 {
		t.Errorf("48h availability = %v, want 60.0", stats48h.Availability)
	}
	wantAvg48h := 800.0 / 3.0
	if stats48h.AverageLatencyMs == nil || (*stats48h.AverageLatencyMs-wantAvg48h) > 1e-9 || (wantAvg48h-*stats48h.AverageLatencyMs) > 1e-9 {
		t.Errorf("48h average_latency_ms = %v, want %v", stats48h.AverageLatencyMs, wantAvg48h)
	}

	// 4. All-failed monitor returns availability = 0 and average_latency_ms = nil.
	m2, err := s.Create(ctx, &model.Monitor{
		Name:            "All Failed Stats Monitor",
		URL:             "https://api.example.com/failing",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
	})
	if err != nil {
		t.Fatalf("failed to create monitor m2: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = s.Delete(cleanupCtx, m2.ID)
	})

	for i := 0; i < 2; i++ {
		_, err := database.Pool().Exec(ctx,
			`INSERT INTO check_results (id, monitor_id, checked_at, success, http_status, latency_ms, error_message)
			 VALUES ($1, $2, $3, false, $4, $5, $6)`,
			uuid.New(), m2.ID, now.Add(-time.Duration(i+1)*time.Minute), &status503, int64(300*(i+1)), &err503,
		)
		if err != nil {
			t.Fatalf("failed to seed failed check for m2: %v", err)
		}
	}

	allFailedStats, err := s.GetMonitorStats(ctx, m2.ID, 24)
	if err != nil {
		t.Fatalf("GetMonitorStats on all-failed monitor failed: %v", err)
	}
	if allFailedStats.TotalChecks != 2 || allFailedStats.SuccessfulChecks != 0 || allFailedStats.FailedChecks != 2 {
		t.Errorf("unexpected counts for all-failed monitor: %+v", allFailedStats)
	}
	if allFailedStats.Availability == nil || *allFailedStats.Availability != 0.0 {
		t.Errorf("expected 0.0 availability for all-failed monitor, got %v", allFailedStats.Availability)
	}
	if allFailedStats.AverageLatencyMs != nil {
		t.Errorf("expected nil average_latency_ms for all-failed monitor, got %v", *allFailedStats.AverageLatencyMs)
	}
}
