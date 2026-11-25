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

	// 1. UNKNOWN -> failure 1 -> remains UNKNOWN
	t1 := time.Now().UTC().Truncate(time.Microsecond)
	c1 := t1.Add(150 * time.Millisecond)
	out1, err := s.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:    created.ID,
		CheckedAt:    t1,
		CompletedAt:  c1,
		Success:      false,
		HTTPStatus:   &status500,
		LatencyMs:    150,
		ErrorMessage: &err500,
	})
	if err != nil {
		t.Fatalf("failure 1 failed: %v", err)
	}
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
	assertTimeMicroEqual(t, "failure 1 next_check_at", out1.Monitor.NextCheckAt, c1.Add(60*time.Second))

	// 2. UNKNOWN -> failure 2 -> remains UNKNOWN
	t2 := c1.Add(60 * time.Second)
	c2 := t2.Add(200 * time.Millisecond)
	out2, err := s.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:    created.ID,
		CheckedAt:    t2,
		CompletedAt:  c2,
		Success:      false,
		HTTPStatus:   &status500,
		LatencyMs:    200,
		ErrorMessage: &err500,
	})
	if err != nil {
		t.Fatalf("failure 2 failed: %v", err)
	}
	if out2.Monitor.CurrentStatus != model.StatusUnknown || out2.Monitor.ConsecutiveFailures != 2 {
		t.Fatalf("failure 2: expected UNKNOWN/2, got %s/%d", out2.Monitor.CurrentStatus, out2.Monitor.ConsecutiveFailures)
	}

	// 3. UNKNOWN -> failure 3 -> transitions to DOWN
	t3 := c2.Add(60 * time.Second)
	c3 := t3.Add(250 * time.Millisecond)
	out3, err := s.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:    created.ID,
		CheckedAt:    t3,
		CompletedAt:  c3,
		Success:      false,
		HTTPStatus:   &status500,
		LatencyMs:    250,
		ErrorMessage: &err500,
	})
	if err != nil {
		t.Fatalf("failure 3 failed: %v", err)
	}
	if out3.Monitor.CurrentStatus != model.StatusDown || out3.Monitor.ConsecutiveFailures != 3 {
		t.Fatalf("failure 3: expected DOWN/3, got %s/%d", out3.Monitor.CurrentStatus, out3.Monitor.ConsecutiveFailures)
	}

	// 4. DOWN -> success -> transitions to UP and resets consecutive_failures
	t4 := c3.Add(60 * time.Second)
	c4 := t4.Add(40 * time.Millisecond)
	out4, err := s.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:   created.ID,
		CheckedAt:   t4,
		CompletedAt: c4,
		Success:     true,
		HTTPStatus:  &status200,
		LatencyMs:   40,
	})
	if err != nil {
		t.Fatalf("success check failed: %v", err)
	}
	if out4.Monitor.CurrentStatus != model.StatusUp || out4.Monitor.ConsecutiveFailures != 0 {
		t.Fatalf("success check: expected UP/0, got %s/%d", out4.Monitor.CurrentStatus, out4.Monitor.ConsecutiveFailures)
	}
	if out4.Monitor.LastSuccessAt == nil {
		t.Fatal("success check: expected non-nil last_success_at")
	}
	assertTimeMicroEqual(t, "success check last_success_at", *out4.Monitor.LastSuccessAt, t4)

	// 5. UP -> failure 1 -> remains UP
	t5 := c4.Add(60 * time.Second)
	c5 := t5.Add(300 * time.Millisecond)
	out5, err := s.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:    created.ID,
		CheckedAt:    t5,
		CompletedAt:  c5,
		Success:      false,
		HTTPStatus:   &status500,
		LatencyMs:    300,
		ErrorMessage: &err500,
	})
	if err != nil {
		t.Fatalf("UP failure 1 failed: %v", err)
	}
	if out5.Monitor.CurrentStatus != model.StatusUp || out5.Monitor.ConsecutiveFailures != 1 {
		t.Fatalf("UP failure 1: expected UP/1, got %s/%d", out5.Monitor.CurrentStatus, out5.Monitor.ConsecutiveFailures)
	}
	assertTimeMicroEqual(t, "UP failure 1 preserved last_success_at", *out5.Monitor.LastSuccessAt, t4)

	// 6. UP -> failure 2 -> remains UP
	t6 := c5.Add(60 * time.Second)
	c6 := t6.Add(310 * time.Millisecond)
	out6, err := s.RecordCheckResult(ctx, RecordCheckParams{
		MonitorID:    created.ID,
		CheckedAt:    t6,
		CompletedAt:  c6,
		Success:      false,
		HTTPStatus:   &status500,
		LatencyMs:    310,
		ErrorMessage: &err500,
	})
	if err != nil {
		t.Fatalf("UP failure 2 failed: %v", err)
	}
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

	// Test 1: INSERT check_results succeeds, then a SQL failure aborts the transaction before UPDATE monitors.
	// Both the INSERT and monitor update must roll back, leaving next_check_at due and check_results empty.
	tx, err := database.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	status200 := 200
	checkedAt := time.Now().UTC().Truncate(time.Microsecond)
	completedAt := checkedAt.Add(100 * time.Millisecond)

	_, err = s.recordCheckResultTx(ctx, tx, RecordCheckParams{
		MonitorID:   created.ID,
		CheckedAt:   checkedAt,
		CompletedAt: completedAt,
		Success:     true,
		HTTPStatus:  &status200,
		LatencyMs:   100,
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
		MonitorID:   missingID,
		CheckedAt:   checkedAt,
		CompletedAt: completedAt,
		Success:     true,
		HTTPStatus:  &status200,
		LatencyMs:   100,
	})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for unknown monitor ID, got %v", err)
	}
}
