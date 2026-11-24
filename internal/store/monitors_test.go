package store_test

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
	"pulse/internal/store"
)

func TestStore_NilMonitor(t *testing.T) {
	s := store.New(nil)
	_, err := s.Create(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error when creating nil monitor, got nil")
	}
}

func TestStore_ErrNotFound(t *testing.T) {
	if store.ErrNotFound == nil {
		t.Fatal("expected ErrNotFound to be defined")
	}
	if store.ErrNotFound.Error() != "monitor not found" {
		t.Errorf("expected error message 'monitor not found', got %q", store.ErrNotFound.Error())
	}
}

func TestStore_LiveIntegration(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = config.DefaultDatabaseURL
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	database, err := db.Connect(ctx, dbURL)
	if err != nil {
		t.Skipf("PostgreSQL unavailable at %s; live store integration test skipped (%v)", dbURL, err)
		return
	}
	defer database.Close()

	if err := database.RunSchema(ctx); err != nil {
		t.Fatalf("failed to run schema: %v", err)
	}

	s := store.New(database.Pool())

	// 1. Test Create Monitor
	monitorName := "Integration Gateway Monitor"
	monitorURL := "https://api.example.com/health"
	created, err := s.Create(ctx, &model.Monitor{
		Name:            monitorName,
		URL:             monitorURL,
		IntervalSeconds: 45,
		TimeoutSeconds:  3,
	})
	if err != nil {
		t.Fatalf("failed to create monitor: %v", err)
	}

	if created.ID == uuid.Nil {
		t.Errorf("expected non-nil UUID for created monitor")
	}
	if created.Name != monitorName {
		t.Errorf("expected name %q, got %q", monitorName, created.Name)
	}
	if created.URL != monitorURL {
		t.Errorf("expected URL %q, got %q", monitorURL, created.URL)
	}
	if created.IntervalSeconds != 45 {
		t.Errorf("expected interval 45, got %d", created.IntervalSeconds)
	}
	if created.TimeoutSeconds != 3 {
		t.Errorf("expected timeout 3, got %d", created.TimeoutSeconds)
	}
	if created.CurrentStatus != model.StatusUnknown {
		t.Errorf("expected status UNKNOWN, got %q", created.CurrentStatus)
	}
	if created.ConsecutiveFailures != 0 {
		t.Errorf("expected failures 0, got %d", created.ConsecutiveFailures)
	}
	if created.LastCheckedAt != nil {
		t.Errorf("expected nil last_checked_at, got %v", created.LastCheckedAt)
	}
	if created.LastSuccessAt != nil {
		t.Errorf("expected nil last_success_at, got %v", created.LastSuccessAt)
	}

	// next_check_at must equal created_at so monitor is due immediately
	if created.NextCheckAt.IsZero() {
		t.Error("expected non-zero next_check_at")
	}
	if created.CreatedAt.IsZero() {
		t.Error("expected non-zero created_at")
	}
	if !created.NextCheckAt.Equal(created.CreatedAt) {
		t.Errorf("expected next_check_at (%v) to equal created_at (%v)", created.NextCheckAt, created.CreatedAt)
	}

	// 2. Test GetByID
	fetched, err := s.GetByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("failed to get monitor by ID: %v", err)
	}
	if fetched.ID != created.ID {
		t.Errorf("expected ID %v, got %v", created.ID, fetched.ID)
	}
	if fetched.Name != monitorName {
		t.Errorf("expected name %q, got %q", monitorName, fetched.Name)
	}
	if fetched.CurrentStatus != model.StatusUnknown {
		t.Errorf("expected status UNKNOWN, got %q", fetched.CurrentStatus)
	}

	// 3. Test GetByID with non-existent UUID -> ErrNotFound
	nonExistentID := uuid.New()
	_, err = s.GetByID(ctx, nonExistentID)
	if err == nil {
		t.Fatal("expected ErrNotFound for non-existent monitor, got nil")
	}
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected errors.Is(err, store.ErrNotFound), got %v", err)
	}

	// 4. Test List Monitors
	monitors, err := s.List(ctx)
	if err != nil {
		t.Fatalf("failed to list monitors: %v", err)
	}
	found := false
	for _, m := range monitors {
		if m.ID == created.ID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("created monitor %v not found in list output (%d items)", created.ID, len(monitors))
	}

	// 5. Test Cascade Deletion:
	// Insert a record into check_results referencing created.ID
	checkID := uuid.New()
	_, err = database.Pool().Exec(ctx,
		`INSERT INTO check_results (id, monitor_id, checked_at, success, http_status, latency_ms)
		 VALUES ($1, $2, NOW(), true, 200, 120)`,
		checkID, created.ID,
	)
	if err != nil {
		t.Fatalf("failed to insert check_result for cascade test: %v", err)
	}

	// Delete the monitor
	if err := s.Delete(ctx, created.ID); err != nil {
		t.Fatalf("failed to delete monitor: %v", err)
	}

	// Verify monitor is gone
	_, err = s.GetByID(ctx, created.ID)
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound after deletion, got %v", err)
	}

	// Verify check_result was cascade-deleted
	var count int
	err = database.Pool().QueryRow(ctx, "SELECT count(*) FROM check_results WHERE id = $1", checkID).Scan(&count)
	if err != nil {
		t.Fatalf("failed to check check_results table: %v", err)
	}
	if count != 0 {
		t.Errorf("expected check_result to be cascade-deleted, found count = %d", count)
	}

	// 6. Test Delete non-existent ID -> ErrNotFound
	if err := s.Delete(ctx, created.ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound when deleting non-existent monitor, got %v", err)
	}
}
