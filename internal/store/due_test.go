package store

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"pulse/internal/model"
)

func TestListDueMonitors_LiveDB(t *testing.T) {
	database := connectTestDB(t)
	if database == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s := New(database.Pool())

	// 1. Verify newly created monitor has next_check_at == created_at and is due immediately.
	fresh, err := s.Create(ctx, &model.Monitor{
		Name:            "Fresh Immediately Due Monitor",
		URL:             "https://example.com/fresh",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
	})
	if err != nil {
		t.Fatalf("failed to create fresh monitor: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = s.Delete(cleanupCtx, fresh.ID)
	})

	if !fresh.NextCheckAt.Equal(fresh.CreatedAt) {
		t.Fatalf("expected fresh next_check_at (%v) to equal created_at (%v)", fresh.NextCheckAt, fresh.CreatedAt)
	}

	dueAtCreation, err := s.ListDueMonitors(ctx, fresh.CreatedAt, DefaultDueBatchLimit)
	if err != nil {
		t.Fatalf("ListDueMonitors at creation time failed: %v", err)
	}
	foundFresh := false
	for _, m := range dueAtCreation {
		if m.ID == fresh.ID {
			foundFresh = true
			break
		}
	}
	if !foundFresh {
		t.Errorf("expected newly created monitor %v to be immediately due at %v", fresh.ID, fresh.CreatedAt)
	}

	// 2. Use an isolated historical anchor so unrelated monitors with current timestamps do not interfere.
	anchor := time.Date(2000, 1, 2, 12, 0, 0, 0, time.UTC)

	createWithSchedule := func(name string, nextCheckAt time.Time, interval int) *model.Monitor {
		t.Helper()
		m, err := s.Create(ctx, &model.Monitor{
			Name:            name,
			URL:             "https://example.com/" + name,
			IntervalSeconds: interval,
			TimeoutSeconds:  5,
		})
		if err != nil {
			t.Fatalf("failed to create monitor %s: %v", name, err)
		}
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			_ = s.Delete(cleanupCtx, m.ID)
		})

		if _, err := database.Pool().Exec(ctx,
			`UPDATE monitors SET next_check_at = $2 WHERE id = $1`,
			m.ID, nextCheckAt,
		); err != nil {
			t.Fatalf("failed to set next_check_at for %s: %v", name, err)
		}
		m.NextCheckAt = nextCheckAt
		return m
	}

	// Overdue by multiple intervals (selected once, no historical backfill).
	overdue := createWithSchedule("overdue", anchor.Add(-15*time.Minute), 30)
	// Two monitors sharing identical next_check_at to test deterministic id ASC tie-breaking.
	tieA := createWithSchedule("tie-a", anchor.Add(-1*time.Minute), 60)
	tieB := createWithSchedule("tie-b", anchor.Add(-1*time.Minute), 60)
	// Exact boundary: next_check_at == anchor -> due.
	atAnchor := createWithSchedule("boundary", anchor, 60)
	// Future: next_check_at > anchor -> not due.
	notDue := createWithSchedule("not-due", anchor.Add(1*time.Second), 60)

	tieOrdered := []*model.Monitor{tieA, tieB}
	sort.Slice(tieOrdered, func(i, j int) bool {
		return bytes.Compare(tieOrdered[i].ID[:], tieOrdered[j].ID[:]) < 0
	})

	wantOrder := []uuid.UUID{overdue.ID, tieOrdered[0].ID, tieOrdered[1].ID, atAnchor.ID}

	// 3. Verify due vs not-due filtering and deterministic ordering.
	due, err := s.ListDueMonitors(ctx, anchor, DefaultDueBatchLimit)
	if err != nil {
		t.Fatalf("ListDueMonitors failed: %v", err)
	}
	if len(due) != len(wantOrder) {
		t.Fatalf("expected %d due monitors, got %d", len(wantOrder), len(due))
	}
	for i, wantID := range wantOrder {
		if due[i].ID != wantID {
			t.Errorf("due[%d].ID = %v, want %v", i, due[i].ID, wantID)
		}
	}

	// 4. Verify batch limit.
	limited, err := s.ListDueMonitors(ctx, anchor, 2)
	if err != nil {
		t.Fatalf("ListDueMonitors(limit=2) failed: %v", err)
	}
	if len(limited) != 2 || limited[0].ID != overdue.ID || limited[1].ID != tieOrdered[0].ID {
		t.Errorf("unexpected limited batch: got %v, want [%v, %v]", limited, overdue.ID, tieOrdered[0].ID)
	}

	// Verify default (0) and capped (>100) limits work without error.
	if _, err := s.ListDueMonitors(ctx, anchor, 0); err != nil {
		t.Fatalf("ListDueMonitors(limit=0) failed: %v", err)
	}
	if _, err := s.ListDueMonitors(ctx, anchor, 250); err != nil {
		t.Fatalf("ListDueMonitors(limit=250) failed: %v", err)
	}

	// 5. Verify no mutation of next_check_at and no synthetic check_results created during discovery.
	for _, seeded := range []*model.Monitor{overdue, tieA, tieB, atAnchor, notDue} {
		fetched, err := s.GetByID(ctx, seeded.ID)
		if err != nil {
			t.Fatalf("GetByID(%v) failed: %v", seeded.ID, err)
		}
		assertTimeMicroEqual(t, "next_check_at unchanged after discovery", fetched.NextCheckAt, seeded.NextCheckAt)

		var checkCount int
		if err := database.Pool().QueryRow(ctx, `SELECT count(*) FROM check_results WHERE monitor_id = $1`, seeded.ID).Scan(&checkCount); err != nil {
			t.Fatalf("count check_results failed: %v", err)
		}
		if checkCount != 0 {
			t.Errorf("expected 0 check_results for monitor %v during discovery, got %d", seeded.ID, checkCount)
		}
	}
}
