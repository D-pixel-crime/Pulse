package scheduler_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"pulse/internal/checker"
	"pulse/internal/config"
	"pulse/internal/db"
	"pulse/internal/model"
	"pulse/internal/scheduler"
	"pulse/internal/store"
)

func TestScheduler_FreeExecutionSlotsCap(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	mon1 := newTestMonitor(now.Add(-3*time.Second), "https://example.com/slot1", 60)
	mon2 := newTestMonitor(now.Add(-2*time.Second), "https://example.com/slot2", 60)
	mon3 := newTestMonitor(now.Add(-1*time.Second), "https://example.com/slot3", 60)

	st := newFakeDueStore(mon1, mon2, mon3)
	st.clock = func() time.Time { return now }
	ordered := &orderedDueStore{fakeDueStore: st, order: []uuid.UUID{mon1.ID, mon2.ID, mon3.ID}}

	mon1Started := make(chan struct{})
	mon3Started := make(chan struct{})
	releaseBlocked := make(chan struct{})
	doneCh := make(chan uuid.UUID, 5)
	ordered.onRecordHook = func(p store.RecordCheckParams) {
		doneCh <- p.MonitorID
	}

	chk := fakeChecker{
		fn: func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
			switch target.RawURL() {
			case "https://example.com/slot1":
				close(mon1Started)
				<-releaseBlocked
			case "https://example.com/slot3":
				close(mon3Started)
				<-releaseBlocked
			}
			s := http.StatusOK
			return checker.CheckObservation{
				CheckedAt:   now,
				CompletedAt: now.Add(10 * time.Millisecond),
				Success:     true,
				HTTPStatus:  &s,
				Latency:     10 * time.Millisecond,
			}
		},
	}

	sched, err := scheduler.New(
		ordered,
		chk,
		slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		scheduler.WithWorkerCount(2),
		scheduler.WithQueueCapacity(10),
		scheduler.WithBatchLimit(100),
		scheduler.WithPollInterval(1*time.Hour),
		scheduler.WithNowFunc(func() time.Time { return now }),
	)
	if err != nil {
		t.Fatalf("scheduler.New failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- sched.Run(ctx)
	}()

	// Poll 1: 0 in-flight, 2 workers -> ClaimDueMonitors limit must be 2 (not 100).
	sched.PollOnce(ctx)
	<-mon1Started
	// Wait for mon2 to finish and release its InFlightSet slot, leaving only mon1 in-flight.
	if got := <-doneCh; got != mon2.ID {
		close(releaseBlocked)
		cancel()
		<-runDone
		t.Fatalf("expected mon2 to finish first, got %v", got)
	}
	deadline := time.Now().Add(2 * time.Second)
	for sched.InFlight().Len() != 1 {
		if time.Now().After(deadline) {
			close(releaseBlocked)
			cancel()
			<-runDone
			t.Fatalf("timed out waiting for mon2 to leave InFlightSet; len=%d", sched.InFlight().Len())
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Poll 2: 1 in-flight (mon1), 2 workers -> freeSlots=1 -> ClaimDueMonitors limit must be 1.
	sched.PollOnce(ctx)
	<-mon3Started

	// Poll 3: 2 in-flight (mon1, mon3), 2 workers -> freeSlots=0 -> ClaimDueMonitors must NOT be called.
	sched.PollOnce(ctx)

	st.mu.Lock()
	limits := append([]int(nil), st.listLimits...)
	st.mu.Unlock()

	if len(limits) != 2 || limits[0] != 2 || limits[1] != 1 {
		close(releaseBlocked)
		cancel()
		<-runDone
		t.Fatalf("expected ClaimDueMonitors limits [2, 1] with 3rd poll skipped, got %v", limits)
	}

	close(releaseBlocked)
	<-doneCh
	<-doneCh
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("scheduler.Run failed: %v", err)
	}
}

func TestScheduler_PersistenceFailureAndClaimLostReleaseBehavior(t *testing.T) {
	t.Run("generic persistence failure releases exact claim token", func(t *testing.T) {
		now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
		mon := newTestMonitor(now, "https://example.com/persist-fail", 60)
		st := newFakeDueStore(mon)
		st.clock = func() time.Time { return now }
		st.recordErr = errors.New("db connection lost")

		var claimedToken uuid.UUID
		st.onClaimHook = func(_ int, claimed []model.ClaimedMonitor) {
			if len(claimed) == 1 {
				claimedToken = claimed[0].ClaimToken
			}
		}

		releasedCh := make(chan releaseCall, 1)
		st.onReleaseHook = func(monitorID, claimToken uuid.UUID, released bool) {
			if released {
				releasedCh <- releaseCall{monitorID: monitorID, claimToken: claimToken}
			}
		}

		chk := fakeChecker{
			fn: func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
				s := http.StatusOK
				return checker.CheckObservation{
					CheckedAt:   now,
					CompletedAt: now.Add(10 * time.Millisecond),
					Success:     true,
					HTTPStatus:  &s,
					Latency:     10 * time.Millisecond,
				}
			},
		}

		sched, err := scheduler.New(
			st,
			chk,
			slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
			scheduler.WithWorkerCount(1),
			scheduler.WithQueueCapacity(5),
			scheduler.WithPollInterval(1*time.Hour),
			scheduler.WithNowFunc(func() time.Time { return now }),
		)
		if err != nil {
			t.Fatalf("scheduler.New failed: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		runDone := make(chan error, 1)
		go func() {
			runDone <- sched.Run(ctx)
		}()

		sched.PollOnce(ctx)
		select {
		case rel := <-releasedCh:
			if rel.monitorID != mon.ID || rel.claimToken != claimedToken {
				cancel()
				<-runDone
				t.Fatalf("expected release (%v, %v), got (%v, %v)", mon.ID, claimedToken, rel.monitorID, rel.claimToken)
			}
		case <-time.After(2 * time.Second):
			cancel()
			<-runDone
			t.Fatal("timed out waiting for ReleaseClaim on persistence failure")
		}

		cancel()
		if err := <-runDone; err != nil {
			t.Fatalf("scheduler.Run failed: %v", err)
		}
		snap, count, _ := st.snapshot(mon.ID)
		if count != 0 || !snap.NextCheckAt.Equal(now) {
			t.Fatalf("expected no persisted checks and unchanged next_check_at, got count=%d next=%v", count, snap.NextCheckAt)
		}
	})

	t.Run("ErrClaimLost does not call ReleaseClaim", func(t *testing.T) {
		now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
		mon := newTestMonitor(now, "https://example.com/claim-lost", 60)
		st := newFakeDueStore(mon)
		st.clock = func() time.Time { return now }
		st.recordErr = store.ErrClaimLost

		done := make(chan struct{}, 1)
		st.onRecordHook = func(store.RecordCheckParams) {
			done <- struct{}{}
		}

		chk := fakeChecker{
			fn: func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
				s := http.StatusOK
				return checker.CheckObservation{
					CheckedAt:   now,
					CompletedAt: now.Add(10 * time.Millisecond),
					Success:     true,
					HTTPStatus:  &s,
					Latency:     10 * time.Millisecond,
				}
			},
		}

		sched, err := scheduler.New(
			st,
			chk,
			slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
			scheduler.WithWorkerCount(1),
			scheduler.WithQueueCapacity(5),
			scheduler.WithPollInterval(1*time.Hour),
			scheduler.WithNowFunc(func() time.Time { return now }),
		)
		if err != nil {
			t.Fatalf("scheduler.New failed: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		runDone := make(chan error, 1)
		go func() {
			runDone <- sched.Run(ctx)
		}()

		sched.PollOnce(ctx)
		<-done
		cancel()
		if err := <-runDone; err != nil {
			t.Fatalf("scheduler.Run failed: %v", err)
		}

		st.mu.Lock()
		releases := len(st.releaseCalls)
		st.mu.Unlock()
		if releases != 0 {
			t.Fatalf("expected ReleaseClaim NOT to be called on ErrClaimLost, got %d calls", releases)
		}
	})
}

func TestScheduler_PerMonitorTimeoutPersistsFailureAndHTTPPrecedesPersistence(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	mon := newTestMonitor(now, "https://example.com/timeout", 60)
	st := newFakeDueStore(mon)
	st.clock = func() time.Time { return now }

	var events []string
	var evMu sync.Mutex
	done := make(chan struct{}, 1)
	st.onRecordHook = func(store.RecordCheckParams) {
		evMu.Lock()
		events = append(events, "record_start")
		evMu.Unlock()
		done <- struct{}{}
	}

	chk := fakeChecker{
		fn: func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
			evMu.Lock()
			events = append(events, "http_start")
			evMu.Unlock()
			errMsg := context.DeadlineExceeded.Error()
			evMu.Lock()
			events = append(events, "http_end")
			evMu.Unlock()
			return checker.CheckObservation{
				CheckedAt:    now,
				CompletedAt:  now.Add(5 * time.Second),
				Success:      false,
				Latency:      5 * time.Second,
				ErrorMessage: &errMsg,
			}
		},
	}

	sched, err := scheduler.New(
		st,
		chk,
		slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		scheduler.WithWorkerCount(1),
		scheduler.WithQueueCapacity(5),
		scheduler.WithPollInterval(1*time.Hour),
		scheduler.WithNowFunc(func() time.Time { return now }),
	)
	if err != nil {
		t.Fatalf("scheduler.New failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- sched.Run(ctx)
	}()

	sched.PollOnce(ctx)
	<-done
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("scheduler.Run failed: %v", err)
	}

	snap, count, _ := st.snapshot(mon.ID)
	if count != 1 || snap.ConsecutiveFailures != 1 {
		t.Fatalf("expected per-monitor timeout to record unhealthy failure, got count=%d failures=%d", count, snap.ConsecutiveFailures)
	}
	evMu.Lock()
	defer evMu.Unlock()
	if len(events) != 3 || events[0] != "http_start" || events[1] != "http_end" || events[2] != "record_start" {
		t.Fatalf("expected HTTP check to complete before persistence begins, got order %v", events)
	}
}

func TestScheduler_TwoSchedulersConcurrentClaim_LiveDB(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = config.DefaultDatabaseURL
	}

	connCtx, connCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer connCancel()

	dbA, err := db.Connect(connCtx, dbURL)
	if err != nil {
		t.Skipf("PostgreSQL unavailable at %s; live two-scheduler test skipped (%v)", dbURL, err)
		return
	}
	t.Cleanup(dbA.Close)

	lockConn, err := dbA.Pool().Acquire(connCtx)
	if err != nil {
		t.Fatalf("failed to acquire DB lock connection: %v", err)
	}
	if _, err := lockConn.Exec(connCtx, `SELECT pg_advisory_lock(70756973)`); err != nil {
		lockConn.Release()
		t.Fatalf("failed to acquire advisory lock: %v", err)
	}
	t.Cleanup(func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer unlockCancel()
		_, _ = lockConn.Exec(unlockCtx, `SELECT pg_advisory_unlock(70756973)`)
		lockConn.Release()
	})

	if err := dbA.RunSchema(connCtx); err != nil {
		t.Fatalf("RunSchema failed: %v", err)
	}

	dbB, err := db.Connect(connCtx, dbURL)
	if err != nil {
		t.Fatalf("second db.Connect failed: %v", err)
	}
	t.Cleanup(dbB.Close)

	storeA := store.New(dbA.Pool())
	storeB := store.New(dbB.Pool())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Seed 4 monitors that are immediately due and use a 120s interval so they run once in this test.
	const totalMonitors = 4
	seededIDs := make([]uuid.UUID, 0, totalMonitors)
	seededByURL := make(map[string]uuid.UUID, totalMonitors)
	for i := 0; i < totalMonitors; i++ {
		rawURL := "https://example.com/multi-scheduler-" + uuid.New().String()
		m, err := storeA.Create(ctx, &model.Monitor{
			Name:            "Multi Scheduler Monitor",
			URL:             rawURL,
			IntervalSeconds: 120,
			TimeoutSeconds:  5,
		})
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		seededIDs = append(seededIDs, m.ID)
		seededByURL[rawURL] = m.ID
		t.Cleanup(func() {
			cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cCancel()
			_ = storeA.Delete(cCtx, m.ID)
		})
	}

	var mu sync.Mutex
	checkCallsByID := make(map[uuid.UUID]int)
	var totalPersisted atomic.Int32
	allPersisted := make(chan struct{})
	var closeOnce sync.Once

	makeChecker := func() scheduler.HealthChecker {
		return fakeChecker{
			fn: func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
				id, ok := seededByURL[target.RawURL()]
				if ok {
					mu.Lock()
					checkCallsByID[id]++
					mu.Unlock()
				}
				// Verify no DB row lock blocks reading the monitor while HTTP check is in flight.
				fetched, err := storeA.GetByID(ctx, id)
				if err != nil || fetched == nil {
					t.Errorf("GetByID during HTTP check failed: %v", err)
				}
				now := time.Now().UTC()
				s := http.StatusOK
				return checker.CheckObservation{
					CheckedAt:   now,
					CompletedAt: now.Add(5 * time.Millisecond),
					Success:     true,
					HTTPStatus:  &s,
					Latency:     5 * time.Millisecond,
				}
			},
		}
	}

	schedA, err := scheduler.New(
		storeA,
		makeChecker(),
		slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		scheduler.WithWorkerCount(2),
		scheduler.WithQueueCapacity(10),
		scheduler.WithPollInterval(25*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("schedA New failed: %v", err)
	}
	schedB, err := scheduler.New(
		storeB,
		makeChecker(),
		slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		scheduler.WithWorkerCount(2),
		scheduler.WithQueueCapacity(10),
		scheduler.WithPollInterval(25*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("schedB New failed: %v", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	go func() { doneA <- schedA.Run(runCtx) }()
	go func() { doneB <- schedB.Run(runCtx) }()

	// Poll PostgreSQL check_results until all 4 seeded monitors have been persisted.
	go func() {
		for {
			if runCtx.Err() != nil {
				return
			}
			var count int
			err := dbA.Pool().QueryRow(runCtx,
				`SELECT count(*) FROM check_results WHERE monitor_id = ANY($1)`,
				seededIDs,
			).Scan(&count)
			if err == nil && count >= totalMonitors {
				totalPersisted.Store(int32(count))
				closeOnce.Do(func() { close(allPersisted) })
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	select {
	case <-allPersisted:
	case <-time.After(10 * time.Second):
		runCancel()
		<-doneA
		<-doneB
		t.Fatal("timed out waiting for two schedulers to check and persist all monitors")
	}

	// Allow one extra poll cycle to confirm no duplicate execution occurs after persistence.
	time.Sleep(60 * time.Millisecond)
	runCancel()
	if err := <-doneA; err != nil {
		t.Fatalf("schedA Run failed: %v", err)
	}
	if err := <-doneB; err != nil {
		t.Fatalf("schedB Run failed: %v", err)
	}

	if got := totalPersisted.Load(); got != totalMonitors {
		t.Fatalf("expected %d total persisted checks, got %d", totalMonitors, got)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, id := range seededIDs {
		if checkCallsByID[id] != 1 {
			t.Errorf("expected monitor %v checked exactly once across both schedulers, got %d", id, checkCallsByID[id])
		}
	}
}
