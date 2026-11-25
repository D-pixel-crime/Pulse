package scheduler_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"pulse/internal/checker"
	"pulse/internal/model"
	"pulse/internal/scheduler"
	"pulse/internal/store"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type fakeDueStore struct {
	mu           sync.Mutex
	monitors     map[uuid.UUID]*model.Monitor
	checks       map[uuid.UUID][]*model.CheckResult
	listErr      error
	recordErr    error
	recordCalls  int
	listCalls    int
	onRecordHook func(params store.RecordCheckParams)
}

func newFakeDueStore(monitors ...*model.Monitor) *fakeDueStore {
	m := make(map[uuid.UUID]*model.Monitor, len(monitors))
	for _, mon := range monitors {
		cp := *mon
		m[mon.ID] = &cp
	}
	return &fakeDueStore{
		monitors: m,
		checks:   make(map[uuid.UUID][]*model.CheckResult),
	}
}

func (f *fakeDueStore) ListDueMonitors(ctx context.Context, now time.Time, limit int) ([]*model.Monitor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.listCalls++
	if f.listErr != nil {
		return nil, f.listErr
	}

	var due []*model.Monitor
	for _, mon := range f.monitors {
		if !mon.NextCheckAt.After(now) {
			cp := *mon
			due = append(due, &cp)
			if len(due) >= limit {
				break
			}
		}
	}
	return due, nil
}

func (f *fakeDueStore) RecordCheckResult(ctx context.Context, params store.RecordCheckParams) (*store.RecordCheckOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.recordCalls++
	if f.onRecordHook != nil {
		f.onRecordHook(params)
	}
	if f.recordErr != nil {
		return nil, f.recordErr
	}

	mon, ok := f.monitors[params.MonitorID]
	if !ok {
		return nil, store.ErrNotFound
	}

	nextState, err := model.TransitionMonitorHealth(
		mon.CurrentStatus,
		mon.ConsecutiveFailures,
		mon.LastSuccessAt,
		mon.IntervalSeconds,
		params.Success,
		params.CheckedAt,
		params.CompletedAt,
	)
	if err != nil {
		return nil, err
	}

	mon.CurrentStatus = nextState.CurrentStatus
	mon.ConsecutiveFailures = nextState.ConsecutiveFailures
	mon.LastCheckedAt = nextState.LastCheckedAt
	mon.LastSuccessAt = nextState.LastSuccessAt
	mon.NextCheckAt = nextState.NextCheckAt
	mon.UpdatedAt = nextState.UpdatedAt

	cr := &model.CheckResult{
		ID:           uuid.New(),
		MonitorID:    params.MonitorID,
		CheckedAt:    params.CheckedAt.UTC(),
		Success:      params.Success,
		HTTPStatus:   params.HTTPStatus,
		LatencyMs:    params.LatencyMs,
		ErrorMessage: params.ErrorMessage,
	}
	f.checks[params.MonitorID] = append(f.checks[params.MonitorID], cr)

	monCopy := *mon
	crCopy := *cr
	return &store.RecordCheckOutput{
		CheckResult: &crCopy,
		Monitor:     &monCopy,
	}, nil
}

func (f *fakeDueStore) snapshot(id uuid.UUID) (model.Monitor, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mon := *f.monitors[id]
	return mon, len(f.checks[id]), f.recordCalls
}

type fakeChecker struct {
	fn func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation
}

func (c fakeChecker) Check(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
	return c.fn(ctx, target, timeout)
}

func newTestMonitor(now time.Time, rawURL string, intervalSec int) *model.Monitor {
	return &model.Monitor{
		ID:              uuid.New(),
		Name:            "Test Monitor",
		URL:             rawURL,
		IntervalSeconds: intervalSec,
		TimeoutSeconds:  5,
		CurrentStatus:   model.StatusUnknown,
		NextCheckAt:     now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}

func TestScheduler_DuplicateInFlightSuppression(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	mon := newTestMonitor(now, "https://example.com/health", 60)
	st := newFakeDueStore(mon)

	checkStarted := make(chan struct{}, 5)
	releaseCheck := make(chan struct{})
	var checkCount atomic.Int32
	persisted := make(chan struct{}, 5)

	st.onRecordHook = func(store.RecordCheckParams) {
		select {
		case persisted <- struct{}{}:
		default:
		}
	}

	chk := fakeChecker{
		fn: func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
			checkCount.Add(1)
			checkStarted <- struct{}{}
			<-releaseCheck
			status := http.StatusOK
			return checker.CheckObservation{
				CheckedAt:   now,
				CompletedAt: now.Add(50 * time.Millisecond),
				Success:     true,
				HTTPStatus:  &status,
				Latency:     50 * time.Millisecond,
			}
		},
	}

	sched, err := scheduler.New(
		st,
		chk,
		slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		scheduler.WithWorkerCount(2),
		scheduler.WithQueueCapacity(10),
		scheduler.WithPollInterval(15*time.Millisecond),
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

	// Wait for the first check to begin.
	<-checkStarted

	// Wait until multiple poll cycles have actually executed while the first check is blocked.
	deadline := time.Now().Add(2 * time.Second)
	for {
		st.mu.Lock()
		polls := st.listCalls
		st.mu.Unlock()
		if polls >= 4 {
			break
		}
		if time.Now().After(deadline) {
			close(releaseCheck)
			cancel()
			<-runDone
			t.Fatalf("timed out waiting for multiple poll cycles; got %d", polls)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := checkCount.Load(); got != 1 {
		close(releaseCheck)
		cancel()
		<-runDone
		t.Fatalf("expected duplicate in-flight check to be suppressed (count=1), got %d", got)
	}
	if !sched.InFlight().Contains(mon.ID) {
		close(releaseCheck)
		cancel()
		<-runDone
		t.Fatal("expected monitor ID to remain in InFlightSet while check is executing")
	}

	// Release the blocked check and wait for persistence.
	close(releaseCheck)
	<-persisted

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("scheduler.Run returned error: %v", err)
	}

	if sched.InFlight().Len() != 0 {
		t.Fatalf("expected InFlightSet empty after worker completion, got %d", sched.InFlight().Len())
	}
	if got := checkCount.Load(); got != 1 {
		t.Fatalf("expected total checks=1 after rescheduling into future, got %d", got)
	}
}

func TestScheduler_QueueFullReleasesInFlightAndRetriesLater(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	mon1 := newTestMonitor(now.Add(-3*time.Second), "https://example.com/1", 60)
	mon2 := newTestMonitor(now.Add(-2*time.Second), "https://example.com/2", 60)
	mon3 := newTestMonitor(now.Add(-1*time.Second), "https://example.com/3", 60)
	st := newFakeDueStore(mon1, mon2, mon3)

	// Deterministic FIFO order via custom ListDueMonitors wrapper.
	orderedStore := &orderedDueStore{
		fakeDueStore: st,
		order:        []uuid.UUID{mon1.ID, mon2.ID, mon3.ID},
	}

	firstStarted := make(chan uuid.UUID, 5)
	releaseFirst := make(chan struct{})
	allDone := make(chan uuid.UUID, 10)

	orderedStore.onRecordHook = func(p store.RecordCheckParams) {
		allDone <- p.MonitorID
	}

	chk := fakeChecker{
		fn: func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
			if strings.HasSuffix(target.RawURL(), "/1") {
				firstStarted <- mon1.ID
				<-releaseFirst
			}
			status := http.StatusOK
			return checker.CheckObservation{
				CheckedAt:   now,
				CompletedAt: now.Add(10 * time.Millisecond),
				Success:     true,
				HTTPStatus:  &status,
				Latency:     10 * time.Millisecond,
			}
		},
	}

	var logBuf syncBuffer
	sched, err := scheduler.New(
		orderedStore,
		chk,
		slog.New(slog.NewJSONHandler(&logBuf, nil)),
		scheduler.WithWorkerCount(1),
		scheduler.WithQueueCapacity(1),
		scheduler.WithPollInterval(15*time.Millisecond),
		scheduler.WithNowFunc(func() time.Time { return now }),
	)
	if err != nil {
		t.Fatalf("scheduler.New failed: %v", err)
	}

	// Start scheduler with a long-ish poll interval first? No: 15ms poll interval, or test PollOnce?
	// Let's use a 1h ticker in Run? No, let's let Run tick with 20ms and verify mon3 was dropped on tick 1
	// and picked up on tick N after release!
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- sched.Run(ctx)
	}()

	// Wait until worker is busy with mon1 (and queue cap 1 holds mon2, so mon3 hit queue full).
	<-firstStarted

	// Poll a couple ticks while mon1 is still blocked to make sure queue-full path triggers and releases mon3.
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(logBuf.String(), "work queue full") {
		if time.Now().After(deadline) {
			close(releaseFirst)
			cancel()
			<-runDone
			t.Fatalf("expected queue-full warning log, got: %s", logBuf.String())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// mon3 was rejected from full queue, so it must NOT remain stuck in InFlightSet and its NextCheckAt must stay due.
	if sched.InFlight().Contains(mon3.ID) {
		close(releaseFirst)
		cancel()
		<-runDone
		t.Fatal("expected mon3 in-flight marker to be released immediately when queue was full")
	}
	mon3Snap, mon3Checks, _ := st.snapshot(mon3.ID)
	if mon3Checks != 0 {
		close(releaseFirst)
		cancel()
		<-runDone
		t.Fatalf("expected 0 checks persisted for mon3 while blocked, got %d", mon3Checks)
	}
	if !mon3Snap.NextCheckAt.Equal(now.Add(-1 * time.Second)) {
		close(releaseFirst)
		cancel()
		<-runDone
		t.Fatalf("expected mon3 next_check_at unchanged, got %v", mon3Snap.NextCheckAt)
	}

	// Now unblock mon1; mon2 and mon3 should both complete via retry on subsequent tick.
	close(releaseFirst)

	seen := map[uuid.UUID]bool{}
	for len(seen) < 3 {
		select {
		case id := <-allDone:
			seen[id] = true
		case <-time.After(3 * time.Second):
			cancel()
			<-runDone
			t.Fatalf("timed out waiting for all 3 monitors to complete; seen=%v", seen)
		}
	}

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("scheduler.Run failed: %v", err)
	}

	if sched.InFlight().Len() != 0 {
		t.Fatalf("expected InFlightSet empty, got %d", sched.InFlight().Len())
	}
}

type orderedDueStore struct {
	*fakeDueStore
	order []uuid.UUID
}

func (o *orderedDueStore) ListDueMonitors(ctx context.Context, now time.Time, limit int) ([]*model.Monitor, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.listCalls++
	var due []*model.Monitor
	for _, id := range o.order {
		mon := o.monitors[id]
		if mon != nil && !mon.NextCheckAt.After(now) {
			cp := *mon
			due = append(due, &cp)
			if len(due) >= limit {
				break
			}
		}
	}
	return due, nil
}

func TestScheduler_WorkerResultFlow_SuccessDownAndDueRescheduling(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	var currentNow atomic.Value
	currentNow.Store(now)

	// Monitor overdue by 2 hours: must be checked ONCE on restart, not replayed!
	mon := newTestMonitor(now.Add(-2*time.Hour), "https://example.com/api", 60)
	st := newFakeDueStore(mon)

	var nextHealthy atomic.Bool
	nextHealthy.Store(false)

	doneCh := make(chan struct{}, 10)
	st.onRecordHook = func(store.RecordCheckParams) {
		doneCh <- struct{}{}
	}

	chk := fakeChecker{
		fn: func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
			tNow := currentNow.Load().(time.Time)
			if nextHealthy.Load() {
				s := http.StatusOK
				return checker.CheckObservation{
					CheckedAt:   tNow,
					CompletedAt: tNow.Add(25 * time.Millisecond),
					Success:     true,
					HTTPStatus:  &s,
					Latency:     25 * time.Millisecond,
				}
			}
			s := http.StatusServiceUnavailable
			errMsg := "HTTP status 503 Service Unavailable"
			return checker.CheckObservation{
				CheckedAt:    tNow,
				CompletedAt:  tNow.Add(30 * time.Millisecond),
				Success:      false,
				HTTPStatus:   &s,
				Latency:      30 * time.Millisecond,
				ErrorMessage: &errMsg,
			}
		},
	}

	sched, err := scheduler.New(
		st,
		chk,
		slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		scheduler.WithWorkerCount(2),
		scheduler.WithQueueCapacity(10),
		scheduler.WithPollInterval(15*time.Millisecond),
		scheduler.WithNowFunc(func() time.Time { return currentNow.Load().(time.Time) }),
	)
	if err != nil {
		t.Fatalf("scheduler.New failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- sched.Run(ctx)
	}()

	// 1st check (overdue by 2h): runs ONCE, fails -> status remains UNKNOWN, failures=1, next_check_at = now + 30ms + 60s
	<-doneCh
	time.Sleep(40 * time.Millisecond) // verify no replay occurs while currentNow hasn't reached next_check_at
	snap, count, _ := st.snapshot(mon.ID)
	if count != 1 {
		cancel()
		<-runDone
		t.Fatalf("expected overdue monitor to be checked once (no replay), got %d checks", count)
	}
	if snap.CurrentStatus != model.StatusUnknown || snap.ConsecutiveFailures != 1 {
		cancel()
		<-runDone
		t.Fatalf("expected UNKNOWN with 1 failure, got status=%s failures=%d", snap.CurrentStatus, snap.ConsecutiveFailures)
	}
	wantNext := now.Add(30*time.Millisecond + 60*time.Second)
	if !snap.NextCheckAt.Equal(wantNext) {
		cancel()
		<-runDone
		t.Fatalf("expected next_check_at=%v, got %v", wantNext, snap.NextCheckAt)
	}

	// Advance clock past next_check_at for 2nd and 3rd failures -> transitions to DOWN on 3rd.
	currentNow.Store(snap.NextCheckAt)
	<-doneCh
	snap, count, _ = st.snapshot(mon.ID)
	if count != 2 || snap.CurrentStatus != model.StatusUnknown || snap.ConsecutiveFailures != 2 {
		cancel()
		<-runDone
		t.Fatalf("after 2nd failure: count=%d status=%s failures=%d", count, snap.CurrentStatus, snap.ConsecutiveFailures)
	}

	currentNow.Store(snap.NextCheckAt)
	<-doneCh
	snap, count, _ = st.snapshot(mon.ID)
	if count != 3 || snap.CurrentStatus != model.StatusDown || snap.ConsecutiveFailures != 3 {
		cancel()
		<-runDone
		t.Fatalf("after 3rd failure: count=%d status=%s failures=%d", count, snap.CurrentStatus, snap.ConsecutiveFailures)
	}

	// Now become healthy -> transitions immediately to UP and resets failures to 0.
	nextHealthy.Store(true)
	currentNow.Store(snap.NextCheckAt)
	<-doneCh
	snap, count, _ = st.snapshot(mon.ID)
	if count != 4 || snap.CurrentStatus != model.StatusUp || snap.ConsecutiveFailures != 0 {
		cancel()
		<-runDone
		t.Fatalf("after recovery: count=%d status=%s failures=%d", count, snap.CurrentStatus, snap.ConsecutiveFailures)
	}

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("scheduler.Run failed: %v", err)
	}
}

func TestScheduler_PersistenceFailureLeavesDueAndLogsSanitized(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	// Include query secret in URL to ensure it is NEVER logged on persistence error.
	mon := newTestMonitor(now, "https://example.com/health?token=supersecret123", 60)
	st := newFakeDueStore(mon)
	st.recordErr = errors.New("pq: transaction commit failed")

	attemptCh := make(chan struct{}, 10)
	st.onRecordHook = func(store.RecordCheckParams) {
		attemptCh <- struct{}{}
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

	var logBuf syncBuffer
	sched, err := scheduler.New(
		st,
		chk,
		slog.New(slog.NewJSONHandler(&logBuf, nil)),
		scheduler.WithWorkerCount(1),
		scheduler.WithQueueCapacity(10),
		scheduler.WithPollInterval(15*time.Millisecond),
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

	// Wait for initial persistence failure and at least one automatic retry on a later tick.
	<-attemptCh
	<-attemptCh

	// Verify next_check_at stayed due while failing.
	snap, count, _ := st.snapshot(mon.ID)
	if count != 0 {
		cancel()
		<-runDone
		t.Fatalf("expected 0 persisted checks while failing, got %d", count)
	}
	if !snap.NextCheckAt.Equal(now) {
		cancel()
		<-runDone
		t.Fatalf("expected next_check_at unchanged (%v), got %v", now, snap.NextCheckAt)
	}

	// Clear DB error and verify subsequent retry succeeds.
	st.mu.Lock()
	st.recordErr = nil
	st.mu.Unlock()

	deadline := time.Now().Add(2 * time.Second)
	for {
		snap, count, _ = st.snapshot(mon.ID)
		if count == 1 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-runDone
			t.Fatalf("timed out waiting for retry to succeed after clearing persistence error")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("scheduler.Run failed: %v", err)
	}

	if snap.CurrentStatus != model.StatusUp {
		t.Errorf("expected UP after retry succeeded, got %s", snap.CurrentStatus)
	}
	if !snap.NextCheckAt.After(now) {
		t.Errorf("expected next_check_at to advance after commit, got %v", snap.NextCheckAt)
	}

	logs := logBuf.String()
	if strings.Contains(logs, "supersecret123") || strings.Contains(logs, "?token=") {
		t.Errorf("logs must not contain query string parameters: %s", logs)
	}
	if !strings.Contains(logs, "https://example.com/health") {
		t.Errorf("expected sanitized URL in logs, got: %s", logs)
	}
}

func TestScheduler_GracefulShutdownAndBoundedTimeout(t *testing.T) {
	t.Run("allows in-progress check to commit within shutdown timeout", func(t *testing.T) {
		now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
		mon := newTestMonitor(now, "https://example.com/slow", 60)
		st := newFakeDueStore(mon)

		checkStarted := make(chan struct{})
		releaseCheck := make(chan struct{})

		chk := fakeChecker{
			fn: func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
				close(checkStarted)
				<-releaseCheck
				s := http.StatusOK
				return checker.CheckObservation{
					CheckedAt:   now,
					CompletedAt: now.Add(20 * time.Millisecond),
					Success:     true,
					HTTPStatus:  &s,
					Latency:     20 * time.Millisecond,
				}
			},
		}

		sched, err := scheduler.New(
			st,
			chk,
			slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
			scheduler.WithWorkerCount(2),
			scheduler.WithQueueCapacity(10),
			scheduler.WithPollInterval(10*time.Millisecond),
			scheduler.WithShutdownTimeout(2*time.Second),
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

		<-checkStarted
		// Trigger shutdown while check is in-progress.
		cancel()

		// Release check after shutdown begins; it must finish and commit cleanly.
		close(releaseCheck)

		select {
		case err := <-runDone:
			if err != nil {
				t.Fatalf("expected clean shutdown, got %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for graceful shutdown")
		}

		snap, count, _ := st.snapshot(mon.ID)
		if count != 1 || snap.CurrentStatus != model.StatusUp {
			t.Fatalf("expected in-flight check to persist before exit: count=%d status=%s", count, snap.CurrentStatus)
		}
		if sched.InFlight().Len() != 0 {
			t.Fatalf("expected InFlightSet empty after shutdown, got %d", sched.InFlight().Len())
		}
	})

	t.Run("cancels hung check when shutdown timeout expires", func(t *testing.T) {
		now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
		mon := newTestMonitor(now, "https://example.com/hung", 60)
		st := newFakeDueStore(mon)

		checkStarted := make(chan struct{})
		chk := fakeChecker{
			fn: func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
				close(checkStarted)
				<-ctx.Done()
				errMsg := ctx.Err().Error()
				return checker.CheckObservation{
					CheckedAt:    now,
					CompletedAt:  now,
					Success:      false,
					ErrorMessage: &errMsg,
				}
			},
		}

		sched, err := scheduler.New(
			st,
			chk,
			slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
			scheduler.WithWorkerCount(1),
			scheduler.WithQueueCapacity(10),
			scheduler.WithPollInterval(10*time.Millisecond),
			scheduler.WithShutdownTimeout(50*time.Millisecond),
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

		<-checkStarted
		cancel()

		select {
		case err := <-runDone:
			if err != nil {
				t.Fatalf("expected nil error on bounded shutdown timeout, got %v", err)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("scheduler.Run blocked past bounded shutdown timeout")
		}

		// Aborted check must not persist a synthetic failure; monitor stays due on restart.
		snap, count, _ := st.snapshot(mon.ID)
		if count != 0 || !snap.NextCheckAt.Equal(now) {
			t.Fatalf("expected aborted check not to persist or advance next_check_at: count=%d next=%v", count, snap.NextCheckAt)
		}
		if sched.InFlight().Len() != 0 {
			t.Fatalf("expected InFlightSet empty, got %d", sched.InFlight().Len())
		}
	})

	t.Run("drains buffered queued tasks on shutdown within timeout", func(t *testing.T) {
		now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
		mon1 := newTestMonitor(now.Add(-2*time.Second), "https://example.com/drain1", 60)
		mon2 := newTestMonitor(now.Add(-1*time.Second), "https://example.com/drain2", 60)
		st := newFakeDueStore(mon1, mon2)
		ordered := &orderedDueStore{fakeDueStore: st, order: []uuid.UUID{mon1.ID, mon2.ID}}

		firstStarted := make(chan struct{})
		releaseFirst := make(chan struct{})
		var startedOnce sync.Once
		persistedCh := make(chan uuid.UUID, 5)
		ordered.onRecordHook = func(p store.RecordCheckParams) {
			persistedCh <- p.MonitorID
		}

		chk := fakeChecker{
			fn: func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
				if strings.HasSuffix(target.RawURL(), "/drain1") {
					startedOnce.Do(func() { close(firstStarted) })
					<-releaseFirst
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
			scheduler.WithWorkerCount(1),
			scheduler.WithQueueCapacity(5),
			scheduler.WithPollInterval(10*time.Millisecond),
			scheduler.WithShutdownTimeout(2*time.Second),
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

		<-firstStarted
		// Wait until mon2 is also marked in-flight in the buffered queue before canceling.
		deadline := time.Now().Add(1 * time.Second)
		for !sched.InFlight().Contains(mon2.ID) {
			if time.Now().After(deadline) {
				close(releaseFirst)
				cancel()
				<-runDone
				t.Fatal("timed out waiting for mon2 to be queued in-flight")
			}
			time.Sleep(2 * time.Millisecond)
		}

		cancel()
		close(releaseFirst)

		select {
		case err := <-runDone:
			if err != nil {
				t.Fatalf("expected clean shutdown, got %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for shutdown drain")
		}

		if len(persistedCh) != 2 {
			t.Fatalf("expected both active and queued checks to drain and persist, got %d", len(persistedCh))
		}
		if sched.InFlight().Len() != 0 {
			t.Fatalf("expected InFlightSet empty after drain, got %d", sched.InFlight().Len())
		}
	})
}

func TestScheduler_DuplicateInFlightSuppression_WhileQueued(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	mon1 := newTestMonitor(now.Add(-2*time.Second), "https://example.com/blocker", 60)
	mon2 := newTestMonitor(now.Add(-1*time.Second), "https://example.com/queued", 60)
	st := newFakeDueStore(mon1, mon2)
	ordered := &orderedDueStore{fakeDueStore: st, order: []uuid.UUID{mon1.ID, mon2.ID}}

	mon1Started := make(chan struct{})
	releaseMon1 := make(chan struct{})
	var mon2Runs atomic.Int32
	doneCh := make(chan uuid.UUID, 5)
	ordered.onRecordHook = func(p store.RecordCheckParams) {
		doneCh <- p.MonitorID
	}

	chk := fakeChecker{
		fn: func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
			if strings.HasSuffix(target.RawURL(), "/blocker") {
				close(mon1Started)
				<-releaseMon1
			} else {
				mon2Runs.Add(1)
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
		scheduler.WithWorkerCount(1),
		scheduler.WithQueueCapacity(10),
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

	// Poll 1: dispatches mon1 (starts on sole worker) and mon2 (waits in queue).
	sched.PollOnce(ctx)
	<-mon1Started

	// Poll 2 while mon2 is still sitting in queue: mon2 must be deduplicated!
	sched.PollOnce(ctx)
	sched.PollOnce(ctx)

	close(releaseMon1)
	for i := 0; i < 2; i++ {
		<-doneCh
	}

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("scheduler.Run failed: %v", err)
	}

	if got := mon2Runs.Load(); got != 1 {
		t.Fatalf("expected queued monitor mon2 to run exactly once despite repeated polls, got %d", got)
	}
}

func TestScheduler_DueOrderingAndQueueFullDeterministic(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	mon1 := newTestMonitor(now.Add(-3*time.Second), "https://example.com/first", 60)
	mon2 := newTestMonitor(now.Add(-2*time.Second), "https://example.com/second", 60)
	mon3 := newTestMonitor(now.Add(-1*time.Second), "https://example.com/third", 60)
	st := newFakeDueStore(mon1, mon2, mon3)
	ordered := &orderedDueStore{fakeDueStore: st, order: []uuid.UUID{mon1.ID, mon2.ID, mon3.ID}}

	mon1Started := make(chan struct{})
	releaseMon1 := make(chan struct{})
	var mu sync.Mutex
	var execOrder []uuid.UUID
	doneCh := make(chan uuid.UUID, 5)
	ordered.onRecordHook = func(p store.RecordCheckParams) {
		doneCh <- p.MonitorID
	}

	chk := fakeChecker{
		fn: func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
			mu.Lock()
			switch {
			case strings.HasSuffix(target.RawURL(), "/first"):
				execOrder = append(execOrder, mon1.ID)
			case strings.HasSuffix(target.RawURL(), "/second"):
				execOrder = append(execOrder, mon2.ID)
			case strings.HasSuffix(target.RawURL(), "/third"):
				execOrder = append(execOrder, mon3.ID)
			}
			mu.Unlock()
			if strings.HasSuffix(target.RawURL(), "/first") {
				close(mon1Started)
				<-releaseMon1
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
		scheduler.WithWorkerCount(1),
		scheduler.WithQueueCapacity(1),
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

	// Wait until worker is busy? Wait: PollOnce sends mon1, mon2, mon3 synchronously while worker may pick up mon1!
	// To guarantee mon1 is occupying the worker BEFORE mon2 & mon3 are dispatched, run a two-step deterministic check:
	// First, enqueue mon1 alone and block worker, then PollOnce with mon2+mon3.
	ordered.mu.Lock()
	ordered.order = []uuid.UUID{mon1.ID}
	ordered.mu.Unlock()
	sched.PollOnce(ctx)
	<-mon1Started

	ordered.mu.Lock()
	ordered.order = []uuid.UUID{mon2.ID, mon3.ID}
	ordered.mu.Unlock()
	sched.PollOnce(ctx)

	// mon2 fills queue (cap 1); mon3 hits queue-full and is released from InFlightSet immediately.
	if !sched.InFlight().Contains(mon1.ID) || !sched.InFlight().Contains(mon2.ID) {
		close(releaseMon1)
		cancel()
		<-runDone
		t.Fatal("expected mon1 and mon2 to be in-flight")
	}
	if sched.InFlight().Contains(mon3.ID) {
		close(releaseMon1)
		cancel()
		<-runDone
		t.Fatal("expected mon3 to be released from InFlightSet when queue is full")
	}

	// Unblock mon1 -> mon1 and mon2 finish.
	close(releaseMon1)
	<-doneCh
	<-doneCh

	// Next PollOnce picks up mon3 because its next_check_at remained due.
	sched.PollOnce(ctx)
	<-doneCh

	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("scheduler.Run failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(execOrder) != 3 || execOrder[0] != mon1.ID || execOrder[1] != mon2.ID || execOrder[2] != mon3.ID {
		t.Fatalf("expected deterministic due execution order [%v, %v, %v], got %v", mon1.ID, mon2.ID, mon3.ID, execOrder)
	}
}

func TestScheduler_UnsafeMonitorURLPersistsSanitizedFailure(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	mon := newTestMonitor(now, "http://127.0.0.1:8080/internal?token=topsecret999", 60)
	st := newFakeDueStore(mon)

	persisted := make(chan struct{}, 1)
	st.onRecordHook = func(store.RecordCheckParams) {
		persisted <- struct{}{}
	}

	var checkerCalled atomic.Bool
	chk := fakeChecker{
		fn: func(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
			checkerCalled.Store(true)
			return checker.CheckObservation{}
		},
	}

	var logBuf syncBuffer
	sched, err := scheduler.New(
		st,
		chk,
		slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})),
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
	<-persisted
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("scheduler.Run failed: %v", err)
	}

	if checkerCalled.Load() {
		t.Fatal("expected unsafe target to be rejected before HealthChecker.Check is invoked")
	}
	snap, count, _ := st.snapshot(mon.ID)
	if count != 1 || snap.ConsecutiveFailures != 1 {
		t.Fatalf("expected 1 failed check recorded, got count=%d failures=%d", count, snap.ConsecutiveFailures)
	}
	if strings.Contains(logBuf.String(), "topsecret999") || strings.Contains(logBuf.String(), "?token=") {
		t.Fatalf("logs leaked query secret: %s", logBuf.String())
	}
}
