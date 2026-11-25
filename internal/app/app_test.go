package app_test

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"pulse/internal/api"
	"pulse/internal/app"
	"pulse/internal/checker"
	"pulse/internal/config"
	"pulse/internal/model"
	"pulse/internal/scheduler"
	"pulse/internal/store"
)

type fakeAppDueStore struct {
	due      []*model.Monitor
	recorded atomic.Int32
}

func (s *fakeAppDueStore) ListDueMonitors(ctx context.Context, now time.Time, limit int) ([]*model.Monitor, error) {
	if s.recorded.Load() > 0 {
		return nil, nil
	}
	return s.due, nil
}

func (s *fakeAppDueStore) RecordCheckResult(ctx context.Context, params store.RecordCheckParams) (*store.RecordCheckOutput, error) {
	s.recorded.Add(1)
	now := params.CompletedAt
	return &store.RecordCheckOutput{
		CheckResult: &model.CheckResult{
			ID:        uuid.New(),
			MonitorID: params.MonitorID,
			CheckedAt: params.CheckedAt,
			Success:   params.Success,
			LatencyMs: params.LatencyMs,
		},
		Monitor: &model.Monitor{
			ID:            params.MonitorID,
			CurrentStatus: model.StatusUp,
			NextCheckAt:   now.Add(60 * time.Second),
		},
	}, nil
}

type fakeAppChecker struct {
	started chan struct{}
	release chan struct{}
}

func (c *fakeAppChecker) Check(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation {
	select {
	case c.started <- struct{}{}:
	default:
	}
	select {
	case <-c.release:
	case <-ctx.Done():
	}
	status := http.StatusOK
	now := time.Now().UTC()
	return checker.CheckObservation{
		CheckedAt:   now,
		CompletedAt: now,
		Success:     true,
		HTTPStatus:  &status,
		Latency:     5 * time.Millisecond,
	}
}

func TestApp_New(t *testing.T) {
	cfg := config.NewDefault()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	t.Run("valid initialization", func(t *testing.T) {
		a, err := app.New(cfg, logger)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if a.Config() != cfg {
			t.Errorf("expected config %v, got %v", cfg, a.Config())
		}
		if a.Logger() != logger {
			t.Errorf("expected logger %v, got %v", logger, a.Logger())
		}
		if a.Server() == nil {
			t.Errorf("expected non-nil server")
		}
		if a.Checker() == nil {
			t.Errorf("expected non-nil checker")
		}
		if a.Scheduler() != nil {
			t.Errorf("expected nil scheduler when store is nil, got %v", a.Scheduler())
		}
		if a.DB() != nil {
			t.Errorf("expected nil DB when not configured, got %v", a.DB())
		}
		if a.Store() != nil {
			t.Errorf("expected nil Store when not configured, got %v", a.Store())
		}
	})

	t.Run("with store initializes scheduler", func(t *testing.T) {
		st := store.New(nil)
		a, err := app.New(cfg, logger, app.WithStore(st))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if a.Store() != st {
			t.Errorf("expected store %v, got %v", st, a.Store())
		}
		if a.Scheduler() == nil {
			t.Fatal("expected non-nil scheduler when store is configured")
		}
	})

	t.Run("with custom store and nil db", func(t *testing.T) {
		a, err := app.New(cfg, logger, app.WithDB(nil), app.WithStore(nil))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if a.DB() != nil {
			t.Errorf("expected nil DB, got %v", a.DB())
		}
		if a.Store() != nil {
			t.Errorf("expected nil Store, got %v", a.Store())
		}
	})

	t.Run("nil config returns error", func(t *testing.T) {
		_, err := app.New(nil, logger)
		if err == nil {
			t.Fatal("expected error with nil config, got nil")
		}
	})

	t.Run("nil logger defaults to slog.Default()", func(t *testing.T) {
		a, err := app.New(cfg, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if a.Logger() == nil {
			t.Fatal("expected non-nil logger")
		}
	})
}

func TestApp_Run(t *testing.T) {
	cfg := config.NewDefault()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	// Inject an ephemeral listener to guarantee zero port collisions during concurrent test execution
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer ln.Close()

	srv := api.NewServer(ln.Addr().String(), logger, api.WithListener(ln))
	a, err := app.New(cfg, logger, app.WithServer(srv))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after 50ms to verify graceful shutdown
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	if err := a.Run(ctx); err != nil {
		t.Fatalf("expected clean run, got error: %v", err)
	}

	output := buf.String()
	if !bytes.Contains(buf.Bytes(), []byte("pulse application initialized")) {
		t.Errorf("expected startup log, got: %s", output)
	}
	if !bytes.Contains(buf.Bytes(), []byte("pulse application shutting down gracefully")) {
		t.Errorf("expected shutdown log, got: %s", output)
	}
}

func TestApp_Run_IntegratesServerSchedulerAndGracefulShutdown(t *testing.T) {
	cfg := config.NewDefault()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer ln.Close()

	dueMon := &model.Monitor{
		ID:              uuid.New(),
		Name:            "App Lifecycle Monitor",
		URL:             "https://example.com/health",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		CurrentStatus:   model.StatusUnknown,
		NextCheckAt:     time.Now().UTC().Add(-time.Second),
	}
	fakeStore := &fakeAppDueStore{due: []*model.Monitor{dueMon}}
	fakeChk := &fakeAppChecker{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}

	sched, err := scheduler.New(
		fakeStore,
		fakeChk,
		logger,
		scheduler.WithWorkerCount(2),
		scheduler.WithQueueCapacity(10),
		scheduler.WithPollInterval(10*time.Millisecond),
		scheduler.WithShutdownTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("scheduler.New failed: %v", err)
	}

	srv := api.NewServer(ln.Addr().String(), logger, api.WithListener(ln))
	a, err := app.New(cfg, logger,
		app.WithServer(srv),
		app.WithScheduler(sched),
		app.WithShutdownTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("app.New failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- a.Run(ctx)
	}()

	// Wait until HTTP server is accepting requests and scheduler worker starts the check.
	resp, err := http.Get("http://" + ln.Addr().String() + "/healthz")
	if err != nil {
		close(fakeChk.release)
		cancel()
		<-runDone
		t.Fatalf("GET /healthz failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		close(fakeChk.release)
		cancel()
		<-runDone
		t.Fatalf("expected 200 OK from /healthz, got %d", resp.StatusCode)
	}

	<-fakeChk.started

	// Trigger shutdown while check is in-flight; release check and verify clean drain.
	cancel()
	close(fakeChk.release)

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("expected clean app shutdown, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for app.Run shutdown")
	}

	if fakeStore.recorded.Load() != 1 {
		t.Errorf("expected in-progress check to persist during graceful shutdown, got recorded=%d", fakeStore.recorded.Load())
	}
}

func TestApp_Run_ServerStartupErrorStopsScheduler(t *testing.T) {
	// Occupy a local port so server.Run fails to bind immediately.
	busyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind busy listener: %v", err)
	}
	defer busyLn.Close()

	cfg := config.NewDefault()
	cfg.AppAddr = busyLn.Addr().String()
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))

	sched, err := scheduler.New(
		&fakeAppDueStore{},
		&fakeAppChecker{started: make(chan struct{}, 1), release: make(chan struct{})},
		logger,
		scheduler.WithPollInterval(10*time.Millisecond),
		scheduler.WithShutdownTimeout(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("scheduler.New failed: %v", err)
	}

	a, err := app.New(cfg, logger, app.WithScheduler(sched))
	if err != nil {
		t.Fatalf("app.New failed: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Run(context.Background())
	}()

	select {
	case runErr := <-errCh:
		if runErr == nil {
			t.Fatal("expected server bind error from app.Run, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("app.Run did not shut down scheduler after server bind failure")
	}
}
