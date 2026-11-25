package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/google/uuid"

	"pulse/internal/checker"
	"pulse/internal/config"
	"pulse/internal/model"
	"pulse/internal/store"
)

const defaultShutdownTimeout = 5 * time.Second

// DueMonitorStore defines the persistence operations required by the scheduler and workers.
type DueMonitorStore interface {
	ListDueMonitors(ctx context.Context, now time.Time, limit int) ([]*model.Monitor, error)
	RecordCheckResult(ctx context.Context, params store.RecordCheckParams) (*store.RecordCheckOutput, error)
}

// HealthChecker defines the outbound HTTP check execution contract required by workers.
type HealthChecker interface {
	Check(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation
}

// Scheduler polls due monitors on a fixed interval, suppresses concurrent duplicate
// dispatches with an in-memory InFlightSet, and dispatches work to a fixed-size worker pool.
type Scheduler struct {
	store           DueMonitorStore
	checker         HealthChecker
	inFlight        *InFlightSet
	pool            *WorkerPool[*model.Monitor]
	workerCount     int
	queueCapacity   int
	pollInterval    time.Duration
	batchLimit      int
	defaultTimeout  time.Duration
	shutdownTimeout time.Duration
	now             func() time.Time
	logger          *slog.Logger
}

// Option configures optional Scheduler settings.
type Option func(*Scheduler)

// WithWorkerCount sets the number of worker goroutines (valid range 1–64).
func WithWorkerCount(n int) Option {
	return func(s *Scheduler) {
		s.workerCount = n
	}
}

// WithQueueCapacity sets the bounded work channel capacity.
func WithQueueCapacity(n int) Option {
	return func(s *Scheduler) {
		s.queueCapacity = n
	}
}

// WithPollInterval sets the scheduler ticker interval.
func WithPollInterval(d time.Duration) Option {
	return func(s *Scheduler) {
		if d > 0 {
			s.pollInterval = d
		}
	}
}

// WithBatchLimit sets the maximum number of due monitors fetched per poll tick.
func WithBatchLimit(limit int) Option {
	return func(s *Scheduler) {
		if limit > 0 {
			s.batchLimit = limit
		}
	}
}

// WithDefaultTimeout sets the fallback check timeout if a monitor's timeout is unset.
func WithDefaultTimeout(d time.Duration) Option {
	return func(s *Scheduler) {
		if d > 0 {
			s.defaultTimeout = d
		}
	}
}

// WithShutdownTimeout sets the bounded graceful-shutdown wait duration for in-progress checks.
func WithShutdownTimeout(d time.Duration) Option {
	return func(s *Scheduler) {
		if d > 0 {
			s.shutdownTimeout = d
		}
	}
}

// WithNowFunc sets a custom clock function (primarily for deterministic tests).
func WithNowFunc(now func() time.Time) Option {
	return func(s *Scheduler) {
		if now != nil {
			s.now = now
		}
	}
}

// New constructs a Scheduler wired with a bounded work queue, fixed worker pool,
// and in-flight monitor deduplication set.
func New(dueStore DueMonitorStore, healthChecker HealthChecker, logger *slog.Logger, opts ...Option) (*Scheduler, error) {
	if dueStore == nil {
		return nil, errors.New("scheduler store must not be nil")
	}
	if healthChecker == nil {
		return nil, errors.New("scheduler health checker must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	s := &Scheduler{
		store:           dueStore,
		checker:         healthChecker,
		inFlight:        NewInFlightSet(),
		workerCount:     config.DefaultWorkerCount,
		queueCapacity:   config.DefaultQueueCapacity,
		pollInterval:    config.DefaultSchedulerPollInterval,
		batchLimit:      store.DefaultDueBatchLimit,
		defaultTimeout:  config.DefaultCheckTimeout,
		shutdownTimeout: defaultShutdownTimeout,
		now:             time.Now,
		logger:          logger,
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.batchLimit > store.MaxDueBatchLimit {
		s.batchLimit = store.MaxDueBatchLimit
	}

	pool, err := NewWorkerPool[*model.Monitor](s.workerCount, s.queueCapacity, s.handleMonitor)
	if err != nil {
		return nil, fmt.Errorf("invalid scheduler worker pool configuration: %w", err)
	}
	s.pool = pool

	return s, nil
}

// InFlight returns the scheduler's in-flight monitor tracking set.
func (s *Scheduler) InFlight() *InFlightSet {
	return s.inFlight
}

// Run starts the worker pool and runs the single scheduler ticker loop until ctx is canceled.
// On cancellation, it stops scheduling new checks, closes the work queue only after the
// polling loop has exited, and waits up to the configured shutdown timeout for workers
// to finish in-progress checks and persist their results.
func (s *Scheduler) Run(ctx context.Context) error {
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	s.pool.Start(workerCtx)

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	s.logger.Info("scheduler started",
		slog.Int("worker_count", s.pool.WorkerCount()),
		slog.Int("queue_capacity", s.pool.QueueCap()),
		slog.Duration("poll_interval", s.pollInterval),
	)

	running := true
	for running {
		select {
		case <-ctx.Done():
			running = false
		case <-ticker.C:
			s.PollOnce(ctx)
		}
	}

	// Safe to close queue now because the single polling loop has stopped and will never send again.
	s.pool.Close()

	done := make(chan struct{})
	go func() {
		s.pool.Wait()
		close(done)
	}()

	timer := time.NewTimer(s.shutdownTimeout)
	defer timer.Stop()

	select {
	case <-done:
		s.logger.Info("scheduler workers shut down gracefully")
	case <-timer.C:
		s.logger.Warn("scheduler worker shutdown timed out; canceling remaining in-progress checks",
			slog.Duration("timeout", s.shutdownTimeout),
		)
		workerCancel()
		<-done
	}

	return nil
}

// PollOnce executes one bounded due-monitor query and dispatches eligible monitors to the work queue.
func (s *Scheduler) PollOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	monitors, err := s.store.ListDueMonitors(ctx, s.now().UTC(), s.batchLimit)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Error("failed to query due monitors", slog.String("error", err.Error()))
		}
		return
	}

	for _, m := range monitors {
		if ctx.Err() != nil {
			return
		}
		if m == nil || m.ID == uuid.Nil {
			continue
		}
		if !s.inFlight.TryAdd(m.ID) {
			continue
		}
		if !s.pool.TryEnqueue(m) {
			s.inFlight.Remove(m.ID)
			s.logger.Warn("work queue full; releasing monitor for retry on next tick",
				slog.String("monitor_id", m.ID.String()),
				slog.String("url", sanitizeURLForLog(m.URL)),
			)
		}
	}
}

func (s *Scheduler) handleMonitor(ctx context.Context, m *model.Monitor) {
	if m == nil {
		return
	}
	defer s.inFlight.Remove(m.ID)

	if ctx.Err() != nil {
		return
	}

	target, err := checker.ValidateTarget(m.URL)
	if err != nil {
		now := s.now().UTC()
		errMsg := checker.SanitizeErrorMessage(err.Error(), nil)
		s.persistObservation(ctx, m.ID, nil, m.URL, checker.CheckObservation{
			CheckedAt:    now,
			CompletedAt:  now,
			Success:      false,
			HTTPStatus:   nil,
			Latency:      0,
			ErrorMessage: &errMsg,
		})
		return
	}

	timeout := time.Duration(m.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = s.defaultTimeout
	}

	obs := s.checker.Check(ctx, target, timeout)
	if ctx.Err() != nil {
		s.logger.Info("check aborted due to worker shutdown; leaving monitor due",
			slog.String("monitor_id", m.ID.String()),
			slog.String("url", target.SanitizedURL()),
		)
		return
	}

	s.persistObservation(ctx, m.ID, target, m.URL, obs)
}

func (s *Scheduler) persistObservation(
	ctx context.Context,
	monitorID uuid.UUID,
	target *checker.Target,
	rawURL string,
	obs checker.CheckObservation,
) {
	sanitizedTarget := sanitizeURLForLog(rawURL)
	if target != nil {
		sanitizedTarget = target.SanitizedURL()
	}

	out, err := s.store.RecordCheckResult(ctx, store.RecordCheckParams{
		MonitorID:    monitorID,
		CheckedAt:    obs.CheckedAt,
		CompletedAt:  obs.CompletedAt,
		Success:      obs.Success,
		HTTPStatus:   obs.HTTPStatus,
		LatencyMs:    obs.LatencyMs(),
		ErrorMessage: obs.ErrorMessage,
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			s.logger.Info("monitor deleted before check persistence; skipping",
				slog.String("monitor_id", monitorID.String()),
			)
			return
		}
		s.logger.Error("failed to persist check result; leaving monitor due for retry",
			slog.String("monitor_id", monitorID.String()),
			slog.String("url", sanitizedTarget),
			slog.Bool("success", obs.Success),
			slog.String("error", checker.SanitizeErrorMessage(err.Error(), target)),
		)
		return
	}

	s.logger.Debug("monitor check persisted",
		slog.String("monitor_id", monitorID.String()),
		slog.String("url", sanitizedTarget),
		slog.String("status", string(out.Monitor.CurrentStatus)),
		slog.Int("consecutive_failures", out.Monitor.ConsecutiveFailures),
		slog.Int64("latency_ms", out.CheckResult.LatencyMs),
		slog.Time("next_check_at", out.Monitor.NextCheckAt),
	)
}

func sanitizeURLForLog(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || !u.IsAbs() || u.Host == "" {
		return "[invalid URL]"
	}
	u.RawQuery = ""
	u.Fragment = ""
	u.User = nil
	return u.String()
}
