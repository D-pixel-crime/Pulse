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

const (
	defaultShutdownTimeout = 5 * time.Second
	defaultReleaseTimeout  = 5 * time.Second
)

// DueMonitorStore defines the persistence operations required by the scheduler and workers.
type DueMonitorStore interface {
	ClaimDueMonitors(ctx context.Context, limit int) ([]model.ClaimedMonitor, error)
	ReleaseClaim(ctx context.Context, monitorID, claimToken uuid.UUID) (bool, error)
	RecordCheckResult(ctx context.Context, params store.RecordCheckParams) (*store.RecordCheckOutput, error)
}

// HealthChecker defines the outbound HTTP check execution contract required by workers.
type HealthChecker interface {
	Check(ctx context.Context, target *checker.Target, timeout time.Duration) checker.CheckObservation
}

// Scheduler polls due monitors on a fixed interval, claims them in PostgreSQL with
// temporary leases, suppresses local duplicate dispatches with an in-memory InFlightSet,
// and dispatches work to a fixed-size worker pool.
type Scheduler struct {
	store           DueMonitorStore
	checker         HealthChecker
	inFlight        *InFlightSet
	pool            *WorkerPool
	workerCount     int
	pollInterval    time.Duration
	batchLimit      int
	shutdownTimeout time.Duration
	retention       *RetentionLoop
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

// WithPollInterval sets the scheduler ticker interval.
func WithPollInterval(d time.Duration) Option {
	return func(s *Scheduler) {
		if d > 0 {
			s.pollInterval = d
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
		pollInterval:    config.DefaultSchedulerPollInterval,
		batchLimit:      store.DefaultDueBatchLimit,
		shutdownTimeout: defaultShutdownTimeout,
		logger:          logger,
	}

	if retStore, ok := dueStore.(RetentionStore); ok && retStore != nil {
		retLoop, err := NewRetentionLoop(retStore, logger)
		if err == nil {
			s.retention = retLoop
		}
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.batchLimit > store.MaxDueBatchLimit {
		s.batchLimit = store.MaxDueBatchLimit
	}

	pool, err := NewWorkerPool(s.workerCount, s.handleMonitor)
	if err != nil {
		return nil, fmt.Errorf("invalid scheduler worker pool configuration: %w", err)
	}
	s.pool = pool

	return s, nil
}

// Run starts the worker pool, retention loop (if configured), and runs the single scheduler ticker loop until ctx is canceled.
// On cancellation, it stops scheduling new checks, closes the work queue only after the
// polling loop has exited, and waits up to the configured shutdown timeout for workers
// to finish in-progress checks and persist their results.
func (s *Scheduler) Run(ctx context.Context) error {
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	s.pool.Start(workerCtx)

	var retentionDone chan struct{}
	if s.retention != nil {
		retentionDone = make(chan struct{})
		go func() {
			defer close(retentionDone)
			_ = s.retention.Run(ctx)
		}()
	}

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

	if retentionDone != nil {
		<-retentionDone
	}

	return nil
}

// PollOnce calculates local free execution slots, claims a bounded batch of due monitors
// from PostgreSQL, and dispatches eligible claimed tasks to the work queue.
func (s *Scheduler) PollOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	freeSlots := s.workerCount - s.inFlight.Len()
	if freeSlots <= 0 {
		return
	}

	claimLimit := freeSlots
	if s.batchLimit > 0 && s.batchLimit < claimLimit {
		claimLimit = s.batchLimit
	}
	if queueCap := s.pool.QueueCap(); queueCap > 0 && queueCap < claimLimit {
		claimLimit = queueCap
	}

	claimed, err := s.store.ClaimDueMonitors(ctx, claimLimit)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Error("failed to claim due monitors", slog.String("error", err.Error()))
		}
		return
	}

	for i, cm := range claimed {
		if ctx.Err() != nil {
			for j := i; j < len(claimed); j++ {
				rem := claimed[j]
				if rem.Monitor != nil && rem.Monitor.ID != uuid.Nil {
					s.releaseClaim(rem.Monitor.ID, rem.ClaimToken, sanitizeURLForLog(rem.Monitor.URL), nil)
				}
			}
			return
		}
		if cm.Monitor == nil || cm.Monitor.ID == uuid.Nil {
			continue
		}
		sanitizedURL := sanitizeURLForLog(cm.Monitor.URL)
		if !s.inFlight.TryAdd(cm.Monitor.ID) {
			s.releaseClaim(cm.Monitor.ID, cm.ClaimToken, sanitizedURL, nil)
			continue
		}
		if !s.pool.TryEnqueue(cm) {
			s.inFlight.Remove(cm.Monitor.ID)
			s.releaseClaim(cm.Monitor.ID, cm.ClaimToken, sanitizedURL, nil)
			s.logger.Warn("work queue full; releasing monitor for retry on next tick",
				slog.String("monitor_id", cm.Monitor.ID.String()),
				slog.String("url", sanitizedURL),
			)
		}
	}
}

func (s *Scheduler) handleMonitor(ctx context.Context, cm model.ClaimedMonitor) {
	if cm.Monitor == nil || cm.Monitor.ID == uuid.Nil {
		return
	}
	m := cm.Monitor
	defer s.inFlight.Remove(m.ID)

	sanitizedURL := sanitizeURLForLog(m.URL)
	if ctx.Err() != nil {
		s.releaseClaim(m.ID, cm.ClaimToken, sanitizedURL, nil)
		return
	}

	target, err := checker.ValidateTarget(m.URL)
	if err != nil {
		now := time.Now().UTC()
		errMsg := checker.SanitizeErrorMessage(err.Error(), nil)
		s.persistObservation(ctx, m.ID, cm.ClaimToken, nil, m.URL, checker.CheckObservation{
			CheckedAt:    now,
			Success:      false,
			HTTPStatus:   nil,
			Latency:      0,
			ErrorMessage: &errMsg,
		})
		return
	}

	timeout := time.Duration(m.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(model.DefaultTimeoutSeconds) * time.Second
	}

	obs := s.checker.Check(ctx, target, timeout)
	if ctx.Err() != nil {
		s.logger.Info("check aborted due to worker shutdown; releasing claim",
			slog.String("monitor_id", m.ID.String()),
			slog.String("url", target.SanitizedURL()),
		)
		s.releaseClaim(m.ID, cm.ClaimToken, target.SanitizedURL(), target)
		return
	}

	s.persistObservation(ctx, m.ID, cm.ClaimToken, target, m.URL, obs)
}

func (s *Scheduler) persistObservation(
	ctx context.Context,
	monitorID uuid.UUID,
	claimToken uuid.UUID,
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
		ClaimToken:   claimToken,
		CheckedAt:    obs.CheckedAt,
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
		if errors.Is(err, store.ErrClaimLost) {
			s.logger.Warn("monitor claim lost before check persistence; skipping",
				slog.String("monitor_id", monitorID.String()),
				slog.String("url", sanitizedTarget),
			)
			return
		}
		s.logger.Error("failed to persist check result; releasing claim for retry",
			slog.String("monitor_id", monitorID.String()),
			slog.String("url", sanitizedTarget),
			slog.Bool("success", obs.Success),
			slog.String("error", checker.SanitizeErrorMessage(err.Error(), target)),
		)
		s.releaseClaim(monitorID, claimToken, sanitizedTarget, target)
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

func (s *Scheduler) releaseClaim(monitorID, claimToken uuid.UUID, sanitizedURL string, target *checker.Target) {
	if monitorID == uuid.Nil || claimToken == uuid.Nil {
		return
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), defaultReleaseTimeout)
	defer cancel()

	released, err := s.store.ReleaseClaim(releaseCtx, monitorID, claimToken)
	if err != nil {
		s.logger.Warn("failed to release monitor claim",
			slog.String("monitor_id", monitorID.String()),
			slog.String("url", sanitizedURL),
			slog.String("error", checker.SanitizeErrorMessage(err.Error(), target)),
		)
		return
	}
	if !released {
		s.logger.Debug("monitor claim already released or superseded",
			slog.String("monitor_id", monitorID.String()),
			slog.String("url", sanitizedURL),
		)
	}
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
