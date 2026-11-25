package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"pulse/internal/api"
	"pulse/internal/checker"
	"pulse/internal/config"
	"pulse/internal/db"
	"pulse/internal/scheduler"
	"pulse/internal/store"
)

// App is the root composition point for the Pulse monitoring service.
type App struct {
	cfg             *config.Config
	logger          *slog.Logger
	server          *api.Server
	db              *db.DB
	store           *store.Store
	checker         *checker.Checker
	scheduler       *scheduler.Scheduler
	shutdownTimeout time.Duration
}

// Option configures optional App attributes.
type Option func(*App)

// WithServer allows injecting a custom or test API server instance.
func WithServer(s *api.Server) Option {
	return func(a *App) {
		a.server = s
	}
}

// WithDB configures the database connection for the application.
func WithDB(database *db.DB) Option {
	return func(a *App) {
		a.db = database
		if database != nil && database.Pool() != nil && a.store == nil {
			a.store = store.New(database.Pool())
		}
	}
}

// WithStore allows injecting a custom or test store instance.
func WithStore(s *store.Store) Option {
	return func(a *App) {
		a.store = s
	}
}

// WithChecker allows injecting a custom HTTP health checker instance.
func WithChecker(c *checker.Checker) Option {
	return func(a *App) {
		a.checker = c
	}
}

// WithScheduler allows injecting a custom or test scheduler instance.
func WithScheduler(s *scheduler.Scheduler) Option {
	return func(a *App) {
		a.scheduler = s
	}
}

// WithShutdownTimeout customizes the bounded graceful-shutdown duration for background workers.
func WithShutdownTimeout(d time.Duration) Option {
	return func(a *App) {
		if d > 0 {
			a.shutdownTimeout = d
		}
	}
}

// New creates a new App instance with the provided configuration, logger, and options.
func New(cfg *config.Config, logger *slog.Logger, opts ...Option) (*App, error) {
	if cfg == nil {
		return nil, errors.New("app: config must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	appInstance := &App{
		cfg:             cfg,
		logger:          logger,
		shutdownTimeout: api.DefaultShutdownTimeout,
	}

	for _, opt := range opts {
		opt(appInstance)
	}

	if appInstance.server == nil {
		var readyChecker api.ReadyChecker
		if appInstance.db != nil {
			readyChecker = appInstance.db
		}
		var monitorRepo api.MonitorRepository
		if appInstance.store != nil {
			monitorRepo = appInstance.store
		}
		appInstance.server = api.NewServer(cfg.AppAddr, logger,
			api.WithReadyChecker(readyChecker),
			api.WithMonitorRepository(monitorRepo),
			api.WithShutdownTimeout(appInstance.shutdownTimeout),
		)
	}

	if appInstance.checker == nil {
		appInstance.checker = checker.NewChecker(checker.NewSafeDialer())
	}

	if appInstance.scheduler == nil && appInstance.store != nil {
		sched, err := scheduler.New(
			appInstance.store,
			appInstance.checker,
			logger,
			scheduler.WithWorkerCount(cfg.WorkerCount),
			scheduler.WithQueueCapacity(cfg.QueueCapacity),
			scheduler.WithPollInterval(cfg.SchedulerPollInterval),
			scheduler.WithDefaultTimeout(cfg.DefaultCheckTimeout),
			scheduler.WithShutdownTimeout(appInstance.shutdownTimeout),
		)
		if err != nil {
			return nil, fmt.Errorf("app: failed to initialize scheduler: %w", err)
		}
		appInstance.scheduler = sched
	}

	return appInstance, nil
}

// Config returns the application configuration.
func (a *App) Config() *config.Config {
	return a.cfg
}

// Logger returns the application logger.
func (a *App) Logger() *slog.Logger {
	return a.logger
}

// Server returns the configured API server.
func (a *App) Server() *api.Server {
	return a.server
}

// DB returns the configured database connection wrapper.
func (a *App) DB() *db.DB {
	return a.db
}

// Store returns the configured monitor store.
func (a *App) Store() *store.Store {
	return a.store
}

// Checker returns the configured outbound HTTP checker.
func (a *App) Checker() *checker.Checker {
	return a.checker
}

// Scheduler returns the configured in-process scheduler, or nil if no store was configured.
func (a *App) Scheduler() *scheduler.Scheduler {
	return a.scheduler
}

// Run executes the application lifecycle, coordinating the HTTP server and background
// scheduler/worker pool until ctx is canceled or a component fails.
func (a *App) Run(ctx context.Context) error {
	a.logger.Info("pulse application initialized",
		slog.Any("config", a.cfg),
	)

	if a.scheduler == nil {
		a.logger.Info("pulse foundation running; awaiting termination signal")
		if err := a.server.Run(ctx); err != nil {
			return fmt.Errorf("server error: %w", err)
		}
		a.logger.Info("pulse application shutting down gracefully")
		return nil
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	serverErrCh := make(chan error, 1)
	schedErrCh := make(chan error, 1)

	go func() {
		serverErrCh <- a.server.Run(runCtx)
	}()

	go func() {
		schedErrCh <- a.scheduler.Run(runCtx)
	}()

	a.logger.Info("pulse running; awaiting termination signal")

	var serverErr error
	var schedErr error
	serverDone := false
	schedDone := false

	for !serverDone || !schedDone {
		select {
		case err := <-serverErrCh:
			serverDone = true
			if err != nil {
				serverErr = fmt.Errorf("server error: %w", err)
			}
			cancelRun()
		case err := <-schedErrCh:
			schedDone = true
			if err != nil {
				schedErr = fmt.Errorf("scheduler error: %w", err)
			}
			cancelRun()
		}
	}

	a.logger.Info("pulse application shutting down gracefully")

	return errors.Join(serverErr, schedErr)
}
