package app

import (
	"context"
	"errors"
	"log/slog"

	"pulse/internal/config"
)

// App is the root composition point for the Pulse monitoring service.
type App struct {
	cfg    *config.Config
	logger *slog.Logger
}

// New creates a new App instance with the provided configuration and logger.
func New(cfg *config.Config, logger *slog.Logger) (*App, error) {
	if cfg == nil {
		return nil, errors.New("app: config must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &App{
		cfg:    cfg,
		logger: logger,
	}, nil
}

// Config returns the application configuration.
func (a *App) Config() *config.Config {
	return a.cfg
}

// Logger returns the application logger.
func (a *App) Logger() *slog.Logger {
	return a.logger
}

// Run executes the application lifecycle.
// It serves as the composition point for server and background components.
func (a *App) Run(ctx context.Context) error {
	a.logger.Info("pulse application initialized",
		slog.Any("config", a.cfg),
	)

	// Subsystem composition points for subsequent implementation phases:
	// - Phase 2: db.Connect(ctx, a.cfg.DatabaseURL) & db.RunSchema(ctx, db)
	// - Phase 3: checker.NewSafeChecker(...)
	// - Phase 4: store.New(db)
	// - Phase 5: scheduler.New(...), worker.NewPool(...)
	// - Phase 6: api.NewServer(a.cfg.AppAddr, ...)
	// - Phase 7: graceful shutdown of server, scheduler, worker pool, and db pool

	a.logger.Info("pulse foundation running; awaiting termination signal")

	<-ctx.Done()

	a.logger.Info("pulse application shutting down gracefully")
	return nil
}
