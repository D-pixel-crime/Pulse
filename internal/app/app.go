package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"pulse/internal/api"
	"pulse/internal/config"
)

// App is the root composition point for the Pulse monitoring service.
type App struct {
	cfg    *config.Config
	logger *slog.Logger
	server *api.Server
}

// Option configures optional App attributes.
type Option func(*App)

// WithServer allows injecting a custom or test API server instance.
func WithServer(s *api.Server) Option {
	return func(a *App) {
		a.server = s
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
		cfg:    cfg,
		logger: logger,
		server: api.NewServer(cfg.AppAddr, logger),
	}

	for _, opt := range opts {
		opt(appInstance)
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
	// - Phase 6: api.NewServer(a.cfg.AppAddr, ...) (instantiated in app root)
	// - Phase 7: graceful shutdown of server, scheduler, worker pool, and db pool

	a.logger.Info("pulse foundation running; awaiting termination signal")

	if err := a.server.Run(ctx); err != nil {
		return fmt.Errorf("server error: %w", err)
	}

	a.logger.Info("pulse application shutting down gracefully")
	return nil
}
