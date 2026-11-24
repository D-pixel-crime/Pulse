package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"pulse/internal/api"
	"pulse/internal/config"
	"pulse/internal/db"
	"pulse/internal/store"
)

// App is the root composition point for the Pulse monitoring service.
type App struct {
	cfg    *config.Config
	logger *slog.Logger
	server *api.Server
	db     *db.DB
	store  *store.Store
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
	}

	for _, opt := range opts {
		opt(appInstance)
	}

	if appInstance.server == nil {
		var readyChecker api.ReadyChecker
		if appInstance.db != nil {
			readyChecker = appInstance.db
		}
		appInstance.server = api.NewServer(cfg.AppAddr, logger, api.WithReadyChecker(readyChecker))
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

// Run executes the application lifecycle.
// It serves as the composition point for server and background components.
func (a *App) Run(ctx context.Context) error {
	a.logger.Info("pulse application initialized",
		slog.Any("config", a.cfg),
	)

	a.logger.Info("pulse foundation running; awaiting termination signal")

	if err := a.server.Run(ctx); err != nil {
		return fmt.Errorf("server error: %w", err)
	}

	a.logger.Info("pulse application shutting down gracefully")
	return nil
}
