package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"pulse/internal/app"
	"pulse/internal/config"
	"pulse/internal/db"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Initialize structured logging with JSON handler to stdout
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Load and validate environment configuration
	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load configuration", slog.String("error", err.Error()))
		return fmt.Errorf("configuration error: %w", err)
	}

	// Listen for termination signals for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize database connection pool with sensible limits and ping at startup
	database, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to connect to database", slog.String("error", err.Error()))
		return fmt.Errorf("database connection error: %w", err)
	}
	defer database.Close()

	// Execute idempotent initial schema setup
	if err := database.RunSchema(ctx); err != nil {
		logger.Error("failed to initialize database schema", slog.String("error", err.Error()))
		return fmt.Errorf("database schema error: %w", err)
	}
	logger.Info("database schema initialized successfully")

	// Initialize application composition root
	application, err := app.New(cfg, logger, app.WithDB(database))
	if err != nil {
		logger.Error("failed to initialize application", slog.String("error", err.Error()))
		return fmt.Errorf("application initialization error: %w", err)
	}

	// Run application lifecycle
	if err := application.Run(ctx); err != nil {
		logger.Error("application encountered an error", slog.String("error", err.Error()))
		return fmt.Errorf("runtime error: %w", err)
	}

	return nil
}
