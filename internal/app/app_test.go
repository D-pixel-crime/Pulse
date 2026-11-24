package app_test

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"pulse/internal/api"
	"pulse/internal/app"
	"pulse/internal/config"
)

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
