package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"pulse/internal/config"
)

const (
	// DefaultReadHeaderTimeout mitigates Slowloris attacks on incoming connections.
	DefaultReadHeaderTimeout = 2 * time.Second
	// DefaultReadTimeout covers the maximum duration for reading the entire request.
	DefaultReadTimeout = 5 * time.Second
	// DefaultWriteTimeout covers the maximum duration before timing out writes of the response.
	DefaultWriteTimeout = 10 * time.Second
	// DefaultIdleTimeout is the maximum amount of time to wait for the next request when keep-alives are enabled.
	DefaultIdleTimeout = 30 * time.Second
	// DefaultShutdownTimeout is the maximum bounded duration allocated for in-flight requests to complete during graceful shutdown.
	DefaultShutdownTimeout = 5 * time.Second
	// DefaultMaxHeaderBytes specifies the maximum size in bytes of HTTP request headers.
	DefaultMaxHeaderBytes = 1 << 20 // 1 MB
)

// responseCapture intercepts header writes and discards default plain-text error bodies,
// enabling the router to substitute uniform JSON error responses.
type responseCapture struct {
	http.ResponseWriter
	statusCode int
}

func (rc *responseCapture) WriteHeader(code int) {
	if rc.statusCode == 0 {
		rc.statusCode = code
	}
}

func (rc *responseCapture) Write(b []byte) (int, error) {
	if rc.statusCode == 0 {
		rc.statusCode = http.StatusOK
	}
	return len(b), nil
}

// Router wraps http.ServeMux to guarantee consistent JSON error formatting for 404 Not Found
// and 405 Method Not Allowed responses across the entire API surface.
type Router struct {
	mux    *http.ServeMux
	logger *slog.Logger
}

// NewRouter constructs a configured router with all foundation routes and fallback error handling.
func NewRouter(logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", HealthzHandler())

	return &Router{
		mux:    mux,
		logger: logger,
	}
}

// ServeHTTP dispatches requests to registered routes or produces standard JSON error responses.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h, pattern := rt.mux.Handler(r)
	if pattern == "" {
		capture := &responseCapture{ResponseWriter: w}
		h.ServeHTTP(capture, r)

		if capture.statusCode == http.StatusMethodNotAllowed {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		writeError(w, http.StatusNotFound, "not found")
		return
	}

	h.ServeHTTP(w, r)
}

// Server encapsulates the HTTP server, configured timeouts, and graceful shutdown lifecycle.
type Server struct {
	httpServer      *http.Server
	logger          *slog.Logger
	shutdownTimeout time.Duration
	listener        net.Listener
}

// Option provides functional options for configuring a Server instance.
type Option func(*Server)

// WithShutdownTimeout customizes the graceful shutdown deadline timeout.
func WithShutdownTimeout(d time.Duration) Option {
	return func(s *Server) {
		if d > 0 {
			s.shutdownTimeout = d
		}
	}
}

// WithListener sets a custom net.Listener, allowing test suites to inject ephemeral or pre-bound listeners.
func WithListener(ln net.Listener) Option {
	return func(s *Server) {
		s.listener = ln
	}
}

// WithHandler overrides the default HTTP handler.
func WithHandler(h http.Handler) Option {
	return func(s *Server) {
		s.httpServer.Handler = h
	}
}

// WithTimeouts customizes HTTP server timeouts.
func WithTimeouts(readHeader, read, write, idle, shutdown time.Duration) Option {
	return func(s *Server) {
		if readHeader > 0 {
			s.httpServer.ReadHeaderTimeout = readHeader
		}
		if read > 0 {
			s.httpServer.ReadTimeout = read
		}
		if write > 0 {
			s.httpServer.WriteTimeout = write
		}
		if idle > 0 {
			s.httpServer.IdleTimeout = idle
		}
		if shutdown > 0 {
			s.shutdownTimeout = shutdown
		}
	}
}

// New constructs a new Server instance using application configuration.
func New(cfg *config.Config, logger *slog.Logger, opts ...Option) *Server {
	addr := config.DefaultAppAddr
	if cfg != nil && cfg.AppAddr != "" {
		addr = cfg.AppAddr
	}
	return NewServer(addr, logger, opts...)
}

// NewServer creates a new HTTP server configured with sensible local-service timeouts.
func NewServer(addr string, logger *slog.Logger, opts ...Option) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	s := &Server{
		logger:          logger,
		shutdownTimeout: DefaultShutdownTimeout,
		httpServer: &http.Server{
			Addr:              addr,
			ReadHeaderTimeout: DefaultReadHeaderTimeout,
			ReadTimeout:       DefaultReadTimeout,
			WriteTimeout:      DefaultWriteTimeout,
			IdleTimeout:       DefaultIdleTimeout,
			MaxHeaderBytes:    DefaultMaxHeaderBytes,
		},
	}

	s.httpServer.Handler = NewRouter(logger)

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Addr returns the server's configured address or the active listener's network address if set.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.httpServer.Addr
}

// Handler returns the root HTTP handler configured on the server.
func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler
}

// ShutdownTimeout returns the configured shutdown deadline.
func (s *Server) ShutdownTimeout() time.Duration {
	return s.shutdownTimeout
}

// HTTPServer returns the underlying standard library *http.Server for inspection.
func (s *Server) HTTPServer() *http.Server {
	return s.httpServer
}

// Run starts listening on the configured address and accepts connections until ctx is canceled,
// at which point it initiates graceful shutdown bounded by the shutdown timeout.
func (s *Server) Run(ctx context.Context) error {
	var ln net.Listener
	var err error

	if s.listener != nil {
		ln = s.listener
	} else {
		ln, err = net.Listen("tcp", s.httpServer.Addr)
		if err != nil {
			return fmt.Errorf("http server failed to listen on %s: %w", s.httpServer.Addr, err)
		}
	}

	return s.Serve(ctx, ln)
}

// Serve serves connections from the given listener until ctx is canceled, then executes
// signal-aware graceful shutdown within the bounded shutdown deadline.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	serverErrCh := make(chan error, 1)

	go func() {
		s.logger.Info("http server listening", slog.String("addr", ln.Addr().String()))
		if err := s.httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
		close(serverErrCh)
	}()

	select {
	case err := <-serverErrCh:
		return fmt.Errorf("http server runtime error: %w", err)
	case <-ctx.Done():
		s.logger.Info("shutdown signal received; commencing graceful http server shutdown")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()

	if shutdownErr := s.httpServer.Shutdown(shutdownCtx); shutdownErr != nil {
		s.logger.Error("http server graceful shutdown failed", slog.String("error", shutdownErr.Error()))
		if closeErr := s.httpServer.Close(); closeErr != nil {
			s.logger.Error("http server force close error", slog.String("error", closeErr.Error()))
		}
		// Ensure Serve goroutine has exited
		<-serverErrCh
		return fmt.Errorf("http server graceful shutdown failed: %w", shutdownErr)
	}

	s.logger.Info("http server shutdown completed gracefully")

	// Ensure Serve goroutine has exited
	if err := <-serverErrCh; err != nil {
		return fmt.Errorf("http server error during shutdown: %w", err)
	}

	return nil
}
