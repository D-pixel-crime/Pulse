package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path"
	"strings"
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
// and 405 Method Not Allowed responses across the entire API surface, and enforces service-to-service
// Bearer authentication for all /api/v1/ routes while keeping health probes (/healthz, /readyz) public.
type Router struct {
	mux        *http.ServeMux
	logger     *slog.Logger
	apiKeyHash [32]byte
	hasAPIKey  bool
}

// NewRouterWithRepo constructs a configured router including monitor CRUD routes when repo is non-nil
// and protects all /api/v1/ routes with the provided apiKey.
func NewRouterWithRepo(logger *slog.Logger, repo MonitorRepository, apiKey string, checkers ...ReadyChecker) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}

	var checker ReadyChecker
	if len(checkers) > 0 {
		checker = checkers[0]
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", HealthzHandler())
	mux.HandleFunc("GET /readyz", ReadyzHandler(checker))

	if repo != nil {
		RegisterMonitorRoutes(mux, repo, logger)
	}

	var apiKeyHash [32]byte
	var hasAPIKey bool
	if len(apiKey) >= config.MinAPIKeyLength {
		apiKeyHash = sha256.Sum256([]byte(apiKey))
		hasAPIKey = true
	}

	return &Router{
		mux:        mux,
		logger:     logger,
		apiKeyHash: apiKeyHash,
		hasAPIKey:  hasAPIKey,
	}
}

// authenticateBearer validates that the incoming request contains an Authorization: Bearer <token>
// header matching the configured API key using subtle.ConstantTimeCompare on fixed-size SHA-256 digests.
// Neither the key nor header is ever logged.
func (rt *Router) authenticateBearer(r *http.Request) bool {
	if !rt.hasAPIKey {
		return false
	}

	var authHeaders []string
	for k, v := range r.Header {
		if strings.EqualFold(k, "Authorization") {
			authHeaders = append(authHeaders, v...)
		}
	}
	if len(authHeaders) != 1 {
		return false
	}

	parts := strings.Fields(authHeaders[0])
	if len(parts) != 2 {
		return false
	}
	if !strings.EqualFold(parts[0], "Bearer") {
		return false
	}
	token := parts[1]
	if token == "" {
		return false
	}

	suppliedHash := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(suppliedHash[:], rt.apiKeyHash[:]) == 1
}

// ServeHTTP dispatches requests to registered routes or produces standard JSON error responses.
// It intercepts all /api/v1/ routes to enforce Bearer token authentication.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cleanedPath := path.Clean(r.URL.Path)
	if cleanedPath == "/api/v1" || strings.HasPrefix(cleanedPath, "/api/v1/") {
		if !rt.authenticateBearer(r) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
	}

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

	rt.mux.ServeHTTP(w, r)
}

// Server encapsulates the HTTP server, configured timeouts, and graceful shutdown lifecycle.
type Server struct {
	httpServer      *http.Server
	logger          *slog.Logger
	shutdownTimeout time.Duration
	listener        net.Listener
	readyChecker    ReadyChecker
	monitorRepo     MonitorRepository
	apiKey          string
}

// Option provides functional options for configuring a Server instance.
type Option func(*Server)

// WithAPIKey configures the service-to-service Bearer token for protecting /api/v1/ routes.
func WithAPIKey(key string) Option {
	return func(s *Server) {
		s.apiKey = key
		s.httpServer.Handler = NewRouterWithRepo(s.logger, s.monitorRepo, s.apiKey, s.readyChecker)
	}
}

// WithReadyChecker configures the database connectivity checker for the GET /readyz probe.
func WithReadyChecker(checker ReadyChecker) Option {
	return func(s *Server) {
		s.readyChecker = checker
		s.httpServer.Handler = NewRouterWithRepo(s.logger, s.monitorRepo, s.apiKey, checker)
	}
}

// WithMonitorRepository configures the repository for monitor CRUD routes.
func WithMonitorRepository(repo MonitorRepository) Option {
	return func(s *Server) {
		s.monitorRepo = repo
		s.httpServer.Handler = NewRouterWithRepo(s.logger, repo, s.apiKey, s.readyChecker)
	}
}

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

	for _, opt := range opts {
		opt(s)
	}

	if s.httpServer.Handler == nil {
		s.httpServer.Handler = NewRouterWithRepo(s.logger, s.monitorRepo, s.apiKey, s.readyChecker)
	}

	return s
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
