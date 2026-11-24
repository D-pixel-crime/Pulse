package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// HealthResponse represents the payload returned by the GET /healthz probe.
type HealthResponse struct {
	Status string `json:"status"`
}

// ReadyResponse represents the payload returned by the GET /readyz probe.
type ReadyResponse struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// ReadyChecker defines an interface for probing dependency readiness.
type ReadyChecker interface {
	Ping(ctx context.Context) error
}

// ReadyCheckFunc adapts a function to satisfy the ReadyChecker interface.
type ReadyCheckFunc func(ctx context.Context) error

// Ping calls the underlying function to verify readiness.
func (f ReadyCheckFunc) Ping(ctx context.Context) error {
	return f(ctx)
}

// ErrorResponse represents standard JSON error payloads across the API.
type ErrorResponse struct {
	Error string `json:"error"`
}

// writeJSON writes a JSON response with the given status code and application/json Content-Type header.
func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

// writeError writes a standard JSON error response with the given status code.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, ErrorResponse{Error: message})
}

// HealthzHandler returns an http.HandlerFunc that responds to process liveness checks.
// GET requests return HTTP 200 with the JSON liveness response.
// HEAD requests return HTTP 200 with identical headers and an empty response body.
// Other unsupported methods return HTTP 405 Method Not Allowed with Allow: GET, HEAD.
func HealthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			return
		}
		writeJSON(w, http.StatusOK, HealthResponse{Status: "ok"})
	}
}

// ReadyzHandler returns an http.HandlerFunc that verifies database connectivity via a ping check.
// GET requests return HTTP 200 only when PostgreSQL can be reached and HTTP 503 otherwise.
// HEAD requests return the corresponding status code with headers and an empty body.
// Other unsupported methods return HTTP 405 Method Not Allowed with Allow: GET, HEAD.
func ReadyzHandler(checker ReadyChecker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		if checker == nil {
			if r.Method == http.MethodHead {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			writeJSON(w, http.StatusServiceUnavailable, ReadyResponse{
				Status: "unavailable",
				Error:  "database readiness checker not configured",
			})
			return
		}

		if err := checker.Ping(r.Context()); err != nil {
			if r.Method == http.MethodHead {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			writeJSON(w, http.StatusServiceUnavailable, ReadyResponse{
				Status: "unavailable",
				Error:  fmt.Sprintf("database ping failed: %v", err),
			})
			return
		}

		if r.Method == http.MethodHead {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			return
		}

		writeJSON(w, http.StatusOK, ReadyResponse{Status: "ready"})
	}
}
