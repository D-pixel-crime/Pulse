package api

import (
	"encoding/json"
	"net/http"
)

// HealthResponse represents the payload returned by the GET /healthz probe.
type HealthResponse struct {
	Status string `json:"status"`
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
