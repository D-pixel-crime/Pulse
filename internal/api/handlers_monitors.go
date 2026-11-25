package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"pulse/internal/model"
	"pulse/internal/store"
)

// MonitorRepository defines the data access contract required by monitor CRUD, history, and stats handlers.
type MonitorRepository interface {
	Create(ctx context.Context, m *model.Monitor) (*model.Monitor, error)
	GetByID(ctx context.Context, id uuid.UUID) (*model.Monitor, error)
	List(ctx context.Context) ([]*model.Monitor, error)
	Delete(ctx context.Context, id uuid.UUID) error
	ListChecks(ctx context.Context, monitorID uuid.UUID, limit int) ([]*model.CheckResult, error)
	GetMonitorStats(ctx context.Context, monitorID uuid.UUID, hours int) (*model.MonitorStats, error)
}

// CreateMonitorRequest represents the incoming JSON payload for registering a new monitor.
type CreateMonitorRequest struct {
	Name            string `json:"name"`
	URL             string `json:"url"`
	IntervalSeconds *int   `json:"interval_seconds"`
	TimeoutSeconds  *int   `json:"timeout_seconds"`
}

// ListMonitorsResponse represents the response containing an array of monitors.
type ListMonitorsResponse struct {
	Monitors []*model.Monitor `json:"monitors"`
}

// ListChecksResponse represents the response containing recent check history for a monitor.
type ListChecksResponse struct {
	MonitorID uuid.UUID            `json:"monitor_id"`
	Limit     int                  `json:"limit"`
	Checks    []*model.CheckResult `json:"checks"`
}

const (
	// maxRequestBodySize limits request bodies to 1 MB to protect against memory exhaustion.
	maxRequestBodySize = 1 << 20
)

// RegisterMonitorRoutes registers the REST endpoints for monitor CRUD, check history, and stats operations.
func RegisterMonitorRoutes(mux *http.ServeMux, repo MonitorRepository, logger *slog.Logger) {
	mux.HandleFunc("POST /api/v1/monitors", CreateMonitorHandler(repo, logger))
	mux.HandleFunc("GET /api/v1/monitors", ListMonitorsHandler(repo, logger))
	mux.HandleFunc("GET /api/v1/monitors/{id}", GetMonitorHandler(repo, logger))
	mux.HandleFunc("DELETE /api/v1/monitors/{id}", DeleteMonitorHandler(repo, logger))
	mux.HandleFunc("GET /api/v1/monitors/{id}/checks", ListMonitorChecksHandler(repo, logger))
	mux.HandleFunc("GET /api/v1/monitors/{id}/stats", GetMonitorStatsHandler(repo, logger))
}

// CreateMonitorHandler handles POST /api/v1/monitors to register a new monitor.
func CreateMonitorHandler(repo MonitorRepository, logger *slog.Logger) http.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}

	return func(w http.ResponseWriter, r *http.Request) {
		// Validate Content-Type
		ct := r.Header.Get("Content-Type")
		if ct == "" {
			writeError(w, http.StatusBadRequest, "Content-Type must be application/json")
			return
		}
		mediaType, _, err := mime.ParseMediaType(ct)
		if err != nil || mediaType != "application/json" {
			writeError(w, http.StatusBadRequest, "Content-Type must be application/json")
			return
		}

		// Enforce bounded request body size
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)

		var req CreateMonitorRequest
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&req); err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				writeError(w, http.StatusBadRequest, "request body exceeds maximum allowed size")
				return
			}
			if errors.Is(err, io.EOF) {
				writeError(w, http.StatusBadRequest, "request body cannot be empty")
				return
			}
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON payload: %v", err))
			return
		}

		// Verify that the request body contains only one complete JSON value followed by optional whitespace.
		var trailing any
		if err := dec.Decode(&trailing); err != nil {
			if !errors.Is(err, io.EOF) {
				var maxBytesErr *http.MaxBytesError
				if errors.As(err, &maxBytesErr) {
					writeError(w, http.StatusBadRequest, "request body exceeds maximum allowed size")
					return
				}
				writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON payload: %v", err))
				return
			}
		} else {
			writeError(w, http.StatusBadRequest, "request body must contain only a single JSON object")
			return
		}

		// Validate name
		name := strings.TrimSpace(req.Name)
		if name == "" {
			writeError(w, http.StatusBadRequest, "validation failed: name must not be empty")
			return
		}
		if utf8.RuneCountInString(name) > 255 {
			writeError(w, http.StatusBadRequest, "validation failed: name must not exceed 255 characters")
			return
		}

		// Validate URL basic syntax
		rawURL := strings.TrimSpace(req.URL)
		if rawURL == "" {
			writeError(w, http.StatusBadRequest, "validation failed: url must not be empty")
			return
		}
		parsedURL, err := url.Parse(rawURL)
		if err != nil || !parsedURL.IsAbs() || parsedURL.Host == "" {
			writeError(w, http.StatusBadRequest, "validation failed: url must be a valid absolute URL")
			return
		}
		scheme := strings.ToLower(parsedURL.Scheme)
		if scheme != "http" && scheme != "https" {
			writeError(w, http.StatusBadRequest, "validation failed: url scheme must be http or https")
			return
		}
		if parsedURL.User != nil {
			writeError(w, http.StatusBadRequest, "validation failed: url must not contain embedded user credentials")
			return
		}

		// Validate and default interval_seconds (default: 60s, range: 30s–86400s)
		interval := model.DefaultIntervalSeconds
		if req.IntervalSeconds != nil {
			interval = *req.IntervalSeconds
			if interval < model.MinIntervalSeconds || interval > model.MaxIntervalSeconds {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("validation failed: interval_seconds must be between %d and %d", model.MinIntervalSeconds, model.MaxIntervalSeconds))
				return
			}
		}

		// Validate and default timeout_seconds (default: 5s, range: 1s–30s)
		timeout := model.DefaultTimeoutSeconds
		if req.TimeoutSeconds != nil {
			timeout = *req.TimeoutSeconds
			if timeout > interval {
				writeError(w, http.StatusBadRequest, "validation failed: timeout_seconds must not exceed interval_seconds")
				return
			}
			if timeout < model.MinTimeoutSeconds || timeout > model.MaxTimeoutSeconds {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("validation failed: timeout_seconds must be between %d and %d", model.MinTimeoutSeconds, model.MaxTimeoutSeconds))
				return
			}
		}

		now := time.Now().UTC()
		m := &model.Monitor{
			ID:                  uuid.New(),
			Name:                name,
			URL:                 rawURL,
			IntervalSeconds:     interval,
			TimeoutSeconds:      timeout,
			CurrentStatus:       model.StatusUnknown,
			ConsecutiveFailures: 0,
			LastCheckedAt:       nil,
			LastSuccessAt:       nil,
			NextCheckAt:         now,
			CreatedAt:           now,
			UpdatedAt:           now,
		}

		created, err := repo.Create(r.Context(), m)
		if err != nil {
			logger.Error("failed to create monitor", slog.String("error", err.Error()))
			writeError(w, http.StatusInternalServerError, "failed to create monitor")
			return
		}

		writeJSON(w, http.StatusCreated, created)
	}
}

// ListMonitorsHandler handles GET /api/v1/monitors to list all monitors ordered by created_at DESC.
func ListMonitorsHandler(repo MonitorRepository, logger *slog.Logger) http.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}

	return func(w http.ResponseWriter, r *http.Request) {
		monitors, err := repo.List(r.Context())
		if err != nil {
			logger.Error("failed to list monitors", slog.String("error", err.Error()))
			writeError(w, http.StatusInternalServerError, "failed to list monitors")
			return
		}

		if monitors == nil {
			monitors = make([]*model.Monitor, 0)
		}

		writeJSON(w, http.StatusOK, ListMonitorsResponse{Monitors: monitors})
	}
}

// GetMonitorHandler handles GET /api/v1/monitors/{id} to retrieve a single monitor by UUID.
func GetMonitorHandler(repo MonitorRepository, logger *slog.Logger) http.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}

	return func(w http.ResponseWriter, r *http.Request) {
		idStr := r.PathValue("id")
		id, err := uuid.Parse(idStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid monitor id: must be a valid UUID")
			return
		}

		m, err := repo.GetByID(r.Context(), id)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeError(w, http.StatusNotFound, "monitor not found")
				return
			}
			logger.Error("failed to get monitor", slog.String("id", id.String()), slog.String("error", err.Error()))
			writeError(w, http.StatusInternalServerError, "failed to get monitor")
			return
		}

		writeJSON(w, http.StatusOK, m)
	}
}

// DeleteMonitorHandler handles DELETE /api/v1/monitors/{id} to remove a monitor and cascade delete checks.
func DeleteMonitorHandler(repo MonitorRepository, logger *slog.Logger) http.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}

	return func(w http.ResponseWriter, r *http.Request) {
		idStr := r.PathValue("id")
		id, err := uuid.Parse(idStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid monitor id: must be a valid UUID")
			return
		}

		err = repo.Delete(r.Context(), id)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeError(w, http.StatusNotFound, "monitor not found")
				return
			}
			logger.Error("failed to delete monitor", slog.String("id", id.String()), slog.String("error", err.Error()))
			writeError(w, http.StatusInternalServerError, "failed to delete monitor")
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func parseMonitorID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid monitor id: must be a valid UUID")
		return uuid.Nil, false
	}
	return id, true
}

func parseBoundedIntQueryParam(r *http.Request, name string, defaultVal, minVal, maxVal int) (int, error) {
	q := r.URL.Query()
	if !q.Has(name) {
		return defaultVal, nil
	}

	vals := q[name]
	if len(vals) != 1 {
		return 0, fmt.Errorf("invalid query parameter: %s must be between %d and %d", name, minVal, maxVal)
	}

	val, err := strconv.Atoi(vals[0])
	if err != nil || val < minVal || val > maxVal {
		return 0, fmt.Errorf("invalid query parameter: %s must be between %d and %d", name, minVal, maxVal)
	}
	return val, nil
}

// ListMonitorChecksHandler handles GET /api/v1/monitors/{id}/checks to retrieve recent check history newest first.
func ListMonitorChecksHandler(repo MonitorRepository, logger *slog.Logger) http.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}

	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := parseMonitorID(w, r)
		if !ok {
			return
		}

		limit, err := parseBoundedIntQueryParam(
			r,
			"limit",
			model.DefaultCheckHistoryLimit,
			model.MinCheckHistoryLimit,
			model.MaxCheckHistoryLimit,
		)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		checks, err := repo.ListChecks(r.Context(), id, limit)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeError(w, http.StatusNotFound, "monitor not found")
				return
			}
			logger.Error("failed to list monitor checks", slog.String("id", id.String()), slog.String("error", err.Error()))
			writeError(w, http.StatusInternalServerError, "failed to list monitor checks")
			return
		}

		if checks == nil {
			checks = make([]*model.CheckResult, 0)
		}

		writeJSON(w, http.StatusOK, ListChecksResponse{
			MonitorID: id,
			Limit:     limit,
			Checks:    checks,
		})
	}
}

// GetMonitorStatsHandler handles GET /api/v1/monitors/{id}/stats to compute uptime and latency statistics.
func GetMonitorStatsHandler(repo MonitorRepository, logger *slog.Logger) http.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}

	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := parseMonitorID(w, r)
		if !ok {
			return
		}

		hours, err := parseBoundedIntQueryParam(
			r,
			"hours",
			model.DefaultStatsWindowHours,
			model.MinStatsWindowHours,
			model.MaxStatsWindowHours,
		)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		stats, err := repo.GetMonitorStats(r.Context(), id, hours)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeError(w, http.StatusNotFound, "monitor not found")
				return
			}
			logger.Error("failed to get monitor stats", slog.String("id", id.String()), slog.String("error", err.Error()))
			writeError(w, http.StatusInternalServerError, "failed to get monitor stats")
			return
		}

		writeJSON(w, http.StatusOK, stats)
	}
}
