package model

import (
	"time"

	"github.com/google/uuid"
)

// MaxErrorMessageLength defines the maximum character length for persisted check error messages,
// matching the VARCHAR(1024) column limit in PostgreSQL.
const MaxErrorMessageLength = 1024

const (
	// DefaultCheckHistoryLimit is the default number of check history rows returned.
	DefaultCheckHistoryLimit = 50
	// MinCheckHistoryLimit is the minimum allowed limit for check history queries.
	MinCheckHistoryLimit = 1
	// MaxCheckHistoryLimit is the maximum allowed limit for check history queries.
	MaxCheckHistoryLimit = 200

	// DefaultStatsWindowHours is the default sliding window size in hours for monitor stats.
	DefaultStatsWindowHours = 24
	// MinStatsWindowHours is the minimum allowed sliding window size in hours for monitor stats.
	MinStatsWindowHours = 1
	// MaxStatsWindowHours is the maximum allowed sliding window size in hours for monitor stats.
	MaxStatsWindowHours = 720
)

// CheckResult represents a single executed health check record stored in PostgreSQL.
type CheckResult struct {
	ID           uuid.UUID `json:"id"`
	MonitorID    uuid.UUID `json:"monitor_id"`
	CheckedAt    time.Time `json:"checked_at"`
	Success      bool      `json:"success"`
	HTTPStatus   *int      `json:"http_status"`
	LatencyMs    int64     `json:"latency_ms"`
	ErrorMessage *string   `json:"error_message"`
}

// MonitorStats represents aggregated uptime and latency statistics for a monitor over a sliding window.
// Availability is percentage (0-100) over all checks in the window, or nil when total_checks is 0.
// AverageLatencyMs is the arithmetic mean of latency_ms across successful checks only, or nil when successful_checks is 0.
type MonitorStats struct {
	MonitorID        uuid.UUID `json:"monitor_id"`
	WindowHours      int       `json:"window_hours"`
	TotalChecks      int64     `json:"total_checks"`
	SuccessfulChecks int64     `json:"successful_checks"`
	FailedChecks     int64     `json:"failed_checks"`
	Availability     *float64  `json:"availability"`
	AverageLatencyMs *float64  `json:"average_latency_ms"`
}
