package model

import (
	"time"

	"github.com/google/uuid"
)

// MaxErrorMessageLength defines the maximum character length for persisted check error messages,
// matching the VARCHAR(1024) column limit in PostgreSQL.
const MaxErrorMessageLength = 1024

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
