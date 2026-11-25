package model

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Status represents the operational health status of a target monitor.
type Status string

const (
	// StatusUnknown indicates a monitor that has not yet completed enough checks to determine health.
	StatusUnknown Status = "UNKNOWN"
	// StatusUp indicates a monitor whose last check was successful.
	StatusUp Status = "UP"
	// StatusDown indicates a monitor with 3 or more consecutive check failures.
	StatusDown Status = "DOWN"

	// DefaultIntervalSeconds defines the standard check frequency when omitted.
	DefaultIntervalSeconds = 60
	// MinIntervalSeconds defines the lower bound for check intervals.
	MinIntervalSeconds = 30
	// MaxIntervalSeconds defines the upper bound for check intervals (24 hours).
	MaxIntervalSeconds = 86400

	// DefaultTimeoutSeconds defines the standard outbound request timeout when omitted.
	DefaultTimeoutSeconds = 5
	// MinTimeoutSeconds defines the lower bound for request timeouts.
	MinTimeoutSeconds = 1
	// MaxTimeoutSeconds defines the upper bound for request timeouts.
	MaxTimeoutSeconds = 30
)

var (
	ErrEmptyName        = errors.New("name must not be empty")
	ErrNameTooLong      = errors.New("name must not exceed 255 characters")
	ErrEmptyURL         = errors.New("url must not be empty")
	ErrInvalidURL       = errors.New("url must be a valid absolute URL")
	ErrInvalidScheme    = errors.New("url scheme must be http or https")
	ErrCredentialsInURL = errors.New("url must not contain embedded user credentials")
	ErrEmptyHost        = errors.New("url must contain a valid host")
	ErrInvalidInterval  = fmt.Errorf("interval_seconds must be between %d and %d", MinIntervalSeconds, MaxIntervalSeconds)
	ErrInvalidTimeout   = fmt.Errorf("timeout_seconds must be between %d and %d", MinTimeoutSeconds, MaxTimeoutSeconds)
	ErrTimeoutExceeds   = errors.New("timeout_seconds must not exceed interval_seconds")
)

// Monitor represents a target endpoint monitored by Pulse.
type Monitor struct {
	ID                  uuid.UUID  `json:"id"`
	Name                string     `json:"name"`
	URL                 string     `json:"url"`
	IntervalSeconds     int        `json:"interval_seconds"`
	TimeoutSeconds      int        `json:"timeout_seconds"`
	CurrentStatus       Status     `json:"current_status"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	LastCheckedAt       *time.Time `json:"last_checked_at"`
	LastSuccessAt       *time.Time `json:"last_success_at"`
	NextCheckAt         time.Time  `json:"next_check_at"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
}

// ClaimedMonitor pairs a due Monitor with its PostgreSQL fencing claim token
// and lease expiration timestamp. Claim fields are internal coordination state
// and are excluded from JSON serialization.
type ClaimedMonitor struct {
	Monitor        *Monitor  `json:"-"`
	ClaimToken     uuid.UUID `json:"-"`
	ClaimExpiresAt time.Time `json:"-"`
}

// CreateMonitorParams holds input parameters for registering a new monitor.
type CreateMonitorParams struct {
	Name            string `json:"name"`
	URL             string `json:"url"`
	IntervalSeconds int    `json:"interval_seconds"`
	TimeoutSeconds  int    `json:"timeout_seconds"`
}

// NewMonitor validates input parameters, applies specification defaults,
// and constructs a fresh Monitor with proper initial invariants.
func NewMonitor(params CreateMonitorParams) (*Monitor, error) {
	name := strings.TrimSpace(params.Name)
	if name == "" {
		return nil, ErrEmptyName
	}
	if utf8.RuneCountInString(name) > 255 {
		return nil, ErrNameTooLong
	}

	rawURL := strings.TrimSpace(params.URL)
	if rawURL == "" {
		return nil, ErrEmptyURL
	}
	parsedURL, err := url.Parse(rawURL)
	if err != nil || !parsedURL.IsAbs() {
		return nil, ErrInvalidURL
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return nil, ErrInvalidScheme
	}
	if parsedURL.User != nil {
		return nil, ErrCredentialsInURL
	}
	if parsedURL.Host == "" {
		return nil, ErrEmptyHost
	}

	interval := params.IntervalSeconds
	if interval == 0 {
		interval = DefaultIntervalSeconds
	}
	if interval < MinIntervalSeconds || interval > MaxIntervalSeconds {
		return nil, ErrInvalidInterval
	}

	timeout := params.TimeoutSeconds
	if timeout == 0 {
		timeout = DefaultTimeoutSeconds
	}
	if timeout < MinTimeoutSeconds || timeout > MaxTimeoutSeconds {
		return nil, ErrInvalidTimeout
	}

	if timeout > interval {
		return nil, ErrTimeoutExceeds
	}

	now := time.Now().UTC()
	return &Monitor{
		ID:                  uuid.New(),
		Name:                name,
		URL:                 rawURL,
		IntervalSeconds:     interval,
		TimeoutSeconds:      timeout,
		CurrentStatus:       StatusUnknown,
		ConsecutiveFailures: 0,
		LastCheckedAt:       nil,
		LastSuccessAt:       nil,
		NextCheckAt:         now,
		CreatedAt:           now,
		UpdatedAt:           now,
	}, nil
}

// Validate checks the Monitor's fields against domain rules and database constraints.
func (m *Monitor) Validate() error {
	name := strings.TrimSpace(m.Name)
	if name == "" {
		return ErrEmptyName
	}
	if utf8.RuneCountInString(name) > 255 {
		return ErrNameTooLong
	}

	rawURL := strings.TrimSpace(m.URL)
	if rawURL == "" {
		return ErrEmptyURL
	}
	parsedURL, err := url.Parse(rawURL)
	if err != nil || !parsedURL.IsAbs() {
		return ErrInvalidURL
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return ErrInvalidScheme
	}
	if parsedURL.User != nil {
		return ErrCredentialsInURL
	}
	if parsedURL.Host == "" {
		return ErrEmptyHost
	}

	if m.IntervalSeconds < MinIntervalSeconds || m.IntervalSeconds > MaxIntervalSeconds {
		return ErrInvalidInterval
	}
	if m.TimeoutSeconds < MinTimeoutSeconds || m.TimeoutSeconds > MaxTimeoutSeconds {
		return ErrInvalidTimeout
	}
	if m.TimeoutSeconds > m.IntervalSeconds {
		return ErrTimeoutExceeds
	}

	switch m.CurrentStatus {
	case StatusUnknown, StatusUp, StatusDown:
	default:
		return fmt.Errorf("invalid current_status: %q", m.CurrentStatus)
	}

	if m.ConsecutiveFailures < 0 {
		return errors.New("consecutive_failures must not be negative")
	}

	return nil
}

// InitializeCreationDefaults sets the mandatory initial state attributes for a newly created monitor.
// Invariant: current_status is UNKNOWN, consecutive_failures is 0, last_checked_at and last_success_at
// are nil, and next_check_at equals created_at (due immediately).
func (m *Monitor) InitializeCreationDefaults(now time.Time) {
	if m.ID == uuid.Nil {
		m.ID = uuid.New()
	}
	if m.IntervalSeconds == 0 {
		m.IntervalSeconds = DefaultIntervalSeconds
	}
	if m.TimeoutSeconds == 0 {
		m.TimeoutSeconds = DefaultTimeoutSeconds
	}
	m.CurrentStatus = StatusUnknown
	m.ConsecutiveFailures = 0
	m.LastCheckedAt = nil
	m.LastSuccessAt = nil
	m.CreatedAt = now.UTC()
	m.UpdatedAt = now.UTC()
	m.NextCheckAt = now.UTC()
}
