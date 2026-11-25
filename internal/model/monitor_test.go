package model_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"pulse/internal/model"
)

func TestNewMonitor_DefaultsAndInvariants(t *testing.T) {
	before := time.Now().UTC()

	m, err := model.NewMonitor(model.CreateMonitorParams{
		Name: "Production API",
		URL:  "https://example.com/healthz",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	after := time.Now().UTC()

	if m.ID == uuid.Nil {
		t.Errorf("expected non-nil UUID")
	}
	if m.Name != "Production API" {
		t.Errorf("expected name 'Production API', got %q", m.Name)
	}
	if m.URL != "https://example.com/healthz" {
		t.Errorf("expected URL 'https://example.com/healthz', got %q", m.URL)
	}
	if m.IntervalSeconds != model.DefaultIntervalSeconds {
		t.Errorf("expected default interval %d, got %d", model.DefaultIntervalSeconds, m.IntervalSeconds)
	}
	if m.TimeoutSeconds != model.DefaultTimeoutSeconds {
		t.Errorf("expected default timeout %d, got %d", model.DefaultTimeoutSeconds, m.TimeoutSeconds)
	}
	if m.CurrentStatus != model.StatusUnknown {
		t.Errorf("expected initial status UNKNOWN, got %q", m.CurrentStatus)
	}
	if m.ConsecutiveFailures != 0 {
		t.Errorf("expected initial failures 0, got %d", m.ConsecutiveFailures)
	}
	if m.LastCheckedAt != nil {
		t.Errorf("expected last_checked_at to be nil, got %v", m.LastCheckedAt)
	}
	if m.LastSuccessAt != nil {
		t.Errorf("expected last_success_at to be nil, got %v", m.LastSuccessAt)
	}

	// Invariant: next_check_at equals created_at (due immediately)
	if !m.NextCheckAt.Equal(m.CreatedAt) {
		t.Errorf("expected next_check_at (%v) to equal created_at (%v)", m.NextCheckAt, m.CreatedAt)
	}
	if !m.CreatedAt.Equal(m.UpdatedAt) {
		t.Errorf("expected created_at (%v) to equal updated_at (%v)", m.CreatedAt, m.UpdatedAt)
	}
	if m.CreatedAt.Before(before) || m.CreatedAt.After(after) {
		t.Errorf("created_at %v not within expected window [%v, %v]", m.CreatedAt, before, after)
	}
}

func TestNewMonitor_CustomIntervalAndTimeout(t *testing.T) {
	m, err := model.NewMonitor(model.CreateMonitorParams{
		Name:            "Custom Service",
		URL:             "http://example.com:8080/ping",
		IntervalSeconds: 120,
		TimeoutSeconds:  10,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if m.IntervalSeconds != 120 {
		t.Errorf("expected interval 120, got %d", m.IntervalSeconds)
	}
	if m.TimeoutSeconds != 10 {
		t.Errorf("expected timeout 10, got %d", m.TimeoutSeconds)
	}
}

func TestNewMonitor_ValidationErrors(t *testing.T) {
	tests := []struct {
		name        string
		params      model.CreateMonitorParams
		expectedErr error
	}{
		{
			name:        "empty name",
			params:      model.CreateMonitorParams{Name: "", URL: "https://example.com"},
			expectedErr: model.ErrEmptyName,
		},
		{
			name:        "whitespace name",
			params:      model.CreateMonitorParams{Name: "   ", URL: "https://example.com"},
			expectedErr: model.ErrEmptyName,
		},
		{
			name:        "name exceeding 255 chars",
			params:      model.CreateMonitorParams{Name: strings.Repeat("a", 256), URL: "https://example.com"},
			expectedErr: model.ErrNameTooLong,
		},
		{
			name:        "name exceeding 255 multibyte unicode chars",
			params:      model.CreateMonitorParams{Name: strings.Repeat("世", 256), URL: "https://example.com"},
			expectedErr: model.ErrNameTooLong,
		},
		{
			name:        "empty url",
			params:      model.CreateMonitorParams{Name: "Test", URL: ""},
			expectedErr: model.ErrEmptyURL,
		},
		{
			name:        "relative url",
			params:      model.CreateMonitorParams{Name: "Test", URL: "/path/to/health"},
			expectedErr: model.ErrInvalidURL,
		},
		{
			name:        "invalid scheme ftp",
			params:      model.CreateMonitorParams{Name: "Test", URL: "ftp://example.com/file"},
			expectedErr: model.ErrInvalidScheme,
		},
		{
			name:        "embedded credentials in url",
			params:      model.CreateMonitorParams{Name: "Test", URL: "https://user:password@example.com/api"},
			expectedErr: model.ErrCredentialsInURL,
		},
		{
			name:        "url without host",
			params:      model.CreateMonitorParams{Name: "Test", URL: "http:///path"},
			expectedErr: model.ErrEmptyHost,
		},
		{
			name: "interval below minimum (29s)",
			params: model.CreateMonitorParams{
				Name:            "Test",
				URL:             "https://example.com",
				IntervalSeconds: 29,
			},
			expectedErr: model.ErrInvalidInterval,
		},
		{
			name: "interval above maximum (86401s)",
			params: model.CreateMonitorParams{
				Name:            "Test",
				URL:             "https://example.com",
				IntervalSeconds: 86401,
			},
			expectedErr: model.ErrInvalidInterval,
		},
		{
			name: "timeout below minimum (0s when explicitly negative)",
			params: model.CreateMonitorParams{
				Name:            "Test",
				URL:             "https://example.com",
				IntervalSeconds: 60,
				TimeoutSeconds:  -1,
			},
			expectedErr: model.ErrInvalidTimeout,
		},
		{
			name: "timeout above maximum (31s)",
			params: model.CreateMonitorParams{
				Name:            "Test",
				URL:             "https://example.com",
				IntervalSeconds: 60,
				TimeoutSeconds:  31,
			},
			expectedErr: model.ErrInvalidTimeout,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := model.NewMonitor(tc.params)
			if err == nil {
				t.Fatalf("expected error %v, got nil", tc.expectedErr)
			}
			if !errors.Is(err, tc.expectedErr) && err.Error() != tc.expectedErr.Error() {
				t.Errorf("expected error %q, got %q", tc.expectedErr, err)
			}
		})
	}
}

func TestMonitor_InitializeCreationDefaults(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	m := &model.Monitor{
		Name: "Explicit Init",
		URL:  "https://example.com",
	}

	m.InitializeCreationDefaults(now)

	if m.ID == uuid.Nil {
		t.Error("expected generated UUID")
	}
	if m.IntervalSeconds != model.DefaultIntervalSeconds {
		t.Errorf("expected default interval %d, got %d", model.DefaultIntervalSeconds, m.IntervalSeconds)
	}
	if m.TimeoutSeconds != model.DefaultTimeoutSeconds {
		t.Errorf("expected default timeout %d, got %d", model.DefaultTimeoutSeconds, m.TimeoutSeconds)
	}
	if m.CurrentStatus != model.StatusUnknown {
		t.Errorf("expected status UNKNOWN, got %q", m.CurrentStatus)
	}
	if m.ConsecutiveFailures != 0 {
		t.Errorf("expected consecutive failures 0, got %d", m.ConsecutiveFailures)
	}
	if !m.NextCheckAt.Equal(now) {
		t.Errorf("expected next_check_at %v, got %v", now, m.NextCheckAt)
	}
	if !m.CreatedAt.Equal(now) {
		t.Errorf("expected created_at %v, got %v", now, m.CreatedAt)
	}
	if !m.UpdatedAt.Equal(now) {
		t.Errorf("expected updated_at %v, got %v", now, m.UpdatedAt)
	}
}

func TestMonitor_UnicodeNameValidation(t *testing.T) {
	t.Run("255 multibyte Unicode characters are accepted in NewMonitor", func(t *testing.T) {
		name255 := strings.Repeat("世", 255)
		m, err := model.NewMonitor(model.CreateMonitorParams{
			Name: name255,
			URL:  "https://example.com",
		})
		if err != nil {
			t.Fatalf("expected 255 multibyte Unicode characters to be accepted, got error: %v", err)
		}
		if m.Name != name255 {
			t.Errorf("expected name to be preserved")
		}
	})

	t.Run("255 multibyte Unicode characters are accepted in Validate", func(t *testing.T) {
		name255 := strings.Repeat("世", 255)
		m := &model.Monitor{
			Name:            name255,
			URL:             "https://example.com",
			IntervalSeconds: 60,
			TimeoutSeconds:  5,
			CurrentStatus:   model.StatusUnknown,
		}
		if err := m.Validate(); err != nil {
			t.Fatalf("expected Validate() to accept 255 multibyte Unicode characters, got %v", err)
		}
	})

	t.Run("256 multibyte Unicode characters are rejected in NewMonitor", func(t *testing.T) {
		name256 := strings.Repeat("世", 256)
		_, err := model.NewMonitor(model.CreateMonitorParams{
			Name: name256,
			URL:  "https://example.com",
		})
		if !errors.Is(err, model.ErrNameTooLong) {
			t.Fatalf("expected ErrNameTooLong for 256 multibyte Unicode characters, got: %v", err)
		}
	})

	t.Run("256 multibyte Unicode characters are rejected in Validate", func(t *testing.T) {
		name256 := strings.Repeat("世", 256)
		m := &model.Monitor{
			Name:            name256,
			URL:             "https://example.com",
			IntervalSeconds: 60,
			TimeoutSeconds:  5,
			CurrentStatus:   model.StatusUnknown,
		}
		if err := m.Validate(); !errors.Is(err, model.ErrNameTooLong) {
			t.Fatalf("expected Validate() to reject 256 multibyte Unicode characters with ErrNameTooLong, got %v", err)
		}
	})

	t.Run("ASCII 255 chars accepted and 256 chars rejected", func(t *testing.T) {
		ascii255 := strings.Repeat("a", 255)
		m, err := model.NewMonitor(model.CreateMonitorParams{
			Name: ascii255,
			URL:  "https://example.com",
		})
		if err != nil {
			t.Fatalf("expected 255 ASCII characters to be accepted, got error: %v", err)
		}
		if m.Name != ascii255 {
			t.Errorf("expected name to be preserved")
		}

		ascii256 := strings.Repeat("a", 256)
		_, err = model.NewMonitor(model.CreateMonitorParams{
			Name: ascii256,
			URL:  "https://example.com",
		})
		if !errors.Is(err, model.ErrNameTooLong) {
			t.Fatalf("expected ErrNameTooLong for 256 ASCII characters, got: %v", err)
		}
	})
}

func TestClaimedMonitor_NeverExposesClaimTokenInJSON(t *testing.T) {
	now := time.Now().UTC()
	token := uuid.New()
	mon, err := model.NewMonitor(model.CreateMonitorParams{
		Name: "JSON Safety Monitor",
		URL:  "https://example.com/health",
	})
	if err != nil {
		t.Fatalf("NewMonitor failed: %v", err)
	}

	monJSON, err := json.Marshal(mon)
	if err != nil {
		t.Fatalf("json.Marshal(Monitor) failed: %v", err)
	}
	if strings.Contains(string(monJSON), "claim_token") || strings.Contains(string(monJSON), "claim_expires_at") {
		t.Fatalf("Monitor JSON unexpectedly exposed claim fields: %s", monJSON)
	}

	claimed := model.ClaimedMonitor{
		Monitor:        mon,
		ClaimToken:     token,
		ClaimExpiresAt: now.Add(35 * time.Second),
	}
	claimedJSON, err := json.Marshal(claimed)
	if err != nil {
		t.Fatalf("json.Marshal(ClaimedMonitor) failed: %v", err)
	}
	if strings.Contains(string(claimedJSON), token.String()) || strings.Contains(string(claimedJSON), "claim_token") || strings.Contains(string(claimedJSON), "ClaimToken") {
		t.Fatalf("ClaimedMonitor JSON unexpectedly exposed claim token: %s", claimedJSON)
	}
}
