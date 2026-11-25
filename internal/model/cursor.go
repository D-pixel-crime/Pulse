package model

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	// DefaultMonitorListLimit is the default page size for listing monitors.
	DefaultMonitorListLimit = 50
	// MinMonitorListLimit is the minimum allowed page size for listing monitors.
	MinMonitorListLimit = 1
	// MaxMonitorListLimit is the maximum allowed page size for listing monitors.
	MaxMonitorListLimit = 200
)

// MonitorCursor represents a deterministic pagination cursor based on (created_at, id).
type MonitorCursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// EncodeCursor produces a URL-safe base64-encoded string representation of the cursor tuple (created_at, id).
func EncodeCursor(createdAt time.Time, id uuid.UUID) string {
	raw := fmt.Sprintf("%s,%s", createdAt.UTC().Format(time.RFC3339Nano), id.String())
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor validates and decodes a URL-safe base64 cursor string into a MonitorCursor.
// It strictly validates the base64 format, timestamp parsing, and non-nil UUID.
func DecodeCursor(cursorStr string) (*MonitorCursor, error) {
	cursorStr = strings.TrimSpace(cursorStr)
	if cursorStr == "" {
		return nil, errors.New("cursor must not be empty")
	}

	decoded, err := base64.RawURLEncoding.DecodeString(cursorStr)
	if err != nil {
		// Fallback to padded URL encoding in case client included padding
		decoded, err = base64.URLEncoding.DecodeString(cursorStr)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: malformed base64 encoding: %w", err)
		}
	}

	raw := string(decoded)
	parts := strings.Split(raw, ",")
	if len(parts) != 2 {
		return nil, errors.New("invalid cursor: expected 2 comma-separated values")
	}

	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid cursor timestamp: %w", err)
	}
	if t.IsZero() {
		return nil, errors.New("invalid cursor timestamp: must not be zero")
	}

	id, err := uuid.Parse(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid cursor id: %w", err)
	}
	if id == uuid.Nil {
		return nil, errors.New("invalid cursor id: must not be nil UUID")
	}

	return &MonitorCursor{
		CreatedAt: t.UTC(),
		ID:        id,
	}, nil
}
