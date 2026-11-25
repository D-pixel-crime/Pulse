package model

import (
	"errors"
	"fmt"
	"time"
)

const (
	// DownFailureThreshold is the number of consecutive failed checks required
	// before a monitor transitions to DOWN.
	DownFailureThreshold = 3
)

var (
	// ErrZeroCheckedAt indicates a check observation had a zero observation timestamp.
	ErrZeroCheckedAt = errors.New("checked_at must not be zero")
)

// HealthState holds the updated monitor health fields computed by a single check transition.
// Persistent scheduling timestamps (next_check_at, updated_at) are owned by PostgreSQL.
type HealthState struct {
	CurrentStatus       Status
	ConsecutiveFailures int
	LastCheckedAt       *time.Time
	LastSuccessAt       *time.Time
}

// TransitionMonitorHealth computes the next monitor health state from the monitor's prior
// state and a single completed check observation.
//
// Rules:
//   - last_checked_at is always set to checkedAt (the observation time).
//   - For successful checks: status becomes UP, consecutive_failures resets to 0,
//     and last_success_at is set to checkedAt.
//   - For unhealthy checks: consecutive_failures increments by 1; current_status changes
//     to DOWN only when consecutive_failures reaches DownFailureThreshold (3); otherwise
//     the previous current_status (including UNKNOWN or UP) is preserved; and last_success_at
//     is preserved.
func TransitionMonitorHealth(
	currentStatus Status,
	consecutiveFailures int,
	lastSuccessAt *time.Time,
	success bool,
	checkedAt time.Time,
) (HealthState, error) {
	switch currentStatus {
	case StatusUnknown, StatusUp, StatusDown:
	default:
		return HealthState{}, fmt.Errorf("invalid current_status: %q", currentStatus)
	}

	if consecutiveFailures < 0 {
		return HealthState{}, errors.New("consecutive_failures must not be negative")
	}

	if checkedAt.IsZero() {
		return HealthState{}, ErrZeroCheckedAt
	}

	checkedAtUTC := checkedAt.UTC()
	lastCheckedCopy := checkedAtUTC

	if success {
		lastSuccessCopy := checkedAtUTC
		return HealthState{
			CurrentStatus:       StatusUp,
			ConsecutiveFailures: 0,
			LastCheckedAt:       &lastCheckedCopy,
			LastSuccessAt:       &lastSuccessCopy,
		}, nil
	}

	nextFailures := consecutiveFailures + 1
	nextStatus := currentStatus
	if nextFailures >= DownFailureThreshold {
		nextStatus = StatusDown
	}

	var nextLastSuccess *time.Time
	if lastSuccessAt != nil {
		t := lastSuccessAt.UTC()
		nextLastSuccess = &t
	}

	return HealthState{
		CurrentStatus:       nextStatus,
		ConsecutiveFailures: nextFailures,
		LastCheckedAt:       &lastCheckedCopy,
		LastSuccessAt:       nextLastSuccess,
	}, nil
}
