package model_test

import (
	"errors"
	"testing"
	"time"

	"pulse/internal/model"
)

func TestTransitionMonitorHealth_UnknownToDownAfterThreeFailures(t *testing.T) {
	t1 := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)

	// UNKNOWN -> failure 1: remains UNKNOWN, consecutive_failures = 1
	s1, err := model.TransitionMonitorHealth(model.StatusUnknown, 0, nil, false, t1)
	if err != nil {
		t.Fatalf("failure 1 returned unexpected error: %v", err)
	}
	if s1.CurrentStatus != model.StatusUnknown {
		t.Errorf("failure 1: expected status UNKNOWN, got %q", s1.CurrentStatus)
	}
	if s1.ConsecutiveFailures != 1 {
		t.Errorf("failure 1: expected consecutive_failures 1, got %d", s1.ConsecutiveFailures)
	}
	if s1.LastCheckedAt == nil || !s1.LastCheckedAt.Equal(t1) {
		t.Errorf("failure 1: expected last_checked_at %v, got %v", t1, s1.LastCheckedAt)
	}
	if s1.LastSuccessAt != nil {
		t.Errorf("failure 1: expected nil last_success_at, got %v", s1.LastSuccessAt)
	}

	// failure 1 -> failure 2: remains UNKNOWN, consecutive_failures = 2
	t2 := t1.Add(60 * time.Second)
	s2, err := model.TransitionMonitorHealth(s1.CurrentStatus, s1.ConsecutiveFailures, s1.LastSuccessAt, false, t2)
	if err != nil {
		t.Fatalf("failure 2 returned unexpected error: %v", err)
	}
	if s2.CurrentStatus != model.StatusUnknown {
		t.Errorf("failure 2: expected status UNKNOWN, got %q", s2.CurrentStatus)
	}
	if s2.ConsecutiveFailures != 2 {
		t.Errorf("failure 2: expected consecutive_failures 2, got %d", s2.ConsecutiveFailures)
	}
	if s2.LastCheckedAt == nil || !s2.LastCheckedAt.Equal(t2) {
		t.Errorf("failure 2: expected last_checked_at %v, got %v", t2, s2.LastCheckedAt)
	}
	if s2.LastSuccessAt != nil {
		t.Errorf("failure 2: expected nil last_success_at, got %v", s2.LastSuccessAt)
	}

	// failure 2 -> failure 3: transitions to DOWN, consecutive_failures = 3
	t3 := t2.Add(60 * time.Second)
	s3, err := model.TransitionMonitorHealth(s2.CurrentStatus, s2.ConsecutiveFailures, s2.LastSuccessAt, false, t3)
	if err != nil {
		t.Fatalf("failure 3 returned unexpected error: %v", err)
	}
	if s3.CurrentStatus != model.StatusDown {
		t.Errorf("failure 3: expected status DOWN, got %q", s3.CurrentStatus)
	}
	if s3.ConsecutiveFailures != 3 {
		t.Errorf("failure 3: expected consecutive_failures 3, got %d", s3.ConsecutiveFailures)
	}
	if s3.LastCheckedAt == nil || !s3.LastCheckedAt.Equal(t3) {
		t.Errorf("failure 3: expected last_checked_at %v, got %v", t3, s3.LastCheckedAt)
	}
	if s3.LastSuccessAt != nil {
		t.Errorf("failure 3: expected nil last_success_at, got %v", s3.LastSuccessAt)
	}

	// failure 4 while DOWN: remains DOWN, consecutive_failures = 4
	t4 := t3.Add(60 * time.Second)
	s4, err := model.TransitionMonitorHealth(s3.CurrentStatus, s3.ConsecutiveFailures, s3.LastSuccessAt, false, t4)
	if err != nil {
		t.Fatalf("failure 4 returned unexpected error: %v", err)
	}
	if s4.CurrentStatus != model.StatusDown {
		t.Errorf("failure 4: expected status DOWN, got %q", s4.CurrentStatus)
	}
	if s4.ConsecutiveFailures != 4 {
		t.Errorf("failure 4: expected consecutive_failures 4, got %d", s4.ConsecutiveFailures)
	}
}

func TestTransitionMonitorHealth_SuccessResetsFailuresAndSetsUp(t *testing.T) {
	checkedAt := time.Date(2026, 11, 25, 11, 0, 0, 0, time.UTC)
	oldSuccess := checkedAt.Add(-10 * time.Minute)

	cases := []struct {
		name        string
		prevStatus  model.Status
		prevFails   int
		prevSuccess *time.Time
	}{
		{
			name:        "UNKNOWN with 0 failures -> UP",
			prevStatus:  model.StatusUnknown,
			prevFails:   0,
			prevSuccess: nil,
		},
		{
			name:        "UNKNOWN with 2 failures -> UP and reset",
			prevStatus:  model.StatusUnknown,
			prevFails:   2,
			prevSuccess: nil,
		},
		{
			name:        "UP with 2 failures -> UP and reset",
			prevStatus:  model.StatusUp,
			prevFails:   2,
			prevSuccess: &oldSuccess,
		},
		{
			name:        "DOWN with 5 failures -> UP and reset",
			prevStatus:  model.StatusDown,
			prevFails:   5,
			prevSuccess: &oldSuccess,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next, err := model.TransitionMonitorHealth(
				tc.prevStatus,
				tc.prevFails,
				tc.prevSuccess,
				true,
				checkedAt,
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if next.CurrentStatus != model.StatusUp {
				t.Errorf("expected status UP, got %q", next.CurrentStatus)
			}
			if next.ConsecutiveFailures != 0 {
				t.Errorf("expected consecutive_failures 0, got %d", next.ConsecutiveFailures)
			}
			if next.LastCheckedAt == nil || !next.LastCheckedAt.Equal(checkedAt) {
				t.Errorf("expected last_checked_at %v, got %v", checkedAt, next.LastCheckedAt)
			}
			if next.LastSuccessAt == nil || !next.LastSuccessAt.Equal(checkedAt) {
				t.Errorf("expected last_success_at %v, got %v", checkedAt, next.LastSuccessAt)
			}
		})
	}
}

func TestTransitionMonitorHealth_UpWithOneOrTwoFailuresRemainsUp(t *testing.T) {
	initialSuccess := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)

	// UP (0 failures) -> failure 1: remains UP, failures = 1, last_success_at preserved
	t1 := initialSuccess.Add(30 * time.Second)
	s1, err := model.TransitionMonitorHealth(model.StatusUp, 0, &initialSuccess, false, t1)
	if err != nil {
		t.Fatalf("failure 1 returned unexpected error: %v", err)
	}
	if s1.CurrentStatus != model.StatusUp {
		t.Errorf("failure 1: expected status UP, got %q", s1.CurrentStatus)
	}
	if s1.ConsecutiveFailures != 1 {
		t.Errorf("failure 1: expected consecutive_failures 1, got %d", s1.ConsecutiveFailures)
	}
	if s1.LastSuccessAt == nil || !s1.LastSuccessAt.Equal(initialSuccess) {
		t.Errorf("failure 1: expected preserved last_success_at %v, got %v", initialSuccess, s1.LastSuccessAt)
	}

	// UP (1 failure) -> failure 2: remains UP, failures = 2, last_success_at preserved
	t2 := t1.Add(30 * time.Second)
	s2, err := model.TransitionMonitorHealth(s1.CurrentStatus, s1.ConsecutiveFailures, s1.LastSuccessAt, false, t2)
	if err != nil {
		t.Fatalf("failure 2 returned unexpected error: %v", err)
	}
	if s2.CurrentStatus != model.StatusUp {
		t.Errorf("failure 2: expected status UP, got %q", s2.CurrentStatus)
	}
	if s2.ConsecutiveFailures != 2 {
		t.Errorf("failure 2: expected consecutive_failures 2, got %d", s2.ConsecutiveFailures)
	}
	if s2.LastSuccessAt == nil || !s2.LastSuccessAt.Equal(initialSuccess) {
		t.Errorf("failure 2: expected preserved last_success_at %v, got %v", initialSuccess, s2.LastSuccessAt)
	}
	if s2.LastCheckedAt == nil || !s2.LastCheckedAt.Equal(t2) {
		t.Errorf("failure 2: expected last_checked_at %v, got %v", t2, s2.LastCheckedAt)
	}

	// Verify pointer isolation: mutating initialSuccess must not mutate s2.LastSuccessAt
	mutated := initialSuccess.Add(time.Hour)
	initialSuccess = mutated
	if s2.LastSuccessAt.Equal(mutated) {
		t.Errorf("expected LastSuccessAt copy not to alias caller pointer")
	}

	// UP (2 failures) -> failure 3: transitions to DOWN, failures = 3, last_success_at preserved
	t3 := t2.Add(30 * time.Second)
	s3, err := model.TransitionMonitorHealth(s2.CurrentStatus, s2.ConsecutiveFailures, s2.LastSuccessAt, false, t3)
	if err != nil {
		t.Fatalf("failure 3 returned unexpected error: %v", err)
	}
	if s3.CurrentStatus != model.StatusDown {
		t.Errorf("failure 3: expected status DOWN, got %q", s3.CurrentStatus)
	}
	if s3.ConsecutiveFailures != 3 {
		t.Errorf("failure 3: expected consecutive_failures 3, got %d", s3.ConsecutiveFailures)
	}
	if s3.LastSuccessAt == nil || s3.LastSuccessAt.Equal(mutated) {
		t.Errorf("failure 3: expected preserved original last_success_at, got %v", s3.LastSuccessAt)
	}

	// DOWN (3 failures) -> failure 4 -> remains DOWN (failures=4) -> success -> recovers to UP (failures=0)
	t4 := t3.Add(30 * time.Second)
	s4, err := model.TransitionMonitorHealth(s3.CurrentStatus, s3.ConsecutiveFailures, s3.LastSuccessAt, false, t4)
	if err != nil {
		t.Fatalf("failure 4 returned unexpected error: %v", err)
	}
	if s4.CurrentStatus != model.StatusDown || s4.ConsecutiveFailures != 4 {
		t.Fatalf("failure 4: expected DOWN/4, got %s/%d", s4.CurrentStatus, s4.ConsecutiveFailures)
	}
	t5 := t4.Add(30 * time.Second)
	s5, err := model.TransitionMonitorHealth(s4.CurrentStatus, s4.ConsecutiveFailures, s4.LastSuccessAt, true, t5)
	if err != nil {
		t.Fatalf("recovery returned unexpected error: %v", err)
	}
	if s5.CurrentStatus != model.StatusUp || s5.ConsecutiveFailures != 0 {
		t.Fatalf("recovery: expected UP/0, got %s/%d", s5.CurrentStatus, s5.ConsecutiveFailures)
	}
	if s5.LastSuccessAt == nil || !s5.LastSuccessAt.Equal(t5) {
		t.Errorf("recovery: expected last_success_at=%v, got %v", t5, s5.LastSuccessAt)
	}
}

func TestTransitionMonitorHealth_ValidationErrors(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

	t.Run("zero checked_at", func(t *testing.T) {
		_, err := model.TransitionMonitorHealth(model.StatusUnknown, 0, nil, true, time.Time{})
		if !errors.Is(err, model.ErrZeroCheckedAt) {
			t.Errorf("expected ErrZeroCheckedAt, got %v", err)
		}
	})

	t.Run("negative consecutive failures", func(t *testing.T) {
		_, err := model.TransitionMonitorHealth(model.StatusUnknown, -1, nil, false, now)
		if err == nil {
			t.Fatal("expected error for negative consecutive_failures, got nil")
		}
	})

	t.Run("invalid status", func(t *testing.T) {
		_, err := model.TransitionMonitorHealth(model.Status("INVALID"), 0, nil, true, now)
		if err == nil {
			t.Fatal("expected error for invalid status, got nil")
		}
	})
}
