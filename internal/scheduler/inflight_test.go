package scheduler_test

import (
	"sync"
	"testing"

	"github.com/google/uuid"

	"pulse/internal/scheduler"
)

func TestInFlightSet_BasicAndDuplicateSuppression(t *testing.T) {
	set := scheduler.NewInFlightSet()
	id1 := uuid.New()
	id2 := uuid.New()

	if set.TryAdd(uuid.Nil) {
		t.Fatal("expected TryAdd(uuid.Nil) to return false")
	}
	if set.Contains(uuid.Nil) {
		t.Fatal("expected Contains(uuid.Nil) to return false")
	}
	if set.Len() != 0 {
		t.Fatalf("expected initial Len=0, got %d", set.Len())
	}

	if !set.TryAdd(id1) {
		t.Fatal("expected first TryAdd(id1) to succeed")
	}
	if set.TryAdd(id1) {
		t.Fatal("expected duplicate TryAdd(id1) to return false")
	}
	if !set.Contains(id1) {
		t.Fatal("expected Contains(id1) to be true")
	}

	if !set.TryAdd(id2) {
		t.Fatal("expected TryAdd(id2) to succeed")
	}
	if set.Len() != 2 {
		t.Fatalf("expected Len=2, got %d", set.Len())
	}

	set.Remove(id1)
	if set.Contains(id1) {
		t.Fatal("expected Contains(id1) to be false after Remove")
	}
	if set.Len() != 1 {
		t.Fatalf("expected Len=1 after Remove, got %d", set.Len())
	}

	if !set.TryAdd(id1) {
		t.Fatal("expected TryAdd(id1) to succeed again after Remove")
	}
}

func TestInFlightSet_ConcurrentAccess(t *testing.T) {
	set := scheduler.NewInFlightSet()
	id := uuid.New()

	const goroutines = 32
	var wg sync.WaitGroup
	wg.Add(goroutines)

	wins := make(chan bool, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			wins <- set.TryAdd(id)
		}()
	}
	wg.Wait()
	close(wins)

	winCount := 0
	for won := range wins {
		if won {
			winCount++
		}
	}
	if winCount != 1 {
		t.Fatalf("expected exactly 1 goroutine to win TryAdd, got %d", winCount)
	}

	set.Remove(id)
	if set.Len() != 0 {
		t.Fatalf("expected Len=0 after Remove, got %d", set.Len())
	}
}
