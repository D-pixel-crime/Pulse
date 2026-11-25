package scheduler_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"pulse/internal/config"
	"pulse/internal/scheduler"
)

func TestWorkerPoolRespectsConcurrencyLimit(t *testing.T) {
	const (
		workerCount   = 5
		queueCapacity = config.DefaultQueueCapacity // 100
		totalTasks    = 25
	)

	var (
		active    atomic.Int32
		maxActive atomic.Int32
		completed atomic.Int32
		tasksWG   sync.WaitGroup
	)
	tasksWG.Add(totalTasks)

	startedCh := make(chan struct{}, totalTasks)
	releaseCh := make(chan struct{})

	pool, err := scheduler.NewWorkerPool[int](workerCount, queueCapacity, func(ctx context.Context, taskID int) {
		defer tasksWG.Done()

		cur := active.Add(1)
		for {
			prev := maxActive.Load()
			if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
				break
			}
		}

		startedCh <- struct{}{}
		<-releaseCh

		active.Add(-1)
		completed.Add(1)
	})
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	for i := 0; i < totalTasks; i++ {
		if !pool.TryEnqueue(i) {
			t.Fatalf("failed to enqueue synthetic task %d into capacity %d", i, queueCapacity)
		}
	}

	// Wait deterministically until all 5 workers have started a task and are blocked on releaseCh.
	for i := 0; i < workerCount; i++ {
		<-startedCh
	}

	// Because there are strictly 5 worker goroutines, no 6th task can start while all 5 are blocked.
	if gotActive := active.Load(); gotActive != workerCount {
		close(releaseCh)
		t.Fatalf("expected %d active workers before release, got %d", workerCount, gotActive)
	}
	if len(startedCh) != 0 {
		close(releaseCh)
		t.Fatalf("expected no additional tasks started beyond %d workers, found %d extra", workerCount, len(startedCh))
	}

	// Release all blocked tasks and wait for all 25 tasks to complete.
	close(releaseCh)
	tasksWG.Wait()

	// Drain startedCh signals for remaining tasks and close pool cleanly.
	pool.Close()
	pool.Wait()

	if got := completed.Load(); got != totalTasks {
		t.Fatalf("expected %d completed tasks, got %d", totalTasks, got)
	}
	if gotMax := maxActive.Load(); gotMax != workerCount {
		t.Fatalf("expected observed max concurrency to equal %d, got %d", workerCount, gotMax)
	}
	if gotMax := maxActive.Load(); gotMax > workerCount {
		t.Fatalf("observed max concurrency %d exceeded limit %d", gotMax, workerCount)
	}
	if gotActive := active.Load(); gotActive != 0 {
		t.Fatalf("expected 0 active tasks after completion, got %d", gotActive)
	}
}

func TestNewWorkerPool_Validation(t *testing.T) {
	noop := func(context.Context, int) {}

	if _, err := scheduler.NewWorkerPool[int](0, 10, noop); err == nil {
		t.Error("expected error for workerCount 0")
	}
	if _, err := scheduler.NewWorkerPool[int](65, 10, noop); err == nil {
		t.Error("expected error for workerCount 65")
	}
	if _, err := scheduler.NewWorkerPool[int](5, 0, noop); err == nil {
		t.Error("expected error for queueCapacity 0")
	}
	if _, err := scheduler.NewWorkerPool[int](5, config.MaxQueueCapacity+1, noop); err == nil {
		t.Error("expected error for queueCapacity exceeding max")
	}
	if _, err := scheduler.NewWorkerPool[int](5, 10, nil); err == nil {
		t.Error("expected error for nil handler")
	}

	p, err := scheduler.NewWorkerPool[int](config.MaxWorkerCount, config.DefaultQueueCapacity, noop)
	if err != nil {
		t.Fatalf("expected valid pool at max worker count 64, got: %v", err)
	}
	if p.WorkerCount() != 64 || p.QueueCap() != 100 {
		t.Errorf("unexpected pool dimensions: workers=%d cap=%d", p.WorkerCount(), p.QueueCap())
	}
}

func TestWorkerPool_QueueFullNonBlockingAndContextCancelExit(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})

	pool, err := scheduler.NewWorkerPool[int](1, 1, func(ctx context.Context, _ int) {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
	})
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	// 1st task is picked up by the single worker and blocks on release.
	if !pool.TryEnqueue(1) {
		t.Fatal("expected first TryEnqueue to succeed")
	}
	<-started

	// 2nd task fills the capacity-1 queue.
	if !pool.TryEnqueue(2) {
		t.Fatal("expected second TryEnqueue to fill queue capacity 1")
	}

	// 3rd task must fail immediately without blocking.
	if pool.TryEnqueue(3) {
		t.Fatal("expected third TryEnqueue to return false when queue is full")
	}

	// Cancel context without closing queue; workers must exit cleanly without leaking.
	cancel()
	pool.Wait()
}
