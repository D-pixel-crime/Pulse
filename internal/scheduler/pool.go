package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"pulse/internal/config"
)

// WorkerPool executes tasks using a fixed number of worker goroutines reading
// from a bounded FIFO channel. Workers never spawn a goroutine per task.
type WorkerPool[T any] struct {
	workerCount int
	queue       chan T
	handler     func(context.Context, T)
	wg          sync.WaitGroup
	startOnce   sync.Once
	closeOnce   sync.Once
}

// NewWorkerPool constructs a bounded worker pool with the specified worker count
// and channel capacity.
func NewWorkerPool[T any](workerCount, queueCapacity int, handler func(context.Context, T)) (*WorkerPool[T], error) {
	if workerCount < config.MinWorkerCount || workerCount > config.MaxWorkerCount {
		return nil, fmt.Errorf("worker count must be between %d and %d, got %d",
			config.MinWorkerCount, config.MaxWorkerCount, workerCount)
	}
	if queueCapacity < config.MinQueueCapacity || queueCapacity > config.MaxQueueCapacity {
		return nil, fmt.Errorf("queue capacity must be between %d and %d, got %d",
			config.MinQueueCapacity, config.MaxQueueCapacity, queueCapacity)
	}
	if handler == nil {
		return nil, errors.New("worker pool handler must not be nil")
	}

	return &WorkerPool[T]{
		workerCount: workerCount,
		queue:       make(chan T, queueCapacity),
		handler:     handler,
	}, nil
}

// Start launches the configured number of worker goroutines. Calling Start more
// than once is a no-op.
func (p *WorkerPool[T]) Start(ctx context.Context) {
	p.startOnce.Do(func() {
		p.wg.Add(p.workerCount)
		for i := 0; i < p.workerCount; i++ {
			go p.runWorker(ctx)
		}
	})
}

func (p *WorkerPool[T]) runWorker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-p.queue:
			if !ok || ctx.Err() != nil {
				return
			}
			p.handler(ctx, task)
		}
	}
}

// TryEnqueue attempts a non-blocking send of task onto the bounded queue.
// It returns false immediately if the queue is full.
// Callers must ensure Close is never called concurrently with or before TryEnqueue.
func (p *WorkerPool[T]) TryEnqueue(task T) bool {
	select {
	case p.queue <- task:
		return true
	default:
		return false
	}
}

// Close closes the work queue channel so workers exit once buffered tasks drain.
// Safe to call multiple times; must only be called after all producers have stopped sending.
func (p *WorkerPool[T]) Close() {
	p.closeOnce.Do(func() {
		close(p.queue)
	})
}

// Wait blocks until all worker goroutines have exited.
func (p *WorkerPool[T]) Wait() {
	p.wg.Wait()
}

// WorkerCount returns the configured fixed number of worker goroutines.
func (p *WorkerPool[T]) WorkerCount() int {
	return p.workerCount
}

// QueueCap returns the fixed capacity of the bounded work queue.
func (p *WorkerPool[T]) QueueCap() int {
	return cap(p.queue)
}

// QueueLen returns the current number of buffered tasks waiting in the queue.
func (p *WorkerPool[T]) QueueLen() int {
	return len(p.queue)
}
