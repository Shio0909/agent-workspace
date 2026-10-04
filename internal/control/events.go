package control

import (
	"context"
	"errors"
	"sync"
	"time"
)

// eventWorkers 是事件驱动对账的并发度。事件路径是快速通道，周期调度才是
// 兜底，所以这里不需要大并发。
const eventWorkers = 2

const (
	eventRetryBase = 100 * time.Millisecond
	eventRetryMax  = 30 * time.Second
)

// StartEvents runs the event-driven reconcile loop until ctx is done. Runtime
// events (when the runtime provides them) and explicit intent changes both land
// in one deduplicating, rate-limited queue, so a changed workspace is
// reconciled within milliseconds instead of waiting for the next sweep, and a
// failing one backs off instead of being retried every round. The periodic
// scheduler stays the backstop for everything time-driven (idle, expiry,
// grace) and for anything a dropped event missed.
func (c *Controller) StartEvents(ctx context.Context) {
	q := c.queue
	go func() {
		<-ctx.Done()
		q.Shutdown()
	}()
	if src, ok := c.runtime.(EventSource); ok {
		if ch := src.WorkspaceEvents(); ch != nil {
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case id, ok := <-ch:
						if !ok {
							return
						}
						q.Add(id)
					}
				}
			}()
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < eventWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.eventWorker(ctx, q)
		}()
	}
	wg.Wait()
}

func (c *Controller) eventWorker(ctx context.Context, q *eventQueue) {
	for {
		id, ok := q.Get()
		if !ok {
			return
		}
		rctx, cancel := context.WithTimeout(ctx, c.OperationTimeout)
		_, err := c.Reconcile(rctx, id)
		cancel()
		c.Metrics.EventReconciles.Add(1)
		switch {
		case err == nil, errors.Is(err, ErrNotFound), errors.Is(err, context.Canceled):
			// NotFound means the record is gone; Canceled means shutdown. Neither
			// is worth a retry.
			q.Forget(id)
		default:
			c.Metrics.EventReconcileErrors.Add(1)
			q.AddRateLimited(id)
		}
		// Wake requests waiting for this workspace even on failure: their next
		// Reconcile attempt returns the persisted error instead of waiting out
		// the poll interval.
		c.wake(id)
	}
}

// eventQueue 是去重、带退避的工作区 ID 队列。手写而不是用 client-go 的
// workqueue：control 包刻意不依赖任何 Kubernetes 库，队列语义（去重 +
// 指数退避）本身只有几十行。
type eventQueue struct {
	mu       sync.Mutex
	changed  chan struct{}
	pending  map[string]time.Time // id -> 最早可执行时刻
	failures map[string]int
	closed   bool
}

func newEventQueue() *eventQueue {
	return &eventQueue{changed: make(chan struct{}, 1), pending: map[string]time.Time{}, failures: map[string]int{}}
}

// Add 把 id 放入就绪集合。重复事件合并成一次执行；退避中的 id 被新事件
// 提前到立即可执行，因为新事件意味着状态又变了。
func (q *eventQueue) Add(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.pending[id] = time.Now()
	q.notify()
}

// AddRateLimited 按失败次数指数退避后重排。
func (q *eventQueue) AddRateLimited(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.failures[id]++
	delay := eventRetryBase << min(q.failures[id]-1, 20)
	retry := time.Now().Add(min(delay, eventRetryMax))
	// 处理中到达的新事件不能被失败收尾重新推迟。
	if ready, exists := q.pending[id]; !exists || retry.Before(ready) {
		q.pending[id] = retry
	}
	q.notify()
}

// Forget 清除失败计数。成功一次之后，下一次失败从最小退避重新开始。
func (q *eventQueue) Forget(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.failures, id)
}

// Get 取出下一个到期的 id；ok 为 false 表示队列已关闭。事件只是提示，关闭时
// 不需要排空：周期调度会覆盖所有工作区。
func (q *eventQueue) Get() (id string, ok bool) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return "", false
		}
		now := time.Now()
		pick := ""
		wait := time.Duration(-1)
		for id, notBefore := range q.pending {
			if !notBefore.After(now) {
				pick = id
				break
			}
			if d := notBefore.Sub(now); wait < 0 || d < wait {
				wait = d
			}
		}
		if pick != "" {
			delete(q.pending, pick)
			q.mu.Unlock()
			return pick, true
		}
		q.mu.Unlock()
		if wait < 0 {
			<-q.changed
			continue
		}
		select {
		case <-q.changed:
		case <-time.After(wait):
		}
	}
}

// Shutdown 唤醒所有等待者并忽略后续写入。
func (q *eventQueue) Shutdown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.changed)
}

// notify 在持锁状态下唤醒一个等待者。多次 Add 合并成一次唤醒即可。
func (q *eventQueue) notify() {
	select {
	case q.changed <- struct{}{}:
	default:
	}
}

// lenLocked 供测试使用。
func (q *eventQueue) pendingLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}
