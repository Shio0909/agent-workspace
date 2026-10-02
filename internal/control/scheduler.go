package control

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Scheduler 是控制面唯一的周期性驱动。一轮扫描经由 Reconcile 同时做完四件事：
// 对账运行时、空闲缩容、过期挂起、宽限期满硬删。不引入 cron 库是因为这里只
// 需要固定周期，而固定周期用 time.Ticker 就是最直白的实现。
type Scheduler struct {
	Controller *Controller
	Interval   time.Duration
	// RoundTimeout 是单轮的执行上限。超时后不再派发新的工作区，等在途的
	// 对账收尾后结束本轮，而不是把这一轮无限拖长。
	RoundTimeout time.Duration
	// BatchSize 决定一轮里同时展开多少条工作区记录；Concurrency 决定其中
	// 同时执行多少条。两者分开，是因为扫描往往是内存操作，而对账会调后端。
	BatchSize   int
	Concurrency int
}

func NewScheduler(c *Controller) *Scheduler {
	return &Scheduler{Controller: c, Interval: 5 * time.Second, RoundTimeout: time.Minute,
		BatchSize: 64, Concurrency: 4}
}

// RoundStats 是一轮的统计。单条失败只计数并继续：一轮里坏掉一个工作区，
// 不应该让其余工作区停止对账。
type RoundStats struct {
	Scanned  int
	Failed   int
	Skipped  int
	TimedOut bool
	Duration time.Duration
}

// Run 阻塞运行直到 ctx 结束。停机时不再开始新一轮，并等待在途对账收尾：
// 等待发生在 Round 内部，所以 Run 返回就意味着收尾已经完成。
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		stats := s.Round(ctx)
		if stats.Failed > 0 || stats.TimedOut {
			slog.Warn("scheduler round", "scanned", stats.Scanned, "failed", stats.Failed,
				"skipped", stats.Skipped, "timed_out", stats.TimedOut, "duration", stats.Duration)
		} else {
			slog.Debug("scheduler round", "scanned", stats.Scanned, "skipped", stats.Skipped,
				"duration", stats.Duration)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Round 扫描一遍所有工作区。它分批取数、有界并发，并且永不返回错误：
// 调用方需要的是"这一轮发生了什么"，而不是在第一个坏工作区上退出。
func (s *Scheduler) Round(ctx context.Context) RoundStats {
	started := time.Now()
	stats := RoundStats{}
	roundCtx, cancel := context.WithTimeout(ctx, s.RoundTimeout)
	defer cancel()
	var failed atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, s.Concurrency)
	after := ""
	for {
		batch, more := s.Controller.Scan(after, s.BatchSize)
		if len(batch) == 0 {
			break
		}
		after = batch[len(batch)-1].ID
		for _, ws := range batch {
			// 墓碑不需要对账：PhaseDeleted 的资源已经删干净，只剩记录。
			if ws.Phase == PhaseDeleted {
				stats.Skipped++
				continue
			}
			select {
			case sem <- struct{}{}:
			case <-roundCtx.Done():
				// 本轮超时或进程停机：停止派发，等在途的收尾。
				stats.TimedOut = true
				wg.Wait()
				stats.Failed, stats.Duration = int(failed.Load()), time.Since(started)
				return stats
			}
			stats.Scanned++
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				defer func() { <-sem }()
				endpoint, err := s.Controller.Reconcile(roundCtx, id)
				if err != nil {
					failed.Add(1)
					slog.Warn("reconcile workspace", "workspace", id, "error", err)
					return
				}
				s.Controller.Beat(roundCtx, id, endpoint)
			}(ws.ID)
		}
		if !more {
			break
		}
	}
	wg.Wait()
	stats.Failed, stats.Duration = int(failed.Load()), time.Since(started)
	return stats
}
