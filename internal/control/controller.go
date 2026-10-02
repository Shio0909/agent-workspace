package control

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type slot struct {
	mu         sync.Mutex
	active     int
	endpoint   string
	readyUntil time.Time
	// activity is the newest request activity, kept in memory so the request
	// path never waits for an fsync. It is merged into the persisted
	// LastActivity at most once per ActivityFlushInterval, and on shutdown.
	activity time.Time
	// nextBeat and misses are heartbeat bookkeeping. They live in memory: a
	// controller restart simply starts counting again.
	nextBeat time.Time
	misses   int
	// usage is the newest cumulative usage seen on a heartbeat. Like activity
	// it is merged into the persisted copy at most once per
	// ActivityFlushInterval, except when it crosses the budget. The flags
	// keep a persistent anomaly from being logged or audited on every beat.
	usage          Usage
	usageFlushedAt time.Time
	usageRegressed bool
	usageRejected  bool
	// wake 是容量为 1 的通知槽：事件驱动对账完成后唤醒正在等待这个工作区
	// 就绪的请求，而不是让它们睡满一个 PollInterval。
	wake chan struct{}
}

const runtimeReadyTTL = 5 * time.Second

var (
	startPhaseObservationTimeout  = 2 * time.Second
	startPhaseObservationInterval = 25 * time.Millisecond
	startPhaseDirectTimeout       = time.Second
)

// DefaultActivityFlushInterval bounds how much request activity a crash can
// lose. Losing it is safe: after a restart, idle reclamation is held back for a
// full IdleTimeout (see reclaim), which is far longer than this interval.
const DefaultActivityFlushInterval = 10 * time.Second

type Controller struct {
	store            *Store
	runtime          Runtime
	profiles         map[string]Profile
	slots            sync.Map
	IdleTimeout      time.Duration
	OperationTimeout time.Duration
	PollInterval     time.Duration
	// GracePeriod 是 suspended 到硬删之间的宽限期，也是人工介入恢复的窗口。
	// 挂起态保留 PVC，宽限期结束才会连存储一起删除。
	GracePeriod time.Duration
	// StartupGrace 是进程启动后的回收静默期。重启时工作区可能正在恢复，
	// 或者调用方正准备续期，第一轮扫描不应该把它们当成垃圾回收掉。
	StartupGrace time.Duration
	// OperationLease 是 processing 幂等记录的存活上限。它必须大于一次动作
	// 的最长耗时，否则正在执行的操作会被误判成崩溃残留而被接管。
	OperationLease time.Duration
	// ProbeTimeout 与 ReadyTTL 控制就绪探针：单次探测的上限，以及结果的
	// 缓存时长，避免频繁探针变成对后端 API 的压测。
	ProbeTimeout time.Duration
	ReadyTTL     time.Duration
	// DisableReadyCache forces every request through the readiness probe. It is used only for controlled benchmarks.
	DisableReadyCache bool
	// ActivityFlushInterval is how stale the persisted LastActivity may get
	// while requests keep arriving. Zero persists every request, which is the
	// original behaviour and is kept for controlled benchmarks.
	ActivityFlushInterval time.Duration
	// UpgradeSettle is how long a workspace must stay ready on the new image
	// before an upgrade is committed. A workload that starts and then crashes
	// is ready for a moment too, so readiness alone is not enough.
	UpgradeSettle time.Duration
	// HeartbeatInterval spaces the polls of one workspace; HeartbeatMisses
	// consecutive failures mark it unresponsive and trigger a restart, at most
	// once per RestartCooldown so a workload that crashes on start is not
	// restarted in a tight loop.
	HeartbeatInterval time.Duration
	HeartbeatMisses   int
	RestartCooldown   time.Duration
	HeartbeatClient   *http.Client
	// Metrics 由 New 初始化，控制器内部只做原子自增。
	Metrics *Metrics

	now     func() time.Time
	started time.Time
	// startedAt 记录每次冷启动（进入 Starting）的真实时刻，只用真实时钟：
	// pod 状况时间戳来自 API server，和测试用的假时钟混用会算出负耗时。
	// 控制器重启会丢掉样本，冷启动指标是 best-effort，不影响行为。
	startedAt sync.Map
	queue     *eventQueue
	readyMu   sync.Mutex
	readyErr  error
	readyAt   time.Time
}

func New(store *Store, runtime Runtime, profiles map[string]Profile, idle time.Duration) *Controller {
	return &Controller{store: store, runtime: runtime, profiles: profiles,
		IdleTimeout: idle, OperationTimeout: time.Minute, PollInterval: time.Second,
		GracePeriod: 24 * time.Hour, StartupGrace: time.Minute, OperationLease: 10 * time.Minute,
		ProbeTimeout: 3 * time.Second, ReadyTTL: 2 * time.Second,
		ActivityFlushInterval: DefaultActivityFlushInterval,
		UpgradeSettle:         15 * time.Second,
		HeartbeatInterval:     10 * time.Second, HeartbeatMisses: 3, RestartCooldown: 5 * time.Minute,
		HeartbeatClient: &http.Client{Timeout: 2 * time.Second},
		Metrics:         &Metrics{}, now: time.Now, started: time.Now(), queue: newEventQueue()}
}

func (c *Controller) slot(id string) *slot {
	s, _ := c.slots.LoadOrStore(id, &slot{wake: make(chan struct{}, 1)})
	return s.(*slot)
}

// wake 非阻塞地通知 Acquire 再试一次。事件 worker 不能等待一个正在执行慢
// Stop 的工作区锁；陈旧通知最多多触发一次幂等 Reconcile。
func (c *Controller) wake(id string) {
	if v, ok := c.slots.Load(id); ok {
		s := v.(*slot)
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

// withActivity overlays unflushed request activity so readers never see an
// older LastActivity than the controller itself uses.
func (c *Controller) withActivity(w Workspace) Workspace {
	if v, ok := c.slots.Load(w.ID); ok {
		s := v.(*slot)
		s.mu.Lock()
		mergeActivity(s, &w)
		mergeUsage(s, &w)
		s.mu.Unlock()
	}
	return w
}

func (c *Controller) Get(id string) (Workspace, error) {
	w, err := c.store.Get(id)
	if err != nil {
		return w, err
	}
	return c.withActivity(w), nil
}

func (c *Controller) List() []Workspace {
	items := c.store.List()
	for i := range items {
		items[i] = c.withActivity(items[i])
	}
	return items
}

// touch records request activity. The caller must hold s.mu. It persists only
// when the stored value is older than ActivityFlushInterval, so a busy
// workspace costs one snapshot write per interval instead of two per request.
func (c *Controller) touch(s *slot, w *Workspace, now time.Time) error {
	if now.After(s.activity) {
		s.activity = now
	}
	if s.activity.Sub(w.LastActivity) < c.ActivityFlushInterval {
		return nil
	}
	w.LastActivity, w.UpdatedAt = s.activity, now
	return c.store.Put(*w)
}

// mergeActivity folds unflushed activity into w. The caller must hold s.mu.
func mergeActivity(s *slot, w *Workspace) {
	if s.activity.After(w.LastActivity) {
		w.LastActivity = s.activity
	}
}

// FlushActivity persists all unflushed request activity. It is called on
// graceful shutdown; a crash skips it, which the restart grace tolerates.
func (c *Controller) FlushActivity() error {
	var errs []error
	c.slots.Range(func(key, value any) bool {
		s := value.(*slot)
		s.mu.Lock()
		defer s.mu.Unlock()
		w, err := c.store.Get(key.(string))
		if err != nil {
			return true
		}
		stored := w
		mergeActivity(s, &w)
		mergeUsage(s, &w)
		if w.LastActivity.Equal(stored.LastActivity) && w.Usage == stored.Usage {
			return true
		}
		if err := c.store.Put(w); err != nil {
			errs = append(errs, err)
		}
		return true
	})
	return errors.Join(errs...)
}

// Scan 返回按 ID 排序的一页工作区，more 表示后面还有。调度循环用它分批推进，
// 而不是一次把整个快照展开成工作集。
func (c *Controller) Scan(after string, limit int) ([]Workspace, bool) {
	return c.store.Scan(after, limit)
}

// Audit 查询审计日志。它读的是磁盘上的追加文件，所以控制器重启前写下的记录
// 依然可见。
func (c *Controller) Audit(q AuditQuery) ([]AuditEvent, error) {
	return c.store.Audit().Query(q)
}

func (c *Controller) Create(actor, id, profile string) (Workspace, error) {
	if !ValidName(id) {
		return Workspace{}, fmt.Errorf("%w: invalid workspace id", ErrInvalid)
	}
	if _, ok := c.profiles[profile]; !ok {
		return Workspace{}, fmt.Errorf("%w: unknown profile", ErrInvalid)
	}
	s := c.slot(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if w, err := c.store.Get(id); err == nil {
		if w.Profile == profile && w.Desired != DesiredDeleted {
			return w, nil
		}
		return Workspace{}, ErrConflict
	} else if !errors.Is(err, ErrNotFound) {
		return Workspace{}, err
	}
	now := c.now()
	w := Workspace{ID: id, Profile: profile, Desired: DesiredStopped, Phase: PhaseStopped,
		LastActivity: now, UpdatedAt: now}
	if err := c.store.Put(w); err != nil {
		return Workspace{}, err
	}
	c.audit(actor, ActionCreate, id, "profile="+profile, ResultOK)
	return w, nil
}

func hasLease(w Workspace, now time.Time) bool {
	for _, expires := range w.Leases {
		if now.Before(expires) {
			return true
		}
	}
	return false
}

// SetDesired durably records intent before touching Kubernetes. Reconcile can
// therefore finish an interrupted operation after the controller restarts.
func (c *Controller) SetDesired(actor, id, desired string) (Workspace, error) {
	if !validDesired(desired) {
		return Workspace{}, ErrInvalid
	}
	s := c.slot(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := c.store.Get(id)
	if err != nil {
		return w, err
	}
	now := c.now()
	if w.Desired == DesiredDeleted && desired != DesiredDeleted {
		return w, ErrConflict
	}
	// 挂起和过期都只能靠续期恢复（预算挂起则靠调高预算）：PVC 还在，但直接
	// 唤醒会让调用方一直等到超时，因为下一轮扫描会立刻把它重新挂起。删除
	// 是唯一例外。
	if desired != DesiredDeleted {
		if err := w.blocked(now, desired == DesiredRunning); err != nil {
			return w, err
		}
	}
	if desired != DesiredRunning && (s.active > 0 || hasLease(w, now)) {
		return w, ErrConflict
	}
	w.Desired, w.UpdatedAt, w.LastError = desired, now, ""
	if desired == DesiredRunning {
		w.LastActivity = now
	}
	if desired == DesiredDeleted {
		w.DeletionReason = DeletedByUser
	}
	if err := c.store.Put(w); err != nil {
		return w, err
	}
	s.endpoint, s.readyUntil = "", time.Time{}
	c.queue.Add(id)
	c.audit(actor, desiredAction(desired), id, "desired="+desired, ResultOK)
	return w, nil
}

// Restart records a durable restart intent. Like start and stop, the runtime
// replacement itself happens on the next reconcile, so the request does not
// block on Kubernetes.
func (c *Controller) Restart(actor, id string) (Workspace, error) {
	s := c.slot(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := c.store.Get(id)
	if err != nil {
		return w, err
	}
	now := c.now()
	if w.Desired == DesiredDeleted {
		return w, ErrConflict
	}
	if err := w.blocked(now, true); err != nil {
		return w, err
	}
	if s.active > 0 || hasLease(w, now) {
		return w, ErrConflict
	}
	w.Desired, w.RestartPending, w.Phase = DesiredRunning, true, PhaseStarting
	w.LastActivity, w.UpdatedAt, w.LastError = now, now, ""
	if err := c.store.Put(w); err != nil {
		return w, err
	}
	s.endpoint, s.readyUntil = "", time.Time{}
	// A restart is a cold start of the new pod: the breakdown is measured from
	// here, not from the next round which already sees Phase=starting.
	c.startedAt.Store(id, time.Now())
	c.queue.Add(id)
	c.audit(actor, ActionRestart, id, "workload replaced, volume retained", ResultOK)
	return w, nil
}

// SetExpiry sets, extends or clears the deadline, and is the only way back from
// a suspended workspace. Resuming only rewrites intent: suspension never
// removed storage, so there is nothing to prepare again.
func (c *Controller) SetExpiry(actor, id string, expiresAt time.Time) (Workspace, error) {
	s := c.slot(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := c.store.Get(id)
	if err != nil {
		return w, err
	}
	if w.Desired == DesiredDeleted {
		return w, ErrConflict
	}
	now := c.now()
	w.ExpiresAt, w.UpdatedAt = expiresAt, now
	action, detail := ActionExpirySet, "expires_at="+formatDeadline(expiresAt)
	// A budget suspension is lifted by the budget, not by a new deadline.
	if w.Desired == DesiredSuspended && w.SuspendedFor != SuspendedForBudget && (expiresAt.IsZero() || now.Before(expiresAt)) {
		w.Desired, w.SuspendedAt = DesiredStopped, time.Time{}
		action, detail = ActionResume, "resumed with expires_at="+formatDeadline(expiresAt)
	}
	if err := c.store.Put(w); err != nil {
		return w, err
	}
	c.audit(actor, action, id, detail, ResultOK)
	return w, nil
}

// InvalidateEndpoint makes the next request recheck runtime health after a proxy
// failure. The failed business request itself is never replayed.
func (c *Controller) InvalidateEndpoint(id, endpoint string) {
	s := c.slot(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.endpoint == endpoint {
		s.endpoint, s.readyUntil = "", time.Time{}
	}
}

// Lease protects work which continues after an HTTP response or disconnection.
// Agents must renew their lease before expiration. Leases survive a restart.
func (c *Controller) Lease(actor, id, token string, ttl time.Duration) (string, error) {
	if ttl <= 0 || ttl > time.Hour {
		return "", fmt.Errorf("%w: lease ttl must be in (0, 3600] seconds", ErrInvalid)
	}
	s := c.slot(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := c.store.Get(id)
	if err != nil {
		return "", err
	}
	now := c.now()
	if err := w.blocked(now, true); err != nil {
		return "", err
	}
	if w.Desired != DesiredRunning {
		return "", ErrConflict
	}
	if token != "" {
		expires, ok := w.Leases[token]
		if !ok || !now.Before(expires) {
			return "", ErrNotFound
		}
	} else {
		if len(w.Leases) >= 256 {
			for k, expires := range w.Leases {
				if !now.Before(expires) {
					delete(w.Leases, k)
				}
			}
			if len(w.Leases) >= 256 {
				return "", ErrConflict
			}
		}
		b := make([]byte, 16)
		if _, err := rand.Read(b); err != nil {
			return "", err
		}
		token = hex.EncodeToString(b)
	}
	w.Leases[token] = now.Add(ttl)
	w.LastActivity, w.UpdatedAt = now, now
	if err := c.store.Put(w); err != nil {
		return "", err
	}
	c.audit(actor, ActionLeaseAcquire, id, "ttl="+ttl.String(), ResultOK)
	return token, nil
}

func (c *Controller) ReleaseLease(actor, id, token string) error {
	s := c.slot(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := c.store.Get(id)
	if err != nil {
		return err
	}
	if _, ok := w.Leases[token]; !ok {
		return ErrNotFound
	}
	delete(w.Leases, token)
	w.LastActivity, w.UpdatedAt = c.now(), c.now()
	if err := c.store.Put(w); err != nil {
		return err
	}
	c.audit(actor, ActionLeaseRelease, id, "", ResultOK)
	return nil
}

// Acquire marks activity before starting or probing the runtime. The caller
// holds the activity reference through the entire upstream response (SSE/WS too).
func (c *Controller) Acquire(ctx context.Context, id string) (string, func(), error) {
	waitStart := time.Now()
	s := c.slot(id)
	s.mu.Lock()
	w, err := c.store.Get(id)
	if err == nil && w.Desired == DesiredDeleted {
		err = ErrConflict
	}
	if err == nil {
		err = w.blocked(c.now(), true)
	}
	woke := false
	if err == nil {
		now := c.now()
		if w.Desired != DesiredRunning {
			// Waking a workspace changes intent, and intent is always durable.
			s.activity = now
			w.Desired, w.LastActivity, w.UpdatedAt = DesiredRunning, now, now
			err = c.store.Put(w)
			woke = err == nil
		} else {
			err = c.touch(s, &w, now)
		}
	}
	if err != nil {
		s.mu.Unlock()
		return "", nil, err
	}
	s.active++
	drainWake(s.wake)
	s.mu.Unlock()
	if woke {
		c.queue.Add(id)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.active--
			latest, err := c.store.Get(id)
			if err == nil {
				err = c.touch(s, &latest, c.now())
			}
			if err != nil && !errors.Is(err, ErrNotFound) {
				slog.Error("persist request activity", "workspace", id, "error", err)
			}
		})
	}
	ticker := time.NewTicker(c.PollInterval)
	defer ticker.Stop()
	for {
		endpoint, err := c.Reconcile(ctx, id)
		if err != nil {
			release()
			return "", nil, err
		}
		if endpoint != "" {
			c.Metrics.AcquireWait.Observe(time.Since(waitStart).Seconds())
			return endpoint, release, nil
		}
		select {
		case <-ctx.Done():
			release()
			return "", nil, ctx.Err()
		case <-ticker.C:
		case <-s.wake:
			// 事件驱动的对账已经完成了一轮：立刻重试，而不是睡满 PollInterval。
		}
	}
}

func drainWake(ch <-chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// reclaimDecision 只报告调用方必须知道的两件事：状态是否变了（要落盘），以及
// 这次删除是不是宽限期到了才发生的（审计要区分触发者）。审计本身在转移发生的
// 地方就地记录。
type reclaimDecision struct {
	changed     bool
	graceDelete bool
}

// Reconcile is the only place which talks to the runtime. It enforces the whole
// lifecycle in one pass: reconcile the desired state, stop idle workspaces,
// suspend expired ones and hard delete those whose grace period has passed.
func (c *Controller) Reconcile(ctx context.Context, id string) (endpoint string, err error) {
	c.Metrics.Reconciles.Add(1)
	defer func() {
		if err != nil {
			c.Metrics.ReconcileErrors.Add(1)
		}
	}()
	s := c.slot(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	w, err := c.store.Get(id)
	if err != nil {
		return "", err
	}
	if w.Phase == PhaseDeleted {
		return "", nil
	}
	base, ok := c.profiles[w.Profile]
	if !ok {
		return "", fmt.Errorf("unknown persisted profile %q", w.Profile)
	}
	p, _ := c.profileFor(w)
	now := c.now()
	persisted := w.LastActivity
	mergeActivity(s, &w)
	mergeUsage(s, &w)
	decision := c.reclaim(&w, now, s.active > 0 || hasLease(w, now))
	if !decision.changed && w.LastActivity.Sub(persisted) >= c.ActivityFlushInterval && w.LastActivity.After(persisted) {
		// Periodic flush for workspaces which stay busy: the scheduler visits
		// every workspace each round, so persisted activity never lags by more
		// than one interval plus one round.
		decision.changed = true
	}
	if decision.changed {
		// Persist reclamation intent before touching the runtime: a crash here
		// leaves a workspace which the next round reclaims again, instead of one
		// which looks running while its workload is already gone.
		if err := c.store.Put(w); err != nil {
			return "", err
		}
	}
	if !c.DisableReadyCache && w.Desired == DesiredRunning && s.endpoint != "" && now.Before(s.readyUntil) {
		return s.endpoint, nil
	}
	s.endpoint, s.readyUntil = "", time.Time{}
	opCtx, cancel := context.WithTimeout(ctx, c.OperationTimeout)
	defer cancel()
	previous := w.Phase
	switch w.Desired {
	case DesiredRunning:
		var obs Observation
		obs, err = c.runtime.Observe(opCtx, w, p)
		if err == nil && w.Upgrade != nil {
			obs = c.verifyUpgrade(&w, base, obs, now)
			// A rollback changed the image; apply the one the workspace now has.
			p, _ = c.profileFor(w)
		}
		if err == nil && w.RolloutPending {
			if obs.Ready && (obs.Image == p.Image || obs.Image == "") {
				w.RolloutPending = false
			} else {
				// The old image is still serving: not ready for the purposes of
				// this reconcile, so that Ensure applies the new one.
				obs.Ready = false
			}
		}
		if err == nil && !obs.Ready {
			// Record t0 before Ensure: the Deployment API call and scheduler can
			// create and schedule a Pod before Ensure returns.
			if w.Phase != PhaseStarting {
				c.startedAt.LoadOrStore(w.ID, time.Now())
			}
			err = c.runtime.Ensure(opCtx, w, p)
		}
		if err == nil && w.RestartPending {
			// Ensure first: the workspace may be brand new or scaled to zero, and
			// restarting a Deployment which does not exist yet would fail.
			if err = c.runtime.Restart(opCtx, w); err == nil {
				w.RestartPending, w.Phase = false, PhaseStarting
				w.HeartbeatAt = time.Time{}
			}
			break
		}
		w.Phase = PhaseStarting
		if err == nil && obs.Ready {
			w.Phase, endpoint = PhaseRunning, obs.Endpoint
		}
	case DesiredStopped:
		err = c.runtime.Stop(opCtx, w)
		w.Phase = PhaseStopped
		pauseUpgrade(&w)
		w.HeartbeatAt = time.Time{}
	case DesiredSuspended:
		// Suspension stops the workload but never its storage.
		err = c.runtime.Stop(opCtx, w)
		w.Phase = PhaseSuspended
		pauseUpgrade(&w)
		w.HeartbeatAt = time.Time{}
	case DesiredDeleted:
		err = c.runtime.Delete(opCtx, w)
		w.Phase = PhaseDeleted
	default:
		return "", fmt.Errorf("invalid persisted desired state %q", w.Desired)
	}
	w.LastError = ""
	if err == nil && w.Desired == DesiredSuspended && w.SuspendedFor == SuspendedForBudget {
		w.LastError = budgetMessage(w)
	}
	if err != nil {
		w.Phase, w.LastError = PhaseError, err.Error()
	}
	if w.Phase == PhaseRunning && err == nil {
		c.recordColdStart(opCtx, w)
	} else if w.Phase == PhaseStopped || w.Phase == PhaseSuspended || w.Phase == PhaseDeleted {
		// 没等到就绪就被显式停止：丢弃这次样本。错误重试保留 t0。
		c.startedAt.LoadAndDelete(w.ID)
	}
	w.UpdatedAt = now
	if saveErr := c.store.Put(w); saveErr != nil {
		return "", saveErr
	}
	c.recordCompletion(w, previous, decision, err)
	if endpoint != "" && err == nil {
		s.endpoint, s.readyUntil = endpoint, c.now().Add(runtimeReadyTTL)
	}
	return endpoint, err
}

// recordColdStart 把一次 starting->running 转移记录成冷启动耗时分解。这里全部
// 用真实时钟：pod 状况的时间戳来自 API server，和 c.now 的测试替身不是同一
// 个时钟。运行时没有 StartupObserver 能力（比如单测里的替身）时只记录总耗时。
func (c *Controller) recordColdStart(ctx context.Context, w Workspace) {
	v, ok := c.startedAt.LoadAndDelete(w.ID)
	if !ok {
		return
	}
	t0 := v.(time.Time)
	c.Metrics.StartTotal.Observe(time.Since(t0).Seconds())
	if obs, ok := c.runtime.(StartupObserver); ok {
		go c.observeStartPhases(w, t0, obs)
	}
}

func (c *Controller) observeStartPhases(w Workspace, t0 time.Time, obs StartupObserver) {
	deadline := time.Now().Add(startPhaseObservationTimeout)
	for {
		allowDirect := !time.Now().Add(startPhaseObservationInterval).Before(deadline)
		timeout := startPhaseObservationInterval
		if allowDirect {
			timeout = startPhaseDirectTimeout
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		ts, found := obs.StartupTimestamps(ctx, w, allowDirect)
		cancel()
		if found && ts.Complete() {
			c.Metrics.observeStartPhases(t0, ts)
			return
		}
		if allowDirect {
			if found {
				c.Metrics.observeStartPhases(t0, ts)
			}
			return
		}
		time.Sleep(startPhaseObservationInterval)
	}
}

// reclaim advances both reclamation chains in one pass and reports what it did.
// Everything here is intent only: the caller persists it and then runs the
// runtime action, so a crash in between is retried rather than lost.
func (c *Controller) reclaim(w *Workspace, now time.Time, busy bool) reclaimDecision {
	decision := reclaimDecision{}
	if w.Desired == DesiredRunning && !busy &&
		now.Sub(w.LastActivity) >= c.IdleTimeout && now.Sub(c.started) >= c.IdleTimeout {
		idle := now.Sub(w.LastActivity).Round(time.Second)
		w.Desired, w.UpdatedAt = DesiredStopped, now
		c.Metrics.IdleStops.Add(1)
		c.audit(ActorSystem, ActionIdleStop, w.ID, "idle for "+idle.String()+", volume retained", ResultOK)
		decision.changed = true
	}
	// 空闲缩容之后立刻判过期：同一轮里 stopped -> suspended 是合法的连续转移。
	expiry := c.applyExpiry(w, now, busy)
	decision.changed = decision.changed || expiry.changed
	decision.graceDelete = expiry.graceDelete
	return decision
}

// applyExpiry drives running/stopped --deadline--> suspended --grace--> deleted.
// Reclamation only advances while the workspace is idle: a deadline does not
// justify cutting off a live request or a leased background task, and deferring
// loses nothing because the deadline stays in the past. Reclamation is skipped
// entirely during the startup grace period.
func (c *Controller) applyExpiry(w *Workspace, now time.Time, busy bool) reclaimDecision {
	if now.Sub(c.started) < c.StartupGrace {
		return reclaimDecision{}
	}
	switch w.Desired {
	case DesiredRunning, DesiredStopped:
		if !w.expired(now) || busy {
			return reclaimDecision{}
		}
		deadline := w.ExpiresAt.UTC().Format(time.RFC3339)
		w.Desired, w.SuspendedAt, w.UpdatedAt = DesiredSuspended, now, now
		c.Metrics.Expirations.Add(1)
		c.audit(ActorSystem, ActionExpire, w.ID, "deadline "+deadline+" passed, volume retained", ResultOK)
		return reclaimDecision{changed: true}
	case DesiredSuspended:
		// A budget suspension never ends in a hard delete: running out of tokens
		// is not a reason to destroy the volume, and only a person can say the
		// data is no longer wanted.
		if busy || w.SuspendedFor == SuspendedForBudget || w.SuspendedAt.IsZero() || now.Sub(w.SuspendedAt) < c.GracePeriod {
			return reclaimDecision{}
		}
		w.Desired, w.UpdatedAt, w.DeletionReason = DesiredDeleted, now, DeletedByGrace
		return reclaimDecision{changed: true, graceDelete: true}
	}
	return reclaimDecision{}
}

// recordCompletion audits the runtime side of a state change. Failures are
// recorded once per failure streak, not once per round, so a backend outage
// does not flood the audit log.
func (c *Controller) recordCompletion(w Workspace, previous string, decision reclaimDecision, err error) {
	switch w.Desired {
	case DesiredSuspended:
		if err != nil {
			if previous != PhaseError {
				c.audit(ActorSystem, ActionSuspend, w.ID, "scale to zero failed: "+err.Error(), ResultError)
			}
			return
		}
		if previous == PhaseSuspended {
			return
		}
		c.Metrics.Suspensions.Add(1)
		c.audit(ActorSystem, ActionSuspend, w.ID, "workload scaled to zero, volume retained", ResultOK)
	case DesiredDeleted:
		if err != nil {
			if previous != PhaseError {
				c.audit(ActorSystem, ActionHardDelete, w.ID, "delete failed: "+err.Error(), ResultError)
			}
			return
		}
		if previous == PhaseDeleted {
			return
		}
		// 触发者从持久化的记录里读，而不是从这一轮的决策里读：删除失败重试
		// 之后，宽限期这次转移已经不在内存里了。
		trigger := "user-requested"
		if w.DeletionReason == DeletedByGrace || decision.graceDelete {
			trigger = "grace period elapsed"
		}
		c.Metrics.HardDeletes.Add(1)
		c.audit(ActorSystem, ActionHardDelete, w.ID, trigger+", deployment, service and pvc removed", ResultOK)
	}
}

// Ready reports whether the compute backend is reachable. Liveness and
// readiness are deliberately separate: a controller which cannot reach the API
// should be taken out of rotation, not restarted. Probe results are cached
// briefly so a fast prober does not turn into load on the API server.
func (c *Controller) Ready(ctx context.Context) error {
	c.readyMu.Lock()
	defer c.readyMu.Unlock()
	if c.now().Before(c.readyAt) {
		return c.readyErr
	}
	probeCtx, cancel := context.WithTimeout(ctx, c.ProbeTimeout)
	defer cancel()
	err := c.runtime.Probe(probeCtx)
	c.readyErr, c.readyAt = err, c.now().Add(c.ReadyTTL)
	return err
}

// audit appends an audit event. A failed append neither rolls back the action
// nor disappears: the two writes are not transactional, so the caller has to be
// able to notice the gap through the error log and the failure counter.
func (c *Controller) audit(actor, action, workspace, detail, result string) {
	if actor == "" {
		actor = ActorUnknown
	}
	event := AuditEvent{At: c.now(), Actor: actor, Action: action, Workspace: workspace, Detail: detail, Result: result}
	if err := c.store.Audit().Append(event); err != nil {
		c.Metrics.AuditFailures.Add(1)
		slog.Error("append audit event", "action", action, "workspace", workspace, "error", err)
	}
}

func desiredAction(desired string) string {
	switch desired {
	case DesiredRunning:
		return ActionStart
	case DesiredStopped:
		return ActionStop
	default:
		return ActionDelete
	}
}

func formatDeadline(at time.Time) string {
	if at.IsZero() {
		return "none"
	}
	return at.UTC().Format(time.RFC3339)
}
