package control

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

var (
	ErrNotFound = errors.New("workspace not found")
	ErrConflict = errors.New("workspace is busy or has a conflicting definition")
	ErrInvalid  = errors.New("invalid request")
	// ErrExpired 表示租期已过。挂起态只能靠续期恢复（PVC 还在，但工作负载
	// 已经缩容到 0），所以调用方必须先续期再唤醒，见 SetExpiry。
	ErrExpired = errors.New("workspace expired")
	// ErrBudgetExceeded 表示工作区因 token 预算用尽被挂起。和 ErrExpired 分开：
	// 恢复手段不同（调高预算，而不是续期），HTTP 状态码也不同。
	ErrBudgetExceeded = errors.New("workspace token budget exceeded")

	// 幂等记录的状态机是 processing -> success | failed。两个终态都不可逆：
	// 重复提交只会拿到第一次的结果，不会产生第二次副作用。调用方要重试就
	// 换一个新的 biz_id。
	ErrOperationInProgress = errors.New("operation already in progress")
	ErrOperationSucceeded  = errors.New("operation already succeeded")
	ErrOperationFailed     = errors.New("operation already failed")
	// ErrOperationSuperseded 表示这次收尾所属的那一代执行已经被后来的接管取代，
	// 记录现在归新的持有者，旧持有者的结果不会写进去。
	ErrOperationSuperseded = errors.New("operation was taken over by a later attempt")

	namePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,39}$`)
	// biz_id 由调用方提供，允许 UUID 或带前缀的追踪号。限制字符集是因为它
	// 会进入审计日志和状态快照，不希望出现控制字符或换行。
	bizPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$`)
	// 镜像引用：registry/repo:tag 或 repo@sha256:...。只做字符集检查，是否
	// 允许由 Profile.AllowedImages 决定。
	imagePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/:@-]{0,254}$`)
)

// desired 与 phase 的取值。两者都会进入 JSON 快照和 HTTP 响应，改名等于改协议。
const (
	DesiredRunning   = "running"
	DesiredStopped   = "stopped"
	DesiredSuspended = "suspended"
	DesiredDeleted   = "deleted"

	PhaseStarting  = "starting"
	PhaseRunning   = "running"
	PhaseStopped   = "stopped"
	PhaseSuspended = "suspended"
	PhaseDeleted   = "deleted"
	PhaseError     = "error"
)

// 幂等记录的状态。
const (
	OpProcessing = "processing"
	OpSuccess    = "success"
	OpFailed     = "failed"
)

// 幂等记录的操作类型。Type 是记录的一部分但不是键的一部分：同一个 biz_id
// 换一个动作会被拒绝，而不是默默产生第二次副作用。
const (
	OpStart   = "start"
	OpStop    = "stop"
	OpRestart = "restart"
	OpDelete  = "delete"
	// OpTokenBudget 只在调用方带了 X-Biz-Id 时才会用到。
	OpTokenBudget = "token-budget"
)

// 审计动作。前 11 个覆盖完整的生命周期与租约路径，后 3 个是续期相关动作：
// resume 表示挂起工作区被人工续期恢复，expiry-set 表示只改租期不改状态。
const (
	ActionCreate       = "create"
	ActionStart        = "start"
	ActionStop         = "stop"
	ActionRestart      = "restart"
	ActionDelete       = "delete"
	ActionLeaseAcquire = "lease-acquire"
	ActionLeaseRelease = "lease-release"
	ActionIdleStop     = "idle-stop"
	ActionExpire       = "expire"
	ActionSuspend      = "suspend"
	ActionHardDelete   = "hard-delete"
	ActionResume       = "resume"
	ActionExpirySet    = "expiry-set"
	// 凭据动作只记录版本号和键名，永远不记录值。
	ActionCredentialSet   = "credential-set"
	ActionCredentialClear = "credential-clear"
	// 升级与心跳动作。upgrade-rollback 的结果记为 error：升级没有成功。
	ActionUpgrade          = "upgrade"
	ActionUpgradeCommit    = "upgrade-commit"
	ActionUpgradeRollback  = "upgrade-rollback"
	ActionRollback         = "rollback"
	ActionHeartbeatLost    = "heartbeat-lost"
	ActionHeartbeatRestart = "heartbeat-restart"
	ActionHeartbeatBack    = "heartbeat-recovered"
	// 用量与预算动作。budget-exceeded 是控制面自己的挂起决策，记录在
	// 触发它的那一刻；usage-regressed 表示工作负载报告了比已记录值更小的
	// 累计用量。
	ActionBudgetSet      = "token-budget-set"
	ActionBudgetExceeded = "budget-exceeded"
	ActionUsageRegressed = "usage-regressed"
)

const (
	// ActorSystem 表示动作由调度循环自己发起（空闲回收、过期挂起、宽限硬删）。
	ActorSystem = "system"
	// ActorUnknown 表示调用方没有提供归属信息。Actor 只是归属信息，不是授权
	// 依据：控制面只校验一个共享令牌，调用方可以声称任意身份。
	ActorUnknown = "unknown"
)

type Profile struct {
	Image        string            `json:"image"`
	Port         int               `json:"port"`
	HealthPath   string            `json:"health_path"`
	MountPath    string            `json:"mount_path"`
	Storage      string            `json:"storage"`
	CPU          string            `json:"cpu"`
	Memory       string            `json:"memory"`
	Command      []string          `json:"command,omitempty"`
	Args         []string          `json:"args,omitempty"`
	Env          map[string]string `json:"env,omitempty"`
	EnvSecret    string            `json:"env_secret,omitempty"`
	ConfigSecret string            `json:"config_secret,omitempty"`
	ConfigPath   string            `json:"config_path,omitempty"`
	// CredentialPath 是每个工作区自己的凭据 Secret 的挂载目录。非空才允许
	// 通过 API 注入凭据。它以文件而不是环境变量交付：文件会随 Secret 更新，
	// 环境变量只能靠重启 Pod 才能换新。
	CredentialPath string `json:"credential_path,omitempty"`
	// RestartOnCredentialChange replaces the workload after a credential
	// change, for software that reads its key once at start and cannot reload
	// it. The new key is then live after one restart instead of never. A
	// workspace that is not running is not touched: it starts with the new key.
	RestartOnCredentialChange bool `json:"restart_on_credential_change,omitempty"`
	// AllowedImages lists the images a workspace of this profile may be
	// upgraded to: exact references, or a prefix ending in "*". Empty disables
	// upgrades, because an API that accepts any image is an API that runs any
	// code in the cluster.
	AllowedImages []string `json:"allowed_images,omitempty"`
	// HeartbeatPath, when set, is polled on a running workspace. The workload
	// reports whether it is busy, which counts as activity, and a workload that
	// stops answering is restarted.
	HeartbeatPath string `json:"heartbeat_path,omitempty"`
}

func (p Profile) Validate() error {
	if p.Image == "" || p.Port < 1 || p.Port > 65535 || p.Storage == "" || p.CPU == "" || p.Memory == "" {
		return fmt.Errorf("%w: profile needs image, port, storage, cpu and memory", ErrInvalid)
	}
	if len(p.MountPath) < 2 || p.MountPath[0] != '/' || len(p.HealthPath) == 0 || p.HealthPath[0] != '/' {
		return fmt.Errorf("%w: mount_path and health_path must be absolute", ErrInvalid)
	}
	if p.ConfigSecret != "" && (len(p.ConfigPath) < 2 || p.ConfigPath[0] != '/') {
		return fmt.Errorf("%w: config_secret needs an absolute config_path", ErrInvalid)
	}
	if p.CredentialPath != "" {
		if len(p.CredentialPath) < 2 || p.CredentialPath[0] != '/' {
			return fmt.Errorf("%w: credential_path must be absolute", ErrInvalid)
		}
		if p.CredentialPath == p.MountPath || p.CredentialPath == p.ConfigPath {
			return fmt.Errorf("%w: credential_path must not overlap mount_path or config_path", ErrInvalid)
		}
	}
	if p.HeartbeatPath != "" && p.HeartbeatPath[0] != '/' {
		return fmt.Errorf("%w: heartbeat_path must be absolute", ErrInvalid)
	}
	for _, pattern := range p.AllowedImages {
		if !imagePattern.MatchString(strings.TrimSuffix(pattern, "*")) || pattern == "*" {
			return fmt.Errorf("%w: invalid allowed_images entry %q", ErrInvalid, pattern)
		}
	}
	return nil
}

type Workspace struct {
	ID           string               `json:"id"`
	Profile      string               `json:"profile"`
	Desired      string               `json:"desired"`
	Phase        string               `json:"phase"`
	LastActivity time.Time            `json:"last_activity"`
	UpdatedAt    time.Time            `json:"updated_at"`
	LastError    string               `json:"last_error,omitempty"`
	Leases       map[string]time.Time `json:"leases,omitempty"`
	// ExpiresAt 是租期截止时间，零值表示永不过期。它是回收链条的起点：
	// 到期后工作区先被挂起（保留 PVC），宽限期满才硬删。
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	// SuspendedAt 是进入挂起态的时刻，宽限期从这里开始计时。它必须持久化，
	// 否则控制器重启会把宽限期重新计时，硬删被无限推迟。
	SuspendedAt time.Time `json:"suspended_at,omitempty"`
	// RestartPending 和 Desired 一样是持久化意图：控制器在重启后仍会补做
	// 这次重启，而不是把用户的请求丢掉。
	RestartPending bool `json:"restart_pending,omitempty"`
	// DeletionReason 记录这次删除由谁发起。它不影响行为，只影响审计措辞，
	// 但删除失败重试几轮之后仍然要能说清楚"是谁要删的"。
	DeletionReason string `json:"deletion_reason,omitempty"`
	// CredentialVersion 每次写入凭据加一，0 表示从未写入。凭据的值只存在于
	// Kubernetes Secret 里：这里和审计日志都只保存版本号与键名，所以控制器
	// 的状态库泄露不会泄露 agent 的密钥。
	CredentialVersion   int       `json:"credential_version,omitempty"`
	CredentialKeys      []string  `json:"credential_keys,omitempty"`
	CredentialUpdatedAt time.Time `json:"credential_updated_at,omitempty"`

	// Image overrides the profile's image for this workspace. Empty means
	// "follow the profile". It is set by Upgrade and cleared when a rollback
	// returns to the profile's own image.
	Image string `json:"image,omitempty"`
	// PreviousImage is the image the workspace ran before its last committed
	// upgrade, kept so a later bad release can be undone by hand.
	PreviousImage string `json:"previous_image,omitempty"`
	// RolloutPending means the image above has been changed and the runtime has
	// not yet been seen running it. Reconcile only calls Ensure on a workspace
	// that is not ready, so without this flag a ready workspace would keep its
	// old image after a rollback.
	RolloutPending bool          `json:"rollout_pending,omitempty"`
	Upgrade        *UpgradeState `json:"upgrade,omitempty"`
	LastUpgrade    *UpgradeDone  `json:"last_upgrade,omitempty"`

	// RunEpoch identifies which run of the workload the workspace is in. Store.Put
	// advances it whenever the workload is stopped, started, restarted or given
	// a different image (see replacesWorkload); callers cannot set it. A result
	// obtained from a workload is only valid for the epoch it was requested in.
	RunEpoch int64 `json:"run_epoch,omitempty"`

	// HeartbeatAt is the last successful heartbeat. It is persisted at most
	// once per ActivityFlushInterval, like request activity, so a healthy
	// workspace does not cost a write per beat.
	HeartbeatAt  time.Time `json:"heartbeat_at,omitempty"`
	Unresponsive bool      `json:"unresponsive,omitempty"`
	// AutoRestartAt is the last restart the controller triggered itself after
	// lost heartbeats; it rate-limits further automatic restarts.
	AutoRestartAt time.Time `json:"auto_restart_at,omitempty"`
	AgentVersion  string    `json:"agent_version,omitempty"`

	// TokenBudget 是 Usage.TotalTokens 的上限，0 表示不限。它是终身累计量的
	// 上限，不按周期重置：续期的方式是调高它。
	TokenBudget int64 `json:"token_budget,omitempty"`
	// Usage 是工作负载经心跳报告的累计 LLM 用量，单调不减。从未上报的工作
	// 负载保持零值，序列化时省略。
	Usage Usage `json:"usage,omitzero"`
	// SuspendedFor 说明 Desired=suspended 的原因。空表示租期到期，这是它
	// 出现之前唯一的原因；预算挂起的工作区不会被宽限期硬删，见 applyExpiry。
	SuspendedFor string `json:"suspended_for,omitempty"`
}

// SuspendedForBudget 是 Workspace.SuspendedFor 的取值。
const SuspendedForBudget = "token_budget"

// DeletionReason 的取值。
const (
	DeletedByUser  = "user"
	DeletedByGrace = "grace"
)

// replacesWorkload 判断 next 相对 prev 是否换了一次工作负载：期望状态、重启意图
// 或镜像发生变化。每一项变化要么停掉、要么重建、要么换掉正在运行的 pod，所以
// 在变化之前发出的锁外请求（比如心跳）拿到的结果不再描述现在这个工作负载。
// 意图被执行（RestartPending 或 RolloutPending 清除）同样算一次，因为真正换掉
// pod 发生在这里，而不是在意图被记录的时刻。
func replacesWorkload(prev, next Workspace) bool {
	return prev.Desired != next.Desired ||
		prev.RestartPending != next.RestartPending ||
		prev.RolloutPending != next.RolloutPending ||
		prev.Image != next.Image
}

// expired 判断租期是否已过。零值 ExpiresAt 表示永不过期。
func (w Workspace) expired(now time.Time) bool {
	return !w.ExpiresAt.IsZero() && !now.Before(w.ExpiresAt)
}

// blocked 判断工作区能否被唤醒（start、restart、租约、网关请求）。挂起的
// 工作区拒绝一切非删除操作；wake 为 true 时，已过期但还没被挂起的也拒绝，
// 否则一次请求就能让过期的工作区复活。
func (w Workspace) blocked(now time.Time, wake bool) error {
	switch {
	case w.Desired == DesiredSuspended && w.SuspendedFor == SuspendedForBudget:
		return ErrBudgetExceeded
	case w.Desired == DesiredSuspended || (wake && w.expired(now)):
		return ErrExpired
	}
	return nil
}

// Operation 是一条跨请求的幂等记录。BizID 由调用方提供，是唯一的幂等键；
// 记录只保留一份，所以重复提交能拿到第一次的结论而不是再执行一次。
type Operation struct {
	BizID     string `json:"biz_id"`
	Workspace string `json:"workspace"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	// Generation 标识当前持有这条记录的那一次执行：首次占用为 1，每次过期
	// 接管加 1。FinishOperation 必须带着 BeginOperation 返回的这一代，否则
	// 被接管的旧持有者迟到的收尾会给新持有者仍在执行的操作定性。升级前落盘的
	// 记录没有这个字段（读出为 0），按"第 0 代"处理，接管后进入第 1 代。
	Generation int64 `json:"generation,omitempty"`
	// Error 只保存消息文本。重启后无法还原原始错误类型，所以重放失败时
	// 只能给出消息，不能保证和第一次的 HTTP 状态码一致。
	Error      string    `json:"error,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

type Observation struct {
	Exists   bool
	Ready    bool
	Endpoint string
	// Image is the image of the pods that are actually serving, reported only
	// once the rollout has finished. Ready alone cannot tell an upgraded
	// workload from the old one that is still up while the rollout starts.
	Image string
}

// StartupTimestamps 是最新 pod 的就绪时间线，用来拆冷启动耗时。零值表示那个
// 阶段还没发生（或者缓存还没看到）。
type StartupTimestamps struct {
	Scheduled        time.Time
	ContainerStarted time.Time
	Ready            time.Time
	PodName          string
}

// Complete reports whether every cold-start phase timestamp is present.
func (ts StartupTimestamps) Complete() bool {
	return !ts.Scheduled.IsZero() && !ts.ContainerStarted.IsZero() && !ts.Ready.IsZero()
}

// EventSource 是 Runtime 的可选能力：工作区的运行时状态变化时投递它的 ID。
// 事件只是提示，可以丢；周期调度是兜底。
type EventSource interface {
	WorkspaceEvents() <-chan string
}

// StartupObserver 是 Runtime 的可选能力，只服务于冷启动指标。
type StartupObserver interface {
	StartupTimestamps(context.Context, Workspace, bool) (StartupTimestamps, bool)
}

// MetricsExporter 是 Runtime 的可选能力，把自己对后端的调用统计追加到
// /metrics 输出里。
type MetricsExporter interface {
	WriteMetrics(io.Writer) error
}

// CredentialStore 是 Runtime 的可选能力：为单个工作区保管一组凭据。
// version 随凭据一起写入，让工作区内的 agent 能报告自己加载的是哪一版。
// ClearCredentials 对不存在的凭据不报错。Stop 必须保留凭据，Delete 才会删除。
type CredentialStore interface {
	SetCredentials(ctx context.Context, w Workspace, version int, values map[string]string) error
	ClearCredentials(ctx context.Context, w Workspace) error
}

// Runtime owns compute and workspace storage. Stop must preserve storage;
// Delete removes it only after an explicit workspace deletion request.
type Runtime interface {
	Ensure(context.Context, Workspace, Profile) error
	Observe(context.Context, Workspace, Profile) (Observation, error)
	Stop(context.Context, Workspace) error
	Delete(context.Context, Workspace) error
	// Restart replaces the workload without touching storage. The volume is
	// preserved for the same reason Stop preserves it.
	Restart(context.Context, Workspace) error
	// Probe reports whether the compute backend is reachable. It backs the
	// readiness endpoint, so it must be cheap and bounded by ctx.
	Probe(context.Context) error
}

func ValidName(name string) bool { return namePattern.MatchString(name) }

// validDesired 只接受调用方可以显式设置的目标状态。suspended 是控制面自己
// 的回收决策，不是外部可以指定的意图。
func validDesired(desired string) bool {
	switch desired {
	case DesiredRunning, DesiredStopped, DesiredDeleted:
		return true
	}
	return false
}

func validOpType(opType string) bool {
	switch opType {
	case OpStart, OpStop, OpRestart, OpDelete, OpTokenBudget:
		return true
	}
	return false
}
