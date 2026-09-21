package control

import (
	"fmt"
	"time"
)

// BeginOperation 尝试占用一个幂等记录，是"重复请求只产生一次副作用"的唯一
// 入口。调用方必须先 BeginOperation，只有拿到 err == nil 才允许执行动作。
//
// 返回值约定：
//   - err == nil：首次执行；动作结束后必须调用 FinishOperation。
//   - ErrOperationInProgress：同一个 biz_id 正在执行，不要重复执行。
//   - ErrOperationSucceeded：已经成功过，返回第一次的记录。
//   - ErrOperationFailed：已经失败过，返回第一次的错误文本。
//   - ErrConflict：同一个 biz_id 换了工作区或换了动作类型。
//   - ErrInvalid：biz_id 不合法或缺少必要参数。
func (c *Controller) BeginOperation(bizID, workspace, opType string) (Operation, error) {
	if !bizPattern.MatchString(bizID) || workspace == "" || !validOpType(opType) {
		return Operation{}, fmt.Errorf("%w: biz_id, workspace and a known type are required", ErrInvalid)
	}
	now := c.now()
	fresh := Operation{BizID: bizID, Workspace: workspace, Type: opType, Status: OpProcessing, StartedAt: now}
	existing, found, err := c.store.putOperationIfAbsent(fresh)
	if err != nil {
		return Operation{}, err
	}
	if !found {
		c.Metrics.OperationsStarted.Add(1)
		return fresh, nil
	}
	c.Metrics.OperationReplays.Add(1)
	if existing.Type != opType || existing.Workspace != workspace {
		return existing, fmt.Errorf("%w: biz_id %q already used for %s on %s", ErrConflict, bizID, existing.Type, existing.Workspace)
	}
	switch existing.Status {
	case OpSuccess:
		return existing, ErrOperationSucceeded
	case OpFailed:
		return existing, fmt.Errorf("%w: %s", ErrOperationFailed, existing.Error)
	default:
		return c.takeOverStale(existing, now)
	}
}

// takeOverStale 处理超时未收尾的 processing 记录。控制器在动作中途崩溃会留下
// 一条永远不会自己结束的记录，不接管就等于永久锁死这个 biz_id。接管意味着
// 原请求可能仍在别处执行，所以只有动作本身幂等（写入目标状态）时这个取舍才
// 成立：重启后的副作用是重写一次同样的目标状态，而不是多扣一次钱。
func (c *Controller) takeOverStale(existing Operation, now time.Time) (Operation, error) {
	if now.Sub(existing.StartedAt) < c.OperationLease {
		return existing, fmt.Errorf("%w: started at %s", ErrOperationInProgress, existing.StartedAt.UTC().Format(time.RFC3339))
	}
	taken := existing
	taken.StartedAt, taken.FinishedAt, taken.Error = now, time.Time{}, ""
	if err := c.store.putOperation(taken); err != nil {
		return existing, err
	}
	c.Metrics.OperationTakeovers.Add(1)
	return taken, nil
}

// FinishOperation 把记录置为终态。终态先到先得：重复收尾不会把已经成功的
// 记录翻成失败。传入 nil 表示成功，否则记录错误文本。
func (c *Controller) FinishOperation(bizID string, opErr error) error {
	op, err := c.store.GetOperation(bizID)
	if err != nil {
		return err
	}
	if op.Status != OpProcessing {
		return nil
	}
	op.FinishedAt = c.now()
	if opErr != nil {
		op.Status, op.Error = OpFailed, opErr.Error()
	} else {
		op.Status = OpSuccess
	}
	return c.store.putOperation(op)
}
