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
	fresh := Operation{BizID: bizID, Workspace: workspace, Type: opType, Status: OpProcessing, Generation: 1, StartedAt: now}
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
	if existing.Status == OpProcessing {
		return c.takeOverStale(existing, now)
	}
	return existing, replayError(existing)
}

// replayError 把一条已存在的记录翻译成重复提交应得到的结论。
func replayError(op Operation) error {
	switch op.Status {
	case OpSuccess:
		return ErrOperationSucceeded
	case OpFailed:
		return fmt.Errorf("%w: %s", ErrOperationFailed, op.Error)
	default:
		return fmt.Errorf("%w: started at %s", ErrOperationInProgress, op.StartedAt.UTC().Format(time.RFC3339))
	}
}

// takeOverStale 处理超时未收尾的 processing 记录。控制器在动作中途崩溃会留下
// 一条永远不会自己结束的记录，不接管就等于永久锁死这个 biz_id。接管意味着
// 原请求可能仍在别处执行，所以只有动作本身幂等（写入目标状态）时这个取舍才
// 成立：重启后的副作用是重写一次同样的目标状态，而不是多扣一次钱。
//
// seen 只是调用方更早读到的快照。是否接管要在存储锁内按最新记录重新判定：
// 记录仍是 processing、仍是 seen 的那一代、租约确实已过，三者都成立才接管，
// 并把代数加一。任何一条不成立，都按最新记录给出重复提交的结论，而不是用
// 旧快照覆盖别人已经写下的结果。
func (c *Controller) takeOverStale(seen Operation, now time.Time) (Operation, error) {
	took := false
	cur, err := c.store.updateOperation(seen.BizID, func(cur Operation) (Operation, bool) {
		if cur.Status != OpProcessing || cur.Generation != seen.Generation || now.Sub(cur.StartedAt) < c.OperationLease {
			return cur, false
		}
		cur.Generation++
		cur.StartedAt, cur.FinishedAt, cur.Error = now, time.Time{}, ""
		took = true
		return cur, true
	})
	if err != nil {
		return seen, err
	}
	if !took {
		return cur, replayError(cur)
	}
	c.Metrics.OperationTakeovers.Add(1)
	return cur, nil
}

// FinishOperation 把记录置为终态。op 必须是 BeginOperation 返回的那条记录：
// 收尾绑定到它所属的那一代执行。终态先到先得，重复收尾不会把已经成功的记录
// 翻成失败。传入 nil 表示成功，否则记录错误文本。
//
// 如果记录已被后来的接管换到新一代，旧持有者的结果不会写入，返回
// ErrOperationSuperseded，由调用方决定如何记录这件事。
func (c *Controller) FinishOperation(op Operation, opErr error) error {
	superseded := false
	_, err := c.store.updateOperation(op.BizID, func(cur Operation) (Operation, bool) {
		if cur.Status != OpProcessing {
			return cur, false
		}
		if cur.Generation != op.Generation {
			superseded = true
			return cur, false
		}
		cur.FinishedAt = c.now()
		if opErr != nil {
			cur.Status, cur.Error = OpFailed, opErr.Error()
		} else {
			cur.Status = OpSuccess
		}
		return cur, true
	})
	if err == nil && superseded {
		return fmt.Errorf("%w: biz_id %q", ErrOperationSuperseded, op.BizID)
	}
	return err
}
