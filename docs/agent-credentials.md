# Agent 凭据注入与轮换

工作区里跑的 agent 需要一把 LLM API Key。这份文档说明控制面如何把它送进去、如何在不重启的情况下换掉，以及**哪些事情它保证、哪些不保证**。

## 为什么不用环境变量

环境变量在容器启动时固定，换 Key 只能重启 Pod。重启会打断正在进行的对话和任务，所以这里把凭据放进每个工作区自己的 Secret，以**文件**形式挂载。挂载的 Secret 文件会随 Secret 更新而更新，agent 每次调用模型前重新读取，轮换不需要重启。

## 机制

```
PUT /v1/workspaces/{id}/credentials     整体替换，返回版本号和键名
GET /v1/workspaces/{id}/credentials     只返回元数据
DELETE /v1/workspaces/{id}/credentials  清除
```

- profile 通过 `credential_path` 声明挂载目录。没有声明的 profile 调用这些接口返回 400，行为与之前完全一致。
- 控制器为每个工作区维护一个 Secret `nc-<id>-cred`，带 `managed-by` 与 `workspace` 标签。重名但没有这两个标签的 Secret 会被拒绝覆盖，也会阻止硬删，与 Deployment、Service、PVC 的归属检查一致。
- 控制器额外写一个 `.version` 文件。agent 在状态接口和每次回复里报告它读到的版本，所以"轮换是否生效"可以被外部观察，而不是靠猜。
- Pod 模板只引用 Secret 的名字，不含版本号。轮换不改 Deployment，不触发滚动。有一个单测专门断言这一点。
- 卷是 `optional`：工作区可以在设置第一把 Key 之前启动并通过就绪探针，否则 Key 永远送不进去。
- 只挂目录，不用 `subPath`：`subPath` 挂载不会收到更新。

## 控制面不保存凭据的值

- 值只存在于 Kubernetes Secret。工作区状态库（bbolt）只记录版本号、键名和更新时间。
- 审计日志只记录 `version=N keys=a,b`。测试检查审计和 HTTP 响应里都不出现值，校验失败的错误信息也不回显值。
- 不能保证的是 etcd 里的静态加密：那取决于集群配置。Secret 默认只是 base64。

## 写入顺序与故障

`SetCredentials` 先写 Secret，再写状态库。中间崩溃会让 Secret 的版本比状态库多一，下一次调用会算出同一个版本号并覆盖它，不需要修复步骤。反过来的顺序会记录一个没有 Secret 对应的版本，所以不采用。

- 允许在停止和挂起的工作区上轮换：泄露的 Key 必须在没有任何进程运行时也能被换掉，Pod 下次启动时读到新文件。只有已删除的工作区拒绝。
- `Stop`、`Restart` 保留凭据，和 PVC 一样；只有硬删才删除 Secret。
- PUT 整体替换：请求里没有的键会从 Pod 里消失，不会留下过期的凭据。
- 不保证：同一时刻两个并发的 PUT 之间的先后顺序由工作区锁决定，调用方不应依赖。

## 轮换延迟

kubelet 刷新挂载的 Secret 文件靠它自己的同步周期，不是即时的，通常是分钟量级。

控制器在写完 Secret 后，给该工作区的 Pod 加一个版本注解。这个改动不碰 Pod 模板，不会滚动，但 Pod 对象的更新会让 kubelet 立刻同步卷，延迟降到秒级。注解失败只记日志，kubelet 仍会按自己的周期收敛，所以它只影响速度，不影响正确性。

`scripts/kind-credentials.sh` 会打印实测的轮换延迟，需要时自己跑一遍。仓库不附结果数字。

## 验证了什么

`scripts/kind-credentials.sh` 用真实控制器、真实 agent Pod 和 fake LLM 走一遍：

1. 启动前设置 Key，agent 用它成功回答。
2. 提供方撤销旧 Key，agent 被拒绝（502），并报告它还在用版本 1。
3. 用 PUT 送入新 Key。agent 恢复，版本变为 2；脚本断言 **Pod UID、进程号不变，重启次数为 0**。
4. 停止再启动：Key 和对话历史都还在；硬删后 Secret 被删除。
5. 审计日志里没有任何 Key 的值。

## 不会热加载的程序

有些程序只在启动时读一次 Key（例如把它写进进程内的客户端配置）。对这类 profile 设置 `restart_on_credential_change: true`：每次 `PUT` 或 `DELETE` 凭据后，控制器会把运行中的工作区替换成新 Pod，卷保留，新 Pod 启动时读到新 Key。

- 只对正在运行的工作区生效。已停止或挂起的工作区不动，它们下次启动时本来就会读到新 Key。
- 替换是一次重启，会打断进行中的请求；热加载的 agent 不需要它，默认关闭。
- 重启意图和其他意图一样持久化，控制器在中途崩溃也会补做。

## agent 自身

`cmd/agent-runtime` 是一个很小的 ReAct agent，目的是给控制面一个真实的负载，而不是模型本身：

- 每次调用模型前重读凭据，并在读取前后各检查一次版本，避免读到轮换中途的半新半旧状态。
- 会话以 JSONL 存在工作区卷上，所以停止、重启后对话还在。
- 一轮对话只有在得到最终回答后才落盘。模型调用失败（比如 Key 被撤销）时重试不会堆积没有回答的用户消息。重放时丢弃崩溃留下的不完整一轮。
- 文件工具只能访问工作区的 `files/` 目录，使用 `os.Root`，拒绝 `..`、绝对路径和指向目录外的符号链接。
- `cmd/fake-llm` 是一个确定性的 OpenAI 兼容端点，只接受它被告知的 Key，用来演示和测试轮换。它不是模型。

## 已知边界

- 升级、回滚和心跳见 [upgrade-and-heartbeat.md](upgrade-and-heartbeat.md)。按 agent 版本做兼容性迁移和生命周期钩子仍未做。
- 共享的控制令牌仍然能给任何工作区写凭据，只应留给运维。需要让不同调用方各管各的工作区时，用 `-tokens` 配置作用域令牌，见 [control-plane.md](control-plane.md) 第 11 节：作用域令牌只能轮换自己作用域内的工作区，越权得到和"不存在"一样的 404。
- 凭据文件模式是 0440，靠 Pod 的 fsGroup 让 agent 读取。容器里与 agent 同组的其他进程也能读到它。
