# Explicit Inactivity Watch

## 状态与边界

Inactivity Watch 是 Wake Bridge 的调度能力。Agent 可以为自己的 attention channel 显式登记一个条件：当前已绑定
session 在一段时间内没有可信活动时，生成内部 Event 与 Attention Claim。

当前单 Space release baseline 以固定 `instance_id` 承载逻辑 Agent Space；
Event/Claim 使用 `origin=inactivity_watch`，trigger 使用 condition-based `once | repeat`。旧 v7 实验数据升级时统一
fail-closed 为 `once`，不会因迁移自动获得重复调用。

这不等于 Wake Bridge 保证 agent 记忆连续性。Bridge 不自动替 agent 创建 watch，不判断 agent 是否“忘了”，也不
根据回复内容、self-commitment 或 Claim 状态推断它是否已经想起某事。换窗后的身份、上下文恢复与 register/takeover
仍由 agent harness 负责。

## 作用域

Watch 的逻辑归属键是：

```text
(space_id, attention_channel)
```

它观察的对象始终是该 channel 当前 binding 指向的：

```text
(endpoint_id, binding_generation)
```

takeover 后旧 generation 的 activity lease、waiter 与后续 observation 全部失效。Watch 配置仍留在 channel 上，但在
新 binding 建立且收到可信 activity baseline 前处于 `waiting_for_activity`，不能把旧 endpoint 的沉默解释为新
session 已失活。

没有 binding 时状态为 `waiting_for_endpoint`。Host Adapter 不具备可信 activity-observer capability 时状态为
`unsupported`；Bridge 不用进程存在、窗口 focus、模型输出、普通 source event 或 Wake 自己的投递来猜活动。

## 注册 contract

```json
{
  "space_id": "spc_01...",
  "attention_channel": "life",
  "enabled": true,
  "idle_after_seconds": 7200,
  "trigger": {
    "mode": "repeat",
    "repeat_after_seconds": 1800
  },
  "state": "armed"
}
```

`trigger.mode` 支持：

- `once`：本次 inactive epoch 只触发一次；
- `repeat`：只要当前 binding 持续 inactive，就按显式 `repeat_after_seconds` 再触发；真实活动恢复后结束本轮，下一次
  失活开启新 epoch。

`repeat` 是普通的条件调度，不表示 Bridge 会检查 agent 是否记住、是否创建后续安排或是否完成某事。实现必须设置
公开的最小间隔与服务级速率上限，防止错误配置拖垮本机服务；这些限制必须出现在 configure preview/结果中，不能
静默改变 agent 请求。

Agent-facing 操作：

```text
attention_watch_status(attention_channel?)
attention_watch_configure(
  attention_channel,
  enabled,
  idle_after_seconds,
  mode,
  repeat_after_seconds?
)
```

只有 owning agent capability 可以配置或关闭 watch。普通 source、外部 webhook 与另一个独立实例不能操作它。当前
最小 interval 为 60 秒，每个 scheduler tick 最多 materialize 16 个 inactivity sequence；两项限制会随 status 返回。

## Activity observation

可信 Host Adapter 续 activity lease 时至少提交：

```json
{
  "space_id": "spc_01...",
  "attention_channel": "life",
  "endpoint_id": "ep_01...",
  "binding_generation": 12,
  "observation_id": "act_01...",
  "observed_at": "2026-08-30T12:00:00Z",
  "lease_expires_at": "2026-08-30T12:01:00Z",
  "kind": "session_turn",
  "cause": "independent"
}
```

Bridge 只接受当前 binding generation、短时有效且可去重的 observation。Wake runner 的 start/progress/settled、Bridge
投递、Claim ack/consume/dismiss，以及 source poll 都不是用户或 agent 的独立活动，不能重新开始 idle 计时。
可信 activity 可以是用户交互，也可以是 harness 观察到的独立 session turn；关键是它发生在该 endpoint 上且
`cause=independent`，不是本 watch 所触发 Wake 的回声。轮询型 adapter 不一定能看到 turn 的 start edge，因此当前
generation 的第一条 `activity_progress` 或 `activity_settled` 也可以建立 baseline；`wake_started/progress/settled` 永远
不能建立或重置 baseline。Foreground Presence 仍只接受真实用户 active edge，两种
lease 不应混成同一个语义。

## 触发与幂等

当 activity lease 到期并达到 `idle_after`：

1. scheduler 事务性记录 inactive epoch 与本次 sequence；
2. 创建一个 `wakebridge.core/channel.inactive` Event；
3. 创建一个 `origin=inactivity_watch` 的 Attention Claim；
4. 正常经过 quiet hours、presence、busy、binding 与 transport gates；
5. `once` 进入 `fired`；`repeat` 从本次触发时间计算下一次 due；
6. 新的可信独立活动关闭当前 epoch、清零 repeat sequence，并重新武装 watch。

建议 dedupe/resource 形式：

```text
dedupe:  activity-watch:{channel}:{binding_generation}:{epoch}:{sequence}
resource: wakebridge://activity-watch/{channel}/{epoch}/{sequence}
```

重启、scheduler reconciliation、delivery retry 与 Claim 状态变化不能复制同一 sequence。Claim 被消费或 dismiss 也
不能提前或延后下一次 repeat；重复节奏只由已登记 watch、真实 activity 和 scheduler 时间决定。

## 与 harness 的分界

| 场景 | Wake Bridge 行为 |
| --- | --- |
| agent 没有登记 watch | 不观察、不触发 |
| 当前没有 binding | `waiting_for_endpoint`，不猜窗口 |
| 新 session takeover | fence 旧 generation，等待新 activity baseline |
| adapter 无可信 activity observer | `unsupported`，不从沉默推断 inactive |
| 当前 binding 达到 idle threshold | 按 `once` 或 `repeat` 生成可审计 Event/Claim |
| agent 换窗后忘记恢复身份或 binding | 保持等待；这是 harness 的恢复责任 |
| agent 被唤醒后仍没记住某事 | Bridge 不判断；repeat 只因 inactivity 条件继续成立而触发 |

## 不进入当前 contract 的旧实验（historical）

旧设计中的 `until_covered` 会读取 durable self-commitment/Claim coverage，试图判断一次 continuity wake 是否让 agent
“补上了安排”。这把调度器变成了认知连续性检测器，现已撤回。若 agent 想在持续 inactive 时重复收到注意力，应
直接登记 `mode=repeat`；Wake Bridge 不以“记住了没有”作为停止条件。

## 验收要求

- 两个 Agent Space 的同名 channel/watch 完全隔离；
- 无显式 watch 时绝不自动触发；
- 无 binding 与无 observer capability 分别报告 `waiting_for_endpoint`、`unsupported`；
- takeover 后旧 generation observation 不能续期或触发；
- `once` 每个 inactive epoch 一次，`repeat` 每个 due sequence 一次；
- 重启、重复 observation 与 delivery retry 不复制同一 Event/Claim；
- 真实活动恢复会关闭旧 epoch，Wake 自身活动不会；
- status/preview 明确显示 threshold、mode、repeat interval、当前 binding generation、last activity、next due 与限制。
