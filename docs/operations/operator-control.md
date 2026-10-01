# Operator status and dead-letter recovery

状态：introduced in `wake-bridge@0.9.0-preview.3`; current in `0.9.0-preview.14`。

这组命令面向单 Agent Space 的 operator，不属于第三方 SDK，也不进入 agent MCP。它只补运行态定位与永久投递失败的受控
恢复，不替 agent 判断来源工作是否仍需处理。

## Secret-free status

停止或无法访问 daemon 时，可以直接读取 durable DB：

```bash
wakebridge status --config /absolute/path/wakebridge.config.json
```

这个路径以 observer mode 打开 Core，不回收 expired dispatch lease、不推进确认超时状态，也不运行 scheduler。它汇总：

- 各 batch state 数量、当前 due 数、expired dispatch leases；
- failed attempt 的 `error_class` 计数，以及最多 100 条 dead-letter / needs-attention 摘要；
- endpoint/binding 的 live/stale 数量与 channel generation；
- durable source checkpoint/control 摘要。

离线读取无法观察另一个 daemon 进程内存中的 source supervisor，因此返回 `sources.runtime_observed=false`。运行中应优先请求
owner-authenticated live endpoint：

```bash
curl --fail-with-body \
  -H "Authorization: Bearer ${WAKEBRIDGE_ADMIN_TOKEN}" \
  http://127.0.0.1:4311/v1/status
```

Live status 会加入 source 的 `healthy | backoff | needs_attention | stopped` 状态与已启动 Host Adapter 摘要。Source/host
credential 不能调用该 endpoint。

`health` 语义：

- `healthy`：没有当前 queue/source/host degradation；
- `degraded`：存在可自动恢复的 retry/backoff，或 batch 正等待 endpoint；
- `needs_attention`：存在 dead letter、needs-attention batch、expired dispatch lease 或 source needs-attention。

`ok=false` 与 CLI exit code `2` 只对应 `needs_attention`；`degraded` 仍返回 exit code `0`，但应由监控展示。Status 不返回 route
address、lease token、session ref、provider error message、source credential 或 owner token。它只保留安全的 error class；详细 provider
错误仍留在本地 DB/operator evidence 中，不进入结构化 health payload。

## 列表读取

`wakebridge events|claims|batches|receipts`、owner HTTP `GET /v1/events|claims|batches|receipts` 与 MCP
`attention_list` / 不带 `after` 的 `attention_event_list` 都返回**最近**的 `limit` 条，结果仍按时间从旧到新排列。
HTTP 可用 `?limit=` 指定 1–100000；events 默认 100，其余默认 1000。带 `after` 的 `attention_event_list` 从该时间向后翻页。
需要完整数量时用 `attention_status` 或 `wakebridge status`，它们直接在 SQL 中计数，不受列表上限影响。

## Accepted but unacknowledged delivery

自 preview.10 起提供；preview.8 / preview.9 没有此确认超时处理。

Canonical dispatcher 会将接受投递后长期未收到 agent ack 的 batch 从 `dispatched` 转成
`needs_attention`，`last_error=ack_timeout`，并保留一条状态转换记录。该异常会进入上述 operator health 与
needs-attention 摘要；agent 可用 `attention_wake_health` 查看 accepted 时间、attempt 与已有 receipts。

默认时限为 30 分钟，从当前 accepted attempt 的完成时间起算，而不是事件发生、开始投递或最近一次状态变更时间。
实例配置可设置 `ack_timeout_ms`；CLI/daemon 的 `WAKEBRIDGE_ACK_TIMEOUT_MS` 环境变量优先于配置文件。值必须是正整数毫秒。
时限用于发现缺失确认，不是宿主必须在此时间内完成工作的承诺。

- 不会自动重投：adapter 可能已经实际投递，重投可能重复打扰。
- 不会自动标已读或已处理：Claim/Event 和 receipt 保持原样。
- 若当前 binding 对应的原 accepted attempt 后来收到真实 ack，可转为 `seen` 并解除本项异常；旧 generation 仍被拒绝。
- 若该 batch 携带的每条 Claim 后来都已 `consumed`、`dismissed` 或 `expired`，canonical dispatcher 会在下一轮把它转为
  `cancelled`（转换原因 `all_claims_finalized_reconciled`，`last_error` 仍为 `ack_timeout`），不再占据 needs-attention。
  它至少会以 `needs_attention` 停留一轮，已配置的 core incident 照常发出；只要还有一条 Claim 可能再次投递，本项异常就保持打开。
- `batch-retry` 仍只接受 `dead_letter`，不能重开本项异常。先核对 host/agent 的真实结果，再决定是否需要另行安排唤醒；
  不要为清空异常而伪造 ack。
- 升级后，已有的超时记录会在 dispatcher 恢复/扫描时按相同规则暴露出来。这不是新增投递，也不要求修改数据库 schema。

若希望这些状态主动进入某个运维 attention channel，可在实例配置写入 `incident_attention_channel`，或设置
`WAKEBRIDGE_INCIDENT_ATTENTION_CHANNEL`。该功能默认关闭；启用后只为新检测到的 `ack_timeout` 与 source
`needs_attention` 创建去重的 `wakebridge.core` claim。Core incident 自身超时不会再次创建 incident，避免递归告警。

支持 session activity 的 Host Adapter 可用当前 generation 的一次性 delivery nonce 回报 `host_attested`。这会将 batch 收尾为
`seen` 并解除 `ack_timeout`，但不会生成 `agent_seen`、`agent_consumed` 或 `agent_acted`。

## Dead-letter retry

先读取 dead letter 的 `batch_id` 与当前 `attempt`，修复导致 permanent rejection 的 route、credential 或 provider configuration，
再显式重开：

```bash
wakebridge batch-retry wb_... \
  --config /absolute/path/wakebridge.config.json \
  --expected-attempt 1 \
  --reason "operator confirmed route repair"
```

运行中也可以使用 owner API：

```bash
curl --fail-with-body \
  -X POST http://127.0.0.1:4311/v1/batches/wb_.../retry \
  -H "Authorization: Bearer ${WAKEBRIDGE_ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"expected_attempt":1,"reason":"operator confirmed route repair"}'
```

Contract：

1. 只接受 `dead_letter`；`needs_attention`、等待 endpoint、已 dispatched/seen 的 batch 不可重开；
2. `expected_attempt` 是 CAS fence，防止旧命令重开更新后的失败 generation；
3. 所有 claim 必须仍为 `batched`；已经 consume/dismiss/finalize 的工作不会被复活；
4. 成功只把 batch 改为 `retry_wait` 并写 `dead_letter → retry_wait` transition，调用本身不 dispatch；
5. 相同 attempt 的响应丢失后重放返回 `retried=false`，不会重复写 transition；
6. 后续 canonical daemon scheduler 或 operator 显式 `dispatch` 才创建新 attempt；attempt number 会递增。

Reason 会作为本地 transition audit 保存；不要把 credential、正文或其他 secret 写进 reason。命令限制为 1–200 个可打印字符。

## 历史清理

Wake Bridge 默认永久保留所有记录，不会自动删除任何东西。需要控制库的增长时，由 operator 显式执行：

```bash
wakebridge prune --config /path/to/wakebridge.config.json --older-than-days 30          # 只预览
wakebridge prune --config /path/to/wakebridge.config.json --older-than-days 30 --apply  # 实际删除
```

不带 `--apply` 时只返回将被删除的数量，不改动数据库。`--older-than-days` 必填，范围 1–3650，按记录最后更新时间计算。

会删除（最后更新早于截止时间）：

- 状态为 `seen` 或 `cancelled`、且不再携带任何仍可投递 Claim 的 batch，连同它的状态转换、attempt、receipt 与 delivery
  correlation；
- 已 `consumed`、`dismissed` 或 `expired`、且没有任何保留下来的 batch 再引用的 Claim，连同它的状态转换。

永远不删除：Event、event status 与 event transition、idempotency key。它们是去重记录：删掉后，来源重投旧事件或 agent 重放旧的
schedule 请求都会被当成新工作再次唤醒。`pending`、`waiting_for_endpoint`、`retry_wait`、`dispatching`、`dispatched`、
`needs_attention` 与 `dead_letter` batch，以及任何仍可能再次投递的 Claim，无论多旧都保留。

清理之后：

- 来源重投已清理事件时仍返回 `duplicate: true`，只是不再附带 Claim；
- 用同一个 idempotency key 重放已清理的 self commitment 会得到 `claim_pruned`（409），需要换一个新 key；
- SQLite 文件不会立刻变小，释放的页会被后续写入复用。

预览、计数和删除在同一个写事务里完成，可以在 daemon 运行时执行。建议首次 `--apply` 前先用
[release lifecycle](release-lifecycle.md) 的 backup 留一份快照。

## 仍未覆盖的 operator hardening

本票没有承诺 metrics backend、长期 structured log pipeline、dead-letter bulk replay、任意 export、Linux/systemd、端口扫描或自动
backup drill。`doctor/release-preflight` 继续负责 runtime/schema/config permission readiness；backup/restore 流程见
[release lifecycle](release-lifecycle.md)。这些剩余项应按独立风险与验收继续推进，不能因有了 status 就宣称 production operations
全部完成。
