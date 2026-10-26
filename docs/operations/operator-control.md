# Operator status and dead-letter recovery

状态：introduced in `wake-bridge@0.9.0-preview.3`; current in `0.9.0-preview.19`。

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

## 一条 wake 卡住时的本地只读排查

先确认正在查看同一个 Agent Space 的 config/daemon，不要启动第二个 canonical daemon，也不要通过手改 SQLite、自动 ack/consume 或批量 retry 清空红色状态。
`wakebridge status --config /absolute/path/wakebridge.config.json` 可读 durable 状态；live source 状态按上面的 authenticated `/v1/status`，
或由 owning agent 使用 `attention_source_status` 查看。Service `active` 仅证明进程状态，不证明 source 已启用、host 已接收或 agent 已处理。

Agent 可通过已配置的本地 Wake Bridge MCP 依次调用这些只读工具（下列是工具名与 JSON 参数，不是 shell command）：

```text
attention_binding_status {"channel":"life"}
attention_wake_health {"channel":"life","limit":10}
attention_wake_health {"batch_id":"wb_..."}
```

先检查 channel 与 live binding，再从 issues 选出准确 batch drill-down。`attempt` 是累计尝试数，不表示当前仍会自动重投。
`core_dispatch.eligible` 仅说明当前 state 属于 Core 自动派发候选，不保证下一轮一定投递；还要核对 `in_flight`、`not_before`、
binding/lease 和 claims。`needs_attention` 不在自动派发候选中，不能仅凭旧 attempt 增长记录认定它仍在重试。

### 不要混用三种时间边界

| 字段 / 配置 | 含义 | 不代表什么 |
| --- | --- | --- |
| batch `deadline` | 合批窗口结束时间；创建时为当前时间加 `batch.window_ms` | 不是 HTTP 接收响应的超时预算 |
| Host Adapter reference `timeout_ms` | 本地 HTTP adapter 等待接收响应的预算，默认 5000ms，范围 100–30000ms | 不是等待 agent 完整处理工作的时限 |
| `ack_timeout_ms` / `delivery.agent_ack.deadline_at` | 接受后等待 agent 确认的时限 / 对应确认截止时间 | 不是合批窗口，也不触发对已接受投递的自动重发 |

batch 创建时 `not_before` 为当前时间，因此零长度合批窗口下 `not_before == deadline` 是正常读数。
失败重试会另行推进 `not_before`；不要用 `deadline - not_before` 计算 HTTP 超时。
应检查正在运行的 daemon 实际加载的 Host Adapter reference，而不是只看某个文件里的值或凭一次耗时反推配置。
接收边界见 [Host Adapter protocol](out-of-process-host-adapter.md#接收不是整轮处理完成)。

读证据时分别看 `evidence.transport_accepted`、`evidence.host_attested`、`evidence.agent_seen`，并核对已有 attempts/receipts：
旧 `delivery_state` 或 batch state 的 `seen` 可能来自 host attestation，不自动意味着 `agent_seen`。单 batch 的 `transitions` 提供最近最多
100 条 seq/from_state/to_state/at，`transitions_truncated` 提示历史是否截断；它保留时间线但不暴露任意 reason 正文。
`missing_claim_ids` 是缺失引用，不能当作工作已完成。`next_step` 给出固定操作提示，仍须按真实宿主结果决定操作。

`delivery.agent_ack` 是读取时计算的只读诊断，不改变旧 state：`pending` 要求当前 accepted attempt 有匹配的真实接受/注入证据，
即 `transport_accepted` receipt，或同 attempt、batch、endpoint/generation/transport 的 consumed nonce correlation。
后者覆盖 HTTP 回程丢失但宿主已证明注入的情况，不会补造 `transport_accepted` receipt。
还须尚无 `agent_seen`，且 batch 处于 `dispatched`、`seen` 或 `needs_attention / ack_timeout`，
并仍引用未完成或缺失的 Claim。空引用、全部 Claim 已 consumed/dismissed/expired 均不算 pending。
`deadline_at` 从该 attempt 的接受完成时间加 `ack_timeout_ms` 推算，`now >= deadline` 时 `overdue=true`；
无待确认项时为 null，合法极值 timeout 超出 JavaScript Date 可表示范围时也为 null，但保留 pending 和数值超时判断。
默认列表除原 actionable states 外，也会列出有 `host_attested`、无 `agent_seen` 且仍有未完成或缺失 Claim 的 `seen` 批次。
该筛选在最近 `limit` 条之前完成，结果仍按时间先后返回。

- `waiting_for_endpoint`：检查精确 session 是否在线、channel 是否匹配、lease/generation 是否当前；不要反复 emit 新事件替代修复绑定。
- timeout / retry wait：核对 HTTP 尝试与真实 host 接收记录。Timeout 不证明未注入；有重复风险，不要伪造 202 或盲目手工补投。
- transport accepted、无 agent ack：核对宿主是否确实注入、是否因 busy/权限等待，以及 agent 是否读到 envelope；不能由 adapter 代 ack。
- 全部 claims finalized / empty：Core 在恢复/派发扫描时，会将原 claim 引用非空、每条均存在且已 consumed/dismissed/expired、仍处于派发等待状态的批次收尾为 `cancelled`，历史 `needs_attention / empty_batch` 也适用。Attempts、receipts 和原引用保留；空引用、missing claim 或尚未完成的 claim 不会被当作完成证据。仍有异常时保留证据排查，不删库修复。
- `dead_letter`：先修复实际拒绝原因，符合下一节 CAS/claim 条件后才能显式 retry；不要把 `needs_attention` 当 dead letter。

确认 agent 真正读到 wake 后，用 `attention_ack({"wake_batch_id":"wb_..."})` 记录 agent seen。若抢在接受落地前得到
`409 batch_not_dispatched`，确认同 batch 当前 accepted attempt 后可重试同一 ack；不要吞掉错误或对 stale generation 等所有 409 无限重试。
回读权威来源并审阅/处理对应 claim 后才 consume。最终验收是一次真实 agent ack 加实际来源处理，不是 daemon active、打印 envelope 或 transport 202。

## Retryable transport failure

自 `0.9.0-preview.18` 起，对未被接受、可重试且未提供有效 `retry_after_ms` 的 transport 结果使用有上限的指数退避：默认等待
5、10、20、40、80、160、300 秒，此后每次 300 秒。从每次尝试完成时起算，准确的下一次时间以 batch 的
`not_before` 为准。升级不重写已保存的等待时间，后续失败才按新规则计算。

退避复用持久化的累计 `attempt`，重启或换 binding generation 不将计数归零；它不是“连续网络失败次数”。直接嵌入
Core 时，既有 `retry_delay_ms` 作为基数：0 保留立即重试，基数超过 300 秒时不被缩短。有效的 transport
`retry_after_ms` 仍优先（包括 0），沿用最高 24 小时限制。本次不增加实例文件或 CLI 配置项。

这是降低重复请求频率，不是次数上限、自动告警或 exactly-once 保证；工作不会因为尝试多次而被静默丢弃。
HTTP timeout 仍表示接受结果未证实，宿主可能已经收到，必须使用稳定业务标识去重或核对真实注入证据。
有真实接受/host attestation 的 batch 不因缺 agent ack 而重新进入这个重试路径。派发前 generation fencing 失败
沿用原恢复间隔，不被包装成网络投递失败。

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
因此，host-attested `seen` 的旧 `ack_timeout` 虽已解除，读取时仍可能显示 `agent_ack.pending / overdue`；这只表示缺少真实 agent 确认。
Core 的确认超时扫描仍只将 `dispatched` 转成 `needs_attention`，不会为这类 `seen` 诊断生成新的 core incident 或主动提醒，
也不会自动重投或让 adapter 代 ack。仍须核对实际注入与 agent 是否读到，再由 agent 自己确认。

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
