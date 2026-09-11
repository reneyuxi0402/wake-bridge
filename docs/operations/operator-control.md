# Operator status and dead-letter recovery

状态：introduced in `wake-bridge@0.9.0-preview.3`; current in `0.9.0-preview.9`。

这组命令面向单 Agent Space 的 operator，不属于第三方 SDK，也不进入 agent MCP。它只补运行态定位与永久投递失败的受控
恢复，不替 agent 判断来源工作是否仍需处理。

## Secret-free status

停止或无法访问 daemon 时，可以直接读取 durable DB：

```bash
wakebridge status --config /absolute/path/wakebridge.config.json
```

这个路径以 observer mode 打开 Core，不回收 expired dispatch lease，也不运行 scheduler。它汇总：

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

## 仍未覆盖的 operator hardening

本票没有承诺 metrics backend、长期 structured log pipeline、dead-letter bulk replay、任意 export、Linux/systemd、端口扫描或自动
backup drill。`doctor/release-preflight` 继续负责 runtime/schema/config permission readiness；backup/restore 流程见
[release lifecycle](release-lifecycle.md)。这些剩余项应按独立风险与验收继续推进，不能因有了 status 就宣称 production operations
全部完成。
