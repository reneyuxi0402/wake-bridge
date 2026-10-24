# First Wake Quickstart

状态：`wake-bridge@0.9.0-preview.19` current onboarding（发布候选）。

这条路径从全新安装走到第一条可审计 wake。先选择与你的宿主匹配的接入路径，再安装和验收。

## 先选 Host 路径

1. **显式启动的在线 Claude Code TTY session**：使用随包提供的实验性 [Claude CLI online adapter](../host-adapters/claude-cli/README.md)。已验证 Claude 2.1.270 的 TTY interactive session（直接运行或 detached tmux）；不支持普通已运行 session 的事后挂接、print mode、长驻 stdin `stream-json` 或 cold launch。安装、启动及完整验证范围见该 README。
2. **已有自己的 backend、runner 或 frontend**：实现 [out-of-process Host Adapter](operations/out-of-process-host-adapter.md)，用宿主自己的精确 session 接收/注入 API。
3. **只想检查协议 wiring**：运行 [Reference Host Adapter](../examples/reference-host-adapter/README.md)。默认 stdout sink 不会唤醒 agent；打印 envelope 或返回 202 不是端到端成功。

## 开始前先确认

你需要两个彼此独立的接入面：

1. **Host Adapter**：知道要唤醒的精确 session，并具备如实声明的在线投递、启动、恢复或排队能力。Core 不猜测窗口；随包 Claude adapter 也只有上述在线范围。
2. **Source Connector**：可选。它从论坛、邮件或群聊等来源读取事实；上游 credential 只由 Connector 保管。

以下通用步骤使用自制 adapter；选择 Claude 路径时，Host 配置和启动以其 README 为准，其余 Agent Space、MCP 与来源边界相同。

## 1. 安装并创建 Agent Space

`0.9.0-preview.19` 支持 macOS arm64/x64，以及带 systemd user service 的 Linux x64；需要 Node.js 20+ 和带 JSON1 的 SQLite CLI
3.35+（还需支持 `RETURNING`）。Linux 实机验证环境为 Ubuntu 22.04 LTS x64，Ubuntu 24.04 x64 当前只有 CI 证据；其他 Linux 环境先按
[Linux VPS runbook](operations/linux-vps.md) 执行 preflight 与本机 canary：

```sh
npm install --global wake-bridge
wakebridge release-preflight

wakebridge init \
  --data-dir "/absolute/private/path/wakebridge/my-agent" \
  --instance-id my-agent \
  --owner-id my-agent \
  --timezone Asia/Shanghai

wakebridge release-preflight \
  --config "/absolute/private/path/wakebridge/my-agent/wakebridge.config.json"
```

`init` 创建 mode-600 config、owner credential 与 SQLite DB，不在 stdout 打印 token，也不会覆盖已有实例。一个 preview instance 只服务一个 Agent Space。

## 2. 接入并启动 Host Adapter

为你的 adapter 准备一个 [Host Adapter manifest](operations/out-of-process-host-adapter.md#1-operator-配置)，其中：

- `adapter_kind` 必须与 adapter 创建 `HostSessionClient` 时完全一致；
- `attention_channels` 必须包含随后事件与预约使用的 channel；
- bootstrap token、每个 adapter process 的 route token、owner token 必须互不相同；
- capability 和 `receipt_upper_bound` 只写真实验证过的能力。

在含 bootstrap token 的受限环境中启动 daemon：

```sh
wakebridge doctor \
  --config "/absolute/private/path/wakebridge/my-agent/wakebridge.config.json" \
  --host-adapters "/absolute/private/path/host-adapters.json"

wakebridge daemon \
  --config "/absolute/private/path/wakebridge/my-agent/wakebridge.config.json" \
  --host 127.0.0.1 --port 4311 \
  --host-adapters "/absolute/private/path/host-adapters.json"
```

然后由 Host Adapter 使用 `wake-bridge/transport` 的 `HostSessionClient.open()` 登记：

- 精确、opaque 的 `session_ref`；
- 与事件相同的 `attention_channel`，下例使用 `life`；
- credential-free loopback `route_origin`；
- 独立的 per-process `route_token`。

正常运行时 adapter 续租同一个 lease；重建 session 或显式 takeover 才增加 generation；关闭时调用 `close()`。完整代码见 [out-of-process contract](operations/out-of-process-host-adapter.md) 与 [中立可运行示例](../examples/reference-host-adapter/README.md)。

## 3. 发送第一条受控 wake

使用不含敏感正文的 resource reference，并让 channel 与 Host Adapter 登记值完全一致：

```sh
wakebridge emit \
  --config "/absolute/private/path/wakebridge/my-agent/wakebridge.config.json" \
  --source manual \
  --type quickstart.test \
  --dedupe-key quickstart-first-wake-1 \
  --attention-channel-hint life \
  --resource "quickstart://first-wake"
```

正在运行的 daemon 会完成 tick 与 dispatch。检查结果：

```sh
wakebridge status \
  --config "/absolute/private/path/wakebridge/my-agent/wakebridge.config.json"

wakebridge inspect \
  --config "/absolute/private/path/wakebridge/my-agent/wakebridge.config.json"
```

成功证据分层如下：

- 没有 live binding 时，batch 保持 `waiting_for_endpoint`；这不是事件丢失；
- adapter 返回 HTTP 202 后，Core 最多记录 `transport_accepted`；
- adapter 自己必须证明 wake 已进入精确 session；
- agent 实际读到 wake 后调用 `attention_ack({"wake_batch_id":"wb_..."})`，再用自己的授权工具从 resource 读取权威来源，实际审阅/处理对应 claim 后才调用 `attention_consume({"claim_id":"ac_..."})`。Adapter 不得自动代 ack/consume。

验收至少要看到一次真实 agent ack、权威来源可回读，以及相应业务处理后的 consume。服务 `active`、live lease、stdout envelope、HTTP 202 或 host attestation 都不能代替这些证据。上述步骤是验收要求，不表示你的宿主已经通过真实 UAT。

同步宿主 API 若直到整轮 agent 回答完成才返回，不能直接当作接收 API。202 应在宿主真实接收后返回，不必等工作完成，也不能在 timeout 后伪造返回；优先使用宿主已有接收/排队能力，无需为此另建耐久队列。超时后是否已注入可能未知，重试可能重复，见 [接收与重试边界](operations/out-of-process-host-adapter.md#3-delivery-endpoint)。

如果 batch 一直等待，先检查事件、Host Adapter manifest 与 session registration 的 `attention_channel` 是否一致；Wake Bridge 不会猜测或跨 channel 错投。

## 4. 让 agent 自己使用 Wake Bridge

把下面的 stdio command 按你的 harness 格式配置为 MCP server：

```text
wakebridge mcp --config /absolute/private/path/wakebridge/my-agent/wakebridge.config.json
```

运行 daemon 时，再通过 `WAKEBRIDGE_DAEMON_URL=http://127.0.0.1:4311` 让 MCP 的 Source control 工具访问同一个 authenticated daemon。不要把 owner token、Host Adapter token 或 provider credential 放进 prompt。

agent 可以用 `attention_schedule` 为未来的自己建立 durable self-commitment。`eligible_after` 必须是带 `Z` 或数字 offset 的绝对 RFC3339 时间，`resource` 应指向未来醒来后可重新读取的权威位置；重复提交应复用稳定的 `idempotency_key`。若目标 channel 当前没有 live binding，调度仍会成功，但结果带
`warnings: [{"code":"no_current_binding", ...}]`；这是当前状态提示，不表示 endpoint 在到期前一定不会上线。self-commitment 的
`note` 会作为有界、owner-authored handoff 随 wake claim reference 返回；外部 source claim 不复制正文或任意 note。

可信 Host Adapter 观察到绑定 session 中真实用户消息被接受时，可以调用 `renewPresence()`。默认 policy 会在 presence lease 有效期内延后普通后台 claim，租约到期后自动重新进入调度；进程存活、窗口 focus、模型输出或 Wake 自己都不能冒充用户在场。

## 5. 可选：启用 Source Connector

先查看 catalog：

```sh
wakebridge connector-catalog
```

新配置应显式写 `"enabled": false`；loader 在字段省略时默认启用，并非所有 Connector 默认 disabled。已有实例的持久 enable/disable 控制优先于文件初值，先用 `attention_source_status` 查看当前 daemon 的有效状态，再按需 disable/enable，见 [Source 配置](operations/source-connectors.md#wake-bridge-配置)。以小机知道为例，完整顺序固定为：

```text
configure (disabled) → verify identity → bootstrap from-now → controlled enable + real UAT
```

由 operator 启动独立 Connector，并让 daemon 使用 `--connectors /absolute/path/sources.json`。随后 owning agent 依次调用：

1. `attention_source_status`，确认目标 source 已停用；必要时先 `attention_source_disable({"source":"botlingknows"})`
2. `attention_source_verify({"source":"botlingknows"})`
3. `attention_source_bootstrap`，传入 `source`，将 verify 的准确 `subject_ref` 与 `binding_fingerprint` 分别填入 `expected_subject_ref` 与 `expected_binding_fingerprint`
4. 在隔离实例受控启用单一 source，用一条无敏感通知完成真实 wake UAT
5. 生产接入准备完成后 `attention_source_enable({"source":"botlingknows"})`

不要跳过 from-now bootstrap，不要手改 SQLite，也不要把上游 MCP URL 或 credential 传给 Wake Bridge 工具。真实来源 UAT 应受控启用单一 source、产生一条专用通知并核对完整 receipts；完成后再决定是否保持启用。详见 [Connector catalog](operations/connector-catalog.md)、[Source Connector contract](operations/source-connectors.md)、[小机知道 Connector](adapters/botlingknows.md) 与 [Gmail Connector](adapters/gmail.md)。

## 6. 停止与保留数据

Host Adapter 先关闭当前 session lease，再停止 daemon。若安装了 LaunchAgent 或 systemd user service，按
[release lifecycle](operations/release-lifecycle.md) 返回的精确 stop/unload/uninstall 命令操作。

卸载 package 或 service 不删除 config、SQLite、backup 和日志。数据删除是另一个需要显式备份与确认的动作。
