# Source Connector 安装与本地协议

状态：current Source Connector contract。通用 runner 已通过隔离 fixture；是否可启用仍由 connector catalog、
identity verify、from-now bootstrap 与该来源的真实 UAT 分别决定。本文不证明任何 Host Adapter 的兼容性。

## 谁保管什么

```text
provider credential → Source Connector process
connector-local bearer → Wake Bridge daemon environment
event facts + resource refs + identity-fenced opaque cursor → Wake Bridge SQLite
```

外部 provider 不需要为 Wake Bridge 改 server、增加 scope 或签发专用 credential。Connector 是 owner 安装的适配进程，可以复用 provider 已有 OAuth、API key、MCP 登录、webhook 或本地 session。Wake Bridge 不获得这些上游 secret。

隔离不会把 broad credential 变成 read-only。Connector 被攻破时仍可能继承上游权限；能用 provider 原生最小 scope 时应优先使用。Manifest 只是如实声明和验收依据，不是进程沙箱。

## Connector protocol

Connector 监听一个独占 loopback origin，并用独立 bearer 验证每个请求：

```http
GET /v1/manifest
Authorization: Bearer <connector-local-token>
```

```json
{
  "id": "example-notifications",
  "version": "1.0.0",
  "subject_ref": "example:principal:owner-confirmed-id",
  "binding_fingerprint": "sha256:<64-lowercase-hex>",
  "read_side_effects": "none",
  "credential_custody": "connector",
  "upstream_credential_breadth": "broad",
  "upstream_credential_scopes": ["provider.account"],
  "connector_capabilities": ["notifications.read"]
}
```

```http
POST /v1/poll
Authorization: Bearer <connector-local-token>
Content-Type: application/json

{"cursor": null, "limit": 100}
```

可选的首次安装接口：

```http
POST /v1/bootstrap
Authorization: Bearer <connector-local-token>
Content-Type: application/json

{"mode": "from-now"}
```

它只返回当前水位 cursor，不返回或创建 event。Wake Bridge 只在 source 尚无 checkpoint 时接受一次 `from-now` bootstrap。Agent 必须把刚刚 verify 到的 `subject_ref` 与 `binding_fingerprint` 原样回传，Bridge 不替 agent 猜身份。

```json
{
  "events": [
    {
      "type": "mention",
      "occurred_at": "2026-08-29T05:00:00.000Z",
      "dedupe_key": "notification:123",
      "resource": {"uri": "example://notification/123", "cursor": 123},
      "metadata": {"notification_id": 123}
    }
  ],
  "next_cursor": {"high_watermark": 123},
  "has_more": false
}
```

Event 不得携带 provider credential、完整邮件/消息正文或任意模型指令。`resource.uri` 指回权威来源，agent 醒来后使用自己的授权工具读取。

## Wake Bridge 配置

配置文件不保存 token 值，只保存 token 环境变量名：

```json
{
  "version": 1,
  "sources": [
    {
      "id": "example-notifications",
      "base_url": "http://127.0.0.1:4391",
      "token_env": "WAKEBRIDGE_SOURCE_EXAMPLE_TOKEN",
      "enabled": true,
      "poll_interval_ms": 60000,
      "max_backoff_ms": 900000,
      "limit": 100,
      "max_pages_per_cycle": 4
    }
  ]
}
```

Daemon 用 `WAKEBRIDGE_SOURCE_CONNECTORS_FILE=/absolute/path/sources.json` 读取它。`base_url` 必须是无 path/query/credential 的 loopback HTTP origin。Connector token 只放在 daemon 的 mode-600 env 文件或等价 secret store，不放命令行、配置 JSON、数据库或日志。

安装者只负责提供已安装 connector 槽位和本地运行能力。上游 MCP URL、OAuth 或 API key 由 owning agent 使用 connector/provider 自己的配置机制填写和保管；Wake Bridge 不扫描宿主、shell 或浏览器配置，也不复制、修改这些值。

## 验证与运行

```bash
wakebridge source-validate --connectors /absolute/path/sources.json
wakebridge source-bootstrap example-notifications \
  --mode from-now \
  --connectors /absolute/path/sources.json \
  --expected-subject-ref example:principal:owner-confirmed-id \
  --expected-binding-fingerprint sha256:<64-lowercase-hex>
wakebridge source-once example-notifications \
  --connectors /absolute/path/sources.json \
  --db /private/tmp/wakebridge-source-probe.sqlite
wakebridge daemon \
  --config /absolute/path/to/agent-space/wakebridge.config.json \
  --connectors /absolute/path/sources.json
```

`source-once` 会真实调用 connector，并把标准化 event/checkpoint 写入指定 DB；它不是“只打印、不落库”的假 dry-run。首次验收应显式使用隔离 DB。状态不是 `healthy` 时命令以非零退出。

Agent 日常使用 MCP 自助完成：

1. `attention_source_status` 查看已安装槽位；
2. 启动自己配置好的 connector 后，调用 `attention_source_verify`；
3. 首次接入用 `attention_source_bootstrap`，已有错误/旧身份 checkpoint 用 `attention_source_rebind`；两者都必须回传 verify 结果中的准确身份；
4. 调用 `attention_source_enable` 开始轮询；需要停用时调用 `attention_source_disable`。enable/disable 会持久化并跨 daemon 重启保留。

Source control MCP 只接受 source id、非敏感身份确认、checkpoint revision 与 rebind reason，不接受任何 provider URL 或 credential。Opaque cursor 也不会经 status/verify 暴露给 agent。

对应的 authenticated loopback 管理 API：

```text
GET  /v1/sources
POST /v1/sources/:source_id/poll
POST /v1/sources/:source_id/verify
POST /v1/sources/:source_id/bootstrap
POST /v1/sources/:source_id/rebind
POST /v1/sources/:source_id/enable
POST /v1/sources/:source_id/disable
```

两者都走现有 admin token。Health 只公开 source、状态、时间、错误分类、连续失败数、下一次轮询、checkpoint revision 与上一轮计数，不回显 endpoint、token、manifest scopes 或 event body。

## 失败语义

- connector 暂时不可达、HTTP 5xx/429：`backoff`，指数退避到配置上限；
- 缺 connector token、401/403、manifest/checkpoint identity 不符、读取有副作用、schema/cursor/event 错误：`needs_attention`，停止自动轮询；
- 每次 poll 都核对 live `subject_ref` + `binding_fingerprint`；身份变化时禁止沿用旧 cursor，只能在 disabled 状态显式 rebind，并把旧 checkpoint 归档；
- page 全部 event durable 后才 CAS 推进 checkpoint；
- commit 前崩溃会重放同页，`(source, dedupe_key)` 吸收重复；
- `has_more` 每轮最多读取 `max_pages_per_cycle` 页，避免一个高流量 source 饿死其他来源。

## 首次生产接入 gate

1. 在隔离环境对真实 connector 做 manifest、auth、read-side-effect 与 cursor conformance。
2. 由 owner 明确选择 `from-now` 或受控 backfill；`from-now` 用显式 bootstrap 写入空 checkpoint，不手改 SQLite。
3. 用隔离 DB 先验证 bootstrap + `source-once`，确认旧通知不生成 event、event 不含正文/secret 且 resource 可回读；不要先指向 production DB。Production bootstrap 时 source 配置必须保持 `enabled=false`，完成后再受控启用，避免 poll 与空 checkpoint 竞争。
4. 备份 Wake Bridge DB、release、LaunchAgent/env；安装 connector 自己的 rollback。
5. 先启单一 source，以一条专用无敏感通知做 canary。
6. 验收 event → claim → wake → ack/consume receipts；不自动回复、不改 provider read state。
