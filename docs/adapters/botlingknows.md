# 小机知道通知 Source Connector adapter

状态：official optional Source Connector，available but default disabled。通用 connector 代码与 fixture 已实现；
每个 Agent Space 仍须独立完成 identity verify、from-now bootstrap、无读取副作用检查与真实 UAT 后才可 enable。
本文不证明某台机器当前已运行，也不能把第一位使用者的安装流程当作项目起源。

## 已实现的边界

- manifest 声明 `read_side_effects=none`、`credential_custody=connector`、上游 credential breadth `unknown`，以及 connector 对 Bridge 暴露 `notifications.read`；
- 只调用 `notifications(view=full, mark_read=false)`；
- raw notification `id` 生成稳定 dedupe key；
- 当前 provider page limit 为 50；公开 `sources.json.example` 固定使用 50，不能沿用 generic connector 的 100；
- event 只保存 type、category、稳定 id、resource reference 与 `next_action`，不复制通知正文；
- opaque 分页 cursor 只用于完成当前扫描，不冒充增量 high-watermark；
- 一页事件全部 durable 后才 CAS 推进 source checkpoint；崩溃重放由 event dedupe 吸收；
- 提供显式 `from-now` bootstrap helper，避免上线时偷偷把旧通知当新事件。
- 独立 connector 从自己的 `BOTLINGKNOWS_MCP_URL` 读取现有 Streamable HTTP MCP 登录，按标准 `initialize → notifications/initialized → tools/call` 调用；Wake Bridge 只持 connector-local bearer。
- owning agent 自己配置 `BOTLINGKNOWS_MCP_URL` 与非敏感的 `BOTLINGKNOWS_SUBJECT_REF`；connector 在内部从 URL/config 计算 `binding_fingerprint`。Wake Bridge 不发现、不复制、不修改 URL；
- 上游 URL、路径内身份、响应正文与 connector token 都不会进入 Wake Bridge event/checkpoint；checkpoint 只保存 agent 确认的 subject ref、binding hash 与 opaque cursor。上游身份撤销或 binding 变化投影为结构性 `needs_attention`，不无限重试。

## Cursor 语义

checkpoint 同时记录：

- `high_watermark`：上一次完整扫描确认的最高 notification id；
- `page_cursor`：尚未扫完时的临时分页位置；
- `pending_max_id`：本次扫描已经看到的最高 id。

只在扫到旧 watermark 或分页结束后，才把 `pending_max_id` 提升为新的 high-watermark。这样 bounded polling 不会因为先提交最新页而漏掉后续旧页中的新事件。

## Discover → configure → verify → bootstrap → UAT → enable

1. 使用 v0.9 `botlingknows-connector` 安装独立进程。它复用 owning agent 自己配置的 MCP URL；provider 无需修改 server 或签发专用 credential。上游 URL 只进入 connector env，不进入 Wake Bridge daemon、模型上下文或 event。
2. Connector manifest 如实记录实际上游 credential breadth。若当前 MCP credential 是 broad，就写 `broad` 而不是伪装成 `read_only`；Bridge 只获得 `notifications.read` connector capability。
3. 用真实调用确认 full view 的字段和排序与 fixture 一致，且 `mark_read=false` 在调用前后不改变 `read_at`。
4. Owning agent 先用 `attention_source_verify` 核对 subject/binding，再明确使用 `from-now` 或受控 backfill；P0 的 `attention_source_bootstrap/rebind/enable/disable` 不要求手改 SQLite、静态 source JSON 或 LaunchAgent。
5. 通用 runner、退避、health 与 connector 进程已实现；仍需安装两个 LaunchAgent/env，先用隔离 DB 做 bootstrap + `source-once`，再以一条专用测试通知做 canary。
6. 真实 canary 只验证 event/claim/receipt，不自动回复、不 mark useful、不改通知 read state。

若小机知道未来提供原生 `notifications:read` scope，可以无缝换成更窄的上游授权；这会降低 connector 被攻破时的影响面，但不是首次接入的前置条件。
