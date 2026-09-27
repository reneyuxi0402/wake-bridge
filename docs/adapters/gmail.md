# Gmail Source Connector

状态：official optional，bundled，catalog 为 available，默认 disabled。实现、隔离 conformance 与真实账号 canary UAT
均已完成；每个 Agent Space 仍必须独立完成 configure → verify → from-now bootstrap → UAT 后才能显式 enable。
它使用 Gmail History API 增量发现进入 Inbox 的新邮件，
只向 Wake Bridge 提交稳定 message/thread reference，不复制 Subject、From、snippet、正文、附件或任意邮件 header。

## 已实现的边界

- Connector 在独立进程中保管 Google OAuth refresh token；Wake Bridge daemon 只持 connector-local bearer。
- 启动时调用 `users.getProfile` 得到当前账号与 history waterline，manifest 的 `subject_ref` 由真实账号生成，不能用环境变量冒充。
- `users.history.list` 固定使用 `historyTypes=messageAdded` 与 `labelId=INBOX`；读取不会 mark read、archive、加标签或发送邮件。
- 每条候选消息只调用一次 partial `users.messages.get(format=metadata)`，并用 `fields=id,threadId,labelIds,internalDate` 限制返回字段。
- Event 只保存 message id、thread id 与 `gmail://message/<id>` authoritative resource reference。
- Gmail opaque `nextPageToken` 与 durable `historyId` 分开保存；一页展开超过 Wake Bridge event limit 时还会保存 page 内 offset。
- History cursor 返回 404 时进入 `needs_attention`。Connector 不会静默跳到当前 waterline；owner 必须检查离线缺口并显式 rebind。
- Manifest 如实声明 credential 文件里记录的 OAuth scopes。只有纯 `gmail.metadata` / `gmail.readonly` 被标为 `read_only`；带 `gmail.modify`、settings、send 等 scope 的现有 credential 会标为 `broad`。

Google 文档说明 History ID 通常至少有效一周，但少数情况下可能只有数小时；404 要求客户端重新同步。Wake Bridge 不提供
隐式 full-sync，因为那会把“补齐历史”和“从现在重新开始”混为一谈。参考：
[users.history.list](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.history/list)、
[Gmail scopes](https://developers.google.com/workspace/gmail/api/auth/scopes)。

## OAuth credential 目录

Connector 当前复用 Google OAuth client 常见的两个 JSON 文件，不负责创建 Google Cloud project 或替 operator 扩大 scope：

```text
<private-directory>/credentials.json
<private-directory>/gcp-oauth.keys.json
```

`credentials.json` 需要 refresh token；`gcp-oauth.keys.json` 需要 `installed` 或 `web` client definition。两个文件必须是普通
mode-600 文件，不能是 symlink。推荐为 Wake Bridge 单独授权 `gmail.metadata`；也可复用已有 credential，但 connector 会在
manifest 中如实暴露其更宽的权限等级。不要把这两个文件复制到 Agent Space、git、Wake Bridge config 或模型上下文。

## 启动 connector

把以下内容放进 git 外的 mode-600 env 文件：

```sh
GMAIL_CREDENTIALS_DIR=/absolute/private/gmail-oauth
WAKEBRIDGE_CONNECTOR_TOKEN=<独立生成的至少32字符本地token>
GMAIL_ATTENTION_CHANNEL=life
```

加载环境后启动：

```sh
wakebridge gmail-connector --host 127.0.0.1 --port 4393
```

Connector 只接受 numeric loopback bind；credential 目录与 token 不接受命令行参数，避免进入 argv、shell history 或日志。

## Discover → configure → verify → bootstrap → UAT → enable

私有 `sources.json` 槽位示例：

```json
{
  "id": "gmail",
  "base_url": "http://127.0.0.1:4393",
  "token_env": "WAKEBRIDGE_SOURCE_GMAIL_TOKEN",
  "enabled": false,
  "poll_interval_ms": 60000,
  "max_backoff_ms": 900000,
  "limit": 50,
  "max_pages_per_cycle": 4
}
```

1. 保持 `enabled=false` 启动 connector 与 Wake Bridge daemon。
2. 调用 `attention_source_verify`，人工核对 `gmail:account:<address>` 与 binding fingerprint。
3. 对隔离 DB 显式执行 `from-now` bootstrap；它只保存 profile 当前 history id，不产生历史邮件 event。
4. 给该账号发送一封无敏感内容的 canary 邮件，运行 `source-once gmail`，确认 event 只有 message/thread id 与 resource reference。
5. 通过 agent 自己的 Gmail tool 回读 `gmail://message/<id>`，验收 event → claim → wake → ack/consume；确认邮件仍为未读且标签未改变。
6. 完成 UAT 后才显式 enable。OAuth 撤销、身份变化或 history 过期都应保持 `needs_attention`，不要用自动 rebind 掩盖缺口。

## 真实账号验证记录

2026-09-27 使用脱敏的真实 Gmail 账号完成 UAT：OAuth refresh、`users.getProfile`、从预发送 history waterline
开始的 `users.history.list`，以及字段受限的 `users.messages.get(format=metadata)` 均成功。账号给自己发送了一封只含测试标识的
canary；History API 找到它，adapter 只产生一条 `message.received`，metadata 只有 `message_id` 与 `thread_id`，没有
Subject、From、snippet、正文、附件或 payload preview。读取前后 canary 都保持 Inbox + unread，完整 label 集合没有变化。

同一条最小 event 随后进入临时隔离 Wake Bridge DB，完成 event → claim → mock host wake → `attention_ack` →
`attention_consume`；receipt 顺序为 `transport_accepted`、`agent_seen`、`agent_consumed`。测试没有接触 production Wake Bridge
DB 或服务。OAuth 文件只存在于测试期间创建的 mode-600 临时副本，临时凭据与隔离 DB 在退出时均已销毁。

## 与旧邮件哨兵的差异

旧式 `messages.list(q=is:unread)` + 本地 notified set 可以用于单机提醒，但它把“当前仍未读”当作增量日志：邮件在两次轮询之间
被其他客户端读过就会消失，state 与投递也要自行做两阶段协调。正式 Connector 以 Gmail History ID 为 durable cursor，event 先写入
Wake Bridge 再推进 checkpoint，并由 `(source, dedupe_key)` 吸收崩溃重放。
