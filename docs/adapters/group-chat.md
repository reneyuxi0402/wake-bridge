# Group Chat source adapter

状态：public SDK reference adapter 与 loopback HTTP connector；从 `0.9.0-preview.6` 起进入 package。

本文件属于 Wake Bridge。它描述第一个官方 reference adapter 如何消费独立 Group Chat 项目的公开 event feed；Group Chat 不导入或实现本仓库协议。

“第一个官方 reference adapter”只表示它在最初方案中用于验证真实的 mention/reply 垂直链路，不表示它拥有 core
特权，也不覆盖小机知道用户提问这一项目起源。第三方 source 必须能沿同一 SDK、contract test 与注册路径接入。

## 依赖方向

```text
Wake Bridge group-chat adapter
            │ consumes a narrow connector surface
            ▼
agent-controlled Group Chat connector
            │ existing principal auth
            ▼
Group Chat versioned event-feed contract
```

adapter 是 anti-corruption layer：验证 Group Chat schema，再转换成通用 WakeEvent input。实现通过
`wake-bridge/source/group-chat` 发布，只依赖 public source/event contract；Wake Bridge core 不认识 room/message 领域对象，也不持有 Group Chat principal credential。

下面列的是这个 reference source 已经具备、connector 可以利用的能力，不是 Wake Bridge 要求所有第三方服务新增的接口。若另一个服务只有 OAuth REST、MCP、webhook 或本地 session，应由它自己的 connector 适配现有能力。

## 所需 Group Chat 能力

- service consumer credential，绑定一个 member principal 与 room scopes；
- `read_events(after_event_cursor, limit, wait_ms, types?)`；
- stable event id、global event cursor、schema version；
- resource URI/message id/message cursor；
- 使用相同 principal credential 读取 resource 的 API 或 MCP。

adapter 不需要 Group Chat 管理员 credential。

## v1 映射

| Group Chat event | Wake source/type | coalesce key | 默认 channel hint |
| --- | --- | --- | --- |
| `room.mention.created` | `group_chat/mention` | `group:<room_id>` | `group:<room_id>` |
| `room.reply.created` | `group_chat/reply` | `group:<room_id>` | `group:<room_id>` |
| `room.message.created` | `group_chat/activity` | `group:<room_id>` | `group:<room_id>` |
| `room.message.tombstoned` | `group_chat/tombstone` | `group:<room_id>` | `group:<room_id>` |
| `room.membership.changed` | `group_chat/membership` | `group:<room_id>` | `default` |

映射只表达事件类别，不决定 immediate/scheduled/suppress。个人 policy 才拥有这个决定。

## WakeEvent input 示例

```json
{
  "schema_version": 1,
  "occurred_at": "2026-08-26T03:21:00Z",
  "type": "mention",
  "dedupe_key": "event:gev_01...",
  "coalesce_key": "group:room_home",
  "priority_hint": "high",
  "attention_channel_hint": "group:room_home",
  "actor_ref": "group-member:member-b",
  "resource": {
    "uri": "group-chat://room_home/messages/msg_01...",
    "cursor": "184"
  },
  "metadata": {
    "room_id": "room_home",
    "group_event_cursor": 955,
    "group_schema_version": 1,
    "direct": true
  },
  "payload_preview": null
}
```

`priority_hint=high` 只是 adapter 的保守提示，不能绕过个人 policy 或 quiet-hours rule。

## Cursor 与事务

```text
read group events after local committed cursor
  → validate every event schema/audience
  → map to SourceDeliveries
  → durable insert/dedupe all WakeEvents in the page
  → revision-CAS the opaque group event cursor
```

进程在 event commit 后、cursor CAS 前崩溃会重读同一页；stable ids 使其幂等。adapter 不向 Group Chat 写 consumer ack，也不推进任何 member last-seen cursor。

## 当前证据与集成责任

仓内 fixture 验证五类 event 的 v1 schema fence、mention/reply/activity mapping、resource reference、opaque cursor、
重复页拒绝，并确认 Core 没有 Group Chat 专用分支。Connector tests 另行验证 from-now bootstrap、credential-bound
identity、cursor catch-up、重复读取幂等和正文不进入 Wake Bridge ledger。

接入真实 Group Chat provider 时，维护方仍须验证 provider schema/version、principal scopes、credential revoke、
offline restart/catch-up、读取副作用和真实 Host Adapter 投递。这些现场能力不由 reference fixture 自动保证。

## 醒来后的读取

Wake payload 只携带 resource reference。agent 醒来后使用 Group Chat 自己的 MCP：

```text
group.get_context / group.get_message
  → understand authoritative room history
  → group.mark_seen(actual message cursor)
  → optional group.post_message
```

Wake Bridge 的 `agent_seen` 与 Group Chat 的 `member last_seen` 是不同事实。

## 失败语义

- Group Chat offline：adapter 记录 source unavailable，保留本地 cursor；其他 sources 继续。
- schema version unsupported：停止该 source 并 needs_attention，不丢弃/猜测字段。
- credential revoked：fail closed，不降级为匿名读取。
- event resource tombstoned：仍可消费 tombstone/audit metadata，不要求正文存在。
- Wake Bridge offline：Group Chat 正常运行，event log 等待稍后 catch up。

## 独立性验收

1. Group Chat 仓库不引用 Wake Bridge package、schema 或运行地址。
2. adapter 可以只靠公开 event-feed fixture 完成 contract tests。
3. 删除本 adapter，Wake Bridge 的 manual/timer/其他 sources 继续通过。
4. 停止 Wake Bridge，Group Chat UI/API/MCP/event feed 继续通过自身测试。
5. Wake Bridge core 不出现 Group Chat 专用 schema、数据库列或调度分支。
6. 一个只实现公开 source fixtures 的第二 adapter 能通过同一 conformance suite。
