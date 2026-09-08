# 协议与数据模型

状态：`0.9.0-preview.8` current public contract。Event、Claim、policy、watch、binding、Batch 与 receipt 的
逻辑边界有效；首版 topology 是单 Agent Space instance。policy v1 当前面见第 5、11 节；service registry/enrollment 是
post-MVP proposal。

SQLite `PRAGMA user_version=8` 是当前 release schema。版本 6/7 只能经 owner 显式 `upgrade` 进入 8；任何普通 Core
打开路径都必须返回 upgrade-required，而不是产生隐式写入。升级前 snapshot、失败恢复与 offline restore 的 operator
contract 见 [release lifecycle](operations/release-lifecycle.md)。too-old、too-new 与 non-empty unversioned DB fail closed。

本文件描述一个 agent-owned attention scope 的逻辑 contract。外部项目可以直接调用通用 event ingress，或通过公开
Source Connector SDK 接入；本协议不定义邮件、群聊或小机知道自身的数据模型。Wake Bridge 只支持版本化 Host Adapter
Contract，不直接声明支持某个 agent 产品；具体产品实验不构成当前 support matrix。共享 service 多 Agent registry
不属于当前公共协议。

## 1. Instance、Agent Principal 与 Agent Space

```json
{
  "instance_id": "agent-a-local",
  "owner_id": "agent-a",
  "schema_version": 1,
  "timezone": "Asia/Shanghai"
}
```

`owner_id` 表示本实例唯一 Agent Principal，`instance_id` 表示它唯一 Agent Space 的持久作用域。两者在启动配置中
固定；普通 API payload 不能覆盖。每个实例只有一个 SQLite/credential boundary，多个 agent 使用不同实例。逻辑
contract 可将它们理解为 `agent_id/space_id`，但首版不为机械改名引入 schema migration，也不宣称已实现共享服务。

当前 endpoint contract：

- `endpoint.register(transport_profile_id, endpoint_ref)`：由 Host Adapter 为本实例登记具体 session/task；
- `endpoint.renew(endpoint_id, generation, ttl)`：使用窄 endpoint lease token 续租；
- `attention.takeover(channel, endpoint_id, expected_generation, reason)`：显式换窗并 fencing 旧 endpoint。

`endpoint_ref` 对 Core 不透明，只能引用实例已安装并验证的 transport profile，不能携带任意 executable、provider
credential 或 wake prompt。admin/source/host credential 都属于该实例，不能访问另一实例。

本协议只定义显式 `endpoint.register` 与 `attention.takeover`。由哪一种 runtime、frontend 或 harness
调用它们，怎样启动或恢复 session/credential/context，以及是否自动调用，均在 Wake Bridge contract 之外。没有有效
binding 时 delivery 保持 `waiting_for_endpoint`；Bridge 不从最近 activity、process 或窗口标题补造这些字段，也
不因 harness 漏做登记而启动 continuity loop。

未来 Agent Registry、enrollment、agent credential recovery 与共享 scheduler contract 不属于本协议的首版调用面。

## 2. Source Connector contract

### Direct event ingress

不需要 Bridge 代为轮询的 producer 使用 `POST /v1/ingress/:source/events` 直接提交。Bearer credential 在 daemon 配置中
固定绑定同一个 `:source`；owner-admin `POST /v1/events` 是管理面，不是普通 producer authority：

```json
{
  "schema_version": 1,
  "type": "job.completed",
  "occurred_at": "2026-08-27T08:00:00Z",
  "dedupe_key": "job:42:completed",
  "coalesce_key": "project:demo",
  "priority_hint": "normal",
  "attention_channel_hint": "default",
  "actor_ref": "worker:local",
  "resource": {
    "uri": "job://demo/42",
    "cursor": "7"
  },
  "metadata": {
    "status": "success"
  },
  "payload_preview": null
}
```

source identity 由 ingest credential 注入；agent/space 由实例配置固定。producer 不能提交或覆盖 agent、space、
policy、attention channel 最终值、endpoint、wake prompt、system instruction 或 receipt。请求成功表示 WakeEvent 已
可靠持久化；不表示已经匹配 policy、创建 batch 或唤醒 agent。

### Pull connector

需要 Bridge 维护 cursor 的 adapter 通过 `wake-bridge/source` 实现 exact v1 contract：

```ts
interface SourceAdapterManifest {
  contract_version: 1;
  id: string;
  version: string;
  // identity, credential custody, side-effect and capability declarations
}

interface PullSourceAdapter {
  manifest: SourceAdapterManifest;
  poll(input: { cursor: JsonValue | null; limit: number }): Promise<{
    events: WakeEventInput[];
    next_cursor: JsonValue | null;
    has_more?: boolean;
  }>;
}
```

Runner 只能在对应 WakeEvents 全部持久化后以 CAS 推进 checkpoint，并且不等待 policy、batch 或 dispatch。两步之间崩溃会重放
该页，由 `(source, dedupe_key)` 吸收，不能先推进 cursor。webhook/SSE adapter 也必须构造等价的单调 cursor 或稳定 delivery id。

### Source delivery

```json
{
  "delivery_id": "source-specific-id",
  "occurred_at": "2026-08-26T13:20:00Z",
  "type": "mention",
  "dedupe_key": "stable-source-key",
  "coalesce_key": "group:home",
  "priority_hint": "high",
  "attention_channel_hint": "group:home",
  "actor_ref": "source-specific-actor-ref",
  "resource": {
    "uri": "group-chat://home/messages/msg_01",
    "cursor": "184"
  },
  "metadata": {
    "direct": true
  },
  "payload_preview": null
}
```

adapter 自己负责验证来源 schema，再映射为 Wake Bridge 可接受的 delivery。Bridge core 不解析来源正文，也不依据自然语言猜优先级。第三方 adapter 只能调用公开 ingest contract，不能直接写 core tables。

## 3. WakeEvent

```json
{
  "id": "evt_01...",
  "agent_id": "agt_01...",
  "space_id": "spc_01...",
  "source": "group_chat",
  "type": "mention",
  "occurred_at": "2026-08-26T13:20:00Z",
  "received_at": "2026-08-26T13:20:01Z",
  "dedupe_key": "stable-source-key",
  "coalesce_key": "group:home",
  "priority_hint": "high",
  "attention_channel_hint": "group:home",
  "actor_ref": "source-specific-actor-ref",
  "resource": {
    "uri": "group-chat://home/messages/msg_01",
    "cursor": "184"
  },
  "metadata": {
    "direct": true
  },
  "payload_preview": null
}
```

不变量：

- agent/space 由 service 认证层注入；
- `(source, dedupe_key)` 在 Agent Space 内唯一；
- priority/channel hint 只是建议，最终由 policy 决定；
- resource 指向权威来源；
- preview 默认空，只能保存明确允许的短摘要；
- 完整正文、credential 和模型私有上下文不得进入 event。
- event 不包含 producer 提供的任意 prompt/system instruction；transport envelope 由 Bridge 固定模板生成。

### Event lifecycle

```text
received → matched → open_batch → batched → consumed
                    ├──────────→ suppressed
                    └──────────→ expired
```

`suppressed` 表示不主动叫醒，不等于事件不存在。`consumed` 需要 agent 明确确认已查看、处理或忽略来源事件。

## 4. Source subscription

```json
{
  "id": "src_group_chat",
  "agent_id": "agt_01...",
  "space_id": "spc_01...",
  "adapter_kind": "group_chat",
  "enabled": true,
  "cursor": "183",
  "connector_ref": "unix:///run/user/501/wake-source-group-chat.sock",
  "subject_ref": "group-chat:principal:agent-confirmed-id",
  "binding_fingerprint": "sha256:<64-lowercase-hex>",
  "credential_custody": "connector",
  "upstream_credential_breadth": "broad",
  "upstream_credential_scopes": ["account.default"],
  "connector_capabilities": ["notifications.read"],
  "read_side_effects": "none",
  "poll_interval_ms": 2000,
  "last_success_at": "2026-08-26T13:19:30Z",
  "last_error_class": null
}
```

`connector_ref` 指向 Agent Space 中安装的 Source Connector，而不是 provider secret。`subject_ref` 是 agent 明确确认的非敏感 principal 标签；`binding_fingerprint` 是 connector 内部由当前上游 endpoint/config 计算的 SHA-256 fence。Provider credential 由 connector 自己保管；外部服务无需为了 Wake Bridge 增加专用接口或 scope。`credential_custody`、上游 credential 的真实 breadth/scopes 与 connector 暴露能力必须分开记录：一个使用 broad token 的 connector 可以只暴露 `notifications.read`，但不能把上游 token 谎报成 read-only。`read_side_effects` 必须是 `none | marks_read | consumes | unknown` 之一，并由 adapter conformance test 验证，不能只靠作者声明。

## 5. Policy

```json
{
  "id": "direct-mention",
  "version": 3,
  "enabled": true,
  "order": 100,
  "match": {
    "source": "group_chat",
    "type": "mention",
    "metadata.direct": true
  },
  "delivery": {
    "mode": "immediate",
    "quiet_hours_policy": "bypass",
    "foreground_presence_policy": "defer"
  },
  "batch": {
    "coalesce_by": "coalesce_key",
    "max_events": 20
  },
  "target": {
    "attention_channel": "${attention_channel_hint}"
  }
}
```

当前 public policy v1 delivery modes：`immediate | scheduled | suppress`。`debounce | digest` 和 inert target/delay 字段
已由 WB-MVP-003 从当前 install/type/config contract 移除；未来增加需要新的 schema version。

规则按 order 由高到低 first-match-wins。文件必须含显式 default policy。`(id, version)` 不可变，每个 id 只有最高版本
参与新事件匹配；已匹配 Event/Claim pin 原 version，当前没有 recompute 操作。

### Quiet hours

```json
{
  "timezone": "Asia/Shanghai",
  "windows": [{ "start": "22:00", "end": "08:00" }]
}
```

只有具体 policy 明写 `bypass` 才能穿透。窗口必须使用 `HH:MM`，组合后至少留下一个开放分钟；全日 quiet 配置会在
启动时 fail closed。release 落在精确分钟边界。

## 6. Attention Claim

Policy 不直接把 event 变成 wake batch，而是创建一条 durable Attention Claim。Claim 表示“这个 resource 从某个时刻起值得重新获得一次注意力”，不表示任务、用户指令或外部副作用授权。

```json
{
  "id": "ac_01...",
  "agent_id": "agt_01...",
  "space_id": "spc_01...",
  "origin": "policy",
  "event_ids": ["evt_01..."],
  "resource": {
    "uri": "group-chat://home/messages/msg_01...",
    "cursor": "184"
  },
  "policy_id": "direct-mention",
  "policy_version": 3,
  "attention_channel": "group:home",
  "eligible_after": "2026-08-26T13:20:01Z",
  "expires_at": null,
  "defer_while_presence": true,
  "state": "pending",
  "reason_code": "direct_mention",
  "created_at": "2026-08-26T13:20:01Z"
}
```

Claim lifecycle：

```text
pending → deferred → eligible → batched → consumed
                         ├──────────────→ snoozed → pending
                         ├──────────────→ dismissed
                         └──────────────→ expired
```

- 普通 source ingest capability 只能 emit WakeEvent，不能直接创建或续期 Claim；
- policy 可以从 event 创建 Claim 或 suppress；
- agent capability 的 `claim.schedule(...)` 必须在自己的 space 内同一事务写入一条 `self_commitment` event 与对应 Claim；
- snooze/dismiss/consume 不修改来源正文或来源 read state；
- agent 可能已经从其他路径处理了 resource，Claim 仍保持 pending 是正常的跨系统陈旧状态；消费前必须 revalidate，已处理则廉价 dismiss。

## 7. Foreground Presence Lease

Foreground Presence Lease 表示当前 attention channel 的绑定 session 最近观察到真实用户活动。它不是 agent credential 或 endpoint lease，也不是 quiet hours。

```json
{
  "agent_id": "agt_02...",
  "space_id": "spc_02...",
  "attention_channel": "life",
  "endpoint_id": "ep_02...",
  "binding_generation": 12,
  "renewed_at": "2026-08-28T07:30:00Z",
  "expires_at": "2026-08-28T07:50:00Z",
  "observed_by": "host_adapter",
  "observation": "user_message_accepted"
}
```

不变量：

- 唯一键为 `(space_id, attention_channel)`；lease 必须同时匹配当前 endpoint 与 generation；
- 只依赖 active edge：可信 host 每次接受真实用户消息时滑动续 TTL；没有 idle event，过期即解除；
- 普通 source ingest capability 不能续租；只有绑定 endpoint 的可信 host capability 或管理员可更新；
- `user.active` 可以作为可审计 WakeEvent 进入通用 ingress，但 event 本身不能直接改 lease，必须由允许该 host/source 的 policy action 校验 channel、endpoint 与 generation 后续租；
- `defer_while_presence=true` 的 Claim 在 lease 有效期内保持 deferred；显式 bypass 的 Claim 与其他 channel 不受影响；
- takeover 后旧 generation 的 presence lease 与 waiter 立即失效。

## 8. Explicit Inactivity Watch

Agent 可以在自己的 space 中显式登记一个 channel-scoped inactivity condition：

```json
{
  "space_id": "spc_02...",
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

不变量：

- 配置唯一键为 `(space_id, attention_channel)`；activity observation 必须匹配当前 binding 的
  `endpoint_id + binding_generation`；
- `once` 在本次 inactive epoch 触发一次；`repeat` 在 condition 持续成立时按显式 interval 产生后续 sequence；
- 每个 sequence 事务性创建一条 `wakebridge.core/channel.inactive` Event 与一条
  `origin=inactivity_watch` Claim，并走普通 policy gates、batch 与 dispatch；
- 没有 binding 时为 `waiting_for_endpoint`；Host Adapter 不具备 `session_activity_observable` capability 时为
  `unsupported`，不得从窗口 focus、进程存在、模型输出、source event 或单纯沉默补猜；
- takeover fence 旧 generation；watch 等待新 binding 的可信 activity baseline，不把旧 endpoint 的 idle time
  继承给新 session；
- 真实 activity 关闭当前 epoch并重新计时。Wake start/settled、Claim ack/consume/dismiss 与 delivery retry 不能
  续 activity lease；
- watch activity 可以是可信 host 观察到的用户交互或 `cause=independent` 的 session turn；Foreground Presence
  仍只接受真实用户 active edge，二者不是同一 lease；
- repeat 受公开最小 interval、per-space fairness 与 service rate limit 约束，但不以 self-commitment coverage、
  agent 回复或“是否记住”为停止条件。

完整 contract 与幂等键见 [Explicit Inactivity Watch](./inactivity-watch.md)。

## 9. Wake batch

```json
{
  "id": "wb_01...",
  "agent_id": "agt_01...",
  "space_id": "spc_01...",
  "policy_id": "direct-mention",
  "policy_version": 3,
  "attention_channel": "group:home",
  "claim_ids": ["ac_01..."],
  "event_ids": ["evt_01..."],
  "state": "pending",
  "not_before": "2026-08-26T13:20:01Z",
  "deadline": "2026-08-26T13:20:31Z",
  "attempt": 0,
  "created_at": "2026-08-26T13:20:01Z"
}
```

open batch 在窗口关闭时冻结。冻结后 claim_ids/event_ids 不再变化；迟到 Claim 进入下一 batch。

## 10. Endpoint 与 binding

### Endpoint lease

```json
{
  "id": "ep_01...",
  "agent_id": "agt_01...",
  "space_id": "spc_01...",
  "transport_profile_id": "local-host-adapter",
  "endpoint_ref": "opaque:host-session:01a03b5e-...",
  "lease_token_hash": "...",
  "lease_expires_at": "2026-08-26T14:00:00Z",
  "capabilities": ["exact_live_route", "host_ordered_busy_queue", "hook_activity_observer"],
  "route": {
    "kind": "local_http",
    "ref": "opaque:live-channel:01a03b5e-..."
  },
  "support_tier": "experimental"
}
```

### Channel binding

```json
{
  "agent_id": "agt_01...",
  "space_id": "spc_01...",
  "attention_channel": "group:home",
  "endpoint_id": "ep_01...",
  "generation": 7,
  "bound_at": "2026-08-26T13:00:00Z"
}
```

唯一键：`(space_id, attention_channel)`。只有显式 policy/config 才能 fallback 到 `default`。

### Active waiter

```json
{
  "id": "wait_01...",
  "endpoint_id": "ep_01...",
  "attention_channels": ["group:home", "email", "default"],
  "binding_generations": {
    "group:home": 7,
    "email": 2,
    "default": 5
  },
  "state": "waiting",
  "opened_at": "2026-08-26T13:15:00Z",
  "expires_at": "2026-08-26T13:45:00Z"
}
```

每 endpoint 最多一个 active waiter。waiter 只能领取声明 channel 且 generation 仍匹配的 batch。

waiter 是未选择的 alternate experiment，不属于当前公共 Host Adapter Contract。没有 adapter 已验证的 route 时，
batch 等待 endpoint，不依赖未经证明的 cold route。

## 11. 逻辑操作

### source / event / policy

- `source.register(config)`：管理员创建 source subscription；
- `source.sync(source_id)`：手动触发一次增量读取；
- `event.emit(delivery)`：已认证 producer 通过通用 contract 提交事件；
- `event.list(state?, source?, after?)`：读取当前 Agent Space 的事件索引；
- `policy.list/install/test/preview(...)`：owner-only；JSON v1 当前只接受 `immediate | scheduled | suppress`，test/preview 不执行写入；
- `(policy_id, version)` append-only；更新必须升 version，Claim 保持匹配时冻结的 version；
- `debounce/digest/max_delay/resume_spread/transport preference/busy behavior` 已从当前 contract 移除；历史 DB row 只读保留且不参与新事件匹配。

### claim / presence

- `claim.schedule(resource, eligible_after, reason_code, note?)`：agent-only；在当前 space 事务性创建 self-commitment event + Claim；
- `claim.list(state?, channel?, source?)`；
- `claim.snooze(claim_id, until)`；
- `claim.dismiss(claim_id, reason?)`；
- `claim.consume(claim_id, result?)`；
- `presence.renew(channel, endpoint, generation, ttl, observation)`：可信 host-only；
- `presence.inspect(channel?)`：只读解释当前 lease 与到期时间；
- `activity-watch.configure(channel, enabled, idle_after, mode, repeat_after?)`：agent-only；显式配置 watch；
- `activity-watch.inspect(channel?)`：解释 binding、generation、last activity、epoch、sequence 与 next due；
- `activity.observe(channel, endpoint, generation, observation_id, ttl, kind)`：可信 host-only；

### session

- `endpoint.register(transport_profile_id, endpoint_ref)`；
- `attention.takeover(channel, endpoint_id, expected_generation, reason)`；
- `endpoint.renew(endpoint_id, generation, ttl)`；
- `wake.ack_batch(batch_id, endpoint, generation)`；
- `wake.consume_events(batch_id, event_ids, results?)`。

`wake.wait_for_wake` 是未选择的实验方向，不属于当前 MCP 或发布协议。

注册只解决“投给哪个 session”，不能让已经结束的 turn 自行复活，也不保证 agent 或 harness 在换窗后记得重新
注册。后者不属于 Wake Bridge 的失败语义。

## 12. Scheduler contract

```text
read source delivery
normalize and insert event transactionally
dedupe by (source, dedupe_key)
commit source cursor / return durable-ingest receipt

independent scheduler reads committed event
evaluate explicit policy version

if suppress: mark suppressed
otherwise: create durable Attention Claim

when claim eligible_after arrives:
  apply quiet-hours defer/bypass
  apply foreground-presence defer/bypass
  add eligible claim to open batch keyed by policy + channel + coalesce key + time bucket

when window/cadence/schedule is due:
  freeze batch
  enqueue durable dispatch

when a registered inactivity condition is due:
  CAS current binding generation + epoch + sequence
  create one internal channel.inactive event + inactivity_watch claim
  for repeat mode, schedule the next sequence while the condition remains true
```

若 adapter 需要逐页 commit，cursor 推进和该页全部 event inserts 必须同一事务或可重放。quiet hours、无 endpoint、policy 错误与 transport 故障不能阻塞来源 cursor；它们由 committed event、batch 与 outbox 状态独立承接。

v0.9 pull adapter 的公开最小 surface：

```ts
interface SourceAdapterManifest {
  contract_version: 1;
  id: string;
  version: string;
  subject_ref: string;
  binding_fingerprint: `sha256:${string}`;
  read_side_effects: "none" | "marks_read" | "consumes" | "unknown";
  credential_custody: "none" | "connector" | "wake_bridge_process";
  upstream_credential_breadth: "none" | "read_only" | "broad" | "unknown";
  upstream_credential_scopes: string[];
  connector_capabilities: string[];
}

interface PullSourceAdapter {
  manifest: SourceAdapterManifest;
  poll(input: { cursor: JsonValue | null; limit: number }): Promise<{
    events: WakeEventInput[];
    next_cursor: JsonValue | null;
    has_more?: boolean;
  }>;
}
```

runner 在 poll 前拒绝未知/有副作用读取。Credential gate 只针对实际 custody：broad/unknown credential 若直接注入 Wake Bridge 进程则默认拒绝；若 credential 留在独立 connector 中，runner 允许 provider 原有授权，并按 manifest 暴露的窄 connector capability 工作。每页所有 event 都 durable 后，以 revision CAS 提交不超过 4 KiB 的 opaque cursor。cursor CAS 与 event insert 不需要共享一个 SQLite transaction，因为 cursor 前的 crash 会重放同页并由 `(source, dedupe_key)` 吸收；但绝不允许先推进 cursor 再补写 event。

首个可运行 connector transport 是仅限 loopback 的 HTTP origin：

```text
GET  /v1/manifest
POST /v1/poll  {"cursor": <opaque JSON|null>, "limit": 1..1000}
POST /v1/bootstrap {"mode": "from-now"}  # optional, explicit first-install operation
```

三条请求都使用 connector 自己签发的 bearer；它不是 provider credential。配置文件只登记 source id、loopback origin、承载 bearer 的环境变量名、轮询间隔、page limit 与每轮最多页数。Bridge 不接受公网 connector URL，不保存 bearer，不把 credential 写入 event/checkpoint。Checkpoint 会保存 manifest 的 `subject_ref` 与 `binding_fingerprint`，但不会向 agent 暴露 opaque cursor。

Supervisor 对 `has_more` 做有界续页；到达 page budget 后让出 event loop，再立即安排下一轮。每次 poll 前必须核对 live manifest 与 checkpoint 的 subject/binding；不匹配时 fail closed。连接失败和 provider/connector 5xx 进入指数退避；缺 token、401/403、identity/manifest/schema/cursor 错误进入 `needs_attention`，不永久盲重试。`GET /v1/sources` 返回不含 secret 的状态，管理员可用 `POST /v1/sources/:id/poll` 做一次受控同步。

`source-bootstrap SOURCE --mode from-now` 只允许在该 source 尚无 checkpoint 时执行，并要求调用者显式传回 verify 得到的 subject/binding。它调用 connector 的可选 bootstrap endpoint，把返回 cursor 以 revision 1 写入，不创建 event/claim/wake；第二次调用 fail closed。Bootstrap 仍必须满足 `read_side_effects=none`。Agent-facing MCP 另提供 status/verify/bootstrap/rebind/enable/disable；rebind 会归档旧 checkpoint，enable intent 会持久化。所有 source control tool 都不接受 provider URL/credential。

## 13. Transport capability 与 dispatch contract

每个 transport adapter 提供版本化 capability：

```json
{
  "adapter_kind": "example_host",
  "adapter_version": "1.0.0",
  "host_kinds": ["example_host"],
  "capabilities": {
    "cold_push": false,
    "warm_resume": true,
    "requires_live_binding": true,
    "queue_when_busy": true,
    "steer_when_busy": false,
    "foreground_presence_observable": true,
    "session_activity_observable": true,
    "waiter_turn_settle_compatible": false,
    "receipt_stages": ["transport_accepted"],
    "receipt_upper_bound": "accepted_to_live_pipe",
    "support_tier": "experimental"
  },
  "tested_host_versions": ["example-host/1.0.0"]
}
```

capability 是 adapter 对特定宿主与版本的声明；`doctor` 可以降级或标记 unknown，不能因另一个宿主成功而自动推断。transport 收到的是 Bridge 生成的固定 wake envelope 与 event references，不是来源正文。

### single-route dispatch

```text
resolve binding(space, attention_channel)
assert endpoint lease and generation are current

if matching Host Adapter route exists
and its lease + binding generation are current:
  send one fixed wake envelope with an unguessable delivery nonce
  record transport_accepted only after the adapter's declared acceptance edge
else:
  keep the batch waiting_for_endpoint
```

Core 不把 cold path、busy queue 或 turn-start 行为归因于产品名。写出失败时重新解析 binding/generation，再决定重试；
换窗后的重新登记由 Host Adapter/harness 负责。

### Out-of-process local Host Adapter v1

已有 backend agent/bridge/frontend 的宿主可以通过 public `HostSessionClient` 调用 scoped
`/v1/host-sessions/open|renew|close|activity`。Open 固定 adapter kind、credential-scoped host kind/channel，并登记 credential-free
loopback origin 与独立 route token；bootstrap token 与 route token 不得复用。

Core dispatch 使用 `POST /v1/wakes`，body 为 `protocol_version=1 + attempt_id + delivery_nonce + WakePayload`。只有 HTTP 202
表示 adapter 声明的 pipe/host accepted；408/425/429/5xx 为 retryable，其余为 permanent。Response body 不被读取或写入 durable
state。该 transport 只创建 `transport_accepted`；wake echo 只建立可信 activity correlation，不冒充 agent seen/completed。

协议只接受 loopback HTTP、拒绝 redirect 与 URL credential/path/query。它不加载第三方代码，不发现 session，不访问 runtime DB，
也不把某个实现协议的 runtime 自动列为 supported host。

## 14. Receipt 与状态机

receipt stages：

- `transport_accepted`；
- `agent_completed`；
- `agent_seen`；
- `agent_consumed`；
- `agent_acted`。

它们是追加事实，不能互相冒充。

```text
batch pending
  ├─ quiet hours ─────────→ deferred → pending
  ├─ foreground presence ─→ deferred → pending
  ├─ no endpoint ─────────→ waiting_for_endpoint
  ├─ optional warm path ──→ waiting_for_waiter
  ├─ worker lease ────────→ dispatching
  ├─ all claims finalized → cancelled
  └─ invalid policy/route → needs_attention

dispatching
  ├─ accepted ────────────→ dispatched
  ├─ transient error ─────→ retry_wait → pending
  └─ permanent error ─────→ dead_letter

dispatched
  ├─ agent ack ───────────→ seen
  └─ ack timeout ─────────→ retry_wait or needs_attention
```

状态更新记录 attempt id；迟到 worker 不能覆盖新状态。

`cancelled` 是未投递 batch 的可审计终态：其中最后一个 claim 在其他路径被 consume/dismiss/expire 后，batch 不再有工作可做。
这不是 operator incident，不计入 `needs_attention`。

### Operator retry

`dead_letter` 不会自动回到 dispatch queue。Operator 修复 permanent rejection 的根因后，可以用当前 `attempt` 作为 CAS fence，
把一个仍持有全部 `batched` claims 的 batch 显式改为 `retry_wait`。该 mutation 只记录 audit transition，不在同一调用中 dispatch；
下一次 canonical scheduler pass 才创建 attempt N+1。已 consume/dismiss/finalize 的 claim 不会被恢复，source/host/agent MCP
credential 都没有这项 authority。

统一 operator status 是 secret-free read model：允许 batch/error class、channel/endpoint generation 与 source health；禁止 route
address、session ref、provider error message、credential 与正文。Offline status 不恢复 dispatcher lease，live status 只能由 owner API 读取。

## 15. 隐私与可观测性

允许记录：source/type、event/claim/batch id、policy/version、channel、endpoint/generation、presence lease 时间和错误分类。

默认不记录：来源正文、credential、lease token、模型私有上下文、完整 transport prompt。

两个 Agent Space 不能使用同一个 agent credential、数据库文件或 connector secret namespace。

通用 source ingest capability 只能在自己的 source namespace 内 emit/read ingest receipt；不能更新 claim、presence、policy、binding、endpoint 或 transport config。Provider credential 不属于 Wake Bridge capability，应留在 Source Connector。adapter/transport package 必须通过公开 SDK 与 conformance fixtures 接入，不能依赖 core 私有表结构。

Source Connector manifest（SDK 为兼容现有 v1 仍使用类型名 `SourceAdapterManifest`）必须分别声明 credential custody、
上游 credential 的实际 breadth/scopes、connector 暴露 capabilities 与
`read_side_effects=none | marks_read | consumes | unknown`。`unknown` 或无法禁用的读取副作用默认不能用于常驻轮询；
上游缺少专用只读 credential 不再阻塞接入，只要 credential 留在 agent-controlled connector、Bridge 只获得窄接口，
并由 agent 对 broad/unknown 上游权限显式知情。
