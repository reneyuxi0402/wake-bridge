# 架构与路由

状态：`0.9.0-preview.8` current public contract。首版 deployment topology 为单 Agent Space
instance；共享 service 多 Space 已延后为 post-MVP proposal。

## 1. 系统边界

Wake Bridge 是一个本地优先的 agent attention scheduler and delivery core。每个 agent-owned scope 接收事件、维护注意力请求、
决定节奏，并只向已显式登记的 Host Adapter route 投递。首版一个 daemon instance 只承载一个 Agent Principal / Agent Space。
Wake Bridge 支持 Host Adapter Contract，不直接声明支持某个 agent 产品。

```text
┌─────────────────────────────────────────────────────┐
│ External sources                                    │
│ generic emit / webhook / email / group-chat / etc. │
└──────────────────────┬──────────────────────────────┘
                       │ source-specific cursor/event
                       ▼
┌─────────────────────────────────────────────────────┐
│ Source Connectors                                   │
│ authenticate → fetch/subscribe → normalize → dedupe │
└──────────────────────┬──────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────┐
│ Wake Bridge single-space instance                   │
│ Agent Space: Event → Policy → Claim → Outbox        │
│ Host Adapter Registry                               │
└──────────────────────┬──────────────────────────────┘
                       ▼
       selected agent endpoint + one configured Host Adapter
                       ▼
          host-owned exact agent session/task
                       ▼
       agent reads authoritative source via its MCP/API
```

外部来源和 Wake Bridge 各自独立成立。来源不需要知道 agent 的 quiet hours、session 地址或 transport；Wake Bridge 不拥有来源正文和业务状态。公开产品的核心边界是：source 只能在获授权的 Agent Space 中登记事实，不能直接获得向 session 注入任意 prompt 的权力。

## 2. Single-space instance 与 Agent Space

首版选择 one Agent Principal → one Agent Space → one daemon instance。实例启动时固定：

```yaml
instance_id: agent-a-local
owner_id: agent-a
db: /path/to/agent-a/wake-bridge.sqlite
listen: 127.0.0.1:4311
default_timezone: Asia/Shanghai
```

该实例的 SQLite、data directory、listen address、admin/source/host credential、policy、connector、endpoint registry
与 audit 都属于唯一 Agent Space。`instance_id/owner_id` 是当前 schema/config 对这个固定作用域的实现名称；普通请求
不能覆盖它们。Agent Space 是逻辑安全域，不意味着首版必须有 service-level registry。

同一机器运行多个 agent 时，operator 为每个 agent 启动独立 daemon、端口、DB 与 credential。它们可以共享安装的
Wake Bridge package 和 Host Adapter 实现，但不共享 route token、scheduler、connector secret
或故障域。一个实例失败或升级不停止另一个实例。

当前实现已经具备公开 Host Adapter registry/lifecycle、显式 route、scoped host bootstrap、session-side
register/renew/revoke、generation fencing、durable nonce correlation 与 out-of-process loopback protocol。
具体产品 profile 的历史实验不构成当前 support matrix，也不进入默认 onboarding。

### Deferred shared-service topology

共享 service 多 Agent Space 保留为 post-MVP 方向。若未来实现，应在现有单 Space Core/DB 外增加独立的
supervisor/registry，每个 Space 仍使用独立 SQLite 与 secret namespace；它必须独立证明认证路由、
enrollment/recovery authority、调度公平性、逐 Space migration 和 shared-daemon blast radius。首版不实现
service registry、共享 worker fairness 或跨 Space migration coordinator。

## 3. 组件职责

### Source Connector

简单 producer 可以通过 CLI/MCP owner surface，或 credential-bound `POST /v1/ingress/:source/events` 调用通用 event
ingress。source credential 只允许固定 source 写 event，不获得 inspect、policy、claim、endpoint 或 dispatch authority。
需要 Bridge 代为维护 cursor、poll、SSE 或 webhook 生命周期的来源才实现 Source Connector。

每种 pull source 由 Agent Space 内的 agent-controlled Source Connector 提供增量读取面。外部 provider 不需要知道 Wake Bridge，也不需要为它增加 endpoint、scope 或专用 credential。Connector 可以使用 provider 已有的 OAuth、API key、MCP 登录、webhook 或本地 session；Wake Bridge 只调用 connector 暴露的窄接口。

每种 pull connector 只做：

1. 通过 Source Connector 读取来源；provider 原生最小 scope 可用时优先使用，但不是通用前置条件；
2. 通过 webhook、SSE、long poll 或 cursor poll 取得增量事件；
3. 转成标准 WakeEvent；
4. 本地事务提交后才推进来源 cursor；
5. 对重复 delivery 产生相同 dedupe key。
6. 分别声明读取副作用、credential custody、上游 credential 的真实 breadth/scopes，以及 connector 实际暴露的 capabilities。

当前 source 证据：

- credential-bound generic push ingress 与 `wake-bridge/event` client；
- versioned `wake-bridge/source` pull contract、loopback connector、cursor CAS 与 supervisor；
- `wake-bridge/source/group-chat` reference mapper/conformance fixture 与 loopback HTTP connector；
- botlingknows read-only connector implementation，作为额外独立来源证据，不取代 Group Chat 的公开 canary 位置。

小机知道是首发 official optional connector；Gmail 保持 planned，完成独立 provider/compliance/UAT 提案前不可 enable。
Group Chat 只作为 reference/conformance source。更多 provider connector 不应在 SDK 边界之外提前扩张。

Connector 不决定是否唤醒，也不把完整正文复制进 Bridge。标准事件保存 resource reference、必要 metadata 与可选的短 preview。Source Connector 取得的 ingest capability 只能使用自己的 namespace/dedupe domain，不能更新 policy、claim、presence、binding 或 endpoint。

每个 connector 必须在 capability manifest 中声明 `read_side_effects=none | marks_read | consumes | unknown`。`unknown` 或不可关闭的 `marks_read/consumes` 默认禁止作为常驻轮询 source。credential 安全与读取副作用是两条不同轴：上游 token 即使是 broad，也可以留在独立 connector 中，由 connector 只暴露 `notifications.read` 等窄操作；manifest 必须如实写 `upstream_credential_breadth=broad|unknown`，不能伪装成 read-only。只有 broad/unknown credential 被直接注入 Wake Bridge 进程时才默认 fail closed，agent 可用的 provider 原生最小 scope 是增强项而非依赖项。

```text
provider
  ↕ existing auth / webhook
Source Connector  ← credential custody boundary
  ↓ bounded poll/emit + resource refs
Wake Bridge daemon ← no provider credential
```

### Public Event Ingress

所有来源最终进入同一个版本化的 ingest service：

```text
authenticated source
  → validate schema/size/resource URI
  → inject source + agent-space identity
  → dedupe and persist WakeEvent
  → commit source cursor/delivery receipt
```

ingest 成功只表示事件已可靠保存，不等待 policy、batch 或 transport。scheduler 从 Event Store 独立消费；因此 quiet hours、无 endpoint 或 transport 故障都不能反压来源 cursor。

### Event Store

- append-only；
- `(source, dedupe_key)` 唯一；
- 记录 received、matched、batched、suppressed、consumed、expired；
- Bridge 重启后未消费事件可重新调度；
- 来源暂时不可用时保留最后确认 cursor，不把“拉取失败”写成“没有事件”。

### Rule Engine

规则按 order 确定性匹配。首版不让 LLM 在热路径自由分类。

policy 把 event 转成 durable Attention Claim，也可以 suppress。它可以声明：

- source/type/actor/channel/metadata selector；
- 当前公开 v1 的 `immediate`、`scheduled`、`suppress`；
- scheduled local time；
- quiet hours、foreground presence 与各自穿透条件；
- coalesce key 与 batch 上限；
- attention channel。

policy 的 `(id, version)` 不可变；每个 id 只有最高版本参与新事件匹配，最高版本 disabled 时不回退旧版本。Claim
冻结精确 version，后续安装不会改写其解释。JSON 文件、CLI 与 owner API 提供 install/list/test/preview，preview 不写生产状态。
当前没有 event recompute 操作；已经匹配的 event/claim 保持原 version。

WB-MVP-003 已选择删除而不是补造五 mode：`debounce`、`digest`、`max_delay`、resume spread、transport preference 与
busy behavior 不进入 public policy v1，也不能经 Core install/config 绕过。旧 DB row 只读保留、标记
`legacy_unsupported`，不参与新事件匹配；既有 Claim 仍使用冻结的 eligibility/version 完成后续 gate 与 batch。

### Release / schema lifecycle

schema 8 是当前 package baseline，6 是最低显式升级版本。新 DB 只由 `init` bootstrap；普通 daemon、CLI、MCP 与 Core
打开旧 schema 时 fail closed，不拥有 migration authority。`wakebridge upgrade` 先创建并验证一致性 SQLite snapshot，
再显式授权 migration；迁移后 schema/integrity 验证失败会自动恢复 snapshot。restore 同样要求 offline assertion、精确
instance id、checksum/integrity，并在替换前再备份 current DB。

首个 service profile 是 macOS LaunchAgent；plist 固定 loopback、config 与已安装 CLI 路径，不保存 owner credential。
`0.9.0-preview.9` 候选增加 Linux systemd user service，同样固定 loopback，并只引用 mode-600 environment/source credential
file，不把 secret 写进 unit 或 argv。已发布的 `0.9.0-preview.8` 支持矩阵仍只有 macOS；Linux 必须通过 Ubuntu 24.04 LTS
x64 真实 reboot canary 并进入新 release note 后才构成公开支持。

### Attention Claim Store

Attention Claim 表示“某个 resource 从某个时刻起值得重新获得一次注意力”，不是任务或执行授权。

- 普通 source ingest capability 只能提交 event；provider credential 留在 connector；
- policy 可以从 event 创建 claim；
- agent capability 可以在自己的 space 内事务性创建 `self_commitment` event + claim；
- claim 支持 pending、deferred、eligible、batched、consumed、dismissed、expired；
- snooze 只改变下一次 eligible 时间，不改写来源事实；
- agent 从其他路径已经处理来源时，Bridge 中出现 stale claim 是正常边界，醒来后重新验证并廉价 dismiss。

### Scheduler / Batcher

- immediate：立即创建 eligible Claim；
- fixed batch window：从首条 eligible Claim 创建 Batch 时冻结 deadline；后续 append 不重置窗口；
- max events：达到上限后同 key 的后续 Claim 创建下一 Batch；
- scheduled：Claim 在 agent timezone 指定时间 eligible；
- suppress：不创建 Claim，但 event 保留。

quiet hours 当前默认 defer。允许 bypass 必须由具体 policy 明写；release 使用精确本地分钟边界，不提供 resume spread。

### Foreground Presence Lease

前台在场不是钟表 quiet hours，也不等于单个 turn busy。它是一项按 `attention_channel + endpoint_id + binding_generation` 绑定的短 TTL 调度租约：

- 可信 host adapter 观察到真实用户消息 accepted 时续租；
- 只依赖 active edge，不要求判断用户何时离开；
- TTL 到期自动解除，不允许 idle 事件缺失造成永久静默；
- policy 可选择 `defer_while_presence` 或显式 bypass；
- 普通 source ingest capability 无权续租，否则外部来源可以通过反复伪造 active 让 agent 永久失聪；
- generation 变化立即使旧 lease 失效，避免换窗后旧 host 继续静默新 session。

### Explicit Inactivity Watch

Inactivity Watch 是 agent 显式登记的调度条件，不是 Bridge 自动推导的 continuity rescue：

- 配置属于 `(space_id, attention_channel)`，只观察当前 binding 的 `endpoint_id + generation`；
- 可信 Host Adapter 以短 TTL activity lease/edge 报告用户交互或独立 session turn；普通 source 与 Wake 自身投递
  不能续租。该 lease 与只代表真实用户在场的 Presence Lease 分开；
- 无 binding 时为 `waiting_for_endpoint`，无 `session_activity_observable` capability 时为 `unsupported`；
- 达到 `idle_after` 后事务性生成内部 Event 与 Claim；`once` 每个 inactive epoch 一次，`repeat` 按 agent 显式配置
  的 cadence 继续生成幂等 sequence；
- takeover fence 旧 observation，并等待新 binding 的 activity baseline；
- Bridge 不读取 agent 回复、self-commitment coverage 或未完成事项来判断它是否“记住了”。

因此重复 watch 是普通 scheduler recurrence。是否登记、换窗后是否恢复 binding，以及 agent 的私有记忆如何恢复，
仍由 harness 决定。

### Endpoint Registry

- Core 与 agent-facing MCP 不发现任何具体宿主的 session，也不认识宿主产品的私有状态；
  agent 或其 harness 必须通过通用 register/takeover contract 显式声明 opaque session/endpoint ref；
- agent harness 自己决定怎样在开窗时注入身份、恢复上下文和调用 register/takeover。Wake Bridge 不规定
  launcher/SessionStart/loop，也不把 harness 漏登记解释为需要自动修复的记忆故障；
- endpoint registry、attention channel 与 binding generation 全部以 Agent Space 为作用域；两个独立实例中的 agent
  可各自拥有名为 `life` 的 channel，互不冲突；
- endpoint 是 agent 或其 harness 显式声明、由 Host Adapter 验证并实现投递的 opaque delivery target，
  不是 Agent Principal；
- Host Adapter 不替 agent 选择“当前窗口”，只验证/实现它所声明的 endpoint route 并报告 transport capability；
- register 只登记当前 MCP caller，已占用 channel 的换手必须另做带 `expected_generation` 与 reason 的显式 takeover；
- 未 register 或不再匹配 binding generation 的 caller，除 status/inspect 外不能创建或改变 endpoint-scoped state；
- binding key 为 attention channel；
- 多个 channel 可以显式指向同一 endpoint；
- takeover 使用 compare-and-swap 并递增 generation；
- 不根据 process name、last_active 或“最近窗口”自动接管；
- waiter 是一次尚未返回的 MCP invocation，不能跨 Bridge 重启恢复。

仓库外宿主不通过动态 import 获得 daemon 内对象。Out-of-process local Host Adapter 使用 public `HostSessionClient` 以 scoped
bootstrap credential 登记 opaque session、loopback delivery origin 与独立 route token；daemon 内置 generic HTTP transport 只发送
versioned WakePayload reference。具体 bridge/frontend/runner 保留在宿主侧，不能访问 Core、DB 或 owner control。

### Wake Outbox / Dispatcher

- wake batch 先落 durable outbox；
- worker 以 lease 领取，崩溃后可恢复；
- 同一 batch 的重试沿用逻辑 id；
- warm 与 cold 不并发双投；
- 分开记录 transport accepted、agent completed、agent seen、event consumed/acted。

### Host Capability Registry

transport adapter 必须显式报告能力，而不是让 core 按宿主名称猜测：

- `cold_push`：能否在没有 pending turn 时创建新 turn；
- `warm_resume`：能否通过 live channel/waiter 进入现有 session；
- `requires_live_binding`：adapter 是否只能服务当前存活且已登记的 endpoint；
- `queue_when_busy` / `steer_when_busy`；
- `foreground_presence_observable`：能否在模型回合外可靠观察真实用户 active edge；
- `waiter_turn_settle_compatible`：pending waiter 是否允许普通用户输入、取消和 turn settle；
- 支持的 receipt stage；
- transport receipt 的最强上界与 support tier；
- 地址验证与最低宿主版本。

`wakebridge doctor` 只报告 contract 与已配置 adapter 明确声明的能力。Wake Bridge 不把任何具体 agent 产品列入
公共支持矩阵；某个 adapter 的 UAT 只能证明该 adapter 与对应 host/version 的组合。

Generic local protocol 是 integration contract，不是对所有实现者的支持声明。外部 adapter 固定为 experimental，必须以自己的
host/version 验证 busy、restart、activity、receipt 与 exact-session routing 后，才能另行讨论 support tier。

## 4. 事件、Claim 到唤醒的完整路径

```text
source reports event
  → adapter normalizes WakeEvent
  → Event Store dedupes and commits
  → source receives durable-ingest receipt / cursor commit
  → Rule Engine selects policy version
  → create/suppress durable Attention Claim
  → claim reaches eligible time
  → quiet-hours and foreground-presence gates
  → Scheduler opens/freezes wake batch from eligible claims
  → resolve attention-channel binding
  → one exact live adapter route OR waiting_for_endpoint
  → agent acknowledges batch
  → agent reads authoritative source
  → agent records consumed/acted result
```

显式 inactivity watch 走一条并列入口：`trusted activity observation → idle condition due → internal
channel.inactive Event + Claim`，随后复用同一套 gates、batch、binding 与 dispatch 路径。

最小 wake payload：

```json
{
  "service_id": "personal-mac",
  "agent_id": "agt_01...",
  "space_id": "spc_01...",
  "wake_batch_id": "wb_01...",
  "attention_channel": "group:home",
  "claim_refs": [
    {
      "claim_id": "ac_01...",
      "event_id": "evt_01...",
      "source": "group_chat",
      "resource": "group-chat://home/messages/msg_01...",
      "cursor": 184
    }
  ],
  "binding_generation": 7
}
```

payload 不复制邮件正文、群聊上下文、小机知道正文或来源 credential。

## 5. endpoint 选择算法

```text
1. 从 policy 得到 attention_channel。
2. 读取该 channel 的唯一 primary binding；只有显式配置才可 fallback 到 default。
3. 无 binding 或 lease 过期：batch 等待 endpoint，不猜其他 session。
4. generation 变化：重新解析当前 binding。
5. 若存在匹配 generation/channel、lease 有效且由已配置 Host Adapter 验证的 route：向该 endpoint 投递一次；
   busy/queue/steer 语义以 adapter capability 为准。
6. 否则保持 `waiting_for_endpoint`；Core 不为等待未知 route 长时间占住 worker，也不创建 replacement session。
7. 任一时刻只有一个 active attempt。
8. permanent rejection 进入 needs_attention/dead_letter，原 event 保留。
```

`dead_letter` 只能由 operator 在修复根因后，用当前 attempt fence 显式重开为 `retry_wait`；重开调用不直接 dispatch，也不会复活
已经 finalized 的 claim。统一 status 只暴露 error class 与 liveness 摘要，不输出 route/session/provider error details。

同一份 Host Adapter 实现可以被多个独立实例复用；每个 adapter process 仍只使用本实例 credential，投递到
本 Agent Space binding 指向的 endpoint。换窗时是否以及何时调用 takeover 由 agent harness 决定；Bridge 只执行
显式请求，普通 mutation 失败不能自动触发 takeover。
没有已安装并验收的 adapter 时，batch 保持 `waiting_for_endpoint`；Core 不扫描最近窗口、进程或私有目录来补猜。

## 6. MCP warm waiter（未选择的 alternate experiment）

```text
register/takeover endpoint
          ↓
wait_for_wake(endpoint, channels, generations, timeout)
          ↓  tool call remains pending
      eligible batch arrives
          ↓
return minimal wake payload
          ↓
ack batch → read source → consume/act → re-arm
```

这只可能恢复仍在进行的 turn。普通 endpoint 注册不能让已经结束的 task 自行复活。任一宿主对超时、取消、
后台、休眠、用户插话与 turn settle 的行为都必须由对应 adapter 按明确版本实测。

warm waiter 不属于当前公共 Host Adapter Contract。除非后续独立 spike 证明另有价值，否则不把长期 pending tool
call 作为基线。它也不是 Event Store、claim、policy 或 binding 的前置依赖。

## 7. Host Adapter Contract

Host Adapter 必须：

- 为一个精确 session/task 建立 opaque route，并显式 register/renew/close；
- 自己实现 start/resume/inject、busy queue 与 session lifecycle；
- 只声明已经实测的 capability 与 receipt upper bound；
- 如果提供 presence/activity observation，只在观察到对应真实 host edge 时续租；
- 以当前 binding 的 outstanding delivery nonce 排除 Wake echo，不能只信消息文本；
- Core 在 transport write 前只持久化 nonce hash；raw nonce 不写入 receipt/inspect/log，匹配后 one-shot consume，
  replay、旧 generation 与 session close 后使用均 fail closed；
- route 失效或 lease 到期后停止接收；没有有效 endpoint 时等待，不扫描窗口或静默改投另一个 session。

仓库提供一个中立 reference Host Adapter，演示 out-of-process loopback v1 的最小生命周期。它是 contract 示例，不是
某个 agent 产品的兼容层，也不证明使用者自己的 host 可以可靠立即唤醒。旧的具体产品实验保留在 research 与 historical
operations 文档中，只作为当时能力和失败边界的证据。

## 8. 独立性与故障语义

| 场景 | 行为 |
| --- | --- |
| source 重复投递 | event dedupe，不产生第二个逻辑事件 |
| source 暂时离线 | 保留 cursor；其他 sources 继续工作 |
| agent 无 endpoint | event/batch 留在该 Agent Space 等待，不改投另一个 agent |
| 新 session takeover | 旧 generation fencing；未完成 batch 重新解析 |
| quiet hours | 非 bypass 事件延后 |
| foreground presence lease 有效 | `defer_while_presence` claim 延后；其他 channel 与显式 bypass 不受影响 |
| host 只能观察 active edge | 每次活动续 TTL，过期自动解除；不等待 idle 事件 |
| 已登记 watch 达到 idle threshold | 按 `once` 或显式 `repeat` cadence 生成幂等 Event/Claim |
| watch 无有效 binding | `waiting_for_endpoint`；不把旧 endpoint 沉默算作当前 session inactive |
| host 无 session activity observer | watch 为 `unsupported`；不从进程、focus 或模型输出猜测 |
| dispatcher 崩溃 | lease 到期后恢复相同 batch |
| waiter 被取消 | batch 未 ack 时仍可审计重试 |
| harness 报告 endpoint route 已失效 | batch 等待新 binding；Bridge 不创建 replacement window |
| delivery target 真正改变 | harness 提交新 endpoint register/takeover；旧 waiter/presence lease 按 generation 失效 |
| harness 未在换窗后恢复 binding | batch/watch 保持 `waiting_for_endpoint`；Bridge 不猜窗口；显式 watch 也不掩盖漏登记 |
| 来源已被 agent 从别处处理 | claim 仍可能到期；agent revalidate 后 dismiss，不把它当 Bridge 数据损坏 |
| 一个 Agent Space/daemon 故障 | 该实例暂停；其他独立实例与任何 source 自身功能不变 |
| daemon 重启 | 该 Agent Space 从自己的 durable state 续跑 |

- 每个首版实例独享 SQLite、admin/host/source credential、source ingest capabilities 与 connector secret namespace；
  package 与 adapter 实现可以共享，运行时 secret/route 不共享。
- 管理 API 只绑定 loopback 或认证本地 IPC。
- 日志默认只记 event/claim/batch/policy/endpoint id、时间和错误分类。
- adapter 不执行来源正文中的 shell 或模型指令。
- 通用 ingress 拒绝任意 prompt/system instruction 字段；wake prompt 由 Bridge 固定模板生成。
- 开源 adapter 使用版本化 conformance fixtures，不能通过导入 core 私有模块获得额外权限。
