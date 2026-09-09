# Wake Bridge

Wake Bridge 是一个 local-first 的 **agent attention scheduler and delivery core**。外部来源登记“发生了什么”，agent 用自己的 policy 决定是否、何时重新获得注意力；Wake Bridge 把结构化 wake envelope 可靠交给 agent 所在宿主明确登记的 Host Adapter。

它不是 agent runtime、聊天前端或记忆系统，也不直接承诺唤醒某一种 CLI/Desktop 产品。真正启动、恢复或排队 agent session 的能力属于拥有该 session lifecycle 的 Host Adapter。

当前 preview 已实现并验证：

- durable Event、Claim、Batch、outbox 与分层 receipt；
- immediate、scheduled、suppress、quiet hours 与 presence gate；
- agent 自主预约未来的 self-commitment；
- endpoint lease、attention-channel binding 与 generation fencing；
- source-scoped ingress、Source Connector checkpoint 与身份 fencing；
- versioned event/source/transport SDK；
- out-of-process loopback Host Adapter Contract；
- 单 Agent Principal / 单 Agent Space / 单 daemon 的本地部署。

当前公开边界见 [架构与路由](docs/architecture.md) 和
[Out-of-process Host Adapter contract](docs/operations/out-of-process-host-adapter.md)。

## 开始使用

从 [First Wake Quickstart](docs/quickstart.md) 开始。它覆盖 clean install、Agent Space 初始化、仓库外 Host Adapter、第一条受控 wake、MCP、presence、self-commitment 与可选 Source Connector。完整公共文档见 [文档导航](docs/README.md)。

`0.9.0-preview.8` 是首个公开 pre-release，已通过 clean-artifact 首发演练并发布到 npm 的 `preview` channel。Wake Bridge 本身不内置任何具体 agent 产品 Host Adapter；使用者必须已有或实现一个能精确控制目标 session 的 adapter。

## 为什么有 Wake Bridge

项目最初来自小机知道上的一个公共产品问题：站方是否会做 Wake Bridge，让 agent 不必反复轮询通知，也能在模型回合之外发生事件时重新获得注意力。

它解决的不是“怎样让 agent 永远记住某件事”，而是另一层问题：

- 不需要模型反复轮询邮件、论坛或群聊才能发现新事件；
- 调度与待处理事项不依赖某一个模型回合持续运行；
- agent 可以为未来的自己预约何时、为何重新醒来；
- agent 可以选择自己的 Source Connector 与个人唤醒策略；
- 可信 presence 表明用户正在交谈时，后台注意力可以让位；
- 事件、决定、投递和消费都有 durable、可审计的生命周期。

完整背景见 [项目背景与目标](docs/background.md)。

## 架构边界

```text
External Source
  └─ Source Connector / authenticated ingress
       └─ WakeEvent reference
            ▼
Wake Bridge Core
  Event → Policy → Claim → Gates → Batch → Durable Outbox
                         ▲                    │
                  Presence lease             ▼
                                     Host Adapter Contract
                                              │
                                              ▼
                                  exact agent session/task
```

### Wake Bridge Core 负责

- 事件归一化、去重、checkpoint 与有限 metadata；
- durable Attention Claim 与 self-commitment；
- 个人 policy、quiet hours、presence 与 inactivity watch；
- endpoint/binding lease、generation fencing、batch/outbox 与重试；
- 生成固定的 wake envelope，并记录不高于真实证据的 receipt。

### Host Adapter 负责

- 识别自己拥有的精确 agent session；
- 启动、恢复或排队该 session；
- 将 wake envelope 注入 agent 输入面；
- 报告自己真正观察到的 delivery/session 阶段；
- 在具备可信观察能力时维护 session lifecycle 与 presence。

### Agent/harness 负责

- 醒来后读取 envelope 引用的权威来源；
- ack、consume、snooze 或 dismiss claim；
- 上下文、记忆、身份与换窗恢复；
- 决定是否安排下一次 self-commitment。

`transport_accepted`、`host_accepted`、宿主实际观察到的 turn start、`agent_seen` 与 `agent_consumed` 是不同证据层。
当前公共 adapter HTTP 202 最多只产生 `transport_accepted` receipt，不能冒充 agent 已醒来。

## Host Adapter Contract

已有 backend、agent runner、bridge service 或自制 frontend 的宿主，应通过公开 Host Adapter Contract 接入。推荐使用
[`HostSessionClient`](docs/operations/out-of-process-host-adapter.md) 登记 opaque session id 与 credential-free loopback route；delivery credential 只属于该 adapter process，不与 owner/source credential 共用。

实现协议只证明 compatibility，不自动获得“Wake Bridge 支持某宿主产品”的分类。具体集成维护方必须自己声明并验证：

- exact identity；
- cold start / warm resume / busy queue 行为；
- activity/presence 是否可观察；
- receipt 上界；
- restart、lease expiry 与 generation takeover；
- 端到端 wake 时延与失败语义。

仓库提供一个不具名的 [reference Host Adapter](examples/reference-host-adapter/README.md)，只演示公开 contract 与生命周期，不内建任何 agent runtime。

## Source 与 Source Connector

- **Source**：发生事实的外部系统或 feed，例如论坛、邮件服务或群聊。
- **Source Connector**：从 Source 增量读取或接收事实，并转换为 `WakeEvent` 的独立程序。
- **Source binding/subscription**：某个 Agent Space 对具体 source identity 的配置与 checkpoint。

Source Connector 只报告事实，不能选择 endpoint、绕过 policy 或注入任意 prompt。Wake envelope 只携带 resource reference；正文仍由 agent 醒来后通过来源自己的 API/MCP 读取。

上游 credential 由独立 connector 保管，不进入 Wake Bridge daemon、event、receipt 或模型上下文。Manifest 必须分别声明上游 credential breadth、credential custody、connector 实际暴露能力与读取副作用。

### 首发 catalog

Connector 采用“可发现、可选配置、默认关闭”的模型。默认提供不表示默认登录、创建 checkpoint、轮询或唤醒。

| Connector | 状态 | 默认行为 |
| --- | --- | --- |
| 小机知道 | official optional | disabled；verify + from-now bootstrap 后才能 enable |
| Gmail | planned | 不可 enable；完成独立设计、provider 合规与真实 UAT 后再进入 available |
| Group Chat fixture | reference/conformance | disabled；用于证明通用 connector contract |

Source Connector 的本地协议、身份 fencing、cursor CAS 与错误处理见
[Source Connector 安装与本地协议](docs/operations/source-connectors.md)。

## Preview 安装

当前 package 名称为 `wake-bridge`，发布 channel 为 `preview`。已验收平台为 macOS arm64/x64、Node.js 20+ 与 SQLite CLI 3.33+；Linux/systemd 尚未进入支持矩阵。

从 npm 安装当前 preview：

```bash
npm install --global wake-bridge@preview
wakebridge release-preflight
```

这是 npm registry 中的首个也是当前唯一版本，因此 registry 同时把它作为 `latest` 的初始指向；在首个 stable 发布前，无标签安装也会解析到此 preview。为明确表达依赖意图，当前仍推荐显式使用 `wake-bridge@preview`。

从源码 checkout 生成并验证本地 tarball：

```bash
npm pack --json
npm install --global ./wake-bridge-0.9.0-preview.8.tgz
wakebridge release-preflight
wakebridge init \
  --data-dir /absolute/private/path \
  --instance-id default \
  --owner-id your-agent \
  --timezone UTC
```

旧 schema 不会在普通 daemon/inspect 启动时隐式迁移。升级必须使用带 verified snapshot 和失败自动恢复的 `wakebridge upgrade`。完整流程见 [release lifecycle runbook](docs/operations/release-lifecycle.md)。

### 公共扩展入口

第三方集成只依赖这些版本化 subpath：

```text
wake-bridge/event
wake-bridge/source
wake-bridge/transport
```

Root import 只聚合公开 contract。Core、SQLite、daemon 与 owner control 不属于 package API。详见
[public SDK boundary](docs/operations/public-sdk.md)。

### 无 Host Adapter 时

Wake Bridge 仍会可靠保存 Event、Claim 与 Batch，但 delivery 明确停在 `waiting_for_endpoint`。Core 不扫描窗口、不猜最近 session，也不悄悄降级到另一个宿主入口。

## 安全原则

- 一个 preview daemon instance 固定服务一个 Agent Principal / Agent Space。
- 每个 Space 独享 SQLite、端口、owner/source/host credential、policy、connector 与 endpoint registry。
- source credential 只能在固定 source namespace 内登记 Event。
- Host Adapter 只能登记获授权的 host kind 与 route，不能获得 owner API 或读取其他 source secret。
- 通用 ingress 和 Source Connector 不能提交 wake prompt。
- 来源正文、provider token 与 connector bearer 不进入 durable event/receipt。
- broad/unknown provider credential 若直接注入 Wake Bridge process，默认 fail closed。

## 当前非目标

- 内建 agent runtime、聊天 frontend、memory system 或 session manager；
- 保证任意 CLI/Desktop 产品能被外部进程及时唤醒；
- 通过进程、窗口焦点或“最近活动”猜 agent identity；
- 替 harness 保证换窗后的记忆、身份 bootstrap 或 loop 连续性；
- 将模型放进 scheduler 热路径自由决定优先级；
- 复制邮件、论坛、群聊等来源正文；
- 自动登录第三方服务或默认启用 connector；
- 把 `transport_accepted` 宣传成 agent 已读或已处理；
- 在首版中提供共享多 Agent Space daemon。

## 主要文档

- [First Wake Quickstart](docs/quickstart.md)
- [文档导航](docs/README.md)
- [项目背景与目标](docs/background.md)
- [架构与路由](docs/architecture.md)
- [协议与数据模型](docs/protocol.md)
- [Out-of-process Host Adapter](docs/operations/out-of-process-host-adapter.md)
- [Source Connector](docs/operations/source-connectors.md)
- [Source-scoped ingress](docs/operations/source-ingress.md)
- [Personal policy](docs/operations/policies.md)
- [Explicit inactivity watch](docs/inactivity-watch.md)
- [Release lifecycle](docs/operations/release-lifecycle.md)
- [公开验证口径](docs/verification.md)
- [小机知道 Source Connector](docs/adapters/botlingknows.md)

## 项目词汇

| 词 | 含义 |
| --- | --- |
| Agent Principal | Wake Bridge 中一个稳定、独立的 agent 身份 |
| Agent Space | 一个 Agent Principal 独享的状态、凭证与策略边界 |
| Source | 发生外部事实的系统或 feed |
| Source Connector | 把 Source 的增量事实转换为 WakeEvent 的独立程序 |
| Event | 已发生事实的不可变 reference，不等于模型唤醒 |
| Attention Claim | 一件事从某时起值得重新获得一次注意力的 durable 请求 |
| Policy | 从 Event 生成 Claim，并决定时间、静默、presence 与聚合的规则 |
| Presence lease | 可信 host observation 产生的短 TTL 调度门 |
| Inactivity watch | agent 显式登记的 session 失活条件，不等于记忆保证 |
| Wake batch | 一次 delivery 携带的一组 claim/event references |
| Attention channel | agent 自己定义的注意力槽，例如 `life` 或 `work` |
| Endpoint | Host Adapter 显式登记的 opaque delivery target |
| Binding | attention channel 当前使用的 primary endpoint |
| Generation | takeover 时递增、用于 fencing stale endpoint 的代际号 |
