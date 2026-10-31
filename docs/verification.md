# 公开验证口径

## 发行基线与源码增量

当前源码候选为 `0.9.0-preview.21`，尚未发布，见 [候选 release notes](releases/0.9.0-preview.21.md)。
npm 发行基线仍为 `0.9.0-preview.20`；以下保留其 [release notes](releases/0.9.0-preview.20.md) 与历史源码验收证据。
发布准备前源码提交 `c281df19469e03b18826a9259b671e789049d7dd` 的本地完整回归为 488 项 Vitest + 17 项 Node，共 505 项；
[同提交 CI](https://github.com/reneyuxi0402/wake-bridge/actions/runs/40284178626) 四组首轮全部通过。
这些是源码验收，不代替 preview.20 自身 tag 的发布门禁；该 tag 的四组全部通过后，工作流才发布 npm `latest` 与 GitHub prerelease。
发布和安装状态应以对应 tag 工作流及 npm 实际可下载产物为准，不以源码版本号或 main CI 成功推断。

### preview.21 本轮 review 修复与验收

本轮新增安全删除 guard：默认使用中的 channel 返回 `409 channel_in_use` 与安全 usage，仅 MCP/owner HTTP
显式严格 boolean `force:true` 才能强制删除；已删 ID 不恢复/复用，在途消息不能追回，ack/consume 的 `channel_deleted` 为终态。
隐式来源 emit 额度不足改为落盘 suppressed（`channel_limit_reached`），cursor 继续、无 channel/claim；
显式 create/schedule/watch/host 注册仍429，已删目标来源 emit 为 `channel_deleted`，释放额度与原 dedupe 重放均不补发。
Operator `ingest.channel_suppressions` 与 MCP 紧凑事件 `suppression_reason` 提供保留历史诊断，不含 payload、
不单独改变 health/attention_required、不新增 incident。Reference 单路旧 handle 在确定 stale/deleted 后停止观察与接受，
不影响 survivor，不自动抢回路由；迟到旧错误不能删除替代 handle。

2026-10-30（America/Los_Angeles）本轮 macOS arm64 / Node 20.20.1 完整回归：46文件603项 Vitest +17项 Node，共620项全部通过。
typecheck、build、仓外 tarball 安装通过；监督器退出0，无超时、观察失败、退出后泄漏或自有进程残留，原30秒测试门限未改。
H1/H2/API/reference 定向反例先红后绿；下方590项是修复前历史，不冒充此轮结果。
本轮同 tag 的 macOS/Ubuntu × Node 20/24 四组发布门禁尚未执行，main CI 与发布状态也须分别核对，不据本地结果推断已经发布。
候选仍为每 agent 一个 Space/daemon/DB，Claude adapter 仍单路；不新增其他具体宿主接入、真实模型或新平台 UAT 声明。

### 多 Agent / 多 Channel 源码增量（尚未发布）

未发布 preview.21 源码增加 `openChannels` / `leases`，支持一个真实 session 原子登记完整 channel 集合。
旧单 channel 响应保持兼容；共享 endpoint 的重登、续租、关闭范围明确，遗漏现有 channel 的重登会拒绝。
这不是共享多租户服务，仍为每个 Agent Space 独立 daemon、SQLite 和凭据。操作见 [多路接入](operations/multi-agent-channels.md)。

新增回归覆盖：

- 公开 HTTP/SDK 的完整登记、回滚、确定性竞争注入、bootstrap scope、token 旋转与逐 channel generation；
- 两个 agent 都有同名 life/work，同/不同 session 两种拓扑，错误凭据与错 channel nonce 不改变状态；
- 一路换窗、关闭、presence/quiet 或 HTTP timeout，不误投另一 channel / agent；已接受缺 ack 不重投；
- 两个独立 daemon 子进程中终止 A，B 继续 HTTP 收发与续租；A 从原 SQLite 恢复原 claim 并续投；
- 仓库外 tarball 的新 SDK 类型和实际多 channel 登记/投递，以及双实例配置和 service profile 文件互不覆盖；
- reference adapter 在登记响应尚未返回时以可重试 425 拒绝过早投递，而非把工作永久送入 dead letter。

这些是合成来源、loopback 接收方与临时数据库测试；fixture 显式模拟 agent ack/consume，不启动模型。
服务测试只生成隔离目录中的 profile，不激活系统服务；进程故障测试不模拟 dispatch 中途崩溃或系统资源耗尽。
不新增真实宿主、多日运行、跨平台 CI 或延迟 SLA 的验收结论，随包 Claude CLI adapter 仍为单 channel。

2026-10-30 本地 macOS arm64 / Node 20.20.1 完整复验为 43 个文件、533 项 Vitest + 17 项 Node，共 550 项通过；
typecheck、build、仓外安装检查通过。首轮既有 1105-claim 积压/重启用例超过原 30 秒门限（34.276 秒），其余 532 项通过；
未改代码或门限，单跑 4.573 秒、完整复验中 3.780 秒通过。耗时波动原因未确认，不把复验通过写成首轮全绿或根因已修复。

### Agent 自主管理 Channel 源码增量（尚未发布）

在上述多路基础上，新增公开 MCP / owner HTTP 的 channel 创建、列出、显示名修改和删除。
稳定 ID 与显示名分开；有效 channel 默认上限32、由实例配置调整；每条 channel 仍最多绑定一个 session。
动态 host scope 须显式选择本 Space 已登记 channel，不扩大旧 allowlist。新增 schema 9，已有数据库须显式离线 upgrade。

回归覆盖原子额度竞争、旧调用隐式登记、已删 ID 不复用、删除事务回滚、删除与 schedule/watch/绑定/presence/activity/ack/consume 竞争、
删除进行中投递和共享 session 另一 channel 的 nonce/ack/consume。来源继续命中已删路由时，事件持久化为 suppressed，不阻塞 source cursor。
迁移覆盖实际历史路由、只有固定 policy 的空实例、忽略未被 policy 采纳的原始 hint，以及已有数量超限时不裁剪。

公开 MCP 组合测试启动两个真实 CLI MCP 子进程、两个隔离 daemon/DB 和 loopback host 接收方，执行同名 channel 的创建、改名、
投递、ack、consume 与删除。这里两个 daemon 在同一个测试进程中，不能用它证明进程故障隔离；上节独立 daemon fixture 另行覆盖退出/重启。
安装包黑盒验证 owner HTTP CRUD 与 SDK 错误类型；reference 验证临时续租失败恢复和确定 endpoint fencing 后停止接收、不自动抢回绑定。
显式 create 的 ID 首尾空格反例先红后绿，避免创建和 host 登记产生不同路由；不改变显示名 trim 或旧数据读取/删除行为。

并发 claim 回归在新事务入口下更新测试注入接缝，保留原断言，并逐一确认竞争回调实际执行一次。
所有上述消费与回执均为合成 fixture，不新增真实模型、第三方宿主或多日运行保证。

2026-10-30 本轮 review 修复前的历史本地 macOS arm64 / Node 20.20.1 回归：46 文件、573 项 Vitest + 17 项 Node，共590项通过，
typecheck、build、仓外安装与进程清理观察通过。仍是未发布源码，不是 npm preview.20 的能力或新跨平台 CI 结论。
早期全量曾受 loopback/进程观察的 sandbox EPERM 阻止，且暴露上述并发测试接缝失效；均保留失败记录，不称首轮全绿。

### preview.19 历史发行基线

`wake-bridge@0.9.0-preview.19` 已发布。其 tag 提交为 `e62eaa34238445bb92f6c14805f1fd1f3631f804`，
[发布工作流](https://github.com/reneyuxi0402/wake-bridge/actions/runs/40104086788) 在同 tag 的四组 CI 全部成功后发布；
macOS Node 20 首轮停滞取消、第二次通过的事实保留，根因未确认。npm 与 GitHub 提供同字节 tarball，SHA-256 为
`76ce265fb0be12e413f1747058e211ec040bcb6a5f8eaba83197613d0b64fec5`。该发行基线每组产品回归为 392 项 Vitest + 17 项 Node 测试。

preview.20 汇入发行后的文档与 CI 诊断改动，以及安全错误分类、source 有效配置解释、积压调度边界和恢复失败清理；
这些不会改变已发布的 preview.19。
当前维护回归不新增真实模型 UAT、不扩大宿主或平台支持范围；具体测试覆盖与限制见下文及相关运维页。

### 后续维护的回归边界

- 安全诊断：以合成敏感标记覆盖 SQLite 失败、配置解析、scheduler warning、HTTP 未分类异常和 connector 网络错误；不宣称任意第三方 adapter 的日志已自动脱敏。
- 有效 source 配置：覆盖省略 `enabled`、持久开关覆盖、重启、enable/disable 与离线不可观察；不改变原默认值或优先级。
- 积压：真实 SQLite 中放入超过十万条未来 Claim / Batch，确认新到期工作仍可推进；另覆盖无关超大字段、每轮预算、重启、并发 CAS、等待项轮转与 resource 等价性。
  千条预算/轮转测试对固定 fixture 的重复只读查询使用局部快照，排序与写事务仍走真实 DB；它们是正确性回归，不是完整子进程吞吐基准。
- 维护恢复：校验坏备份、坏 manifest、损坏原库的隔离恢复、prune 后去重保留，以及备份写入/恢复复制的一次性 ENOSPC 注入。
  不模拟真实磁盘耗尽，也不保证持续文件系统故障下自动回滚一定成功。

上述积压边界不意味着全部 SQL 都已分页、无限数据量可用或延迟有 SLA；显式大范围 inspect、单个超大记录与恢复扫描仍有容量/成本边界。
两小时独立试用和合成回归也不替代多日真实运行证据。操作与备份敏感性见 [维护及恢复演练](operations/release-lifecycle.md#长期维护与隔离恢复演练)。

## 验证范围

公共仓库用以下证据验证自身，不用具体宿主产品的现场结果代替通用契约：

- GitHub CI：每次 `main` push 与 pull request 均在 macOS 和 Ubuntu 24.04 上覆盖最低 Node 20 与当前 Node 24；

- Core durability：Event、Claim、Batch、outbox、retry 与 receipt 在 SQLite 中具有可恢复状态；
- storage integrity：多语句事务遇错整体回滚；超过一页的历史积压不会遮住新的 Claim/Batch；retention 不删除去重记录；
- policy：immediate、scheduled、suppress、quiet hours、presence、inactivity watch 与 self-commitment；
- Host Adapter contract：open、renew、close、takeover generation fencing、route credential separation 与 receipt upper bound；
- Source Connector contract：manifest identity、from-now bootstrap、cursor CAS、dedupe、retry 与 `needs_attention`；bundled Gmail Connector 另有真实账号 History/metadata canary 与无读取副作用证据；
- package black box：从 tarball 在仓库外安装后完成 import、init、daemon、emit、inspect、external Host Adapter、backup 与 service profile；
- reference adapter：只依赖公开 `wake-bridge/transport` subpath，并通过独立 conformance tests。

一次 HTTP 202 只证明 adapter transport 接受了请求。它不自动证明 agent session 已启动、agent 已看到消息或事项已处理。
更高层 receipt 必须来自宿主或 agent 能实际观察到的证据。

具体 Host Adapter 和 Source Connector 维护方仍须针对自己的版本完成真实登录、identity、restart、offline catch-up、latency、
presence/activity 与 failure-semantics UAT。通过公共 contract 代表 compatibility，不代表 Wake Bridge 项目替第三方宿主提供运行保证。
