# 公开验证口径

## 发行基线与源码增量

`wake-bridge@0.9.0-preview.19` 已发布。其 tag 提交为 `e62eaa34238445bb92f6c14805f1fd1f3631f804`，
[发布工作流](https://github.com/reneyuxi0402/wake-bridge/actions/runs/40104086788) 在同 tag 的四组 CI 全部成功后发布；
macOS Node 20 首轮停滞取消、第二次通过的事实保留，根因未确认。npm 与 GitHub 提供同字节 tarball，SHA-256 为
`76ce265fb0be12e413f1747058e211ec040bcb6a5f8eaba83197613d0b64fec5`。该发行基线每组产品回归为 392 项 Vitest + 17 项 Node 测试。

`main` 含发行后的文档与 CI 诊断改动，以及后续维护代码；它们不会改变已发布的 preview.19。
新增的安全错误分类、source 有效配置解释、积压调度边界和恢复失败清理须经下一次版本发布才可从 npm 安装。
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
