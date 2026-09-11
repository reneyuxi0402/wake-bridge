# 公开验证口径

状态：`wake-bridge@0.9.0-preview.8` 已发布到 npm `preview` / `latest` 与 GitHub pre-release。

公共仓库用以下证据验证自身，不用具体宿主产品的现场结果代替通用契约：

- GitHub CI：每次 `main` push 与 pull request 均在 macOS 和 Ubuntu 24.04 上覆盖最低 Node 20 与当前 Node 24；

- Core durability：Event、Claim、Batch、outbox、retry 与 receipt 在 SQLite 中具有可恢复状态；
- policy：immediate、scheduled、suppress、quiet hours、presence、inactivity watch 与 self-commitment；
- Host Adapter contract：open、renew、close、takeover generation fencing、route credential separation 与 receipt upper bound；
- Source Connector contract：manifest identity、from-now bootstrap、cursor CAS、dedupe、retry 与 `needs_attention`；
- package black box：从 tarball 在仓库外安装后完成 import、init、daemon、emit、inspect、external Host Adapter、backup 与 service profile；
- reference adapter：只依赖公开 `wake-bridge/transport` subpath，并通过独立 conformance tests。

一次 HTTP 202 只证明 adapter transport 接受了请求。它不自动证明 agent session 已启动、agent 已看到消息或事项已处理。
更高层 receipt 必须来自宿主或 agent 能实际观察到的证据。

具体 Host Adapter 和 Source Connector 维护方仍须针对自己的版本完成真实登录、identity、restart、offline catch-up、latency、
presence/activity 与 failure-semantics UAT。通过公共 contract 代表 compatibility，不代表 Wake Bridge 项目替第三方宿主提供运行保证。
