# Wake Bridge documentation

这里仅收录当前公开产品边界、使用方法和扩展契约。内部评审、具名部署记录、撤回实验和研发计划不属于公共仓库。

本版为 `0.9.0-preview.21`，包含新 channel 能力、schema 9 与本轮 review 修复。实际发行状态以同 tag 门禁与产物为准，见 [验证口径](verification.md)。

## 开始使用

- [First Wake Quickstart](quickstart.md)
- [项目背景与目标](background.md)
- [架构与路由](architecture.md)
- [协议与数据模型](protocol.md)
- [Explicit inactivity watch](inactivity-watch.md)

## Host Adapter

- [Out-of-process Host Adapter](operations/out-of-process-host-adapter.md)
- [多 Agent / 多 Channel 接入](operations/multi-agent-channels.md)
- [Reference Host Adapter](../examples/reference-host-adapter/README.md)
- [Claude CLI online-session Host Adapter](../host-adapters/claude-cli/README.md)
- [Public SDK boundary](operations/public-sdk.md)

## Source Connector

- [Connector catalog](operations/connector-catalog.md)
- [Source Connector contract](operations/source-connectors.md)
- [Source-scoped ingress](operations/source-ingress.md)
- [小机知道 Connector](adapters/botlingknows.md)
- [Group Chat reference connector](adapters/group-chat.md)
- [Gmail Source Connector](adapters/gmail.md)

## 运维与发布

- [发行基线、源码增量与验证边界](verification.md)
- [长期维护与隔离恢复演练](operations/release-lifecycle.md#长期维护与隔离恢复演练)
- [Source 实际启停来源](operations/source-connectors.md#有效配置为什么与文件不同)
- [Personal policy](operations/policies.md)
- [Operator control](operations/operator-control.md)
- [Release lifecycle](operations/release-lifecycle.md)
- [Linux VPS deployment](operations/linux-vps.md)
- [公开验证口径](verification.md)
- [0.9.0-preview.8 release notes](releases/0.9.0-preview.8.md)
- [0.9.0-preview.9 release notes](releases/0.9.0-preview.9.md)
- [0.9.0-preview.10 release notes](releases/0.9.0-preview.10.md)
- [0.9.0-preview.11 release notes](releases/0.9.0-preview.11.md)
- [0.9.0-preview.12 release notes](releases/0.9.0-preview.12.md)
- [0.9.0-preview.13 release notes](releases/0.9.0-preview.13.md)
- [0.9.0-preview.14 release notes](releases/0.9.0-preview.14.md)
- [0.9.0-preview.15 release notes](releases/0.9.0-preview.15.md)
- [0.9.0-preview.16 release notes](releases/0.9.0-preview.16.md)
- [0.9.0-preview.17 release notes](releases/0.9.0-preview.17.md)
- [0.9.0-preview.18 release notes](releases/0.9.0-preview.18.md)
- [0.9.0-preview.19 release notes](releases/0.9.0-preview.19.md)
- [0.9.0-preview.20 release notes](releases/0.9.0-preview.20.md)
- [0.9.0-preview.21 release notes](releases/0.9.0-preview.21.md)
