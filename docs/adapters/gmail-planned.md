# Gmail Source Connector

状态：planned，not bundled，not enableable。

Gmail 出现在 catalog 中仅用于说明预期的 Source Connector 形状；当前发行物没有 Gmail 实现、登录流程或支持承诺。

进入 `available` 以前至少需要完成：

- 选择并记录合规的 Google OAuth 应用与最小 scope；
- 定义 message/thread resource reference，不把邮件正文复制进 Wake Bridge；
- 明确 History API cursor、过期 cursor、初次 from-now bootstrap 和 backfill 语义；
- 验证读取不会自动 mark read、archive、modify 或发送邮件；
- 实现 token custody、refresh、revoke、rate limit、offline catch-up 与 `needs_attention`；
- 使用隔离账号完成真实身份、重启、重复 delivery 和端到端 wake UAT。

这些条件完成并经过独立版本验收后，catalog 才能把 Gmail 改为 `available`。
