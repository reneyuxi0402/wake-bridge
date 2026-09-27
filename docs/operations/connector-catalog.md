# Source Connector catalog

状态：current public catalog（2026-09-27）。Catalog 是可发现性清单，不是自动安装器；所有 connector 默认
`disabled`，未配置时不会登录 provider、创建 checkpoint、轮询或产生 wake。

查看机器可读清单：

```sh
wakebridge connector-catalog
```

## 当前条目

| Connector | 类型 | 可用性 | Bundled | Enable |
| --- | --- | --- | --- | --- |
| 小机知道 | official optional | available | yes | 完成 configure → verify identity → from-now bootstrap → UAT 后可显式 enable |
| Gmail | official optional | available | yes | 完成 configure → verify identity → from-now bootstrap → UAT 后可显式 enable |
| Group Chat fixture | reference | available | yes | 仅用于 contract/conformance 与自建来源参考 |

Catalog 的 `availability` 描述发行物能力；某个已配置 source 的 `disabled | enabled | needs_attention` 是运行状态，
两者不可混用。`available` 也不等于已经登录、已启用或生产 SLA。

## 小机知道：首次启用

1. 按 [小机知道 Connector](../adapters/botlingknows.md) 启动独立 connector process，并让 provider credential
   只留在该 process。
2. 将 source 写入私有 `sources.json`，但保持 disabled。
3. 运行 `attention_source_verify`，由 agent/operator 核对返回的 `subject_ref` 与 `binding_fingerprint`。
4. 显式执行 `attention_source_bootstrap(mode="from-now")`，避免把历史通知当作新事件。
5. 用一条专用测试通知验收 cursor、restart、`mark_read=false`、event/claim/wake 与 authoritative resource。
6. UAT 通过后执行 `attention_source_enable`。失败时保持 disabled 或 `needs_attention`，不要盲目重试登录。

命令行环境可用 `source-validate`、`source-bootstrap` 与 `source-once` 完成相同的低层验证。Connector 不携带默认
唤醒策略；用户需单独安装 policy。仓库提供的
[`botlingknows-conservative.json`](../../examples/policies/botlingknows-conservative.json) 只是可选 recipe：@mention
即时但尊重 quiet hours/presence，其余通知排到本地 09:00。安装前应按自己的 attention channel 与节奏修改并 preview。
这样同一条通知可以由不同 Agent Space 按各自节奏处理。

## Gmail：首次启用

1. 按 [Gmail Source Connector](../adapters/gmail.md) 准备私有 mode-600 OAuth credential 目录并启动 `gmail-connector`。
2. 将 Gmail 槽位写入私有 `sources.json`，保持 disabled；Google refresh token 只属于 connector process。
3. verify 返回的 `gmail:account:<address>` 必须由 owner 人工核对，再显式执行 from-now bootstrap。
4. 用隔离 DB 和一封无敏感内容的 canary 邮件确认 History cursor、分页、restart、event 最小化与 authoritative resource 回读。
5. 确认 connector 没有改变 unread/label 状态后才 enable。History 过期或 OAuth 撤销会进入 `needs_attention`，不得自动跳过缺口。

当前 catalog 刻意不包含尚未沟通和验收的第三方来源。添加条目需要先明确公开接口、认证与权限、读取副作用、
cursor/replay、速率限制、维护责任和真实 UAT，不因“能抓到数据”就自动成为 official connector。
