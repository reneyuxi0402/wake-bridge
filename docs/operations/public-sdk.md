# Public SDK boundary

状态：introduced in `wake-bridge@0.9.0-preview.2`; current contract `0.9.0-preview.21`，实际发行以同 tag 门禁与产物为准。

Wake Bridge 只把 event、source 与 transport 三条版本化 contract 作为第三方 package surface。Core、DB、daemon、owner
control 与 release lifecycle 是 package 内部实现，不是可导入 subpath；历史具体产品 Host Adapter 实验也不进入当前 tarball。`exports` map 会拒绝
`wake-bridge/core`、`wake-bridge/db`、`wake-bridge/daemon` 等路径。

`wake-bridge/transport` 在 preview.4 增加 `LOCAL_HOST_PROTOCOL_VERSION=1`、`HostSessionClient` 与 delivery validator，使仓库外
bridge service 可以通过固定 loopback wire contract 接入 stock daemon。Public client 只有 host-session lifecycle、foreground
presence 与 activity capability，
不暴露 Core、DB 或 owner control；内置 local HTTP transport 仍是 package private implementation。

preview.21 新增 `openChannels()` / `leases()`；必须使用同版本 daemon 和 SDK，不能对旧安装直接调用。
多个 channel 的 handle 共用 endpoint/token，generation 各自独立；续租/关闭仍是 endpoint 级。Agent 的 channel
create/list/rename/delete 走 owner MCP/HTTP，不扩充 host token 的管理权限，见 [多路与动态 channel](multi-agent-channels.md)。
实例配置可显式为 host 授权 `attention_channels: "*"`，仅覆盖本 Space 已登记的有效 channel；旧数组白名单保持不变。

`WakeBridgeSdkError.code` 保留兼容值。HostSessionClient 收到服务端失败时，`remote_code?: string` 提供经过格式检查的稳定原因，
例如 `channel_deleted`、`channel_limit_reached`、`endpoint_fenced`；无合法原因时为 undefined，不回传任意错误正文。
临时断连可保留原 token 重试；fencing 后不能自动重新 open 抢回路由，须明确处理失去登记的状态。
presence/activity 的 `stale_generation` 或 `channel_deleted` 只证明对应旧 channel handle 失效，应停止该路观察与接受，
保留其他路；迟到错误不能清除已替换 handle。`endpoint_fenced` 则是整个 endpoint token 失效。
来源 emit 的隐式目标达到 channel 额度时，事件可成功落盘为 suppressed（`channel_limit_reached`），无新 channel/claim；
不应把 ingest 成功当作已排队。显式 create/schedule/watch/host 注册仍返回 429，详见 [channel 生命周期](multi-agent-channels.md)。

## Entrypoints

```ts
import {
  EVENT_CONTRACT_VERSION,
  SourcePushClient,
  validateWakeEventInput,
} from "wake-bridge/event";

import {
  SOURCE_ADAPTER_CONTRACT_VERSION,
  validateSourceManifest,
  type PullSourceAdapter,
} from "wake-bridge/source";

import {
  HOST_ADAPTER_CONTRACT_VERSION,
  LOCAL_HOST_PROTOCOL_VERSION,
  HostSessionClient,
  validateLocalHostDeliveryRequest,
  validateHostAdapterManifest,
  type HostAdapter,
} from "wake-bridge/transport";
```

root `wake-bridge` 只聚合以上 contract 与 `RELEASE_VERSION`，不再导出 `WakeBridge`、DB、daemon、operator lifecycle 或
内置 adapters。JSON event schema 通过 `wake-bridge/schema/event-v1` 提供；policy schema 继续通过
`wake-bridge/schema/policy-v1` 提供。

## Version negotiation

- event payload 必须使用 `schema_version: 1`；省略时 SDK/daemon 规范化为 v1，其他版本 fail closed；
- source manifest 必须同时声明 `contract_version: 1` 与 adapter 自己的 `version`，二者不能混用；
- host manifest 必须声明 `contract_version: 1` 与 `adapter_version`；
- 当前 preview 只支持 exact v1，不做猜测、向下转换或“尽力兼容”。未来扩展必须增加 supported-version set 与明确迁移。

## Capability isolation

Source adapter 只能返回 event page；generic push client 只能向配置时固定的 loopback `source` 路径提交 v1 event。Host adapter
只收到 dispatch context 与 `{instance_id, daemon_origin}` lifecycle context，不会获得 Core、SQLite handle、owner token 或
owner-control client。Transport route 可以包含该 adapter 完成投递所需的窄 route credential，但不能据此调用 owner/source API。

`HostSessionClient.renewPresence()` 只接受当前 lease 与 host 自己观察到的真实用户 active edge；Core 校验 endpoint/generation 并
生成短 TTL lease。它与 `observeActivity()` 分离，避免把 agent/model activity 或 Wake echo 冒充用户仍在前台。

实际 tarball test 会在仓库外安装 package、用 TypeScript 编译 fake source + fake host、运行 validators，并逐一确认 private
subpaths 返回 `ERR_PACKAGE_PATH_NOT_EXPORTED`。这是一条受支持 API boundary，不是对同机恶意代码的 OS sandbox；安装目录仍应
由 operator 权限保护。

## Generic push

```ts
const client = new SourcePushClient({
  base_url: "http://127.0.0.1:4311",
  source: "ci",
  token: process.env.WAKEBRIDGE_SOURCE_CI_TOKEN!,
});

await client.emit({
  schema_version: EVENT_CONTRACT_VERSION,
  type: "job.completed",
  dedupe_key: "build:1842",
  resource: { uri: "ci://build/1842" },
});
```

client 不接受公网 daemon origin，不把 `source` 写入 body，并验证返回 receipt 的 source 与 event contract。Credential 的创建、
轮换与 daemon 配置见 [source ingress](source-ingress.md)；SDK 不提供 owner API。

## Reference source

`wake-bridge/source/group-chat` 提供只依赖公开 feed contract 的 `GroupChatSourceAdapter` 与 mapper。仓内 conformance fixture 已覆盖
mention/reply mapping、opaque cursor、schema fencing 与 duplicate-page rejection。真实独立 Group Chat feed canary 仍是外部验收项；
在取得实际 feed endpoint、principal credential 与 provider version 之前，不得把 fixture 结果写成 production support。
