# 多 Agent 与单 Agent 多 Channel

状态：本页描述 `0.9.0-preview.21` 的 `openChannels`、channel 管理与 schema 9；实际发行以同 tag 门禁与产物为准。不要直接对已安装的 preview.20 调用新方法。
部署仍是每个 Agent Space 一个 daemon/SQLite；不提供共享多租户服务，也不承诺具体宿主已经完成多 agent 接入。

## 集成方向与产品责任

宿主或集成方通过公开契约接入 Wake Bridge；Bridge 不以某个宿主已有 adapter 的内部实现定义自己的接口或路线。
多 agent 的身份隔离、单 agent 的多路注意力、可靠投递与可核对的确认，是 Bridge 自身的产品能力。

Bridge 交付公开协议与 SDK、声明清楚的生命周期和失败语义、中立 reference adapter、契约测试及安装诊断文档。
集成方负责将自己的身份、会话与执行机制映射到这些契约，验证目标归属，并如实声明和验证宿主能力。
Bridge 不要求宿主采用某种会话数据库、内部队列、流式协议或模型引擎，也不把它们的细节放进 Core。

接入时若暴露问题：违反已有 Bridge 契约的缺陷由 Bridge 修；宿主侧映射与实现问题由集成方修；
确需新增能力时，先说明宿主无关的使用场景、现有契约缺口与维护代价，再决定是否扩展。
某个宿主的接入进度不替代 Bridge 通用能力的验收，也不是 Bridge 产品交付的前置条件。

## 身份与路由分开

| 对象 | 含义 | 谁决定 |
| --- | --- | --- |
| Agent Principal / Agent Space | 独立 agent 的授权与持久状态范围 | operator 配置；一个实例固定一个 owner |
| attention channel | 同一 agent 内的注意力路由，例如 `life`、`work` | agent 的 policy 与显式 binding |
| endpoint / session | adapter 验证的精确宿主会话 | 宿主选择并登记，Bridge 不猜最近窗口 |

A、B 都可以有 `life` 和 `work`，同名不意味着共享消息。channel 不是凭据或隐私隔离域；不能用两个 channel 冒充两个 agent 的隔离。
会话是否属于配置的 agent 必须由 adapter 核实，Bridge 不读取宿主会话库验证身份。
Bridge 只做事件持久化、policy 调度、准确投递与分层确认。群聊成员、@ 谁、模型引擎、忙时执行、审批、记忆与协作属于来源/宿主。
来源若要通知两位 agent，须分别向获授权的 Space 登记事件；没有默认广播或跨 Space consume。

## Agent 自己管理 Channel

`life`、`work` 都只是示例，不是 Bridge 保留的类别。Agent 在自己的 MCP 中使用：

| 工具 | 参数 | 作用 |
| --- | --- | --- |
| `attention_channel_create` | `{"name":"研究","attention_channel":"research"}` | 创建；可省略 ID，由 Bridge 生成 |
| `attention_channel_list` | `{}` | 列出有效 channel 与 `max_channels` |
| `attention_channel_rename` | `{"attention_channel":"research","name":"论文与实验"}` | 改显示名，不改变路由 ID 或历史 |
| `attention_channel_delete` | `{"attention_channel":"research","force":false}` | 默认拒绝删除使用中的路；明确强制删除后停止该路唤醒，重复删除幂等 |

同样的管理操作可通过 owner HTTP `GET/POST /v1/channels` 与 `PATCH/DELETE /v1/channels/:id` 完成。
Source token、host bootstrap token 与 endpoint token 没有这些管理权限。改名只接受 `name`，不会重写已安装 policy。
新建时手填 ID 不能带首尾空白，避免与宿主登记的归一化产生不同路由；显示名会去掉首尾空白。
名字允许重复；路由仍引用稳定 `attention_channel`。删除的 ID 不复用，以防旧请求恢复旧路由；可以用同一个名字创建一个新 ID。
旧调用省略目标时仍沿用 `default` 这一兼容路由 ID，它不是不可删除的系统 channel；需要别的默认路由时由 agent 的 policy 明确选择。

实例配置的 `max_channels` 默认 32，可设为 1–1024 的整数。上限只计有效 channel，不由 agent 工具扩大。
显式创建超过额度返回 `429 channel_limit_reached`；已有数量超过新上限时不裁剪数据，仍可读、改名和删除。
旧 API 的首次使用入口保持兼容：policy 匹配后的事件目标、owner schedule/watch、显式授权的 host 登记和 binding 可隐式创建
channel，但受相同的原子额度检查。只有来源 emit 的隐式目标额度不足会将事件持久化为 `suppressed`、reason 为
`channel_limit_reached`，不创建 channel 或 claim；source cursor 可继续推进。显式 create、schedule、watch 和 host 注册
仍返回 429，不转成成功。删除后这些入口不能自动恢复它；来源 emit 若仍命中已删 ID，同样落盘为 suppressed、reason 为
`channel_deleted`，不阻塞 cursor 或改投另一路。释放额度不会自动补发；原 `(source, dedupe_key)` 重放仍返回原 suppressed 事件。
应更新 policy/来源配置，并由调用方明确决定新的有效事件，不自动重写历史。

默认删除遇到当前 binding、启用的 activity watch，或当前启用且受支持 policy 的字面 channel target 时，返回
`409 channel_in_use` 和安全 `usage`：`binding` 为 `null` 或 `{endpoint_id,generation}`，`activity_watch` 为 boolean，
`policies` 为 `{id,version}` 列表。不扫描任意表达式或把历史/禁用 policy 当作当前使用。
先查看并处理这些引用，或明确承担终止该路的后果：agent MCP 使用
`attention_channel_delete({"attention_channel":"research","force":true})`，owner HTTP 使用
`DELETE /v1/channels/research`、JSON body `{"force":true}`。`force` 只接受严格 boolean，省略为 false，字符串/数字/null 均无效。
删除不可恢复，ID 不可复用；没有 channel restore API，也不因之后还有来源事件而复活。

删除在一个事务里解除该路 binding、清 presence、停止 activity watch、取消未完成唤醒并撤销待确认 nonce；
保留事件去重记录与历史 receipt，不把取消伪装成 agent 已处理。共享 session 的其他 channel、route token 与续租不受影响。
已经发给宿主的在途请求无法追回；晚返回不会把已取消工作恢复为可投递。Host 如需停止已经开始的执行，仍须由宿主自己处理。
ack/consume 等后续操作若收到 `channel_deleted`，应视作终态、检查 status 后停止盲目重试；删除不等于 agent 已消费。
每个 channel 最多绑定一个精确 session；换 session 是显式 takeover，不是同时广播到两个 session。

旧库须先走 [离线显式 upgrade](release-lifecycle.md#显式-upgrade)，从 schema 6/7/8 升到 9；先备份，不直接让 daemon 自动迁移。
升级回填已有 channel 引用，并保留其原路由 ID；不因默认上限删除旧 channel。

## 两个 Agent 的部署

复用同一份 package，但分别初始化；下面是本机 operator 示例，不会自动启动 service：

```sh
wakebridge init --data-dir /absolute/private/agent-a --instance-id agent-a --owner-id agent-a --timezone UTC
wakebridge init --data-dir /absolute/private/agent-b --instance-id agent-b --owner-id agent-b --timezone UTC
```

为两者分别准备 Host Adapter manifest。固定路由可保留 `attention_channels: ["life", "work"]`。
需要 agent 动态创建 channel 时，operator 可以显式配置 `attention_channels: "*"`，授权该 host 使用本 Space 所有已登记的有效 channel。
这不跨 Space，也不允许 wildcard host 自己创建未知 channel；必须先由 agent 创建，再由宿主选择 session 并登记。
旧数组不会自动扩大权限。下面是一份完整的中立动态 manifest，两实例分别保存、分别载入自己的 token：

```json
{
  "version": 1,
  "adapters": [{
    "id": "reference-local",
    "adapter_kind": "reference_host",
    "adapter_version": "1.0.0",
    "host_kind": "reference_host",
    "tested_host_versions": ["reference-demo/1"],
    "token_env": "WAKEBRIDGE_HOST_TOKEN",
    "attention_channels": "*",
    "capabilities": {"warm_resume": true, "exact_live_route": true, "requires_live_binding": true},
    "receipt_upper_bound": "host_accepted"
  }]
}
```

各自配置独立 bootstrap token、delivery route token、source credential 和 connector cursor。JSON 只保存 token 的环境变量名，
不写 secret；按 [Host Adapter runbook](out-of-process-host-adapter.md) 加载。不要复制有真实凭据的目录来创建另一位 agent。
在两个进程环境中分别载入相应凭据，然后启动：

```sh
# 终端 / 服务 A
wakebridge daemon --config /absolute/private/agent-a/wakebridge.config.json --host 127.0.0.1 --port 4311 --host-adapters /absolute/private/agent-a/host-adapters.json
# 终端 / 服务 B
wakebridge daemon --config /absolute/private/agent-b/wakebridge.config.json --host 127.0.0.1 --port 4312 --host-adapters /absolute/private/agent-b/host-adapters.json
```

所有端口都必须空闲；测试可用 `--port 0` 获取动态端口，adapter 应使用 daemon 实际报告的地址。
持久服务沿用 [macOS](release-lifecycle.md#macos-launchagent) / [Linux](linux-vps.md) 安装步骤，分别传 config、端口与配置文件。
service 名由 instance ID 派生；上述名字分别为 `io.wakebridge.agent-a`、`io.wakebridge.agent-b`（Linux 加 `.service`）。
使用可区分的 ASCII instance ID，并检查安装命令返回的精确路径，避免名称归一化冲突导致安装拒绝。不要共享 DB 或监听端口。

各 owning agent 的 MCP 启动参数必须分别指向自己的 config，例如 A 为
`wakebridge mcp --config /absolute/private/agent-a/wakebridge.config.json`，B 为相应的 `agent-b` 路径。
两边各通过上面的 create 工具创建 `life`、`work`，然后分别启动 reference adapter，设置各自 daemon origin、
凭据与精确 session ref，并传 `WAKEBRIDGE_ATTENTION_CHANNELS='["life","work"]'`、`WAKEBRIDGE_REFERENCE_HOST_PORT=0`。
reference 只接收并打印合成 envelope，不执行模型；完整启动说明见 [reference 示例](../../examples/reference-host-adapter/README.md)。

Source 账号权限由每位 agent 的接入分别确定。不要默认共享 provider secret，也不要让多个 poller 竞争会标读/消费的上游；
Bridge 内 cursor 独立不代表上游副作用已经隔离。同机独立进程并非 OS 安全沙箱，CPU、磁盘和同一 OS 用户权限仍然共享。

## 同一 Agent：不同 Channel 指向不同 Session

每个精确 session 使用已有 `client.open({ session_ref, attention_channel, route_origin, route_token })`，保留各自 lease。
`life` 与 `work` 有不同 session ref、endpoint 和接收 route；关闭 life 的 endpoint 不关闭 work。
某一路无 binding/lease 时留在等待状态，不改投另一条路；当前没有跨 channel 自动 fallback。

## 同一 Agent：同一 Session 接收多个 Channel

不要针对同一个 session 连续调用两次单 channel `open`；endpoint 租约属于整个 session，不是每个 channel 各一份。
使用新源码 SDK 一次声明完整集合：

```js
import { HostSessionClient } from "wake-bridge/transport";

// 参数由当前 agent 的私有 adapter 配置和已监听的精确宿主会话提供。
const client = new HostSessionClient({
  base_url: daemonOrigin,
  host_token: hostBootstrapToken,
  adapter_kind: "reference_host",
});
const registration = await client.openChannels({
  session_ref: exactSessionRef,
  attention_channels: ["life", "work"],
  route_origin: listeningRouteOrigin,
  route_token: deliveryRouteToken,
});
const leases = new Map(client.leases(registration).map(lease => [lease.attention_channel, lease]));

// 全部 channel 共享 endpoint/token；一个续租定时器足够。
await client.renew(leases.get("life"));
// host 真正观察到 work 的用户活动时，只更新 work，不替 life 续 presence。
await client.renewPresence(leases.get("work"), { observed_by: "host_user_edge" });
// 会话真正结束时关闭整个 endpoint，会同时影响 life/work。
await client.close(leases.get("life"));
```

- 完整集合非空、无重复，全部 channel 均在 host credential 授权范围内。单/多 channel 字段不能混用。
- 一次登记以一个事务更新 endpoint 和全部 binding；非法请求或写入失败不留下部分绑定、半更新 route 或已旋转 token。
- 每条 binding 有自己的 generation；presence、activity、nonce echo 使用对应 channel 的 lease handle。
- `observeActivity` 的 `observation_id` 仍在 endpoint + generation 范围内去重，不是按 channel 去重。同一个真实活动边沿若确实
  适用于两个 channel，应分别用稳定 ID（如 `life:event-42`、`work:event-42`）上报；复用同一 ID 会被视为重复，不续另一 channel 的 watch。
- 同 session 重登必须包含目前仍绑定到该 endpoint 的全部 channel。遗漏返回 `409 host_session_channels_required`，不偷偷改共享 route。
- 完整重登会旋转 endpoint token，并推进所登记 channel 的 generation；adapter 必须替换全部旧 handle。
- 单独迁移 work 时，将 work 显式登记到另一个精确 session；life 仍绑定原 endpoint。不是先 close 共享 endpoint 再搬 work。
- `renew` / `close` 是 endpoint 级操作，不是单 channel 解绑。bootstrap 形式也须有全部已绑定 channel 的权限。
- 若不再需要某一路，用 agent 的 `attention_channel_delete`，先检查 `channel_in_use`，确认后可显式 `force:true`，不先 close 共享 endpoint。删除后重新登记剩余集合即可；
  adapter 侧应停止报告已删 channel 的 presence/activity，使用剩余有效 handle，不把 `channel_deleted` 当作临时断连。
- 来源 channel hint 仍由 policy 决定是否采纳，不是直接控制路由的授权。

接收方必须检查固定 envelope 的 instance/channel/generation 与当前登记一致。接受、注入、agent 看见和消费仍是不同证据；
不改变 HTTP 202、超时重试、nonce echo 与 ack/consume 的语义，也不提供 exactly-once。
可运行示例见 [Reference Host Adapter](../../examples/reference-host-adapter/README.md)。随包 Claude CLI online adapter 仍仅支持单 channel，
通用 SDK 的扩展不自动扩展其支持范围。

SDK 失败仍保留兼容的 `code`，服务端的有界错误原因另见 `error.remote_code`，例如 `endpoint_fenced`、
`host_session_channels_required`、`channel_deleted`。不要解析 message 或无差别重试所有 409。
Reference 在 endpoint token 被 fencing 后设 `leaseLost=true`，停止续租与接受投递，要求显式重登；临时网络失败继续用原 token 续租，
不会自动抢回别的宿主已接管的路由。自然 lease 到期后有效 token 仍可 renew，不能把到期与 token 被替换混为一谈。
presence/activity 收到确定 `stale_generation` / `channel_deleted` 时，reference 仅移除对应旧 handle 和接收 binding；
其余路可继续观察、续租与接受。迟到旧错误不影响替代 handle；全部路失效时停止续租并拒绝投递，不盲目关闭可能被其他登记使用的 endpoint。

## 来源抑制诊断

`wakebridge status` 与 owner `GET /v1/status` 提供 `ingest.channel_suppressions`：`counts` 分别统计
`channel_deleted` / `channel_limit_reached`，`latest_at` 为最近时间或 null，`recent` 至多20条，包含
`event_id`、`source`、`matched_policy_id`、`matched_policy_version`、`reason`、`at`，不包含 payload。
它基于仍保留的 ingest 抑制历史，不是终身计数，retention 后会减少；删除既有工作产生的取消不计入。
它不单独改变 health / attention_required，不新增永久 incident。
MCP `attention_event_list` 和 `attention_wake_health.events` 的紧凑事件增加 `suppression_reason`，仅上述两种原因或 null。
原始 `attention_channel_hint` 不证明最终路由；应结合 matched policy 与 suppression reason 排查。

## 验收与排查

对 A/B 各自的 config 分别运行 `wakebridge status --config ...`，由各自 owning agent 调用 `attention_binding_status`、
`attention_wake_health` 查看本 Space 的 life/work。分别记录 instance、batch、channel、endpoint/generation 与 receipt 类型，
不公开 token、nonce、来源正文或完整日志。
至少验证双 agent 同名 channel、同/不同 session、一路换窗/关闭/超时/presence 延后、另一实例退出与重启、错凭据/nonce 的拒绝。
中立 fixture 的模拟消费不是模型端到端或具体宿主支持证明。
