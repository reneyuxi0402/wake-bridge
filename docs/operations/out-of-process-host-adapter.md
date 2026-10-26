# Out-of-process local Host Adapter protocol

状态：current public Host Adapter Contract。所有具体 host/product support 由 adapter 维护者按 host/version 独立声明；
通用 contract 不代表任意产品受支持；随包的实验性 [Claude online adapter](../../host-adapters/claude-cli/README.md) 有独立且有限的验证范围。

这个协议面向已经有 backend agent session、bridge service 或自建 frontend 的宿主。外部 service 保管宿主身份与内部 credential，
Wake Bridge daemon 只知道一个 opaque session、声明的 capability 与精确 loopback delivery route。

它不是动态 plugin loader。Daemon 不 import 第三方代码，也不把 Core、DB、owner token 或 agent memory 交给 adapter。具体宿主实现仍是
独立 integration；通过该协议不等于该宿主进入 Wake Bridge supported-host matrix。

## 1. Operator 配置

Bootstrap token 只放环境变量：

```sh
export WAKEBRIDGE_EXAMPLE_HOST_TOKEN="<至少 32 字符的随机 token>"
```

reference file 只保存 env 名、固定 host identity、允许的 channel 与诚实 capability：

```json
{
  "version": 1,
  "adapters": [
    {
      "id": "example-host-local",
      "adapter_kind": "example_host",
      "adapter_version": "1.0.0",
      "host_kind": "example_host",
      "tested_host_versions": ["your-host/1.0"],
      "token_env": "WAKEBRIDGE_EXAMPLE_HOST_TOKEN",
      "attention_channels": ["life"],
      "capabilities": {
        "warm_resume": true,
        "cold_push": false,
        "exact_live_route": true,
        "requires_live_binding": true,
        "queue_when_busy": true,
        "session_activity_observable": true
      },
      "receipt_upper_bound": "host_accepted"
    }
  ]
}
```

启动 daemon：

```sh
wakebridge daemon \
  --config /absolute/path/wakebridge.config.json \
  --host-adapters /absolute/path/host-adapters.json \
  --host 127.0.0.1 --port 4311
```

启动前可以用同一 env/file 运行 `wakebridge doctor --config ... --host-adapters ...`。Doctor 只输出 adapter kind/version、host kinds、
experimental tier 与错误摘要，不打印 token；缺失 env、无效 capability 或重复 kind 会令 readiness fail。

当前文件只允许 `experimental` out-of-process adapters。`receipt_upper_bound` 与 `tested_host_versions` 都是必填项：
`receipt_upper_bound` 只能是 `accepted_to_live_pipe` 或 `host_accepted`，HTTP 202 不能声明 `agent_completed`；
`tested_host_versions` 是 adapter 实测过的宿主版本字符串数组。每个 adapter kind 与 credential id 必须唯一。

## 2. 外部 service 生命周期

仓库外 service 从 `wake-bridge/transport` 导入 `HostSessionClient`。它在确认自己的精确 session 后启动一个 credential-free
loopback HTTP origin，并为该进程生成独立 route token：

```ts
import { HostSessionClient } from "wake-bridge/transport";

const client = new HostSessionClient({
  base_url: "http://127.0.0.1:4311",
  host_token: process.env.WAKEBRIDGE_EXAMPLE_HOST_TOKEN!,
  adapter_kind: "example_host",
});

const registration = await client.open({
  session_ref: "opaque-session-id",
  attention_channel: "life",
  route_origin: "http://127.0.0.1:18790",
  route_token: process.env.EXAMPLE_HOST_ROUTE_TOKEN!,
});
const lease = client.lease(registration);
```

Service 周期性 `renew(lease)`，正常退出时 `close(lease)`。重新 open 同一 session 会执行显式 takeover 并递增 binding generation；旧
lease、旧 activity 与旧 delivery correlation 随后 fail closed。Route token 不得复用 owner、任一 host bootstrap 或 source token。

`HostSessionClient` 只有 open/renew/close/presence/activity/wake-echo 方法，不能 inspect、emit、配置 policy、dispatch 或执行 operator retry。

## 3. Delivery endpoint

外部 service 实现：

```text
POST /v1/wakes
Authorization: Bearer <per-process route token>
X-WakeBridge-Delivery-Nonce: <same nonce as body>
Content-Type: application/json
```

Body 使用 `LOCAL_HOST_PROTOCOL_VERSION=1`：

```json
{
  "protocol_version": 1,
  "attempt_id": "att_...",
  "delivery_nonce": "opaque-one-shot-nonce",
  "wake": {
    "schema_version": 1,
    "instance_id": "agent-a",
    "wake_batch_id": "wb_...",
    "attention_channel": "life",
    "claim_refs": [],
    "binding_generation": 3
  }
}
```

使用 `validateLocalHostDeliveryRequest()` 验证 body，并另行比较 nonce header。Wake payload 只有 durable reference 与有限 metadata，
不是 prompt injection channel，也不复制来源正文。

响应语义固定：

- `202`：adapter 声明的 pipe/host 已接收；Core 只写 `transport_accepted`；
- `408 | 425 | 429 | 5xx`：retryable；
- 其他状态：permanent rejection，最终可进入 dead letter；
- response body 被忽略，不进入 receipt、status 或 error message。

Transport 使用有界 timeout、拒绝 redirects，只连接 `127.0.0.1` 或 `::1`。公网 URL、带 userinfo/path/query 的 origin 均在登记前
拒绝。

### 接收不是整轮处理完成

`202` 的前提是声明的精确 session pipe/host 已真实接受 envelope，不是 HTTP handler 已启动，也不是准备稍后尝试注入。
优先调用宿主已有的接收/排队 primitive，在其确认接受后响应；宿主已有可靠接收机制时不需要另建耐久队列。
若现有同步接口要等 agent 完整回答才返回，应先找到真实接收边界，而不是让投递请求等待完整工作，或 catch timeout 后改回 202。
单纯延长 timeout 也不能提供接收证明。

每个 adapter reference 可设置 `timeout_ms`（100–30000 毫秒，默认 5000）；这是等待接受响应的预算，不是等待
agent 完整工作的时限。可重试失败的默认节奏与 `retry_after_ms` 边界见 [operator control](operator-control.md#retryable-transport-failure)。
batch 的 `deadline` 则是合批窗口结束时间，不是本次 HTTP 调用的截止时间；零合批窗口下它可以等于创建时的
`not_before`。接受后的 `ack_timeout_ms` 另管 agent 确认，三者不要互相替代。见 [时间边界对照](operator-control.md#不要混用三种时间边界)。

请求超时/响应丢失时，Core 没有 HTTP 接受证据，但宿主可能已经接收；不要据此断言未投递，也不要自动 ack/consume。
Core retry 会创建新的 `attempt_id`。同 attempt 的进程内去重不足以防止跨 retry 或 adapter 重启后的重复业务投递；
按宿主现有能力使用稳定 wake/claim 标识做幂等处理，并明确其范围，不宣称跨重启 exactly-once。没有可确认的接受机制时，应保留失败/未知证据。
若 host 确实观察到该次 delivery 注入相同 session/generation，可使用下一节的现有 nonce echo；不能凭 handler 收到请求或 agent 正在运行伪造 echo。

## 4. Activity 与更高层 receipt

若 manifest 声明 `foreground_presence_observable`，adapter 必须只在真实用户 active edge 上调用
`renewPresence(lease, { observed_by, observation, ttl_ms? })`。普通模型 activity、进程存活、窗口 focus 与 Wake echo 都不能续
Foreground Presence Lease；Core 会同时核对 capability、endpoint token 与 current binding generation，未声明 observer 的 host
调用会 fail closed。

若 manifest 声明 `session_activity_observable`，外部 service 可以调用 `observeActivity()` 上报真实用户/runner activity，也可以在确认
delivery nonce 确实进入相同 generation 后调用 `consumeWakeEcho()`。Core 会消费这个一次性 nonce，记录独立的
`host_attested` receipt，并把对应当前 attempt 的 batch 收尾为 `seen`；这表示可信 host 已把该 wake 注入目标 session，仍不等于
agent 理解、消费或执行了其中工作。agent 随后显式 `attention_ack` 时，Core 会在同一 `seen` batch 上补记独立的
`agent_seen` receipt；`agent_consumed` / `agent_acted` 仍只能由 agent/harness 对具体 claim 明确记录。

nonce echo 可以和原 transport request 并发发生：若 host 已证明注入，但 HTTP 202 回程超时，Core 以更强的 host 证明收尾，避免
误重投。nonce、endpoint、binding generation 与 attempt 任一不匹配都会 fail closed；不存在可由 adapter 任意指定 batch id 的 ack
入口。

### Agent 的确认顺序与竞态

Agent 实际读到 envelope 后调用 MCP：

```json
{"wake_batch_id":"wb_..."}
```

这是 `attention_ack` 的参数；不要附加 endpoint、generation、nonce 或 source。随后用 agent 自己的授权来源工具读取 resource，
审阅/处理完某条 claim 后才调用 `attention_consume({"claim_id":"ac_..."})`。Adapter 不替 agent 确认理解或完成工作，
也不能为消除告警而自动调用这两个工具。`consumeWakeEcho()` 只是 host 注入证明，不是 `attention_ack` 的替代。

Agent 可能先于 Core 落地 accepted attempt 调用 ack，此时可得到 `409 batch_not_dispatched`。先用
`attention_wake_health({"batch_id":"wb_..."})` 核对同 batch 的接受证据；确认当前投递已被接受后，agent 可重试同一个 ack，成功后重复 ack 是幂等的。
不要吞掉 409，也不要对全部 409 无限重试：`stale_generation` 等 fencing 失败应核对精确 session、当前 binding 与 takeover，
其他状态冲突按实际错误定位，不要用新 session 给旧投递补虚假确认。

自 preview.10 起，Core 默认在接受投递 30 分钟后仍无 agent ack 时标记 `needs_attention` / `ack_timeout`，而不是重新发送已接受的
delivery。该时限可由 operator 配置；合法的迟到 agent ack 或当前 attempt 的 nonce echo 仍可收尾，但 generation fencing 不变；batch 的全部 claim 都被
consume/dismiss/expire 后，Core 也会把它收尾为 `cancelled`。Adapter 的 busy queue、
实际注入与换窗处置仍由 adapter 负责；ack/consume/dismiss 不表示 Core 能撤回 adapter 已接收的消息。

## 5. 支持边界

这项 contract 使自建 backend、CLI harness、frontend 或私有 runtime 可以独立实现 integration。它不使任何具体 runtime
自动成为 Wake Bridge 支持对象。Core 不增加产品私有字段、不扫描 conversation、不读取 runtime DB、不选择最近窗口，也不
承诺换窗后的 agent memory continuity。具体 adapter 必须用自己的真实 host/version 做 conformance 与 field canary。

可运行的中立示例见 [Reference Host Adapter](../../examples/reference-host-adapter/README.md)。
