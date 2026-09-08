# Out-of-process local Host Adapter protocol

状态：current public Host Adapter Contract。所有具体 host/product support 由 adapter 维护者按 host/version 独立声明；
Wake Bridge package 本身不内置产品支持。

这个协议面向已经有 backend agent session、bridge service 或自建 frontend 的宿主。外部 service 保管宿主身份与内部 credential，
Wake Bridge daemon 只知道一个 opaque session、声明的 capability 与精确 loopback delivery route。

它不是动态 plugin loader。Daemon 不 import 第三方代码，也不把 Core、DB、owner token 或 agent memory 交给 adapter。具体宿主实现仍是
仓库外 integration；通过该协议不等于该宿主进入 Wake Bridge supported-host matrix。

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

当前文件只允许 `experimental` out-of-process adapters。`receipt_upper_bound` 只能是 `accepted_to_live_pipe` 或
`host_accepted`；HTTP 202 不能声明 `agent_completed`。每个 adapter kind 与 credential id 必须唯一。

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

## 4. Activity 与更高层 receipt

若 manifest 声明 `foreground_presence_observable`，adapter 必须只在真实用户 active edge 上调用
`renewPresence(lease, { observed_by, observation, ttl_ms? })`。普通模型 activity、进程存活、窗口 focus 与 Wake echo 都不能续
Foreground Presence Lease；Core 会同时核对 capability、endpoint token 与 current binding generation，未声明 observer 的 host
调用会 fail closed。

若 manifest 声明 `session_activity_observable`，外部 service 可以调用 `observeActivity()` 上报真实用户/runner activity，也可以在确认
delivery nonce 确实进入相同 generation 后调用 `consumeWakeEcho()`。Wake echo 只建立可信 `wake_started` activity，不冒充
`agent_seen`；batch 的 seen/consumed/acted 仍需 agent/harness 通过既有明确 acknowledgement 完成。

## 5. 支持边界

这项 contract 使自建 backend、CLI harness、frontend 或私有 runtime 可以独立实现 integration。它不使任何具体 runtime
自动成为 Wake Bridge 支持对象。Core 不增加产品私有字段、不扫描 conversation、不读取 runtime DB、不选择最近窗口，也不
承诺换窗后的 agent memory continuity。具体 adapter 必须用自己的真实 host/version 做 conformance 与 field canary。

可运行的中立示例见 [Reference Host Adapter](../../examples/reference-host-adapter/README.md)。
