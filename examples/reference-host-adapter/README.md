# Reference Host Adapter

这是一个中立的 out-of-process Host Adapter v1 示例，不对应、也不声称支持任何具体 agent 产品。

它演示：

- 以 scoped host credential 登记一个精确 session 与一个或多个 attention channel；
- 只在本机 loopback 接收固定 Wake envelope；
- 在同一进程内、最近 1000 个已完成接受记录中去重同一个 `attempt_id`（不是并发请求或跨重启 exactly-once）；
- 定期 renew endpoint lease，退出时 close；
- 由 host 在观察到真实用户/会话 edge 时显式上报 presence 与 activity。

唯一需要接入方替换的是 `acceptWake`。这个回调必须把 envelope 交给 `session_ref` 指定的精确 session/task，并且只在
宿主已经接受该 delivery 后 resolve。它如何启动或恢复 session、怎样在 busy 时排队、能否 cold push，以及多久真正
启动 agent，全部属于该 Host Adapter 的能力与 UAT，不由 Wake Bridge Core 推断。

当前源码支持同一 session 的多 channel：一次完整列表登记，共享一个 endpoint、route token 和 endpoint lease，
每个 channel 保留自己的 binding generation。入口只接受当前登记的 `instance_id`、channel 与 generation，
不匹配时返回 409，不猜测或转投“最近的 session”。channel 是路由与注意力分类，不是安全隔离域。
不同 agent 必须使用各自独立的 daemon、私有配置/数据库和凭据，不能用两个 channel 代替隔离。

这里的“接受”不是“完整 agent 回答已完成”。优先用宿主已有的精确 session 接收/排队 API，确认接受后 resolve，随后才响应 202。
不要把一个等待整轮处理完成的同步 API 直接当作接收 API，更不能在 timeout 后假装接受成功。宿主已有可靠接收机制时，不需要再建第二套耐久队列。
Timeout/响应丢失可能发生在真实接受之后；Core retry 会生成新 `attempt_id`，本示例的内存 Set 无法消除这些重复，也会在进程重启时丢失。
真实 integration 应利用宿主已有幂等能力并如实说明范围。只有确认该次 nonce 已进入相同 session/generation，才能通过
`consumeWakeEcho()` 提交 host attestation；本示例的 stdout callback 没有这种证据，也不调用它。

## 运行示例

先把 [host-adapters.json.example](host-adapters.json.example) 复制到私有配置目录，替换
`tested_host_versions` 和 capability 声明，并让 daemon 使用 `--host-adapters` 加载它。Host bootstrap token 与 delivery
route token 必须是两个不同的、至少 32 字符的随机值；不要写进 JSON、shell history 或仓库。

从源码 checkout 运行时，先构建 package，并在仓库根目录执行下面的命令。已安装 npm package 无需再构建，但必须先进入安装后的
`wake-bridge` package 目录（本地安装例如 `/absolute/path/to/node_modules/wake-bridge`；全局安装可用
`cd "$(npm root --global)/wake-bridge"`），不能直接在任意项目目录运行这个相对路径。

多 channel 自 `0.9.0-preview.21` 提供，需使用同版本 daemon、SDK 与本例；实际发行以同 tag 门禁与产物为准。旧版
`0.9.0-preview.20` 只有旧单 channel 能力，不支持 `attentionChannels` / `WAKEBRIDGE_ATTENTION_CHANNELS`。

Reference adapter 默认监听 `127.0.0.1:4393`；该端口已占用时会报 `EADDRINUSE`。下面显式使用
`WAKEBRIDGE_REFERENCE_HOST_PORT=0`，让 OS 选择空闲 loopback 端口，adapter 会自动登记实际 `route_origin`。

```sh
export WAKEBRIDGE_DAEMON_ORIGIN=http://127.0.0.1:4311
export WAKEBRIDGE_HOST_TOKEN='<host bootstrap token>'
export WAKEBRIDGE_ROUTE_TOKEN='<different route token>'
export WAKEBRIDGE_SESSION_REF='<exact host session id>'
export WAKEBRIDGE_ATTENTION_CHANNEL=default
export WAKEBRIDGE_REFERENCE_HOST_PORT=0
node examples/reference-host-adapter/adapter.mjs
```

新源码的多 channel 示例（替换上面的单 channel 设置，不要同时设置两者）：

```sh
unset WAKEBRIDGE_ATTENTION_CHANNEL
export WAKEBRIDGE_ATTENTION_CHANNELS='["life","work"]'
node examples/reference-host-adapter/adapter.mjs
```

`WAKEBRIDGE_ATTENTION_CHANNELS` 必须是非空、无重复、元素为非空字符串的 JSON 数组，非法 JSON 或同时设置新旧变量会直接报错。
先让 host manifest 的允许 channel 列表包含所有请求的 channel。多路使用一次 `openChannels()` 登记完整列表，
不能循环 `open()`，否则后一次登记可能替换前一次 session/route。定时 renew 与退出 close 对共享 endpoint 各只操作一次。

Agent 动态创建 channel 时，可由 operator 显式将 manifest 的 `attention_channels` 设为 `"*"`；先通过
`attention_channel_create` 创建，才能登记。原数组白名单不会自动扩大。[完整两实例示例](../../docs/operations/multi-agent-channels.md)
说明创建/改名/删除、数量限制和各自 MCP config。显示名改变不影响 ID；删除某一路不会关闭共享 session 的其他路。

续租遇到暂时网络失败会继续使用原 token；若 SDK 的 `remote_code` 是 `endpoint_fenced`，本例置 `leaseLost=true`，
停止续租并拒绝后续投递，日志明确要求关闭当前 adapter、核对目标后显式登记。不会自动抢回路由。
presence/activity 收到明确的 `stale_generation`（该路已迁走）或 `channel_deleted` 时，本例只移除该次调用对应的旧
channel handle 与接收 binding，向调用方原样返回该次错误，并停止该路后续 presence/activity 与投递接受。
迟到的旧 handle 错误不会移除已替换的新 handle。共享 endpoint 的其他路仍可观察、续租和接收；退出 close 仍是
endpoint 级操作，不是只关闭某一路。若所有路均已失效，本例停止续租并拒绝所有投递，但不会盲目 close 可能已被
其他登记使用的 endpoint。与 `endpoint_fenced` 导致整个 endpoint 失效不同，单路失效不影响其余路，也不会自动重新登记或抢回路由。
恢复失效路由需要核对目标后显式登记；其他错误（包括暂时网络失败）不会被当作确定的单路失效。

通过代码嵌入时，旧 `attentionChannel: "life"` 与无目标的观察方法保持兼容；新选项是
`attentionChannels: ["life", "work"]`，不能同时传入新旧选项。多 channel 的真实用户/会话观察必须明确指定目标：

```js
await adapter.noteUserPresence("user-message-one", "work");
await adapter.noteSessionActivity("life:turn-one", "activity_started", "life");
```

省略目标或传入未登记的 channel 会报错，不会把一条观察广播到所有 channel。
Activity ID 在 endpoint + generation 范围去重；同一真实活动确实适用于两路时，分别使用带 channel 前缀的稳定 ID。

保持 daemon 与 adapter 运行，在另一个终端发送第一条协议测试事件：

```sh
wakebridge emit \
  --config /absolute/path/wakebridge.config.json \
  --source manual --type reference.test \
  --dedupe-key reference-first-wake-1 \
  --attention-channel-hint default \
  --resource quickstart://reference-first-wake
```

等待 daemon 下一轮调度、adapter 的 stdout 打印 envelope 后，再检查 receipt：

```sh
wakebridge receipts --config /absolute/path/wakebridge.config.json
```

本例 manifest 与 session 使用 `default` channel，测试事件也必须一致。Quickstart 通用示例使用 `life`；若直接沿用它，
应把事件改为 `default`，或把 manifest、session 和事件三处都改为 `life`，否则 batch 会保持 `waiting_for_endpoint`。
若运行上面的多 channel 示例，则把测试事件的 `--attention-channel-hint` 改成 `life` 或 `work`，与登记和 manifest 保持一致。
stdout envelope 与 `transport_accepted` 只表示协议投递通过；本例没有真实 agent，因此预期没有 `agent_seen` 或 `agent_consumed`。

仓库自带的 `acceptWake` 只把 envelope 写到 stdout，因此只证明协议 wiring，不会启动 agent。接入真实宿主时：

1. 用该宿主的公开、精确 session API 替换 stdout callback；
2. 按真实行为填写 capability 与最强 receipt upper bound，不把 HTTP 202 写成 agent seen；
3. 验收 idle、busy、restart、重复 delivery、session close、generation takeover、presence TTL 与 activity watch；
4. 只有在这些 UAT 通过后，才由 adapter 维护者声明该 host/version 的支持等级。

接收、注入与 agent 处理是不同层。Agent 真正读到 wake 后才调用 `attention_ack({"wake_batch_id":"wb_..."})`，
用自己的工具回读权威来源，处理对应 claim 后才 `attention_consume({"claim_id":"ac_..."})`。不要在 adapter 中自动代 ack/consume。
若 ack 抢在 accepted 写入前得到 `409 batch_not_dispatched`，确认同 batch 已接受后可重试；这不意味着所有 409 都应无限重试，
generation/session fencing 应按实际错误排查。完整说明见 runbook 的确认顺序。

完整协议与安全边界见 [out-of-process Host Adapter runbook](../../docs/operations/out-of-process-host-adapter.md)。
