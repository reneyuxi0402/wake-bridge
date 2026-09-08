# Reference Host Adapter

这是一个中立的 out-of-process Host Adapter v1 示例，不对应、也不声称支持任何具体 agent 产品。

它演示：

- 以 scoped host credential 登记一个精确 session 与 attention channel；
- 只在本机 loopback 接收固定 Wake envelope；
- 对同一个 `attempt_id` 幂等接受；
- 定期 renew endpoint lease，退出时 close；
- 由 host 在观察到真实用户/会话 edge 时显式上报 presence 与 activity。

唯一需要接入方替换的是 `acceptWake`。这个回调必须把 envelope 交给 `session_ref` 指定的精确 session/task，并且只在
宿主已经接受该 delivery 后 resolve。它如何启动或恢复 session、怎样在 busy 时排队、能否 cold push，以及多久真正
启动 agent，全部属于该 Host Adapter 的能力与 UAT，不由 Wake Bridge Core 推断。

## 运行示例

先把 [host-adapters.json.example](host-adapters.json.example) 复制到私有配置目录，替换
`tested_host_versions` 和 capability 声明，并让 daemon 使用 `--host-adapters` 加载它。Host bootstrap token 与 delivery
route token 必须是两个不同的、至少 32 字符的随机值；不要写进 JSON、shell history 或仓库。

构建 package 后启动示例：

```sh
export WAKEBRIDGE_DAEMON_ORIGIN=http://127.0.0.1:4311
export WAKEBRIDGE_HOST_TOKEN='<host bootstrap token>'
export WAKEBRIDGE_ROUTE_TOKEN='<different route token>'
export WAKEBRIDGE_SESSION_REF='<exact host session id>'
export WAKEBRIDGE_ATTENTION_CHANNEL=default
node examples/reference-host-adapter/adapter.mjs
```

仓库自带的 `acceptWake` 只把 envelope 写到 stdout，因此只证明协议 wiring，不会启动 agent。接入真实宿主时：

1. 用该宿主的公开、精确 session API 替换 stdout callback；
2. 按真实行为填写 capability 与最强 receipt upper bound，不把 HTTP 202 写成 agent seen；
3. 验收 idle、busy、restart、重复 delivery、session close、generation takeover、presence TTL 与 activity watch；
4. 只有在这些 UAT 通过后，才由 adapter 维护者声明该 host/version 的支持等级。

完整协议与安全边界见 [out-of-process Host Adapter runbook](../../docs/operations/out-of-process-host-adapter.md)。
