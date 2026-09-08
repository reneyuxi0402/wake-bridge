# Source-scoped event ingress

状态：current local implementation。该入口适合 loopback producer、webhook sidecar 或同机 job runner；它只授予固定 source 的
event ingest authority，不授予 owner control。

## 1. 初始化一个 Agent Space

```sh
node dist/src/cli.js init \
  --data-dir /absolute/path/to/agent-space \
  --instance-id agent-a \
  --owner-id agent-a-owner \
  --timezone Asia/Shanghai
```

命令创建私有 config 与 DB，不在 stdout 打印 owner token。先检查本地 readiness：

```sh
node dist/src/cli.js doctor \
  --config /absolute/path/to/agent-space/wakebridge.config.json
```

不要把 `wakebridge.config.json` 提交到 git、复制给 source producer 或传给任何 agent host。

## 2. 配置窄 source credential

token 只放环境变量：

```sh
export WAKEBRIDGE_SOURCE_CI_TOKEN="<至少 32 字符的随机 token>"
```

reference file 只保存 env 名与固定 source：

```json
{
  "version": 1,
  "credentials": [
    {
      "id": "ci-primary",
      "source": "ci",
      "token_env": "WAKEBRIDGE_SOURCE_CI_TOKEN"
    }
  ]
}
```

启动 daemon：

```sh
node dist/src/cli.js daemon \
  --config /absolute/path/to/agent-space/wakebridge.config.json \
  --source-credentials /absolute/path/to/source-credentials.json \
  --host 127.0.0.1 \
  --port 4311
```

## 3. Producer 登记 event

```sh
curl --fail-with-body \
  -X POST http://127.0.0.1:4311/v1/ingress/ci/events \
  -H "Authorization: Bearer ${WAKEBRIDGE_SOURCE_CI_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "schema_version": 1,
    "type": "job.completed",
    "dedupe_key": "build:1842",
    "attention_channel_hint": "default",
    "resource": {"uri": "ci://build/1842"},
    "metadata": {"result": "success"}
  }'
```

请求 body 不接受 `source`。Bridge 从已经验证的 token 与 URL path 注入 `source=ci`。同一个 token 不能访问
`/v1/ingress/other/events`，也不能访问 `/v1/inspect`、`/v1/tick` 或其他 owner API。

`POST /v1/events` 仍存在，但它是 owner-admin surface，用于本实例 owner 的手动与管理式操作；不要把 owner token 发给普通
producer。

## 4. 轮换与撤销

source token 不进 DB。安全轮换流程：

1. 为同一个 source 增加第二个 credential id 与新的 token env；
2. 重启 daemon；
3. producer 切换到新 token；
4. 从 reference file/env 删除旧 token并再次重启。

owner config-backed token 使用：

```sh
node dist/src/cli.js owner-token-rotate \
  --config /absolute/path/to/agent-space/wakebridge.config.json
```

轮换后重启 daemon。命令不会打印新 token；若设置了 `WAKEBRIDGE_ADMIN_TOKEN`，必须先 unset。config-backed profile 不允许
CLI/env 改写固定的 instance、owner、DB、timezone 或 admin token，避免同一 credential 被带到另一个 Agent Space。

## Development escape hatch

只有不带任何真实数据的本地 fixture 才可使用：

```sh
node dist/src/cli.js daemon --unsafe-no-auth --host 127.0.0.1
```

它不能绑定 `0.0.0.0` 或其他非 loopback 地址，也不能让 host session 或 scoped source endpoint 绕过各自 credential。
