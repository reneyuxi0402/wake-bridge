# Personal policy 文件与控制面

状态：current local implementation（WB-MVP-002）。本页只描述 public policy v1；Core 中的迁移类型不扩大支持矩阵。

## 1. 写一个完整 policy 文件

policy 文件是一个 owner-authored snapshot，每个 id 在同一文件中只能出现一次，并必须有显式 default：

```json
{
  "schema_version": 1,
  "policies": [
    {
      "id": "ci-failures",
      "version": 1,
      "order": 100,
      "match": {"source": "ci", "metadata.status": "failed"},
      "delivery": {
        "mode": "immediate",
        "quiet_hours_policy": "bypass",
        "foreground_presence_policy": "defer"
      },
      "batch": {"coalesce_by": "metadata.project", "max_events": 20, "window_ms": 5000},
      "target": {"attention_channel": "engineering"},
      "reason_code": "ci_failed"
    },
    {
      "id": "default",
      "version": 2,
      "enabled": true,
      "order": -1000000,
      "match": {},
      "delivery": {"mode": "suppress"}
    }
  ]
}
```

内置 bootstrap rule 已占用 `default@1`，所以第一次安装 owner default 用 version 2。之后改变任何内容都必须同时升 version。
同版本内容完全相同可安全重跑；同版本内容不同或安装低于现有最高版本会返回 conflict。

当前 delivery mode 只有 `immediate`、`scheduled` 与 `suppress`。scheduled 使用本实例 timezone：

```json
{"mode":"scheduled","scheduled_local_time":"08:30"}
```

`debounce`、`digest`、`max_delay_ms`、`prefer_transport`、`busy_behavior` 当前会被拒绝。`resume_spread_ms` 与
`default_action` 也不接受为 quiet-hours config。不要根据历史 DB row 推断它们已经受支持。

## 2. 安装与查看

```sh
node dist/src/cli.js policy install \
  --config /absolute/path/to/agent-space/wakebridge.config.json \
  --file /absolute/path/to/policies.json

node dist/src/cli.js policy list \
  --config /absolute/path/to/agent-space/wakebridge.config.json
```

list 同时返回 append-only `policies` 历史、每个 id 的 `active` 最高版本和 `status`。`new_event_matching` 可能是
`eligible | disabled | historical | legacy_unsupported`；最后一种表示升级前残留 row 只供历史解释，不会匹配新事件。
要停用一个规则，安装更高且
`"enabled": false` 的版本；系统不会回退到旧 enabled 版本。文件中没有出现的已有 id 不会被删除或自动停用。

## 3. Test 与 preview

下面的命令只读取当前安装规则：

```sh
EVENT='{"type":"job.completed","dedupe_key":"dry-run-1","resource":{"uri":"ci://build/1842"},"metadata":{"status":"failed","project":"demo"}}'

node dist/src/cli.js policy test \
  --config /absolute/path/to/agent-space/wakebridge.config.json \
  --source ci --event "$EVENT"

node dist/src/cli.js policy preview \
  --config /absolute/path/to/agent-space/wakebridge.config.json \
  --source ci --event "$EVENT"
```

加 `--file /path/to/candidate.json` 可在安装前评估 candidate。test 返回 matched rule；preview 还返回当前求值时间、是否
suppress、Claim eligibility/expiry/channel、quiet/presence gate 与 batch key。两者都不登记 Event，不创建 Claim/Batch。

## 4. Owner API

所有 endpoint 都要求 owner token，不能把它交给 source producer：

```sh
curl --fail-with-body http://127.0.0.1:4311/v1/policies \
  -H "Authorization: Bearer ${WAKEBRIDGE_ADMIN_TOKEN}"

curl --fail-with-body -X POST http://127.0.0.1:4311/v1/policies/install \
  -H "Authorization: Bearer ${WAKEBRIDGE_ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  --data-binary @/absolute/path/to/policies.json

curl --fail-with-body -X POST http://127.0.0.1:4311/v1/policies/preview \
  -H "Authorization: Bearer ${WAKEBRIDGE_ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"source":"ci","event":{"type":"job.completed","dedupe_key":"dry-run-api","resource":{"uri":"ci://build/1842"}}}'
```

若要 preview 未安装文件，把完整对象放在 request 的 `policy_file` 字段。source-scoped ingress、host bootstrap 与 endpoint
lease credential 对这些 API 一律 unauthorized。
