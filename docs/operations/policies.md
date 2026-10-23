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

批次合并只在同 policy/version、channel、coalesce key 的开放等待窗口内发生，并受 `max_events` 限制。
`window_ms: 0` 表示不等待合并；即使多条 claim 同时到期，也不保证只形成一个 batch。需要合并时显式配置短窗口，
不能把多个 batch 直接判成重复事件。合并投递仍保留各条 claim，不会根据相同 resource 自动只留最新任务。
Core 可取消尚未取得投递 lease 且工作已终结的 batch，但不能撤回在途请求或 adapter 已接受的消息；
不能把“尚未返回 accepted”当作“仍可撤回”。self-commitment 使用自己的 `self_commitment` policy identity，
未安装该 policy 时使用内建零窗口规则；修改名为 `default` 的 policy 不会自动改变它的分批。

当前 delivery mode 只有 `immediate`、`scheduled` 与 `suppress`。scheduled 使用本实例 timezone：

```json
{"mode":"scheduled","scheduled_local_time":"08:30"}
```

`debounce`、`digest`、`max_delay_ms`、`prefer_transport`、`busy_behavior` 当前会被拒绝。`resume_spread_ms` 与
`default_action` 也不接受为 quiet-hours config。不要根据历史 DB row 推断它们已经受支持。

quiet hours 配置在私有实例文件 `wakebridge.config.json` 中，不在 policy 文件中。例如，加入：

```json
{
  "quiet_hours": {
    "timezone": "Asia/Shanghai",
    "windows": [{"start": "22:00", "end": "08:00"}]
  }
}
```

修改后重启读取该实例文件的进程。省略 `quiet_hours`、设为 `null` 或使用空 `windows` 均表示不启用。
`quiet_hours.timezone` 省略时使用实例顶层 `timezone`；提供时仅覆盖 quiet hours 的时区，不改变 scheduled policy 的时区。
时间必须是 `HH:MM`；窗口包含开始、不包含结束，支持跨午夜。无效时区、无效窗口，以及单个或多个窗口合起来覆盖全天，
都会在打开数据库前拒绝。普通 policy 默认 `defer`，仅显式 `quiet_hours_policy: "bypass"` 绕过该 gate。

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

加 `--file /path/to/candidate.json` 可在安装前评估 candidate，使用与安装一致的有效规则集合：文件中的新版本覆盖同 id 的旧版本，
未列出的已安装规则仍保留。要停用旧规则，须在文件中提供更高版本并设 `enabled: false`，仅删掉文件中的规则不会停用它。
版本冲突或回退会和 install 一样报错；test/preview 不写入规则、事件或 claims。test 返回 matched rule；preview 还返回当前求值时间、是否
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
