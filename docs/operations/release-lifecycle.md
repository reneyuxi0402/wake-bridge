# Release install, upgrade, backup, and rollback

状态：`0.9.0-preview.21` 未发布候选 runbook，npm 基线仍为 preview.20；本地验收与发布门禁分开记录，见 [验证口径](../verification.md)。Wake Bridge 发布包只支持 Host Adapter Contract；
完成本页不等于任何具体 agent 产品获得兼容性或时延保证。

## 支持矩阵

| 项目 | 当前承诺 |
| --- | --- |
| package | `wake-bridge`，npm dist-tag `latest` |
| license | Apache-2.0 |
| OS | macOS；带 systemd user service 的 Linux |
| CPU | macOS arm64/x64；Linux x64 已验证，Linux arm64 尚未验证 |
| Node.js | 20 或更高 |
| SQLite CLI | 3.35.0 或更高，必须支持 `-json` 输出与 JSON1 函数（`json_each`） |
| topology | 每个 Agent Space 一个本地 instance / SQLite DB |
| schema | 已发布 preview.20 为 8；未发布 preview.21 候选为 9，显式升级支持 6、7、8 → 9 |
| host integration | out-of-process Host Adapter Contract v1；捆绑实验性 Claude CLI online adapter，不代表通用产品支持 |

自 `0.9.0-preview.9` 起，npm manifest 允许 macOS 与 Linux，并按平台生成 LaunchAgent 或 systemd user unit。Linux 实机证据来自
Ubuntu 22.04 LTS x64 / systemd 249 的连续两轮 reboot canary；Ubuntu 24.04 x64 当前只有 CI 证据。
SQLite 是外部 runtime prerequisite，不由 npm 安装；现有 endpoint renewal 与 dead-letter retry 使用的
[`RETURNING` 语法要求 3.35.0+](https://www.sqlite.org/lang_returning.html)。只有 JSON output 支持还不够。JSON1 自 3.38.0 起默认内置，
更早的版本需在编译时启用。macOS 自带版本与 CI 使用的 Ubuntu 24.04 均满足；其他环境以 `release-preflight` 的 `json_functions` 为准。

Linux 的安装、linger、journal 与回滚步骤见 [Linux VPS runbook](linux-vps.md)。Wake Bridge、
Source Connector、Host Adapter 与 agent runtime 必须同机；跨机器 Remote Host Adapter 不在该 profile 内。

## 从 tarball 干净安装

发布前先生成并审计 tarball：

```bash
npm pack --json
npm install --global ./wake-bridge-0.9.0-preview.21.tgz
wakebridge release-preflight
```

正式发布到 npm 后，等价安装命令是：

```bash
npm install --global wake-bridge
```

## Maintainer release

公开版本由 GitHub tag 驱动的 release workflow 发布。发布前必须同时提交版本号与
`docs/releases/<version>.md`，并确保 tag 精确等于 `v<version>`。推送 tag 后，workflow 会在
同一 tag commit 上调用 `.github/workflows/ci.yml`，在 macOS / Ubuntu 24.04 × Node 20 / 24 四组 runner 上
重新执行 typecheck 与全量测试。四组全部成功后，才运行打包与发布 job：`npm pack` 后通过 npm Trusted Publishing/OIDC
把同一个 tarball 发布到 `latest`，随后创建带 tarball 和 SHA-256 清单的 GitHub pre-release。任一验证失败或取消都会阻止发布，
不依赖另一次 main CI 的完成时序。测试 job 只有 contents read 权限，发布 job 才持有 release 写权限与 OIDC。

此门禁为 preview.18 发布后的修复；已发布的 preview.18 使用旧流程，不改写其 tag 或 tarball。
确认 package.json 已更新为下一未发布版本，且对应说明与代码均已提交，再执行：

```bash
release_tag="v$(node -p 'require("./package.json").version')"
git tag "$release_tag"
git push origin "$release_tag"
```

打 tag 之前，先把仍写着上一个版本号的“当前版本”标注一并更新（README、quickstart、protocol、architecture、public SDK、
operator control、Linux runbook、verification 与 release lifecycle 示例）：

```bash
git grep -n "0.9.0-preview.<上一个版本>" -- README.md docs ':!docs/releases'
```

只记录历史事实的句子（例如“自某版本起”）保持原样。

发布链路不保存长期 npm token。workflow 文件固定为 `.github/workflows/publish.yml`；npm package 的
trusted publisher 必须限制到 `reneyuxi0402/wake-bridge` 和这个文件，并允许 direct publish。

初始化一个私有 Agent Space：

```bash
wakebridge init \
  --data-dir "$HOME/Library/Application Support/WakeBridge/default" \
  --instance-id default \
  --owner-id your-agent \
  --timezone Asia/Shanghai

wakebridge release-preflight \
  --config "$HOME/Library/Application Support/WakeBridge/default/wakebridge.config.json"
```

`init` 生成 mode-600 config、owner credential 与 SQLite DB，不在 stdout 打印 token。它不会覆盖已有 config/DB。

## macOS LaunchAgent

安装真实 profile，但不替 operator 静默启动进程：

```bash
wakebridge service install \
  --config "$HOME/Library/Application Support/WakeBridge/default/wakebridge.config.json" \
  --port 4311
```

返回值包含精确 `plist_path`、`load_command` 和 `unload_command`。plist 只保存 Node、已安装 CLI、配置文件路径、loopback
host/port 与日志位置，不复制 owner token。可选 `--host-adapters`、`--connectors`、`--source-credentials` 会将绝对路径传给 daemon；
`--environment-file` 由 Node 在启动时读取，适合放置 host/source 所需环境变量，内容不会复制进 plist，也不会作为 shell 脚本执行。
环境文件与 source credential 文件须为 group/other 不可读的普通文件。macOS 使用环境文件需 Node.js 20.6.0+，
格式参见 [Node 的 env-file 说明](https://nodejs.org/download/release/latest-v20.x/docs/api/cli.html#--env-fileconfig)。
修改环境文件后需重启服务才生效；已经存在的进程环境变量优先。核对后执行返回的 `launchctl bootstrap ...`，再检查：

同机多个 Agent Space 必须为每个 instance 选择不同 loopback port；profile 不会猜测或自动抢占已有端口。

```bash
curl --fail -H "Authorization: Bearer $WAKEBRIDGE_ADMIN_TOKEN" http://127.0.0.1:4311/health
wakebridge doctor --config "$HOME/Library/Application Support/WakeBridge/default/wakebridge.config.json"
```

同一 instance 不允许覆盖已有 plist。升级 package 后 plist 的全局安装路径保持不变；若 npm 安装布局改变，先 bootout、
`service uninstall`，再由新 bin 重装 profile。

## Linux systemd user service

Linux 上同一个 `wakebridge service install` 写入 instance-scoped user unit，并返回分开的 daemon-reload、enable、start、stop 与
disable 命令。unit 使用 `UMask=0077`、`Restart=on-failure`，输出进入 journal；它固定监听 `127.0.0.1`，不会自行开放公网端口。

可选的 `--environment-file` 与 `--source-credentials` 必须指向 group/other 不可读的普通文件。unit 只保存文件路径，不复制
token；`--host-adapters`、`--connectors` 与 `--source-credentials` 则把相应绝对路径交给 daemon。systemd user manager 的
linger 是 operator/OS 权限动作，CLI 不替用户执行。

## 一致性备份

daemon 可运行时执行 online SQLite snapshot：

```bash
wakebridge backup \
  --config /absolute/path/wakebridge.config.json \
  --output /absolute/path/backups/pre-upgrade-0.9.0-preview.9
```

输出目录必须事先不存在。命令使用 SQLite `VACUUM INTO` 生成一致性 DB，随后执行 `quick_check`，写入 mode-600
`manifest.json` 与 SHA-256。备份只包含 durable DB 和无 secret manifest，不复制带 owner token 的 config；config 应由 operator
另行放进加密 secret backup。不要用文件复制代替 online snapshot，也不要只复制 `-wal` 或 `-shm`。

## 显式 upgrade

普通 `daemon`、`inspect`、MCP 或其他 Core 打开路径看到旧 schema 时会 fail closed，并要求显式 upgrade；它们不会先改 DB
再补备份。

```bash
# 先停止/bootout daemon，避免 migration 期间仍有业务写入。
wakebridge upgrade \
  --config /absolute/path/wakebridge.config.json \
  --backup-output /absolute/path/backups/pre-schema-9 \
  --confirm-offline
```

使用新源码的 upgrade 顺序固定：`offline assertion → WAL checkpoint → quick_check → verified backup → migrate → schema 9 + quick_check`。migration/验证失败会用刚创建的
backup 自动尝试恢复原 DB；持续的磁盘或权限故障也可能阻止恢复，须保全 verified backup 并先排除故障。schema 9 再执行为 no-op；
schema 6、7、8 是新源码的升级基线；更旧或更新的 schema 都拒绝。schema 9 回填已有 channel，不因默认数量上限裁剪旧数据；
普通 daemon/CLI/MCP 不自动升级，已发布 preview.20 也不能直接打开 schema 9，回退须使用升级前的 verified backup。

## 数据 rollback / restore

restore 是 offline、instance-fenced 的破坏性操作：

```bash
# 1. 执行 service install 曾返回的 unload_command，并确认 daemon 已停止。
# 2. 恢复备份；confirm 值必须与 config 中 instance_id 完全一致。
wakebridge restore \
  --config /absolute/path/wakebridge.config.json \
  --backup /absolute/path/backups/pre-schema-9 \
  --rollback-output /absolute/path/backups/pre-restore-current \
  --confirm-instance-id default \
  --confirm-offline
```

restore 先校验目标 manifest、checksum、schema 与 integrity，再对当前 DB 做 checkpoint 和 verified rollback snapshot，然后原子替换 DB；
替换失败时自动尝试把刚才的 current snapshot 放回；这一步仍可能因持续的磁盘或权限故障失败，须保留 rollback snapshot。
恢复旧 schema 后不要再用新 daemon 打开 DB：先安装与该 schema 配套的旧 tarball，
或重新执行新版本显式 upgrade。

代码 rollback 与数据 rollback 是两个独立动作：

1. bootout 新 daemon；
2. 保存 inspect/日志证据；
3. 必要时用 `restore` 恢复 pre-upgrade DB；
4. `npm install --global /absolute/path/old-release.tgz`；
5. 用旧版本的 `release-preflight/doctor` 验证后再 bootstrap LaunchAgent。

## 长期维护与隔离恢复演练

已有 backup/restore/prune 是手动工具，没有自动备份、自动清库或自动缩小 DB。先按实际事件量和可接受数据损失决定备份间隔，
每次升级或首次 `prune --apply` 前另做备份；不要把“命令返回成功”当作已经验证恢复。建议定期以及升级后做以下隔离演练：

1. 用 `release-preflight --config ...` 检查原实例；记录 `status` 中队列、失败类别及 source checkpoint 时间，并观察 DB/WAL 与日志的实际大小趋势。
2. 用上述 `backup` 命令创建全新的备份目录，另行安全保管配置/凭据。完整 DB 备份含资源引用、业务元数据和 endpoint route credential，也可能包含 transport 错误里的敏感内容；按敏感数据保管，不能公开上传。
3. 在新的私有目录初始化同一 `instance_id` / `owner_id` 的临时实例；不配置 source、host 或服务，不启动 daemon。它会生成自己的 owner credential，不复用生产 owner credential；恢复的 DB 仍带有原 endpoint 等记录，因此整个演练保持离线。
4. 用已有 `restore` 恢复到这个临时实例，再执行 `release-preflight` 与 `status`；核对关键未完成事项和 checkpoint，与备份时的预期相符。
5. 记录备份时间、恢复耗时、校验结果和可接受的恢复点；确认没有进程使用临时目录后，只清理这个演练目录，不删除原库或备份。

命令示例（将所有占位路径和身份替换为本次精确目标；备份 schema 必须与所用 package 兼容）：

```sh
wakebridge init --data-dir /absolute/private/recovery-drill \
  --instance-id my-agent --owner-id my-agent --timezone Asia/Shanghai
wakebridge restore --config /absolute/private/recovery-drill/wakebridge.config.json \
  --backup /absolute/private/backups/verified-snapshot \
  --rollback-output /absolute/private/backups/drill-empty-before-restore \
  --confirm-instance-id my-agent --confirm-offline
wakebridge release-preflight --config /absolute/private/recovery-drill/wakebridge.config.json
wakebridge status --config /absolute/private/recovery-drill/wakebridge.config.json
```

**原库损坏时不要直接覆盖。** 正常 restore 要先备份当前库，因此当前库损坏或不存在时会拒绝；这不是自动灾难恢复入口。
停止原 daemon，原样保全 DB/WAL/SHM 与配置，再用上面的独立目录从已验证备份恢复。恢复结果确认后，切换服务路径与凭据属于另一个显式操作，
不能在演练时启动第二个相同身份的生产实例。不要通过删除 WAL、改 manifest checksum 或绕过 integrity 检查“修复”原库。

故障回归覆盖：损坏备份及重算 checksum 后的坏 DB 被拒绝、备份 manifest 写入失败不留下成功样子的备份、恢复复制中断清理临时文件并保留当前数据、
保留损坏原库后恢复到隔离实例，以及 prune 后去重与未完成工作跨备份恢复不丢失。写入/复制 ENOSPC 是受控故障注入，不声称已测试真实磁盘耗尽或所有文件系统故障。

## Uninstall

先执行 install 返回的 `unload_command`，再删除 profile：

```bash
wakebridge service uninstall --config /absolute/path/wakebridge.config.json
npm uninstall --global wake-bridge
```

`service uninstall` 只删除该 instance 的 plist 或 systemd user unit，明确保留 config、DB、backups 和 logs。数据删除不属于 uninstall；如确需删除，
由 operator 在完成 verified backup 后另行处理明确目录。

## Artifact 内容

tarball 包含运行 CLI 所需的 compiled `dist/src`、event/policy schema、LICENSE、README，以及当前 install/policy/source/SDK/operator/local-host runbook，
也包含 reference host adapter 和实验性 Claude CLI adapter 的运行文件、示例及文档（`wakebridge-claude`）。
tests、历史私有运行环境 artifacts、私有现场记录、Core TypeScript 源码和开发依赖不进入发布包。`exports` 只公开 root contract aggregation、
`event`、`source`、`transport`、Group Chat reference source 与 schemas；Core/DB/daemon/operator/local-HTTP modules 只供 package 自己的 CLI 使用。
边界和仓库外 fixture 见 [public SDK](public-sdk.md)；运行态与 dead-letter recovery 见
[operator control](operator-control.md)。
[Out-of-process host integration](out-of-process-host-adapter.md) 使用同一个 public transport subpath，但具体 runtime adapter 不因进入
artifact 而获得 supported tier。
