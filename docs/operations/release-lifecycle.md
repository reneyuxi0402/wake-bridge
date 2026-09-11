# Release install, upgrade, backup, and rollback

状态：engineering preview current runbook（2026-09-07）。Wake Bridge 发布包只支持 Host Adapter Contract；
完成本页不等于任何具体 agent 产品获得兼容性或时延保证。

## 支持矩阵

| 项目 | 当前承诺 |
| --- | --- |
| package | `wake-bridge`，npm dist-tag `preview` |
| license | Apache-2.0 |
| OS | 已发布 `0.9.0-preview.8`：macOS / `darwin`；`0.9.0-preview.9` 候选：Ubuntu 24.04 LTS x64 / systemd user service |
| CPU | arm64、x64 |
| Node.js | 20 或更高 |
| SQLite CLI | 3.33.0 或更高，且必须支持 `-json` |
| topology | 每个 Agent Space 一个本地 instance / SQLite DB |
| schema | current 8；显式升级支持 6、7 → 8 |
| host integration | out-of-process Host Adapter Contract v1；不内置具体 agent 产品支持 |

已发布 `0.9.0-preview.8` 的 npm manifest 会在非 macOS 平台拒绝安装。`0.9.0-preview.9` 源码候选允许 Linux 并生成
systemd user unit，但完成真实 VPS reboot canary 与新 preview 发布前，这不是已发布支持。
SQLite 是外部 runtime prerequisite，不由 npm 安装；3.33.0 是 CLI 加入 JSON output mode 的版本。

Ubuntu 24.04 LTS x64 的候选安装、linger、journal 与回滚步骤见 [Linux VPS runbook](linux-vps.md)。Wake Bridge、
Source Connector、Host Adapter 与 agent runtime 必须同机；跨机器 Remote Host Adapter 不在该 profile 内。

## 从 tarball 干净安装

发布前先生成并审计 tarball：

```bash
npm pack --json
npm install --global ./wake-bridge-0.9.0-preview.9.tgz
wakebridge release-preflight
```

正式发布到 preview channel 后，等价安装命令是：

```bash
npm install --global wake-bridge@preview
```

## Maintainer release

公开版本由 GitHub tag 驱动的 release workflow 发布。发布前必须同时提交版本号与
`docs/releases/<version>.md`，并确保 tag 精确等于 `v<version>`。推送 tag 后，workflow 会在
GitHub-hosted runner 上重新执行 typecheck、全量测试与 `npm pack`，通过 npm Trusted Publishing/OIDC
把同一个 tarball 发布到 `preview`，随后创建带 tarball 和 SHA-256 清单的 GitHub pre-release。

```bash
git tag v0.9.0-preview.9
git push origin v0.9.0-preview.9
```

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

返回值包含精确 `plist_path`、`load_command` 和 `unload_command`。plist 只保存 Node、已安装 CLI、config path、loopback
host/port 与日志位置，不复制 owner token。核对后执行返回的 `launchctl bootstrap ...`，再检查：

同机多个 Agent Space 必须为每个 instance 选择不同 loopback port；profile 不会猜测或自动抢占已有端口。

```bash
curl --fail -H "Authorization: Bearer $WAKEBRIDGE_ADMIN_TOKEN" http://127.0.0.1:4311/health
wakebridge doctor --config "$HOME/Library/Application Support/WakeBridge/default/wakebridge.config.json"
```

同一 instance 不允许覆盖已有 plist。升级 package 后 plist 的全局安装路径保持不变；若 npm 安装布局改变，先 bootout、
`service uninstall`，再由新 bin 重装 profile。

## Linux systemd user service（`0.9.0-preview.9` 候选）

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
  --backup-output /absolute/path/backups/pre-schema-8 \
  --confirm-offline
```

upgrade 顺序固定：`offline assertion → WAL checkpoint → quick_check → verified backup → migrate → schema 8 + quick_check`。任何 migration/验证失败都会用刚创建的
backup 自动恢复原 DB。schema 8 再执行为 no-op；schema 6、7 是本 preview 的升级基线；更旧或更新的 schema 都拒绝。

## 数据 rollback / restore

restore 是 offline、instance-fenced 的破坏性操作：

```bash
# 1. 执行 service install 曾返回的 unload_command，并确认 daemon 已停止。
# 2. 恢复备份；confirm 值必须与 config 中 instance_id 完全一致。
wakebridge restore \
  --config /absolute/path/wakebridge.config.json \
  --backup /absolute/path/backups/pre-schema-8 \
  --rollback-output /absolute/path/backups/pre-restore-current \
  --confirm-instance-id default \
  --confirm-offline
```

restore 先为当前 DB 再做一份 verified rollback snapshot，校验目标 manifest、checksum、schema 与 integrity，然后原子替换 DB；
失败时自动把刚才的 current snapshot 放回。恢复旧 schema 后不要再用新 daemon 打开 DB：先安装与该 schema 配套的旧 tarball，
或重新执行新版本显式 upgrade。

代码 rollback 与数据 rollback 是两个独立动作：

1. bootout 新 daemon；
2. 保存 inspect/日志证据；
3. 必要时用 `restore` 恢复 pre-upgrade DB；
4. `npm install --global /absolute/path/old-release.tgz`；
5. 用旧版本的 `release-preflight/doctor` 验证后再 bootstrap LaunchAgent。

## Uninstall

先执行 install 返回的 `unload_command`，再删除 profile：

```bash
wakebridge service uninstall --config /absolute/path/wakebridge.config.json
npm uninstall --global wake-bridge
```

`service uninstall` 只删除该 instance 的 plist，明确保留 config、DB、backups 和 logs。数据删除不属于 uninstall；如确需删除，
由 operator 在完成 verified backup 后另行处理明确目录。

## Artifact 内容

tarball 包含运行 CLI 所需的 compiled `dist/src`、event/policy schema、LICENSE、README，以及当前 install/policy/source/SDK/operator/local-host runbook。
tests、具体产品 Host Adapter 实验实现与文档、历史私有运行环境 artifacts、私有现场记录、源码和开发依赖不进入发布包。`exports` 只公开 root contract aggregation、
`event`、`source`、`transport`、Group Chat reference source 与 schemas；Core/DB/daemon/operator/local-HTTP modules 只供 package 自己的 CLI 使用。
边界和仓库外 fixture 见 [public SDK](public-sdk.md)；运行态与 dead-letter recovery 见
[operator control](operator-control.md)。
[Out-of-process host integration](out-of-process-host-adapter.md) 使用同一个 public transport subpath，但具体 runtime adapter 不因进入
artifact 而获得 supported tier。
