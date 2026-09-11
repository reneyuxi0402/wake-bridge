# Linux VPS deployment candidate

状态：下一版 preview 候选；目标验证环境为 Ubuntu 24.04 LTS x64。已发布的 `0.9.0-preview.8` 仍是 macOS-only，
完成真实 VPS reboot canary 和新版本发布前，不得把本页写成现有支持承诺。

## 1. 支持边界

本 profile 只覆盖同一台 VPS 内的部署：

```text
Source Connector → Wake Bridge daemon → Host Adapter → agent runtime
                   all loopback / same VPS
```

Wake Bridge daemon、source ingress 与 Host Adapter route 都保持 `127.0.0.1`。不要将 4311 或 connector/adapter 端口直接暴露
到公网；远程 operator 访问使用 SSH tunnel。公网 webhook 应先进入独立、受认证且有限流的 ingress sidecar，再转发到同机
loopback source endpoint。

Bridge 在 VPS、agent runtime 在另一台 Mac/PC 的分布式拓扑不受支持。当前 SDK 会拒绝非 loopback route；不要删除该校验。

## 2. 前置环境

使用专用非 root OS 用户。以下是 operator 示例，不是 Wake Bridge 自动执行的命令：

```sh
sudo useradd --create-home --shell /bin/bash wakebridge
sudo loginctl enable-linger wakebridge
```

Ubuntu 需要 Node.js 20+ 与支持 JSON output 的 SQLite CLI 3.33+。Node 安装方式由 operator 选择；SQLite 可来自系统仓库：

```sh
sudo apt-get update
sudo apt-get install sqlite3
node --version
sqlite3 --version
```

切换/登录专用用户后，把 npm global prefix 放到其私有 home，而不是使用 root 安装：

```sh
mkdir -p "$HOME/.local"
npm install --global --prefix "$HOME/.local" /absolute/path/to/wake-bridge-candidate.tgz
export PATH="$HOME/.local/bin:$PATH"
wakebridge release-preflight
```

## 3. 初始化 Agent Space

CLI 继续要求显式 data directory，不猜测或迁移旧路径：

```sh
WAKEBRIDGE_STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/wake-bridge/default"
mkdir -p "$WAKEBRIDGE_STATE_DIR"
chmod 700 "$WAKEBRIDGE_STATE_DIR"

wakebridge init \
  --data-dir "$WAKEBRIDGE_STATE_DIR" \
  --instance-id default \
  --owner-id your-agent \
  --timezone UTC

wakebridge release-preflight \
  --config "$WAKEBRIDGE_STATE_DIR/wakebridge.config.json"
```

`init` 创建 mode-600 config/token/SQLite。不要把 owner token 写进 unit、命令行、日志或 Git。

## 4. 可选 daemon 配置文件

Host Adapter bootstrap token 等只通过 mode-600 environment file 注入。Host Adapter manifest 与 Source Connector manifest
仍按各自 runbook 创建；source ingress credential file 同样必须 mode 600：

```sh
chmod 600 "$WAKEBRIDGE_STATE_DIR/daemon.env"
chmod 600 "$WAKEBRIDGE_STATE_DIR/source-credentials.json"
```

`daemon.env` 使用 systemd `EnvironmentFile=` 接受的 `NAME=value` 格式。它不能包含 shell 命令，也不要在验收输出中打印内容。

## 5. 安装并启动 systemd user service

最小 Core：

```sh
wakebridge service install \
  --config "$WAKEBRIDGE_STATE_DIR/wakebridge.config.json" \
  --port 4311
```

带 Host Adapter/Source 配置的 daemon：

```sh
wakebridge service install \
  --config "$WAKEBRIDGE_STATE_DIR/wakebridge.config.json" \
  --port 4311 \
  --environment-file "$WAKEBRIDGE_STATE_DIR/daemon.env" \
  --host-adapters "$WAKEBRIDGE_STATE_DIR/host-adapters.json" \
  --connectors "$WAKEBRIDGE_STATE_DIR/sources.json" \
  --source-credentials "$WAKEBRIDGE_STATE_DIR/source-credentials.json"
```

install 只写 `~/.config/systemd/user/io.wakebridge.<instance>.service`，不启动进程。执行 JSON 输出中的精确命令，典型顺序为：

```sh
systemctl --user daemon-reload
systemctl --user enable io.wakebridge.default.service
systemctl --user start io.wakebridge.default.service
systemctl --user status io.wakebridge.default.service
journalctl --user -u io.wakebridge.default.service
```

检查 daemon 时从 mode-600 instance config 读取 token 到当前 shell，避免把它保存到命令历史之外的其他位置：

```sh
wakebridge doctor --config "$WAKEBRIDGE_STATE_DIR/wakebridge.config.json"
wakebridge status --config "$WAKEBRIDGE_STATE_DIR/wakebridge.config.json"
```

Host Adapter 与具体 agent runtime 是独立 service/process；Wake Bridge package 不内置或自动启动它们。没有 live Host Adapter
binding 时，batch 正确状态是 `waiting_for_endpoint`。

## 6. Reboot canary

正式支持声明前必须在全新 VPS 上连续完成两轮：

1. synthetic Source → Claim/Batch → reference Host Adapter → receipt；
2. `sudo reboot`；
3. 不进行 SSH 登录或手动启动，等待 systemd user manager 自动恢复；
4. 登录后核对 service activation time、journal、health/inspect 和第二条 synthetic wake；
5. 确认监听地址只有 loopback，unit/argv/journal 不含任何 token。

自动化测试、容器或 QEMU 不能替代这项真实 reboot 证据。

## 7. 停止、卸载与回滚

先关闭 Host Adapter session lease，再停止 daemon：

```sh
systemctl --user stop io.wakebridge.default.service
systemctl --user disable io.wakebridge.default.service
wakebridge service uninstall \
  --config "$WAKEBRIDGE_STATE_DIR/wakebridge.config.json"
systemctl --user daemon-reload
```

`service uninstall` 只删除 unit，保留 config、SQLite 与 backup。package 回滚前先执行 `wakebridge backup`；本次平台支持不改变
schema，因此可停止候选 service、安装旧 tarball，再使用该版本的 `release-preflight/doctor` 核对。已发布 preview.8 的 npm
manifest 会拒绝 Linux 安装，这属于预期的 fail-closed 回滚边界，不代表数据损坏。
