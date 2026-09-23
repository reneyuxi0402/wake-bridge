import { randomBytes } from "node:crypto";
import { spawn } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, rmSync, statSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";
import { loopbackHttpOrigin } from "wake-bridge/transport";
import { WAKE_BRIDGE_VERSION } from "./version.mjs";

const ROOT = dirname(fileURLToPath(import.meta.url));
const MIN_TOKEN = 32;
const SOURCE = "wakebridge_channel";

function loopbackOrigin(raw) { return loopbackHttpOrigin(raw, "daemon origin").origin; }

function token(name, value) {
  if (typeof value !== "string" || value.length < MIN_TOKEN) throw new Error(`${name} must contain at least ${MIN_TOKEN} characters`);
  return value;
}

function channel(value) {
  const result = String(value ?? "default").trim();
  if (!result || result.length > 256 || /[\u0000-\u001f\u007f]/u.test(result)) throw new Error("attention channel is invalid");
  return result;
}

function privateJson(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, { encoding: "utf8", mode: 0o600 });
  chmodSync(path, 0o600);
}

function shellQuote(value) { return `'${String(value).replaceAll("'", `'\\''`)}'`; }

function validateClaudeArgs(args) {
  const forbidden = new Set(["--mcp-config", "--plugin-dir", "--settings", "--strict-mcp-config", "--dangerously-load-development-channels", "--channels", "--bare", "--skip-permissions", "--dangerously-skip-permissions"]);
  for (const arg of args) {
    if (arg === "-p" || arg === "--print" || arg.startsWith("-p=") || arg.startsWith("--print=")) {
      throw new Error("Claude print/non-interactive mode is unsupported");
    }
    if (forbidden.has(arg) || [...forbidden].some((flag) => arg.startsWith(`${flag}=`))) throw new Error("Claude arguments attempt to override adapter configuration");
  }
}

function validateBridgeConfig(path) {
  if (!isAbsolute(path)) throw new Error("bridge config must be an absolute path");
  let info;
  try { info = statSync(path); } catch { throw new Error("bridge config is not readable"); }
  if (!info.isFile() || (info.mode & 0o077) !== 0) throw new Error("bridge config must be a private regular file");
  return path;
}

export async function allocateChannelPort() {
  const server = createServer();
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const address = server.address();
  const port = address && typeof address !== "string" ? address.port : 0;
  await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  if (!port) throw new Error("could not allocate channel port");
  return port;
}

export function prepareClaudeLaunch(options = {}) {
  if (options.experimental !== true) throw new Error("--experimental is required");
  const parentEnv = options.parent_env ?? process.env;
  const bridgeConfig = validateBridgeConfig(options.bridge_config ?? "");
  const daemonOrigin = loopbackOrigin(options.daemon_origin ?? parentEnv.WAKEBRIDGE_DAEMON_ORIGIN ?? "http://127.0.0.1:4311");
  const attentionChannel = channel(options.attention_channel ?? parentEnv.WAKEBRIDGE_ATTENTION_CHANNEL ?? "default");
  const hostToken = token("WAKEBRIDGE_CLAUDE_HOST_TOKEN", options.host_token ?? parentEnv.WAKEBRIDGE_CLAUDE_HOST_TOKEN);
  const routeToken = options.route_token ?? randomBytes(32).toString("hex");
  token("Claude route token", routeToken);
  if (routeToken === hostToken) throw new Error("Claude route token must differ from host token");
  const channelPort = options.channel_port;
  if (!Number.isSafeInteger(channelPort) || channelPort < 1 || channelPort > 65535) throw new Error("channel port is invalid");
  validateClaudeArgs(options.claude_args ?? []);
  const tempDir = mkdtempSync(join(options.temp_parent ?? tmpdir(), "wakebridge-claude-"));
  chmodSync(tempDir, 0o700);
  try {
    const configDir = join(tempDir, "config");
    const pluginDir = join(tempDir, "plugin");
    mkdirSync(configDir, { mode: 0o700 });
    mkdirSync(pluginDir, { mode: 0o700 });
    const mcpConfigPath = join(configDir, "mcp.json");
    const pluginManifestDir = join(pluginDir, ".claude-plugin");
    const hooksDir = join(pluginDir, "hooks");
    mkdirSync(pluginManifestDir, { mode: 0o700 });
    mkdirSync(hooksDir, { mode: 0o700 });
    const hooksConfigPath = join(hooksDir, "hooks.json");
    const channelEntry = join(ROOT, "channel.mjs");
    const ownerEntry = join(ROOT, "owner-mcp.mjs");
    const hookEntry = join(ROOT, "hook.mjs");
    const hook = [{ matcher: "", hooks: [{ type: "command", command: `${shellQuote(process.execPath)} ${shellQuote(hookEntry)}` }] }];
    privateJson(join(pluginManifestDir, "plugin.json"), { name: "wakebridge-claude-launch", version: WAKE_BRIDGE_VERSION });
    privateJson(hooksConfigPath, { hooks: { SessionStart: hook, UserPromptSubmit: hook, Stop: hook, StopFailure: hook, SessionEnd: hook } });
    privateJson(mcpConfigPath, { mcpServers: {
      [SOURCE]: { command: process.execPath, args: [channelEntry], env: {
        WB_CLAUDE_CHANNEL_PORT: "${WB_CLAUDE_CHANNEL_PORT}",
        WB_CLAUDE_CHANNEL_TOKEN: "${WAKEBRIDGE_CLAUDE_ROUTE_TOKEN}",
        WAKEBRIDGE_DAEMON_ORIGIN: "${WAKEBRIDGE_DAEMON_ORIGIN}",
        WAKEBRIDGE_CLAUDE_HOST_TOKEN: "${WAKEBRIDGE_CLAUDE_HOST_TOKEN}",
        WAKEBRIDGE_ATTENTION_CHANNEL: "${WAKEBRIDGE_ATTENTION_CHANNEL}",
        WAKEBRIDGE_CLAUDE_CHANNEL_SOURCE: SOURCE,
        WB_CLAUDE_SESSION_STATE_DIR: "${WB_CLAUDE_SESSION_STATE_DIR}",
      } },
      wakebridge: { command: process.execPath, args: [ownerEntry, "--bridge-config", bridgeConfig, "--daemon-origin", daemonOrigin] },
    } });
    const env = { ...parentEnv };
    for (const key of Object.keys(env)) if (key.startsWith("WAKEBRIDGE_") || key.startsWith("WB_CLAUDE_")) delete env[key];
    Object.assign(env, { WAKEBRIDGE_CLAUDE_HOST_TOKEN: hostToken, WAKEBRIDGE_CLAUDE_ROUTE_TOKEN: routeToken,
      WAKEBRIDGE_DAEMON_ORIGIN: daemonOrigin, WAKEBRIDGE_ATTENTION_CHANNEL: attentionChannel,
      WAKEBRIDGE_CLAUDE_CHANNEL_SOURCE: SOURCE, WB_CLAUDE_CHANNEL_PORT: String(channelPort), WB_CLAUDE_SESSION_STATE_DIR: tempDir });
    return { command: options.claude_bin ?? "claude", args: ["--dangerously-load-development-channels", `server:${SOURCE}`, "--plugin-dir", pluginDir, "--mcp-config", mcpConfigPath, ...(options.claude_args ?? [])], env, temp_dir: tempDir, mcp_config_path: mcpConfigPath, hooks_config_path: hooksConfigPath, channel_port: channelPort, cleanup: () => rmSync(tempDir, { recursive: true, force: true }) };
  } catch (error) { rmSync(tempDir, { recursive: true, force: true }); throw error; }
}

export async function launchClaude(options = {}) {
  const port = options.channel_port ?? await allocateChannelPort();
  const launch = prepareClaudeLaunch({ ...options, channel_port: port });
  return await new Promise((resolve, reject) => {
    const child = spawn(launch.command, launch.args, { env: launch.env, stdio: "inherit" });
    const signals = ["SIGINT", "SIGTERM", "SIGHUP"];
    const forward = (signal) => { if (child.exitCode === null && child.signalCode === null) child.kill(signal); };
    const handlers = new Map(signals.map((signal) => [signal, () => forward(signal)]));
    for (const [signal, handler] of handlers) process.on(signal, handler);
    const remove = () => { for (const [signal, handler] of handlers) process.off(signal, handler); };
    const done = (code) => { remove(); launch.cleanup(); resolve(code ?? 1); };
    child.once("error", (error) => { remove(); launch.cleanup(); reject(new Error(`Claude process failed to start: ${error.code ?? "error"}`)); });
    child.once("exit", (code, signal) => done(signal === "SIGINT" ? 130 : signal === "SIGTERM" ? 143 : signal === "SIGHUP" ? 129 : code));
  });
}
