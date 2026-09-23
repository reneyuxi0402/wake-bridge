import assert from "node:assert/strict";
import { chmodSync, existsSync, mkdtempSync, statSync, writeFileSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test, { afterEach } from "node:test";
import { launchClaude, prepareClaudeLaunch } from "../src/launcher.mjs";

const host = "host-token-012345678901234567890123";
const adapterRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const fixtureRoots = [];
afterEach(() => { for (const path of fixtureRoots.splice(0)) rmSync(path, { recursive: true, force: true }); });

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "wakebridge-claude-launch-test-"));
  fixtureRoots.push(root);
  const config = join(root, "wakebridge.json");
  writeFileSync(config, "{}", { mode: 0o600 });
  return { root, config };
}

function fakeCli(root, body) {
  const path = join(root, "fake-cli.mjs");
  writeFileSync(path, `#!/usr/bin/env node\n${body}\n`, { mode: 0o700 });
  chmodSync(path, 0o700);
  return path;
}

function runCli(args, env = {}) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [join(adapterRoot, "src", "cli.mjs"), ...args], { env: { ...process.env, ...env }, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = ""; let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += String(chunk); });
    child.stderr.on("data", (chunk) => { stderr += String(chunk); });
    child.on("exit", (code, signal) => resolve({ code, signal, stdout, stderr }));
  });
}

test("prepare creates private config and redacts tokens from files/argv", () => {
  const { root, config } = fixture();
  const launch = prepareClaudeLaunch({ experimental: true, bridge_config: config, host_token: host, channel_port: 19001, temp_parent: root, claude_args: ["--verbose"] });
  try {
    assert.equal(statSync(launch.temp_dir).mode & 0o777, 0o700);
    assert.equal(statSync(launch.mcp_config_path).mode & 0o777, 0o600);
    assert.equal(statSync(launch.hooks_config_path).mode & 0o777, 0o600);
    assert.ok(existsSync(join(launch.temp_dir, "plugin", ".claude-plugin", "plugin.json")));
    assert.ok(existsSync(join(launch.temp_dir, "plugin", "hooks", "hooks.json")));
    const text = readFileSync(launch.mcp_config_path, "utf8") + JSON.stringify(launch.args);
    assert.equal(text.includes(host), false);
    assert.equal(text.includes(launch.env.WAKEBRIDGE_CLAUDE_ROUTE_TOKEN), false);
    assert.match(readFileSync(launch.hooks_config_path, "utf8"), /'[^']*node[^']*'/u);
    assert.equal(launch.args.at(-1), "--verbose");
  } finally { launch.cleanup(); }
  assert.equal(existsSync(launch.temp_dir), false);
});

test("prepare rejects insecure config, missing experimental, and adapter overrides", () => {
  const { config } = fixture();
  chmodSync(config, 0o644);
  assert.throws(() => prepareClaudeLaunch({ bridge_config: config, host_token: host, channel_port: 19002 }), /experimental/u);
  assert.throws(() => prepareClaudeLaunch({ experimental: true, bridge_config: config, host_token: host, channel_port: 19002 }), /private/u);
  const secure = fixture().config;
  assert.throws(() => prepareClaudeLaunch({ experimental: true, bridge_config: secure, host_token: host, channel_port: 19002, claude_args: ["--mcp-config=/tmp/secret"] }), /override/u);
  for (const arg of ["-p", "--print", "--print=stream-json", "-p=stream-json"]) {
    assert.throws(() => prepareClaudeLaunch({ experimental: true, bridge_config: secure, host_token: host, channel_port: 19002, claude_args: [arg] }), /print\/non-interactive mode is unsupported/u);
  }
});

test("CLI rejects parser errors without spawning and never echoes host token", async () => {
  const { root, config } = fixture();
  const marker = join(root, "spawned");
  const claude = fakeCli(root, `const fs = await import("node:fs"); fs.writeFileSync(${JSON.stringify(marker)}, "yes");`);
  const unknown = await runCli(["launch", "--experimental", "--bridge-config", config, "--claude-bin", claude, "--unknown"]);
  const missing = await runCli(["launch", "--experimental", "--bridge-config", config, "--attention-channel"]);
  const secret = "super-secret-host-token-0123456789";
  const hostFlag = await runCli(["launch", "--experimental", "--bridge-config", config, "--host-token", secret, "--", claude]);
  const hostInline = await runCli(["launch", "--experimental", "--bridge-config", config, `--host-token=${secret}`, "--", claude]);
  const conflict = await runCli(["launch", "--experimental", "--bridge-config", config, "--", "--mcp-config=/tmp/override"]);
  assert.equal(unknown.code, 2);
  assert.equal(missing.code, 2);
  assert.equal(hostFlag.code, 2);
  assert.equal(hostInline.code, 2);
  assert.equal(conflict.code, 2);
  assert.equal(existsSync(marker), false);
  assert.equal(hostFlag.stderr.includes(secret), false);
  assert.equal(hostInline.stderr.includes(secret), false);
});

test("CLI passes --help after separator to fake Claude", async () => {
  const { root, config } = fixture();
  const marker = join(root, "args.json");
  const claude = fakeCli(root, `const fs = await import("node:fs"); fs.writeFileSync(${JSON.stringify(marker)}, JSON.stringify(process.argv.slice(2)));`);
  const result = await runCli(["launch", "--experimental", "--bridge-config", config, "--host-token-does-not-exist", "--", "--help"]);
  // The pre-separator typo is intentionally rejected and therefore must not run.
  assert.equal(result.code, 2);
  const ok = await runCli(["launch", "--experimental", "--bridge-config", config, "--channel-port", "19011", "--claude-bin", claude, "--", "--help"], { WAKEBRIDGE_CLAUDE_HOST_TOKEN: host });
  assert.equal(ok.code, 0);
  const args = JSON.parse(readFileSync(marker, "utf8"));
  assert.deepEqual(args.slice(0, 3), ["--dangerously-load-development-channels", "server:wakebridge_channel", "--plugin-dir"]);
  assert.equal(args.at(-1), "--help");
});

test("launch cleans up after spawn failure and nonzero exit", async () => {
  const { root, config } = fixture();
  await assert.rejects(() => launchClaude({ experimental: true, bridge_config: config, host_token: host, channel_port: 19012, claude_bin: join(root, "missing"), temp_parent: root }), /failed to start/u);
  assert.deepEqual(readdirSync(root).filter((name) => name.startsWith("wakebridge-claude-")), []);
  const claude = fakeCli(root, "process.exit(7);");
  const code = await launchClaude({ experimental: true, bridge_config: config, host_token: host, channel_port: 19013, claude_bin: claude, temp_parent: root });
  assert.equal(code, 7);
  assert.deepEqual(readdirSync(root).filter((name) => name.startsWith("wakebridge-claude-")), []);
});

test("launch forwards SIGTERM and leaves no child or temp directory", async () => {
  const { root, config } = fixture();
  const marker = join(root, "terminated");
  const started = join(root, "started");
  const claude = fakeCli(root, `const fs = await import("node:fs"); fs.writeFileSync(${JSON.stringify(started)}, "yes"); process.on("SIGTERM", () => { fs.writeFileSync(${JSON.stringify(marker)}, "yes"); process.exit(143); }); setInterval(() => {}, 1000);`);
  const child = spawn(process.execPath, [join(adapterRoot, "src", "cli.mjs"), "launch", "--experimental", "--bridge-config", config, "--channel-port", "19014", "--claude-bin", claude], { env: { ...process.env, TMPDIR: root, WAKEBRIDGE_CLAUDE_HOST_TOKEN: host }, stdio: "ignore" });
  const deadline = Date.now() + 3000;
  while (!existsSync(started) && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 25));
  assert.equal(existsSync(started), true);
  child.kill("SIGTERM");
  const result = await new Promise((resolve) => child.on("exit", (code, signal) => resolve({ code, signal })));
  assert.equal(result.code, 143);
  assert.equal(readFileSync(marker, "utf8"), "yes");
  assert.deepEqual(readdirSync(root).filter((name) => name.startsWith("wakebridge-claude-")), []);
});
