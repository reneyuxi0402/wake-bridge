import { spawn } from "node:child_process";
import { Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { StringDecoder } from "node:string_decoder";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ACK_SCHEMA = {
  type: "object",
  properties: { wake_batch_id: { type: "string", minLength: 1, maxLength: 200 } },
  required: ["wake_batch_id"],
  additionalProperties: false,
};

export function rewriteToolsListMessage(value) {
  if (!value || value.jsonrpc !== "2.0" || value.result == null || !Array.isArray(value.result.tools)) return value;
  let changed = false;
  const tools = value.result.tools.map((tool) => {
    if (!tool || tool.name !== "attention_ack") return tool;
    changed = true;
    return { ...tool, inputSchema: ACK_SCHEMA };
  });
  return changed ? { ...value, result: { ...value.result, tools } } : value;
}

export function rewriteJsonlLine(line) {
  const ending = line.endsWith("\r\n") ? "\r\n" : line.endsWith("\n") ? "\n" : "";
  const body = ending ? line.slice(0, -ending.length) : line;
  if (!body.trim()) return line;
  try {
    const parsed = JSON.parse(body);
    const rewritten = rewriteToolsListMessage(parsed);
    return rewritten === parsed ? line : `${JSON.stringify(rewritten)}${ending}`;
  } catch { return line; }
}

export class ToolsListTransform extends Transform {
  #buffer = "";
  #decoder = new StringDecoder("utf8");
  constructor() { super({ decodeStrings: false }); }
  _transform(chunk, encoding, callback) {
    this.#buffer += typeof chunk === "string" ? chunk : this.#decoder.write(chunk);
    const lines = this.#buffer.split("\n");
    this.#buffer = lines.pop() ?? "";
    try { for (const line of lines) this.push(rewriteJsonlLine(`${line}\n`)); callback(); }
    catch (error) { callback(error); }
  }
  _flush(callback) {
    try {
      this.#buffer += this.#decoder.end();
      if (this.#buffer) this.push(rewriteJsonlLine(this.#buffer));
      callback();
    }
    catch (error) { callback(error); }
  }
}

function arg(name) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : undefined;
}
async function runOwnerMcp() {
  const bridgeConfig = arg("--bridge-config");
  const daemonOrigin = arg("--daemon-origin") ?? "http://127.0.0.1:4311";
  if (!bridgeConfig) throw new Error("owner MCP requires --bridge-config");
  const packageUrl = import.meta.resolve("wake-bridge/package.json");
  const packageRoot = dirname(fileURLToPath(packageUrl));
  const cli = join(packageRoot, "dist", "src", "cli.js");
  const env = { ...process.env };
  for (const key of Object.keys(env)) if (key.startsWith("WAKEBRIDGE_") || key.startsWith("WB_CLAUDE_")) delete env[key];
  env.WAKEBRIDGE_DAEMON_URL = daemonOrigin;
  const child = spawn(process.execPath, [cli, "mcp", "--config", bridgeConfig], { env, stdio: ["inherit", "pipe", "inherit"] });
  const forwarders = new Map(["SIGINT", "SIGTERM", "SIGHUP"].map((signal) => [signal, () => {
    if (child.exitCode === null && child.signalCode === null) child.kill(signal);
  }]));
  for (const [signal, handler] of forwarders) process.on(signal, handler);
  const removeForwarders = () => { for (const [signal, handler] of forwarders) process.off(signal, handler); };
  const output = pipeline(child.stdout, new ToolsListTransform(), process.stdout).catch(() => {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
  });
  child.once("error", () => { removeForwarders(); process.exitCode = 1; });
  child.once("exit", async (code, signal) => {
    await output;
    removeForwarders();
    process.exitCode = signal === "SIGINT" ? 130 : signal === "SIGTERM" ? 143 : signal === "SIGHUP" ? 129 : (code ?? 1);
  });
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) runOwnerMcp().catch(() => { process.exitCode = 1; });
