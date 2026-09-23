#!/usr/bin/env node
import { launchClaude } from "./launcher.mjs";

const usage = "usage: wakebridge-claude launch --experimental --bridge-config /absolute/config [options] -- [claude args]\noptions: --daemon-origin URL --attention-channel NAME --claude-bin PATH --channel-port PORT\ncredential: WAKEBRIDGE_CLAUDE_HOST_TOKEN (environment only)";
function fail(message) { process.stderr.write(`${message}\n${usage}\n`); process.exitCode = 2; }
const argv = process.argv.slice(2);
if (argv[0] === "--help" || argv[0] === "-h") { process.stdout.write(`${usage}\n`); process.exit(0); }
if (argv[0] !== "launch") { fail("expected launch"); }
else {
  const options = {}; let index = 1; let claudeArgs = []; let parseError = null;
  while (index < argv.length && argv[index] !== "--") {
    const key = argv[index++];
    if (key === "--experimental") options.experimental = true;
    else if (["--bridge-config", "--daemon-origin", "--attention-channel", "--claude-bin", "--channel-port"].includes(key)) {
      const value = argv[index++]; if (!value || value.startsWith("--")) { parseError = `${key} requires a value`; break; }
      options[{ "--bridge-config": "bridge_config", "--daemon-origin": "daemon_origin", "--attention-channel": "attention_channel", "--claude-bin": "claude_bin", "--channel-port": "channel_port" }[key]] = key === "--channel-port" ? Number(value) : value;
    } else { parseError = "invalid option"; break; }
  }
  if (argv[index] === "--") claudeArgs = argv.slice(index + 1);
  if (parseError) fail(parseError);
  else if (!options.experimental) fail("--experimental is required");
  else if (!options.bridge_config) fail("--bridge-config is required");
  else launchClaude({ ...options, claude_args: claudeArgs }).then((code) => { process.exitCode = code; }).catch((error) => fail(error.message));
}
