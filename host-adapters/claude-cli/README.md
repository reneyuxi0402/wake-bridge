# Wake Bridge · Claude CLI online adapter

Official, experimental Host Adapter bundled with `wake-bridge` for a **running, explicitly launched Claude Code session**. It uses Wake Bridge's public transport contract while keeping Claude-specific lifecycle code outside Core.

This adapter is part of the experimental `wake-bridge` npm preview, not a stable support promise. Claude custom channels remain an upstream research preview. Integration evidence and exact tested versions belong in `docs/verification.md`.

**Verified scope:** real Claude 2.1.270 TTY interactive-session UAT passed idle wake, busy queue delivery, model-generated acknowledgement, exit lease closure and no cold fallback, both directly and inside a fully detached tmux session. Print mode is explicitly rejected (`-p` / `--print`). Long-lived stdin `stream-json` sessions without `-p`, including the Vinka-style flags, also did not process channel notifications on Claude 2.1.270 or 2.1.273 and are unsupported. This is a narrow public preview, not a production deployment; see [the verification record](docs/verification.md).

## Scope

- Start stock Claude through `wakebridge-claude launch`; only that session receives channel notifications.
- SessionStart registers the exact session; renew keeps its lease live; exit closes it. A later explicit launch takes over the configured attention channel.
- Busy notifications use Claude's own channel queue. This adapter does not steer an in-flight turn or maintain a second durable queue.
- HTTP delivery means `accepted_to_live_pipe`, not that Claude read or processed it. A later exact channel hook echoes the one-shot nonce as
  `host_attested`; this proves injection into the bound session, not understanding or action.
- Claude should still call the separate `wakebridge` MCP's `attention_ack` for each batch it receives so Core can record the stronger
  `agent_seen` evidence. `attention_consume` is separate and appropriate only after a claim is actually handled.
- No cold launch, automatic resume, window discovery, terminal keystroke injection, permission relay, or automatic agent acknowledgement.

An ordinary Claude session already running without this adapter cannot be attached retroactively. Exit and relaunch that session through the launcher if you want this route. Launching another session on the same attention channel is an explicit takeover, not broadcast.

## Install the preview

Requires Node.js 20+, SQLite CLI (for Wake Bridge), and separately installed/authenticated stock Claude Code. The adapter neither logs in for you nor reads another application's authentication files.

Complete Claude's normal first-run interactive onboarding as well as authentication before using the adapter. In the tested version, a successful `claude auth login` alone did not finish that onboarding. Use the same Claude configuration for onboarding and the adapter launch.

The adapter is not a separate package. Install the Wake Bridge preview:

```sh
npm install --global wake-bridge@preview
wakebridge-claude --help
```

For a source checkout, run `npm ci`, `npm test`, and `npm pack --ignore-scripts` at the repository root; install the resulting Wake Bridge tarball into your chosen, versioned runtime directory. `wakebridge-claude --help` describes launcher options.

## Configure a dedicated host route

1. Use your existing private Wake Bridge instance config, or create an isolated Agent Space with `wakebridge init`.
2. Copy `examples/host-adapters.json` to an operator-owned config location. Set `attention_channels` explicitly if not `default`.
3. Provide a newly generated, distinct host bootstrap token in `WAKEBRIDGE_CLAUDE_HOST_TOKEN` to both the daemon and launcher via your normal private environment/secret store. Do not put it in command arguments or JSON. Never reuse the Bridge owner or connector token.
4. Start the daemon with the adapter config:

```sh
wakebridge daemon --config /absolute/agent-space/wakebridge.config.json \
  --host-adapters /absolute/host-adapters.json --host 127.0.0.1 --port 4311
```

The example is for a fresh isolated daemon. For an existing service, merge this adapter entry into its existing host-adapters configuration, preserve other entries, and use its existing service management workflow. Do not start a second canonical daemon against the same database.

## Launch an online Claude session

In the project where you want Claude to work:

```sh
wakebridge-claude launch --experimental \
  --bridge-config /absolute/agent-space/wakebridge.config.json \
  --daemon-origin http://127.0.0.1:4311 \
  --attention-channel default -- --model sonnet
```

This command must launch Claude's normal TTY interface. Keeping a `stream-json` stdin process alive does not make it an interactive channel session in Claude Code's current implementation. Runtimes that own Claude through JSONL stdin need a runtime-level Wake Bridge Host Adapter rather than this channel launcher.

A detached tmux pane preserves the required TTY semantics and passed the same end-to-end UAT. tmux is optional; it is useful when the session should remain online after the outer terminal disconnects. Wake delivery still uses the authenticated channel pipe, never `tmux send-keys`.

The config **must belong to the same instance as the specified daemon**. It is passed only to the separate owner MCP process. That MCP exposes the owner's normal Wake Bridge tools, not merely an ack-only permission. Choose a Claude session you trust with that Agent Space.

The launcher adds a process-local channel server named `wakebridge_channel`, a separate owner MCP named `wakebridge`, and a temporary lifecycle-hook plugin. Existing settings are not rewritten. These two MCP names are reserved for this launch; don't configure conflicting servers with the same names.

The owner MCP wrapper advertises `attention_ack` with the canonical required `wake_batch_id` argument. Claude can filter out Core's original root-level union schema (which also offers the legacy `batch_id` alias); this narrow presentation conversion avoids that host-specific limitation. Calls and Core validation are unchanged, and the wrapper never acknowledges a batch itself. The channel negotiates only its implemented MCP revision, `2025-06-18`.

Claude may require its development-channel confirmation. Organization policy may disable channels; the adapter does not bypass it. The launcher does not disable tool permissions. In unattended operation, Claude may pause for approval; approve permitted Wake Bridge tools through Claude's normal mechanism.

Only references and the fixed Wake envelope are delivered. Source data remains untrusted: a resource title, URI, or metadata is not authority to run instructions. Receipt rules are part of channel instructions, not a claim that every model will always comply. Missing acknowledgements remain visible as Core's `ack_timeout` attention state.

## Lifecycle and credentials

- Each launch creates a random route token, a private temporary directory (0700), private generated configuration (0600), and a separate loopback port.
- Inherited Wake Bridge secrets are stripped before injecting the scoped host/route values. The owner MCP strips those host values again and loads its own explicitly selected config.
- A temporary private lease file allows SessionEnd to close the exact lease even when Claude has already stopped the channel subprocess. No owner token is written into the adapter's generated configuration.
- Normal exit removes the launcher's temporary directory. An uncatchable process/machine crash can leave temporary files; endpoint lease expiry remains the fallback. Do not mistake process existence for a live binding.
- A stale SessionEnd or generation cannot revoke a replacement session. Previously accepted notifications in an old Claude host queue cannot be remotely withdrawn; this limitation is not hidden by ack/consume/dismiss.
- Losing a lease or ending the registered session disables that channel process. A later automatic SessionStart (for example during context compaction) must not reclaim a route taken over elsewhere. Explicitly relaunch through the adapter to reconnect; it does not silently recover by taking over again.

## Verify and roll back

Test against an isolated Agent Space first: registration, idle wake, busy delivery, explicit ack, source/resource handling, exit, and no-endpoint behavior. `npm test` is deterministic conformance testing, **not a real Claude-model UAT**.

To stop using this adapter, exit the launched Claude session and start stock `claude` normally. Remove its manifest entry from the daemon only after the session is closed. No Claude global settings, credentials, user hooks, Core schema, or production databases are modified by installation.

## Upstream references

- [Claude channels reference](https://code.claude.com/docs/en/channels-reference)
- [Claude hook reference](https://code.claude.com/docs/en/hooks)
- [Wake Bridge Host Adapter contract](https://github.com/reneyuxi0402/wake-bridge/blob/main/docs/operations/out-of-process-host-adapter.md)

Historical channel/launcher concepts were adapted from the Wake Bridge engineering experiment under Apache-2.0. This package's runtime integration uses only the public SDK and contract; historical UAT does not certify this version.
