# Verification record

Date: 2026-09-21; follow-up 2026-09-22. Candidate originally tested as `wake-bridge-claude-cli@0.1.0-preview.1`; the same adapter is bundled into `wake-bridge@0.9.0-preview.11`.

## Scope and environment

- Core dependency: published `wake-bridge@0.9.0-preview.10`, unchanged.
- Local runtime: Node.js 20.20.1 / macOS arm64 / SQLite 3.51.0.
- Tested Claude CLI: 2.1.270. The stock CLI auto-updater later changed the default binary to 2.1.278; follow-up runs explicitly pinned the retained 2.1.270 binary and set `DISABLE_AUTOUPDATER=1`. No wake compatibility claim is made for 2.1.278.
- Production Wake Bridge instances, sources, Vinka runtime and existing AI sessions are untouched.

## Evidence status

The parent-run final deterministic suite passed: **18 tests, 0 failures**. `npm run check` also passed. Separately, **real Claude 2.1.270 interactive-session UAT passed** idle delivery, busy queue delivery, model-generated ack, exit lease closure and no cold fallback on 2026-09-22. Print/non-interactive diagnostics failed and that mode is now explicitly rejected by the launcher. The package is released only as an experimental npm preview; no production deployment was performed.

- 7 channel/hook checks: implemented-protocol negotiation, readiness, bearer/body/nonce/generation validation, concurrent delivery deduplication, verified wake echo versus foreground presence, renewal failure, stdout callback error and bounded timeout (including a delayed error), re-registration fencing and exact-lease SessionEnd fallback.
- 1 black-box public-Core integration, described below.
- 6 launcher checks: private configuration, argument validation, credential redaction, separator handling, spawn/nonzero-exit cleanup and signal forwarding.
- 3 owner-wrapper checks: canonical ack schema mapping, preservation of other JSONL messages and UTF-8 characters split across byte-sized chunks.
- 1 packaging check: an offline tarball installation with published Core into a fresh directory outside the source tree, followed by the installed command's help entry point.

Tests use temporary local data and loopback ports. The two existing Core worktrees remain clean; no production service or schema was changed. The pack file list contains only source, example configuration, license and documentation, never tests, temporary state or credentials.

The isolated integration fixture runs the installed published Core daemon, channel/hook subprocesses and separate owner MCP. It schedules synthetic `probe://` resources through MCP, checks the transport receipt, explicitly acknowledges and consumes via MCP, takes over the route, rejects an old-generation acknowledgement, and checks both live and post-channel-exit SessionEnd paths. It does not import Core internals or access a production database.

Real-host UAT was authorized on 2026-09-21, with an aggregate US$0.50 budget. No upstream token belongs in this record, package, fixture, argv or logs. Offline fixture tests do not certify Claude's busy queue or model-generated acknowledgement behavior.

The chosen existing OAuth credential was supplied only through the isolated Claude subprocess environment. The adapter itself never discovers or migrates credentials. All model prompts were synthetic; no Vinka/other-agent instructions, memories or histories were loaded. Claude used an empty project directory, a fresh configuration directory, disabled skills/session persistence, and explicit tool permissions without permission bypass. Core used a separate temporary Agent Space, database, credentials and loopback ports.

## Initial print-mode diagnostics (Claude 2.1.270)

Model: `claude-haiku-4-5-20251001`. Three initial model turns returned `READY` successfully. CLI-reported costs were US$0.015546, US$0.016182 and US$0.0044664, totaling **US$0.0361944**. Subscription accounting may differ from the displayed API-equivalent cost. Other startup-only diagnostics sent no model prompt.

| Check | Evidence | Result |
| --- | --- | --- |
| Authentication/model access | Three successful initial model turns | pass |
| MCP connection | Both `wakebridge_channel` and `wakebridge` reported connected | pass |
| SessionStart registration | Exact isolated route registered at generation 1 | pass |
| Ack tool discovery, original wrapper | Claude explicitly logged that `attention_ack` was skipped for its root-level `anyOf` | fail, fixed locally |
| Ack tool discovery, fixed wrapper | Subsequent Claude init listed `mcp__wakebridge__attention_ack` | pass |
| Idle wake | Each synthetic batch became `dispatched` with `transport_accepted`; no model turn or `agent_seen` appeared during the 60-second observation | fail |
| Explicit listen diagnostic | Adding `--channels server:wakebridge_channel` in addition to the development flag did not change idle behavior | fail; not added to product |
| Busy queue/model-generated ack | Not reached because idle delivery failed | not verified |
| Interactive startup | After choosing a theme, fresh configuration requested an official login method despite the inference token working in print mode; test was cancelled without submitting a model prompt | login prerequisite remains |

Three failing idle batches (isolated fixtures only): `wb_1e04de1f-6ad6-4fa1-99d4-882d996fcf90`, `wb_6404071d-1a68-4f35-aca1-533d026de3fb`, `wb_278002fc-a627-412f-abb3-1d447ba52b5f`.

### Confirmed fixes and remaining uncertainty

The independent owner wrapper now presents only canonical `wake_batch_id` for `attention_ack`, without a root-level combinator. Its requests and results still go through the published Core CLI unchanged. Claude's [MCP schema documentation](https://code.claude.com/docs/en/mcp#tool-input-schemas-with-a-root-level-combinator) describes the filtering observed in the startup log. This is a host-specific presentation fix, not a change to Core's API or an automatic acknowledgement.

The channel also stops echoing arbitrary requested protocol versions and declares only the implemented `2025-06-18` revision. This is independently regression-tested; it did not by itself resolve idle delivery. The final owner wrapper preserves multibyte UTF-8 across stdout chunks.

The reason print-mode channel notifications failed to reach a model turn remains unresolved. Read-only inspection of the installed CLI confirms that `pollChannel` belongs to a separate session-notices path, not custom-channel registration. The real connection log explicitly reported `protocolEra:"legacy"`, ruling out the modern-protocol rejection path in these runs. An absent cached account profile or an empty remote-settings response is **not** sufficient evidence to blame organization policy or assert a specific root cause. Neither feature flags nor account/approval state were fabricated or overridden. Interactive testing requires a normally authenticated test configuration; completing login is a next diagnostic step, not a proven fix for print mode.

At that initial checkpoint, all test processes were stopped and a process check found no remaining test daemon, channel or owner-wrapper process. Temporary Agent Spaces, generated credentials, fresh Claude configuration, model debug logs and startup diagnostics were removed after recording the redacted evidence above. The credential-reading test harness contained no token value and remained outside the package. Production services and both Core worktrees were untouched; `tested_host_versions` was still empty pending interactive UAT. The later results below supersede that verification status.

### Official-login follow-up (2026-09-22)

- The user completed `claude auth login --claudeai` successfully in `.uat/claude-config` (private directory, excluded from packaging). The installed CLI scopes its Keychain service by the custom config directory; no Vinka or other-agent credentials were copied.
- A new print-mode run used that official login, not an environment OAuth token. Both adapter MCP servers connected, the ack tool was present, generation 1 registered, and `READY` returned. Its isolated batch `wb_7213c56a-4085-4f04-9c09-d54795d52bf8` remained `dispatched` after 60 seconds without a model-generated ack. **Official login alone did not fix the print-mode failure.**
- This run reported US$0.016301; the cumulative reported model cost is now **US$0.0524954**, within the authorized US$0.50 aggregate budget.
- Official account login automatically discovered account-connected MCP servers during startup. No remote tools were called. Subsequent fixtures explicitly set the documented `ENABLE_CLAUDEAI_MCP_SERVERS=false` to disable these unrelated cloud connectors.
- The deterministic suite was rerun: `npm run check` passed; all 18 tests passed with local loopback listening permitted. Restricted-sandbox `EPERM` bind failures are environmental, not assertion failures.
- Interactive onboarding still requested a browser authorization after selecting a theme, despite the successful separate auth command and model turn. That fixture timed out at registration without scheduling any batch or sending a model prompt. A standalone onboarding process was then started without the fixture deadline. The user explicitly authorized completing the existing account's normal browser sign-in, which succeeded. Onboarding/account state was not fabricated. The independent auth configuration is retained for follow-up, unlike the earlier disposable token-based configurations.

### Interactive UAT passed (2026-09-22)

The same published Core and adapter were exercised through a real terminal session with normal folder-trust and local-development-channel confirmations. Cloud account connectors were disabled; no remote resources, other-agent memories or existing sessions were read. Fixture keystrokes supplied only the synthetic busy-test prompt; wake notifications themselves always arrived over the MCP channel, never terminal injection.

- First interactive run: batch `wb_fe423753-0dec-4f50-b046-dfa8d318b5a5` produced both `transport_accepted` and `agent_seen`. The new test session's transcript contained the model's exact `mcp__wakebridge__attention_ack` call. Reported session cost: US$0.01576.
- Second run confirmed both idle and busy acknowledgement. The busy notification arrived at 08:17:28.613 UTC during the `sleep 8` tool (08:17:25.274–08:17:33.332); the model acknowledged at 08:17:35.525. A fixture assertion raced transcript flushing and stopped that run before exit checks; after-exit inspection confirmed both calls. This was corrected by waiting for the transcript entry, not by changing the adapter. Reported session cost: US$0.0351922.
- Final run passed all five checks: exact generation-1 registration; idle batch `wb_a578e414-29e9-43fc-a627-807218d69091`; queued busy batch `wb_7a732aba-aa3d-4404-bbb8-6899de521d01`; lease closed after real Claude exit; a new post-exit batch remained `waiting_for_endpoint`, without launching another host. Both acknowledgements came from the model and matched the transcript. Reported session cost: US$0.0188347.
- Cumulative reported cost across token-based, official-login and interactive model tests: **US$0.1222823**, below the authorized US$0.50 aggregate limit. Interactive costs were read from each isolated session's final `cost-state`; the initial interactive harness's zero counter was not a valid billing measurement. Subscription billing may differ from API-equivalent reported costs.
- The launcher now rejects `-p`, `--print` and their equals forms with an explicit unsupported-mode error. This bounds the current adapter to the verified interactive path; it does not claim to fix or identify the upstream print-mode cause. All 18 deterministic tests still pass, including the added argument-rejection assertions.
- `tested_host_versions` now contains only `2.1.270`. The adapter remains experimental; Core source, Core schemas and production Wake Bridge services were not changed.
- After recording these results, the five follow-up temporary Agent Spaces (including their generated credentials/debug logs) and three synthetic session transcripts were removed. The test processes were confirmed stopped. The dedicated signed-in config/Keychain entry is retained for reuse; none of it is packaged. Raw removed fixture data is not recoverable from this package; the redacted evidence above is retained.

### Long-lived stdin stream-json does not receive channels (2026-09-22)

Three further isolated runs tested whether process lifetime, rather than TTY interactivity, was the relevant boundary. All kept stdin open and omitted both `-p` and `--print`.

- Claude 2.1.270 with basic `--input-format stream-json --output-format stream-json`: the first ordinary stdin turn returned `READY`, generation 1 registered, and the process remained live. The wake produced only one `transport_accepted` receipt and no `agent_seen` within 60 seconds. Reported cost: US$0.0049014.
- Claude 2.1.270 with Vinka-style `--include-partial-messages --permission-prompt-tool stdio`: two ordinary JSONL turns in the same process returned `READY` and `READY_TWO`, proving the runner remained usable between turns. The wake still produced only `transport_accepted`. Reported turn costs: US$0.0046114 and US$0.006108.
- Claude 2.1.273, the version recorded as Vinka Runtime's current pinned main CLI, with the same Vinka-style flags: two turns again succeeded in the same process, but the wake again stopped at `transport_accepted`. Reported turn costs: US$0.0047564 and US$0.006297.

In all three runs both MCP servers connected and the ack tool was visible, but the debug log never emitted the interactive run's authoritative `Channel notifications registered` line. It classified the CLI surface as `nonInteractive=true`. The adjacent `pollChannel=false` field belongs to the separate session-notices subsystem and is not used as the diagnosis. The evidence supports a narrower conclusion: current Claude Code does not register custom-channel delivery for stdin stream-json sessions, even when they are long-lived and accept multiple turns. This is not a Wake Bridge transport or process-lifetime failure.

The conservative sum of every reported model turn raises aggregate test cost to **US$0.1489565**, below the authorized US$0.50 cap. Vinka production services, credentials, sessions and its dirty worktree were not changed. Supporting Agent Spaces/debug logs were deleted after recording this redacted result.

### Detached tmux UAT passed (2026-09-22)

The final interactive matrix was repeated inside a new tmux 3.6a server on a dedicated socket. The harness and Claude ran in a detached pane; `session_attached=0` was checked before delivery and again during the busy test. No existing tmux server or Vinka process was used.

- The log contained the authoritative `Channel notifications registered` line.
- Idle batch `wb_4a5d1745-bc6e-4573-9bcf-2ff4fc1a13b6` produced `transport_accepted` and `agent_seen`, with a matching model-generated `attention_ack`.
- Busy batch `wb_c664a079-fb14-4b3b-848f-e871b5a1e7eb` arrived while the model's allowed `sleep 8` tool was running, queued, and was acknowledged afterward.
- Claude exit closed the exact lease; a post-exit batch remained `waiting_for_endpoint`, proving there was no cold launch or tmux keystroke fallback.
- The harness exited successfully with all five checks true. Reported session cost was US$0.0300702, bringing the conservative aggregate to **US$0.1790267**, below the US$0.50 authorization.

tmux supplied only terminal persistence. Test setup used `send-keys` for the same synthetic busy-test user prompt that a human would type; Wake Bridge batches themselves arrived exclusively through `notifications/claude/channel`. The dedicated tmux session/socket, temporary Agent Space, debug log and synthetic transcript were removed after recording the result.

## Acceptance checklist

- Installed public Core daemon and adapter communicate solely through public HTTP/SDK surfaces.
- Fixed `POST /v1/wakes` contract, readiness/auth/nonce/generation fences, and no synthetic acknowledgement.
- SessionStart, renewal, EOF/exit, stale SessionEnd, takeover and no cold fallback.
- Separate owner MCP explicitly acknowledges a real accepted batch in an isolated database.
- Temporary configuration/lease permissions and credential redaction.
- A clean tarball installs and starts outside this source tree.
- Real Claude: idle receive, explicit ack, busy queue receive, correct target, clean exit; upstream preview/policy constraints remain visible.
