#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { WakeBridge } from "./core.js";
import { startDaemon } from "./daemon.js";
import { bridgeConfigFromFile, initializeInstance, instanceConfigSecurity, instanceDataSecurity, rotateOwnerCredential } from "./instance-config.js";
import { WakeBridgeMcpServer, runMcpStdio } from "./mcp.js";
import { bootstrapSourceConnector, loadSourceConnectorFile, SourceSupervisor } from "./source-connector.js";
import { startBotlingKnowsConnector } from "./connectors/botlingknows-mcp.js";
import { startGroupChatConnector } from "./connectors/group-chat-http.js";
import { CONNECTOR_RUNTIME_STATES, connectorCatalog } from "./connector-catalog.js";
import { DaemonSourceControlClient } from "./source-control-client.js";
import { loadSourceIngestCredentialFile } from "./source-ingress.js";
import { loadPolicyFile } from "./policy-control.js";
import { operatorStatus, retryDeadLetter } from "./operator-control.js";
import { loadOutOfProcessHostFile } from "./out-of-process-host.js";
import {
  backupInstance,
  installMacLaunchAgent,
  releasePreflight,
  restoreInstance,
  uninstallMacLaunchAgent,
  upgradeInstance,
} from "./release-lifecycle.js";
import type { BridgeConfig } from "./types.js";

type Args = Record<string, string | boolean>;

const KNOWN_COMMANDS = new Set([
  "help", "--help", "init", "owner-token-rotate", "release-preflight", "backup", "upgrade", "restore",
  "service", "doctor", "connector-catalog", "source-validate", "botlingknows-connector", "group-chat-connector",
  "daemon", "mcp", "source-bootstrap", "source-once", "policy", "emit", "claim-schedule", "claim-snooze",
  "claim-dismiss", "claim-consume", "endpoint-register", "endpoint-renew", "takeover", "presence-renew", "tick",
  "dispatch", "inspect", "status", "batch-retry", "events", "claims", "batches", "receipts", "bindings", "endpoints",
]);

function parseArgs(values: string[]): { command: string; rest: string[]; options: Args } {
  const command = values[0] || "help";
  const rest: string[] = [];
  const options: Args = {};
  for (let index = 1; index < values.length; index += 1) {
    const value = values[index];
    if (value === "--") {
      rest.push(...values.slice(index + 1));
      break;
    }
    if (!value.startsWith("--")) {
      rest.push(value);
      continue;
    }
    const key = value.slice(2).replaceAll("-", "_");
    const next = values[index + 1];
    if (next && !next.startsWith("--")) {
      options[key] = next;
      index += 1;
    } else {
      options[key] = true;
    }
  }
  return { command, rest, options };
}

function option(options: Args, key: string, fallback?: string): string | undefined {
  const value = options[key];
  return typeof value === "string" ? value : fallback;
}

function jsonOption(options: Args, key: string, fallback: unknown): any {
  const raw = option(options, key);
  if (!raw) return fallback;
  try { return JSON.parse(raw); } catch (error) { throw new Error(`--${key.replaceAll("_", "-")} must be JSON: ${String(error)}`); }
}

function optionalNumber(value: string | undefined, field: string): number | undefined {
  if (value === undefined || value === "") return undefined;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error(`${field} must be a finite number`);
  return parsed;
}

function instanceConfigPath(options: Args): string | undefined {
  return option(options, "config", process.env.WAKEBRIDGE_CONFIG);
}

function mergedConfig(options: Args): BridgeConfig {
  return {
    instance_id: option(options, "instance_id", process.env.WAKEBRIDGE_INSTANCE_ID || "local")!,
    owner_id: option(options, "owner_id", process.env.WAKEBRIDGE_OWNER_ID || "local-owner")!,
    db_path: option(options, "db", process.env.WAKEBRIDGE_DB || "./wake-bridge.sqlite")!,
    timezone: option(options, "timezone", process.env.WAKEBRIDGE_TIMEZONE || "UTC")!,
    admin_token: option(options, "admin_token", process.env.WAKEBRIDGE_ADMIN_TOKEN),
    presence_ttl_ms: optionalNumber(process.env.WAKEBRIDGE_PRESENCE_TTL_MS, "WAKEBRIDGE_PRESENCE_TTL_MS"),
    activity_ttl_ms: optionalNumber(process.env.WAKEBRIDGE_ACTIVITY_TTL_MS, "WAKEBRIDGE_ACTIVITY_TTL_MS"),
    endpoint_lease_ms: optionalNumber(process.env.WAKEBRIDGE_ENDPOINT_LEASE_MS, "WAKEBRIDGE_ENDPOINT_LEASE_MS"),
  };
}

function configFrom(options: Args): BridgeConfig {
  const file = instanceConfigPath(options);
  if (!file) return mergedConfig(options);
  const stored = bridgeConfigFromFile(file);
  const explicit = [
    ["instance_id", option(options, "instance_id", process.env.WAKEBRIDGE_INSTANCE_ID), stored.instance_id],
    ["owner_id", option(options, "owner_id", process.env.WAKEBRIDGE_OWNER_ID), stored.owner_id],
    ["db_path", option(options, "db", process.env.WAKEBRIDGE_DB), stored.db_path],
    ["timezone", option(options, "timezone", process.env.WAKEBRIDGE_TIMEZONE), stored.timezone],
    ["admin_token", option(options, "admin_token", process.env.WAKEBRIDGE_ADMIN_TOKEN), stored.admin_token],
  ] as const;
  for (const [field, requested, fixed] of explicit) {
    if (requested !== undefined && requested !== fixed) {
      throw new Error(`${field} cannot override the fixed Agent Space config`);
    }
  }
  return {
    ...stored,
    presence_ttl_ms: optionalNumber(process.env.WAKEBRIDGE_PRESENCE_TTL_MS, "WAKEBRIDGE_PRESENCE_TTL_MS"),
    activity_ttl_ms: optionalNumber(process.env.WAKEBRIDGE_ACTIVITY_TTL_MS, "WAKEBRIDGE_ACTIVITY_TTL_MS"),
    endpoint_lease_ms: optionalNumber(process.env.WAKEBRIDGE_ENDPOINT_LEASE_MS, "WAKEBRIDGE_ENDPOINT_LEASE_MS"),
  };
}

function openBridge(
  config: ReturnType<typeof configFrom>,
  options: Args,
  recoverDispatchLeases: boolean,
): WakeBridge {
  return new WakeBridge(config, { recoverDispatchLeases });
}

function output(value: unknown): void {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

function help(): void {
  output({
    commands: [
      "emit --source SOURCE --type TYPE --dedupe-key KEY --resource URI [--metadata JSON]",
      "claim-schedule --resource URI --eligible-after ISO [--attention-channel CHANNEL]",
      "claim-snooze CLAIM_ID --until ISO",
      "claim-dismiss CLAIM_ID [--reason TEXT]",
      "claim-consume CLAIM_ID [--result JSON]",
      "endpoint-register --host-kind KIND --session-ref REF [--routes JSON]",
      "endpoint-renew ENDPOINT_ID --lease-token TOKEN",
      "takeover --attention-channel CHANNEL --endpoint-id ID [--expected-generation N]",
      "presence-renew --attention-channel CHANNEL --endpoint-id ID --generation N --lease-token TOKEN",
      "tick",
      "dispatch",
      "policy list",
      "policy install --file PATH",
      "policy test --source SOURCE --event JSON [--file PATH]",
      "policy preview --source SOURCE --event JSON [--file PATH]",
      "inspect | events | claims | batches | receipts | bindings | endpoints",
      "status --config PATH (secret-free queue/host/source health; does not recover leases)",
      "batch-retry BATCH_ID --expected-attempt N --reason TEXT (reopens dead-letter only; does not dispatch)",
      "init --data-dir PATH --instance-id ID --owner-id OWNER [--timezone ZONE]",
      "owner-token-rotate --config PATH (atomically replaces the stored owner credential)",
      "release-preflight [--config PATH]",
      "backup --config PATH --output DIRECTORY",
      "upgrade --config PATH --backup-output DIRECTORY --confirm-offline",
      "restore --config PATH --backup DIRECTORY --rollback-output DIRECTORY --confirm-instance-id ID --confirm-offline",
      "service install --config PATH [--port N] [--launch-agents-dir DIRECTORY] [--logs-dir DIRECTORY]",
      "service uninstall --config PATH [--launch-agents-dir DIRECTORY]",
      "daemon --config PATH --port 4311 [--host 127.0.0.1] [--source-credentials PATH] [--host-adapters PATH]",
      "daemon --unsafe-no-auth --host 127.0.0.1 (explicit development mode only)",
      "doctor",
      "connector-catalog (lists available/planned connectors; all default disabled)",
      "source-validate --connectors PATH",
      "source-once SOURCE --connectors PATH",
      "source-bootstrap SOURCE --mode from-now --connectors PATH --expected-subject-ref REF --expected-binding-fingerprint sha256:...",
      "botlingknows-connector --port 4391 (requires env-only upstream URL, subject ref, and connector token)",
      "group-chat-connector --port 4392 (requires env-only Group Chat URL, service token, and connector token)",
      "mcp (stdio JSON-RPC; use WAKEBRIDGE_DB/WAKEBRIDGE_INSTANCE_ID/WAKEBRIDGE_OWNER_ID)",
    ],
  });
}

async function main(): Promise<void> {
  const parsed = parseArgs(process.argv.slice(2));
  if (!KNOWN_COMMANDS.has(parsed.command)) throw new Error(`unknown command: ${parsed.command}`);
  if (parsed.command === "help" || parsed.command === "--help") {
    help();
    return;
  }
  if (parsed.command === "init") {
    if (Object.prototype.hasOwnProperty.call(parsed.options, "admin_token")) {
      throw new Error("init generates the owner credential; --admin-token is not accepted");
    }
    const dataDir = option(parsed.options, "data_dir", process.env.WAKEBRIDGE_DATA_DIR);
    const instanceId = option(parsed.options, "instance_id", process.env.WAKEBRIDGE_INSTANCE_ID);
    const ownerId = option(parsed.options, "owner_id", process.env.WAKEBRIDGE_OWNER_ID);
    if (!dataDir || !instanceId || !ownerId) throw new Error("init requires --data-dir, --instance-id, and --owner-id");
    const initialized = initializeInstance({
      data_dir: dataDir,
      instance_id: instanceId,
      owner_id: ownerId,
      timezone: option(parsed.options, "timezone", process.env.WAKEBRIDGE_TIMEZONE || "UTC"),
      db_path: option(parsed.options, "db"),
      config_path: instanceConfigPath(parsed.options),
    });
    output({
      ok: true,
      instance_id: initialized.config.instance_id,
      owner_id: initialized.config.owner_id,
      db_path: initialized.config.db_path,
      config_path: initialized.config_path,
      credential: "stored_in_private_config",
    });
    return;
  }
  if (parsed.command === "owner-token-rotate") {
    const path = instanceConfigPath(parsed.options);
    if (!path) throw new Error("owner-token-rotate requires --config");
    if (process.env.WAKEBRIDGE_ADMIN_TOKEN) {
      throw new Error("unset WAKEBRIDGE_ADMIN_TOKEN before rotating the config-backed owner credential");
    }
    output({ ok: true, ...rotateOwnerCredential(path), credential: "stored_in_private_config" });
    return;
  }
  if (parsed.command === "release-preflight") {
    const result = releasePreflight(instanceConfigPath(parsed.options));
    output(result);
    if (!result.ok) process.exitCode = 2;
    return;
  }
  if (parsed.command === "backup") {
    const configPath = instanceConfigPath(parsed.options);
    const destination = option(parsed.options, "output");
    if (!configPath || !destination) throw new Error("backup requires --config and --output");
    output({ ok: true, backup: backupInstance(configPath, destination) });
    return;
  }
  if (parsed.command === "upgrade") {
    const configPath = instanceConfigPath(parsed.options);
    const destination = option(parsed.options, "backup_output");
    if (!configPath || !destination) throw new Error("upgrade requires --config and --backup-output");
    output({ ok: true, ...upgradeInstance(configPath, destination, parsed.options.confirm_offline === true) });
    return;
  }
  if (parsed.command === "restore") {
    const configPath = instanceConfigPath(parsed.options);
    const backupDirectory = option(parsed.options, "backup");
    const rollbackOutput = option(parsed.options, "rollback_output");
    const confirmInstanceId = option(parsed.options, "confirm_instance_id");
    if (!configPath || !backupDirectory || !rollbackOutput || !confirmInstanceId) {
      throw new Error("restore requires --config, --backup, --rollback-output, and --confirm-instance-id");
    }
    output({ ok: true, ...restoreInstance({
      config_path: configPath,
      backup_directory: backupDirectory,
      rollback_output: rollbackOutput,
      confirm_instance_id: confirmInstanceId,
      confirm_offline: parsed.options.confirm_offline === true,
    }) });
    return;
  }
  if (parsed.command === "service") {
    const action = parsed.rest[0];
    const configPath = instanceConfigPath(parsed.options);
    if (!configPath) throw new Error("service requires --config");
    if (action === "install") {
      output({ ok: true, service: installMacLaunchAgent({
        config_path: configPath,
        launch_agents_directory: option(parsed.options, "launch_agents_dir"),
        logs_directory: option(parsed.options, "logs_dir"),
        port: optionalNumber(option(parsed.options, "port"), "--port"),
      }) });
      return;
    }
    if (action === "uninstall") {
      output({ ok: true, service: uninstallMacLaunchAgent({
        config_path: configPath,
        launch_agents_directory: option(parsed.options, "launch_agents_dir"),
      }) });
      return;
    }
    throw new Error("service requires install or uninstall");
  }
  if (parsed.command === "doctor") {
    let config: BridgeConfig;
    let configLoadError: string | null = null;
    try {
      config = configFrom(parsed.options);
    } catch (error) {
      configLoadError = error instanceof Error ? error.message : String(error);
      config = mergedConfig({ ...parsed.options, config: false });
    }
    let installedSqlite: string | null = null;
    try {
      installedSqlite = execFileSync("sqlite3", ["--version"], { encoding: "utf8", timeout: 5_000 }).trim();
    } catch {
      // Runtime readiness is reported below.
    }
    const fileSecurity = instanceConfigSecurity(instanceConfigPath(parsed.options));
    const dataSecurity = instanceDataSecurity(config.db_path);
    const adminReady = typeof config.admin_token === "string" && config.admin_token.length >= 32;
    const sourceCredentialPath = option(parsed.options, "source_credentials", process.env.WAKEBRIDGE_SOURCE_CREDENTIALS_FILE);
    const release = releasePreflight(instanceConfigPath(parsed.options));
    let sourceCredentialCount = 0;
    let sourceCredentialError: string | null = null;
    if (sourceCredentialPath) {
      try {
        sourceCredentialCount = loadSourceIngestCredentialFile(sourceCredentialPath).length;
      } catch (error) {
        sourceCredentialError = error instanceof Error ? error.message : String(error);
      }
    }
    const hostAdapterPath = option(parsed.options, "host_adapters", process.env.WAKEBRIDGE_HOST_ADAPTERS_FILE);
    let externalHostAdapters: Array<{ adapter_kind: string; adapter_version: string; host_kinds: string[]; support_tier: string }> = [];
    let externalHostError: string | null = null;
    if (hostAdapterPath) {
      try {
        externalHostAdapters = loadOutOfProcessHostFile(hostAdapterPath).adapters.map((adapter) => ({
          adapter_kind: adapter.manifest.adapter_kind,
          adapter_version: adapter.manifest.adapter_version,
          host_kinds: [...adapter.manifest.host_kinds],
          support_tier: adapter.manifest.support_tier,
        }));
      } catch (error) {
        externalHostError = error instanceof Error ? error.message : String(error);
      }
    }
    output({
      ok: release.ok && Boolean(installedSqlite) && adminReady && fileSecurity.ready && dataSecurity.ready
        && configLoadError === null && sourceCredentialError === null && externalHostError === null,
      release,
      runtime: { sqlite3: { available: Boolean(installedSqlite), installed_version: installedSqlite } },
      security: {
        owner_api_authenticated: adminReady,
        config_load_error: configLoadError,
        config_file: fileSecurity,
        database: dataSecurity,
        source_ingress: {
          configured: Boolean(sourceCredentialPath),
          credential_count: sourceCredentialCount,
          error: sourceCredentialError,
        },
      },
      hosts: {
        contract: {
          protocol: "out_of_process_loopback_v1",
          product_hosts_bundled: false,
          responsibility: "session start/resume and envelope injection belong to the configured Host Adapter",
          receipt_rule: "transport acceptance is not agent start, seen, or consumed",
        },
        configured_adapters: {
          configured: Boolean(hostAdapterPath),
          adapter_count: externalHostAdapters.length,
          adapters: externalHostAdapters,
          error: externalHostError,
        },
      },
    });
    return;
  }
  if (parsed.command === "connector-catalog") {
    output({ schema_version: 1, runtime_states: CONNECTOR_RUNTIME_STATES, connectors: connectorCatalog() });
    return;
  }
  const config = configFrom(parsed.options);
  if (parsed.command === "source-validate") {
    const path = option(parsed.options, "connectors", process.env.WAKEBRIDGE_SOURCE_CONNECTORS_FILE);
    if (!path) throw new Error("--connectors is required");
    const sources = loadSourceConnectorFile(path);
    output({ ok: true, sources });
    return;
  }
  if (parsed.command === "botlingknows-connector") {
    if (Object.prototype.hasOwnProperty.call(parsed.options, "upstream_url")
      || Object.prototype.hasOwnProperty.call(parsed.options, "connector_token")) {
      throw new Error("connector secrets must be supplied through the environment");
    }
    const upstreamUrl = process.env.BOTLINGKNOWS_MCP_URL;
    const connectorToken = process.env.WAKEBRIDGE_CONNECTOR_TOKEN;
    const subjectRef = process.env.BOTLINGKNOWS_SUBJECT_REF;
    if (!upstreamUrl || !connectorToken || !subjectRef) {
      throw new Error("BOTLINGKNOWS_MCP_URL, BOTLINGKNOWS_SUBJECT_REF, and WAKEBRIDGE_CONNECTOR_TOKEN are required");
    }
    const connector = await startBotlingKnowsConnector({
      upstream_url: upstreamUrl,
      connector_token: connectorToken,
      subject_ref: subjectRef,
      host: option(parsed.options, "host", "127.0.0.1"),
      port: Number(option(parsed.options, "port", "4391")),
    });
    output({ ok: true, source: "botlingknows", address: connector.address });
    const close = async () => connector.close();
    process.once("SIGINT", () => { void close().finally(() => process.exit(0)); });
    process.once("SIGTERM", () => { void close().finally(() => process.exit(0)); });
    await new Promise<void>(() => undefined);
    return;
  }
  if (parsed.command === "group-chat-connector") {
    if (
      Object.prototype.hasOwnProperty.call(parsed.options, "upstream_url") ||
      Object.prototype.hasOwnProperty.call(parsed.options, "upstream_token") ||
      Object.prototype.hasOwnProperty.call(parsed.options, "connector_token")
    ) {
      throw new Error("connector secrets and upstream identity must be supplied through the environment");
    }
    const upstreamUrl = process.env.GROUP_CHAT_BASE_URL;
    const upstreamToken = process.env.GROUP_CHAT_SERVICE_TOKEN;
    const connectorToken = process.env.WAKEBRIDGE_CONNECTOR_TOKEN;
    if (!upstreamUrl || !upstreamToken || !connectorToken) {
      throw new Error(
        "GROUP_CHAT_BASE_URL, GROUP_CHAT_SERVICE_TOKEN, and WAKEBRIDGE_CONNECTOR_TOKEN are required",
      );
    }
    const connector = await startGroupChatConnector({
      upstream_url: upstreamUrl,
      upstream_token: upstreamToken,
      connector_token: connectorToken,
      host: option(parsed.options, "host", "127.0.0.1"),
      port: Number(option(parsed.options, "port", "4392")),
    });
    output({ ok: true, source: "group_chat", address: connector.address });
    const close = async () => connector.close();
    process.once("SIGINT", () => { void close().finally(() => process.exit(0)); });
    process.once("SIGTERM", () => { void close().finally(() => process.exit(0)); });
    await new Promise<void>(() => undefined);
    return;
  }
  if (parsed.command === "daemon") {
    if (Object.prototype.hasOwnProperty.call(parsed.options, "admin_token")) {
      throw new Error("daemon owner credentials must come from --config or WAKEBRIDGE_ADMIN_TOKEN, never command arguments");
    }
    const unsafeNoAuth = parsed.options.unsafe_no_auth === true;
    if (!config.admin_token && !unsafeNoAuth) {
      throw new Error("daemon requires an owner credential from --config or WAKEBRIDGE_ADMIN_TOKEN; --unsafe-no-auth is loopback development only");
    }
    const connectorPath = option(parsed.options, "connectors", process.env.WAKEBRIDGE_SOURCE_CONNECTORS_FILE);
    const sourceConnectors = connectorPath ? loadSourceConnectorFile(connectorPath) : [];
    const sourceCredentialPath = option(parsed.options, "source_credentials", process.env.WAKEBRIDGE_SOURCE_CREDENTIALS_FILE);
    const sourceCredentials = sourceCredentialPath ? loadSourceIngestCredentialFile(sourceCredentialPath) : [];
    const hostAdapterPath = option(parsed.options, "host_adapters", process.env.WAKEBRIDGE_HOST_ADAPTERS_FILE);
    const externalHosts = hostAdapterPath ? loadOutOfProcessHostFile(hostAdapterPath) : { adapters: [], credentials: [] };
    const bridge = openBridge(config, parsed.options, true);
    const daemon = await startDaemon(config, {
      bridge,
      host: option(parsed.options, "host", "127.0.0.1"),
      port: Number(option(parsed.options, "port", "4311")),
      scheduler_interval_ms: Number(option(parsed.options, "scheduler_interval_ms", "1000")),
      unsafe_no_auth: unsafeNoAuth,
      host_adapters: externalHosts.adapters,
      host_credentials: externalHosts.credentials,
      source_connectors: sourceConnectors,
      source_credentials: sourceCredentials,
    });
    output({ ok: true, address: daemon.address, instance_id: config.instance_id, owner_id: config.owner_id });
    const close = async () => { await daemon.close(); daemon.bridge.close(); };
    process.once("SIGINT", () => { void close().finally(() => process.exit(0)); });
    process.once("SIGTERM", () => { void close().finally(() => process.exit(0)); });
    await new Promise<void>(() => undefined);
    return;
  }
  if (parsed.command === "mcp") {
    // MCP is an agent-facing control plane sharing the daemon's DB. Opening
    // it during a long wake turn must never reclaim the daemon's live lease.
    const bridge = openBridge(config, parsed.options, false);
    try {
      const sourceControl = config.admin_token
        ? new DaemonSourceControlClient(process.env.WAKEBRIDGE_DAEMON_URL || "http://127.0.0.1:4311", config.admin_token)
        : undefined;
      await runMcpStdio(new WakeBridgeMcpServer({ bridge, source_control: sourceControl }));
    } finally {
      bridge.close();
    }
    return;
  }
  // The daemon and an explicit `dispatch` invocation are dispatcher owners.
  // Inspect/schedule/ack/consume CLI processes must not recover their leases.
  const bridge = openBridge(config, parsed.options, parsed.command === "dispatch");
  try {
    switch (parsed.command) {
      case "source-bootstrap": {
        const path = option(parsed.options, "connectors", process.env.WAKEBRIDGE_SOURCE_CONNECTORS_FILE);
        const source = parsed.rest[0];
        const mode = option(parsed.options, "mode");
        const expectedSubjectRef = option(parsed.options, "expected_subject_ref");
        const expectedBindingFingerprint = option(parsed.options, "expected_binding_fingerprint");
        if (!path || !source || mode !== "from-now" || !expectedSubjectRef || !expectedBindingFingerprint) {
          throw new Error("source-bootstrap requires SOURCE, --mode from-now, --connectors, --expected-subject-ref, and --expected-binding-fingerprint");
        }
        const config = loadSourceConnectorFile(path).find((item) => item.id === source);
        if (!config) throw new Error(`source connector ${source} is not configured`);
        output({ checkpoint: await bootstrapSourceConnector(bridge, config, "from-now", process.env, fetch, {
          subject_ref: expectedSubjectRef,
          binding_fingerprint: expectedBindingFingerprint,
        }) });
        break;
      }
      case "source-once": {
        const path = option(parsed.options, "connectors", process.env.WAKEBRIDGE_SOURCE_CONNECTORS_FILE);
        const source = parsed.rest[0];
        if (!path || !source) throw new Error("source-once requires SOURCE and --connectors");
        const supervisor = new SourceSupervisor(bridge, loadSourceConnectorFile(path));
        const status = await supervisor.runOnce(source);
        output(status);
        if (status.state !== "healthy") process.exitCode = 2;
        await supervisor.stop();
        break;
      }
      case "policy": {
        const action = parsed.rest[0];
        if (action === "list") {
          output({ policies: bridge.listPolicies(), active: bridge.listActivePolicies(), status: bridge.listPolicyStatuses() });
          break;
        }
        const filePath = option(parsed.options, "file");
        if (action === "install") {
          if (!filePath) throw new Error("policy install requires --file");
          const file = loadPolicyFile(filePath);
          bridge.installPolicies(file.policies);
          output({ ok: true, installed: file.policies.map((policy) => ({ id: policy.id, version: policy.version })), active: bridge.listActivePolicies(), status: bridge.listPolicyStatuses() });
          break;
        }
        if (action === "test" || action === "preview") {
          const source = option(parsed.options, "source");
          const event = jsonOption(parsed.options, "event", undefined);
          if (!source || !event || typeof event !== "object" || Array.isArray(event)) {
            throw new Error(`policy ${action} requires --source and an object --event JSON value`);
          }
          const candidates = filePath ? loadPolicyFile(filePath).policies : undefined;
          output(action === "test"
            ? bridge.testPolicy(source, event, candidates)
            : bridge.previewPolicy(source, event, candidates));
          break;
        }
        throw new Error("policy requires one of: list, install, test, preview");
      }
      case "emit": {
        const resource = option(parsed.options, "resource");
        if (!resource) throw new Error("--resource is required");
        const event = jsonOption(parsed.options, "event", {
          type: option(parsed.options, "type", "event"),
          occurred_at: option(parsed.options, "occurred_at"),
          dedupe_key: option(parsed.options, "dedupe_key", `cli:${Date.now()}`),
          coalesce_key: option(parsed.options, "coalesce_key"),
          priority_hint: option(parsed.options, "priority_hint", "normal"),
          attention_channel_hint: option(parsed.options, "attention_channel_hint", "default"),
          resource: { uri: resource },
          metadata: jsonOption(parsed.options, "metadata", {}),
          payload_preview: option(parsed.options, "payload_preview"),
        });
        output(bridge.emitEvent(option(parsed.options, "source", "manual")!, event));
        break;
      }
      case "claim-schedule": {
        const resource = option(parsed.options, "resource");
        const eligible = option(parsed.options, "eligible_after");
        if (!resource || !eligible) throw new Error("--resource and --eligible-after are required");
        output(bridge.scheduleClaim({ resource: { uri: resource }, eligible_after: eligible, attention_channel: option(parsed.options, "attention_channel"), reason_code: option(parsed.options, "reason_code"), note: option(parsed.options, "note"), idempotency_key: option(parsed.options, "idempotency_key") }));
        break;
      }
      case "claim-snooze":
        output(bridge.snoozeClaim(parsed.rest[0], option(parsed.options, "until")!));
        break;
      case "claim-dismiss":
        output(bridge.dismissClaim(parsed.rest[0], option(parsed.options, "reason", "dismissed")));
        break;
      case "claim-consume":
        output(bridge.consumeClaim(parsed.rest[0], jsonOption(parsed.options, "result", null)));
        break;
      case "endpoint-register":
        {
          const hostKind = option(parsed.options, "host_kind", "mock")!;
          if (Object.prototype.hasOwnProperty.call(parsed.options, "cold_routes")) {
            throw new Error("--cold-routes has been removed; use --routes");
          }
          const routes = option(parsed.options, "routes");
          const parsedRoutes = routes === undefined ? undefined : jsonOption(parsed.options, "routes", undefined);
          output(bridge.registerEndpoint({ id: option(parsed.options, "id"), host_kind: hostKind, session_ref: option(parsed.options, "session_ref", "cli")!, capabilities: jsonOption(parsed.options, "capabilities", undefined), routes: parsedRoutes }));
        }
        break;
      case "endpoint-renew":
        output(bridge.renewEndpoint(parsed.rest[0], option(parsed.options, "lease_token")!, Number(option(parsed.options, "lease_ms", "3600000"))));
        break;
      case "takeover":
        output(bridge.takeover(option(parsed.options, "attention_channel", "default")!, option(parsed.options, "endpoint_id")!, option(parsed.options, "expected_generation") == null ? undefined : Number(option(parsed.options, "expected_generation"))));
        break;
      case "presence-renew":
        output(bridge.renewPresence({ attention_channel: option(parsed.options, "attention_channel", "default")!, endpoint_id: option(parsed.options, "endpoint_id")!, generation: Number(option(parsed.options, "generation", "1")), lease_token: option(parsed.options, "lease_token"), ttl_ms: Number(option(parsed.options, "ttl_ms", "1200000")), observed_by: option(parsed.options, "observed_by", "cli")!, observation: option(parsed.options, "observation", "user_message_accepted") }));
        break;
      case "tick":
        output(bridge.tick());
        break;
      case "dispatch":
        output(await bridge.dispatchDue());
        break;
      case "inspect":
        output(bridge.inspect());
        break;
      case "status": {
        const status = operatorStatus(bridge);
        output(status);
        if (status.attention_required) process.exitCode = 2;
        break;
      }
      case "batch-retry": {
        const batchId = parsed.rest[0];
        const expectedAttempt = Number(option(parsed.options, "expected_attempt"));
        const reason = option(parsed.options, "reason");
        if (!batchId || !Number.isSafeInteger(expectedAttempt) || expectedAttempt < 1 || !reason) {
          throw new Error("batch-retry requires BATCH_ID, --expected-attempt, and --reason");
        }
        output(retryDeadLetter(bridge, { batch_id: batchId, expected_attempt: expectedAttempt, reason }));
        break;
      }
      case "events":
        output(bridge.listEvents());
        break;
      case "claims":
        output(bridge.listClaims());
        break;
      case "batches":
        output(bridge.listBatches());
        break;
      case "receipts":
        output(bridge.listReceipts(parsed.rest[0]));
        break;
      case "bindings":
        output(bridge.listBindings());
        break;
      case "endpoints":
        output(bridge.listEndpoints());
        break;
      default:
        throw new Error(`unknown command: ${parsed.command}`);
    }
  } finally {
    bridge.close();
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
