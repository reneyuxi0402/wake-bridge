import { execFileSync, spawn, spawnSync, type ChildProcess } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import type { Server } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

const children = new Set<ChildProcess>();
const servers = new Set<Server>();

afterEach(async () => {
  await Promise.all([...children].map(async (child) => {
    if (child.exitCode === null) child.kill("SIGTERM");
    await new Promise<void>((done) => child.once("exit", () => done()));
  }));
  children.clear();
  await Promise.all([...servers].map((server) => new Promise<void>((done) => server.close(() => done()))));
  servers.clear();
});

function run(file: string, args: string[], cwd: string): string {
  return execFileSync(file, args, {
    cwd,
    encoding: "utf8",
    env: { ...process.env, npm_config_cache: join(tmpdir(), "wake-bridge-release-npm-cache") },
    timeout: 60_000,
  });
}

async function daemonAddress(
  binary: string,
  config: string,
  cwd: string,
  options: { args?: string[]; env?: NodeJS.ProcessEnv } = {},
): Promise<{ child: ChildProcess; address: string }> {
  const child = spawn(binary, ["daemon", "--config", config, "--host", "127.0.0.1", "--port", "0", ...(options.args ?? [])], {
    cwd,
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, ...options.env },
  });
  children.add(child);
  let stdout = "";
  let stderr = "";
  child.stdout!.on("data", (chunk) => { stdout += String(chunk); });
  child.stderr!.on("data", (chunk) => { stderr += String(chunk); });
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const host = /"address":\s*"([^"]+)"/u.exec(stdout)?.[1];
    const port = /"port":\s*(\d+)/u.exec(stdout)?.[1];
    if (host && port) return { child, address: `http://${host}:${port}` };
    if (child.exitCode !== null) throw new Error(`installed daemon exited ${child.exitCode}: ${stderr || stdout}`);
    await new Promise((done) => setTimeout(done, 50));
  }
  throw new Error(`timed out waiting for installed daemon: ${stderr || stdout}`);
}

describe("published tarball", () => {
  it("installs outside the repository and completes import, init, daemon, emit, inspect, backup, and service profile", async () => {
    const root = mkdtempSync(join(tmpdir(), "wake-bridge-package-"));
    try {
      const packDirectory = join(root, "pack");
      const installRoot = join(root, "consumer");
      const dataDirectory = join(root, "agent-space");
      mkdirSync(packDirectory);
      mkdirSync(installRoot);
      const packed = JSON.parse(run("npm", ["pack", "--ignore-scripts", "--json", "--pack-destination", packDirectory], resolve("."))) as Array<{
        filename: string;
        files: Array<{ path: string }>;
      }>;
      const tarball = join(packDirectory, packed[0].filename);
      const paths = packed[0].files.map((file) => file.path);
      expect(paths).toEqual(expect.arrayContaining([
        "LICENSE",
        "README.md",
        "dist/src/index.js",
        "dist/src/event-sdk.js",
        "dist/src/source-sdk.js",
        "dist/src/transport-sdk.js",
        "dist/src/out-of-process-host.js",
        "dist/src/release-lifecycle.js",
        "docs/quickstart.md",
        "docs/verification.md",
        "docs/operations/release-lifecycle.md",
        "docs/operations/operator-control.md",
        "docs/operations/out-of-process-host-adapter.md",
        "docs/operations/connector-catalog.md",
        "docs/adapters/botlingknows.md",
        "docs/adapters/gmail-planned.md",
        "docs/operations/source-connectors.md",
        "examples/reference-host-adapter/adapter.mjs",
        "examples/reference-host-adapter/host-adapters.json.example",
        "examples/policies/botlingknows-conservative.json",
        "docs/operations/templates/sources.json.example",
        "docs/releases/0.9.0-preview.8.md",
        "schemas/event-v1.schema.json",
        "schemas/policy-v1.schema.json",
      ]));
      expect(paths.some((path) => path.startsWith("test/") || path.startsWith("src/"))).toBe(false);

      run("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund", "--prefix", installRoot, tarball], root);
      const binary = join(installRoot, "node_modules", ".bin", "wakebridge");
      expect(existsSync(binary)).toBe(true);
      const installedPackage = JSON.parse(readFileSync(join(installRoot, "node_modules", "wake-bridge", "package.json"), "utf8")) as { version: string };
      expect(run(process.execPath, ["--input-type=module", "--eval",
        "import('wake-bridge').then(m => { if (m.WakeBridge || m.releasePreflight || m.EVENT_CONTRACT_VERSION !== 1 || m.SOURCE_ADAPTER_CONTRACT_VERSION !== 1 || m.HOST_ADAPTER_CONTRACT_VERSION !== 1 || m.connectorCatalog().length !== 3) process.exit(2); console.log(m.RELEASE_VERSION); })"], installRoot).trim())
        .toBe(installedPackage.version);

      writeFileSync(join(installRoot, "package.json"), JSON.stringify({ type: "module" }));
      writeFileSync(join(installRoot, "consumer.ts"), `
        import { EVENT_CONTRACT_VERSION, validateWakeEventInput } from "wake-bridge/event";
        import { SOURCE_ADAPTER_CONTRACT_VERSION, validateSourceManifest, type PullSourceAdapter } from "wake-bridge/source";
        import {
          HOST_ADAPTER_CONTRACT_VERSION, LOCAL_HOST_PROTOCOL_VERSION, HostSessionClient,
          validateHostAdapterManifest, validateLocalHostDeliveryRequest, type HostAdapter
        } from "wake-bridge/transport";

        const fixtureEvent = { schema_version: EVENT_CONTRACT_VERSION, type: "fixture", dedupe_key: "one", resource: { uri: "fixture://one" } };
        const source: PullSourceAdapter = {
          manifest: {
            contract_version: SOURCE_ADAPTER_CONTRACT_VERSION,
            id: "fixture", version: "1.0.0", subject_ref: "fixture:owner",
            binding_fingerprint: "sha256:${"1".repeat(64)}", read_side_effects: "none",
            credential_custody: "none", upstream_credential_breadth: "none",
            upstream_credential_scopes: [], connector_capabilities: ["events.read"]
          },
          poll: ({ cursor }) => ({ events: [fixtureEvent], next_cursor: cursor })
        };
        const host: HostAdapter = {
          kind: "fixture_host",
          manifest: {
            contract_version: HOST_ADAPTER_CONTRACT_VERSION, adapter_kind: "fixture_host", adapter_version: "1.0.0",
            host_kinds: ["fixture"], support_tier: "mock", tested_host_versions: ["fixture"],
            capabilities: { warm_resume: true }, receipt_upper_bound: "accepted_to_live_pipe"
          },
          start: (context) => { if ("bridge" in context) throw new Error("Core leaked into public lifecycle"); },
          dispatch: () => ({ accepted: true, transport_kind: "fixture_host" })
        };
        validateSourceManifest(source.manifest);
        validateHostAdapterManifest(host.manifest);
        if (LOCAL_HOST_PROTOCOL_VERSION !== 1 || typeof HostSessionClient !== "function") throw new Error("local host protocol missing");
        validateLocalHostDeliveryRequest({
          protocol_version: 1, attempt_id: "attempt", delivery_nonce: "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn",
          wake: { schema_version: 1, instance_id: "i", wake_batch_id: "b", attention_channel: "default", claim_refs: [], binding_generation: 1 }
        });
        validateWakeEventInput(fixtureEvent);
        await host.start?.({ instance_id: "fixture-instance", daemon_origin: null });
        console.log("public-sdk-ok");
      `);
      run(resolve("node_modules/typescript/bin/tsc"), [
        "consumer.ts", "--module", "NodeNext", "--moduleResolution", "NodeNext", "--target", "ES2022",
        "--strict", "--skipLibCheck", "--outDir", "built",
      ], installRoot);
      expect(run(process.execPath, [join(installRoot, "built", "consumer.js")], installRoot).trim()).toBe("public-sdk-ok");
      for (const privatePath of [
        "wake-bridge/core", "wake-bridge/db", "wake-bridge/daemon", "wake-bridge/operator-control",
        "wake-bridge/out-of-process-host", "wake-bridge/release-lifecycle",
      ]) {
        const denied = spawnSync(process.execPath, ["--input-type=module", "--eval", `import(${JSON.stringify(privatePath)})`], {
          cwd: installRoot, encoding: "utf8",
        });
        expect(denied.status).not.toBe(0);
        expect(denied.stderr).toMatch(/ERR_PACKAGE_PATH_NOT_EXPORTED/u);
      }

      expect(JSON.parse(run(binary, ["release-preflight"], installRoot))).toMatchObject({
        ok: true,
        runtime: { node: { supported: true }, sqlite3: { supported: true } },
      });
      const initialized = JSON.parse(run(binary, [
        "init", "--data-dir", dataDirectory, "--instance-id", "blackbox", "--owner-id", "consumer", "--timezone", "UTC",
      ], installRoot)) as { config_path: string };
      expect(JSON.parse(run(binary, ["release-preflight", "--config", initialized.config_path], installRoot)))
        .toMatchObject({ ok: true, instance: { schema_state: "current", integrity: "ok" } });
      expect(JSON.parse(run(binary, ["status", "--config", initialized.config_path], installRoot))).toMatchObject({
        ok: true,
        health: "healthy",
        queue: { counts: { dead_letter: 0, needs_attention: 0 } },
        sources: { runtime_observed: false },
      });

      const daemon = await daemonAddress(binary, initialized.config_path, installRoot);
      const initializedConfig = JSON.parse(readFileSync(initialized.config_path, "utf8")) as { admin_token: string; db_path: string };
      expect(await fetch(`${daemon.address}/health`, {
        headers: { authorization: `Bearer ${initializedConfig.admin_token}` },
      }).then((response) => response.json()))
        .toMatchObject({ ok: true, instance_id: "blackbox" });
      expect(JSON.parse(run(binary, [
        "emit", "--config", initialized.config_path, "--source", "blackbox", "--type", "package.installed",
        "--dedupe-key", "package-1", "--resource", "package://wake-bridge/preview",
      ], installRoot))).toMatchObject({ event: { source: "blackbox", type: "package.installed" } });
      expect(JSON.parse(run(binary, ["inspect", "--config", initialized.config_path], installRoot)))
        .toMatchObject({ events: [expect.objectContaining({ type: "package.installed" })] });
      daemon.child.kill("SIGTERM");
      await new Promise<void>((done) => daemon.child.once("exit", () => done()));
      children.delete(daemon.child);

      const routeToken = "installed-route-token-0123456789abcdef";
      const hostToken = "installed-host-token-0123456789abcdef";
      const deliveries: unknown[] = [];
      const routeServer = (await import("node:http")).createServer(async (request, response) => {
        let raw = "";
        for await (const chunk of request) raw += String(chunk);
        if (request.method !== "POST" || request.url !== "/v1/wakes"
          || request.headers.authorization !== `Bearer ${routeToken}`) {
          response.writeHead(401).end();
          return;
        }
        deliveries.push(JSON.parse(raw));
        response.writeHead(202).end();
      });
      servers.add(routeServer);
      await new Promise<void>((done, reject) => {
        routeServer.once("error", reject);
        routeServer.listen(0, "127.0.0.1", done);
      });
      const routePort = (routeServer.address() as { port: number }).port;
      const hostFile = join(root, "host-adapters.json");
      writeFileSync(hostFile, JSON.stringify({
        version: 1,
        adapters: [{
          id: "installed-host", adapter_kind: "installed_host_bridge", adapter_version: "1.0.0",
          host_kind: "fixture_runner", tested_host_versions: ["fixture"], token_env: "WAKEBRIDGE_INSTALLED_HOST_TOKEN",
          attention_channels: ["default"], capabilities: { warm_resume: true, exact_live_route: true, requires_live_binding: true },
          receipt_upper_bound: "host_accepted",
        }],
      }));
      const externalDaemon = await daemonAddress(binary, initialized.config_path, installRoot, {
        args: ["--scheduler-interval-ms", "0", "--host-adapters", hostFile],
        env: { WAKEBRIDGE_INSTALLED_HOST_TOKEN: hostToken },
      });
      const installedTransportLoader = join(installRoot, "transport-loader.mjs");
      writeFileSync(installedTransportLoader, "export * from \"wake-bridge/transport\";\n");
      const installedTransport = await import(pathToFileURL(installedTransportLoader).href) as typeof import("../src/transport-sdk.js");
      const sessionClient = new installedTransport.HostSessionClient({
        base_url: externalDaemon.address,
        host_token: hostToken,
        adapter_kind: "installed_host_bridge",
      });
      const session = await sessionClient.open({
        session_ref: "installed-session",
        attention_channel: "default",
        route_origin: `http://127.0.0.1:${routePort}`,
        route_token: routeToken,
      });
      const ownerHeaders = { authorization: `Bearer ${initializedConfig.admin_token}`, "content-type": "application/json" };
      const emittedResponse = await fetch(`${externalDaemon.address}/v1/events`, {
        method: "POST", headers: ownerHeaders,
        body: JSON.stringify({ source: "package", type: "external.host", dedupe_key: "external-host-1", resource: { uri: "package://external-host" } }),
      });
      expect(emittedResponse.status).toBe(201);
      const tickResponse = await fetch(`${externalDaemon.address}/v1/tick`, { method: "POST", headers: ownerHeaders, body: "{}" });
      expect(tickResponse.status).toBe(200);
      const dispatchResponse = await fetch(`${externalDaemon.address}/v1/dispatch`, { method: "POST", headers: ownerHeaders, body: "{}" });
      expect(dispatchResponse.status).toBe(200);
      const dispatchBody = await dispatchResponse.json() as { results: Array<{ status: string }> };
      expect(dispatchBody.results).toHaveLength(2);
      expect(dispatchBody.results.every((result) => result.status === "accepted")).toBe(true);
      expect(deliveries).toHaveLength(2);
      const externalDelivery = deliveries.map((delivery) => installedTransport.validateLocalHostDeliveryRequest(delivery))
        .find((delivery) => delivery.wake.claim_refs.some((claim) => claim.resource.uri === "package://external-host"));
      expect(externalDelivery).toMatchObject({
        protocol_version: 1, wake: { attention_channel: "default", claim_refs: [{ resource: { uri: "package://external-host" } }] },
      });
      await sessionClient.close(sessionClient.lease(session));
      externalDaemon.child.kill("SIGTERM");
      await new Promise<void>((done) => externalDaemon.child.once("exit", () => done()));
      children.delete(externalDaemon.child);
      await new Promise<void>((done) => routeServer.close(() => done()));
      servers.delete(routeServer);

      expect(JSON.parse(run(binary, [
        "backup", "--config", initialized.config_path, "--output", join(root, "backup"),
      ], installRoot))).toMatchObject({ ok: true, backup: { instance_id: "blackbox", source_schema_version: 8 } });

      execFileSync("sqlite3", [initializedConfig.db_path], {
        input: "DELETE FROM schema_migrations WHERE version > 6; PRAGMA user_version=6;\n",
        encoding: "utf8",
      });
      const fenced = spawnSync(binary, ["inspect", "--config", initialized.config_path], { cwd: installRoot, encoding: "utf8" });
      expect(fenced.status).toBe(1);
      expect(fenced.stderr).toMatch(/requires explicit upgrade/u);
      const preUpgrade = join(root, "pre-upgrade");
      expect(JSON.parse(run(binary, [
        "upgrade", "--config", initialized.config_path, "--backup-output", preUpgrade, "--confirm-offline",
      ], installRoot))).toMatchObject({ ok: true, upgraded: true, from_schema: 6, to_schema: 8 });
      expect(JSON.parse(run(binary, [
        "restore", "--config", initialized.config_path, "--backup", preUpgrade,
        "--rollback-output", join(root, "pre-restore"), "--confirm-instance-id", "blackbox", "--confirm-offline",
      ], installRoot))).toMatchObject({ ok: true, restored: true, schema_version: 6 });
      expect(execFileSync("sqlite3", [initializedConfig.db_path, "PRAGMA user_version;"], { encoding: "utf8" }).trim()).toBe("6");
      expect(JSON.parse(run(binary, [
        "upgrade", "--config", initialized.config_path, "--backup-output", join(root, "second-pre-upgrade"), "--confirm-offline",
      ], installRoot))).toMatchObject({ ok: true, upgraded: true, from_schema: 6, to_schema: 8 });

      const launchAgents = join(root, "LaunchAgents");
      const installedService = JSON.parse(run(binary, [
        "service", "install", "--config", initialized.config_path,
        "--launch-agents-dir", launchAgents, "--logs-dir", join(root, "logs"),
      ], installRoot)) as { service: { plist_path: string } };
      const configSecret = initializedConfig.admin_token;
      expect(readFileSync(installedService.service.plist_path, "utf8")).not.toContain(configSecret);
      expect(JSON.parse(run(binary, [
        "service", "uninstall", "--config", initialized.config_path, "--launch-agents-dir", launchAgents,
      ], installRoot))).toMatchObject({ service: { removed: true, data_preserved: true } });
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 120_000);
});
