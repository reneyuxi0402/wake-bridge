import { createServer } from "node:http";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { startDaemon } from "../src/daemon.js";
import { initializeInstance } from "../src/instance-config.js";
import { LocalHttpHostAdapter, loadOutOfProcessHostFile } from "../src/out-of-process-host.js";
import {
  HostSessionClient,
  validateLocalHostDeliveryRequest,
  type HostAdapterManifest,
  type LocalHostDeliveryRequest,
  type TransportContext,
} from "../src/transport-sdk.js";
import type { BridgeConfig } from "../src/types.js";

const HOST_TOKEN = "external-host-bootstrap-token-0123456789abcdef";
const ROUTE_TOKEN = "external-host-route-token-0123456789abcdef";

function config(): BridgeConfig {
  return {
    instance_id: "external-host-instance",
    owner_id: "external-host-owner",
    db_path: join(mkdtempSync(join(tmpdir(), "wake-bridge-external-host-")), "bridge.sqlite"),
    timezone: "UTC",
    admin_token: "external-host-owner-token-0123456789abcdef",
  };
}

function manifest(): HostAdapterManifest {
  return {
    contract_version: 1,
    adapter_kind: "fixture_host_bridge",
    adapter_version: "1.0.0",
    host_kinds: ["fixture_runner"],
    support_tier: "experimental",
    tested_host_versions: ["fixture-1"],
    capabilities: {
      cold_push: false,
      warm_resume: true,
      exact_live_route: true,
      requires_live_binding: true,
      queue_when_busy: true,
      foreground_presence_observable: true,
      session_activity_observable: true,
      receipt_stages: ["transport_accepted"],
      receipt_upper_bound: "host_accepted",
      support_tier: "experimental",
    },
    receipt_upper_bound: "host_accepted",
  };
}

async function listen(server: ReturnType<typeof createServer>): Promise<string> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

describe("out-of-process local Host Adapter protocol", () => {
  it("loads only declared experimental adapters and keeps bootstrap secrets in env", () => {
    const root = mkdtempSync(join(tmpdir(), "wake-bridge-host-file-"));
    const path = join(root, "hosts.json");
    writeFileSync(path, JSON.stringify({
      version: 1,
      adapters: [{
        id: "fixture-host",
        adapter_kind: "fixture_host_bridge",
        adapter_version: "1.0.0",
        host_kind: "fixture_runner",
        tested_host_versions: ["fixture-1"],
        token_env: "WAKEBRIDGE_FIXTURE_HOST_TOKEN",
        attention_channels: ["life"],
        capabilities: { exact_live_route: true, requires_live_binding: true, queue_when_busy: true },
        receipt_upper_bound: "host_accepted",
      }],
    }));

    const loaded = loadOutOfProcessHostFile(path, { WAKEBRIDGE_FIXTURE_HOST_TOKEN: HOST_TOKEN });
    expect(loaded.adapters).toHaveLength(1);
    expect(loaded.adapters[0].manifest).toMatchObject({
      adapter_kind: "fixture_host_bridge",
      host_kinds: ["fixture_runner"],
      support_tier: "experimental",
      receipt_upper_bound: "host_accepted",
      capabilities: { exact_live_route: true, requires_live_binding: true, receipt_stages: ["transport_accepted"] },
    });
    expect(loaded.credentials).toEqual([{
      id: "fixture-host",
      token: HOST_TOKEN,
      adapter_kind: "fixture_host_bridge",
      host_kind: "fixture_runner",
      attention_channels: ["life"],
    }]);
    expect(readFileSync(path, "utf8")).not.toContain(HOST_TOKEN);

    writeFileSync(path, JSON.stringify({
      version: 1,
      adapters: [{
        id: "invalid-host", adapter_kind: "invalid_host", adapter_version: "1", host_kind: "invalid_host",
        tested_host_versions: [], token_env: "WAKEBRIDGE_FIXTURE_HOST_TOKEN", attention_channels: ["life"],
        capabilities: { warm_resume: true }, receipt_upper_bound: "host_accepted",
      }],
    }));
    expect(() => loadOutOfProcessHostFile(path, { WAKEBRIDGE_FIXTURE_HOST_TOKEN: HOST_TOKEN })).toThrow(/exact_live_route/u);
  });

  it("reports external adapter readiness through doctor without printing credentials", () => {
    const root = mkdtempSync(join(tmpdir(), "wake-bridge-host-doctor-"));
    const initialized = initializeInstance({
      data_dir: join(root, "agent-space"), instance_id: "host-doctor", owner_id: "owner", timezone: "UTC",
    });
    const path = join(root, "hosts.json");
    writeFileSync(path, JSON.stringify({
      version: 1,
      adapters: [{
        id: "fixture-host", adapter_kind: "fixture_host_bridge", adapter_version: "1.0.0", host_kind: "fixture_runner",
        tested_host_versions: ["fixture-1"], token_env: "WAKEBRIDGE_FIXTURE_HOST_TOKEN", attention_channels: ["life"],
        capabilities: { warm_resume: true, exact_live_route: true, requires_live_binding: true }, receipt_upper_bound: "host_accepted",
      }],
    }));
    const doctor = spawnSync(process.execPath, [
      "dist/src/cli.js", "doctor", "--config", initialized.config_path, "--host-adapters", path,
    ], { cwd: process.cwd(), encoding: "utf8", env: { ...process.env, WAKEBRIDGE_FIXTURE_HOST_TOKEN: HOST_TOKEN } });
    expect(doctor.status, doctor.stderr).toBe(0);
    expect(JSON.parse(doctor.stdout)).toMatchObject({
      ok: true,
      hosts: { configured_adapters: { configured: true, adapter_count: 1, adapters: [{ adapter_kind: "fixture_host_bridge", support_tier: "experimental" }], error: null } },
    });
    expect(doctor.stdout).not.toContain(HOST_TOKEN);
  });

  it("registers an exact external session, delivers once, observes its wake echo, and fails closed after exit", async () => {
    const deliveries: LocalHostDeliveryRequest[] = [];
    const hostServer = createServer(async (request, response) => {
      let raw = "";
      for await (const chunk of request) raw += String(chunk);
      if (request.method !== "POST" || request.url !== "/v1/wakes") return void response.writeHead(404).end();
      if (request.headers.authorization !== `Bearer ${ROUTE_TOKEN}`) return void response.writeHead(401).end();
      const body = validateLocalHostDeliveryRequest(JSON.parse(raw));
      if (request.headers["x-wakebridge-delivery-nonce"] !== body.delivery_nonce) return void response.writeHead(400).end();
      deliveries.push(body);
      response.writeHead(202).end();
    });
    const hostOrigin = await listen(hostServer);
    const configValue = config();
    const bridge = new WakeBridge(configValue, { autoMockTransport: false, recoverDispatchLeases: false });
    const daemon = await startDaemon(configValue, {
      bridge,
      host: "127.0.0.1",
      port: 0,
      scheduler_interval_ms: 0,
      host_adapters: [new LocalHttpHostAdapter(manifest())],
      host_credentials: [{
        id: "fixture-host", token: HOST_TOKEN, adapter_kind: "fixture_host_bridge",
        host_kind: "fixture_runner", attention_channels: ["life"],
      }],
    });
    const daemonOrigin = `http://127.0.0.1:${(daemon.address as AddressInfo).port}`;
    const client = new HostSessionClient({ base_url: daemonOrigin, host_token: HOST_TOKEN, adapter_kind: "fixture_host_bridge" });
    try {
      const registration = await client.open({
        session_ref: "opaque-session-one",
        attention_channel: "life",
        route_origin: hostOrigin,
        route_token: ROUTE_TOKEN,
      });
      const lease = client.lease(registration);
      expect(registration.endpoint).toMatchObject({ host_kind: "fixture_runner", session_ref: "opaque-session-one" });
      expect(registration.binding).toMatchObject({ attention_channel: "life", generation: 1 });
      expect(JSON.stringify(registration)).not.toContain(ROUTE_TOKEN);

      bridge.emit("fixture", {
        type: "external.host.test",
        dedupe_key: "external:one",
        attention_channel_hint: "life",
        resource: { uri: "fixture://external/one" },
      });
      bridge.tick();
      await expect(bridge.dispatchDue()).resolves.toMatchObject([{ status: "accepted" }]);
      expect(deliveries).toHaveLength(1);
      expect(deliveries[0]).toMatchObject({
        protocol_version: 1,
        wake: { attention_channel: "life", binding_generation: 1, claim_refs: [{ resource: { uri: "fixture://external/one" } }] },
      });
      expect(bridge.listReceipts(deliveries[0].wake.wake_batch_id)).toMatchObject([{
        stage: "transport_accepted",
        details: { protocol_version: 1, receipt_upper_bound: "host_accepted", host_status: 202 },
      }]);

      await expect(client.consumeWakeEcho(lease, {
        observation_id: "wake-echo-one",
        delivery_nonce: deliveries[0].delivery_nonce,
      })).resolves.toMatchObject({ duplicate: false });
      expect(bridge.getBatch(deliveries[0].wake.wake_batch_id)).toMatchObject({ state: "dispatched" });
      expect(bridge.listDeliveryCorrelations()).toMatchObject([{ state: "consumed", transport_kind: "fixture_host_bridge" }]);

      await expect(client.renewPresence(lease, {
        observed_by: "fixture_user_edge",
        observation: "user_message_accepted",
        ttl_ms: 60_000,
      })).resolves.toMatchObject({
        endpoint_id: lease.endpoint_id,
        binding_generation: lease.generation,
        observed_by: "fixture_user_edge",
        observation: "user_message_accepted",
      });
      expect(bridge.getPresence("life")?.expires_at).toBeTruthy();

      await client.close(lease);
      expect(bridge.getPresence("life")).toBeNull();
      await expect(client.renewPresence(lease, { observed_by: "stale_fixture" })).rejects.toMatchObject({
        code: "host_session_request_failed",
        status: 409,
      });
      bridge.emit("fixture", {
        type: "external.host.after-close",
        dedupe_key: "external:two",
        attention_channel_hint: "life",
        resource: { uri: "fixture://external/two" },
      });
      bridge.tick();
      await expect(bridge.dispatchDue()).resolves.toMatchObject([{ status: "waiting_for_endpoint" }]);
      expect(bridge.listBatches().some((batch) => batch.state === "waiting_for_endpoint")).toBe(true);
      expect(deliveries).toHaveLength(1);

      const encodedStatus = JSON.stringify(await fetch(`${daemonOrigin}/v1/status`, {
        headers: { authorization: `Bearer ${configValue.admin_token}` },
      }).then((response) => response.json()));
      expect(encodedStatus).not.toContain(ROUTE_TOKEN);
      expect(encodedStatus).not.toContain(HOST_TOKEN);
      expect(encodedStatus).not.toContain("opaque-session-one");
    } finally {
      await daemon.close();
      bridge.close();
      await new Promise<void>((resolve) => hostServer.close(() => resolve()));
    }
  });

  it("rejects non-loopback routes and credential reuse before registration", async () => {
    expect(() => new HostSessionClient({
      base_url: "https://bridge.example.com",
      host_token: HOST_TOKEN,
      adapter_kind: "fixture_host_bridge",
    })).toThrow(/loopback/u);

    const configValue = config();
    const bridge = new WakeBridge(configValue, { autoMockTransport: false, recoverDispatchLeases: false });
    const daemon = await startDaemon(configValue, {
      bridge,
      host: "127.0.0.1",
      port: 0,
      scheduler_interval_ms: 0,
      host_adapters: [new LocalHttpHostAdapter(manifest())],
      host_credentials: [{
        id: "fixture-host", token: HOST_TOKEN, adapter_kind: "fixture_host_bridge",
        host_kind: "fixture_runner", attention_channels: ["life"],
      }],
    });
    const daemonOrigin = `http://127.0.0.1:${(daemon.address as AddressInfo).port}`;
    const client = new HostSessionClient({ base_url: daemonOrigin, host_token: HOST_TOKEN, adapter_kind: "fixture_host_bridge" });
    try {
      await expect(client.open({
        session_ref: "same-token",
        attention_channel: "life",
        route_origin: "http://127.0.0.1:18789",
        route_token: HOST_TOKEN,
      })).rejects.toThrow(/credential_scope_collision/u);
      await expect(client.open({
        session_ref: "owner-token",
        attention_channel: "life",
        route_origin: "http://127.0.0.1:18789",
        route_token: configValue.admin_token!,
      })).rejects.toThrow(/credential_scope_collision/u);
      await expect(client.open({
        session_ref: "remote-route",
        attention_channel: "life",
        route_origin: "https://host.example.com",
        route_token: ROUTE_TOKEN,
      })).rejects.toThrow(/loopback/u);
      expect(bridge.listEndpoints()).toHaveLength(0);
    } finally {
      await daemon.close();
      bridge.close();
    }
  });

  it("maps bounded HTTP status semantics without reading provider response bodies", async () => {
    const context = {
      batch: {
        id: "wb_fixture", instance_id: "i", policy_id: "default", policy_version: 1, attention_channel: "life",
        claim_ids: ["clm_fixture"], event_ids: ["evt_fixture"], state: "dispatching", not_before: "2026-08-31T00:00:00.000Z",
        attempt: 1, created_at: "2026-08-31T00:00:00.000Z", updated_at: "2026-08-31T00:00:00.000Z",
      },
      endpoint: {
        id: "ep_fixture", instance_id: "i", host_kind: "fixture_runner", session_ref: "session",
        lease_expires_at: "2026-08-31T01:00:00.000Z", capabilities: manifest().capabilities,
        routes: [{ kind: "fixture_host_bridge", address: { base_url: "http://127.0.0.1:18789/", token: ROUTE_TOKEN } }],
        registered_at: "2026-08-31T00:00:00.000Z", updated_at: "2026-08-31T00:00:00.000Z",
      },
      binding: { instance_id: "i", attention_channel: "life", endpoint_id: "ep_fixture", generation: 1, bound_at: "2026-08-31T00:00:00.000Z" },
      payload: { schema_version: 1, instance_id: "i", wake_batch_id: "wb_fixture", attention_channel: "life", claim_refs: [], binding_generation: 1 },
      attempt_id: "attempt_fixture",
      delivery_nonce: "delivery-nonce-0123456789abcdef0123456789",
    } satisfies TransportContext;
    const result = async (status: number) => new LocalHttpHostAdapter(manifest(), {
      fetch: (async () => new Response("provider secret must not be read", { status })) as typeof fetch,
    }).dispatch(context);

    await expect(result(425)).resolves.toMatchObject({ accepted: false, retryable: true, error_class: "host_temporarily_unavailable" });
    await expect(result(503)).resolves.toMatchObject({ accepted: false, retryable: true, error_class: "host_temporarily_unavailable" });
    await expect(result(400)).resolves.toMatchObject({ accepted: false, retryable: false, error_class: "host_rejected" });
    expect(JSON.stringify(await result(400))).not.toContain("provider secret");
  });
});
