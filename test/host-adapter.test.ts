import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { startDaemon } from "../src/daemon.js";
import {
  validateEndpointForAdapter,
  validateHostAdapterManifest,
  type HostAdapter,
  type HostAdapterLifecycleContext,
} from "../src/transport-sdk.js";
import { HostAdapterRegistry } from "../src/host-adapter-registry.js";
import type { TransportContext, TransportResult } from "../src/types.js";

function dbPath(): string {
  return join(mkdtempSync(join(tmpdir(), "wake-bridge-host-adapter-")), "bridge.sqlite");
}

class LifecycleAdapter implements HostAdapter {
  readonly kind = "lifecycle_test";
  readonly capabilities = { warm_resume: true, cold_push: false };
  readonly manifest = {
    contract_version: 1 as const,
    adapter_kind: this.kind,
    adapter_version: "0.0.1",
    host_kinds: ["test_host"],
    support_tier: "mock" as const,
    tested_host_versions: ["fixture"],
    capabilities: this.capabilities,
    receipt_upper_bound: "accepted_to_live_pipe" as const,
  };
  starts: HostAdapterLifecycleContext[] = [];
  stops = 0;

  start(context: HostAdapterLifecycleContext): void {
    this.starts.push(context);
  }

  stop(): void {
    this.stops += 1;
  }

  dispatch(_context: TransportContext): TransportResult {
    return { accepted: true, transport_kind: this.kind };
  }
}

describe("public Host Adapter contract", () => {
  it("validates manifests, endpoint routes and duplicate adapter kinds", () => {
    const adapter = new LifecycleAdapter();
    expect(validateHostAdapterManifest(adapter.manifest)).toBe(adapter.manifest);
    expect(() => validateEndpointForAdapter({
      host_kind: "test_host",
      session_ref: "one",
      routes: [{ kind: adapter.kind, address: {} }],
    }, adapter.manifest)).not.toThrow();
    expect(() => validateEndpointForAdapter({ host_kind: "other", session_ref: "one", routes: [] }, adapter.manifest)).toThrow(/not supported/);
    expect(() => new HostAdapterRegistry([adapter, new LifecycleAdapter()])).toThrow(/duplicate host adapter/);
  });

  it("starts and stops daemon adapters through a generic lifecycle", async () => {
    const adapter = new LifecycleAdapter();
    const config = { instance_id: "i", owner_id: "o", db_path: dbPath(), timezone: "UTC" };
    const bridge = new WakeBridge(config, { autoMockTransport: false });
    const daemon = await startDaemon(config, { bridge, host: "127.0.0.1", port: 0, scheduler_interval_ms: 0, host_adapters: [adapter], unsafe_no_auth: true });

    expect(adapter.starts).toHaveLength(1);
    expect(adapter.starts[0]).not.toHaveProperty("bridge");
    expect(adapter.starts[0].instance_id).toBe("i");
    expect(adapter.starts[0].daemon_origin).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/u);
    expect(daemon.adapters()).toMatchObject([{ adapter_kind: "lifecycle_test", support_tier: "mock" }]);
    expect(bridge.transports.get(adapter.kind)).toBe(adapter);

    await daemon.close();
    expect(adapter.stops).toBe(1);
    expect(bridge.transports.has(adapter.kind)).toBe(false);
    bridge.close();
  });

  it("does not guess mock transport for an unrouted real-host endpoint", async () => {
    const bridge = new WakeBridge({ instance_id: "i", owner_id: "o", db_path: dbPath(), timezone: "UTC" });
    const endpoint = bridge.registerEndpoint({ host_kind: "unconfigured_host", session_ref: "unrouted" });
    bridge.takeover("default", endpoint.id, 0);
    bridge.emit("manual", { type: "job", dedupe_key: "unrouted", resource: { uri: "job://unrouted" } });
    bridge.tick();

    await expect(bridge.dispatchDue()).resolves.toMatchObject([{ status: "waiting_for_endpoint", error_class: "no_compatible_transport" }]);
    expect(bridge.mockTransport.calls).toHaveLength(0);
    bridge.close();
  });

  it("scopes host bootstrap authority and fences resumed session leases", async () => {
    const adapter = new LifecycleAdapter();
    const unsafeConfig = { instance_id: "unsafe", owner_id: "o", db_path: dbPath(), timezone: "UTC" };
    const unsafeBridge = new WakeBridge(unsafeConfig, { autoMockTransport: false });
    await expect(startDaemon(unsafeConfig, {
      bridge: unsafeBridge,
      host_adapters: [adapter],
      host_credentials: [{
        id: "unsafe",
        token: "host-bootstrap-secret-0123456789abcdef",
        adapter_kind: "lifecycle_test",
        host_kind: "test_host",
        attention_channels: ["life"],
      }],
    })).rejects.toThrow(/admin_token is required/);
    unsafeBridge.close();

    const config = { instance_id: "i", owner_id: "o", db_path: dbPath(), timezone: "UTC", admin_token: "owner-secret-0123456789abcdef012345" };
    const bridge = new WakeBridge(config, { autoMockTransport: false });
    const daemon = await startDaemon(config, {
      bridge,
      host: "127.0.0.1",
      port: 0,
      scheduler_interval_ms: 0,
      host_adapters: [adapter],
      host_credentials: [{
        id: "fixture-host",
        token: "host-bootstrap-secret-0123456789abcdef",
        adapter_kind: "lifecycle_test",
        host_kind: "test_host",
        attention_channels: ["life"],
      }],
    });
    const address = daemon.address as { port: number };
    const origin = `http://127.0.0.1:${address.port}`;
    const post = (path: string, token: string, body: Record<string, unknown>) => fetch(`${origin}${path}`, {
      method: "POST",
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const registration = {
      adapter_kind: "lifecycle_test",
      session_ref: "session-1",
      attention_channel: "life",
      route_address: { base_url: "http://127.0.0.1:18789/", token: "route-secret-0123456789abcdef0123456789" },
    };

    try {
      expect((await post("/v1/host-sessions/open", "wrong", registration)).status).toBe(401);
      const openedResponse = await post("/v1/host-sessions/open", "host-bootstrap-secret-0123456789abcdef", registration);
      expect(openedResponse.status).toBe(201);
      const opened = await openedResponse.json() as any;
      expect(opened.endpoint).toMatchObject({ host_kind: "test_host", session_ref: "session-1" });
      expect(opened.endpoint.lease_token).toBeTruthy();
      expect(JSON.stringify(opened.endpoint.routes)).not.toContain("route-secret-0123456789abcdef0123456789");
      expect(opened.binding).toMatchObject({ attention_channel: "life", generation: 1, endpoint_id: opened.endpoint.id });

      const renewedResponse = await post("/v1/host-sessions/renew", "host-bootstrap-secret-0123456789abcdef", {
        adapter_kind: "lifecycle_test",
        session_ref: "session-1",
        attention_channel: "life",
        expected_generation: 1,
      });
      expect(renewedResponse.status).toBe(200);
      const renewed = await renewedResponse.json() as any;
      expect(renewed.binding).toMatchObject({ attention_channel: "life", generation: 1, endpoint_id: opened.endpoint.id });
      expect(renewed.endpoint.lease_expires_at).toBeTruthy();

      expect((await fetch(`${origin}/v1/inspect`, { headers: { authorization: "Bearer host-bootstrap-secret-0123456789abcdef" } })).status).toBe(401);
      const ownerInspect = await fetch(`${origin}/v1/inspect`, { headers: { authorization: "Bearer owner-secret-0123456789abcdef012345" } });
      expect(await ownerInspect.text()).not.toContain("route-secret-0123456789abcdef0123456789");
      expect((await post(`/v1/host-sessions/${opened.endpoint.id}/renew`, "host-bootstrap-secret-0123456789abcdef", {})).status).toBe(409);
      expect((await post(`/v1/host-sessions/${opened.endpoint.id}/renew`, opened.endpoint.lease_token, {})).status).toBe(200);

      const resumedResponse = await post("/v1/host-sessions/open", "host-bootstrap-secret-0123456789abcdef", registration);
      expect(resumedResponse.status).toBe(201);
      const resumed = await resumedResponse.json() as any;
      expect(resumed.endpoint.id).toBe(opened.endpoint.id);
      expect(resumed.binding.generation).toBe(2);
      expect((await post("/v1/host-sessions/renew", "host-bootstrap-secret-0123456789abcdef", {
        adapter_kind: "lifecycle_test",
        session_ref: "session-1",
        attention_channel: "life",
        expected_generation: 1,
      })).status).toBe(409);
      expect((await post(`/v1/host-sessions/${opened.endpoint.id}/renew`, opened.endpoint.lease_token, {})).status).toBe(409);
      expect((await post("/v1/host-sessions/close", "host-bootstrap-secret-0123456789abcdef", {
        adapter_kind: "lifecycle_test",
        session_ref: "session-1",
        attention_channel: "life",
        expected_generation: 1,
      })).status).toBe(409);
      expect(new Date(bridge.getEndpoint(resumed.endpoint.id)!.lease_expires_at).getTime()).toBeGreaterThan(Date.now());
      expect((await post("/v1/host-sessions/close", "host-bootstrap-secret-0123456789abcdef", {
        adapter_kind: "lifecycle_test",
        session_ref: "session-1",
        attention_channel: "life",
        expected_generation: 2,
      })).status).toBe(200);

      const reopenedResponse = await post("/v1/host-sessions/open", "host-bootstrap-secret-0123456789abcdef", registration);
      const reopened = await reopenedResponse.json() as any;
      expect(reopened.binding.generation).toBe(3);
      expect((await post(`/v1/host-sessions/${reopened.endpoint.id}/close`, reopened.endpoint.lease_token, {})).status).toBe(200);
      expect(new Date(bridge.getEndpoint(reopened.endpoint.id)!.lease_expires_at).getTime()).toBeLessThanOrEqual(Date.now());
    } finally {
      await daemon.close();
      bridge.close();
    }
  });
});
