import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { mkdtempSync, writeFileSync } from "node:fs";
import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { startDaemon } from "../src/daemon.js";
import {
  loadSourceConnectorFile,
  SourceSupervisor,
  validateSourceConnectorConfig,
} from "../src/source-connector.js";

function temporary(name: string): string {
  return join(mkdtempSync(join(tmpdir(), "wake-bridge-connector-")), name);
}

function send(response: ServerResponse, status: number, value: unknown): void {
  response.statusCode = status;
  response.setHeader("content-type", "application/json");
  response.end(JSON.stringify(value));
}

async function body(request: IncomingMessage): Promise<any> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk as Uint8Array));
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : null;
}

async function fixtureConnector(token: string): Promise<{
  baseUrl: string;
  calls: Array<{ path: string; authorization?: string; body: any }>;
  setIdentity: (subjectRef: string, bindingFingerprint: string) => void;
  close: () => Promise<void>;
}> {
  const calls: Array<{ path: string; authorization?: string; body: any }> = [];
  let page = 0;
  let subjectRef = "fixture:owner";
  let bindingFingerprint = `sha256:${"3".repeat(64)}`;
  const server = createServer(async (request, response) => {
    const call = {
      path: request.url || "",
      authorization: request.headers.authorization,
      body: request.method === "POST" ? await body(request) : null,
    };
    calls.push(call);
    if (call.authorization !== `Bearer ${token}`) {
      send(response, 401, { error: "unauthorized" });
      return;
    }
    if (request.method === "GET" && request.url === "/v1/manifest") {
      send(response, 200, {
        contract_version: 1,
        id: "fixture-notifications",
        version: "1.0.0",
        subject_ref: subjectRef,
        binding_fingerprint: bindingFingerprint,
        read_side_effects: "none",
        credential_custody: "connector",
        upstream_credential_breadth: "broad",
        upstream_credential_scopes: ["provider.account"],
        connector_capabilities: ["notifications.read"],
      });
      return;
    }
    if (request.method === "POST" && request.url === "/v1/bootstrap") {
      send(response, 200, { cursor: { page: 0 } });
      return;
    }
    if (request.method === "POST" && request.url === "/v1/poll") {
      page += 1;
      send(response, 200, page === 1 ? {
        events: [{
          type: "notification",
          occurred_at: "2026-08-29T05:00:00.000Z",
          dedupe_key: "notification:1",
          resource: { uri: "fixture://notification/1", cursor: 1 },
          metadata: { notification_id: 1 },
        }],
        next_cursor: { page: 1 },
        has_more: true,
      } : {
        events: [],
        next_cursor: { page: 2 },
        has_more: false,
      });
      return;
    }
    send(response, 404, { error: "not_found" });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address() as AddressInfo;
  return {
    baseUrl: `http://127.0.0.1:${address.port}`,
    calls,
    setIdentity(nextSubjectRef, nextBindingFingerprint) {
      subjectRef = nextSubjectRef;
      bindingFingerprint = nextBindingFingerprint;
    },
    close: async () => new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve())),
  };
}

describe("owner-controlled source connector", () => {
  const adminToken = "admin-token-0123456789abcdef0123456789";
  it("keeps the provider outside Wake Bridge, drains bounded pages, and persists only event facts plus cursor", async () => {
    const connectorToken = "fixture-connector-token";
    const connector = await fixtureConnector(connectorToken);
    const db = temporary("bridge.sqlite");
    const bridge = new WakeBridge({ instance_id: "connector-test", owner_id: "owner", db_path: db, timezone: "UTC" });
    const supervisor = new SourceSupervisor(bridge, [{
      id: "fixture-notifications",
      base_url: connector.baseUrl,
      token_env: "FIXTURE_CONNECTOR_TOKEN",
      enabled: false,
      limit: 10,
      max_pages_per_cycle: 2,
    }], { FIXTURE_CONNECTOR_TOKEN: connectorToken });
    try {
      await expect(supervisor.runOnce("fixture-notifications")).resolves.toMatchObject({
        state: "healthy",
        checkpoint_revision: 2,
        last_run: { events_seen: 1, events_inserted: 1, has_more: false },
      });
      expect(bridge.listEvents({ source: "fixture-notifications" })).toHaveLength(1);
      expect(connector.calls.map((call) => call.path)).toEqual([
        "/v1/manifest", "/v1/poll", "/v1/poll",
      ]);
      expect(connector.calls[1].body).toEqual({ cursor: null, limit: 10 });
      expect(connector.calls[2].body).toEqual({ cursor: { page: 1 }, limit: 10 });
      const durable = JSON.stringify({
        events: bridge.db.query("SELECT * FROM events"),
        checkpoints: bridge.db.query("SELECT * FROM source_checkpoints"),
      });
      expect(durable).not.toContain(connectorToken);
      expect(durable).not.toContain("provider.account");
    } finally {
      await supervisor.stop();
      bridge.close();
      await connector.close();
    }
  });

  it("continues a has_more source on the next turn after the per-cycle page budget", async () => {
    const token = "bounded-cycle-token";
    const connector = await fixtureConnector(token);
    const bridge = new WakeBridge({
      instance_id: "bounded-cycle",
      owner_id: "owner",
      db_path: temporary("bridge.sqlite"),
      timezone: "UTC",
    });
    const supervisor = new SourceSupervisor(bridge, [{
      id: "fixture-notifications",
      base_url: connector.baseUrl,
      token_env: "BOUNDED_CYCLE_TOKEN",
      enabled: true,
      limit: 10,
      max_pages_per_cycle: 1,
    }], { BOUNDED_CYCLE_TOKEN: token });
    try {
      supervisor.start();
      // This asserts eventual next-turn continuation, not a wall-clock SLA.
      // Leave headroom for parallel SQLite-heavy suites on slower CI hosts.
      const deadline = Date.now() + 10_000;
      while ((supervisor.list()[0]?.checkpoint_revision ?? 0) < 2 && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      expect(supervisor.list()[0]).toMatchObject({
        state: "healthy",
        checkpoint_revision: 2,
        last_run: { events_seen: 0, has_more: false },
      });
      expect(bridge.listEvents({ source: "fixture-notifications" })).toHaveLength(1);
    } finally {
      await supervisor.stop();
      bridge.close();
      await connector.close();
    }
  });

  it("fails local configuration closed and reports connector auth failures as observable backoff", async () => {
    expect(() => validateSourceConnectorConfig({
      id: "remote",
      base_url: "https://example.com",
      token_env: "REMOTE_TOKEN",
    })).toThrowError(/loopback/);

    const path = temporary("sources.json");
    writeFileSync(path, JSON.stringify({
      version: 1,
      sources: [
        { id: "same", base_url: "http://127.0.0.1:1", token_env: "ONE" },
        { id: "same", base_url: "http://127.0.0.1:2", token_env: "TWO" },
      ],
    }));
    expect(() => loadSourceConnectorFile(path)).toThrowError(/duplicate/);

    const bridge = new WakeBridge({ instance_id: "auth-test", owner_id: "owner", db_path: temporary("bridge.sqlite"), timezone: "UTC" });
    const supervisor = new SourceSupervisor(bridge, [{
      id: "missing-token",
      base_url: "http://127.0.0.1:1",
      token_env: "MISSING_TOKEN",
      enabled: false,
    }], {});
    try {
      await expect(supervisor.runOnce("missing-token")).resolves.toMatchObject({
        state: "needs_attention",
        last_error_class: "source_connector_auth_unavailable",
        consecutive_failures: 1,
        checkpoint_revision: 0,
      });
    } finally {
      await supervisor.stop();
      bridge.close();
    }
  });

  it("exposes source health through the authenticated daemon API", async () => {
    const bridge = new WakeBridge({
      instance_id: "daemon-source-test",
      owner_id: "owner",
      db_path: temporary("bridge.sqlite"),
      timezone: "UTC",
      admin_token: adminToken,
    });
    const daemon = await startDaemon(bridge.config, {
      bridge,
      host: "127.0.0.1",
      port: 0,
      scheduler_interval_ms: 0,
      source_connectors: [],
    });
    try {
      const address = daemon.address as AddressInfo;
      const unauthorized = await fetch(`http://127.0.0.1:${address.port}/v1/sources`);
      expect(unauthorized.status).toBe(401);
      const response = await fetch(`http://127.0.0.1:${address.port}/v1/sources`, {
        headers: { authorization: `Bearer ${adminToken}` },
      });
      expect(response.status).toBe(200);
      await expect(response.json()).resolves.toEqual({ sources: [] });
    } finally {
      await daemon.close();
      bridge.close();
    }
  });

  it("lets the owner verify, bootstrap, rebind, and durably enable an installed connector without provider credentials", async () => {
    const token = "self-service-connector-token";
    const connector = await fixtureConnector(token);
    const db = temporary("self-service.sqlite");
    const sourceConfig = [{
      id: "fixture-notifications",
      base_url: connector.baseUrl,
      token_env: "SELF_SERVICE_TOKEN",
      enabled: false,
      limit: 10,
      max_pages_per_cycle: 2,
    }];
    const environment = { SELF_SERVICE_TOKEN: token };
    const request = async (address: AddressInfo, path: string, body?: Record<string, unknown>) => {
      const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
        method: body === undefined ? "GET" : "POST",
        headers: { authorization: `Bearer ${adminToken}`, ...(body === undefined ? {} : { "content-type": "application/json" }) },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      return { response, value: await response.json() as any };
    };

    const firstBridge = new WakeBridge({ instance_id: "self-service", owner_id: "owner", db_path: db, timezone: "UTC", admin_token: adminToken });
    const first = await startDaemon(firstBridge.config, {
      bridge: firstBridge, host: "127.0.0.1", port: 0, scheduler_interval_ms: 0,
      source_connectors: sourceConfig, source_environment: environment,
    });
    try {
      const address = first.address as AddressInfo;
      const verified = await request(address, "/v1/sources/fixture-notifications/verify", {});
      expect(verified.response.status).toBe(200);
      expect(verified.value.verification).toMatchObject({
        enabled: false, binding_matches: false,
        manifest: { subject_ref: "fixture:owner", binding_fingerprint: `sha256:${"3".repeat(64)}` },
      });

      const rejected = await request(address, "/v1/sources/fixture-notifications/bootstrap", {
        mode: "from-now", expected_subject_ref: "fixture:someone-else", expected_binding_fingerprint: `sha256:${"3".repeat(64)}`,
      });
      expect(rejected.response.status).toBe(409);
      expect(rejected.value.code).toBe("source_identity_unconfirmed");

      const bootstrapped = await request(address, "/v1/sources/fixture-notifications/bootstrap", {
        mode: "from-now", expected_subject_ref: "fixture:owner", expected_binding_fingerprint: `sha256:${"3".repeat(64)}`,
      });
      expect(bootstrapped.response.status).toBe(200);
      expect(bootstrapped.value.verification).toMatchObject({ binding_matches: true, checkpoint: { revision: 1, subject_ref: "fixture:owner" } });
      expect(bootstrapped.value.verification.checkpoint).not.toHaveProperty("cursor");
      expect(bootstrapped.value.verification.checkpoint).not.toHaveProperty("instance_id");

      connector.setIdentity("fixture:new-owner", `sha256:${"4".repeat(64)}`);
      const mismatch = await request(address, "/v1/sources/fixture-notifications/enable", {});
      expect(mismatch.response.status).toBe(409);
      expect(mismatch.value.code).toBe("source_identity_mismatch");

      const rebound = await request(address, "/v1/sources/fixture-notifications/rebind", {
        expected_checkpoint_revision: 1,
        expected_subject_ref: "fixture:new-owner",
        expected_binding_fingerprint: `sha256:${"4".repeat(64)}`,
        reason: "owner_confirmed_identity_change",
      });
      expect(rebound.response.status).toBe(200);
      expect(rebound.value.verification).toMatchObject({ binding_matches: true, checkpoint: { revision: 2, subject_ref: "fixture:new-owner" } });
      expect(firstBridge.db.query("SELECT subject_ref, reason FROM source_checkpoint_history")).toEqual([
        { subject_ref: "fixture:owner", reason: "owner_confirmed_identity_change" },
      ]);

      const enabled = await request(address, "/v1/sources/fixture-notifications/enable", {});
      expect(enabled.response.status).toBe(200);
      expect(enabled.value.source.enabled).toBe(true);
    } finally {
      await first.close();
      firstBridge.close();
    }

    const secondBridge = new WakeBridge({ instance_id: "self-service", owner_id: "owner", db_path: db, timezone: "UTC", admin_token: adminToken });
    const second = await startDaemon(secondBridge.config, {
      bridge: secondBridge, host: "127.0.0.1", port: 0, scheduler_interval_ms: 0,
      source_connectors: sourceConfig, source_environment: environment,
    });
    try {
      const address = second.address as AddressInfo;
      const status = await request(address, "/v1/sources");
      expect(status.value.sources[0]).toMatchObject({ source: "fixture-notifications", enabled: true });
      const disabled = await request(address, "/v1/sources/fixture-notifications/disable", {});
      expect(disabled.value.source).toMatchObject({ enabled: false, state: "stopped" });
      expect(secondBridge.db.query("SELECT enabled FROM source_controls")).toEqual([{ enabled: 0 }]);
    } finally {
      await second.close();
      secondBridge.close();
      await connector.close();
    }
  });
});
