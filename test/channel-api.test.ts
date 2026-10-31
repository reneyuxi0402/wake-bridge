import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeError, WakeBridge } from "../src/core.js";
import { startDaemon } from "../src/daemon.js";
import { WakeBridgeMcpServer } from "../src/mcp.js";
import { bridgeConfigFromFile, initializeInstance, loadInstanceConfig } from "../src/instance-config.js";
import { LocalHttpHostAdapter, loadOutOfProcessHostFile } from "../src/out-of-process-host.js";
import { HostSessionClient, type HostAdapterManifest } from "../src/transport-sdk.js";
import { WakeBridgeSdkError } from "../src/sdk-error.js";

const OWNER = "channel-owner-token-0123456789abcdef";
const HOST = "channel-wildcard-token-0123456789abcdef";
const ARRAY_HOST = "channel-array-token-0123456789abcdef";
const SOURCE = "channel-source-token-0123456789abcdef";
const ROUTE = "channel-route-token-0123456789abcdef";
const manifest: HostAdapterManifest = {
  contract_version: 1, adapter_kind: "channel_fixture", adapter_version: "1.0.0",
  host_kinds: ["fixture"], support_tier: "mock", tested_host_versions: ["fixture"],
  capabilities: { warm_resume: true, exact_live_route: true, requires_live_binding: true },
  receipt_upper_bound: "host_accepted",
};
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0)) await cleanup(); });

async function fixture(maxChannels = 32) {
  const config = { instance_id: "channel-api", owner_id: "owner", db_path: ":memory:", timezone: "UTC", admin_token: OWNER, max_channels: maxChannels };
  const bridge = new WakeBridge(config, { autoMockTransport: false });
  const daemon = await startDaemon(config, {
    bridge, host: "127.0.0.1", port: 0, scheduler_interval_ms: 0,
    host_adapters: [new LocalHttpHostAdapter(manifest)],
    host_credentials: [
      { id: "dynamic", token: HOST, adapter_kind: manifest.adapter_kind, host_kind: "fixture", attention_channels: "*" },
      { id: "fixed", token: ARRAY_HOST, adapter_kind: manifest.adapter_kind, host_kind: "fixture", attention_channels: ["legacy"] },
    ],
    source_credentials: [{ id: "source", source: "fixture", token: SOURCE }],
  });
  cleanups.push(async () => { await daemon.close(); bridge.close(); });
  const origin = `http://127.0.0.1:${(daemon.address as AddressInfo).port}`;
  const request = (path: string, method = "GET", body?: unknown, token = OWNER) => fetch(`${origin}${path}`, {
    method, headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  const openBody = (channels: string[]) => ({ adapter_kind: manifest.adapter_kind, session_ref: "shared", attention_channels: channels, route_address: { base_url: "http://127.0.0.1:18799/", token: ROUTE } });
  return { bridge, request, openBody, mcp: new WakeBridgeMcpServer(bridge) };
}

describe("public attention channel APIs", () => {
  it("publishes strict MCP schemas and performs CRUD with stable IDs", async () => {
    const { mcp } = await fixture();
    const response = await mcp.handleRequest({ jsonrpc: "2.0", id: 1, method: "tools/list" });
    const tools = (response!.result!.tools as any[]).filter((tool) => tool.name.startsWith("attention_channel_"));
    expect(tools.map((tool) => tool.name).sort()).toEqual(["attention_channel_create", "attention_channel_delete", "attention_channel_list", "attention_channel_rename"]);
    expect(tools.every((tool) => tool.inputSchema.additionalProperties === false)).toBe(true);
    expect(tools.find((tool) => tool.name === "attention_channel_delete").inputSchema.properties.force).toEqual({ type: "boolean", default: false });
    let rpcId = 1;
    const call = async (name: string, args: Record<string, unknown>) => {
      const result = await mcp.handleRequest({ jsonrpc: "2.0", id: ++rpcId, method: "tools/call", params: { name, arguments: args } });
      expect(result!.result!.isError).not.toBe(true);
      return result!.result!.structuredContent as any;
    };
    const created = await call("attention_channel_create", { name: "Work" });
    const id = created.channel.attention_channel;
    expect(created.channel).toMatchObject({ name: "Work", instance_id: "channel-api", deleted_at: null });
    const renamed = await call("attention_channel_rename", { attention_channel: id, name: "Focus" });
    expect(renamed).toMatchObject({ channel: { attention_channel: id, name: "Focus" } });
    expect(await call("attention_channel_list", {})).toMatchObject({ max_channels: 32, channels: [{ attention_channel: id }] });
    expect(await call("attention_channel_delete", { attention_channel: id })).toMatchObject({ channel: { attention_channel: id, deleted_at: expect.any(String) } });
    expect(await call("attention_channel_list", {})).toEqual({ max_channels: 32, channels: [] });
    await expect(mcp.callTool("attention_channel_create", { name: "Again", attention_channel: id })).rejects.toMatchObject({ status: 409 });
  });

  it("rejects unsupported or invalid MCP arguments", async () => {
    const { mcp } = await fixture();
    for (const [name, args] of [
      ["attention_channel_create", { name: "X", extra: true }],
      ["attention_channel_create", { name: 4 }],
      ["attention_channel_create", { name: "" }],
      ["attention_channel_create", { name: "X", attention_channel: null }],
      ["attention_channel_list", { limit: 1 }],
      ["attention_channel_rename", { attention_channel: "missing", name: "X", extra: 1 }],
      ["attention_channel_delete", {}],
      ...[null, "true", "false", 0, 1, {}, []].map((force) => ["attention_channel_delete", { attention_channel: "missing", force }] as const),
    ] as const) await expect(mcp.callTool(name, args)).rejects.toMatchObject({ status: 400 });
  });

  it("rejects whitespace-padded explicit IDs through HTTP and MCP without registering channels", async () => {
    const { bridge, request, mcp } = await fixture();
    const response = await request("/v1/channels", "POST", { name: "  Focus  ", attention_channel: " padded " });
    expect.soft(response.status).toBe(400);
    expect.soft(await response.json()).toMatchObject({ code: "invalid_channel" });
    const rpc = await mcp.handleRequest({ jsonrpc: "2.0", id: 1, method: "tools/call", params: {
      name: "attention_channel_create", arguments: { name: "  Notes  ", attention_channel: " mcp-padded " },
    } });
    expect.soft(rpc!.result).toMatchObject({ isError: true, structuredContent: { error: "invalid_channel" } });
    expect.soft(await (await request("/v1/channels")).json()).toMatchObject({ channels: [] });
    expect.soft(bridge.listChannels(true)).toEqual([]);
    const valid = await request("/v1/channels", "POST", { name: "  Focus  ", attention_channel: "clean" });
    expect(valid.status).toBe(201);
    expect(await valid.json()).toMatchObject({ channel: { attention_channel: "clean", name: "Focus" } });
  });

  it("makes CRUD owner-only and applies the configured active-channel limit", async () => {
    const { request } = await fixture(1);
    for (const token of [HOST, ARRAY_HOST, SOURCE, "wrong-token"]) {
      for (const [path, method, body] of [["/v1/channels", "GET", undefined], ["/v1/channels", "POST", { name: "X" }], ["/v1/channels/fixed", "PATCH", { name: "Y" }], ["/v1/channels/fixed", "DELETE", {}], ["/v1/channels/fixed", "DELETE", { force: true }]] as const) {
        expect((await request(path, method, body, token)).status).toBe(401);
      }
    }
    expect((await request("/v1/channels", "POST", { name: "X", extra: 1 })).status).toBe(400);
    expect((await request("/v1/channels", "POST", { name: "X", attention_channel: "fixed" })).status).toBe(201);
    expect(await (await request("/v1/channels")).json()).toMatchObject({ max_channels: 1, channels: [{ attention_channel: "fixed", name: "X" }] });
    const overflow = await request("/v1/channels", "POST", { name: "Overflow" });
    expect(overflow.status).toBe(429);
    expect(await overflow.json()).toMatchObject({ code: "channel_limit_reached" });
    expect(await (await request("/v1/channels/fixed", "PATCH", { name: "Renamed" })).json()).toMatchObject({ channel: { attention_channel: "fixed", name: "Renamed" } });
    expect((await request("/v1/channels/fixed", "DELETE", { extra: 1 })).status).toBe(400);
    expect((await request("/v1/channels/fixed", "DELETE", {})).status).toBe(200);
    expect((await request("/v1/channels", "POST", { name: "Replacement" })).status).toBe(201);
  });

  it("allows dynamic registered channels but never creates unknown or deleted IDs through wildcard hosts", async () => {
    const { bridge, request, openBody } = await fixture();
    expect((await request("/v1/host-sessions/open", "POST", openBody(["future"]), HOST)).status).toBe(404);
    expect(bridge.getChannel("future")).toBeNull();
    await request("/v1/channels", "POST", { name: "Future", attention_channel: "future" });
    expect((await request("/v1/host-sessions/open", "POST", openBody(["future"]), ARRAY_HOST)).status).toBe(400);
    expect((await request("/v1/host-sessions/open", "POST", openBody(["future"]), HOST)).status).toBe(201);
    expect((await request("/v1/channels/future", "DELETE", { force: true })).status).toBe(200);
    expect((await request("/v1/host-sessions/open", "POST", openBody(["future"]), HOST)).status).toBe(409);
    expect((await request("/v1/host-sessions/open", "POST", openBody(["legacy"]), ARRAY_HOST)).status).toBe(201);
    expect(bridge.getChannel("legacy")).toMatchObject({ attention_channel: "legacy", deleted_at: null });
  });

  it("round-trips printable stable IDs through encoded HTTP path segments", async () => {
    const { request } = await fixture();
    const id = "focus/notes ?#";
    expect((await request("/v1/channels", "POST", { name: "Notes", attention_channel: id })).status).toBe(201);
    const path = `/v1/channels/${encodeURIComponent(id)}`;
    expect(await (await request(path, "PATCH", { name: "Research" })).json()).toMatchObject({ channel: { attention_channel: id, name: "Research" } });
    expect((await request(path, "DELETE", {})).status).toBe(200);
  });

  it("protects used channels through HTTP and MCP, reports safe usage, and requires an explicit boolean force", async () => {
    const { bridge, request, openBody, mcp } = await fixture();
    bridge.createChannel({ attention_channel: "used", name: "Used" });
    const opened = await request("/v1/host-sessions/open", "POST", openBody(["used"]), HOST);
    expect(opened.status).toBe(201);
    const registration = await opened.json() as any;
    bridge.configureActivityWatch({ attention_channel: "used", enabled: true, idle_after_ms: 60_000 });
    bridge.updatePolicy({ id: "used-route", version: 1, match: { type: "used.event" }, delivery: { mode: "immediate" }, target: { attention_channel: "used" } });
    const usage = {
      binding: { endpoint_id: registration.endpoint.id, generation: registration.bindings[0].generation },
      activity_watch: true,
      policies: [{ id: "used-route", version: 1 }],
    };
    const rpc = (args: Record<string, unknown>) => mcp.handleRequest({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "attention_channel_delete", arguments: args } });
    const before = { channel: bridge.getChannel("used"), binding: bridge.getBinding("used"), watch: bridge.getActivityWatch("used") };
    for (const force of [undefined, false]) {
      const body = force === undefined ? {} : { force };
      const response = await request("/v1/channels/used", "DELETE", body);
      expect(response.status).toBe(409);
      expect(await response.json()).toEqual({ error: expect.any(String), code: "channel_in_use", usage });
      const result = await rpc({ attention_channel: "used", ...body });
      expect(result!.result).toMatchObject({ isError: true, structuredContent: { error: "channel_in_use", usage } });
    }
    for (const force of [null, "true", "false", 0, 1, {}, []]) {
      const response = await request("/v1/channels/used", "DELETE", { force });
      expect(response.status).toBe(400);
      expect(await response.json()).toMatchObject({ code: "invalid_arguments" });
      expect((await rpc({ attention_channel: "used", force }))!.result).toMatchObject({ isError: true, structuredContent: { error: "invalid_arguments" } });
    }
    expect({ channel: bridge.getChannel("used"), binding: bridge.getBinding("used"), watch: bridge.getActivityWatch("used") }).toEqual(before);
    const encodedUsage = JSON.stringify((await rpc({ attention_channel: "used" }))!.result);
    for (const secret of [OWNER, HOST, ROUTE, "shared", "base_url", "routes", "lease_token"]) expect(encodedUsage).not.toContain(secret);
    const forced = await rpc({ attention_channel: "used", force: true });
    expect(forced!.result).not.toHaveProperty("isError");
    const deleted = (forced!.result!.structuredContent as any).channel;
    expect(deleted.deleted_at).toEqual(expect.any(String));
    expect((await rpc({ attention_channel: "used" }))!.result!.structuredContent).toEqual({ channel: deleted });
    const replay = await request("/v1/channels/used", "DELETE", { force: true });
    expect(replay.status).toBe(200);
    expect(await replay.json()).toEqual({ channel: deleted });
    expect(bridge.getBinding("used")).toBeNull();
    expect(bridge.getActivityWatch("used")?.enabled).toBe(false);

    bridge.createChannel({ attention_channel: "http-used", name: "HTTP used" });
    bridge.configureActivityWatch({ attention_channel: "http-used", enabled: true, idle_after_ms: 60_000 });
    const httpForced = await request("/v1/channels/http-used", "DELETE", { force: true });
    expect(httpForced.status).toBe(200);
    expect(await httpForced.json()).toMatchObject({ channel: { attention_channel: "http-used", deleted_at: expect.any(String) } });
  });

  it("does not serialize arbitrary error usage, details, or causes", async () => {
    const { bridge, request, mcp } = await fixture();
    const error = Object.assign(new BridgeError("bounded public message", "channel_in_use", 409), {
      usage: { token: "usage-private-token" }, details: { token: "details-private-token" }, cause: new Error("cause-private-token"),
    });
    const mocked = vi.spyOn(bridge, "deleteChannel").mockImplementation(() => { throw error; });
    try {
      const response = await request("/v1/channels/anything", "DELETE", {});
      expect(await response.json()).toEqual({ error: "bounded public message", code: "channel_in_use" });
      const result = await mcp.handleRequest({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "attention_channel_delete", arguments: { attention_channel: "anything" } } });
      expect(result!.result!.structuredContent).toEqual({ error: "channel_in_use", message: "bounded public message" });
    } finally { mocked.mockRestore(); }
  });

  it("persists quota-limited source ingress and exposes historical diagnostics only to the owner", async () => {
    const { bridge, request, mcp } = await fixture(1);
    bridge.createChannel({ attention_channel: "full", name: "Full" });
    const event = {
      type: "quota.event", dedupe_key: "quota-ingress", attention_channel_hint: "overflow",
      resource: { uri: "secret://resource-value" }, metadata: { token: "metadata-private-token" }, payload_preview: "payload-private-token",
    };
    const first = await request("/v1/ingress/fixture/events", "POST", { event }, SOURCE);
    expect(first.status).toBe(201);
    const emitted = await first.json() as any;
    expect(emitted).toMatchObject({ suppressed: true, duplicate: false, event: { state: "suppressed" } });
    expect(emitted.claim).toBeUndefined();
    const replay = await request("/v1/ingress/fixture/events", "POST", { event }, SOURCE);
    expect(replay.status).toBe(201);
    expect(await replay.json()).toMatchObject({ suppressed: true, duplicate: true, event: { id: emitted.event.id } });
    expect(bridge.getChannel("overflow")).toBeNull();
    expect((await request("/v1/channels", "POST", { name: "Explicit overflow" })).status).toBe(429);
    await expect(mcp.callTool("attention_schedule", { resource: "fixture://explicit-overflow", eligible_after: bridge.nowIso(), attention_channel: "overflow" })).rejects.toMatchObject({ code: "channel_limit_reached", status: 429 });
    for (const token of [SOURCE, HOST, ARRAY_HOST]) expect((await request("/v1/status", "GET", undefined, token)).status).toBe(401);
    const response = await request("/v1/status");
    expect(response.status).toBe(200);
    const status = await response.json();
    expect(status).toMatchObject({ health: "healthy", attention_required: false, ingest: { channel_suppressions: {
      counts: { channel_deleted: 0, channel_limit_reached: 1 }, latest_at: expect.any(String),
      recent: [{ event_id: emitted.event.id, source: "fixture", reason: "channel_limit_reached" }],
    } } });
    const encoded = JSON.stringify(status);
    for (const secret of ["resource-value", "metadata-private-token", "payload-private-token", OWNER, SOURCE, HOST]) expect(encoded).not.toContain(secret);
  });

  it("expands wildcard bootstrap scope for endpoint-wide renewal and close", async () => {
    const { bridge, request, openBody } = await fixture();
    bridge.createChannel({ attention_channel: "one", name: "One" });
    bridge.createChannel({ attention_channel: "two", name: "Two" });
    const opened = await request("/v1/host-sessions/open", "POST", openBody(["one", "two"]), HOST);
    expect(opened.status).toBe(201);
    const registration = await opened.json() as any;
    const body = { adapter_kind: manifest.adapter_kind, session_ref: "shared", attention_channel: "one", expected_generation: registration.bindings[0].generation };
    expect((await request("/v1/host-sessions/renew", "POST", body, HOST)).status).toBe(200);
    expect((await request("/v1/host-sessions/close", "POST", body, HOST)).status).toBe(200);
  });

  it("loads explicit wildcard manifests without widening array declarations", () => {
    const root = mkdtempSync(join(tmpdir(), "wake-channel-host-"));
    const path = join(root, "hosts.json");
    const declaration = { id: "fixture", adapter_kind: manifest.adapter_kind, adapter_version: "1.0.0", host_kind: "fixture", tested_host_versions: ["fixture"], token_env: "HOST_TOKEN", attention_channels: "*", capabilities: manifest.capabilities, receipt_upper_bound: "host_accepted" };
    writeFileSync(path, JSON.stringify({ version: 1, adapters: [declaration] }));
    expect(loadOutOfProcessHostFile(path, { HOST_TOKEN: HOST }).credentials[0].attention_channels).toBe("*");
    writeFileSync(path, JSON.stringify({ version: 1, adapters: [{ ...declaration, attention_channels: ["legacy"] }] }));
    expect(loadOutOfProcessHostFile(path, { HOST_TOKEN: HOST }).credentials[0].attention_channels).toEqual(["legacy"]);
  });

  it("validates and forwards max_channels from private instance config", () => {
    const root = mkdtempSync(join(tmpdir(), "wake-channel-config-"));
    const { config, config_path } = initializeInstance({ data_dir: root, instance_id: "channel-config", owner_id: "owner" });
    for (const value of [1, 1024]) {
      writeFileSync(config_path, JSON.stringify({ ...config, max_channels: value }), { mode: 0o600 });
      expect(bridgeConfigFromFile(config_path).max_channels).toBe(value);
    }
    for (const value of [0, 1025, 1.5, "32", null]) {
      writeFileSync(config_path, JSON.stringify({ ...config, max_channels: value }), { mode: 0o600 });
      expect(() => loadInstanceConfig(config_path)).toThrow(/max_channels/u);
    }
  });

  it("preserves SDK error compatibility and exposes only bounded safe remote codes", async () => {
    for (const code of ["endpoint_fenced", "stale_generation", "lease_expired", "secret text / token", "endpoint_fenced\n", "x".repeat(65), { secret: "token" }, undefined]) {
      const client = new HostSessionClient({ base_url: "http://127.0.0.1:18799", host_token: HOST, adapter_kind: manifest.adapter_kind, fetch: async () => new Response(JSON.stringify({ code, error: "private raw diagnostic" }), { status: 409 }) });
      let caught: unknown;
      try { await client.open({ session_ref: "s", attention_channel: "one", route_origin: "http://127.0.0.1:18800", route_token: ROUTE }); } catch (error) { caught = error; }
      expect(caught).toBeInstanceOf(WakeBridgeSdkError);
      const error = caught as WakeBridgeSdkError;
      const remoteCode: string | undefined = error.remote_code;
      expect(error.code).toBe("host_session_request_failed");
      expect(error.status).toBe(409);
      expect(remoteCode).toBe(typeof code === "string" && /^[a-z][a-z0-9_]{0,63}$/u.test(code) ? code : undefined);
      expect(error.message).not.toContain("private raw diagnostic");
      expect(error.message).not.toContain("secret");
    }
  });
});
