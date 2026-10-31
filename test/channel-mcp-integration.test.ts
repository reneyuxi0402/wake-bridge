import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { describe, expect, it } from "vitest";
import { startDaemon } from "../src/daemon.js";
import { LocalHttpHostAdapter } from "../src/out-of-process-host.js";
import { HostSessionClient, validateLocalHostDeliveryRequest, type HostAdapterManifest, type LocalHostDeliveryRequest } from "../src/transport-sdk.js";

const manifest: HostAdapterManifest = {
  contract_version: 1, adapter_kind: "channel_mcp_fixture", adapter_version: "1.0.0",
  host_kinds: ["fixture_runner"], support_tier: "mock", tested_host_versions: ["fixture"],
  capabilities: { warm_resume: true, exact_live_route: true, requires_live_binding: true, foreground_presence_observable: true },
  receipt_upper_bound: "host_accepted",
};

/** Actual built CLI, not an in-process MCP server or a direct Core action. */
function mcpProcess(configPath: string, origin: string) {
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("WAKEBRIDGE_")));
  const child = spawn(process.execPath, [join(process.cwd(), "dist/src/cli.js"), "mcp", "--config", configPath], {
    stdio: ["pipe", "pipe", "pipe"], env: { ...env, WAKEBRIDGE_DAEMON_URL: origin },
  });
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr = (stderr + String(chunk)).slice(-4096); });
  let nextId = 0;
  const pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void; timer: ReturnType<typeof setTimeout> }>();
  const fail = (error: Error) => {
    for (const entry of pending.values()) { clearTimeout(entry.timer); entry.reject(error); }
    pending.clear();
  };
  const lines = createInterface({ input: child.stdout });
  lines.on("line", (line) => {
    try {
      const response = JSON.parse(line);
      const entry = pending.get(response.id);
      if (!entry) return;
      clearTimeout(entry.timer);
      pending.delete(response.id);
      if (response.error) entry.reject(new Error(JSON.stringify(response.error)));
      else entry.resolve(response.result);
    } catch (error) { fail(error instanceof Error ? error : new Error(String(error))); }
  });
  child.on("error", fail);
  child.stdin.on("error", fail);
  const exited = new Promise<void>((resolve) => child.once("close", (code, signal) => {
    fail(new Error(`MCP CLI exited (${code ?? signal}): ${stderr}`));
    resolve();
  }));
  const request = (method: string, params: Record<string, unknown> = {}): Promise<any> => {
    if (child.exitCode !== null || child.signalCode !== null) return Promise.reject(new Error(`MCP CLI already exited: ${stderr}`));
    const id = ++nextId;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { pending.delete(id); reject(new Error(`MCP ${method} timed out: ${stderr}`)); }, 10_000);
      pending.set(id, { resolve, reject, timer });
      child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
    });
  };
  return {
    child,
    async initialize() {
      const result = await request("initialize", { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "channel-integration", version: "1" } });
      expect(result.serverInfo.name).toBe("wake-bridge");
      child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" })}\n`);
      const listed = await request("tools/list");
      expect(listed.tools.map((tool: { name: string }) => tool.name)).toEqual(expect.arrayContaining([
        "attention_channel_create", "attention_channel_list", "attention_channel_rename", "attention_channel_delete", "attention_ack", "attention_consume",
      ]));
    },
    async call(name: string, args: Record<string, unknown> = {}) {
      const result = await request("tools/call", { name, arguments: args });
      return JSON.parse(result.content[0].text);
    },
    async close() {
      if (child.exitCode !== null || child.signalCode !== null) return;
      child.stdin.end();
      const timer = setTimeout(() => child.kill("SIGKILL"), 5_000);
      try { await exited; } finally { clearTimeout(timer); lines.close(); }
    },
  };
}

describe("channel lifecycle through built CLI MCP and public host routes", () => {
  it("keeps two agents' identically named channels isolated through dispatch, ack, consume, and deletion", async () => {
    const root = mkdtempSync(join(tmpdir(), "wake-bridge-channel-mcp-"));
    const cleanups: (() => Promise<void>)[] = [];
    try {
      const agents = [];
      for (const name of ["a", "b"]) {
        const config = {
          version: 1 as const, instance_id: `channel-mcp-${name}`, owner_id: `owner-${name}`,
          db_path: join(root, `${name}.sqlite`), timezone: "UTC", max_channels: 2,
          admin_token: `channel-mcp-${name}-owner-token-0123456789abcdef`,
        };
        const configPath = join(root, `${name}.json`);
        writeFileSync(configPath, JSON.stringify(config), { mode: 0o600 });
        const routeToken = `channel-mcp-${name}-route-token-0123456789abcdef`;
        const deliveries: LocalHostDeliveryRequest[] = [];
        const receiver = createServer(async (request, response) => {
          if (request.method !== "POST" || request.url !== "/v1/wakes") return void response.writeHead(404).end();
          if (request.headers.authorization !== `Bearer ${routeToken}`) return void response.writeHead(401).end();
          try {
            let raw = "";
            for await (const chunk of request) raw += String(chunk);
            const delivery = validateLocalHostDeliveryRequest(JSON.parse(raw));
            if (request.headers["x-wakebridge-delivery-nonce"] !== delivery.delivery_nonce) return void response.writeHead(400).end();
            deliveries.push(delivery);
            response.writeHead(202).end();
          } catch { response.writeHead(400).end(); }
        });
        await new Promise<void>((resolve, reject) => { receiver.once("error", reject); receiver.listen(0, "127.0.0.1", resolve); });
        cleanups.push(async () => { receiver.closeAllConnections(); await new Promise<void>((resolve, reject) => receiver.close((error) => error ? reject(error) : resolve())); });
        const routeOrigin = `http://127.0.0.1:${(receiver.address() as AddressInfo).port}`;
        const hostToken = `channel-mcp-${name}-host-token-0123456789abcdef`;
        // Core is created only by the daemon fixture. All agent mutations below
        // go through the real MCP child or the public HTTP/HostSessionClient API.
        const daemon = await startDaemon(config, {
          host: "127.0.0.1", port: 0, scheduler_interval_ms: 0,
          host_adapters: [new LocalHttpHostAdapter(manifest, { timeout_ms: 2_000 })],
          host_credentials: [{ id: "dynamic", token: hostToken, adapter_kind: manifest.adapter_kind, host_kind: "fixture_runner", attention_channels: "*" }],
        });
        cleanups.push(async () => { await daemon.close(); daemon.bridge.close(); });
        const origin = `http://127.0.0.1:${(daemon.address as AddressInfo).port}`;
        const mcp = mcpProcess(configPath, origin);
        cleanups.push(() => mcp.close());
        await mcp.initialize();
        const http = async (path: string, body?: unknown) => {
          const response = await fetch(`${origin}${path}`, {
            method: body === undefined ? "GET" : "POST", headers: { authorization: `Bearer ${config.admin_token}`, "content-type": "application/json" },
            ...(body === undefined ? {} : { body: JSON.stringify(body) }), signal: AbortSignal.timeout(5_000),
          });
          return { status: response.status, body: await response.json() as any };
        };
        const client = new HostSessionClient({ base_url: origin, host_token: hostToken, adapter_kind: manifest.adapter_kind });
        const open = (channels: string[]) => client.openChannels({ session_ref: "same-session", attention_channels: channels, route_origin: routeOrigin, route_token: routeToken });
        await expect(open(["unknown"])).rejects.toMatchObject({ status: 404 });
        expect((await http("/v1/bindings")).body.bindings).toEqual([]);
        const life = await mcp.call("attention_channel_create", { attention_channel: "life", name: "Life" });
        const work = await mcp.call("attention_channel_create", { attention_channel: "work", name: "Work" });
        expect(life.channel).toMatchObject({ instance_id: config.instance_id, attention_channel: "life", name: "Life" });
        expect(work.channel).toMatchObject({ instance_id: config.instance_id, attention_channel: "work", name: "Work" });
        expect(await mcp.call("attention_channel_list")).toMatchObject({ max_channels: 2, channels: [expect.any(Object), expect.any(Object)] });
        const registration = await open(["life", "work"]);
        const leases = client.leases(registration);
        expect(leases[0].endpoint_id).toBe(leases[1].endpoint_id);
        agents.push({ config, mcp, http, client, open, registration, leases, deliveries });
      }
      const [a, b] = agents;
      expect(a.mcp.child.pid).not.toBe(b.mcp.child.pid);
      expect(a.registration.endpoint.id).not.toBe(b.registration.endpoint.id);
      const beforeRename = (await a.http("/v1/bindings")).body.bindings;
      expect(await a.mcp.call("attention_channel_rename", { attention_channel: "life", name: "Personal" })).toMatchObject({ channel: { attention_channel: "life", name: "Personal" } });
      expect((await a.http("/v1/bindings")).body.bindings).toEqual(beforeRename);
      expect((await b.mcp.call("attention_channel_list")).channels.find((channel: any) => channel.attention_channel === "life").name).toBe("Life");

      const emit = async (agent: typeof a, channel: string, key: string) => {
        const result = await agent.http("/v1/events", { source: "fixture", event: {
          type: "fixture.channel", dedupe_key: key, attention_channel_hint: channel, resource: { uri: `fixture://${channel}/${key}` },
        } });
        expect(result.status).toBe(201);
        return result.body;
      };
      const dispatch = async (agent: typeof a) => {
        expect((await agent.http("/v1/tick", {})).status).toBe(200);
        const result = await agent.http("/v1/dispatch", {});
        expect(result.status).toBe(200);
        return result.body.results;
      };
      const acknowledge = async (agent: typeof a, delivery: LocalHostDeliveryRequest) => {
        expect(delivery.wake.instance_id).toBe(agent.config.instance_id);
        const batchId = delivery.wake.wake_batch_id;
        expect(await agent.mcp.call("attention_ack", { wake_batch_id: batchId })).toMatchObject({ batch: { id: batchId, state: "seen" } });
        for (const claim of delivery.wake.claim_refs) {
          expect(await agent.mcp.call("attention_consume", { claim_id: claim.claim_id, result: { handled: true } })).toMatchObject({ claim: { id: claim.claim_id, state: "consumed" } });
        }
        const receipts = (await agent.http(`/v1/receipts/${batchId}`)).body.receipts;
        expect(receipts.map((receipt: any) => receipt.stage)).toEqual(["transport_accepted", "agent_seen", "agent_consumed"]);
      };
      for (const agent of agents) {
        for (const channel of ["life", "work"]) {
          expect(await emit(agent, channel, `same-${channel}`)).toMatchObject({ duplicate: false });
        }
        expect((await dispatch(agent)).map((result: any) => result.status)).toEqual(["accepted", "accepted"]);
        expect(agent.deliveries.map((delivery) => delivery.wake.attention_channel).sort()).toEqual(["life", "work"]);
      }
      expect(await b.mcp.call("attention_ack", { wake_batch_id: a.deliveries[0].wake.wake_batch_id })).toMatchObject({ error: "batch_not_found" });
      expect(await b.mcp.call("attention_consume", { claim_id: a.deliveries[0].wake.claim_refs[0].claim_id })).toMatchObject({ error: "claim_not_found" });
      for (const agent of agents) for (const delivery of agent.deliveries) await acknowledge(agent, delivery);

      const pending = await a.mcp.call("attention_schedule", {
        attention_channel: "life", resource: "fixture://pending-delete", eligible_after: new Date(Date.now() - 1_000).toISOString(), idempotency_key: "pending-delete",
      });
      const bBindings = (await b.http("/v1/bindings")).body.bindings;
      expect(await a.mcp.call("attention_channel_delete", { attention_channel: "life" })).toMatchObject({ error: "channel_in_use", usage: { binding: { endpoint_id: a.registration.endpoint.id }, activity_watch: false, policies: [] } });
      expect(await a.mcp.call("attention_channel_delete", { attention_channel: "life", force: "true" })).toMatchObject({ error: "invalid_arguments" });
      const deleted = await a.mcp.call("attention_channel_delete", { attention_channel: "life", force: true });
      expect(deleted).toMatchObject({ channel: { attention_channel: "life", deleted_at: expect.any(String) } });
      expect(await a.mcp.call("attention_channel_delete", { attention_channel: "life" })).toEqual(deleted);
      expect((await a.mcp.call("attention_channel_list")).channels.map((channel: any) => channel.attention_channel)).toEqual(["work"]);
      expect((await b.mcp.call("attention_channel_list")).channels).toHaveLength(2);
      expect((await b.http("/v1/bindings")).body.bindings).toEqual(bBindings);
      expect((await a.http("/v1/claims")).body.claims.find((claim: any) => claim.id === pending.claim.id).state).toBe("dismissed");
      await expect(a.open(["life", "work"])).rejects.toMatchObject({ status: 409 });
      await expect(a.client.renewPresence(a.leases.find((lease) => lease.attention_channel === "life")!, { observed_by: "old-deleted-channel" })).rejects.toMatchObject({ status: 409 });
      expect(await a.mcp.call("attention_schedule", { attention_channel: "life", resource: "fixture://deleted", eligible_after: new Date().toISOString() })).toMatchObject({ error: "channel_deleted" });
      expect(await a.mcp.call("attention_channel_create", { attention_channel: "life", name: "Must not resurrect" })).toMatchObject({ error: "channel_exists" });
      expect((await emit(a, "life", "after-delete")).event.state).toBe("suppressed");
      const aWorkLease = a.leases.find((lease) => lease.attention_channel === "work")!;
      await expect(a.client.renew(aWorkLease)).resolves.toMatchObject({ id: a.registration.endpoint.id });
      expect((await a.http("/v1/bindings")).body.bindings).toEqual(beforeRename.filter((binding: any) => binding.attention_channel === "work"));
      await emit(a, "work", "survives-delete");
      for (const channel of ["life", "work"]) await emit(b, channel, "survives-delete-" + channel);
      expect((await dispatch(a)).map((result: any) => result.status)).toEqual(["accepted"]);
      expect((await dispatch(b)).map((result: any) => result.status)).toEqual(["accepted", "accepted"]);
      expect(a.deliveries).toHaveLength(3);
      expect(b.deliveries).toHaveLength(4);
      expect(a.deliveries[2].wake.attention_channel).toBe("work");
      await acknowledge(a, a.deliveries[2]);
      for (const delivery of b.deliveries.slice(2)) await acknowledge(b, delivery);
      const replacement = await a.mcp.call("attention_channel_create", { name: "Personal" });
      expect(replacement.channel).toMatchObject({ name: "Personal", deleted_at: null });
      expect(replacement.channel.attention_channel).toEqual(expect.any(String));
      expect(replacement.channel.attention_channel).not.toBe("life");
      expect((await a.mcp.call("attention_channel_list")).channels).toHaveLength(2);
    } finally {
      let cleanupError: unknown;
      try {
        for (const cleanup of cleanups.reverse()) {
          try { await cleanup(); } catch (error) { cleanupError ??= error; }
        }
      } finally { rmSync(root, { recursive: true, force: true }); }
      if (cleanupError) throw cleanupError;
    }
  }, 45_000);
});
