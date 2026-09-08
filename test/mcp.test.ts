import { PassThrough } from "node:stream";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { MCP_PROTOCOL_VERSION, WakeBridgeMcpServer, runMcpStdio } from "../src/mcp.js";

function bridge(): WakeBridge {
  return new WakeBridge({
    instance_id: "mcp-instance",
    owner_id: "mcp-owner",
    db_path: join(mkdtempSync(join(tmpdir(), "wake-bridge-mcp-")), "bridge.sqlite"),
    timezone: "UTC",
    endpoint_lease_ms: 24 * 60 * 60_000,
  });
}

function parsed(response: Awaited<ReturnType<WakeBridgeMcpServer["handleRequest"]>>): any {
  expect(response).not.toBeNull();
  expect(response?.result).toBeTruthy();
  const text = (response?.result as any).content?.[0]?.text;
  return JSON.parse(text);
}

async function call(server: WakeBridgeMcpServer, id: number, name: string, args: Record<string, unknown> = {}): Promise<any> {
  return parsed(await server.handleRequest({ jsonrpc: "2.0", id, method: "tools/call", params: { name, arguments: args } }));
}

describe("Wake Bridge stdio MCP", () => {
  it("speaks initialize/tools/list and has no warm waiter tool", async () => {
    const b = bridge();
    const server = new WakeBridgeMcpServer(b);
    const initialized = await server.handleRequest({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: { protocolVersion: MCP_PROTOCOL_VERSION, clientInfo: { name: "test", version: "1" } },
    });
    expect(initialized?.result).toMatchObject({ protocolVersion: MCP_PROTOCOL_VERSION, serverInfo: { name: "wake-bridge" } });
    const listed = await server.handleRequest({ jsonrpc: "2.0", id: 2, method: "tools/list", params: {} });
    const names = (listed?.result as any).tools.map((tool: any) => tool.name);
    expect(names).toEqual([
      "attention_schedule",
      "attention_ack",
      "attention_consume",
      "attention_snooze",
      "attention_dismiss",
      "attention_status",
      "attention_list",
      "attention_event_list",
      "attention_binding_status",
      "attention_wake_health",
      "attention_watch_status",
      "attention_watch_configure",
      "attention_source_status",
      "attention_source_verify",
      "attention_source_bootstrap",
      "attention_source_rebind",
      "attention_source_enable",
      "attention_source_disable",
    ]);
    expect(names).not.toContain("attention_wait");
    expect(await server.handleRequest({ jsonrpc: "2.0", method: "notifications/initialized" })).toBeNull();
    b.close();
  });

  it("uses the same Core SQLite state for schedule, list, snooze, dismiss, consume, and status", async () => {
    const b = bridge();
    const server = new WakeBridgeMcpServer(b);
    const eligible = new Date(Date.now() + 60_000).toISOString();
    const scheduled = await call(server, 1, "attention_schedule", {
      resource: "memory://thought/mcp",
      eligible_after: eligible,
      attention_channel: "life",
      reason_code: "reconsider",
      idempotency_key: "mcp-schedule-1",
    });
    expect(scheduled.claim).toMatchObject({ id: expect.any(String), state: "pending", attention_channel: "life", resource: { uri: "memory://thought/mcp" } });
    expect(b.getClaim(scheduled.claim.id)?.state).toBe("pending");
    const duplicate = await call(server, 2, "attention_schedule", {
      resource: "memory://other",
      eligible_after: eligible,
      attention_channel: "life",
      idempotency_key: "mcp-schedule-1",
    });
    expect(duplicate.duplicate).toBe(true);
    expect(duplicate.claim.id).toBe(scheduled.claim.id);
    const listed = await call(server, 3, "attention_list", { state: "pending", channel: "life" });
    expect(listed.claims).toHaveLength(1);
    const snoozed = await call(server, 4, "attention_snooze", { claim_id: scheduled.claim.id, until: new Date(Date.now() + 120_000).toISOString() });
    expect(snoozed.claim).toMatchObject({ id: scheduled.claim.id, state: "pending" });
    const dismissed = await call(server, 5, "attention_dismiss", { claim_id: scheduled.claim.id, reason: "handled_elsewhere" });
    expect(dismissed.claim).toMatchObject({ id: scheduled.claim.id, state: "dismissed" });

    const second = await call(server, 6, "attention_schedule", {
      resource: "memory://thought/mcp-consume",
      eligible_after: new Date(Date.now() + 60_000).toISOString(),
      idempotency_key: "mcp-schedule-2",
      defer_while_presence: false,
    });
    expect(second.claim.defer_while_presence).toBe(false);
    const consumed = await call(server, 7, "attention_consume", { claim_id: second.claim.id, result: { handled: true } });
    expect(consumed.claim).toMatchObject({ id: second.claim.id, state: "consumed" });
    const status = await call(server, 8, "attention_status");
    expect(status.claims).toMatchObject({ dismissed: 1, consumed: 1 });
    b.close();
  });

  it("lets the owner configure and inspect a durable inactivity watch", async () => {
    const b = bridge();
    const server = new WakeBridgeMcpServer(b);
    const configured = await call(server, 1, "attention_watch_configure", {
      attention_channel: "life",
      enabled: true,
      idle_after_seconds: 1800,
    });
    expect(configured.activity_watch).toMatchObject({ attention_channel: "life", enabled: true, idle_after_ms: 1_800_000, epoch: 1, condition_status: "waiting_for_endpoint" });
    const repeated = await call(server, 2, "attention_watch_configure", {
      attention_channel: "life",
      enabled: true,
      idle_after_seconds: 1800,
    });
    expect(repeated.activity_watch.epoch).toBe(1);
    const status = await call(server, 3, "attention_watch_status", { attention_channel: "life" });
    expect(status.activity_watch).toMatchObject({ attention_channel: "life", enabled: true });
    const repeating = await call(server, 4, "attention_watch_configure", {
      attention_channel: "life",
      enabled: true,
      idle_after_seconds: 1800,
      mode: "repeat",
      repeat_after_seconds: 3600,
    });
    expect(repeating.activity_watch).toMatchObject({ mode: "repeat", repeat_after_ms: 3_600_000 });
    b.close();
  });

  it("exposes read-only event, binding, and drillable delivery diagnostics", async () => {
    const b = bridge();
    const server = new WakeBridgeMcpServer(b);
    const endpoint = b.registerEndpoint({ host_kind: "mock", session_ref: "session-1", capabilities: { session_activity_observable: true } });
    b.takeover("life", endpoint.id, 0);
    b.emit("fixture", { type: "new_reply", dedupe_key: "reply-1", resource: { uri: "fixture://reply/1" }, attention_channel_hint: "life" });
    b.tick();
    await b.dispatchDue();
    const batch = b.listBatches()[0];

    const events = await call(server, 1, "attention_event_list", { source: "fixture" });
    expect(events.events).toMatchObject([{ source: "fixture", type: "new_reply", resource: { uri: "fixture://reply/1" } }]);
    const binding = await call(server, 2, "attention_binding_status", { channel: "life" });
    expect(binding.binding).toMatchObject({ attention_channel: "life", generation: 1, live: true, endpoint: { host_kind: "mock", session_ref: "session-1" } });
    expect(JSON.stringify(binding)).not.toContain("lease_token");
    expect(JSON.stringify(binding)).not.toContain("routes");

    const health = await call(server, 3, "attention_wake_health", { batch_id: batch.id });
    expect(health.delivery).toMatchObject({ batch_id: batch.id, delivery_state: "accepted_but_unseen", unseen_since: expect.any(String) });
    expect(health.attempts).toMatchObject([{ state: "accepted", endpoint_id: endpoint.id }]);
    expect(health.receipts).toMatchObject([{ stage: "transport_accepted" }]);
    expect(health.claims).toHaveLength(1);
    expect(health.events).toHaveLength(1);
    b.close();
  });

  it("exposes owner self-service source controls without accepting provider credentials", async () => {
    const b = bridge();
    const calls: string[] = [];
    const sourceControl = {
      async status() { calls.push("status"); return { sources: [{ source: "fixture", enabled: false }] }; },
      async verify(source: string) { calls.push(`verify:${source}`); return { verification: { source, binding_matches: false } }; },
      async bootstrap(source: string, input: any) { calls.push(`bootstrap:${source}:${input.expected_subject_ref}`); return { verification: { source } }; },
      async rebind(source: string, input: any) { calls.push(`rebind:${source}:${input.expected_checkpoint_revision}`); return { verification: { source } }; },
      async enable(source: string) { calls.push(`enable:${source}`); return { source: { source, enabled: true } }; },
      async disable(source: string) { calls.push(`disable:${source}`); return { source: { source, enabled: false } }; },
    };
    const server = new WakeBridgeMcpServer({ bridge: b, source_control: sourceControl });
    const fingerprint = `sha256:${"a".repeat(64)}`;
    await call(server, 1, "attention_source_status");
    await call(server, 2, "attention_source_verify", { source: "fixture" });
    await call(server, 3, "attention_source_bootstrap", { source: "fixture", expected_subject_ref: "fixture:owner", expected_binding_fingerprint: fingerprint });
    await call(server, 4, "attention_source_rebind", { source: "fixture", expected_checkpoint_revision: 4, expected_subject_ref: "fixture:new", expected_binding_fingerprint: fingerprint, reason: "owner_changed" });
    await call(server, 5, "attention_source_enable", { source: "fixture" });
    await call(server, 6, "attention_source_disable", { source: "fixture" });
    expect(calls).toEqual(["status", "verify:fixture", "bootstrap:fixture:fixture:owner", "rebind:fixture:4", "enable:fixture", "disable:fixture"]);
    const leaked = await server.handleRequest({ jsonrpc: "2.0", id: 7, method: "tools/call", params: { name: "attention_source_verify", arguments: { source: "fixture", upstream_url: "secret" } } });
    expect(parsed(leaked)).toMatchObject({ error: "invalid_arguments" });
    b.close();
  });

  it("requires absolute RFC3339 offsets for schedule and snooze timestamps", async () => {
    const b = bridge();
    const server = new WakeBridgeMcpServer(b);
    const localTime = await server.handleRequest({
      jsonrpc: "2.0",
      id: 1,
      method: "tools/call",
      params: {
        name: "attention_schedule",
        arguments: { resource: "memory://thought/time", eligible_after: "2026-09-03T10:00:00" },
      },
    });
    expect(parsed(localTime)).toMatchObject({ error: "invalid_arguments" });
    const accepted = await call(server, 2, "attention_schedule", {
      resource: "memory://thought/time",
      eligible_after: "2026-09-03T10:00:00+08:00",
      idempotency_key: "mcp-time",
    });
    expect(accepted.claim.eligible_after).toBe("2026-09-03T02:00:00.000Z");
    const invalidSnooze = await server.handleRequest({
      jsonrpc: "2.0",
      id: 3,
      method: "tools/call",
      params: { name: "attention_snooze", arguments: { claim_id: accepted.claim.id, until: "2026-09-03T10:00:00" } },
    });
    expect(parsed(invalidSnooze)).toMatchObject({ error: "invalid_arguments" });
    b.close();
  });

  it("acknowledges a dispatched batch with endpoint/generation fencing", async () => {
    const b = bridge();
    const endpoint = b.registerEndpoint({ host_kind: "mock", session_ref: "mcp-session" });
    const binding = b.takeover("life", endpoint.id, 0);
    const server = new WakeBridgeMcpServer(b);
    const scheduled = await call(server, 1, "attention_schedule", {
      resource: { uri: "memory://thought/ack" },
      eligible_after: new Date(Date.now() - 1_000).toISOString(),
      attention_channel: "life",
      idempotency_key: "mcp-ack-1",
    });
    b.tick();
    const dispatched = await b.dispatchDue();
    expect(dispatched[0]?.status).toBe("accepted");
    const batch = b.listBatches()[0];
    const acknowledged = await call(server, 2, "attention_ack", { wake_batch_id: batch.id });
    expect(acknowledged.batch).toMatchObject({ id: batch.id, state: "seen" });
    const consumed = await call(server, 3, "attention_consume", { claim_id: scheduled.claim.id, result: { viewed: true } });
    expect(consumed.claim.state).toBe("consumed");
    expect(b.listReceipts(batch.id).map((receipt) => receipt.stage)).toEqual(["transport_accepted", "agent_seen", "agent_consumed"]);
    b.close();
  });

  it("returns bounded tool errors and JSON-RPC errors without stack traces", async () => {
    const b = bridge();
    const server = new WakeBridgeMcpServer(b);
    const invalid = await server.handleRequest({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "attention_schedule", arguments: { prompt: "secret" } } });
    expect(invalid?.result).toMatchObject({ isError: true });
    expect(parsed(invalid)).toMatchObject({ error: "invalid_arguments" });
    expect(JSON.stringify(invalid)).not.toContain("stack");
    const unknown = await server.handleRequest({ jsonrpc: "2.0", id: 2, method: "no/such/method" });
    expect(unknown?.error).toMatchObject({ code: -32601 });
    b.close();
  });

  it("runs newline-delimited stdio and suppresses notification responses", async () => {
    const b = bridge();
    const input = new PassThrough();
    const output = new PassThrough();
    const chunks: Buffer[] = [];
    output.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    const serving = runMcpStdio(new WakeBridgeMcpServer(b), input, output);
    input.write(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: {} }) + "\n");
    input.write(JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }) + "\n");
    input.end();
    await serving;
    const lines = Buffer.concat(chunks).toString("utf8").trim().split("\n").filter(Boolean).map((line) => JSON.parse(line));
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({ jsonrpc: "2.0", id: 1, result: { serverInfo: { name: "wake-bridge" } } });
    b.close();
  });
});
