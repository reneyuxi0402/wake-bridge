import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { WakeBridge } from "../src/core.js";
import { sqlJson, sqlValue } from "../src/db.js";
import { WakeBridgeMcpServer } from "../src/mcp.js";
import type { TransportContext, WakeTransport } from "../src/types.js";

const ACK_TIMEOUT_MS = 60_000;
const PRIVATE_TEXT = "private-provider-evidence-do-not-export";
const bridges: WakeBridge[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const bridge of bridges.splice(0)) bridge.close();
});

function fixture(ackTimeoutMs = ACK_TIMEOUT_MS) {
  let time = Date.parse("2026-10-21T12:00:00.000Z");
  let sequence = 0;
  const clock = { now: () => new Date(time), advance: (ms: number) => { time += ms; } };
  const calls: TransportContext[] = [];
  const transport: WakeTransport = {
    kind: "ack-visibility",
    capabilities: { cold_push: true, session_activity_observable: true },
    dispatch: (context) => {
      calls.push(context);
      return { accepted: true, transport_kind: "ack-visibility", receipt_details: { private_text: PRIVATE_TEXT } };
    },
  };
  const bridge = new WakeBridge({
    instance_id: "ack-visibility", owner_id: "ack-owner", timezone: "UTC",
    db_path: join(mkdtempSync(join(tmpdir(), "wake-ack-visibility-")), "bridge.sqlite"),
    ack_timeout_ms: ackTimeoutMs,
    endpoint_lease_ms: 24 * 60 * 60_000,
    default_policy: {
      id: "default", version: 1, match: {}, delivery: { mode: "immediate" },
      batch: { coalesce_by: "none", window_ms: 0 },
    },
  }, { clock: clock.now, transports: [transport], autoMockTransport: false });
  bridges.push(bridge);
  const endpoint = bridge.registerEndpoint({
    host_kind: transport.kind, session_ref: "busy-session", capabilities: transport.capabilities!,
    routes: [{ kind: transport.kind, address: {} }],
  });
  const server = new WakeBridgeMcpServer(bridge);
  const call = async (name: string, args: Record<string, unknown> = {}) => {
    const response = await server.handleRequest({
      jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args },
    });
    expect(response?.result?.isError).not.toBe(true);
    return response!.result!.structuredContent as any;
  };
  const health = (args: Record<string, unknown> = {}) => call("attention_wake_health", args);
  const acceptedBatch = async (channel = "default", hostAttested = true) => {
    if (!bridge.getBinding(channel)) bridge.takeover(channel, endpoint.id, 0);
    const emitted = bridge.emit("manual", {
      type: "ack.visibility", dedupe_key: `ack:${++sequence}`,
      resource: { uri: `ack://item/${sequence}` }, attention_channel_hint: channel,
    });
    bridge.tick();
    const batch = bridge.listBatches().find((item) => item.claim_ids.includes(emitted.claim!.id))!;
    await expect(bridge.dispatchDue()).resolves.toMatchObject([{ batch_id: batch.id, status: "accepted" }]);
    if (hostAttested) {
      // Exercise the real authenticated nonce path used by host injection,
      // rather than fabricating a seen state or host_attested receipt.
      const context = calls.find((item) => item.batch.id === batch.id)!;
      const binding = bridge.getBinding(channel)!;
      bridge.consumeWakeEcho({
        attention_channel: channel, endpoint_id: endpoint.id, generation: binding.generation,
        lease_token: endpoint.lease_token!, observation_id: `host-injected-${sequence}`,
        delivery_nonce: context.delivery_nonce,
      });
    }
    return bridge.getBatch(batch.id)!;
  };
  const loseResponseAfterHostEcho = () => bridge.transports.set("ack-visibility", {
    kind: "ack-visibility",
    capabilities: { cold_push: true, session_activity_observable: true },
    dispatch: (context) => {
      bridge.consumeWakeEcho({
        attention_channel: context.batch.attention_channel, endpoint_id: endpoint.id,
        generation: context.binding.generation, lease_token: endpoint.lease_token!,
        observation_id: "injected-before-timeout", delivery_nonce: context.delivery_nonce,
      });
      return { accepted: false, retryable: true, error_class: "transport_timeout" };
    },
  });
  return { bridge, clock, endpoint, calls, call, health, acceptedBatch, loseResponseAfterHostEcho };
}

describe("read-only agent acknowledgement visibility", () => {
  it("keeps a genuinely host-attested but unacknowledged batch in default health", async () => {
    const { bridge, health, acceptedBatch } = fixture();
    const batch = await acceptedBatch();
    expect(batch.state).toBe("seen");
    expect(bridge.listReceipts(batch.id).map((receipt) => receipt.stage).sort())
      .toEqual(["host_attested", "transport_accepted"]);
    expect((await health()).deliveries.map((item: any) => item.batch_id)).toEqual([batch.id]);
  });

  it.each([false, true])("reports the exact deadline before, at and after it (host attested: %s)", async (hostAttested) => {
    const { bridge, clock, health, acceptedBatch } = fixture();
    const batch = await acceptedBatch("default", hostAttested);
    const acceptedAt = bridge.listAttempts(batch.id)[0].finished_at as string;
    const deadlineAt = new Date(Date.parse(acceptedAt) + ACK_TIMEOUT_MS).toISOString();
    clock.advance(ACK_TIMEOUT_MS - 1);
    expect((await health({ batch_id: batch.id })).delivery.agent_ack)
      .toEqual({ pending: true, overdue: false, deadline_at: deadlineAt });
    clock.advance(1);
    const atDeadline = await health({ batch_id: batch.id });
    expect(atDeadline.delivery.agent_ack).toEqual({ pending: true, overdue: true, deadline_at: deadlineAt });
    expect(atDeadline.delivery.next_step).toContain("host injection");
    expect(atDeadline.delivery.next_step).toContain("not agent acknowledgement");
    expect(atDeadline.delivery.next_step).toContain("automatically resend");
    expect(atDeadline.delivery.next_step).toContain("must not acknowledge");
    clock.advance(1);
    expect((await health({ batch_id: batch.id })).delivery.agent_ack)
      .toEqual({ pending: true, overdue: true, deadline_at: deadlineAt });
    expect(bridge.getBatch(batch.id)?.state).toBe(hostAttested ? "seen" : "dispatched");
    if (hostAttested) {
      // A dispatcher sweep still does not turn seen into an ack_timeout incident.
      await expect(bridge.dispatchDue()).resolves.toEqual([]);
      expect(bridge.getBatch(batch.id)).toMatchObject({ state: "seen", last_error: null });
    }
  });

  it("clears pending diagnostics only after the real agent acknowledges a host-attested batch", async () => {
    const { bridge, clock, call, health, acceptedBatch } = fixture();
    const batch = await acceptedBatch();
    clock.advance(ACK_TIMEOUT_MS);
    expect((await health({ batch_id: batch.id })).delivery.agent_ack.overdue).toBe(true);
    await call("attention_ack", { wake_batch_id: batch.id });
    expect((await health({ batch_id: batch.id })).delivery).toMatchObject({
      state: "seen", evidence: { host_attested: true, agent_seen: true },
      agent_ack: { pending: false, overdue: false, deadline_at: null },
    });
    expect((await health()).deliveries).toEqual([]);
    expect(bridge.listReceipts(batch.id).filter((receipt) => receipt.stage === "agent_seen")).toHaveLength(1);
  });

  it.each(["consumed", "dismissed", "expired", "empty"])("does not flag a seen batch whose claims are %s", async (state) => {
    const { bridge, clock, health, acceptedBatch } = fixture();
    const batch = await acceptedBatch();
    if (state === "consumed") bridge.consumeClaim(batch.claim_ids[0], { handled: true });
    else if (state === "dismissed") bridge.dismissClaim(batch.claim_ids[0], "handled elsewhere");
    else if (state === "expired") bridge.db.exec(`UPDATE claims SET state='expired' WHERE id=${sqlValue(batch.claim_ids[0])};`);
    else bridge.db.exec(`UPDATE batches SET claim_ids_json='[]' WHERE id=${sqlValue(batch.id)};`);
    clock.advance(ACK_TIMEOUT_MS);
    expect((await health({ batch_id: batch.id })).delivery.agent_ack)
      .toEqual({ pending: false, overdue: false, deadline_at: null });
    expect((await health()).deliveries).toEqual([]);
    expect(bridge.getBatch(batch.id)?.state).toBe("seen");
  });

  it("keeps a missing claim reference pending even when every present claim is finalized", async () => {
    const { bridge, clock, health, acceptedBatch } = fixture();
    const batch = await acceptedBatch();
    bridge.consumeClaim(batch.claim_ids[0], { handled: true });
    bridge.db.exec(`UPDATE batches SET claim_ids_json=${sqlJson([...batch.claim_ids, "missing-claim"])} WHERE id=${sqlValue(batch.id)};`);
    clock.advance(ACK_TIMEOUT_MS);
    const before = bridge.inspect();
    expect((await health({ batch_id: batch.id }))).toMatchObject({
      delivery: { agent_ack: { pending: true, overdue: true } }, missing_claim_ids: ["missing-claim"],
    });
    expect((await health()).deliveries.map((item: any) => item.batch_id)).toEqual([batch.id]);
    expect(bridge.inspect()).toEqual(before);
  });

  it("does not infer acceptance or agent-ack debt from a legacy seen state alone", async () => {
    const { bridge, health } = fixture();
    bridge.emit("manual", { type: "legacy", dedupe_key: "legacy", resource: { uri: "ack://legacy" } });
    bridge.tick();
    const batch = bridge.listBatches()[0];
    bridge.db.exec(`UPDATE batches SET state='seen' WHERE id=${sqlValue(batch.id)};`);
    expect((await health({ batch_id: batch.id })).delivery).toMatchObject({
      evidence: { transport_accepted: false, host_attested: false, agent_seen: false },
      agent_ack: { pending: false, overdue: false, deadline_at: null },
    });
    expect((await health()).deliveries).toEqual([]);
  });

  it("keeps genuine current acceptance visible on a legacy seen snapshot without assuming host attestation", async () => {
    const { bridge, health, acceptedBatch } = fixture();
    const batch = await acceptedBatch("default", false);
    bridge.db.exec(`UPDATE batches SET state='seen' WHERE id=${sqlValue(batch.id)};`);
    expect((await health({ batch_id: batch.id })).delivery).toMatchObject({
      evidence: { transport_accepted: true, host_attested: false, agent_seen: false },
      agent_ack: { pending: true, overdue: false },
    });
    // The default list adds only host-attested seen items, not every legacy seen.
    expect((await health()).deliveries).toEqual([]);
  });

  it("keeps agent acknowledgement pending after a genuine host echo and lost HTTP response", async () => {
    const { bridge, clock, health, acceptedBatch, loseResponseAfterHostEcho } = fixture();
    loseResponseAfterHostEcho();
    const batch = await acceptedBatch("default", false);
    const acceptedAt = bridge.listAttempts(batch.id)[0].finished_at as string;
    const deadlineAt = new Date(Date.parse(acceptedAt) + ACK_TIMEOUT_MS).toISOString();
    expect(bridge.getBatch(batch.id)?.state).toBe("seen");
    expect((await health({ batch_id: batch.id })).delivery).toMatchObject({
      evidence: { transport_accepted: false, host_attested: true, agent_seen: false },
      agent_ack: { pending: true, overdue: false, deadline_at: deadlineAt },
    });
    clock.advance(ACK_TIMEOUT_MS);
    const before = bridge.inspect();
    expect((await health({ batch_id: batch.id })).delivery).toMatchObject({
      evidence: { transport_accepted: false, host_attested: true, agent_seen: false },
      agent_ack: { pending: true, overdue: true, deadline_at: deadlineAt },
    });
    expect((await health()).deliveries.map((item: any) => item.batch_id)).toEqual([batch.id]);
    expect(bridge.inspect()).toEqual(before);
    expect(bridge.listReceipts(batch.id).map((receipt) => receipt.stage)).toEqual(["host_attested"]);
  });

  it("keeps valid extreme timeout configurations readable when the deadline exceeds the Date range", async () => {
    const { health, acceptedBatch } = fixture(8_640_000_000_000_000);
    const batch = await acceptedBatch();
    expect((await health({ batch_id: batch.id })).delivery.agent_ack)
      .toEqual({ pending: true, overdue: false, deadline_at: null });
  });

  it.each(["attempt", "generation", "endpoint", "finished_at", "transport"])("does not use an old receipt for a mismatched current accepted %s", async (mismatch) => {
    const { bridge, health, acceptedBatch } = fixture();
    const batch = await acceptedBatch("default", false);
    if (mismatch === "attempt") bridge.db.exec(`UPDATE batches SET attempt=attempt+1 WHERE id=${sqlValue(batch.id)};`);
    else if (mismatch === "generation") bridge.db.exec(`UPDATE batches SET binding_generation=binding_generation+1 WHERE id=${sqlValue(batch.id)};`);
    else {
      const update = mismatch === "endpoint" ? "endpoint_id='previous-endpoint'"
        : mismatch === "transport" ? "transport_kind='previous-transport'" : "finished_at='2026-10-21T12:00:01.000Z'";
      bridge.db.exec(`UPDATE outbox_attempts SET ${update} WHERE batch_id=${sqlValue(batch.id)};`);
    }
    const result = await health({ batch_id: batch.id });
    expect(result.delivery.evidence.transport_accepted).toBe(true); // Historical evidence remains visible.
    expect(result.delivery.agent_ack).toEqual({ pending: false, overdue: false, deadline_at: null });
  });

  it.each(["attempt", "route"])("does not reuse consumed host correlation from a mismatched %s", async (mismatch) => {
    const { bridge, health, acceptedBatch, loseResponseAfterHostEcho } = fixture();
    loseResponseAfterHostEcho();
    const batch = await acceptedBatch("default", false);
    if (mismatch === "attempt") {
      bridge.db.transaction([
        `INSERT INTO outbox_attempts(id, batch_id, attempt_no, state, transport_kind, endpoint_id, binding_generation, started_at, finished_at)
         SELECT 'current-accepted-attempt', batch_id, attempt_no+1, state, transport_kind, endpoint_id, binding_generation, started_at, finished_at
         FROM outbox_attempts WHERE batch_id=${sqlValue(batch.id)};`,
        `UPDATE batches SET attempt=attempt+1 WHERE id=${sqlValue(batch.id)};`,
      ]);
    } else {
      bridge.db.exec(`UPDATE delivery_correlations SET endpoint_id='stale-endpoint', binding_generation=binding_generation+1,
        transport_kind='stale-transport' WHERE batch_id=${sqlValue(batch.id)};`);
    }
    expect((await health({ batch_id: batch.id })).delivery.agent_ack)
      .toEqual({ pending: false, overdue: false, deadline_at: null });
  });

  it("keeps the current accepted deadline when a newer unrelated historical receipt exists", async () => {
    const { bridge, clock, health, acceptedBatch } = fixture();
    const batch = await acceptedBatch();
    const acceptedAt = bridge.listAttempts(batch.id)[0].finished_at as string;
    clock.advance(30_000);
    bridge.recordReceipt(batch.id, "transport_accepted", { private_text: PRIVATE_TEXT }, "old-endpoint", 99);
    expect((await health({ batch_id: batch.id })).delivery.agent_ack).toEqual({
      pending: true, overdue: false, deadline_at: new Date(Date.parse(acceptedAt) + ACK_TIMEOUT_MS).toISOString(),
    });
  });

  it("reports a real dispatched ack_timeout but not other terminal or retry states", async () => {
    const { bridge, clock, health, acceptedBatch } = fixture();
    const batch = await acceptedBatch("default", false);
    clock.advance(ACK_TIMEOUT_MS);
    await bridge.dispatchDue();
    expect(bridge.getBatch(batch.id)).toMatchObject({ state: "needs_attention", last_error: "ack_timeout" });
    expect((await health({ batch_id: batch.id })).delivery.agent_ack).toMatchObject({ pending: true, overdue: true });
    for (const state of ["retry_wait", "dead_letter", "cancelled", "needs_attention"]) {
      bridge.db.exec(`UPDATE batches SET state=${sqlValue(state)}, last_error='other_error' WHERE id=${sqlValue(batch.id)};`);
      expect((await health({ batch_id: batch.id })).delivery.agent_ack)
        .toEqual({ pending: false, overdue: false, deadline_at: null });
    }
  });

  it("filters before the newest limit, preserves chronological order and bounds hydration by channel and instance", async () => {
    const { bridge, clock, health, acceptedBatch } = fixture();
    const older = await acceptedBatch();
    clock.advance(1);
    const dispatched = await acceptedBatch("default", false);
    clock.advance(1);
    const otherChannel = await acceptedBatch("other-channel");
    clock.advance(1);
    const newest = await acceptedBatch();
    clock.advance(1);
    const completed = await acceptedBatch();
    bridge.consumeClaim(completed.claim_ids[0], { handled: true });
    clock.advance(1);
    const acknowledged = await acceptedBatch();
    bridge.ackBatch(acknowledged.id, bridge.getBinding("default")!.endpoint_id, bridge.getBinding("default")!.generation);
    bridge.db.exec(`INSERT INTO batches(id, instance_id, policy_id, policy_version, attention_channel, claim_ids_json,
      event_ids_json, state, not_before, attempt, created_at, updated_at)
      SELECT 'other-instance-batch', 'other-instance', policy_id, policy_version, attention_channel, claim_ids_json,
        event_ids_json, 'dead_letter', not_before, attempt, ${sqlValue(clock.now().toISOString())}, updated_at
      FROM batches WHERE id=${sqlValue(newest.id)};`);
    const listBatches = vi.spyOn(bridge, "listBatches");
    const getBatch = vi.spyOn(bridge, "getBatch");
    expect((await health({ channel: "default", limit: 2 })).deliveries.map((item: any) => item.batch_id))
      .toEqual([dispatched.id, newest.id]);
    expect(listBatches).not.toHaveBeenCalled();
    expect(getBatch).toHaveBeenCalledTimes(2);
    expect((await health({ limit: 3 })).deliveries.map((item: any) => item.batch_id))
      .toEqual([dispatched.id, otherChannel.id, newest.id]);
    expect((await health({ channel: "default", limit: 3 })).deliveries.map((item: any) => item.batch_id))
      .toEqual([older.id, dispatched.id, newest.id]);
  });

  it("does not mutate health state or expose receipt free text, lease secrets or nonce material", async () => {
    const { bridge, clock, endpoint, calls, health, acceptedBatch } = fixture();
    const batch = await acceptedBatch();
    bridge.db.exec(`UPDATE outbox_attempts SET error_message=${sqlValue(PRIVATE_TEXT)} WHERE batch_id=${sqlValue(batch.id)};`);
    clock.advance(ACK_TIMEOUT_MS);
    const before = bridge.inspect();
    const result = JSON.stringify([await health(), await health({ batch_id: batch.id })]);
    expect(bridge.inspect()).toEqual(before);
    for (const secret of [PRIVATE_TEXT, endpoint.lease_token!, calls[0].delivery_nonce, "lease_token", "delivery_nonce", "nonce_hash", "error_message", "details_json"]) {
      expect(result).not.toContain(secret);
    }
    expect(bridge.listReceipts(batch.id).some((receipt) => receipt.stage === "agent_seen")).toBe(false);
  });
});
