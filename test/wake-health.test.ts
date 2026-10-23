import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { sqlValue } from "../src/db.js";
import { WakeBridgeMcpServer } from "../src/mcp.js";
import type { TransportResult, WakeTransport } from "../src/types.js";

const bridges: WakeBridge[] = [];
afterEach(() => { for (const bridge of bridges.splice(0)) bridge.close(); });

function fixture(transport?: WakeTransport) {
  const bridge = new WakeBridge({
    instance_id: "health-test", owner_id: "health-owner", timezone: "UTC",
    db_path: join(mkdtempSync(join(tmpdir(), "wake-health-")), "bridge.sqlite"),
  }, { clock: () => new Date("2026-10-21T12:00:00.000Z"), ...(transport ? { transports: [transport], autoMockTransport: false } : {}) });
  bridges.push(bridge);
  const endpoint = bridge.registerEndpoint({
    host_kind: transport?.kind ?? "mock", session_ref: "health-session",
    capabilities: { cold_push: true, session_activity_observable: true },
    ...(transport ? { routes: [{ kind: transport.kind, address: {} }] } : {}),
  });
  bridge.takeover("default", endpoint.id, 0);
  bridge.emit("manual", { type: "health.test", dedupe_key: "health-1", resource: { uri: "health://one" } });
  bridge.tick();
  const batch = bridge.listBatches()[0];
  const server = new WakeBridgeMcpServer(bridge);
  const health = async () => {
    const response = await server.handleRequest({
      jsonrpc: "2.0", id: 1, method: "tools/call",
      params: { name: "attention_wake_health", arguments: { batch_id: batch.id } },
    });
    expect(response?.result?.isError).not.toBe(true);
    return response!.result!.structuredContent as any;
  };
  return { bridge, endpoint, batch, server, health };
}

describe("actionable wake delivery health", () => {
  it("separates transport acceptance from agent acknowledgement and does not mutate the queue", async () => {
    const { bridge, endpoint, batch, health } = fixture();
    await bridge.dispatchDue();
    const before = bridge.inspect();
    const result = await health();
    expect(bridge.inspect()).toEqual(before);
    expect(result.delivery).toMatchObject({
      delivery_state: "accepted_but_unseen",
      evidence: { transport_accepted: true, host_attested: false, agent_seen: false },
      core_dispatch: { eligible: false, in_flight: false, not_before: null },
    });
    expect(result.delivery.next_step).toContain("attention_ack");
    expect(result.transitions.map((row: any) => row.to_state)).toEqual(["pending", "dispatching", "dispatched"]);
    expect(result.transitions_truncated).toBe(false);
    expect(result.missing_claim_ids).toEqual([]);

    bridge.ackBatch(batch.id, endpoint.id, 1);
    const seen = await health();
    expect(seen.delivery.evidence.agent_seen).toBe(true);
    expect(seen.delivery.next_step).toContain("handled");
  });

  it("shows a retrying timeout as unproven acceptance and omits error and transition free text", async () => {
    const privateText = "Bearer do-not-copy-private-provider-text";
    const { bridge, batch, health } = fixture({
      kind: "timeout",
      dispatch: () => ({ accepted: false, retryable: true, error_class: "host_timeout", error_message: privateText }),
    });
    await bridge.dispatchDue();
    bridge.db.exec(`INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
      VALUES(${sqlValue(batch.id)}, 'retry_wait', 'retry_wait', ${sqlValue(`operator_retry:${privateText}`)}, '2026-10-21T12:00:00.000Z');`);
    const result = await health();
    expect(result.delivery).toMatchObject({
      core_dispatch: { eligible: true, in_flight: false, not_before: "2026-10-21T12:00:05.000Z" },
      evidence: { transport_accepted: false, host_attested: false, agent_seen: false },
    });
    expect(result.delivery.next_step).toContain("may already");
    expect(JSON.stringify(result)).not.toContain(privateText);
    expect(JSON.stringify(result)).not.toContain("error_message");
    expect(JSON.stringify(result)).not.toContain("delivery_nonce");
  });

  it("does not mistake a cumulative attempt count for automatic retries from needs_attention", async () => {
    const { bridge, batch, health } = fixture();
    // A reported/legacy snapshot: the count alone cannot establish a later retry.
    bridge.db.exec(`UPDATE batches SET state='needs_attention', attempt=29, last_error='empty_batch' WHERE id=${sqlValue(batch.id)};`);
    const before = bridge.inspect();
    const result = await health();
    expect(result.delivery).toMatchObject({ attempt: 29, core_dispatch: { eligible: false, in_flight: false, not_before: null } });
    expect(result.delivery.next_step).toContain("claim");
    expect(bridge.inspect()).toEqual(before);
  });

  it("identifies in-flight delivery without inventing an accepted receipt", async () => {
    let finish!: (result: TransportResult) => void;
    const { bridge, batch, server, health } = fixture({ kind: "slow", dispatch: () => new Promise((resolve) => { finish = resolve; }) });
    const acknowledge = () => server.handleRequest({
      jsonrpc: "2.0", id: 2, method: "tools/call",
      params: { name: "attention_ack", arguments: { wake_batch_id: batch.id } },
    });
    const pending = bridge.dispatchDue();
    try {
      const result = await health();
      expect(result.delivery).toMatchObject({
        core_dispatch: { eligible: false, in_flight: true, not_before: null },
        evidence: { transport_accepted: false, host_attested: false, agent_seen: false },
      });
      expect(result.delivery.next_step).toContain("settle");
      const early = await acknowledge();
      expect(early?.result).toMatchObject({ isError: true, structuredContent: { error: "batch_not_dispatched" } });
    } finally {
      finish({ accepted: true });
      await pending;
    }
    expect((await acknowledge())?.result?.isError).not.toBe(true);
    expect((await acknowledge())?.result?.isError).not.toBe(true);
    expect(bridge.listReceipts(batch.id).filter((receipt) => receipt.stage === "agent_seen")).toHaveLength(1);
  });

  it("keeps host attestation distinct from an agent having read the wake", async () => {
    const { bridge, endpoint, batch, health } = fixture();
    await bridge.dispatchDue();
    // The read model must distinguish independent receipt stages even on a seen batch.
    bridge.recordReceipt(batch.id, "host_attested", undefined, endpoint.id, 1);
    bridge.db.exec(`UPDATE batches SET state='seen' WHERE id=${sqlValue(batch.id)};`);
    const result = await health();
    expect(result.delivery.evidence).toEqual({ transport_accepted: true, host_attested: true, agent_seen: false });
    expect(result.delivery.next_step).toContain("attention_ack");
    expect(JSON.stringify(result)).not.toContain("nonce_hash");
    // This fixture does not manufacture an agent receipt from the host evidence.
    expect(bridge.listReceipts(batch.id).some((receipt) => receipt.stage === "agent_seen")).toBe(false);
  });

  it("bounds transition history and reports missing claim references without repairing them", async () => {
    const { bridge, batch, health } = fixture();
    const missingId = "claim-missing";
    bridge.db.exec(`UPDATE batches SET claim_ids_json=${sqlValue(JSON.stringify([...batch.claim_ids, missingId]))} WHERE id=${sqlValue(batch.id)};`);
    bridge.db.transaction(Array.from({ length: 105 }, () =>
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
       VALUES(${sqlValue(batch.id)}, 'pending', 'pending', 'fixture', '2026-10-21T12:00:00.000Z');`));
    const before = bridge.inspect();
    const result = await health();
    expect(result.transitions).toHaveLength(100);
    expect(result.transitions_truncated).toBe(true);
    expect(result.transitions[0].seq).toBeLessThan(result.transitions[99].seq);
    expect(result.missing_claim_ids).toEqual([missingId]);
    expect(bridge.inspect()).toEqual(before);
  });

  it.each([
    { state: "pending", error: null, eligible: true, due: "2026-10-21T12:10:00.000Z", advice: "dispatch time" },
    { state: "waiting_for_endpoint", error: "no_current_binding", eligible: true, due: "2026-10-21T12:05:00.000Z", advice: "exact host session" },
    { state: "needs_attention", error: "ack_timeout", eligible: false, due: null, advice: "late attention_ack" },
    { state: "dead_letter", error: "host_rejected", eligible: false, due: null, advice: "expected-attempt" },
    { state: "cancelled", error: "all_claims_finalized", eligible: false, due: null, advice: "does not recall" },
  ])("explains $state without claiming a new dispatch", async ({ state, error, eligible, due, advice }) => {
    const { bridge, batch, health } = fixture();
    bridge.db.exec(`UPDATE batches SET state=${sqlValue(state)}, last_error=${sqlValue(error)},
      not_before='2026-10-21T12:05:00.000Z', deadline='2026-10-21T12:10:00.000Z' WHERE id=${sqlValue(batch.id)};`);
    const before = bridge.inspect();
    const result = await health();
    expect(result.delivery.core_dispatch).toEqual({ eligible, in_flight: false, not_before: due });
    expect(result.delivery.next_step).toContain(advice);
    expect(bridge.inspect()).toEqual(before);
  });
});
