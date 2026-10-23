import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { sqlJson, sqlValue } from "../src/db.js";
import type { BridgeConfig, TransportContext, TransportResult, WakeTransport } from "../src/types.js";

class TestClock {
  private value = new Date("2026-10-01T12:00:00.000Z");
  now = () => new Date(this.value.getTime());
  advance(ms: number): void { this.value = new Date(this.value.getTime() + ms); }
}

// The caller controls completion, so all timeout/ack interleavings below are
// deterministic and do not need real sleeps or a live host service.
class ControlledTransport implements WakeTransport {
  readonly kind = "controlled";
  readonly capabilities = { cold_push: true, session_activity_observable: true };
  readonly calls: TransportContext[] = [];
  private readonly waiters: Array<(result: TransportResult) => void> = [];
  dispatch(context: TransportContext): Promise<TransportResult> {
    this.calls.push(context);
    return new Promise((resolve) => this.waiters.push(resolve));
  }
  finish(result: TransportResult): void {
    const resolve = this.waiters.shift();
    if (!resolve) throw new Error("no pending transport call");
    resolve(result);
  }
  timeout(): void {
    this.finish({ accepted: false, retryable: true, error_class: "transport_timeout" });
  }
}

function setup(claimCount = 1) {
  const clock = new TestClock();
  const transport = new ControlledTransport();
  const config: BridgeConfig = {
    instance_id: "delivery-reliability",
    owner_id: "delivery-owner",
    db_path: join(mkdtempSync(join(tmpdir(), "wake-bridge-delivery-")), "bridge.sqlite"),
    timezone: "UTC",
    endpoint_lease_ms: 24 * 60 * 60_000,
    dispatch_lease_ms: 120_000,
    retry_delay_ms: 5_000,
    default_policy: {
      id: "default", version: 1, match: {}, delivery: { mode: "immediate" },
      batch: { coalesce_by: "coalesce_key", window_ms: 1 },
    },
  };
  const bridge = new WakeBridge(config, { clock: clock.now, transports: [transport], autoMockTransport: false });
  const endpoint = bridge.registerEndpoint({
    host_kind: transport.kind, session_ref: "delivery-session",
    capabilities: transport.capabilities, routes: [{ kind: transport.kind, address: {} }],
  });
  const binding = bridge.takeover("default", endpoint.id, 0);
  for (let index = 0; index < claimCount; index += 1) {
    bridge.emit("manual", {
      type: "delivery.test", dedupe_key: `delivery:${index}`, coalesce_key: "delivery-batch",
      resource: { uri: `delivery://item/${index}` },
    });
  }
  bridge.tick();
  clock.advance(1);
  const batches = bridge.listBatches();
  expect(batches).toHaveLength(1);
  expect(batches[0].claim_ids).toHaveLength(claimCount);
  return { bridge, clock, transport, config, endpoint, binding, batch: batches[0] };
}

type Fixture = ReturnType<typeof setup>;

function reopen(fixture: Fixture, recoverDispatchLeases = true) {
  const transport = new ControlledTransport();
  const bridge = new WakeBridge(fixture.config, {
    clock: fixture.clock.now, transports: [transport], autoMockTransport: false, recoverDispatchLeases,
  });
  return { bridge, transport };
}

function transitions(bridge: WakeBridge, batchId: string) {
  return bridge.db.query(`SELECT from_state, to_state, reason FROM batch_transitions WHERE batch_id=${sqlValue(batchId)} ORDER BY seq;`);
}

// Model the durable rows left by the old empty-payload branch. The old attempt
// was never sent to the host; its failure and all original refs must survive.
function seedLegacyEmptyBatch(fixture: Fixture): void {
  const { bridge, batch, endpoint, binding, clock } = fixture;
  const now = clock.now().toISOString();
  bridge.db.transaction([
    `INSERT INTO outbox_attempts(id, batch_id, attempt_no, state, endpoint_id, binding_generation, started_at, finished_at, error_class, error_message)
     VALUES('legacy-empty-attempt', ${sqlValue(batch.id)}, 1, 'failed', ${sqlValue(endpoint.id)}, ${binding.generation}, ${sqlValue(now)}, ${sqlValue(now)}, 'empty_batch', 'all claims were finalized before dispatch');`,
    `UPDATE batches SET state='needs_attention', attempt=1, binding_generation=${binding.generation}, last_error='empty_batch' WHERE id=${sqlValue(batch.id)};`,
  ]);
}

describe("delivery reliability evidence and finalized-batch recovery", () => {
  it("records a slow timeout as a failed attempt, never as transport acceptance or agent seen", async () => {
    const { bridge, clock, transport, batch } = setup();
    try {
      const pending = bridge.dispatchDue();
      expect(transport.calls).toHaveLength(1);
      expect(bridge.getBatch(batch.id)?.state).toBe("dispatching");
      expect(bridge.listReceipts(batch.id)).toEqual([]);
      clock.advance(60_000);
      transport.timeout();
      await expect(pending).resolves.toMatchObject([{ status: "retry_wait", error_class: "transport_timeout" }]);
      expect(bridge.listAttempts(batch.id)).toMatchObject([{
        state: "failed", error_class: "transport_timeout", finished_at: clock.now().toISOString(),
      }]);
      expect(bridge.getBatch(batch.id)?.not_before).toBe(new Date(clock.now().getTime() + 5_000).toISOString());
      expect(bridge.getClaim(batch.claim_ids[0])?.state).toBe("batched");
      expect(bridge.listReceipts(batch.id)).toEqual([]);
      expect(bridge.listDeliveryCorrelations()[0].state).toBe("discarded");

      clock.advance(4_999);
      expect(await bridge.dispatchDue()).toEqual([]);
      expect(transport.calls).toHaveLength(1);
      clock.advance(1);
      const retry = bridge.dispatchDue();
      expect(transport.calls).toHaveLength(2);
      transport.timeout();
      await expect(retry).resolves.toMatchObject([{ status: "retry_wait" }]);
      expect(bridge.listReceipts(batch.id)).toEqual([]);
    } finally {
      bridge.close();
    }
  });

  it("uses an authenticated host nonce echo before a lost HTTP response to prevent redelivery, without inventing agent ack", async () => {
    const { bridge, clock, transport, batch, endpoint, binding } = setup();
    try {
      const pending = bridge.dispatchDue();
      const context = transport.calls[0];
      clock.advance(30_000);
      bridge.consumeWakeEcho({
        attention_channel: "default", endpoint_id: endpoint.id, generation: binding.generation,
        lease_token: endpoint.lease_token!, observation_id: "host-injected-wake", delivery_nonce: context.delivery_nonce,
      });
      expect(bridge.listReceipts(batch.id).map((receipt) => receipt.stage)).toEqual(["host_attested"]);
      expect(bridge.getBatch(batch.id)?.state).toBe("dispatching");
      clock.advance(30_000);
      transport.timeout();
      await expect(pending).resolves.toMatchObject([{ status: "accepted" }]);
      expect(bridge.getBatch(batch.id)).toMatchObject({ state: "seen", last_error: null });
      expect(bridge.listAttempts(batch.id)).toMatchObject([{ state: "accepted", error_class: null }]);
      expect(bridge.listReceipts(batch.id).map((receipt) => receipt.stage)).toEqual(["host_attested"]);
      clock.advance(5_000);
      expect(await bridge.dispatchDue()).toEqual([]);
      expect(transport.calls).toHaveLength(1);
    } finally {
      bridge.close();
    }
  });

  it("rejects an early genuine agent ack, then accepts the same batch ack idempotently after transport acceptance", async () => {
    const { bridge, clock, transport, batch, endpoint, binding } = setup();
    try {
      const pending = bridge.dispatchDue();
      expect(transport.calls[0].payload.wake_batch_id).toBe(batch.id);
      expect(() => bridge.ackBatch(batch.id, endpoint.id, binding.generation))
        .toThrowError(expect.objectContaining({ code: "batch_not_ready", status: 409 }));
      expect(bridge.listReceipts(batch.id)).toEqual([]);
      expect(bridge.listAttempts(batch.id)[0].state).toBe("leased");

      clock.advance(1);
      transport.finish({ accepted: true, transport_kind: transport.kind });
      await expect(pending).resolves.toMatchObject([{ status: "accepted" }]);
      expect(bridge.getBatch(batch.id)?.state).toBe("dispatched");
      expect(bridge.listReceipts(batch.id).map((receipt) => receipt.stage)).toEqual(["transport_accepted"]);
      const acknowledged = bridge.ackBatch(batch.id, endpoint.id, binding.generation);
      expect(acknowledged.state).toBe("seen");
      expect(bridge.ackBatch(batch.id, endpoint.id, binding.generation)).toEqual(acknowledged);
      expect(bridge.listReceipts(batch.id).filter((receipt) => receipt.stage === "agent_seen")).toHaveLength(1);
      expect(transitions(bridge, batch.id).filter((transition) => transition.reason === "agent_ack")).toHaveLength(1);
      expect(bridge.getClaim(batch.claim_ids[0])?.state).toBe("batched");
    } finally {
      bridge.close();
    }
  });

  it.each(["consumed", "dismissed", "mixed"])("closes all %s claims finalized during an in-flight timeout without another attempt or host call", async (mode) => {
    const { bridge, clock, transport, batch } = setup(2);
    try {
      const pending = bridge.dispatchDue();
      for (const [index, claimId] of batch.claim_ids.entries()) {
        if (mode === "dismissed" || (mode === "mixed" && index === 1)) bridge.dismissClaim(claimId, "handled elsewhere");
        else bridge.consumeClaim(claimId, { handled: true });
      }
      expect(bridge.getBatch(batch.id)?.state).toBe("dispatching");
      const receipts = bridge.listReceipts(batch.id);
      clock.advance(60_000);
      transport.timeout();
      await expect(pending).resolves.toMatchObject([{ status: "retry_wait", error_class: "transport_timeout" }]);
      const attempts = bridge.listAttempts(batch.id);
      clock.advance(5_000);
      expect(await bridge.dispatchDue()).toEqual([]);
      expect(bridge.getBatch(batch.id)).toMatchObject({
        state: "cancelled", attempt: 1, last_error: "transport_timeout", claim_ids: batch.claim_ids, event_ids: batch.event_ids,
      });
      expect(bridge.listAttempts(batch.id)).toEqual(attempts);
      expect(bridge.listReceipts(batch.id)).toEqual(receipts);
      expect(transitions(bridge, batch.id).at(-1)).toEqual({
        from_state: "retry_wait", to_state: "cancelled", reason: "all_claims_finalized_reconciled",
      });
      clock.advance(60_000);
      expect(await bridge.dispatchDue()).toEqual([]);
      expect(transport.calls).toHaveLength(1);
    } finally {
      bridge.close();
    }
  });

  it.each(["retry_wait", "dispatching"])("recovers finalized %s work after restart without another host dispatch or fabricated receipt", async (state) => {
    const fixture = setup();
    const { bridge, clock, transport, batch } = fixture;
    const pending = bridge.dispatchDue();
    bridge.consumeClaim(batch.claim_ids[0], { handled: true });
    if (state === "retry_wait") {
      transport.timeout();
      await pending;
    }
    const receipts = bridge.listReceipts(batch.id);
    const attempts = bridge.listAttempts(batch.id);
    bridge.close();
    clock.advance(120_001);
    const observer = reopen(fixture, false);
    expect(observer.bridge.getBatch(batch.id)?.state).toBe(state);
    expect(observer.bridge.listAttempts(batch.id)).toEqual(attempts);
    observer.bridge.close();

    const restarted = reopen(fixture);
    try {
      expect(restarted.bridge.getBatch(batch.id)).toMatchObject({ state: "cancelled", attempt: 1, claim_ids: batch.claim_ids });
      const recoveredAttempts = restarted.bridge.listAttempts(batch.id);
      if (state === "dispatching") expect(recoveredAttempts).toMatchObject([{ state: "expired", error_class: "dispatcher_recovered" }]);
      else expect(recoveredAttempts).toEqual(attempts);
      expect(restarted.bridge.listReceipts(batch.id)).toEqual(receipts);
      expect(await restarted.bridge.dispatchDue()).toEqual([]);
      expect(restarted.transport.calls).toEqual([]);
      if (state === "dispatching") {
        transport.finish({ accepted: true, transport_kind: transport.kind });
        await expect(pending).resolves.toMatchObject([{ status: "skipped", error_class: "stale_attempt" }]);
        expect(restarted.bridge.listAttempts(batch.id)).toEqual(recoveredAttempts);
        expect(restarted.bridge.listReceipts(batch.id)).toEqual(receipts);
      }
      expect(transitions(restarted.bridge, batch.id).filter((transition) => transition.reason === "all_claims_finalized_reconciled")).toHaveLength(1);
    } finally {
      restarted.bridge.close();
    }
  });

  it("retries only the unfinished claim when a peer claim was finalized in flight", async () => {
    const { bridge, clock, transport, batch } = setup(2);
    try {
      const pending = bridge.dispatchDue();
      bridge.consumeClaim(batch.claim_ids[0], { handled: true });
      transport.timeout();
      await pending;
      clock.advance(5_000);
      const retry = bridge.dispatchDue();
      expect(bridge.getBatch(batch.id)?.state).toBe("dispatching");
      expect(transport.calls).toHaveLength(2);
      expect(transport.calls[1].payload.claim_refs.map((claim) => claim.claim_id)).toEqual([batch.claim_ids[1]]);
      transport.finish({ accepted: true, transport_kind: transport.kind });
      await expect(retry).resolves.toMatchObject([{ status: "accepted" }]);
      expect(bridge.getBatch(batch.id)).toMatchObject({ state: "dispatched", claim_ids: batch.claim_ids });
      expect(bridge.getClaim(batch.claim_ids[1])?.state).toBe("batched");
      expect(transitions(bridge, batch.id).some((transition) => transition.to_state === "cancelled")).toBe(false);
    } finally {
      bridge.close();
    }
  });

  it("reconciles a legacy empty_batch on restart and preserves its failed attempt, original refs, and genuine receipts", async () => {
    const fixture = setup(2);
    const { bridge, batch } = fixture;
    seedLegacyEmptyBatch(fixture);
    bridge.consumeClaim(batch.claim_ids[0], { handled: true });
    bridge.dismissClaim(batch.claim_ids[1], "handled elsewhere");
    const attempts = bridge.listAttempts(batch.id);
    const receipts = bridge.listReceipts(batch.id);
    const before = transitions(bridge, batch.id);
    bridge.close();
    const restarted = reopen(fixture);
    try {
      expect(restarted.bridge.getBatch(batch.id)).toMatchObject({
        state: "cancelled", attempt: 1, last_error: "empty_batch", claim_ids: batch.claim_ids, event_ids: batch.event_ids,
      });
      expect(restarted.bridge.listAttempts(batch.id)).toEqual(attempts);
      expect(restarted.bridge.listReceipts(batch.id)).toEqual(receipts);
      expect(transitions(restarted.bridge, batch.id)).toEqual([...before, {
        from_state: "needs_attention", to_state: "cancelled", reason: "all_claims_finalized_reconciled",
      }]);
      expect(await restarted.bridge.dispatchDue()).toEqual([]);
      expect(restarted.transport.calls).toEqual([]);
      expect(transitions(restarted.bridge, batch.id)).toHaveLength(before.length + 1);
    } finally {
      restarted.bridge.close();
    }
  });

  it.each(["missing", "pending", "deferred", "eligible", "batched", "no_refs"])("does not silently reconcile legacy empty_batch with %s claim evidence", async (evidence) => {
    const fixture = setup(2);
    const { bridge, batch } = fixture;
    seedLegacyEmptyBatch(fixture);
    bridge.consumeClaim(batch.claim_ids[0], null);
    if (evidence === "missing") bridge.db.exec(`DELETE FROM claims WHERE id=${sqlValue(batch.claim_ids[1])};`);
    else if (evidence === "no_refs") bridge.db.exec(`UPDATE batches SET claim_ids_json=${sqlJson([])} WHERE id=${sqlValue(batch.id)};`);
    else bridge.db.exec(`UPDATE claims SET state=${sqlValue(evidence)} WHERE id=${sqlValue(batch.claim_ids[1])};`);
    const attempts = bridge.listAttempts(batch.id);
    const receipts = bridge.listReceipts(batch.id);
    const before = transitions(bridge, batch.id);
    bridge.close();
    const restarted = reopen(fixture);
    try {
      expect(restarted.bridge.getBatch(batch.id)).toMatchObject({ state: "needs_attention", last_error: "empty_batch" });
      expect(await restarted.bridge.dispatchDue()).toEqual([]);
      expect(restarted.transport.calls).toEqual([]);
      expect(restarted.bridge.listAttempts(batch.id)).toEqual(attempts);
      expect(restarted.bridge.listReceipts(batch.id)).toEqual(receipts);
      expect(transitions(restarted.bridge, batch.id)).toEqual(before);
    } finally {
      restarted.bridge.close();
    }
  });

  it.each(["missing", "snoozed"])("keeps %s claims visible when retry payload is empty and records its actual transition", async (evidence) => {
    const { bridge, clock, transport, batch } = setup();
    try {
      const pending = bridge.dispatchDue();
      if (evidence === "missing") bridge.db.exec(`DELETE FROM claims WHERE id=${sqlValue(batch.claim_ids[0])};`);
      else bridge.snoozeClaim(batch.claim_ids[0], new Date(clock.now().getTime() + 60_000).toISOString());
      transport.timeout();
      await pending;
      clock.advance(5_000);
      await expect(bridge.dispatchDue()).resolves.toMatchObject([{ status: "needs_attention", error_class: "empty_batch" }]);
      expect(bridge.getBatch(batch.id)).toMatchObject({ state: "needs_attention", attempt: 2, claim_ids: batch.claim_ids });
      expect(transitions(bridge, batch.id).at(-1)).toEqual({
        from_state: "dispatching", to_state: "needs_attention", reason: "empty_batch",
      });
      expect(bridge.listAttempts(batch.id)).toMatchObject([
        { state: "failed" }, { state: "failed", error_class: "empty_batch", error_message: "batch has no deliverable claims" },
      ]);
      expect(bridge.listReceipts(batch.id)).toEqual([]);
      expect(await bridge.dispatchDue()).toEqual([]);
      expect(transport.calls).toHaveLength(1);
    } finally {
      bridge.close();
    }
  });
});

describe("bounded transport retry backoff", () => {
  function fixture(base = 5_000) {
    const directory = mkdtempSync(join(tmpdir(), "wake-bridge-retry-backoff-"));
    const clock = new TestClock();
    const transport = new ControlledTransport();
    const config: BridgeConfig = {
      instance_id: "retry-backoff", owner_id: "retry-owner",
      db_path: join(directory, "bridge.sqlite"), timezone: "UTC",
      endpoint_lease_ms: 48 * 60 * 60_000, retry_delay_ms: base,
      default_policy: {
        id: "default", version: 1, match: {}, delivery: { mode: "immediate" },
        batch: { coalesce_by: "coalesce_key", window_ms: 1 },
      },
    };
    const open = () => new WakeBridge(config, { clock: clock.now, transports: [transport], autoMockTransport: false });
    let bridge = open();
    const endpoint = bridge.registerEndpoint({
      host_kind: transport.kind, session_ref: "retry-session", capabilities: transport.capabilities,
      routes: [{ kind: transport.kind, address: {} }],
    });
    bridge.takeover("default", endpoint.id, 0);
    bridge.emit("manual", { type: "retry.test", dedupe_key: "retry", resource: { uri: "fixture://retry" } });
    bridge.tick();
    clock.advance(1);
    const batchId = bridge.listBatches()[0].id;
    return {
      get bridge() { return bridge; }, clock, transport, endpoint, batchId,
      restart() { bridge.close(); bridge = open(); },
      close() { bridge.close(); rmSync(directory, { recursive: true, force: true }); },
    };
  }

  it("backs off cumulative attempts through restart and generation changes, capped at five minutes", async () => {
    const f = fixture();
    try {
      for (const [index, delay] of [5_000, 10_000, 20_000, 40_000, 80_000, 160_000, 300_000, 300_000].entries()) {
        const pending = f.bridge.dispatchDue();
        expect(f.transport.calls).toHaveLength(index + 1);
        // The completion timestamp, not the send timestamp, anchors the delay.
        f.clock.advance(60_000);
        f.transport.finish({ accepted: false, retryable: true, error_class: "host_timeout" });
        await expect(pending).resolves.toMatchObject([{ status: "retry_wait" }]);
        const batch = f.bridge.getBatch(f.batchId)!;
        expect(batch.attempt).toBe(index + 1);
        expect(Date.parse(batch.not_before) - f.clock.now().getTime()).toBe(delay);
        if (index === 2) {
          f.restart();
          expect(f.bridge.getBatch(f.batchId)).toMatchObject({ attempt: batch.attempt, not_before: batch.not_before });
          f.bridge.takeover("default", f.endpoint.id, 1);
          expect(f.bridge.getBatch(f.batchId)).toMatchObject({ attempt: batch.attempt, not_before: batch.not_before });
        }
        f.clock.advance(delay - 1);
        expect(await f.bridge.dispatchDue()).toEqual([]);
        expect(f.transport.calls).toHaveLength(index + 1);
        f.clock.advance(1);
      }
      expect(f.transport.calls.at(-1)?.binding.generation).toBe(2);
      expect(f.bridge.listReceipts(f.batchId)).toEqual([]);
    } finally { f.close(); }
  });

  it("keeps zero base immediately retryable without advancing the clock", async () => {
    const f = fixture(0);
    try {
      for (let attempt = 1; attempt <= 3; attempt += 1) {
        const pending = f.bridge.dispatchDue();
        f.transport.timeout();
        await pending;
        expect(f.bridge.getBatch(f.batchId)).toMatchObject({ attempt, not_before: f.clock.now().toISOString() });
      }
      expect(f.transport.calls).toHaveLength(3);
    } finally { f.close(); }
  });

  it("prioritizes valid transport delays with the existing 24-hour cap and backs off invalid ones", async () => {
    const f = fixture();
    try {
      const cases = [[0, 0], [37, 37], [172_800_000, 86_400_000], [-1, 40_000], [NaN, 80_000], [Infinity, 160_000]];
      for (const [retryAfter, delay] of cases) {
        const pending = f.bridge.dispatchDue();
        f.transport.finish({ accepted: false, retryable: true, error_class: "host_timeout", retry_after_ms: retryAfter });
        await pending;
        expect(Date.parse(f.bridge.getBatch(f.batchId)!.not_before) - f.clock.now().getTime()).toBe(delay);
        f.clock.advance(delay);
      }
    } finally { f.close(); }
  });

  it.each([5_000, 600_000])("bounds huge cumulative attempts without shortening a %i ms base", async (base) => {
    const f = fixture(base);
    try {
      f.bridge.db.exec(`UPDATE batches SET attempt=1000000 WHERE id=${sqlValue(f.batchId)};`);
      const pending = f.bridge.dispatchDue();
      f.transport.timeout();
      await pending;
      expect(f.bridge.getBatch(f.batchId)!.attempt).toBe(1_000_001);
      expect(Date.parse(f.bridge.getBatch(f.batchId)!.not_before) - f.clock.now().getTime()).toBe(Math.max(base, 300_000));
    } finally { f.close(); }
  });
});
