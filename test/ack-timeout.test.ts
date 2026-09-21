import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { WakeBridgeMcpServer } from "../src/mcp.js";
import { operatorStatus } from "../src/operator-control.js";
import type { TransportContext, TransportResult, WakeTransport } from "../src/types.js";

class TestClock {
  value: Date;
  constructor(iso = "2026-09-20T12:00:00.000Z") { this.value = new Date(iso); }
  now = () => new Date(this.value.getTime());
  advance(ms: number): void { this.value = new Date(this.value.getTime() + ms); }
}

class SlowTransport implements WakeTransport {
  readonly kind = "slow";
  private resolver: ((result: TransportResult) => void) | null = null;
  dispatch(_context: TransportContext): Promise<TransportResult> {
    return new Promise((resolve) => { this.resolver = resolve; });
  }
  accept(): void {
    if (!this.resolver) throw new Error("slow transport has no pending dispatch");
    const resolve = this.resolver;
    this.resolver = null;
    resolve({ accepted: true, transport_kind: this.kind });
  }
}

function dbPath(): string {
  return join(mkdtempSync(join(tmpdir(), "wake-bridge-ack-timeout-")), "bridge.sqlite");
}

function config(path: string, extra: Record<string, unknown> = {}): any {
  return {
    instance_id: "ack-timeout-test",
    owner_id: "ack-timeout-owner",
    db_path: path,
    timezone: "UTC",
    endpoint_lease_ms: 24 * 60 * 60_000,
    ...extra,
  };
}

function setupBridge(clock: TestClock, extra: Record<string, unknown> = {}): WakeBridge {
  const bridge = new WakeBridge(config(dbPath(), extra), { clock: clock.now });
  prepareBatch(bridge);
  return bridge;
}

function prepareBatch(bridge: WakeBridge) {
  const endpoint = bridge.registerEndpoint({ host_kind: "mock", session_ref: "ack-session" });
  const binding = bridge.takeover("default", endpoint.id, 0);
  bridge.emit("manual", { type: "ack.test", dedupe_key: "ack:one", resource: { uri: "ack://test" } });
  bridge.tick();
  return { endpoint, binding };
}

async function dispatchAccepted(bridge: WakeBridge, endpointId?: string) {
  const result = await bridge.dispatchDue();
  expect(result[0]?.status).toBe("accepted");
  const batch = bridge.listBatches()[0];
  expect(batch.state).toBe("dispatched");
  if (endpointId) expect(result[0]?.endpoint_id).toBe(endpointId);
  return batch;
}

describe("accepted wake acknowledgement timeout", () => {
  it("uses the accepted completion timestamp, transitions at the threshold once, and exposes operator health", async () => {
    const clock = new TestClock();
    const bridge = setupBridge(clock, { ack_timeout_ms: 10_000 });
    try {
      const batch = await dispatchAccepted(bridge);
      const claimsBefore = bridge.listClaims();
      const eventsBefore = bridge.listEvents();
      const attemptsBefore = bridge.listAttempts(batch.id);
      const receiptsBefore = bridge.listReceipts(batch.id);
      // Other bookkeeping must not restart the accepted attempt's deadline.
      bridge.db.exec(`UPDATE batches SET updated_at='2026-09-21T12:00:00.000Z' WHERE id='${batch.id}';`);

      clock.advance(9_999);
      await bridge.dispatchDue();
      expect(bridge.getBatch(batch.id)?.state).toBe("dispatched");

      clock.advance(1);
      await bridge.dispatchDue();
      expect(bridge.getBatch(batch.id)).toMatchObject({ state: "needs_attention", last_error: "ack_timeout" });
      expect(bridge.listClaims()).toEqual(claimsBefore);
      expect(bridge.listEvents()).toEqual(eventsBefore);
      expect(bridge.listAttempts(batch.id)).toEqual(attemptsBefore);
      expect(bridge.listReceipts(batch.id)).toEqual(receiptsBefore);
      expect(bridge.db.query(`SELECT from_state, to_state, reason FROM batch_transitions WHERE batch_id='${batch.id}' ORDER BY seq`)).toEqual([
        { from_state: null, to_state: "pending", reason: "freeze" },
        { from_state: "pending", to_state: "dispatching", reason: "worker_lease" },
        { from_state: "dispatching", to_state: "dispatched", reason: "transport_accepted" },
        { from_state: "dispatched", to_state: "needs_attention", reason: "ack_timeout" },
      ]);

      await bridge.dispatchDue();
      expect(bridge.getBatch(batch.id)).toMatchObject({ state: "needs_attention", last_error: "ack_timeout" });
      expect(bridge.db.query(`SELECT COUNT(*) AS count FROM batch_transitions WHERE batch_id='${batch.id}' AND reason='ack_timeout'`)[0]?.count).toBe(1);
      expect(bridge.mockTransport.calls).toHaveLength(1);
      expect(operatorStatus(bridge)).toMatchObject({ health: "needs_attention", attention_required: true, queue: { needs_attention: [{ batch_id: batch.id, error_class: "ack_timeout" }] } });
    } finally {
      bridge.close();
    }
  });

  it("starts the timeout after a slow transport completes, not when dispatch starts", async () => {
    const clock = new TestClock();
    const transport = new SlowTransport();
    const bridge = new WakeBridge(config(dbPath(), { ack_timeout_ms: 10_000 }), {
      clock: clock.now,
      transports: [transport],
      autoMockTransport: false,
    });
    try {
      const endpoint = bridge.registerEndpoint({
        host_kind: "slow",
        session_ref: "slow-session",
        capabilities: { cold_push: true },
        routes: [{ kind: "slow", address: {} }],
      });
      bridge.takeover("default", endpoint.id, 0);
      bridge.emit("manual", { type: "ack.slow", dedupe_key: "ack:slow", resource: { uri: "ack://slow" } });
      bridge.tick();
      const pending = bridge.dispatchDue();
      clock.advance(60_000);
      transport.accept();
      const result = await pending;
      expect(result[0]?.status).toBe("accepted");
      const batch = bridge.listBatches()[0];
      expect(bridge.listAttempts(batch.id)[0]?.finished_at).toBe("2026-09-20T12:01:00.000Z");

      clock.advance(9_999);
      await bridge.dispatchDue();
      expect(bridge.getBatch(batch.id)?.state).toBe("dispatched");
      clock.advance(1);
      await bridge.dispatchDue();
      expect(bridge.getBatch(batch.id)).toMatchObject({ state: "needs_attention", last_error: "ack_timeout" });
    } finally {
      bridge.close();
    }
  });

  it("recovers historical accepted batches on restart, while an observer remains read-only", async () => {
    const clock = new TestClock();
    const path = dbPath();
    const first = new WakeBridge(config(path, { ack_timeout_ms: 10_000 }), { clock: clock.now });
    const { endpoint } = prepareBatch(first);
    const batch = await dispatchAccepted(first, endpoint.id);
    const transitionsBefore = first.db.query(`SELECT COUNT(*) AS count FROM batch_transitions WHERE batch_id='${batch.id}'`)[0]?.count;
    first.close();

    clock.advance(10_000);
    const observer = new WakeBridge(config(path, { ack_timeout_ms: 10_000 }), { clock: clock.now, recoverDispatchLeases: false });
    expect(observer.getBatch(batch.id)?.state).toBe("dispatched");
    expect(observer.db.query(`SELECT COUNT(*) AS count FROM batch_transitions WHERE batch_id='${batch.id}'`)[0]?.count).toBe(transitionsBefore);
    observer.close();

    const restarted = new WakeBridge(config(path, { ack_timeout_ms: 10_000 }), { clock: clock.now });
    try {
      expect(restarted.getBatch(batch.id)).toMatchObject({ state: "needs_attention", last_error: "ack_timeout" });
      expect(restarted.db.query(`SELECT COUNT(*) AS count FROM batch_transitions WHERE batch_id='${batch.id}' AND reason='ack_timeout'`)[0]?.count).toBe(1);
    } finally {
      restarted.close();
    }
  });

  it("allows one correctly fenced late ack and rejects old or unrelated generations", async () => {
    const clock = new TestClock();
    const bridge = setupBridge(clock, { ack_timeout_ms: 10_000 });
    try {
      const endpoint = bridge.listEndpoints()[0];
      const binding = bridge.getBinding("default")!;
      const batch = await dispatchAccepted(bridge, endpoint.id);
      clock.advance(10_000);
      await bridge.dispatchDue();

      const seen = bridge.ackBatch(batch.id, endpoint.id, binding.generation);
      expect(seen).toMatchObject({ state: "seen", last_error: null });
      expect(bridge.listReceipts(batch.id).filter((receipt) => receipt.stage === "agent_seen")).toHaveLength(1);
      expect(bridge.db.query(`SELECT from_state, to_state, reason FROM batch_transitions WHERE batch_id='${batch.id}' ORDER BY seq DESC LIMIT 1`)).toEqual([
        { from_state: "needs_attention", to_state: "seen", reason: "agent_ack" },
      ]);
      expect(bridge.ackBatch(batch.id, endpoint.id, binding.generation).state).toBe("seen");
      expect(bridge.listReceipts(batch.id).filter((receipt) => receipt.stage === "agent_seen")).toHaveLength(1);

      const secondClock = new TestClock();
      const second = setupBridge(secondClock, { ack_timeout_ms: 10_000 });
      try {
        const oldEndpoint = second.listEndpoints()[0];
        const oldBinding = second.getBinding("default")!;
        const oldBatch = await dispatchAccepted(second, oldEndpoint.id);
        secondClock.advance(10_000);
        await second.dispatchDue();
        const nextEndpoint = second.registerEndpoint({ host_kind: "mock", session_ref: "new-session" });
        const nextBinding = second.takeover("default", nextEndpoint.id, oldBinding.generation);
        expect(() => second.ackBatch(oldBatch.id, oldEndpoint.id, oldBinding.generation)).toThrowError(expect.objectContaining({ code: "stale_generation" }));
        expect(() => second.ackBatch(oldBatch.id, nextEndpoint.id, nextBinding.generation)).toThrowError(expect.objectContaining({ code: "stale_generation" }));
        expect(second.getBatch(oldBatch.id)?.state).toBe("needs_attention");
      } finally {
        second.close();
      }
    } finally {
      bridge.close();
    }
  });

  it("does not timeout a normal seen batch, unrelated needs_attention, or a non-current accepted attempt", async () => {
    const clock = new TestClock();
    const bridge = setupBridge(clock, { ack_timeout_ms: 10_000 });
    try {
      const endpoint = bridge.listEndpoints()[0];
      const binding = bridge.getBinding("default")!;
      const seenBatch = await dispatchAccepted(bridge, endpoint.id);
      bridge.ackBatch(seenBatch.id, endpoint.id, binding.generation);
      clock.advance(20_000);
      await bridge.dispatchDue();
      expect(bridge.getBatch(seenBatch.id)?.state).toBe("seen");

      const otherBatch = bridge.listBatches()[0];
      bridge.db.exec(`UPDATE batches SET state='needs_attention', last_error='operator_fault' WHERE id='${otherBatch.id}';`);
      expect(() => bridge.ackBatch(otherBatch.id, endpoint.id, binding.generation)).toThrowError(expect.objectContaining({ code: "batch_not_ready" }));

      const mismatchClock = new TestClock();
      const mismatch = setupBridge(mismatchClock, { ack_timeout_ms: 10_000 });
      try {
        const mismatchEndpoint = mismatch.listEndpoints()[0];
        const mismatchBatch = await dispatchAccepted(mismatch, mismatchEndpoint.id);
        mismatch.db.exec(`UPDATE outbox_attempts SET attempt_no=99 WHERE batch_id='${mismatchBatch.id}';`);
        mismatchClock.advance(10_000);
        await mismatch.dispatchDue();
        expect(mismatch.getBatch(mismatchBatch.id)?.state).toBe("dispatched");
        mismatch.db.exec(`UPDATE outbox_attempts SET attempt_no=1, binding_generation=999 WHERE batch_id='${mismatchBatch.id}';`);
        await mismatch.dispatchDue();
        expect(mismatch.getBatch(mismatchBatch.id)?.state).toBe("dispatched");
      } finally {
        mismatch.close();
      }
    } finally {
      bridge.close();
    }
  });

  it("supports a late acknowledgement through the MCP attention_ack tool", async () => {
    const clock = new TestClock();
    const bridge = setupBridge(clock, { ack_timeout_ms: 10_000 });
    try {
      const batch = await dispatchAccepted(bridge);
      clock.advance(10_000);
      await bridge.dispatchDue();
      const result = await new WakeBridgeMcpServer({ bridge }).callTool("attention_ack", { wake_batch_id: batch.id });
      expect(result.batch).toMatchObject({ batch_id: batch.id, state: "seen" });
    } finally {
      bridge.close();
    }
  });

  it("defaults to 30 minutes and rejects invalid timeout configuration", () => {
    const defaultBridge = new WakeBridge(config(dbPath()));
    expect(defaultBridge.config.ack_timeout_ms).toBe(30 * 60_000);
    defaultBridge.close();
    for (const value of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER]) {
      expect(() => new WakeBridge(config(dbPath(), { ack_timeout_ms: value }))).toThrowError(expect.objectContaining({ code: "invalid_config" }));
    }
    expect(() => new WakeBridge(config(dbPath(), { ack_timeout_ms: "1000" }))).toThrowError(expect.objectContaining({ code: "invalid_config" }));
    const nullDefault = new WakeBridge(config(dbPath(), { ack_timeout_ms: null }));
    expect(nullDefault.config.ack_timeout_ms).toBe(30 * 60_000);
    nullDefault.close();
  });
});
