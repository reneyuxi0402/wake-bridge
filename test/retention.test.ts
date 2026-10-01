import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { pruneHistory } from "../src/retention.js";

class TestClock {
  value = new Date("2026-09-01T12:00:00.000Z");
  now = () => new Date(this.value.getTime());
  advanceDays(days: number): void { this.value = new Date(this.value.getTime() + days * 24 * 60 * 60_000); }
}

const DAY = 24 * 60 * 60_000;

function setup() {
  const clock = new TestClock();
  const bridge = new WakeBridge({
    instance_id: "retention",
    owner_id: "owner",
    db_path: join(mkdtempSync(join(tmpdir(), "wake-bridge-retention-")), "bridge.sqlite"),
    timezone: "UTC",
    endpoint_lease_ms: 365 * DAY,
    ack_timeout_ms: 365 * DAY,
  } as any, { clock: clock.now });
  const endpoint = bridge.registerEndpoint({ host_kind: "mock", session_ref: "session", lease_ms: 365 * DAY });
  const binding = bridge.takeover("default", endpoint.id, 0);
  return { clock, bridge, endpoint, binding };
}

function count(bridge: WakeBridge, table: string): number {
  return Number(bridge.db.query<{ total: number }>(`SELECT COUNT(*) AS total FROM ${table};`)[0].total);
}

function emit(bridge: WakeBridge, key: string) {
  return bridge.emit("manual", { type: "job.completed", dedupe_key: key, resource: { uri: `job://${key}` } });
}

describe("pruneHistory", () => {
  it("removes finished history only, and only when applied", async () => {
    const { clock, bridge, endpoint, binding } = setup();

    // Delivered, acknowledged and consumed: everything about it is finished.
    const done = emit(bridge, "done");
    bridge.tick();
    const [delivered] = await bridge.dispatchDue();
    bridge.ackBatch(delivered.batch_id, endpoint.id, binding.generation);
    bridge.consumeClaim(done.claim!.id, null);

    // Dispatched but never acknowledged: the batch is still in flight, so it and its claim stay.
    const inFlight = emit(bridge, "in-flight");
    bridge.tick();
    const [dispatched] = await bridge.dispatchDue();
    bridge.consumeClaim(inFlight.claim!.id, null);

    // Dismissed before it was ever batched.
    const dismissed = emit(bridge, "dismissed");
    bridge.dismissClaim(dismissed.claim!.id, "handled elsewhere");

    // A finished self commitment.
    const commitment = bridge.scheduleClaim({ resource: { uri: "job://commitment" }, eligible_after: clock.now().toISOString(), idempotency_key: "commitment" });
    bridge.dismissClaim(commitment.claim.id, "done");

    clock.advanceDays(31);
    const recent = emit(bridge, "recent");
    bridge.consumeClaim(recent.claim!.id, null);
    const live = emit(bridge, "live");

    const events = count(bridge, "events");
    const idempotency = count(bridge, "idempotency_keys");
    const eventTransitions = count(bridge, "event_transitions");

    const preview = pruneHistory(bridge, { older_than_days: 30 });
    expect(preview).toMatchObject({
      cutoff: new Date(clock.value.getTime() - 30 * DAY).toISOString(),
      applied: false,
      deleted: { batches: 1, claims: 3 },
    });
    expect(preview.deleted.batch_transitions).toBeGreaterThan(0);
    expect(preview.deleted.receipts).toBeGreaterThan(0);
    expect(bridge.getBatch(delivered.batch_id)).not.toBeNull();
    expect(bridge.getClaim(done.claim!.id)).not.toBeNull();

    const applied = pruneHistory(bridge, { older_than_days: 30, apply: true });
    expect(applied).toEqual({ ...preview, applied: true });

    expect(bridge.getBatch(delivered.batch_id)).toBeNull();
    expect(bridge.listReceipts(delivered.batch_id)).toEqual([]);
    expect(bridge.listAttempts(delivered.batch_id)).toEqual([]);
    for (const claim of [done.claim!, dismissed.claim!, commitment.claim]) expect(bridge.getClaim(claim.id)).toBeNull();
    expect(bridge.db.query(`SELECT 1 FROM claim_transitions WHERE claim_id='${done.claim!.id}';`)).toEqual([]);

    expect(bridge.getBatch(dispatched.batch_id)?.state).toBe("dispatched");
    expect(bridge.getClaim(inFlight.claim!.id)?.state).toBe("consumed");
    expect(bridge.getClaim(recent.claim!.id)?.state).toBe("consumed");
    expect(bridge.getClaim(live.claim!.id)?.state).toBe("pending");

    // The dedupe record is untouched, so nothing finished can come back as new work.
    expect(count(bridge, "events")).toBe(events);
    expect(count(bridge, "idempotency_keys")).toBe(idempotency);
    expect(count(bridge, "event_transitions")).toBe(eventTransitions);
    const redelivered = emit(bridge, "done");
    expect(redelivered).toMatchObject({ duplicate: true, claim: undefined });
    expect(() => bridge.scheduleClaim({ resource: { uri: "job://commitment" }, eligible_after: clock.now().toISOString(), idempotency_key: "commitment" }))
      .toThrowError(expect.objectContaining({ code: "claim_pruned", status: 409 }));

    expect(pruneHistory(bridge, { older_than_days: 30, apply: true }).deleted).toMatchObject({ batches: 0, claims: 0 });
    bridge.close();
  });

  it("keeps a needs_attention batch and the claims it references", async () => {
    const { clock, bridge } = setup();
    const stuck = emit(bridge, "stuck");
    bridge.tick();
    const [result] = await bridge.dispatchDue();
    bridge.db.exec(`UPDATE batches SET state='needs_attention', last_error='ack_timeout' WHERE id='${result.batch_id}';`);
    bridge.db.exec(`UPDATE claims SET state='consumed' WHERE id='${stuck.claim!.id}';`);
    // Only the dispatcher may resolve it (to cancelled); until then prune leaves it and its claim alone.
    clock.advanceDays(60);

    expect(pruneHistory(bridge, { older_than_days: 30, apply: true }).deleted).toMatchObject({ batches: 0, claims: 0 });
    expect(bridge.getClaim(stuck.claim!.id)).not.toBeNull();
    bridge.close();
  });

  it("rejects a missing or out-of-range age", () => {
    const { bridge } = setup();
    for (const value of [Number.NaN, 0, -1, 1.5, 3651]) {
      expect(() => pruneHistory(bridge, { older_than_days: value })).toThrowError(expect.objectContaining({ code: "invalid_arguments" }));
    }
    bridge.close();
  });
});
