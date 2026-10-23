import { afterEach, describe, expect, it, vi } from "vitest";
import { WakeBridge } from "../src/core.js";
import { sqlJson, sqlValue } from "../src/db.js";
import type { Binding, WakeBatch, WakePayload } from "../src/types.js";

const bridges: WakeBridge[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const bridge of bridges.splice(0)) bridge.close();
});

function fixture(count: number) {
  const now = "2026-10-22T12:00:00.000Z";
  const bridge = new WakeBridge({ instance_id: "payload-read", owner_id: "fixture", db_path: ":memory:", timezone: "UTC" }, {
    clock: () => new Date(now),
  });
  bridges.push(bridge);
  const endpoint = bridge.registerEndpoint({ host_kind: "mock", session_ref: "payload-fixture" });
  const binding = bridge.takeover("default", endpoint.id, 0);
  const scheduled = Array.from({ length: count }, (_, index) => bridge.scheduleClaim({
    resource: { uri: `memory://payload-fixture/${index}` }, eligible_after: now,
    idempotency_key: `payload-${index}`, note: `fixture note ${index}`,
  }));
  bridge.tick();
  const batch = bridge.listBatches()[0];
  // Isolate hydration from preflight: malformed/stale references must retain
  // the same filtering and fallback even if encountered after acquisition.
  const build = (claimIds: string[]) => (bridge as unknown as {
    buildPayload(batch: WakeBatch, binding: Binding): WakePayload;
  }).buildPayload({ ...batch, claim_ids: claimIds }, binding);
  return { bridge, scheduled, build, now };
}

describe("bounded exact-reference wake payload reads", () => {
  it("preserves batch order, repeated claims, notes and all referenced event reads in two queries", () => {
    const { bridge, scheduled: [first, second, extra], build } = fixture(3);
    bridge.db.exec(`UPDATE claims SET event_ids_json=${sqlJson([first.event.id, extra.event.id])} WHERE id=${sqlValue(first.claim.id)};`);
    const query = vi.spyOn(bridge.db, "query");

    const payload = build([second.claim.id, first.claim.id, second.claim.id]);

    expect(payload.claim_refs.map((ref) => ref.claim_id)).toEqual([second.claim.id, first.claim.id, second.claim.id]);
    expect(payload.claim_refs.map((ref) => ref.event_id)).toEqual([second.event.id, first.event.id, second.event.id]);
    expect(payload.claim_refs.map((ref) => ref.note)).toEqual(["fixture note 1", "fixture note 0", "fixture note 1"]);
    expect(query).toHaveBeenCalledTimes(2);
    const eventRows = query.mock.results[1].value as Array<{ id: string }>;
    expect(eventRows.map((row) => row.id).sort()).toEqual([first.event.id, second.event.id, extra.event.id].sort());
    // Exclude unused metadata/previews before SQLite serializes a batch: their
    // combined size must not consume the subprocess JSON output budget.
    for (const row of eventRows) expect(Object.keys(row).sort()).toEqual(["id", "source", "type"]);
  });

  it("filters missing, foreign and nonbatched claims and keeps missing/statusless/foreign event fallbacks", () => {
    const { bridge, scheduled: [keep, nonbatched, foreignClaim, foreignEvent, statusless, missingEvent], build } = fixture(6);
    bridge.db.transaction([
      `UPDATE claims SET state='pending' WHERE id=${sqlValue(nonbatched.claim.id)};`,
      `UPDATE claims SET instance_id='another-instance' WHERE id=${sqlValue(foreignClaim.claim.id)};`,
      `UPDATE events SET instance_id='another-instance' WHERE id=${sqlValue(foreignEvent.event.id)};`,
      `DELETE FROM event_status WHERE event_id=${sqlValue(statusless.event.id)};`,
      `UPDATE claims SET event_ids_json=${sqlJson(["missing-event", keep.event.id])} WHERE id=${sqlValue(missingEvent.claim.id)};`,
    ]);
    const query = vi.spyOn(bridge.db, "query");

    const payload = build([keep.claim.id, "missing-claim", nonbatched.claim.id, foreignClaim.claim.id,
      foreignEvent.claim.id, statusless.claim.id, missingEvent.claim.id]);

    expect(payload.claim_refs.map((ref) => ref.claim_id)).toEqual([keep.claim.id, foreignEvent.claim.id, statusless.claim.id, missingEvent.claim.id]);
    expect(payload.claim_refs[0]).toMatchObject({ event_id: keep.event.id, source: "self_commitment", type: "attention.schedule" });
    for (const ref of payload.claim_refs.slice(1)) {
      expect(ref).toMatchObject({ event_id: undefined, source: "self_commitment", type: "attention.claim" });
    }
    expect(query).toHaveBeenCalledTimes(2);
  });

  it("resolves the exact event beyond 1000 historical rows without a paged index", () => {
    const { bridge, scheduled: [target], build } = fixture(1);
    bridge.db.transaction([
      `WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM n WHERE value<1001)
       INSERT INTO events(id, instance_id, owner_id, source, type, schema_version, occurred_at, received_at, dedupe_key, resource_json, metadata_json)
       SELECT 'history-'||value, 'payload-read', 'fixture', 'history', 'history', 1, '2020-01-01T00:00:00.000Z',
         '2020-01-01T00:00:00.000Z', 'history-'||value, '{}', '{}' FROM n;`,
      `INSERT INTO event_status(event_id, state, updated_at) SELECT id, 'consumed', received_at FROM events WHERE source='history';`,
    ]);
    const query = vi.spyOn(bridge.db, "query");

    const payload = build([target.claim.id]);

    expect(payload.claim_refs).toHaveLength(1);
    expect(payload.claim_refs[0]).toMatchObject({ event_id: target.event.id, source: "self_commitment", type: "attention.schedule" });
    expect(query).toHaveBeenCalledTimes(2);
  });
});
