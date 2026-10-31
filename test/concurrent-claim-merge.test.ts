import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { WakeBridge } from "../src/core.js";
import type { EmitResult } from "../src/core.js";
import { sqlValue } from "../src/db.js";
import type { BridgeConfig, PolicyRule, WakeEventInput } from "../src/types.js";

const bridges: WakeBridge[] = [];
const interleaves: Array<() => void> = [];
const now = "2026-10-21T12:00:00.000Z";

function pair(policies?: PolicyRule[]) {
  let milliseconds = new Date(now).getTime();
  const config: BridgeConfig = {
    instance_id: "concurrent-claim-merge",
    owner_id: "owner",
    db_path: join(mkdtempSync(join(tmpdir(), "wake-bridge-merge-")), "bridge.sqlite"),
    timezone: "UTC",
    policies,
  };
  const first = new WakeBridge(config, { clock: () => new Date(milliseconds) });
  const second = new WakeBridge(config, { clock: () => new Date(milliseconds) });
  bridges.push(first, second);
  return { first, second, advance: (ms: number) => { milliseconds += ms; } };
}

function input(key: string, extra: Partial<WakeEventInput> = {}): WakeEventInput {
  return {
    type: "job.completed",
    dedupe_key: key,
    coalesce_key: "same-resource",
    resource: { uri: "job://concurrent/same-resource" },
    ...extra,
  };
}

// Both instances use real SQLite. Pause immediately before BEGIN IMMEDIATE:
// the outer operation has read its candidate, then the other instance commits
// first. Cover both transaction()'s exec path and transactions returning rows
// through query(), without intercepting candidate/preflight SELECTs.
function beforeTransaction(bridge: WakeBridge, interleave: () => void, matches = (_sql: string) => true): void {
  const run = vi.fn(interleave);
  interleaves.push(run);
  const before = (sql: string) => {
    if (!run.mock.calls.length && /^\s*BEGIN IMMEDIATE;/u.test(sql) && matches(sql)) run();
  };
  const exec = bridge.db.exec.bind(bridge.db);
  const query = bridge.db.query.bind(bridge.db);
  vi.spyOn(bridge.db, "exec").mockImplementation((sql) => {
    before(sql);
    exec(sql);
  });
  vi.spyOn(bridge.db, "query").mockImplementation(<T extends Record<string, unknown>>(sql: string): T[] => {
    before(sql);
    return query<T>(sql);
  });
}

function beforeBatchTransaction(bridge: WakeBridge, interleave: () => void): void {
  beforeTransaction(bridge, interleave, (sql) => /INSERT INTO batches|UPDATE batches SET claim_ids_json/u.test(sql));
}

function groupedPolicy(maxEvents = 20): PolicyRule[] {
  return [{
    id: "grouped",
    version: 1,
    order: 100,
    match: { source: "jobs" },
    delivery: { mode: "immediate" },
    batch: { coalesce_by: "source", window_ms: 60_000, max_events: maxEvents },
  }];
}

function claimTransitions(bridge: WakeBridge, claimId: string) {
  return bridge.db.query(`SELECT from_state, to_state, reason FROM claim_transitions
    WHERE claim_id=${sqlValue(claimId)} ORDER BY seq;`);
}

afterEach(() => {
  for (const run of interleaves.splice(0)) {
    expect.soft(run, "the competing SQLite operation must run exactly once before the target transaction").toHaveBeenCalledTimes(1);
  }
  vi.restoreAllMocks();
  for (const bridge of bridges.splice(0)) bridge.close();
});

describe("current event membership during claim lifecycle transactions", () => {
  it.each(["consume", "dismiss"] as const)("%s finalizes an event merged after the action's pre-read", (action) => {
    const { first, second } = pair();
    const seed = first.emit("jobs", input("seed"));
    let concurrent: EmitResult | undefined;
    beforeTransaction(first, () => { concurrent = second.emit("jobs", input("concurrent")); });

    const finalized = action === "consume" ? first.consumeClaim(seed.claim!.id) : first.dismissClaim(seed.claim!.id);
    expect(finalized.event_ids).toEqual([seed.event.id, concurrent!.event.id]);
    expect(first.getEvent(concurrent!.event.id)?.state).toBe("consumed");
    expect(first.db.query(`SELECT from_state, to_state, reason FROM event_transitions
      WHERE event_id=${sqlValue(concurrent!.event.id)} ORDER BY seq DESC LIMIT 1;`))
      .toEqual([{ from_state: "matched", to_state: "consumed", reason: action }]);
  });

  it("expires every current reference when an event merges before the expiry transaction", () => {
    const { first, second, advance } = pair([{
      id: "expires", version: 1, order: 100, match: { source: "jobs" },
      delivery: { mode: "immediate" }, expires_after_ms: 1_000,
    }]);
    const seed = first.emit("jobs", input("seed"));
    advance(1_000);
    let concurrent: EmitResult | undefined;
    beforeTransaction(first, () => { concurrent = second.emit("jobs", input("concurrent")); });

    expect(first.tick().expired_claims).toEqual([seed.claim!.id]);
    expect(first.getClaim(seed.claim!.id)?.event_ids).toEqual([seed.event.id, concurrent!.event.id]);
    expect(first.listEvents().every((event) => event.state === "expired")).toBe(true);
    expect(first.listBatches()).toEqual([]);
  });

  it("does not expire events when another instance consumes the claim before expiry commits", () => {
    const { first, second, advance } = pair([{
      id: "expires", version: 1, order: 100, match: { source: "jobs" },
      delivery: { mode: "immediate" }, expires_after_ms: 1_000,
    }]);
    const seed = first.emit("jobs", input("seed"));
    advance(1_000);
    beforeTransaction(first, () => { second.consumeClaim(seed.claim!.id); });

    expect(first.tick().expired_claims).toEqual([]);
    expect(first.getClaim(seed.claim!.id)?.state).toBe("consumed");
    expect(first.getEvent(seed.event.id)?.state).toBe("consumed");
    expect(first.db.query(`SELECT to_state FROM event_transitions WHERE event_id=${sqlValue(seed.event.id)} AND to_state='expired';`))
      .toEqual([]);
  });

  it("does not finalize events when the claim action loses its state comparison", () => {
    const { first, second } = pair();
    const seed = first.emit("jobs", input("seed"));
    beforeTransaction(first, () => { second.tick(); });

    expect(first.consumeClaim(seed.claim!.id).state).toBe("batched");
    expect(first.getEvent(seed.event.id)?.state).toBe("batched");
    expect(first.db.query(`SELECT to_state FROM event_transitions WHERE event_id=${sqlValue(seed.event.id)} AND to_state='consumed';`))
      .toEqual([]);
  });

  it.each(["batched", "already_snoozed"] as const)("a stale %s consume emits no receipt and leaves other claims attached", (state) => {
    const { first, second } = pair(groupedPolicy());
    const seed = first.emit("jobs", input("seed"));
    const peer = first.emit("jobs", input("peer", { resource: { uri: "job://peer" } }));
    first.tick();
    const batch = first.listBatches()[0];
    if (state === "already_snoozed") first.snoozeClaim(seed.claim!.id, "2026-10-21T12:00:10.000Z");
    let concurrent: EmitResult | undefined;
    beforeTransaction(first, () => {
      second.snoozeClaim(seed.claim!.id, "2026-10-21T12:00:20.000Z");
      concurrent = second.emit("jobs", input("concurrent", { resource: { uri: "job://concurrent" } }));
      second.tick();
    });

    first.consumeClaim(seed.claim!.id);
    expect.soft(first.listReceipts(batch.id).filter((receipt) => receipt.stage === "agent_consumed")).toEqual([]);
    expect.soft(first.getClaim(seed.claim!.id)).toMatchObject({ state: "pending", snooze_until: "2026-10-21T12:00:20.000Z" });
    expect(first.getBatch(batch.id)).toMatchObject({
      state: "pending",
      claim_ids: [peer.claim!.id, concurrent!.claim!.id],
      event_ids: [peer.event.id, concurrent!.event.id],
    });
  });

  it("detaches from current batch arrays without dropping a concurrent append", () => {
    const { first, second } = pair(groupedPolicy());
    const seed = first.emit("jobs", input("seed"));
    const peer = first.emit("jobs", input("peer", { resource: { uri: "job://peer" } }));
    first.tick();
    const batch = first.listBatches()[0];
    let concurrent: EmitResult | undefined;
    beforeTransaction(first, () => {
      concurrent = second.emit("jobs", input("concurrent", { resource: { uri: "job://concurrent" } }));
      second.tick();
    });

    first.consumeClaim(seed.claim!.id);
    expect(first.getBatch(batch.id)).toMatchObject({
      state: "pending",
      claim_ids: [peer.claim!.id, concurrent!.claim!.id],
      event_ids: [peer.event.id, concurrent!.event.id],
    });
    expect(first.listReceipts(batch.id).filter((receipt) => receipt.stage === "agent_consumed"))
      .toMatchObject([{ claim_id: seed.claim!.id }]);
    expect(first.getEvent(seed.event.id)?.state).toBe("consumed");
    expect(first.getEvent(concurrent!.event.id)?.state).toBe("batched");
  });

  it("labels a batch emptied by snooze separately from finalized claims", () => {
    const { first } = pair();
    const seed = first.emit("jobs", input("seed"));
    first.tick();
    const batch = first.listBatches()[0];

    first.snoozeClaim(seed.claim!.id, "2026-10-21T12:00:20.000Z");
    expect(first.getBatch(batch.id)).toMatchObject({
      state: "cancelled", last_error: "all_claims_snoozed", claim_ids: [], event_ids: [],
    });
    expect(first.db.query(`SELECT from_state, to_state, reason FROM batch_transitions WHERE batch_id=${sqlValue(batch.id)} ORDER BY seq DESC LIMIT 1;`))
      .toEqual([{ from_state: "pending", to_state: "cancelled", reason: "all_claims_snoozed" }]);
    expect(first.listReceipts(batch.id)).toEqual([]);
  });

  it("freezes current claim references when an emit commits before the new batch transaction", () => {
    const { first, second } = pair();
    const seed = first.emit("jobs", input("seed"));
    let concurrent: EmitResult | undefined;
    beforeBatchTransaction(first, () => { concurrent = second.emit("jobs", input("concurrent")); });

    const result = first.tick();
    expect(result.batches_created).toHaveLength(1);
    expect(first.listBatches()).toMatchObject([{
      claim_ids: [seed.claim!.id], event_ids: [seed.event.id, concurrent!.event.id],
    }]);
    expect(first.listEvents().every((event) => event.state === "batched")).toBe(true);
  });

  it.each(["consume", "other_tick"] as const)("creates no empty or duplicate batch when %s wins the candidate", (winner) => {
    const { first, second } = pair();
    const seed = first.emit("jobs", input("seed"));
    beforeBatchTransaction(first, () => {
      if (winner === "consume") second.consumeClaim(seed.claim!.id);
      else second.tick();
    });

    const result = first.tick();
    expect(result.batches_created).toEqual([]);
    expect(result.batches_updated).toEqual([]);
    expect(first.listBatches()).toHaveLength(winner === "consume" ? 0 : 1);
    expect(first.db.query("SELECT COUNT(*) AS count FROM batch_transitions WHERE reason='freeze';"))
      .toEqual([{ count: winner === "consume" ? 0 : 1 }]);
    expect(first.getEvent(seed.event.id)?.state).toBe(winner === "consume" ? "consumed" : "batched");
  });

  it("appends current claim event references to an existing batch", () => {
    const { first, second } = pair(groupedPolicy());
    const earlier = first.emit("jobs", input("earlier", { resource: { uri: "job://earlier" } }));
    first.tick();
    const seed = first.emit("jobs", input("seed"));
    let concurrent: EmitResult | undefined;
    beforeBatchTransaction(first, () => { concurrent = second.emit("jobs", input("concurrent")); });

    const result = first.tick();
    expect(result.batches_updated).toHaveLength(1);
    expect(first.listBatches()).toMatchObject([{
      claim_ids: [earlier.claim!.id, seed.claim!.id],
      event_ids: [earlier.event.id, seed.event.id, concurrent!.event.id],
    }]);
    expect(first.listEvents().every((event) => event.state === "batched")).toBe(true);
  });

  it("preserves references another scheduler appended to the destination batch", () => {
    const { first, second } = pair(groupedPolicy());
    const earlier = first.emit("jobs", input("earlier", { resource: { uri: "job://earlier" } }));
    first.tick();
    const seed = first.emit("jobs", input("seed"));
    let concurrent: EmitResult | undefined;
    beforeBatchTransaction(first, () => {
      concurrent = second.emit("jobs", input("other-claim", { resource: { uri: "job://other-claim" } }));
      const listClaims = second.listClaims.bind(second);
      // This worker's eligible snapshot predates seed becoming eligible.
      vi.spyOn(second, "listClaims").mockImplementation((filters = {}) => {
        const claims = listClaims(filters);
        return filters.state === "eligible" ? claims.filter((claim) => claim.id === concurrent!.claim!.id) : claims;
      });
      second.tick();
    });

    first.tick();
    expect(first.listBatches()).toMatchObject([{
      claim_ids: [earlier.claim!.id, concurrent!.claim!.id, seed.claim!.id],
      event_ids: [earlier.event.id, concurrent!.event.id, seed.event.id],
    }]);
    expect(first.listEvents().every((event) => event.state === "batched")).toBe(true);
  });

  it.each(["closed", "deadline", "capacity"] as const)("leaves a failed %s append eligible for the next tick", (condition) => {
    const { first, second } = pair(groupedPolicy(condition === "capacity" ? 2 : 20));
    const earlier = first.emit("jobs", input("earlier", { resource: { uri: "job://earlier" } }));
    first.tick();
    const batch = first.listBatches()[0];
    const seed = first.emit("jobs", input("seed"));
    beforeBatchTransaction(first, () => {
      if (condition === "closed") second.db.exec(`UPDATE batches SET state='dispatched' WHERE id=${sqlValue(batch.id)};`);
      else if (condition === "deadline") second.db.exec(`UPDATE batches SET deadline=${sqlValue(now)} WHERE id=${sqlValue(batch.id)};`);
      else {
        const extra = second.emit("jobs", input("capacity", { resource: { uri: "job://capacity" } }));
        // The other scheduler's eligible snapshot predates this worker's seed.
        const listClaims = second.listClaims.bind(second);
        vi.spyOn(second, "listClaims").mockImplementation((filters = {}) => {
          const claims = listClaims(filters);
          return filters.state === "eligible" ? claims.filter((claim) => claim.id === extra.claim!.id) : claims;
        });
        second.tick();
      }
    });

    const result = first.tick();
    expect(result.batches_updated).toEqual([]);
    expect(first.getClaim(seed.claim!.id)?.state).toBe("eligible");
    expect(first.getEvent(seed.event.id)?.state).toBe("matched");
    expect(first.getBatch(batch.id)?.claim_ids).not.toContain(seed.claim!.id);
    expect(claimTransitions(first, seed.claim!.id).some((row) => row.to_state === "batched")).toBe(false);

    first.tick();
    expect(first.getClaim(seed.claim!.id)?.state).toBe("batched");
    expect(first.getEvent(seed.event.id)?.state).toBe("batched");
    expect(first.listBatches()).toHaveLength(2);
    expect(first.getBatch(batch.id)?.claim_ids).toContain(earlier.claim!.id);
  });
});

describe("concurrent attention claim merges", () => {
  it("appends both concurrent events to the current claim and consumes every reference", () => {
    const { first, second } = pair();
    const seed = first.emit("jobs", input("seed"));
    let concurrent: EmitResult | undefined;
    beforeTransaction(first, () => { concurrent = second.emit("jobs", input("concurrent")); });

    const outer = first.emit("jobs", input("outer"));
    const eventIds = [seed.event.id, concurrent!.event.id, outer.event.id];
    expect(outer.duplicate).toBe(false);
    expect(concurrent!.duplicate).toBe(false);
    expect(first.listClaims()).toHaveLength(1);
    expect(outer.claim?.id).toBe(seed.claim!.id);
    expect(first.getClaim(seed.claim!.id)?.event_ids).toEqual(eventIds);
    expect(outer.claim?.event_ids).toEqual(eventIds);
    expect(claimTransitions(first, seed.claim!.id).filter((row) => row.reason === "merge_attention_duplicate"))
      .toHaveLength(2);

    second.consumeClaim(seed.claim!.id, { handled: true });
    expect(first.listEvents()).toHaveLength(3);
    for (const eventId of eventIds) expect(first.getEvent(eventId)?.state).toBe("consumed");
  });

  it.each(["consume", "dismiss"] as const)("creates an atomic fallback when the candidate is finalized by %s", (action) => {
    const policies: PolicyRule[] = ["initial", "followup"].map((type) => ({
      id: type,
      version: 1,
      order: 100,
      match: { source: "jobs", type },
      delivery: { mode: "immediate", foreground_presence_policy: "bypass" },
      expires_after_ms: 60_000,
      reason_code: `${type}_reason`,
    }));
    const { first, second } = pair(policies);
    const seed = first.emit("jobs", input("seed", { type: "initial" }));
    beforeTransaction(first, () => {
      if (action === "consume") second.consumeClaim(seed.claim!.id);
      else second.dismissClaim(seed.claim!.id, "handled_elsewhere");
    });

    const outer = first.emit("jobs", input("outer", { type: "followup" }));
    expect(outer.duplicate).toBe(false);
    expect(first.listClaims()).toHaveLength(2);
    expect(outer.claim?.id).not.toBe(seed.claim!.id);
    expect(outer.claim).toMatchObject({
      event_ids: [outer.event.id],
      state: "pending",
      origin: "policy",
      policy_id: "followup",
      policy_version: 1,
      reason_code: "followup_reason",
      resource: input("unused").resource,
      attention_channel: "default",
      eligible_after: now,
      expires_at: "2026-10-21T12:01:00.000Z",
      defer_while_presence: false,
    });
    expect(first.getEvent(outer.event.id)?.state).toBe("matched");
    expect(first.getClaim(seed.claim!.id)).toMatchObject({
      state: action === "consume" ? "consumed" : "dismissed",
      event_ids: [seed.event.id],
    });
    expect(claimTransitions(first, seed.claim!.id).some((row) => row.reason === "merge_attention_duplicate"))
      .toBe(false);
    expect(claimTransitions(first, outer.claim!.id))
      .toEqual([{ from_state: null, to_state: "pending", reason: "policy" }]);

    const replay = second.emit("jobs", input("outer", { type: "followup" }));
    expect(replay.duplicate).toBe(true);
    expect(replay.claim?.id).toBe(outer.claim!.id);
    second.consumeClaim(outer.claim!.id);
    expect(first.listEvents().every((event) => event.state === "consumed")).toBe(true);
  });

  it("creates a fresh claim when tick freezes the candidate before the emit transaction", () => {
    const { first, second } = pair();
    const seed = first.emit("jobs", input("seed"));
    beforeTransaction(first, () => { second.tick(); });

    const outer = first.emit("jobs", input("outer"));
    expect(first.getClaim(seed.claim!.id)?.state).toBe("batched");
    expect(outer.claim?.id).not.toBe(seed.claim!.id);
    expect(outer.claim).toMatchObject({ state: "pending", event_ids: [outer.event.id] });
    expect(first.listClaims()).toHaveLength(2);
    expect(first.listBatches()).toMatchObject([{ claim_ids: [seed.claim!.id], event_ids: [seed.event.id] }]);

    first.tick();
    const batches = first.listBatches();
    expect(batches).toHaveLength(2);
    expect(batches.flatMap((batch) => batch.event_ids).sort()).toEqual([seed.event.id, outer.event.id].sort());
    for (const claim of first.listClaims()) second.consumeClaim(claim.id);
    expect(first.listEvents().every((event) => event.state === "consumed")).toBe(true);
  });

  it.each(["eligible", "deferred"] as const)("keeps every event if pending becomes %s before commit", (state) => {
    const { first, second } = pair();
    const seed = first.emit("jobs", input("seed"));
    beforeTransaction(first, () => {
      // Model the committed gate transaction, before tick's separate batch freeze.
      second.db.transaction([
        `UPDATE claims SET state=${sqlValue(state)} WHERE id=${sqlValue(seed.claim!.id)};`,
        `INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at)
         VALUES(${sqlValue(seed.claim!.id)}, 'pending', ${sqlValue(state)}, 'interleaved_gate', ${sqlValue(now)});`,
      ]);
      second.emit("jobs", input("concurrent"));
    });

    const outer = first.emit("jobs", input("outer"));
    const eventIds = first.listEvents().map((event) => event.id).sort();
    expect(first.listClaims()).toHaveLength(1);
    expect(outer.claim?.id).toBe(seed.claim!.id);
    expect(outer.claim?.state).toBe(state);
    expect(outer.claim?.event_ids.slice().sort()).toEqual(eventIds);
    expect(claimTransitions(first, seed.claim!.id).at(-1))
      .toEqual({ from_state: state, to_state: state, reason: "merge_attention_duplicate" });
    second.consumeClaim(seed.claim!.id);
    expect(first.listEvents().every((event) => event.state === "consumed")).toBe(true);
  });

  it.each(["dedupe", "idempotency"] as const)("preserves %s idempotence when another instance wins the event insert", (kind) => {
    const { first, second } = pair();
    const seed = first.emit("jobs", input("seed"));
    const outerInput = input("outer", kind === "idempotency" ? { idempotency_key: "shared-key" } : {});
    const concurrentInput = kind === "idempotency"
      ? input("winner-different-dedupe", { idempotency_key: "shared-key" })
      : outerInput;
    let concurrent: EmitResult | undefined;
    beforeTransaction(first, () => { concurrent = second.emit("jobs", concurrentInput); });

    const outer = first.emit("jobs", outerInput);
    expect(outer.duplicate).toBe(true);
    expect(concurrent!.duplicate).toBe(false);
    expect(outer.event.id).toBe(concurrent!.event.id);
    expect(outer.event.dedupe_key).toBe(concurrentInput.dedupe_key);
    expect(outer.claim?.id).toBe(seed.claim!.id);
    expect(outer.claim?.event_ids).toEqual([seed.event.id, concurrent!.event.id]);
    expect(first.listEvents()).toHaveLength(2);
    expect(first.listClaims()).toHaveLength(1);
    expect(claimTransitions(first, seed.claim!.id).filter((row) => row.reason === "merge_attention_duplicate"))
      .toHaveLength(1);
    if (kind === "idempotency") {
      expect(first.db.query("SELECT COUNT(*) AS count FROM idempotency_keys WHERE scope='event:jobs';"))
        .toEqual([{ count: 1 }]);
    }
  });
});
