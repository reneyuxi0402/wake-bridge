import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import type { AddressInfo } from "node:net";
import { setImmediate } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { WakeBridge } from "../src/core.js";
import { startDaemon } from "../src/daemon.js";
import { sqlJson, sqlValue } from "../src/db.js";
import type { BridgeConfig, PolicyRule, ResourceRef, WakeTransport } from "../src/types.js";

const bridges: WakeBridge[] = [];
const directories: string[] = [];
const past = "2020-01-01T00:00:00.000Z";
const future = "2030-01-01T00:00:00.000Z";

function setup(policies: PolicyRule[] = []) {
  let milliseconds = Date.parse("2026-10-21T12:00:00.000Z");
  const directory = mkdtempSync(join(tmpdir(), "wake-bridge-bounded-"));
  directories.push(directory);
  const config: BridgeConfig = {
    instance_id: "bounded-backlog", owner_id: "owner", timezone: "UTC",
    db_path: join(directory, "bridge.sqlite"),
    policies,
  };
  const calls: string[] = [];
  const transport: WakeTransport = {
    kind: "bounded-test",
    dispatch(context) { calls.push(context.batch.id); return { accepted: true }; },
  };
  const open = () => {
    const bridge = new WakeBridge(config, {
      clock: () => new Date(milliseconds), transports: [transport], autoMockTransport: false,
    });
    bridges.push(bridge);
    return bridge;
  };
  const bridge = open();
  const bind = (channel: string, kind = transport.kind) => {
    const endpoint = bridge.registerEndpoint({
      host_kind: kind, session_ref: `session:${channel}`,
      routes: [{ kind, address: {} }], capabilities: { cold_push: true },
    });
    bridge.takeover(channel, endpoint.id, 0);
  };
  return { bridge, open, bind, calls, advance: (ms: number) => { milliseconds += ms; } };
}

function insertClaims(bridge: WakeBridge, count: number, due = future, withEvents = false): void {
  const resource = "json_object('uri', 'job://bulk/' || n)";
  bridge.db.transaction([
    ...(withEvents ? [
      `WITH RECURSIVE numbers(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM numbers WHERE n<${count})
       INSERT INTO events(id, instance_id, owner_id, source, type, schema_version, occurred_at, received_at, dedupe_key, resource_json, metadata_json)
       SELECT 'evt_bulk_' || n, ${sqlValue(bridge.config.instance_id)}, 'owner', 'bulk', 'job.completed', 1,
         ${sqlValue(past)}, ${sqlValue(past)}, 'bulk:' || n, ${resource}, '{}' FROM numbers;`,
      `INSERT INTO event_status(event_id, state, updated_at)
       SELECT id, 'matched', ${sqlValue(past)} FROM events WHERE source='bulk';`,
    ] : []),
    `WITH RECURSIVE numbers(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM numbers WHERE n<${count})
     INSERT INTO claims(id, instance_id, origin, event_ids_json, resource_json, policy_id, policy_version,
       attention_channel, eligible_after, defer_while_presence, state, reason_code, note, created_at, updated_at)
     SELECT 'ac_bulk_' || printf('%06d', n), ${sqlValue(bridge.config.instance_id)}, 'policy',
       ${withEvents ? "json_array('evt_bulk_' || n)" : "'[]'"}, ${resource},
       ${sqlValue(withEvents ? "grouped" : "default")}, 1, 'default', ${sqlValue(due)}, 0,
       'pending', 'event', printf('%0400d', n), ${sqlValue(past)}, ${sqlValue(past)} FROM numbers;`,
  ]);
}

function insertBatches(bridge: WakeBridge, count: number, options: { waiting?: boolean; channel?: string } = {}): void {
  bridge.db.exec(`WITH RECURSIVE numbers(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM numbers WHERE n<${count})
    INSERT INTO batches(id, instance_id, policy_id, policy_version, attention_channel, claim_ids_json,
      event_ids_json, coalesce_key, state, not_before, deadline, attempt, created_at, updated_at, last_error)
    SELECT 'wb_bulk_' || printf('%06d', n), ${sqlValue(bridge.config.instance_id)}, 'default', 1,
      ${sqlValue(options.channel ?? "default")}, '["missing-historical-claim"]', '[]', 'old:' || n,
      ${sqlValue(options.waiting ? "waiting_for_endpoint" : "pending")},
      ${sqlValue(options.waiting ? past : future)}, ${sqlValue(options.waiting ? past : future)}, 0,
      ${sqlValue(past)}, ${sqlValue(past)}, printf('%0400d', n) FROM numbers;`);
}

function fresh(bridge: WakeBridge, key: string, resource: ResourceRef = { uri: `job://fresh/${key}` }) {
  return bridge.emit("bulk", { type: "job.completed", dedupe_key: key, resource });
}

function cacheFixturePolicyReads(bridge: WakeBridge): void {
  // This case verifies the 1000-item boundary, CAS and restart, not the cost of
  // 2000 identical immutable-policy subprocess reads. All other SQL stays real.
  const sql = "SELECT rule_json FROM policies WHERE id='grouped' AND version=1 LIMIT 1;";
  const policy = bridge.db.query(sql);
  const query = bridge.db.query.bind(bridge.db);
  vi.spyOn(bridge.db, "query").mockImplementation(<T extends Record<string, unknown>>(statement: string): T[] =>
    statement === sql ? policy as T[] : query<T>(statement));
}

function cacheFutureWaiterReads(bridge: WakeBridge, channel: string): () => number {
  // Call only after transport has really moved these fixture rows into the
  // future. They cannot change again in this test, so exact getBatch reads can
  // reuse this real snapshot; candidate selection and live bindings stay real.
  const query = bridge.db.query.bind(bridge.db);
  const rows = query(`SELECT * FROM batches WHERE instance_id=${sqlValue(bridge.config.instance_id)}
    AND attention_channel=${sqlValue(channel)} AND state='waiting_for_endpoint' AND not_before=${sqlValue(future)};`);
  const snapshots = new Map(rows.map((row) => [
    `SELECT * FROM batches WHERE id=${sqlValue(row.id)} AND instance_id=${sqlValue(bridge.config.instance_id)} LIMIT 1;`, [row],
  ]));
  let cachedReads = 0;
  vi.spyOn(bridge.db, "query").mockImplementation(<T extends Record<string, unknown>>(sql: string): T[] => {
    const snapshot = snapshots.get(sql);
    if (snapshot) {
      cachedReads += 1;
      return snapshot as T[];
    }
    return query<T>(sql);
  });
  return () => cachedReads;
}

afterEach(async () => {
  vi.restoreAllMocks();
  for (const bridge of bridges.splice(0)) bridge.close();
  for (const directory of directories.splice(0)) rmSync(directory, { recursive: true, force: true });
  // SQLite subprocesses are synchronous. Give fork RPC acknowledgements an I/O
  // turn between tests instead of starving them until the whole file finishes.
  await setImmediate();
});

describe("bounded scheduler backlog work", () => {
  it("ends a sweep on monotonic elapsed time, yields to I/O, and keeps remaining work durable", async () => {
    const { bridge, bind, calls } = setup();
    bind("default");
    for (let n = 0; n < 3; n++) fresh(bridge, `time-budget-${n}`);
    const batches = bridge.tick().batches_created;
    expect(batches).toHaveLength(3);
    const businessTime = bridge.nowIso();
    let elapsed = 0;
    vi.spyOn(performance, "now").mockImplementation(() => elapsed);
    const transport = bridge.transports.get("bounded-test")!;
    const dispatch = transport.dispatch.bind(transport);
    vi.spyOn(transport, "dispatch").mockImplementation((context) => {
      elapsed += 1000;
      return dispatch(context);
    });
    let ioTurn = false;
    const immediate = setImmediate().then(() => { ioTurn = true; });
    const first = await bridge.dispatchDue();
    expect(first).toHaveLength(1);
    expect(ioTurn, "Budget exhaustion must let pending I/O run, not only resolve a microtask").toBe(true);
    await immediate;
    expect(bridge.nowIso()).toBe(businessTime);
    expect(bridge.listBatches().filter((batch) => batch.state === "pending")).toHaveLength(2);
    expect(await bridge.dispatchDue()).toHaveLength(1);
    expect(await bridge.dispatchDue()).toHaveLength(1);
    expect(new Set(calls)).toEqual(new Set(batches));
    expect(calls).toHaveLength(3);
  });

  it("allows one candidate to progress even when recovery has exhausted the budget", async () => {
    const { bridge, bind, calls } = setup();
    bind("default");
    fresh(bridge, "slow-recovery-1");
    fresh(bridge, "slow-recovery-2");
    bridge.tick();
    let elapsed = 0;
    vi.spyOn(performance, "now").mockImplementation(() => elapsed);
    const recovery = bridge as unknown as { recover(): void };
    const recover = recovery.recover.bind(bridge);
    vi.spyOn(recovery, "recover").mockImplementation(() => {
      const result = recover();
      elapsed += 2000;
      return result;
    });
    expect(await bridge.dispatchDue()).toHaveLength(1);
    expect(await bridge.dispatchDue()).toHaveLength(1);
    expect(new Set(calls).size).toBe(2);
  });

  it("continues below 1000ms and stops at the exact budget boundary", async () => {
    const { bridge, bind, calls } = setup();
    bind("default");
    for (let n = 0; n < 3; n++) fresh(bridge, `budget-boundary-${n}`);
    bridge.tick();
    let elapsed = 0;
    vi.spyOn(performance, "now").mockImplementation(() => elapsed);
    const transport = bridge.transports.get("bounded-test")!;
    const dispatch = transport.dispatch.bind(transport);
    vi.spyOn(transport, "dispatch").mockImplementation((context) => {
      elapsed = calls.length === 0 ? 999 : 1000;
      return dispatch(context);
    });
    expect(await bridge.dispatchDue()).toHaveLength(2);
    expect(calls).toHaveLength(2);
    expect(await bridge.dispatchDue()).toHaveLength(1);
    expect(new Set(calls).size).toBe(3);
  });

  it("serves HTTP between budgeted passes through unbound synchronous work", async () => {
    const { bridge, advance } = setup();
    for (let n = 0; n < 3; n++) fresh(bridge, `unbound-budget-${n}`);
    const batches = bridge.tick().batches_created;
    const daemon = await startDaemon(bridge.config, {
      bridge, host: "127.0.0.1", port: 0, scheduler_interval_ms: 0, unsafe_no_auth: true,
    });
    try {
      let elapsed = 0;
      vi.spyOn(performance, "now").mockImplementation(() => elapsed);
      const getBatch = bridge.getBatch.bind(bridge);
      vi.spyOn(bridge, "getBatch").mockImplementation((id) => {
        const batch = getBatch(id);
        elapsed += 1000;
        return batch;
      });
      const origin = `http://127.0.0.1:${(daemon.address as AddressInfo).port}`;
      const visited: string[] = [];
      for (let n = 0; n < 3; n++) {
        advance(1);
        const health = fetch(`${origin}/health`);
        const results = await bridge.dispatchDue();
        expect(results).toHaveLength(1);
        expect(results[0].status).toBe("waiting_for_endpoint");
        visited.push(results[0].batch_id);
        const response = await health;
        expect(response.status).toBe(200);
        await response.arrayBuffer();
      }
      expect(new Set(visited)).toEqual(new Set(batches));
      expect(bridge.listBatches().every((batch) => batch.attempt === 0)).toBe(true);
    } finally { await daemon.close(); }
  });

  it("finds new due work behind more than 100k future active claims and open batches", async () => {
    const { bridge, bind, calls } = setup();
    insertClaims(bridge, 100_005);
    insertBatches(bridge, 100_005);
    bind("default");
    const emitted = fresh(bridge, "after-100k");
    const query = vi.spyOn(bridge.db, "query");

    const tick = bridge.tick();
    expect(tick.eligible_claims).toEqual([emitted.claim!.id]);
    expect(tick.batches_created).toHaveLength(1);
    expect(await bridge.dispatchDue()).toMatchObject([{ batch_id: tick.batches_created[0], status: "accepted" }]);
    expect(calls).toEqual(tick.batches_created);
    expect(bridge.countClaimsByState()).toMatchObject({ pending: 100_005, batched: 1 });
    expect(query.mock.results.every((result) => result.type !== "return" || result.value.length <= 1000)).toBe(true);
  });

  it("never hydrates huge fields belonging to noncandidate claims, events or batches", async () => {
    const { bridge, bind } = setup();
    insertClaims(bridge, 2);
    insertBatches(bridge, 1);
    const resource = { uri: "job://huge-noncandidate", cursor: "current" };
    const huge = "printf('%0*d', 35*1024*1024, 0)";
    bridge.db.transaction([
      `UPDATE claims SET resource_json=${sqlJson({ ...resource, cursor: "older" })}, note=${huge} WHERE id='ac_bulk_000001';`,
      `INSERT INTO events(id, instance_id, owner_id, source, type, schema_version, occurred_at, received_at, dedupe_key, resource_json, metadata_json)
       VALUES('evt_other_source', ${sqlValue(bridge.config.instance_id)}, 'owner', 'other-source', 'job.completed', 1,
         ${sqlValue(past)}, ${sqlValue(past)}, 'other-source', ${sqlJson(resource)}, json_object('large', ${huge}));`,
      `INSERT INTO event_status VALUES('evt_other_source', 'matched', ${sqlValue(past)});`,
      `UPDATE claims SET resource_json=${sqlJson(resource)}, event_ids_json='["evt_other_source"]' WHERE id='ac_bulk_000002';`,
      `UPDATE batches SET last_error=${huge};`,
    ]);
    bind("default");

    const emitted = fresh(bridge, "huge-fields", resource);
    expect(emitted.claim?.id).not.toMatch(/^ac_bulk_/u);
    expect(bridge.tick().eligible_claims).toEqual([emitted.claim!.id]);
    expect(await bridge.dispatchDue()).toMatchObject([{ status: "accepted" }]);
  });

  it("limits each tick and loses no references across a restart", () => {
    const grouped: PolicyRule = {
      id: "grouped", version: 1, order: 10, match: { source: "bulk" }, delivery: { mode: "immediate" },
      batch: { coalesce_by: "source", window_ms: 60_000, max_events: 1000 },
    };
    const { bridge, open } = setup([grouped]);
    insertClaims(bridge, 1105, past, true);
    cacheFixturePolicyReads(bridge);
    const first = bridge.tick();
    expect(first.eligible_claims).toHaveLength(1000);
    expect(first.batches_created).toHaveLength(1);
    expect(bridge.countClaimsByState()).toMatchObject({ batched: 1000, pending: 105 });
    bridge.close();

    const restarted = open();
    cacheFixturePolicyReads(restarted);
    expect(restarted.tick().eligible_claims).toHaveLength(105);
    expect(restarted.tick().batches_created).toEqual([]);
    expect(restarted.countClaimsByState()).toMatchObject({ batched: 1105, pending: 0, eligible: 0 });
    for (const column of ["claim_ids_json", "event_ids_json"]) {
      expect(restarted.db.query(`SELECT COUNT(*) AS total, COUNT(DISTINCT j.value) AS distinct_refs
        FROM batches b, json_each(b.${column}) j WHERE b.instance_id=${sqlValue(bridge.config.instance_id)};`))
        .toEqual([{ total: 1105, distinct_refs: 1105 }]);
    }
  });

  it("does not duplicate or drop references when another tick wins the whole snapshot", () => {
    const grouped: PolicyRule = {
      id: "grouped", version: 1, order: 10, match: { source: "bulk" }, delivery: { mode: "immediate" },
      batch: { coalesce_by: "source", window_ms: 60_000, max_events: 1000 },
    };
    const { bridge, open } = setup([grouped]);
    insertClaims(bridge, 25, past, true);
    const peer = open();
    const transaction = bridge.db.transaction.bind(bridge.db);
    let overlapped = false;
    vi.spyOn(bridge.db, "transaction").mockImplementation((statements) => {
      if (!overlapped && statements.some((sql) => sql.includes("INSERT INTO batches"))) {
        overlapped = true;
        expect(peer.tick().batches_created).toHaveLength(1);
      }
      transaction(statements);
    });

    expect(bridge.tick().batches_created).toEqual([]);
    expect(overlapped).toBe(true);
    expect(bridge.countClaimsByState()).toMatchObject({ batched: 25, pending: 0, eligible: 0 });
    expect(bridge.db.query(`SELECT COUNT(*) AS total, COUNT(DISTINCT j.value) AS distinct_refs
      FROM batches b, json_each(b.event_ids_json) j;`)).toEqual([{ total: 25, distinct_refs: 25 }]);
  });

  it("prioritizes a bound channel over an old unbound waiting page", async () => {
    // Isolate the count/order limit from the independently tested time budget.
    vi.spyOn(performance, "now").mockReturnValue(0);
    const { bridge, bind, calls } = setup();
    insertBatches(bridge, 1001, { waiting: true, channel: "unbound" });
    bind("default");
    fresh(bridge, "bound-behind-waiters");
    const [batchId] = bridge.tick().batches_created;
    const transport = bridge.transports.get("bounded-test")!;
    const dispatch = transport.dispatch.bind(transport);
    let cachedFutureReads = () => 0;
    vi.spyOn(transport, "dispatch").mockImplementation((context) => {
      // Once ordering is established, avoid spending this test on another 999
      // identical missing endpoints. Already selected IDs still need rechecks.
      bridge.db.exec(`UPDATE batches SET not_before=${sqlValue(future)} WHERE attention_channel='unbound';`);
      cachedFutureReads = cacheFutureWaiterReads(bridge, "unbound");
      return dispatch(context);
    });

    expect(await bridge.dispatchDue()).toMatchObject([{ batch_id: batchId, status: "accepted" }]);
    expect(calls).toEqual([batchId]);
    expect(cachedFutureReads(), "Future-row snapshot misses: check the fixture's exact getBatch SQL against the current query").toBe(999);
  });

  it("rotates more than 1000 no-compatible waiters so later bound work is not starved", async () => {
    vi.spyOn(performance, "now").mockReturnValue(0);
    const { bridge, bind, calls, advance } = setup();
    bind("unsupported", "unregistered-kind");
    bind("default");
    insertBatches(bridge, 1001, { waiting: true, channel: "unsupported" });
    fresh(bridge, "compatible-behind-waiters");
    const [batchId] = bridge.tick().batches_created;
    const query = bridge.db.query.bind(bridge.db);
    const transport = bridge.transports.get("bounded-test")!;
    const dispatch = transport.dispatch.bind(transport);
    let cachedFutureReads = () => 0;
    vi.spyOn(transport, "dispatch").mockImplementation((context) => {
      // Fairness is established when this later batch reaches transport.
      // Let the rest of the selected identical waiters take the cheap due gate.
      bridge.db.exec(`UPDATE batches SET not_before=${sqlValue(future)} WHERE attention_channel='unsupported';`);
      cachedFutureReads = cacheFutureWaiterReads(bridge, "unsupported");
      return dispatch(context);
    });
    advance(1);

    // Only this first sweep uses a real, static preflight snapshot: these rows
    // have no claims and their binding cannot change. Candidate ordering and
    // all 1000 timestamp CAS writes still execute against the real database.
    // Restore normal reads before the second sweep; this is not a 1000-item
    // end-to-end subprocess performance benchmark.
    const instance = sqlValue(bridge.config.instance_id);
    const staticReads = new Map<string, Record<string, unknown>[]>();
    for (const row of query("SELECT * FROM batches WHERE attention_channel='unsupported';")) {
      staticReads.set(`SELECT * FROM batches WHERE id=${sqlValue(row.id)} AND instance_id=${instance} LIMIT 1;`, [row]);
    }
    for (const sql of [
      `SELECT c.* FROM json_each(${sqlJson(["missing-historical-claim"])}) j JOIN claims c ON c.id=j.value
      WHERE c.instance_id=${instance};`,
      `SELECT b.*, e.* FROM bindings b JOIN endpoints e ON e.id=b.endpoint_id WHERE b.instance_id=${instance} AND b.attention_channel='unsupported' AND e.lease_expires_at>${sqlValue(bridge.nowIso())} LIMIT 1;`,
    ]) staticReads.set(sql, query(sql));
    const snapshotReads = vi.spyOn(bridge.db, "query").mockImplementation(<T extends Record<string, unknown>>(sql: string): T[] =>
      staticReads.has(sql) ? staticReads.get(sql) as T[] : query<T>(sql));
    const first = await bridge.dispatchDue();
    snapshotReads.mockRestore();
    expect(first).toHaveLength(1000);
    expect(calls).toEqual([]);
    expect(bridge.db.query(`SELECT COUNT(*) AS count FROM batches
      WHERE attention_channel='unsupported' AND updated_at=${sqlValue(bridge.nowIso())};`)).toEqual([{ count: 1000 }]);
    expect(bridge.db.query("SELECT COUNT(*) AS count FROM batch_transitions WHERE batch_id LIKE 'wb_bulk_%';"))
      .toEqual([{ count: 0 }]);
    expect(bridge.getBatch("wb_bulk_000001")).toMatchObject({
      state: "waiting_for_endpoint", attempt: 0, not_before: past,
    });
    advance(1);
    const second = await bridge.dispatchDue();
    expect(second.length).toBeLessThanOrEqual(1000);
    expect(calls).toEqual([batchId]);
    expect(cachedFutureReads(), "Future-row snapshot misses: check the fixture's exact getBatch SQL against the current query").toBe(998);
    expect(bridge.db.query("SELECT COUNT(*) AS count FROM receipts r WHERE r.batch_id LIKE 'wb_bulk_%';"))
      .toEqual([{ count: 0 }]);
  });

  it("preserves resource JSON semantics and merges equivalent policy versions without hydrating other claims", () => {
    const policies: PolicyRule[] = ["initial", "followup"].map((type) => ({
      id: type, version: 1, order: 10, match: { source: "bulk", type }, delivery: { mode: "immediate" },
    }));
    const { bridge } = setup(policies);
    const resource: ResourceRef = { uri: "job://canonical", extra: { first: [1, true, null, {}, []], second: "text" } };
    const seed = bridge.emit("bulk", { type: "initial", dedupe_key: "canonical:first", resource });
    const reordered = bridge.emit("bulk", {
      type: "followup", dedupe_key: "canonical:reordered",
      resource: { extra: { second: "text", first: [1, true, null, {}, []] }, uri: resource.uri },
    });
    expect(reordered.claim?.id).toBe(seed.claim!.id);

    const variants = [
      { first: ["1", true, null, {}, []], second: "text" },
      { first: [1, 1, null, {}, []], second: "text" },
      { first: [1, true, {}, []], second: "text" },
      { first: [true, 1, null, {}, []], second: "text" },
      { first: [1, true, null, [], {}], second: "text" },
      { first: [1, true, null, {}, []], second: null },
      { first: [1, true, null, {}, []] },
    ];
    for (const [index, extra] of variants.entries()) {
      const emitted = bridge.emit("bulk", {
        type: "followup", dedupe_key: `canonical:variant:${index}`, resource: { uri: resource.uri, extra },
      });
      expect(emitted.claim?.id).not.toBe(seed.claim!.id);
    }
    expect(bridge.getClaim(seed.claim!.id)?.event_ids).toEqual([seed.event.id, reordered.event.id]);
  });

  it("accepts optional undefined resource fields without broadening the old merge comparison", () => {
    const { bridge } = setup();
    const resource = { uri: "job://optional-undefined", cursor: undefined };
    const first = fresh(bridge, "undefined-first", resource);
    const second = fresh(bridge, "undefined-second", resource);
    expect(second.claim?.id).not.toBe(first.claim!.id);
    const withoutCursor = fresh(bridge, "without-cursor", { uri: resource.uri });
    expect(withoutCursor.claim?.id).toBe(first.claim!.id);
  });
});
