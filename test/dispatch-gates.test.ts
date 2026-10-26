import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { WakeBridge } from "../src/core.js";
import { sqlJson, sqlValue } from "../src/db.js";
import type { BridgeConfig, PolicyRule, TransportContext, TransportResult, WakeTransport } from "../src/types.js";

class TestClock {
  constructor(private value = new Date("2026-10-21T12:00:00.000Z")) {}
  now = () => new Date(this.value.getTime());
  advance(ms: number): void { this.value = new Date(this.value.getTime() + ms); }
  set(iso: string): void { this.value = new Date(iso); }
}

const bridges: WakeBridge[] = [];
const quietHours = { timezone: "UTC", windows: [{ start: "22:00", end: "08:00" }] };

function policy(extra: Partial<PolicyRule> = {}): PolicyRule {
  return {
    id: "default", version: 1, match: {}, delivery: { mode: "immediate" },
    batch: { coalesce_by: "coalesce_key", window_ms: 0 },
    ...extra,
  };
}

function setup(options: {
  config?: Partial<BridgeConfig>;
  clock?: TestClock;
  dispatch?: (context: TransportContext) => TransportResult | Promise<TransportResult>;
} = {}) {
  const clock = options.clock ?? new TestClock();
  const calls: TransportContext[] = [];
  const transport: WakeTransport = {
    kind: "dispatch-gate-test",
    dispatch(context) {
      calls.push(context);
      return options.dispatch?.(context) ?? { accepted: true, transport_kind: this.kind };
    },
  };
  const config: BridgeConfig = {
    instance_id: "dispatch-gates", owner_id: "owner", timezone: "UTC",
    db_path: join(mkdtempSync(join(tmpdir(), "wake-bridge-gates-")), "bridge.sqlite"),
    endpoint_lease_ms: 24 * 60 * 60_000,
    dispatch_lease_ms: 30_000, retry_delay_ms: 1_000,
    default_policy: policy(),
    ...options.config,
  };
  const bridge = new WakeBridge(config, { clock: clock.now, transports: [transport], autoMockTransport: false });
  bridges.push(bridge);
  const bind = (channel: string, leaseMs?: number) => {
    const endpoint = bridge.registerEndpoint({
      host_kind: transport.kind, session_ref: `session:${channel}`, lease_ms: leaseMs,
      capabilities: { cold_push: true, foreground_presence_observable: true },
      routes: [{ kind: transport.kind, address: {} }],
    });
    const binding = bridge.takeover(channel, endpoint.id, 0);
    return { endpoint, binding };
  };
  const { endpoint, binding } = bind("default");
  const presence = (ttlMs = 10_000) => bridge.renewPresence({
    attention_channel: "default", endpoint_id: endpoint.id, generation: binding.generation,
    lease_token: endpoint.lease_token, observed_by: "test-host", ttl_ms: ttlMs,
  });
  const emit = (key: string, channel = "default") => bridge.emit("jobs", {
    type: "job.completed", dedupe_key: key, coalesce_key: "shared-batch",
    attention_channel_hint: channel, resource: { uri: `job://dispatch-gates/${key}` },
  });
  return { bridge, clock, calls, config, transport, endpoint, binding, bind, presence, emit };
}

function batchTransitions(bridge: WakeBridge, batchId: string) {
  return bridge.db.query(`SELECT from_state, to_state, reason FROM batch_transitions
    WHERE batch_id=${sqlValue(batchId)} ORDER BY seq;`);
}

function reopen(fixture: ReturnType<typeof setup>) {
  const other = new WakeBridge(fixture.config, {
    clock: fixture.clock.now, transports: [fixture.transport], autoMockTransport: false, recoverDispatchLeases: false,
  });
  bridges.push(other);
  return other;
}

function beforeGateTransaction(bridge: WakeBridge, interleave: () => void): () => void {
  const transaction = bridge.db.transaction.bind(bridge.db);
  let ran = false;
  vi.spyOn(bridge.db, "transaction").mockImplementation((statements) => {
    if (!ran && statements.some((statement) => statement.includes("UPDATE batches SET claim_ids_json=") && statement.includes("AND attempt="))) {
      ran = true;
      interleave();
    }
    transaction(statements);
  });
  return () => expect(ran).toBe(true);
}

function beforeAcquire(bridge: WakeBridge, interleave: () => void): () => void {
  const query = bridge.db.query.bind(bridge.db);
  let ran = false;
  vi.spyOn(bridge.db, "query").mockImplementation(<T extends Record<string, unknown>>(sql: string): T[] => {
    if (!ran && sql.includes("BEGIN IMMEDIATE;") && sql.includes("SET state='dispatching'")) {
      ran = true;
      interleave();
    }
    return query<T>(sql);
  });
  return () => expect(ran).toBe(true);
}

afterEach(() => {
  vi.restoreAllMocks();
  for (const bridge of bridges.splice(0)) bridge.close();
});

describe("dispatch-time expiry, quiet hours and presence gates", () => {
  it("expires a one-second claim frozen in a two-second batch without creating an attempt", async () => {
    const { bridge, clock, calls, emit } = setup({ config: {
      default_policy: policy({ expires_after_ms: 1_000, batch: { window_ms: 2_000 } }),
    } });
    const emitted = emit("expires-before-batch-window");
    bridge.tick();
    const batch = bridge.listBatches()[0];
    clock.advance(2_000);

    await bridge.dispatchDue();

    expect(calls).toEqual([]);
    expect(bridge.getClaim(emitted.claim!.id)?.state).toBe("expired");
    expect(bridge.getEvent(emitted.event.id)?.state).toBe("expired");
    expect(bridge.getBatch(batch.id)).toMatchObject({ state: "cancelled", claim_ids: [], event_ids: [], last_error: "expiry" });
    expect(batchTransitions(bridge, batch.id)).toContainEqual({ from_state: "pending", to_state: "cancelled", reason: "expiry" });
    expect(bridge.listAttempts(batch.id)).toEqual([]);
    expect(bridge.listReceipts(batch.id)).toEqual([]);
  });

  it("defers presence arriving after freeze, then actually dispatches after the lease ends", async () => {
    const { bridge, clock, calls, emit, presence } = setup();
    const emitted = emit("presence-after-freeze");
    bridge.tick();
    const batch = bridge.listBatches()[0];
    const lease = presence();

    await bridge.dispatchDue();

    expect(calls).toEqual([]);
    expect(bridge.getClaim(emitted.claim!.id)).toMatchObject({ state: "deferred", eligible_after: lease.expires_at });
    expect(bridge.getEvent(emitted.event.id)?.state).toBe("batched");
    expect(bridge.getBatch(batch.id)).toMatchObject({ state: "cancelled", last_error: "foreground_presence", claim_ids: [], event_ids: [] });
    expect(batchTransitions(bridge, batch.id)).toContainEqual({ from_state: "pending", to_state: "cancelled", reason: "foreground_presence" });
    expect(bridge.listAttempts(batch.id)).toEqual([]);

    clock.advance(10_000);
    bridge.tick();
    await expect(bridge.dispatchDue()).resolves.toMatchObject([{ status: "accepted" }]);
    expect(calls).toHaveLength(1);
    expect(calls[0].payload.claim_refs.map((ref) => ref.claim_id)).toEqual([emitted.claim!.id]);
  });

  it("defers a batch crossing the quiet boundary and releases at the exact open local minute", async () => {
    const clock = new TestClock(new Date("2026-10-21T21:59:59.000Z"));
    const { bridge, calls, emit } = setup({ clock, config: {
      quiet_hours: quietHours, default_policy: policy({ batch: { window_ms: 2_000 } }),
    } });
    const emitted = emit("quiet-after-freeze");
    bridge.tick();
    const batch = bridge.listBatches()[0];
    clock.advance(2_000);

    await bridge.dispatchDue();

    expect(calls).toEqual([]);
    expect(bridge.getClaim(emitted.claim!.id)).toMatchObject({ state: "deferred", eligible_after: "2026-10-22T08:00:00.000Z" });
    expect(bridge.getBatch(batch.id)).toMatchObject({ state: "cancelled", last_error: "quiet_hours" });
    expect(batchTransitions(bridge, batch.id)).toContainEqual({ from_state: "pending", to_state: "cancelled", reason: "quiet_hours" });
    clock.set("2026-10-22T08:00:00.000Z");
    bridge.tick();
    clock.advance(2_000);
    await expect(bridge.dispatchDue()).resolves.toMatchObject([{ status: "accepted" }]);
    expect(calls).toHaveLength(1);
  });

  it("honors policy quiet and presence bypass after freeze", async () => {
    const clock = new TestClock(new Date("2026-10-21T21:59:59.000Z"));
    const { bridge, calls, emit, presence } = setup({ clock, config: {
      quiet_hours: quietHours,
      default_policy: policy({
        delivery: { mode: "immediate", quiet_hours_policy: "bypass", foreground_presence_policy: "bypass" },
        batch: { window_ms: 2_000 },
      }),
    } });
    const emitted = emit("policy-bypass");
    bridge.tick();
    presence();
    clock.advance(2_000);

    await expect(bridge.dispatchDue()).resolves.toMatchObject([{ status: "accepted" }]);
    expect(calls).toHaveLength(1);
    expect(bridge.getClaim(emitted.claim!.id)?.state).toBe("batched");
  });

  it("detaches only the presence-blocked self-commitment from a mixed batch", async () => {
    const { bridge, clock, calls, presence } = setup({ config: {
      policies: [policy({ id: "self_commitment", batch: { coalesce_by: "coalesce_key", window_ms: 2_000 } })],
    } });
    const scheduled = [true, false].map((defer, index) => bridge.scheduleClaim({
      resource: { uri: `memory://dispatch-gate/${index}` }, eligible_after: clock.now().toISOString(),
      attention_channel: "default", defer_while_presence: defer,
    }));
    bridge.tick();
    const batch = bridge.listBatches()[0];
    expect(batch.claim_ids).toHaveLength(2);
    presence();
    clock.advance(2_000);

    await expect(bridge.dispatchDue()).resolves.toMatchObject([{ status: "accepted" }]);

    expect(calls).toHaveLength(1);
    expect(calls[0].payload.claim_refs.map((ref) => ref.claim_id)).toEqual([scheduled[1].claim.id]);
    expect(bridge.getBatch(batch.id)).toMatchObject({
      state: "dispatched", claim_ids: [scheduled[1].claim.id], event_ids: [scheduled[1].event.id],
    });
    expect(bridge.getClaim(scheduled[0].claim.id)?.state).toBe("deferred");
    expect(bridge.getClaim(scheduled[1].claim.id)?.state).toBe("batched");

    clock.advance(8_000);
    bridge.tick();
    clock.advance(2_000);
    await expect(bridge.dispatchDue()).resolves.toMatchObject([{ status: "accepted" }]);
    expect(calls[1].payload.claim_refs.map((ref) => ref.claim_id)).toEqual([scheduled[0].claim.id]);
  });

  it("refreshes time after a slow transport for later endpoint leases, claim expiry and new attempt leases", async () => {
    const clock = new TestClock();
    let count = 0;
    const { bridge, calls, emit, bind, config } = setup({ clock, config: {
      policies: [policy({ id: "expiring", order: 10, match: { attention_channel_hint: "expiring" }, expires_after_ms: 1_000 })],
    }, dispatch: async () => {
      if (count++ === 0) clock.advance(5_000);
      return { accepted: true };
    } });
    bind("short-lease", 1_000);
    bind("expiring");
    bind("still-live");
    emit("slow-first");
    clock.advance(1);
    emit("expired-endpoint", "short-lease");
    clock.advance(1);
    const expires = emit("expired-claim", "expiring");
    clock.advance(1);
    emit("fresh-attempt", "still-live");
    bridge.tick();

    await bridge.dispatchDue();

    expect(calls.map((call) => call.batch.attention_channel)).toEqual(["default", "still-live"]);
    const waiting = bridge.listBatches().find((batch) => batch.attention_channel === "short-lease")!;
    expect(waiting.state).toBe("waiting_for_endpoint");
    expect(bridge.listAttempts(waiting.id)).toEqual([]);
    expect(bridge.getClaim(expires.claim!.id)?.state).toBe("expired");
    expect(bridge.getEvent(expires.event.id)?.state).toBe("expired");
    const lastAttempt = bridge.listAttempts(calls[1].batch.id)[0];
    expect(lastAttempt.started_at).toBe(clock.now().toISOString());
    expect(lastAttempt.leased_until).toBe(new Date(clock.now().getTime() + config.dispatch_lease_ms!).toISOString());
  });

  it.each(["presence", "quiet", "expiry"] as const)("rechecks %s before a failed transport retry", async (gate) => {
    const clock = new TestClock(new Date("2026-10-21T21:59:59.000Z"));
    const { bridge, calls, emit, presence } = setup({ clock, config: {
      ...(gate === "quiet" ? { quiet_hours: quietHours } : {}),
      default_policy: policy(gate === "expiry" ? { expires_after_ms: 1_000 } : {}),
    }, dispatch: () => ({ accepted: false, retryable: true, error_class: "transport_timeout" }) });
    const emitted = emit(`retry-${gate}`);
    bridge.tick();
    const batch = bridge.listBatches()[0];
    await expect(bridge.dispatchDue()).resolves.toMatchObject([{ status: "retry_wait" }]);
    clock.advance(1_000);
    if (gate === "presence") presence();

    await bridge.dispatchDue();

    expect(calls).toHaveLength(1);
    expect(bridge.getClaim(emitted.claim!.id)?.state).toBe(gate === "expiry" ? "expired" : "deferred");
    expect(bridge.getBatch(batch.id)?.state).toBe("cancelled");
    expect(bridge.listAttempts(batch.id)).toHaveLength(1);
    expect(bridge.listAttempts(batch.id)[0]).toMatchObject({ state: "failed", error_class: "transport_timeout" });
    expect(bridge.listReceipts(batch.id)).toEqual([]);
  });

  it("does not retract in-flight or already accepted work when a gate closes", async () => {
    let finish!: (result: TransportResult) => void;
    const clock = new TestClock(new Date("2026-10-21T21:59:59.000Z"));
    const { bridge, calls, emit, presence } = setup({ clock, config: {
      quiet_hours: quietHours, default_policy: policy({ expires_after_ms: 1_000 }),
    }, dispatch: () => new Promise((resolve) => { finish = resolve; }) });
    const emitted = emit("in-flight-is-not-retracted");
    bridge.tick();
    const batch = bridge.listBatches()[0];
    const inFlight = bridge.dispatchDue();
    expect(calls).toHaveLength(1);
    clock.advance(2_000);
    presence();

    expect(await bridge.dispatchDue()).toEqual([]);
    expect(bridge.getBatch(batch.id)?.state).toBe("dispatching");
    expect(bridge.getClaim(emitted.claim!.id)?.state).toBe("batched");
    finish({ accepted: true });
    await expect(inFlight).resolves.toMatchObject([{ status: "accepted" }]);
    expect(await bridge.dispatchDue()).toEqual([]);
    expect(bridge.getBatch(batch.id)?.state).toBe("dispatched");
    expect(bridge.getClaim(emitted.claim!.id)?.state).toBe("batched");
    expect(calls).toHaveLength(1);
  });

  it.each(["past", "future"] as const)("treats an explicit %s dispatch date as a not-earlier-than start while elapsed clock advances", async (direction) => {
    const clock = new TestClock();
    let count = 0;
    const { bridge, calls, emit, config } = setup({ clock, config: {
      default_policy: policy({ batch: { coalesce_by: "none", window_ms: direction === "future" ? 1_000 : 0 } }),
    }, dispatch: async () => {
      if (count++ === 0) clock.advance(5_000);
      return { accepted: true };
    } });
    const liveStart = clock.now().getTime();
    const explicit = new Date(liveStart + (direction === "past" ? -60_000 : 1_000));
    emit(`explicit-${direction}-first`);
    emit(`explicit-${direction}-second`);
    bridge.tick();

    await bridge.dispatchDue(explicit);

    expect(calls).toHaveLength(2);
    const expectedStart = Math.max(liveStart, explicit.getTime());
    const firstAttempt = bridge.listAttempts(calls[0].batch.id)[0];
    const secondAttempt = bridge.listAttempts(calls[1].batch.id)[0];
    expect(firstAttempt.started_at).toBe(new Date(expectedStart).toISOString());
    expect(secondAttempt.started_at).toBe(new Date(expectedStart + 5_000).toISOString());
    expect(secondAttempt.leased_until).toBe(new Date(expectedStart + 5_000 + config.dispatch_lease_ms!).toISOString());
    expect(firstAttempt.finished_at).toBe(new Date(expectedStart + 5_000).toISOString());
    expect(secondAttempt.finished_at).toBe(new Date(expectedStart + 5_000).toISOString());
  });

  it("keeps missing claim references diagnosable when other claims are gated away", async () => {
    const { bridge, clock, calls, emit, presence } = setup({ config: {
      default_policy: policy({ batch: { coalesce_by: "coalesce_key", window_ms: 1 } }),
    } });
    const missing = emit("missing-reference");
    const gated = emit("remaining-reference");
    bridge.tick();
    const batch = bridge.listBatches()[0];
    bridge.db.exec(`DELETE FROM claims WHERE id=${sqlValue(missing.claim!.id)};`);
    presence();
    clock.advance(1);

    await bridge.dispatchDue();

    expect(calls).toEqual([]);
    expect(bridge.getClaim(gated.claim!.id)?.state).toBe("deferred");
    expect(bridge.getBatch(batch.id)).toMatchObject({
      state: "needs_attention", last_error: "empty_batch",
      claim_ids: [missing.claim!.id], event_ids: [missing.event.id],
    });
    expect(bridge.listReceipts(batch.id)).toEqual([]);
  });

  it.each(["expiry", "presence"] as const)("does not overwrite a claim concurrently consumed before the %s gate commits", async (gate) => {
    const fixture = setup({ config: { default_policy: policy(gate === "expiry" ? { expires_after_ms: 1_000 } : {}) } });
    const { bridge, clock, calls, emit, presence } = fixture;
    const other = reopen(fixture);
    const emitted = emit(`consume-before-${gate}`);
    bridge.tick();
    const batch = bridge.listBatches()[0];
    clock.advance(1_000);
    if (gate === "presence") presence();
    const assertInterleaved = beforeGateTransaction(bridge, () => { other.consumeClaim(emitted.claim!.id, { handled: true }); });

    await bridge.dispatchDue();

    assertInterleaved();
    expect(calls).toEqual([]);
    expect(bridge.getClaim(emitted.claim!.id)?.state).toBe("consumed");
    expect(bridge.getEvent(emitted.event.id)?.state).toBe("consumed");
    expect(bridge.getBatch(batch.id)).toMatchObject({ state: "cancelled", last_error: "all_claims_finalized" });
    expect(bridge.db.query(`SELECT reason FROM claim_transitions WHERE claim_id=${sqlValue(emitted.claim!.id)} AND reason IN ('expiry','foreground_presence');`)).toEqual([]);
  });

  it("does not gate a batch acquired by another worker after the gate snapshot", async () => {
    let finish!: (result: TransportResult) => void;
    const fixture = setup({ dispatch: () => new Promise((resolve) => { finish = resolve; }) });
    const { bridge, calls, emit, presence } = fixture;
    const other = reopen(fixture);
    const emitted = emit("another-worker-acquired");
    bridge.tick();
    const batch = bridge.listBatches()[0];
    presence();
    let acquiredByOther!: Promise<unknown>;
    const assertInterleaved = beforeGateTransaction(bridge, () => {
      other.db.exec("DELETE FROM presence_leases;");
      acquiredByOther = other.dispatchDue();
      expect(calls).toHaveLength(1);
    });

    await bridge.dispatchDue();

    assertInterleaved();
    expect(bridge.getBatch(batch.id)).toMatchObject({ state: "dispatching", claim_ids: [emitted.claim!.id], event_ids: [emitted.event.id] });
    expect(bridge.getClaim(emitted.claim!.id)?.state).toBe("batched");
    expect(bridge.getEvent(emitted.event.id)?.state).toBe("batched");
    finish({ accepted: true });
    await acquiredByOther;
    expect(bridge.getBatch(batch.id)?.state).toBe("dispatched");
    expect(calls).toHaveLength(1);
  });

  it.each([false, true])("retains a concurrently appended claim (defer_while_presence=%s) for a fresh gate pass", async (defer) => {
    const fixture = setup({ config: {
      policies: [policy({ id: "self_commitment", batch: { coalesce_by: "coalesce_key", window_ms: 2_000 } })],
    } });
    const { bridge, clock, calls, presence } = fixture;
    const other = reopen(fixture);
    const schedule = (key: string, deferred: boolean) => bridge.scheduleClaim({
      resource: { uri: `memory://dispatch-gates/${key}` }, eligible_after: clock.now().toISOString(),
      attention_channel: "default", defer_while_presence: deferred,
    });
    const blocked = schedule("blocked", true);
    const allowed = schedule("allowed", false);
    bridge.tick();
    const batch = bridge.listBatches()[0];
    const appended = schedule("appended", defer);
    presence();
    clock.advance(2_000);
    // A tick which read the open batch before its deadline can commit its
    // append after this dispatcher's snapshot. Use a second SQLite instance
    // to control precisely that transaction boundary without timing sleeps.
    const assertInterleaved = beforeGateTransaction(bridge, () => other.db.transaction([
      `UPDATE batches SET claim_ids_json=json_insert(claim_ids_json, '$[#]', ${sqlValue(appended.claim.id)}),
         event_ids_json=json_insert(event_ids_json, '$[#]', ${sqlValue(appended.event.id)}) WHERE id=${sqlValue(batch.id)};`,
      `UPDATE claims SET state='batched' WHERE id=${sqlValue(appended.claim.id)};`,
      `UPDATE event_status SET state='batched' WHERE event_id=${sqlValue(appended.event.id)};`,
    ]));

    await bridge.dispatchDue();

    assertInterleaved();
    expect(calls).toEqual([]);
    expect(bridge.getBatch(batch.id)).toMatchObject({
      state: "pending", claim_ids: [blocked.claim.id, allowed.claim.id, appended.claim.id],
      event_ids: [blocked.event.id, allowed.event.id, appended.event.id],
    });
    expect(bridge.getClaim(blocked.claim.id)?.state).toBe("batched");
    await bridge.dispatchDue();
    expect(calls).toHaveLength(1);
    expect(calls[0].payload.claim_refs.map((ref) => ref.claim_id)).toEqual(defer ? [allowed.claim.id] : [allowed.claim.id, appended.claim.id]);
    expect(bridge.getClaim(appended.claim.id)?.state).toBe(defer ? "deferred" : "batched");
  });

  it("fences a presence renewal before the gate transaction and uses the renewed eligibility on the next pass", async () => {
    const fixture = setup();
    const { bridge, calls, emit, presence, endpoint, binding } = fixture;
    const other = reopen(fixture);
    const emitted = emit("renew-presence-before-gate");
    bridge.tick();
    presence();
    const assertInterleaved = beforeGateTransaction(bridge, () => other.renewPresence({
      attention_channel: "default", endpoint_id: endpoint.id, generation: binding.generation,
      lease_token: endpoint.lease_token, observed_by: "test-host", ttl_ms: 20_000,
    }));

    await bridge.dispatchDue();

    assertInterleaved();
    expect(bridge.getClaim(emitted.claim!.id)?.state).toBe("batched");
    expect(calls).toEqual([]);
    await bridge.dispatchDue();
    expect(bridge.getClaim(emitted.claim!.id)).toMatchObject({ state: "deferred", eligible_after: "2026-10-21T12:00:20.000Z" });
    expect(calls).toEqual([]);
  });

  it("fences presence arriving after preflight but before acquisition", async () => {
    const fixture = setup();
    const { bridge, calls, emit, endpoint, binding } = fixture;
    const other = reopen(fixture);
    const emitted = emit("presence-before-acquire");
    bridge.tick();
    const assertInterleaved = beforeAcquire(bridge, () => other.renewPresence({
      attention_channel: "default", endpoint_id: endpoint.id, generation: binding.generation,
      lease_token: endpoint.lease_token, observed_by: "test-host", ttl_ms: 10_000,
    }));

    await bridge.dispatchDue();

    assertInterleaved();
    expect(calls).toEqual([]);
    expect(bridge.listAttempts()).toEqual([]);
    expect(bridge.getClaim(emitted.claim!.id)?.state).toBe("batched");
    await bridge.dispatchDue();
    expect(bridge.getClaim(emitted.claim!.id)?.state).toBe("deferred");
    expect(calls).toEqual([]);
  });

  it("does not let a newly appended gated claim bypass preflight at acquisition", async () => {
    const fixture = setup({ config: {
      policies: [policy({ id: "self_commitment", batch: { coalesce_by: "coalesce_key", window_ms: 1 } })],
    } });
    const { bridge, clock, calls, presence } = fixture;
    const other = reopen(fixture);
    const allowed = bridge.scheduleClaim({
      resource: { uri: "memory://dispatch-gates/acquire-allowed" }, eligible_after: clock.now().toISOString(), defer_while_presence: false,
    });
    bridge.tick();
    const batch = bridge.listBatches()[0];
    const appended = bridge.scheduleClaim({
      resource: { uri: "memory://dispatch-gates/acquire-appended" }, eligible_after: clock.now().toISOString(), defer_while_presence: true,
    });
    presence();
    clock.advance(1);
    const assertInterleaved = beforeAcquire(bridge, () => other.db.transaction([
      `UPDATE batches SET claim_ids_json=json_insert(claim_ids_json, '$[#]', ${sqlValue(appended.claim.id)}),
         event_ids_json=json_insert(event_ids_json, '$[#]', ${sqlValue(appended.event.id)}) WHERE id=${sqlValue(batch.id)};`,
      `UPDATE claims SET state='batched' WHERE id=${sqlValue(appended.claim.id)};`,
      `UPDATE event_status SET state='batched' WHERE event_id=${sqlValue(appended.event.id)};`,
    ]));

    await bridge.dispatchDue();

    assertInterleaved();
    expect(calls).toEqual([]);
    expect(bridge.listAttempts()).toEqual([]);
    expect(bridge.getBatch(batch.id)?.claim_ids).toEqual([allowed.claim.id, appended.claim.id]);
    await bridge.dispatchDue();
    expect(calls).toHaveLength(1);
    expect(calls[0].payload.claim_refs.map((ref) => ref.claim_id)).toEqual([allowed.claim.id]);
    expect(bridge.getClaim(appended.claim.id)?.state).toBe("deferred");
  });

  it.each(["expiry", "quiet"] as const)("does not dispatch when %s closes during preflight reads", async (gate) => {
    const clock = new TestClock(new Date("2026-10-21T21:59:59.000Z"));
    const { bridge, calls, emit } = setup({ clock, config: {
      ...(gate === "quiet" ? { quiet_hours: quietHours } : {}),
      default_policy: policy(gate === "expiry" ? { expires_after_ms: 1_000 } : {}),
    } });
    const emitted = emit(`preflight-clock-${gate}`);
    bridge.tick();
    const query = bridge.db.query.bind(bridge.db);
    let advanced = false;
    vi.spyOn(bridge.db, "query").mockImplementation(<T extends Record<string, unknown>>(sql: string): T[] => {
      const rows = query<T>(sql);
      if (!advanced && sql.includes("SELECT c.* FROM json_each(")) {
        advanced = true;
        clock.advance(1_000);
      }
      return rows;
    });

    await bridge.dispatchDue();

    expect(advanced).toBe(true);
    expect(calls).toEqual([]);
    expect(bridge.listAttempts()).toEqual([]);
    await bridge.dispatchDue();
    expect(bridge.getClaim(emitted.claim!.id)?.state).toBe(gate === "expiry" ? "expired" : "deferred");
  });

  it("uses a fresh endpoint lease check after payload reads immediately before transport", async () => {
    const { bridge, clock, calls, emit } = setup({ config: { endpoint_lease_ms: 1_000 } });
    emit("endpoint-expired-before-send");
    bridge.tick();
    const batch = bridge.listBatches()[0];
    const query = bridge.db.query.bind(bridge.db);
    let advanced = false;
    vi.spyOn(bridge.db, "query").mockImplementation(<T extends Record<string, unknown>>(sql: string): T[] => {
      const rows = query<T>(sql);
      if (!advanced && sql.includes("FROM events e JOIN event_status")) {
        advanced = true;
        clock.advance(1_000);
      }
      return rows;
    });

    await expect(bridge.dispatchDue()).resolves.toMatchObject([{ status: "stale_generation" }]);

    expect(advanced).toBe(true);
    expect(calls).toEqual([]);
    expect(bridge.listAttempts(batch.id)).toMatchObject([{ state: "failed", error_class: "stale_generation", finished_at: clock.now().toISOString() }]);
    expect(bridge.getBatch(batch.id)).toMatchObject({ state: "retry_wait", last_error: "stale_generation" });
    expect(bridge.listReceipts(batch.id)).toEqual([]);
  });

  it("atomically applies different expiry and presence decisions while retaining a bypass claim", async () => {
    const { bridge, clock, calls, presence } = setup({ config: {
      policies: [policy({ id: "self_commitment", batch: { coalesce_by: "coalesce_key", window_ms: 2_000 } })],
    } });
    const scheduled = [0, 1, 2].map((index) => bridge.scheduleClaim({
      resource: { uri: `memory://dispatch-gates/mixed-state-${index}` }, eligible_after: clock.now().toISOString(),
      expires_at: index === 0 ? new Date(clock.now().getTime() + 1_000).toISOString() : null,
      defer_while_presence: index !== 2,
    }));
    bridge.tick();
    const batch = bridge.listBatches()[0];
    const lease = presence();
    clock.advance(2_000);

    await expect(bridge.dispatchDue()).resolves.toMatchObject([{ status: "accepted" }]);

    expect(bridge.getClaim(scheduled[0].claim.id)?.state).toBe("expired");
    expect(bridge.getEvent(scheduled[0].event.id)?.state).toBe("expired");
    expect(bridge.getClaim(scheduled[1].claim.id)).toMatchObject({ state: "deferred", eligible_after: lease.expires_at });
    expect(bridge.getEvent(scheduled[1].event.id)?.state).toBe("batched");
    expect(bridge.getBatch(batch.id)).toMatchObject({ state: "dispatched", claim_ids: [scheduled[2].claim.id], event_ids: [scheduled[2].event.id] });
    expect(batchTransitions(bridge, batch.id)).toContainEqual({ from_state: "pending", to_state: "pending", reason: "dispatch_gates" });
    expect(calls[0].payload.claim_refs.map((ref) => ref.claim_id)).toEqual([scheduled[2].claim.id]);
    expect(bridge.db.query(`SELECT to_state, reason FROM claim_transitions WHERE claim_id IN (${scheduled.slice(0, 2).map(({ claim }) => sqlValue(claim.id)).join(",")}) AND from_state='batched';`))
      .toEqual(expect.arrayContaining([{ to_state: "expired", reason: "expiry" }, { to_state: "deferred", reason: "foreground_presence" }]));
  });

  it("rolls back batch detachment, claims and events together if a gate transaction fails", async () => {
    const { bridge, clock, calls, emit } = setup({ config: { default_policy: policy({ expires_after_ms: 1_000 }) } });
    const emitted = emit("atomic-gate-rollback");
    bridge.tick();
    const batch = bridge.listBatches()[0];
    clock.advance(1_000);
    const transaction = bridge.db.transaction.bind(bridge.db);
    vi.spyOn(bridge.db, "transaction").mockImplementation((statements) => transaction(
      statements.some((statement) => statement.includes("UPDATE batches SET claim_ids_json=") && statement.includes("AND attempt="))
        ? [...statements, "INSERT INTO deliberately_missing_gate_test_table VALUES(1);"] : statements,
    ));

    await expect(bridge.dispatchDue()).rejects.toMatchObject({
      error_class: "database_execution_failed", message: "SQLite execution failed",
    });

    expect(bridge.getBatch(batch.id)).toMatchObject({ state: "pending", claim_ids: batch.claim_ids, event_ids: batch.event_ids });
    expect(bridge.getClaim(emitted.claim!.id)?.state).toBe("batched");
    expect(bridge.getEvent(emitted.event.id)?.state).toBe("batched");
    expect(batchTransitions(bridge, batch.id)).not.toContainEqual(expect.objectContaining({ reason: "expiry" }));
    expect(calls).toEqual([]);
  });

  it.each([false, true])("supports the public 1000-claim batch maximum with a %s presence gate", async (gated) => {
    const { bridge, clock, calls, emit, presence } = setup({ config: {
      default_policy: policy({ batch: { max_events: 1_000, coalesce_by: "coalesce_key", window_ms: 1 } }),
    } });
    const seed = emit("max-batch-seed");
    bridge.tick();
    const batch = bridge.listBatches()[0];
    const claimIds = [seed.claim!.id];
    const eventIds = [seed.event.id];
    const statements: string[] = [];
    // Seed the same durable rows as a full tick without thousands of sqlite3
    // subprocesses in test setup. Every claim has its own event reference.
    for (let index = 1; index < 1_000; index += 1) {
      const claimId = `ac_max_gate_${index}`;
      const eventId = `evt_max_gate_${index}`;
      claimIds.push(claimId);
      eventIds.push(eventId);
      statements.push(
        `INSERT INTO events(id, instance_id, owner_id, source, type, schema_version, occurred_at, received_at, dedupe_key, resource_json, metadata_json, matched_policy_id, matched_policy_version)
         SELECT ${sqlValue(eventId)}, instance_id, owner_id, source, type, schema_version, occurred_at, received_at, ${sqlValue(`max-batch:${index}`)}, resource_json, metadata_json, matched_policy_id, matched_policy_version FROM events WHERE id=${sqlValue(seed.event.id)};`,
        `INSERT INTO event_status(event_id, state, updated_at) VALUES(${sqlValue(eventId)}, 'batched', ${sqlValue(clock.now().toISOString())});`,
        `INSERT INTO claims(id, instance_id, origin, event_ids_json, resource_json, policy_id, policy_version, attention_channel, eligible_after, expires_at, defer_while_presence, state, reason_code, created_at, updated_at)
         SELECT ${sqlValue(claimId)}, instance_id, origin, ${sqlJson([eventId])}, resource_json, policy_id, policy_version, attention_channel, eligible_after, expires_at, defer_while_presence, state, reason_code, created_at, updated_at FROM claims WHERE id=${sqlValue(seed.claim!.id)};`,
      );
    }
    statements.push(`UPDATE batches SET claim_ids_json=${sqlJson(claimIds)}, event_ids_json=${sqlJson(eventIds)} WHERE id=${sqlValue(batch.id)};`);
    bridge.db.transaction(statements);
    if (gated) presence();
    clock.advance(1);

    const query = vi.spyOn(bridge.db, "query");
    await bridge.dispatchDue();
    // Bound actual database calls, not elapsed time: payload hydration must
    // not start one sqlite3 subprocess per claim or event.
    expect(query.mock.calls.length).toBeLessThanOrEqual(20);

    if (gated) {
      expect(calls).toEqual([]);
      expect(bridge.countClaimsByState().deferred).toBe(1_000);
      expect(bridge.getBatch(batch.id)).toMatchObject({ state: "cancelled", claim_ids: [], event_ids: [], last_error: "foreground_presence" });
    } else {
      expect(calls).toHaveLength(1);
      expect(calls[0].payload.claim_refs).toHaveLength(1_000);
      expect(bridge.getBatch(batch.id)?.state).toBe("dispatched");
    }
  });
});
