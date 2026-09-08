import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { BridgeError, WakeBridge } from "../src/core.js";
import { startDaemon } from "../src/daemon.js";
import type { TransportContext, TransportResult, WakeTransport } from "../src/types.js";

class TestClock {
  value: Date;
  constructor(iso: string) { this.value = new Date(iso); }
  now = () => new Date(this.value.getTime());
  advance(ms: number): void { this.value = new Date(this.value.getTime() + ms); }
}

function dbPath(): string {
  return join(mkdtempSync(join(tmpdir(), "wake-bridge-test-")), "bridge.sqlite");
}

function bridge(clock?: TestClock, config: Record<string, unknown> = {}): WakeBridge {
  return new WakeBridge({
    instance_id: "test-instance",
    owner_id: "test-owner",
    db_path: dbPath(),
    timezone: "UTC",
    endpoint_lease_ms: 24 * 60 * 60_000,
    ...config,
  } as any, { clock: clock?.now });
}

class SlowTransport implements WakeTransport {
  readonly kind = "slow";
  readonly waiters: Array<(result: TransportResult) => void> = [];
  calls = 0;
  dispatch(_context: TransportContext): Promise<TransportResult> {
    this.calls += 1;
    return new Promise((resolve) => this.waiters.push(resolve));
  }
  release(index: number): void {
    this.waiters[index]?.({ accepted: true, transport_kind: this.kind });
  }
}

describe("Wake Bridge M1 core", () => {
  it("persists immutable event/claim and dedupes replay across restart", async () => {
    const path = dbPath();
    const first = new WakeBridge({ instance_id: "i", owner_id: "o", db_path: path, timezone: "UTC", endpoint_lease_ms: 60_000 });
    const firstResult = first.emit("manual", { type: "job.completed", dedupe_key: "job:1", resource: { uri: "job://1" }, metadata: { status: "ok" } });
    const duplicate = first.emit("manual", { type: "job.completed", dedupe_key: "job:1", resource: { uri: "job://different" } });
    expect(duplicate.duplicate).toBe(true);
    expect(first.listEvents()).toHaveLength(1);
    expect(first.listClaims()).toHaveLength(1);
    first.close();

    const second = new WakeBridge({ instance_id: "i", owner_id: "o", db_path: path, timezone: "UTC", endpoint_lease_ms: 60_000 });
    const endpoint = second.registerEndpoint({ host_kind: "mock", session_ref: "session" });
    const binding = second.takeover("default", endpoint.id, 0);
    second.tick();
    const dispatched = await second.dispatchDue();
    expect(dispatched[0]?.status).toBe("accepted");
    expect(second.mockTransport.calls).toHaveLength(1);
    expect((await second.dispatchDue()).every((item) => item.status === "skipped" || item.status === "waiting_for_endpoint")).toBe(true);
    expect(second.listEvents()[0].resource.uri).toBe("job://1");
    expect(second.listReceipts()[0].stage).toBe("transport_accepted");
    expect(second.listReceipts()[0].endpoint_id).toBe(endpoint.id);
    expect(binding.generation).toBe(1);
    second.close();
  });

  it("uses a durable dispatch lease so concurrent dispatch cannot double-dispatch", async () => {
    // A fixed clock makes both workers derive the same lease expiry, covering
    // the race that a timestamp-only ownership check would miss.
    const b = bridge(new TestClock("2026-08-28T12:00:00.000Z"));
    const endpoint = b.registerEndpoint({ host_kind: "mock", session_ref: "session" });
    b.takeover("default", endpoint.id, 0);
    b.emit("manual", { type: "job", dedupe_key: "one", resource: { uri: "job://one" } });
    b.tick();
    const [left, right] = await Promise.all([b.dispatchDue(), b.dispatchDue()]);
    expect(b.mockTransport.calls).toHaveLength(1);
    expect([...left, ...right].filter((item) => item.status === "accepted")).toHaveLength(1);
    b.close();
  });

  it("anchors retry timing to transport completion rather than dispatch start", async () => {
    const clock = new TestClock("2026-08-28T12:00:00.000Z");
    const transport: WakeTransport = {
      kind: "clock-advancing",
      dispatch: async () => {
        clock.advance(60_000);
        return {
          accepted: false,
          retryable: true,
          retry_after_ms: 5_000,
          error_class: "synthetic_timeout",
        };
      },
    };
    const b = new WakeBridge({
      instance_id: "i",
      owner_id: "o",
      db_path: dbPath(),
      timezone: "UTC",
      endpoint_lease_ms: 120_000,
    }, { clock: clock.now, transports: [transport], autoMockTransport: false });
    const endpoint = b.registerEndpoint({
      host_kind: "clock-advancing",
      session_ref: "session",
      capabilities: { cold_push: true },
      routes: [{ kind: "clock-advancing", address: {} }],
    });
    b.takeover("default", endpoint.id, 0);
    b.emit("manual", { type: "job", dedupe_key: "completion-clock", resource: { uri: "job://completion-clock" } });
    b.tick();

    await expect(b.dispatchDue()).resolves.toMatchObject([{ status: "retry_wait" }]);
    const batch = b.listBatches()[0];
    expect(batch.not_before).toBe("2026-08-28T12:01:05.000Z");
    expect(batch.updated_at).toBe("2026-08-28T12:01:00.000Z");
    b.close();
  });

  it("does not let a late worker finalize or receipt a reclaimed attempt", async () => {
    const clock = new TestClock("2026-08-28T12:00:00.000Z");
    const slow = new SlowTransport();
    const b = new WakeBridge({ instance_id: "i", owner_id: "o", db_path: dbPath(), timezone: "UTC", endpoint_lease_ms: 60_000, dispatch_lease_ms: 10, retry_delay_ms: 0 }, { clock: clock.now, transports: [slow], autoMockTransport: false });
    const endpoint = b.registerEndpoint({ host_kind: "slow", session_ref: "session", capabilities: { cold_push: true }, routes: [{ kind: "slow", address: {} }] });
    b.takeover("default", endpoint.id, 0);
    b.emit("manual", { type: "job", dedupe_key: "slow", resource: { uri: "job://slow" } });
    b.tick();
    const first = b.dispatchDue();
    expect(slow.calls).toBe(1);
    clock.advance(11);
    const second = b.dispatchDue();
    expect(slow.calls).toBe(2);
    slow.release(0);
    const firstResult = await first;
    expect(firstResult[0].status).toBe("skipped");
    slow.release(1);
    const secondResult = await second;
    expect(secondResult[0].status).toBe("accepted");
    expect(b.listReceipts().filter((receipt) => receipt.stage === "transport_accepted")).toHaveLength(1);
    expect(b.listAttempts().filter((attempt) => attempt.state === "accepted")).toHaveLength(1);
    b.close();
  });

  it("does not let an MCP-style observer reclaim the daemon's live expired lease", async () => {
    const clock = new TestClock("2026-08-28T12:00:00.000Z");
    const path = dbPath();
    const slow = new SlowTransport();
    const config = {
      instance_id: "i",
      owner_id: "o",
      db_path: path,
      timezone: "UTC",
      endpoint_lease_ms: 60_000,
      dispatch_lease_ms: 10,
      retry_delay_ms: 0,
    };
    const owner = new WakeBridge(config, {
      clock: clock.now,
      transports: [slow],
      autoMockTransport: false,
      recoverDispatchLeases: true,
    });
    const endpoint = owner.registerEndpoint({
      host_kind: "slow",
      session_ref: "session",
      capabilities: { cold_push: true },
      routes: [{ kind: "slow", address: {} }],
    });
    owner.takeover("default", endpoint.id, 0);
    owner.emit("manual", { type: "job", dedupe_key: "observer", resource: { uri: "job://observer" } });
    owner.tick();
    const dispatch = owner.dispatchDue();
    expect(slow.calls).toBe(1);
    clock.advance(11);

    const observer = new WakeBridge(config, {
      clock: clock.now,
      autoMockTransport: false,
      recoverDispatchLeases: false,
    });
    expect(observer.listBatches()[0]?.state).toBe("dispatching");
    expect(observer.listAttempts()[0]?.state).toBe("leased");
    observer.close();

    slow.release(0);
    await expect(dispatch).resolves.toMatchObject([{ status: "accepted" }]);
    expect(owner.listReceipts()).toHaveLength(1);
    owner.close();
  });

  it("does not ack or receipt a stale accepted attempt while a batch is dispatching", async () => {
    const b = bridge();
    const endpoint = b.registerEndpoint({ host_kind: "mock", session_ref: "session" });
    b.takeover("default", endpoint.id, 0);
    b.emit("manual", { type: "job", dedupe_key: "late-ack", resource: { uri: "job://late-ack" } });
    b.tick();
    const result = await b.dispatchDue();
    expect(result[0]?.status).toBe("accepted");
    const batch = b.listBatches()[0];
    expect(batch.state).toBe("dispatched");
    expect(b.listAttempts(batch.id)[0]?.state).toBe("accepted");

    // Leave the old accepted outbox row in place while simulating a newer
    // worker's dispatching lease.  A late agent acknowledgement must not turn
    // that non-final state into seen or create an agent_seen receipt.
    b.db.exec(`UPDATE batches SET state='dispatching', last_error='attempt_newer', updated_at='${batch.updated_at}' WHERE id='${batch.id}'`);
    expect(() => b.ackBatch(batch.id, endpoint.id, 1)).toThrowError(BridgeError);
    expect(b.getBatch(batch.id)?.state).toBe("dispatching");
    expect(b.listReceipts(batch.id).filter((receipt) => receipt.stage === "agent_seen")).toHaveLength(0);
    b.close();
  });

  it("waits for the authoritative scheduler pass while closing during slow transport", async () => {
    const slow = new SlowTransport();
    const config = {
      instance_id: "daemon-instance",
      owner_id: "daemon-owner",
      db_path: dbPath(),
      timezone: "UTC",
      endpoint_lease_ms: 60_000,
      dispatch_lease_ms: 60_000,
      retry_delay_ms: 0,
    };
    const b = new WakeBridge(config, { transports: [slow], autoMockTransport: false });
    const endpoint = b.registerEndpoint({ host_kind: "slow", session_ref: "session", capabilities: { cold_push: true }, routes: [{ kind: "slow", address: {} }] });
    b.takeover("default", endpoint.id, 0);
    b.emit("manual", { type: "job", dedupe_key: "daemon-close", resource: { uri: "job://daemon-close" } });
    const daemon = await startDaemon(config, { bridge: b, host: "127.0.0.1", port: 0, scheduler_interval_ms: 1, unsafe_no_auth: true });

    for (let index = 0; index < 100 && slow.calls < 1; index += 1) {
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
    }
    expect(slow.calls).toBe(1);
    // Let at least one interval fire while the first pass is still awaiting
    // the transport.  The interval must not replace schedulerPromise.
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
    let closed = false;
    const closing = daemon.close().then(() => { closed = true; });
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
    expect(closed).toBe(false);
    slow.release(0);
    await closing;
    expect(closed).toBe(true);
    b.close();
  });

  it("fences stale generation and invalidates the old presence lease", () => {
    const clock = new TestClock("2026-08-28T12:00:00.000Z");
    const b = bridge(clock, { presence_ttl_ms: 10_000 });
    const oldEndpoint = b.registerEndpoint({ host_kind: "mock", session_ref: "old", capabilities: { foreground_presence_observable: true } });
    const oldBinding = b.takeover("life", oldEndpoint.id, 0);
    b.renewPresence({ attention_channel: "life", endpoint_id: oldEndpoint.id, generation: oldBinding.generation, lease_token: oldEndpoint.lease_token, observed_by: "host", ttl_ms: 10_000 });
    const newEndpoint = b.registerEndpoint({ host_kind: "mock", session_ref: "new", capabilities: { foreground_presence_observable: true } });
    const newBinding = b.takeover("life", newEndpoint.id, oldBinding.generation);
    expect(newBinding.generation).toBe(2);
    expect(b.getPresence("life")).toBeNull();
    expect(() => b.renewPresence({ attention_channel: "life", endpoint_id: oldEndpoint.id, generation: oldBinding.generation, lease_token: oldEndpoint.lease_token, observed_by: "old-host", ttl_ms: 10_000 })).toThrowError(BridgeError);
    expect(() => b.takeover("life", oldEndpoint.id, 1)).toThrowError(/generation mismatch/);
    b.close();
  });

  it("rejects presence renewal from a host that did not declare a foreground observer", () => {
    const b = bridge();
    const endpoint = b.registerEndpoint({ host_kind: "opaque_host", session_ref: "session" });
    const binding = b.takeover("life", endpoint.id, 0);
    expect(() => b.renewPresence({
      attention_channel: "life",
      endpoint_id: endpoint.id,
      generation: binding.generation,
      lease_token: endpoint.lease_token,
      observed_by: "unsupported_host",
    })).toThrowError(/cannot observe foreground presence/u);
    expect(b.getPresence("life")).toBeNull();
    b.close();
  });

  it("defers background claims during channel presence and releases after TTL", () => {
    const clock = new TestClock("2026-08-28T12:00:00.000Z");
    const b = bridge(clock, { presence_ttl_ms: 10_000 });
    const endpoint = b.registerEndpoint({ host_kind: "mock", session_ref: "session", capabilities: { foreground_presence_observable: true } });
    const binding = b.takeover("life", endpoint.id, 0);
    b.renewPresence({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observed_by: "host", ttl_ms: 10_000 });
    const emitted = b.emit("manual", { type: "job", dedupe_key: "presence", attention_channel_hint: "life", resource: { uri: "job://presence" } });
    const firstTick = b.tick();
    expect(firstTick.deferred_claims).toContain(emitted.claim!.id);
    expect(b.listClaims()[0].state).toBe("deferred");
    expect(b.listBatches()).toHaveLength(0);
    clock.advance(10_001);
    const secondTick = b.tick();
    expect(secondTick.batches_created).toHaveLength(1);
    expect(b.listClaims()[0].state).toBe("batched");
    b.close();
  });

  it("re-applies a gate to an eligible claim recovered between scheduler transactions", () => {
    const clock = new TestClock("2026-08-28T12:00:00.000Z");
    const b = bridge(clock, { presence_ttl_ms: 10_000 });
    const endpoint = b.registerEndpoint({ host_kind: "mock", session_ref: "session", capabilities: { foreground_presence_observable: true } });
    const binding = b.takeover("life", endpoint.id, 0);
    const emitted = b.emit("manual", { type: "job", dedupe_key: "crash-gap", attention_channel_hint: "life", resource: { uri: "job://crash-gap" } });
    b.renewPresence({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observed_by: "host", ttl_ms: 10_000 });
    // Simulate a crash after the eligible transition committed but before the
    // batch transaction committed.
    b.db.exec(`UPDATE claims SET state='eligible', eligible_after='${clock.value.toISOString()}', updated_at='${clock.value.toISOString()}' WHERE id='${emitted.claim!.id}'`);
    b.tick();
    expect(b.getClaim(emitted.claim!.id)?.state).toBe("deferred");
    expect(b.listBatches()).toHaveLength(0);
    b.close();
  });

  it("applies quiet hours, then resumes at the first open local minute", () => {
    const clock = new TestClock("2026-08-28T23:00:00.000Z");
    const b = bridge(clock, { quiet_hours: { timezone: "UTC", windows: [{ start: "22:00", end: "08:00" }] } });
    const emitted = b.emit("manual", { type: "job", dedupe_key: "quiet", resource: { uri: "job://quiet" } });
    b.tick();
    expect(b.getClaim(emitted.claim!.id)?.state).toBe("deferred");
    expect(b.listBatches()).toHaveLength(0);
    clock.advance(9 * 60 * 60_000 + 1_000);
    expect(b.tick().batches_created).toHaveLength(1);
    b.close();
  });

  it("supports snooze, dismiss and consume as durable audited claim actions", () => {
    const clock = new TestClock("2026-08-28T12:00:00.000Z");
    const b = bridge(clock);
    const snoozed = b.emit("manual", { type: "job", dedupe_key: "snooze", resource: { uri: "job://snooze" } }).claim!;
    const until = new Date(clock.value.getTime() + 60_000).toISOString();
    expect(b.snoozeClaim(snoozed.id, until).state).toBe("pending");
    clock.advance(60_001);
    expect(b.tick().batches_created).toHaveLength(1);
    const dismissed = b.emit("manual", { type: "job", dedupe_key: "dismiss", resource: { uri: "job://dismiss" } }).claim!;
    expect(b.dismissClaim(dismissed.id, "handled_elsewhere").state).toBe("dismissed");
    const consumed = b.emit("manual", { type: "job", dedupe_key: "consume", resource: { uri: "job://consume" } }).claim!;
    expect(b.consumeClaim(consumed.id, { handled: true }).state).toBe("consumed");
    expect(b.getEvent(consumed.event_ids[0])?.state).toBe("consumed");
    expect(b.db.query<any>(`SELECT COUNT(*) AS n FROM claim_transitions WHERE claim_id='${snoozed.id}'`).at(0)?.n).toBe(4);
    b.close();
  });

  it("cancels an undispatched batch without raising an operator incident when its final claim is finalized", async () => {
    const clock = new TestClock("2026-08-28T12:00:00.000Z");
    const b = bridge(clock);
    const emitted = b.emit("manual", { type: "job", dedupe_key: "cancel-empty", resource: { uri: "job://cancel-empty" } });
    expect(b.tick().batches_created).toHaveLength(1);
    const batch = b.listBatches()[0];
    expect(batch.state).toBe("pending");

    b.dismissClaim(emitted.claim!.id, "handled_elsewhere");
    expect(b.getBatch(batch.id)).toMatchObject({
      state: "cancelled",
      claim_ids: [],
      event_ids: [],
      last_error: "all_claims_finalized",
    });
    expect(b.db.query<any>(`SELECT from_state, to_state, reason FROM batch_transitions WHERE batch_id='${batch.id}' ORDER BY seq DESC LIMIT 1`))
      .toEqual([{ from_state: "pending", to_state: "cancelled", reason: "all_claims_finalized" }]);

    // Reconcile the exact noisy state produced by pre-fix instances.
    b.db.exec(`UPDATE batches SET state='needs_attention' WHERE id='${batch.id}'`);
    await b.dispatchDue();
    expect(b.getBatch(batch.id)?.state).toBe("cancelled");
    expect(b.db.query<any>(`SELECT from_state, to_state, reason FROM batch_transitions WHERE batch_id='${batch.id}' ORDER BY seq DESC LIMIT 1`))
      .toEqual([{ from_state: "needs_attention", to_state: "cancelled", reason: "all_claims_finalized_reconciled" }]);
    b.close();
  });

  it("creates a self-commitment event and claim atomically with idempotency", () => {
    const b = bridge();
    expect(() => b.scheduleClaim({ resource: { uri: "memory://thought/local-time" }, eligible_after: "2026-09-01T10:00:00" })).toThrowError(/absolute RFC3339/);
    const first = b.scheduleClaim({ resource: { uri: "memory://thought/a" }, eligible_after: "2026-09-01T10:00:00.000Z", attention_channel: "life", reason_code: "reconsider", idempotency_key: "a" });
    const duplicate = b.scheduleClaim({ resource: { uri: "memory://different" }, eligible_after: "2026-09-01T10:00:00.000Z", attention_channel: "life", idempotency_key: "a" });
    expect(duplicate.duplicate).toBe(true);
    expect(duplicate.claim.id).toBe(first.claim.id);
    expect(first.event.source).toBe("self_commitment");
    expect(first.claim.origin).toBe("self_commitment");
    expect(first.claim.defer_while_presence).toBe(true);
    b.close();
  });

  it("fires one durable inactivity claim per non-Wake activity epoch across restart", () => {
    const clock = new TestClock("2026-08-29T00:00:00.000Z");
    const path = dbPath();
    const config = { instance_id: "i", owner_id: "o", db_path: path, timezone: "UTC", endpoint_lease_ms: 24 * 60 * 60_000 };
    const first = new WakeBridge(config, { clock: clock.now });
    const endpoint = first.registerEndpoint({ id: "ep-life", host_kind: "fake_host", session_ref: "life", capabilities: { session_activity_observable: true } });
    const binding = first.takeover("life", endpoint.id, 0);
    first.configureActivityWatch({ attention_channel: "life", enabled: true, idle_after_ms: 60_000 });
    expect(first.getActivityWatch("life")?.condition_status).toBe("waiting_for_activity");
    first.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "turn-1:start", kind: "activity_started", ttl_ms: 30_000 });
    expect(first.getActivityWatch("life")?.condition_status).toBe("watching");
    clock.advance(10_000);
    first.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "turn-1:settle", kind: "activity_settled" });
    first.close();

    const restarted = new WakeBridge(config, { clock: clock.now });
    clock.advance(59_999);
    expect(restarted.tick().inactivity_claims_created).toHaveLength(0);
    clock.advance(1);
    const fired = restarted.tick();
    expect(fired.inactivity_claims_created).toHaveLength(1);
    expect(restarted.listEvents({ source: "wakebridge.core" })).toMatchObject([{ type: "channel.inactive" }]);
    expect(restarted.getClaim(fired.inactivity_claims_created[0])).toMatchObject({ origin: "inactivity_watch", reason_code: "host_inactive", defer_while_presence: true });
    expect(restarted.tick().inactivity_claims_created).toHaveLength(0);

    restarted.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "wake-1:start", kind: "wake_started" });
    expect(restarted.getActivityWatch("life")).toMatchObject({ epoch: 1, fired_epoch: 1 });
    restarted.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "turn-2:start", kind: "activity_started", ttl_ms: 30_000 });
    expect(restarted.getActivityWatch("life")).toMatchObject({ epoch: 2, fired_epoch: null });
    clock.advance(5_000);
    restarted.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "turn-2:settle", kind: "activity_settled" });
    clock.advance(60_000);
    expect(restarted.tick().inactivity_claims_created).toHaveLength(1);
    expect(restarted.listEvents({ source: "wakebridge.core" })).toHaveLength(2);
    restarted.close();
  });

  it("keeps an explicit watch unsupported when the bound host cannot observe session activity", () => {
    const clock = new TestClock("2026-08-29T00:00:00.000Z");
    const b = bridge(clock);
    const endpoint = b.registerEndpoint({ host_kind: "opaque_host", session_ref: "life" });
    b.takeover("life", endpoint.id, 0);
    b.configureActivityWatch({ attention_channel: "life", enabled: true, idle_after_ms: 60_000, mode: "repeat", repeat_after_ms: 60_000 });
    expect(b.getActivityWatch("life")?.condition_status).toBe("unsupported");
    clock.advance(24 * 60 * 60_000);
    expect(b.tick().inactivity_claims_created).toEqual([]);
    b.close();
  });

  it("uses activity progress as a busy lease without moving the settled inactivity baseline", () => {
    const clock = new TestClock("2026-08-29T00:00:00.000Z");
    const b = bridge(clock, { activity_ttl_ms: 30_000 });
    const endpoint = b.registerEndpoint({ host_kind: "fake_host", session_ref: "life", capabilities: { session_activity_observable: true } });
    const binding = b.takeover("life", endpoint.id, 0);
    b.configureActivityWatch({ attention_channel: "life", enabled: true, idle_after_ms: 60_000 });
    b.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "turn:start", kind: "activity_started" });
    clock.advance(25_000);
    b.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "turn:progress:1", kind: "activity_progress" });
    clock.advance(29_000);
    expect(b.tick().inactivity_claims_created).toHaveLength(0);
    b.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "turn:settle", kind: "activity_settled" });
    clock.advance(60_000);
    expect(b.tick().inactivity_claims_created).toHaveLength(1);
    b.close();
  });

  it.each([
    ["activity_progress", true],
    ["activity_settled", false],
  ] as const)("lets a first %s observation establish the current-generation baseline", (kind, busy) => {
    const clock = new TestClock("2026-08-29T00:00:00.000Z");
    const b = bridge(clock, { activity_ttl_ms: 30_000 });
    const endpoint = b.registerEndpoint({ host_kind: "polling_host", session_ref: "life", capabilities: { session_activity_observable: true } });
    const binding = b.takeover("life", endpoint.id, 0);
    b.configureActivityWatch({ attention_channel: "life", enabled: true, idle_after_ms: 60_000 });

    expect(b.getActivityWatch("life")?.condition_status).toBe("waiting_for_activity");
    b.observeActivity({
      attention_channel: "life",
      endpoint_id: endpoint.id,
      generation: binding.generation,
      lease_token: endpoint.lease_token,
      observation_id: `first:${kind}`,
      kind,
    });

    expect(b.getActivityWatch("life")).toMatchObject({
      endpoint_id: endpoint.id,
      binding_generation: binding.generation,
      armed_at: "2026-08-29T00:00:00.000Z",
      last_nonwake_activity_at: "2026-08-29T00:00:00.000Z",
      activity_lease_expires_at: busy ? "2026-08-29T00:00:30.000Z" : null,
      condition_status: "watching",
    });
    b.close();
  });

  it("does not let a first Wake-origin observation establish an inactivity baseline", () => {
    const clock = new TestClock("2026-08-29T00:00:00.000Z");
    const b = bridge(clock);
    const endpoint = b.registerEndpoint({ host_kind: "fake_host", session_ref: "life", capabilities: { session_activity_observable: true } });
    const binding = b.takeover("life", endpoint.id, 0);
    b.configureActivityWatch({ attention_channel: "life", enabled: true, idle_after_ms: 60_000 });

    b.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "wake:first", kind: "wake_started" });
    expect(b.getActivityWatch("life")).toMatchObject({
      endpoint_id: endpoint.id,
      binding_generation: binding.generation,
      armed_at: null,
      last_nonwake_activity_at: null,
      condition_status: "waiting_for_activity",
    });
    b.close();
  });

  it("lets later progress-only host evidence rearm a fired inactivity epoch", () => {
    const clock = new TestClock("2026-08-29T00:00:00.000Z");
    const b = bridge(clock, { activity_ttl_ms: 30_000 });
    const endpoint = b.registerEndpoint({ host_kind: "polling_host", session_ref: "life", capabilities: { session_activity_observable: true } });
    const binding = b.takeover("life", endpoint.id, 0);
    b.configureActivityWatch({ attention_channel: "life", enabled: true, idle_after_ms: 60_000 });
    b.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "first:settled", kind: "activity_settled" });
    clock.advance(60_000);
    expect(b.tick().inactivity_claims_created).toHaveLength(1);
    expect(b.getActivityWatch("life")).toMatchObject({ epoch: 1, fired_epoch: 1 });

    b.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "next:progress", kind: "activity_progress" });
    expect(b.getActivityWatch("life")).toMatchObject({
      epoch: 2,
      fired_epoch: null,
      last_nonwake_activity_at: "2026-08-29T00:01:00.000Z",
      activity_lease_expires_at: "2026-08-29T00:01:30.000Z",
      condition_status: "watching",
    });
    b.close();
  });

  it("lets a Wake runner hold busy without rearming or moving the idle baseline", () => {
    const clock = new TestClock("2026-08-29T00:00:00.000Z");
    const b = bridge(clock, { activity_ttl_ms: 120_000 });
    const endpoint = b.registerEndpoint({ host_kind: "fake_host", session_ref: "life", capabilities: { session_activity_observable: true } });
    const binding = b.takeover("life", endpoint.id, 0);
    b.configureActivityWatch({ attention_channel: "life", enabled: true, idle_after_ms: 60_000 });
    b.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "turn:baseline", kind: "owner_message_accepted" });
    b.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "wake:start", kind: "wake_started" });
    clock.advance(60_000);
    expect(b.tick().inactivity_claims_created).toHaveLength(0);
    expect(b.getActivityWatch("life")).toMatchObject({ epoch: 1, fired_epoch: null, last_nonwake_activity_at: "2026-08-29T00:00:00.000Z" });
    b.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "wake:settle", kind: "wake_settled" });
    expect(b.tick().inactivity_claims_created).toHaveLength(1);
    expect(b.getActivityWatch("life")).toMatchObject({ epoch: 1, fired_epoch: 1 });
    b.close();
  });

  it("repeats while inactivity remains true and ignores self-commitment coverage", () => {
    const clock = new TestClock("2026-08-29T00:00:00.000Z");
    const b = bridge(clock, { activity_ttl_ms: 180_000 });
    const endpoint = b.registerEndpoint({ host_kind: "fake_host", session_ref: "life", lease_ms: 48 * 60 * 60_000, capabilities: { session_activity_observable: true } });
    const binding = b.takeover("life", endpoint.id, 0);
    b.configureActivityWatch({
      attention_channel: "life",
      enabled: true,
      idle_after_ms: 60_000,
      mode: "repeat",
      repeat_after_ms: 120_000,
    });
    b.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "turn:baseline", kind: "owner_message_accepted" });

    clock.advance(60_000);
    expect(b.tick().inactivity_claims_created).toHaveLength(1);
    expect(b.getActivityWatch("life")).toMatchObject({
      epoch: 1,
      fired_epoch: 1,
      sequence: 1,
      next_due_at: "2026-08-29T00:03:00.000Z",
    });

    // A Wake runner's own work holds the retry without creating a new epoch.
    clock.advance(90_000);
    b.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "wake-1:start", kind: "wake_started" });
    clock.advance(120_000);
    expect(b.tick().inactivity_claims_created).toHaveLength(0);
    b.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "wake-1:settle", kind: "wake_settled" });
    const afterSettle = b.getActivityWatch("life")!;
    expect(afterSettle.next_due_at).toBe("2026-08-29T00:06:30.000Z");

    clock.advance(119_999);
    expect(b.tick().inactivity_claims_created).toHaveLength(0);
    clock.advance(1);
    expect(b.tick().inactivity_claims_created).toHaveLength(1);
    expect(b.getActivityWatch("life")).toMatchObject({ sequence: 2 });

    b.scheduleClaim({
      resource: { uri: "memory://thought/next-hop" },
      eligible_after: "2026-08-29T02:00:00.000Z",
      attention_channel: "life",
      idempotency_key: "next-hop",
    });
    clock.advance(120_000);
    expect(b.tick().inactivity_claims_created).toHaveLength(1);
    expect(b.getActivityWatch("life")).toMatchObject({ sequence: 3 });

    // New non-Wake work starts a new epoch and clears old coverage.
    b.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "turn-2:start", kind: "activity_started" });
    expect(b.getActivityWatch("life")).toMatchObject({ epoch: 2, fired_epoch: null, sequence: 0 });
    b.observeActivity({ attention_channel: "life", endpoint_id: endpoint.id, generation: binding.generation, lease_token: endpoint.lease_token, observation_id: "turn-2:settle", kind: "activity_settled" });
    clock.advance(60_000);
    expect(b.tick().inactivity_claims_created).toHaveLength(1);
    expect(b.getActivityWatch("life")).toMatchObject({ epoch: 2, sequence: 1 });
    b.close();
  });

  it("resolves a frozen event reference directly even beyond the inspect index limit", async () => {
    const b = bridge();
    const emitted = b.emit("manual", { type: "target", dedupe_key: "target", resource: { uri: "job://target" } });
    const quote = (value: string) => `'${value.replaceAll("'", "''")}'`;
    const filler = Array.from({ length: 1_000 }, (_, index) => {
      const id = `filler-${String(index).padStart(4, "0")}`;
      return `INSERT INTO events(id, instance_id, owner_id, source, type, schema_version, occurred_at, received_at, dedupe_key, coalesce_key, priority_hint, attention_channel_hint, actor_ref, resource_json, metadata_json, payload_preview) VALUES(${quote(id)}, 'test-instance', 'test-owner', 'filler', 'filler', 1, '2020-01-01T00:00:00.000Z', '2020-01-01T00:00:00.000Z', ${quote(id)}, NULL, NULL, NULL, NULL, ${quote(JSON.stringify({ uri: `filler://${id}` }))}, '{}', NULL); INSERT INTO event_status(event_id, state, updated_at) VALUES(${quote(id)}, 'matched', '2020-01-01T00:00:00.000Z');`;
    }).join("\n");
    b.db.exec(filler);
    const endpoint = b.registerEndpoint({ host_kind: "mock", session_ref: "session" });
    b.takeover("default", endpoint.id, 0);
    b.tick();
    await b.dispatchDue();
    expect(b.mockTransport.calls[0].payload.claim_refs[0].event_id).toBe(emitted.event.id);
    expect(b.mockTransport.calls[0].payload.claim_refs[0].source).toBe("manual");
    b.close();
  });
});
