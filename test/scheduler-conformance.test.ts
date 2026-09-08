import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { BridgeError, WakeBridge } from "../src/core.js";
import { sqlJson, sqlValue } from "../src/db.js";
import type { PolicyRule } from "../src/types.js";

class TestClock {
  value: Date;
  constructor(iso: string) { this.value = new Date(iso); }
  now = (): Date => new Date(this.value.getTime());
  advance(milliseconds: number): void { this.value = new Date(this.value.getTime() + milliseconds); }
}

function temporary(): string {
  return join(mkdtempSync(join(tmpdir(), "wake-bridge-scheduler-")), "bridge.sqlite");
}

function bridge(path = temporary(), clock = new TestClock("2026-08-30T12:00:00.000Z"), config: Record<string, unknown> = {}): WakeBridge {
  return new WakeBridge({
    instance_id: "scheduler-test",
    owner_id: "owner",
    db_path: path,
    timezone: "UTC",
    endpoint_lease_ms: 24 * 60 * 60_000,
    ...config,
  } as any, { clock: clock.now });
}

function policy(id: string, version: number, mode: "immediate" | "scheduled" | "suppress", match: Record<string, unknown>, extra: Partial<PolicyRule> = {}): PolicyRule {
  return {
    id,
    version,
    order: 100,
    match,
    delivery: mode === "scheduled" ? { mode, scheduled_local_time: "08:30" } : { mode },
    ...extra,
  };
}

describe("current scheduler semantic conformance", () => {
  it("rejects legacy scheduler semantics at every current install/config boundary and labels stored history", () => {
    expect(() => bridge(temporary(), new TestClock("2026-08-30T12:00:00.000Z"), {
      quiet_hours: { windows: [], resume_spread_ms: 1000 },
    })).toThrowError(expect.objectContaining<Partial<BridgeError>>({ code: "unsupported_config_field" }));
    expect(() => bridge(temporary(), new TestClock("2026-08-30T12:00:00.000Z"), {
      quiet_hours: { windows: [], default_action: "defer" },
    })).toThrowError(expect.objectContaining<Partial<BridgeError>>({ code: "unsupported_config_field" }));
    expect(() => bridge(temporary(), new TestClock("2026-08-30T12:00:00.000Z"), {
      timezone: "Mars/Olympus_Mons",
    })).toThrowError(expect.objectContaining<Partial<BridgeError>>({ code: "invalid_timezone" }));
    expect(() => bridge(temporary(), new TestClock("2026-08-30T12:00:00.000Z"), {
      quiet_hours: { windows: [{ start: "00:00", end: "12:00" }, { start: "12:00", end: "00:00" }] },
    })).toThrowError(expect.objectContaining<Partial<BridgeError>>({ code: "invalid_quiet_hours" }));

    const current = bridge();
    try {
      expect(() => current.installPolicies([{
        ...policy("legacy", 1, "immediate", { source: "legacy" }),
        delivery: { mode: "digest", cadence_ms: 60_000 },
      } as unknown as PolicyRule])).toThrowError(expect.objectContaining<Partial<BridgeError>>({ code: "unsupported_policy_mode" }));
      expect(() => current.installPolicies([{
        ...policy("inert", 1, "immediate", { source: "legacy" }),
        delivery: { mode: "immediate", max_delay_ms: 60_000 },
      } as unknown as PolicyRule])).toThrowError(expect.objectContaining<Partial<BridgeError>>({ code: "unsupported_policy_field" }));
      expect(() => current.installPolicies([{
        ...policy("transport", 1, "immediate", { source: "legacy" }),
        target: { attention_channel: "default", busy_behavior: "steer" },
      } as unknown as PolicyRule])).toThrowError(expect.objectContaining<Partial<BridgeError>>({ code: "unsupported_policy_field" }));

      const legacy = {
        id: "legacy",
        version: 1,
        enabled: true,
        order: 100,
        match: { source: "legacy" },
        delivery: { mode: "digest", cadence_ms: 60_000, quiet_hours_policy: "defer", foreground_presence_policy: "defer" },
        batch: { coalesce_by: "source", max_events: 20, window_ms: 0 },
        target: { attention_channel: "default", prefer_transport: "warm" },
        reason_code: "legacy",
      };
      current.db.exec(`INSERT INTO policies(id, version, enabled, order_no, rule_json, installed_at)
        VALUES('legacy', 1, 1, 100, ${sqlJson(legacy)}, ${sqlValue(current.nowIso())});`);
      expect(current.listPolicyStatuses()).toEqual(expect.arrayContaining([
        expect.objectContaining({ id: "legacy", version: 1, active: true, new_event_matching: "legacy_unsupported" }),
      ]));
      const emitted = current.emit("legacy", { type: "legacy.event", dedupe_key: "legacy-ignored", resource: { uri: "legacy://ignored" } });
      expect(emitted.event.matched_policy_id).toBe("default");
    } finally {
      current.close();
    }
  });

  it("resolves scheduled local time deterministically across the DST spring gap and autumn fold", () => {
    const springClock = new TestClock("2026-03-08T06:00:00.000Z"); // 01:00 EST; 02:30 does not exist that day.
    const spring = bridge(temporary(), springClock, { timezone: "America/New_York" });
    try {
      spring.installPolicies([{
        ...policy("scheduled", 1, "scheduled", { source: "jobs" }),
        delivery: { mode: "scheduled", scheduled_local_time: "02:30" },
      }]);
      expect(spring.previewPolicy("jobs", { type: "job", dedupe_key: "spring", resource: { uri: "job://spring" } }))
        .toMatchObject({ claim: { eligible_after: "2026-03-09T06:30:00.000Z" } });
    } finally {
      spring.close();
    }

    const foldClock = new TestClock("2026-11-01T05:45:00.000Z"); // 01:45 EDT; a second 01:30 still follows.
    const fold = bridge(temporary(), foldClock, { timezone: "America/New_York" });
    try {
      fold.installPolicies([{
        ...policy("scheduled", 1, "scheduled", { source: "jobs" }),
        delivery: { mode: "scheduled", scheduled_local_time: "01:30" },
      }]);
      expect(fold.previewPolicy("jobs", { type: "job", dedupe_key: "fold", resource: { uri: "job://fold" } }))
        .toMatchObject({ claim: { eligible_after: "2026-11-01T06:30:00.000Z" } });
    } finally {
      fold.close();
    }

    const secondsClock = new TestClock("2026-08-30T08:00:37.456Z");
    const seconds = bridge(temporary(), secondsClock);
    try {
      seconds.installPolicies([policy("scheduled", 1, "scheduled", { source: "jobs" })]);
      expect(seconds.previewPolicy("jobs", { type: "job", dedupe_key: "seconds", resource: { uri: "job://seconds" } }))
        .toMatchObject({ claim: { eligible_after: "2026-08-30T08:30:00.000Z" } });
      secondsClock.value = new Date("2026-08-30T08:30:00.000Z");
      expect(seconds.previewPolicy("jobs", { type: "job", dedupe_key: "exact", resource: { uri: "job://exact" } }))
        .toMatchObject({ claim: { eligible_after: "2026-08-30T08:30:00.000Z" } });
      secondsClock.value = new Date("2026-08-30T08:30:00.001Z");
      expect(seconds.previewPolicy("jobs", { type: "job", dedupe_key: "past", resource: { uri: "job://past" } }))
        .toMatchObject({ claim: { eligible_after: "2026-08-31T08:30:00.000Z" } });
    } finally {
      seconds.close();
    }
  });

  it("persists a scheduled claim across restart and materializes it exactly at its frozen due time", () => {
    const path = temporary();
    const clock = new TestClock("2026-08-30T08:00:00.000Z");
    const first = bridge(path, clock);
    first.installPolicies([policy("morning", 1, "scheduled", { source: "jobs" })]);
    const emitted = first.emit("jobs", { type: "job", dedupe_key: "restart", resource: { uri: "job://restart" } });
    expect(emitted.claim).toMatchObject({ policy_id: "morning", eligible_after: "2026-08-30T08:30:00.000Z", state: "pending" });
    first.close();

    clock.advance(30 * 60_000);
    const restarted = bridge(path, clock);
    try {
      const result = restarted.tick();
      expect(result.batches_created).toHaveLength(1);
      expect(restarted.listBatches()[0]).toMatchObject({ policy_id: "morning", policy_version: 1, not_before: "2026-08-30T08:30:00.000Z" });
    } finally {
      restarted.close();
    }
  });

  it("computes quiet-hour release through a DST gap and lets expiry win before a deferred release", () => {
    const dstClock = new TestClock("2026-03-08T06:45:37.456Z"); // 01:45:37 EST.
    const dst = bridge(temporary(), dstClock, {
      timezone: "America/New_York",
      quiet_hours: { timezone: "America/New_York", windows: [{ start: "01:30", end: "03:30" }] },
    });
    try {
      expect(dst.previewPolicy("jobs", { type: "job", dedupe_key: "quiet-dst", resource: { uri: "job://quiet-dst" } }))
        .toMatchObject({ gates: { quiet_hours: { active: true, defer_until: "2026-03-08T07:30:00.000Z" } } });
    } finally {
      dst.close();
    }

    const expiryClock = new TestClock("2026-08-30T12:00:00.000Z");
    const expiry = bridge(temporary(), expiryClock, {
      quiet_hours: { windows: [{ start: "12:00", end: "14:00" }] },
    });
    try {
      expiry.installPolicies([policy("expires", 1, "immediate", { source: "jobs" }, { expires_after_ms: 60_000 })]);
      const emitted = expiry.emit("jobs", { type: "job", dedupe_key: "expires", resource: { uri: "job://expires" } });
      expiry.tick();
      expect(expiry.getClaim(emitted.claim!.id)).toMatchObject({ state: "deferred", eligible_after: "2026-08-30T14:00:00.000Z" });
      expiryClock.advance(60_000);
      expiry.tick();
      expect(expiry.getClaim(emitted.claim!.id)).toMatchObject({ state: "expired" });
      expect(expiry.getEvent(emitted.event.id)).toMatchObject({ state: "expired" });
    } finally {
      expiry.close();
    }
  });

  it("releases foreground presence at the exact lease boundary across restart", () => {
    const path = temporary();
    const clock = new TestClock("2026-08-30T12:00:00.000Z");
    const first = bridge(path, clock);
    const endpoint = first.registerEndpoint({ host_kind: "mock", session_ref: "presence", capabilities: { foreground_presence_observable: true } });
    const binding = first.takeover("default", endpoint.id, 0);
    first.renewPresence({
      attention_channel: "default",
      endpoint_id: endpoint.id,
      generation: binding.generation,
      lease_token: endpoint.lease_token,
      ttl_ms: 60_000,
      observed_by: "fixture",
      observation: "user_message_accepted",
    });
    const emitted = first.emit("jobs", { type: "job", dedupe_key: "presence", resource: { uri: "job://presence" } });
    first.tick();
    expect(first.getClaim(emitted.claim!.id)).toMatchObject({ state: "deferred", eligible_after: "2026-08-30T12:01:00.000Z" });
    first.close();

    clock.advance(60_000);
    const restarted = bridge(path, clock);
    try {
      const result = restarted.tick();
      expect(result.batches_created).toHaveLength(1);
      expect(restarted.getClaim(emitted.claim!.id)).toMatchObject({ state: "batched" });
    } finally {
      restarted.close();
    }
  });

  it("freezes batch window and max size across append, restart, and dispatch boundaries", async () => {
    const path = temporary();
    const clock = new TestClock("2026-08-30T12:00:00.000Z");
    const first = bridge(path, clock);
    first.installPolicies([policy("grouped", 1, "immediate", { source: "jobs" }, {
      batch: { coalesce_by: "source", max_events: 2, window_ms: 60_000 },
    })]);
    first.emit("jobs", { type: "job", dedupe_key: "one", resource: { uri: "job://one" } });
    first.tick();
    expect(first.listBatches()[0]).toMatchObject({ deadline: "2026-08-30T12:01:00.000Z", claim_ids: expect.any(Array) });

    clock.advance(30_000);
    first.emit("jobs", { type: "job", dedupe_key: "two", resource: { uri: "job://two" } });
    first.emit("jobs", { type: "job", dedupe_key: "three", resource: { uri: "job://three" } });
    first.tick();
    const frozen = first.listBatches();
    expect(frozen).toHaveLength(2);
    expect(frozen[0]).toMatchObject({ deadline: "2026-08-30T12:01:00.000Z" });
    expect(frozen[0].claim_ids).toHaveLength(2);
    expect(frozen[1]).toMatchObject({ deadline: "2026-08-30T12:01:30.000Z" });
    expect(frozen[1].claim_ids).toHaveLength(1);
    first.close();

    const restarted = bridge(path, clock);
    const endpoint = restarted.registerEndpoint({ host_kind: "mock", session_ref: "batch" });
    restarted.takeover("default", endpoint.id, 0);
    clock.advance(30_000);
    expect(await restarted.dispatchDue()).toEqual(expect.arrayContaining([expect.objectContaining({ batch_id: frozen[0].id, status: "accepted" })]));
    expect(restarted.mockTransport.calls).toHaveLength(1);
    clock.advance(30_000);
    expect(await restarted.dispatchDue()).toEqual(expect.arrayContaining([expect.objectContaining({ batch_id: frozen[1].id, status: "accepted" })]));
    expect(restarted.mockTransport.calls).toHaveLength(2);
    restarted.close();
  });

  it("coalesces twenty event references without loss and splits the twenty-first", () => {
    const current = bridge();
    try {
      current.installPolicies([policy("twenty", 1, "immediate", { source: "jobs" }, {
        batch: { coalesce_by: "source", max_events: 20, window_ms: 60_000 },
      })]);
      for (let index = 1; index <= 20; index += 1) {
        current.emit("jobs", { type: "job", dedupe_key: `job-${index}`, resource: { uri: `job://${index}` } });
      }
      current.tick();
      expect(current.listBatches()).toHaveLength(1);
      expect(current.listBatches()[0].claim_ids).toHaveLength(20);
      expect(current.listBatches()[0].event_ids).toHaveLength(20);
      expect(new Set(current.listBatches()[0].event_ids).size).toBe(20);

      current.emit("jobs", { type: "job", dedupe_key: "job-21", resource: { uri: "job://21" } });
      current.tick();
      expect(current.listBatches()).toHaveLength(2);
      expect(current.listBatches()[1].event_ids).toHaveLength(1);
    } finally {
      current.close();
    }
  });
});
