import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { BridgeError, WakeBridge } from "../src/core.js";
import { startDaemon } from "../src/daemon.js";
import { validatePolicyFile } from "../src/policy-control.js";
import type { PolicyFile, PolicyRule } from "../src/types.js";

class TestClock {
  constructor(readonly value: Date) {}
  now = (): Date => new Date(this.value.getTime());
}

function temporary(name = "bridge.sqlite"): string {
  return join(mkdtempSync(join(tmpdir(), "wake-bridge-policy-")), name);
}

function createBridge(path = temporary(), clock?: TestClock, extra: Record<string, unknown> = {}): WakeBridge {
  return new WakeBridge({
    instance_id: "policy-test",
    owner_id: "owner",
    db_path: path,
    timezone: "UTC",
    ...extra,
  } as any, { clock: clock?.now });
}

function rule(id: string, version: number, mode: "immediate" | "scheduled" | "suppress", match: Record<string, unknown> = {}): PolicyRule {
  return {
    id,
    version,
    order: id === "default" ? -1_000_000 : 100,
    match,
    delivery: { mode, ...(mode === "scheduled" ? { scheduled_local_time: "13:00" } : {}) },
  };
}

function file(policies: PolicyRule[]): PolicyFile {
  return { schema_version: 1, policies };
}

function origin(address: AddressInfo | string | null): string {
  if (!address || typeof address === "string") throw new Error("expected TCP daemon address");
  return `http://127.0.0.1:${address.port}`;
}

describe("immutable personal policy control plane", () => {
  it("makes an installed version immutable and only lets the highest version of each id match", () => {
    const bridge = createBridge();
    try {
      const first = rule("jobs", 1, "suppress", { source: "jobs" });
      bridge.installPolicies([first]);
      bridge.installPolicies([first]);
      expect(bridge.listPolicies().filter((policy) => policy.id === "jobs")).toHaveLength(1);
      expect(() => bridge.installPolicies([{ ...first, delivery: { mode: "immediate" } }])).toThrowError(
        expect.objectContaining<Partial<BridgeError>>({ code: "policy_version_conflict", status: 409 }),
      );

      bridge.installPolicies([rule("jobs", 3, "suppress", { source: "other" })]);
      expect(() => bridge.installPolicies([rule("jobs", 2, "suppress", { source: "jobs" })])).toThrowError(
        expect.objectContaining<Partial<BridgeError>>({ code: "policy_version_regression", status: 409 }),
      );
      const result = bridge.emit("jobs", { type: "job.completed", dedupe_key: "active-only", resource: { uri: "job://active-only" } });
      expect(result.suppressed).toBe(false);
      expect(result.event.matched_policy_id).toBe("default");
      expect(bridge.listActivePolicies().find((policy) => policy.id === "jobs")?.version).toBe(3);

      bridge.installPolicies([{ ...rule("jobs", 4, "suppress", { source: "jobs" }), enabled: false }]);
      const disabled = bridge.emit("jobs", { type: "job.completed", dedupe_key: "disabled-no-fallback", resource: { uri: "job://disabled" } });
      expect(disabled.event.matched_policy_id).toBe("default");
      expect(bridge.listActivePolicies().find((policy) => policy.id === "jobs")).toMatchObject({ version: 4, enabled: false });
    } finally {
      bridge.close();
    }
  });

  it("keeps a claim pinned to its exact historical policy across a newer install and restart", () => {
    const path = temporary();
    const first = createBridge(path);
    first.installPolicies([rule("jobs", 1, "immediate", { source: "jobs" })]);
    const emitted = first.emit("jobs", { type: "job.completed", dedupe_key: "frozen", resource: { uri: "job://frozen" } });
    expect(emitted.claim).toMatchObject({ policy_id: "jobs", policy_version: 1 });
    first.installPolicies([rule("jobs", 2, "suppress", { source: "jobs" })]);
    first.close();

    const restarted = createBridge(path);
    try {
      restarted.tick();
      expect(restarted.listBatches()[0]).toMatchObject({ policy_id: "jobs", policy_version: 1 });
      expect(restarted.emit("jobs", { type: "job.completed", dedupe_key: "new", resource: { uri: "job://new" } }).suppressed).toBe(true);
    } finally {
      restarted.close();
    }
  });

  it("requires an explicit default and rejects scheduler modes and inert fields not yet in the public contract", () => {
    expect(() => validatePolicyFile(file([rule("jobs", 1, "immediate", { source: "jobs" })]))).toThrowError(
      expect.objectContaining<Partial<BridgeError>>({ code: "missing_default_policy" }),
    );
    expect(() => validatePolicyFile(file([{ ...rule("default", 2, "immediate"), enabled: false }]))).toThrowError(
      expect.objectContaining<Partial<BridgeError>>({ code: "missing_default_policy" }),
    );
    expect(() => validatePolicyFile({ schema_version: 1, policies: [{ ...rule("default", 2, "immediate"), delivery: { mode: "digest" } }] })).toThrowError(
      expect.objectContaining<Partial<BridgeError>>({ code: "unsupported_policy_mode" }),
    );
    expect(() => validatePolicyFile({ schema_version: 1, policies: [{ ...rule("default", 2, "immediate"), delivery: { mode: "immediate", max_delay_ms: 10 } }] })).toThrowError(
      expect.objectContaining<Partial<BridgeError>>({ code: "unsupported_policy_field" }),
    );
    expect(() => validatePolicyFile({ schema_version: 1, policies: [{ ...rule("default", 2, "immediate"), target: { prefer_transport: "warm" } }] })).toThrowError(
      expect.objectContaining<Partial<BridgeError>>({ code: "unsupported_policy_field" }),
    );
  });

  it("tests and previews candidate policy files without writing production state", () => {
    const clock = new TestClock(new Date("2026-08-30T12:34:00.000Z"));
    const bridge = createBridge(temporary(), clock, {
      quiet_hours: { timezone: "UTC", windows: [{ start: "12:00", end: "14:00" }] },
    });
    try {
      const candidate = validatePolicyFile(file([
        { ...rule("jobs", 1, "scheduled", { source: "jobs" }), batch: { coalesce_by: "source", max_events: 10, window_ms: 5000 } },
        rule("default", 2, "immediate"),
      ])).policies;
      const event = { type: "job.completed", dedupe_key: "preview", resource: { uri: "job://preview" } };
      const before = bridge.inspect();
      expect(bridge.testPolicy("jobs", event, candidate)).toMatchObject({ matched_policy: { id: "jobs", version: 1 } });
      expect(bridge.previewPolicy("jobs", event, candidate)).toMatchObject({
        evaluated_at: "2026-08-30T12:34:00.000Z",
        suppressed: false,
        claim: { eligible_after: "2026-08-30T13:00:00.000Z", attention_channel: "default" },
        gates: { quiet_hours: { active: true, defer_until: "2026-08-30T14:00:00.000Z" } },
        batch: { coalesce_by: "source", coalesce_key: "jobs", max_events: 10, window_ms: 5000 },
      });
      expect(bridge.inspect()).toEqual(before);

      const suppress = validatePolicyFile(file([rule("blocked", 1, "suppress", { source: "jobs" }), rule("default", 2, "immediate")])).policies;
      expect(bridge.previewPolicy("jobs", event, suppress)).toMatchObject({ suppressed: true, claim: null, batch: null });
      expect(bridge.inspect()).toEqual(before);
    } finally {
      bridge.close();
    }
  });

  it("exposes install/list/test/preview only through the authenticated owner API", async () => {
    const token = "owner-policy-token-0123456789abcdef";
    const config = { instance_id: "api", owner_id: "owner", db_path: temporary(), timezone: "UTC", admin_token: token };
    const daemon = await startDaemon(config, { host: "127.0.0.1", port: 0, scheduler_interval_ms: 0 });
    const base = origin(daemon.address);
    const headers = { authorization: `Bearer ${token}`, "content-type": "application/json" };
    try {
      expect((await fetch(`${base}/v1/policies`)).status).toBe(401);
      const policyFile = file([rule("jobs", 1, "suppress", { source: "jobs" }), rule("default", 2, "immediate")]);
      const installed = await fetch(`${base}/v1/policies/install`, { method: "POST", headers, body: JSON.stringify(policyFile) });
      expect(installed.status).toBe(200);
      await expect(installed.json()).resolves.toMatchObject({ ok: true, installed: [{ id: "jobs", version: 1 }, { id: "default", version: 2 }] });
      const event = { type: "job.completed", dedupe_key: "api-preview", resource: { uri: "job://api-preview" } };
      const listed = await fetch(`${base}/v1/policies`, { headers: { authorization: `Bearer ${token}` } });
      await expect(listed.json()).resolves.toMatchObject({
        active: expect.arrayContaining([expect.objectContaining({ id: "jobs", version: 1 })]),
        status: expect.arrayContaining([expect.objectContaining({ id: "jobs", version: 1, new_event_matching: "eligible" })]),
      });
      const tested = await fetch(`${base}/v1/policies/test`, { method: "POST", headers, body: JSON.stringify({ source: "jobs", event }) });
      await expect(tested.json()).resolves.toMatchObject({ matched_policy: { id: "jobs", version: 1 } });
      const previewed = await fetch(`${base}/v1/policies/preview`, { method: "POST", headers, body: JSON.stringify({ source: "jobs", event }) });
      await expect(previewed.json()).resolves.toMatchObject({ matched_policy: { id: "jobs" }, suppressed: true, claim: null });
      expect(daemon.bridge.listEvents()).toHaveLength(0);
    } finally {
      await daemon.close();
      daemon.bridge.close();
    }
  });

  it("supports the file-backed policy workflow through the CLI", () => {
    const dir = mkdtempSync(join(tmpdir(), "wake-bridge-policy-cli-"));
    const db = join(dir, "bridge.sqlite");
    const policyPath = join(dir, "policies.json");
    writeFileSync(policyPath, JSON.stringify(file([rule("jobs", 1, "suppress", { source: "jobs" }), rule("default", 2, "immediate")])));
    const env = { ...process.env, WAKEBRIDGE_DB: db, WAKEBRIDGE_INSTANCE_ID: "cli", WAKEBRIDGE_OWNER_ID: "owner" };
    const run = (...args: string[]) => spawnSync(process.execPath, ["dist/src/cli.js", "policy", ...args], { cwd: process.cwd(), env, encoding: "utf8" });
    const event = JSON.stringify({ type: "job.completed", dedupe_key: "cli-preview", resource: { uri: "job://cli-preview" } });

    const installed = run("install", "--file", policyPath);
    expect(installed.status, installed.stderr).toBe(0);
    expect(JSON.parse(installed.stdout)).toMatchObject({ ok: true });
    const listed = run("list");
    expect(listed.status, listed.stderr).toBe(0);
    expect(JSON.parse(listed.stdout).active).toEqual(expect.arrayContaining([expect.objectContaining({ id: "jobs", version: 1 })]));
    expect(JSON.parse(listed.stdout).status).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: "jobs", version: 1, new_event_matching: "eligible" }),
    ]));
    const tested = run("test", "--source", "jobs", "--event", event);
    expect(tested.status, tested.stderr).toBe(0);
    expect(JSON.parse(tested.stdout)).toMatchObject({ matched_policy: { id: "jobs", version: 1 } });
    const previewed = run("preview", "--source", "jobs", "--event", event);
    expect(previewed.status, previewed.stderr).toBe(0);
    expect(JSON.parse(previewed.stdout)).toMatchObject({ suppressed: true, claim: null });
  });
});
