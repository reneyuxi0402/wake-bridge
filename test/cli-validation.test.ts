import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";

const CLI = [resolve("dist/src/cli.js")];
const cleanEnv = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("WAKEBRIDGE_")));

function run(directory: string, args: string[]) {
  return spawnSync(process.execPath, [...CLI, ...args], {
    cwd: directory,
    env: { ...cleanEnv, WAKEBRIDGE_DB: join(directory, "configured.sqlite"), WAKEBRIDGE_INSTANCE_ID: "cli-validation", WAKEBRIDGE_OWNER_ID: "owner" },
    encoding: "utf8", timeout: 10_000,
  });
}

describe("CLI argument validation before side effects", () => {
  it.each([
    ["events", "--confg", "private.json"],
    ["events", "--__proto__", "ignored"],
    ["events", "--config"],
    ["events", "--config", ""],
    ["events", "--config", "--limit", "1"],
    ["events", "--db"],
    ["events", "--db", "one.sqlite", "--db", "two.sqlite"],
    ["claims", "--limit"],
    ["events", "--limit", "NaN"],
    ["events", "--limit", "0"],
    ["events", "--limit", "1.5"],
    ["events", "--limit", "100001"],
    ["claims", "--state", "not-a-state"],
    ["events", "--state", "pending"],
    ["batches", "--states", "pending,not-a-state"],
    ["batches", "--state", "pending", "--states", "pending"],
    ["events", "--after", "yesterday"],
    ["emit", "--metadata"],
    ["events", "unrelated"],
    ["policy", "typo"],
    ["service", "typo"],
    ["policy", "list", "unrelated"],
    ["service", "install", "--config"],
    ["emit", "--apply"],
    ["bindings", "--limit", "1"],
    ["prune", "--older-than-days", "30", "--apply", "true"],
    ["claim-dismiss"],
    ["claim-dismiss", "one", "two"],
    ["receipts", "one", "two"],
    ["endpoint-register", "--cold-routes", "[]"],
  ])("rejects invalid arguments without creating any files: %j", (...args) => {
    const directory = mkdtempSync(join(tmpdir(), "wake-bridge-cli-arguments-"));
    try {
      const result = run(directory, args);
      expect(result.status, result.stderr).toBe(1);
      expect(result.stderr).not.toBe("");
      expect(readdirSync(directory)).toEqual([]);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it.each([
    "init", "owner-token-rotate", "release-preflight", "backup", "upgrade", "restore", "service", "doctor",
    "connector-catalog", "source-validate", "botlingknows-connector", "gmail-connector", "group-chat-connector",
    "daemon", "mcp", "source-bootstrap", "source-once", "policy", "emit", "claim-schedule", "claim-snooze",
    "claim-dismiss", "claim-consume", "endpoint-register", "endpoint-renew", "takeover", "presence-renew", "tick",
    "dispatch", "inspect", "status", "batch-retry", "prune", "events", "claims", "batches", "receipts", "bindings", "endpoints",
  ])("%s --help succeeds without files, credentials, servers, or stdin", (command) => {
    const directory = mkdtempSync(join(tmpdir(), "wake-bridge-cli-help-"));
    try {
      const result = run(directory, [command, "--help"]);
      expect(result.status, result.stderr).toBe(0);
      expect(JSON.parse(result.stdout).commands).toContain("tick");
      expect(readdirSync(directory)).toEqual([]);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("doctor reports a nonzero status for an unready instance without creating it", () => {
    const directory = mkdtempSync(join(tmpdir(), "wake-bridge-cli-doctor-exit-"));
    try {
      const result = run(directory, ["doctor"]);
      expect(result.status, result.stderr).toBe(2);
      expect(JSON.parse(result.stdout)).toMatchObject({ ok: false, security: { database: { exists: false } } });
      expect(readdirSync(directory)).toEqual([]);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("keeps -- as the boundary for a required positional argument", () => {
    const directory = mkdtempSync(join(tmpdir(), "wake-bridge-cli-separator-"));
    const bridge = new WakeBridge({ instance_id: "cli-validation", owner_id: "owner", db_path: join(directory, "configured.sqlite") });
    try {
      const claim = bridge.scheduleClaim({ resource: { uri: "test://separator" }, eligible_after: "2099-01-01T00:00:00Z" }).claim;
      const result = run(directory, ["claim-dismiss", "--reason", "separator test", "--", claim.id]);
      expect(result.status, result.stderr).toBe(0);
      expect(JSON.parse(result.stdout)).toMatchObject({ id: claim.id, state: "dismissed", dismissed_reason: "separator test" });
    } finally {
      bridge.close();
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("applies list filters and limits exactly where Core applies them", async () => {
    const directory = mkdtempSync(join(tmpdir(), "wake-bridge-cli-filters-"));
    let now = new Date("2026-01-01T00:00:00Z");
    const bridge = new WakeBridge({ instance_id: "cli-validation", owner_id: "owner", db_path: join(directory, "configured.sqlite") }, { clock: () => now });
    try {
      const endpoint = bridge.registerEndpoint({ host_kind: "mock", session_ref: "filters" });
      bridge.takeover("red", endpoint.id, 0);
      bridge.takeover("blue", endpoint.id, 0);
      for (let index = 0; index < 4; index += 1) {
        now = new Date(now.getTime() + 1000);
        bridge.emit(index % 2 ? "beta" : "alpha", {
          type: "test", dedupe_key: `filter-${index}`, resource: { uri: `test://filters/${index}` },
          attention_channel_hint: index % 2 ? "blue" : "red",
        });
      }
      bridge.tick();
      await bridge.dispatchDue();
      bridge.scheduleClaim({ resource: { uri: "test://pending" }, eligible_after: "2099-01-01T00:00:00Z", attention_channel: "red" });
      const batchId = bridge.listBatches()[0].id;
      const cases: Array<{ args: string[]; expected: Array<{ id: string }> }> = [
        { args: ["events", "--limit", "2"], expected: bridge.listEvents({ limit: 2, newest_first: true }) },
        { args: ["events", "--state", "batched", "--source", "alpha", "--limit", "1"], expected: bridge.listEvents({ state: "batched", source: "alpha", limit: 1, newest_first: true }) },
        { args: ["events", "--after", "2026-01-01T00:00:01Z", "--limit", "2"], expected: bridge.listEvents({ after: "2026-01-01T00:00:01Z", limit: 2 }) },
        { args: ["claims", "--state", "batched", "--channel", "blue", "--source", "beta", "--limit", "1"], expected: bridge.listClaims({ state: "batched", channel: "blue", source: "beta", limit: 1, newest_first: true }) },
        { args: ["claims", "--states", "pending,batched", "--limit", "2"], expected: bridge.listClaims({ states: ["pending", "batched"], limit: 2, newest_first: true }) },
        { args: ["claims", "--state", "dismissed"], expected: [] },
        { args: ["batches", "--state", "dispatched", "--channel", "red", "--limit", "1"], expected: bridge.listBatches({ states: ["dispatched"], channel: "red", limit: 1, newest_first: true }) },
        { args: ["batches", "--states", "dispatched,pending", "--limit", "1"], expected: bridge.listBatches({ states: ["dispatched", "pending"], limit: 1, newest_first: true }) },
        { args: ["batches", "--state", "dead_letter"], expected: [] },
        { args: ["receipts", "--limit", "1"], expected: bridge.listReceipts(undefined, { limit: 1, newest_first: true }) },
        { args: ["receipts", batchId, "--limit", "1"], expected: bridge.listReceipts(batchId, { limit: 1, newest_first: true }) },
      ];
      expect(bridge.listReceipts().length).toBeGreaterThan(1);
      for (const { args, expected } of cases) {
        const result = run(directory, args);
        expect(result.status, `${args.join(" ")}: ${result.stderr}`).toBe(0);
        expect(JSON.parse(result.stdout).map((row: { id: string }) => row.id), args.join(" ")).toEqual(expected.map((row) => row.id));
      }
      expect(existsSync(join(directory, "wake-bridge.sqlite"))).toBe(false);
    } finally {
      bridge.close();
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
