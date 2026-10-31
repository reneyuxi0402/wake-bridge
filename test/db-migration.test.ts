import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { SqliteDatabase } from "../src/db.js";

describe("SQLite migrations", () => {
  it("opens a current database without replaying schema writes", () => {
    const dir = mkdtempSync(join(tmpdir(), "wake-bridge-current-schema-"));
    const path = join(dir, "bridge.sqlite");
    const bridge = new WakeBridge({ instance_id: "i", owner_id: "o", db_path: path, timezone: "UTC" }, {
      autoMockTransport: false,
      recoverDispatchLeases: false,
    });
    bridge.close();
    try {
      const before = readFileSync(path);
      const reopened = new SqliteDatabase(path);
      expect(reopened.query<{ user_version: number }>("PRAGMA user_version;")).toEqual([{ user_version: 9 }]);
      reopened.close();
      expect(readFileSync(path)).toEqual(before);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("requires explicit authorization before migrating v6 and then preserves endpoint routes", () => {
    const dir = mkdtempSync(join(tmpdir(), "wake-bridge-route-migration-"));
    const path = join(dir, "bridge.sqlite");
    const routes = [{
      kind: "legacy-test",
      priority: 10,
      address: { base_url: "http://127.0.0.1:18789/", token: "route-secret-0123456789abcdef0123456789" },
    }];
    execFileSync("sqlite3", [path], {
      input: `
CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
INSERT INTO schema_migrations(version, applied_at) VALUES(6, '2026-08-30T00:00:00.000Z');
CREATE TABLE endpoints (
  id TEXT PRIMARY KEY NOT NULL,
  instance_id TEXT NOT NULL,
  host_kind TEXT NOT NULL,
  session_ref TEXT NOT NULL,
  lease_token_hash TEXT NOT NULL,
  lease_expires_at TEXT NOT NULL,
  capabilities_json TEXT NOT NULL,
  cold_routes_json TEXT NOT NULL,
  registered_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
INSERT INTO endpoints VALUES(
  'ep_legacy', 'i', 'legacy-test', 'session-1', 'hash', '2099-01-01T00:00:00.000Z',
  '{"cold_push":false}', '${JSON.stringify(routes)}',
  '2026-08-30T00:00:00.000Z', '2026-08-30T00:00:00.000Z'
);
CREATE TABLE activity_watches (
  instance_id TEXT NOT NULL, attention_channel TEXT NOT NULL, enabled INTEGER NOT NULL, idle_after_ms INTEGER NOT NULL,
  endpoint_id TEXT, binding_generation INTEGER, epoch INTEGER NOT NULL, armed_at TEXT, last_nonwake_activity_at TEXT,
  activity_lease_expires_at TEXT, fired_epoch INTEGER, rescue_mode TEXT NOT NULL DEFAULT 'once',
  repeat_after_ms INTEGER NOT NULL DEFAULT 3600000, rescue_sequence INTEGER NOT NULL DEFAULT 0,
  last_rescue_at TEXT, next_rescue_at TEXT, covered_epoch INTEGER, coverage_claim_id TEXT,
  coverage_eligible_after TEXT, updated_at TEXT NOT NULL, PRIMARY KEY(instance_id, attention_channel)
);
INSERT INTO activity_watches VALUES(
  'i', 'life', 1, 60000, 'ep_legacy', 1, 3, '2026-08-30T00:00:00.000Z', '2026-08-30T00:00:00.000Z',
  NULL, 3, 'until_covered', 120000, 2, '2026-08-30T00:02:00.000Z', '2026-08-30T00:04:00.000Z',
  NULL, NULL, NULL, '2026-08-30T00:02:00.000Z'
);
PRAGMA user_version=6;
`,
      encoding: "utf8",
    });

    expect(() => new WakeBridge({ instance_id: "i", owner_id: "o", db_path: path, timezone: "UTC" }, {
      autoMockTransport: false,
      recoverDispatchLeases: false,
    })).toThrowError(/requires explicit upgrade/u);

    const bridge = new WakeBridge({ instance_id: "i", owner_id: "o", db_path: path, timezone: "UTC" }, {
      autoMockTransport: false,
      recoverDispatchLeases: false,
      allowSchemaUpgrade: true,
    });
    try {
      expect(bridge.getEndpoint("ep_legacy")?.routes).toEqual(routes);
      expect(bridge.db.query<{ name: string }>("PRAGMA table_info(endpoints);").map((row) => row.name))
        .toContain("routes_json");
      expect(bridge.db.query<{ name: string }>("PRAGMA table_info(endpoints);").map((row) => row.name))
        .not.toContain("cold_routes_json");
      expect(bridge.db.query<{ user_version: number }>("PRAGMA user_version;")[0].user_version).toBe(9);
      expect(bridge.db.query<{ version: number }>("SELECT version FROM schema_migrations WHERE version=7;")).toEqual([{ version: 7 }]);
      expect(bridge.db.query<{ version: number }>("SELECT version FROM schema_migrations WHERE version=8;")).toEqual([{ version: 8 }]);
      expect(bridge.db.query<{ version: number }>("SELECT version FROM schema_migrations WHERE version=9;")).toEqual([{ version: 9 }]);
      const watchColumns = bridge.db.query<{ name: string }>("PRAGMA table_info(activity_watches);").map((row) => row.name);
      expect(watchColumns).toContain("mode");
      expect(watchColumns).not.toContain("rescue_mode");
      expect(watchColumns).not.toContain("covered_epoch");
      expect(bridge.getActivityWatch("life")).toMatchObject({ mode: "once", sequence: 2, next_due_at: null });

      const created = bridge.registerEndpoint({
        host_kind: "mock",
        session_ref: "new-session",
        routes: [{ kind: "mock", address: {} }],
      });
      expect(bridge.getEndpoint(created.id)?.routes).toEqual([{ kind: "mock", address: {} }]);
      expect(bridge.db.query<{ integrity_check: string }>("PRAGMA integrity_check;")[0].integrity_check).toBe("ok");
    } finally {
      bridge.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("explicitly upgrades v8 channel references without counting ignored event hints or truncating an over-limit instance", () => {
    const dir = mkdtempSync(join(tmpdir(), "wake-bridge-channel-migration-"));
    const path = join(dir, "bridge.sqlite");
    const config = { instance_id: "i", owner_id: "o", db_path: path, timezone: "UTC", max_channels: 32 };
    const initial = new WakeBridge(config);
    initial.updatePolicy({ id: "fixed", version: 1, order: 10, match: { type: "routed" }, delivery: { mode: "immediate" }, target: { attention_channel: "actual-target" } });
    initial.updatePolicy({ id: "unused", version: 1, enabled: false, delivery: { mode: "suppress" }, target: { attention_channel: "policy-only" } });
    initial.updatePolicy({ id: "template", version: 1, enabled: false, delivery: { mode: "immediate" }, target: { attention_channel: "prefix-${attention_channel_hint}" } });
    const routed = initial.emit("fixture", { type: "routed", dedupe_key: "routed", attention_channel_hint: "ignored-hint", resource: { uri: "job://routed" } });
    initial.scheduleClaim({ resource: { uri: "job://claim" }, attention_channel: "claim-only", eligible_after: initial.nowIso() });
    initial.tick();
    const endpoint = initial.registerEndpoint({ id: "ep", host_kind: "mock", session_ref: "s" });
    initial.takeover("binding-only", endpoint.id);
    initial.configureActivityWatch({ attention_channel: "watch-only", enabled: false, idle_after_ms: 60_000 });
    initial.db.exec(`
      UPDATE batches SET attention_channel='batch-only' WHERE attention_channel='claim-only';
      INSERT INTO presence_leases VALUES('i','presence-only','ep',1,'2026-10-30T00:00:00.000Z','2026-10-30T00:01:00.000Z','fixture','fixture');
      INSERT INTO activity_observations VALUES('i','ep',1,'observation','observation-only','activity_started','2026-10-30T00:00:00.000Z','2026-10-30T00:00:00.000Z');
      INSERT INTO activity_observations VALUES('other','ep-other',1,'observation','other-channel','activity_started','2026-10-30T00:00:00.000Z','2026-10-30T00:00:00.000Z');
      DROP TABLE attention_channels;
      DELETE FROM schema_migrations WHERE version=9;
      PRAGMA user_version=8;
    `);
    initial.close();
    try {
      const before = readFileSync(path);
      expect(() => new SqliteDatabase(path)).toThrowError(/requires explicit upgrade to 9/u);
      expect(readFileSync(path)).toEqual(before);
      const upgraded = new WakeBridge({ ...config, max_channels: 1 }, { allowSchemaUpgrade: true, recoverDispatchLeases: false });
      try {
        expect(upgraded.listChannels().map((channel) => channel.attention_channel).sort()).toEqual([
          "actual-target", "batch-only", "binding-only", "claim-only", "observation-only", "policy-only", "presence-only", "watch-only",
        ]);
        expect(upgraded.getChannel("ignored-hint")).toBeNull();
        expect(upgraded.getChannel("default")).toBeNull();
        expect(upgraded.getChannel("prefix-${attention_channel_hint}")).toBeNull();
        expect(upgraded.getChannel("other-channel")).toBeNull();
        expect(upgraded.getEvent(routed.event.id)?.attention_channel_hint).toBe("ignored-hint");
        expect(upgraded.db.query<{ instance_id: string }>("SELECT DISTINCT instance_id FROM attention_channels ORDER BY instance_id;")).toEqual([{ instance_id: "i" }, { instance_id: "other" }]);
        expect(() => upgraded.createChannel({ name: "new" })).toThrowError(expect.objectContaining({ code: "channel_limit_reached", status: 429 }));
        expect(upgraded.renameChannel("claim-only", "Retained").name).toBe("Retained");
        expect(upgraded.emit("fixture", { type: "more", dedupe_key: "over-limit-existing", attention_channel_hint: "claim-only", resource: { uri: "job://more" } }).claim?.attention_channel).toBe("claim-only");
        expect(upgraded.deleteChannel("watch-only").deleted_at).not.toBeNull();
        expect(upgraded.listChannels()).toHaveLength(7);
        expect(upgraded.db.query<{ integrity_check: string }>("PRAGMA integrity_check;")[0].integrity_check).toBe("ok");
      } finally { upgraded.close(); }
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  it("uses the configured Space ID to migrate fixed policy targets before any runtime instance rows exist", () => {
    const dir = mkdtempSync(join(tmpdir(), "wake-bridge-policy-only-migration-"));
    const path = join(dir, "bridge.sqlite");
    const config = { instance_id: "policy-only-space", owner_id: "o", db_path: path, max_channels: 1 };
    const initial = new WakeBridge(config);
    initial.updatePolicy({ id: "fixed", version: 1, delivery: { mode: "immediate" }, target: { attention_channel: "fixed-channel" } });
    expect(initial.listEvents()).toEqual([]);
    expect(initial.listEndpoints()).toEqual([]);
    initial.db.exec("DROP TABLE attention_channels; DELETE FROM schema_migrations WHERE version=9; PRAGMA user_version=8;");
    initial.close();
    try {
      const upgraded = new WakeBridge(config, { allowSchemaUpgrade: true });
      try {
        expect(upgraded.listChannels()).toMatchObject([{ instance_id: "policy-only-space", attention_channel: "fixed-channel", name: "fixed-channel", deleted_at: null }]);
        expect(() => upgraded.createChannel({ name: "Another" })).toThrowError(expect.objectContaining({ code: "channel_limit_reached", status: 429 }));
        expect(() => upgraded.deleteChannel("fixed-channel")).toThrowError(expect.objectContaining({
          code: "channel_in_use", status: 409, usage: { binding: null, activity_watch: false, policies: [{ id: "fixed", version: 1 }] },
        }));
        upgraded.deleteChannel("fixed-channel", { force: true });
      } finally { upgraded.close(); }
      const restarted = new WakeBridge(config);
      try {
        expect(restarted.listChannels()).toEqual([]);
        expect(restarted.getChannel("fixed-channel")?.deleted_at).not.toBeNull();
      } finally { restarted.close(); }
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });
});
