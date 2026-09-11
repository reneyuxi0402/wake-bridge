import { execFileSync } from "node:child_process";
import { chmodSync, mkdtempSync, rmSync } from "node:fs";
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
      chmodSync(path, 0o444);
      chmodSync(dir, 0o555);
      const reopened = new SqliteDatabase(path);
      expect(reopened.query<{ user_version: number }>("PRAGMA user_version;")).toEqual([{ user_version: 8 }]);
      reopened.close();
    } finally {
      chmodSync(dir, 0o700);
      chmodSync(path, 0o600);
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
      expect(bridge.db.query<{ user_version: number }>("PRAGMA user_version;")[0].user_version).toBe(8);
      expect(bridge.db.query<{ version: number }>("SELECT version FROM schema_migrations WHERE version=7;")).toEqual([{ version: 7 }]);
      expect(bridge.db.query<{ version: number }>("SELECT version FROM schema_migrations WHERE version=8;")).toEqual([{ version: 8 }]);
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
});
