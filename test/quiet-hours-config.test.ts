import { existsSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { BridgeError, WakeBridge } from "../src/core.js";
import { bridgeConfigFromFile } from "../src/instance-config.js";

function instanceFile(extra: Record<string, unknown> = {}) {
  const directory = mkdtempSync(join(tmpdir(), "wake-bridge-quiet-config-"));
  const path = join(directory, "wakebridge.config.json");
  const dbPath = join(directory, "bridge.sqlite");
  writeFileSync(path, JSON.stringify({
    version: 1,
    instance_id: "quiet-test",
    owner_id: "owner",
    db_path: dbPath,
    timezone: "UTC",
    admin_token: "a".repeat(32),
    ...extra,
  }), { mode: 0o600 });
  return { path, dbPath };
}

const event = { type: "test", dedupe_key: "quiet-test", resource: { uri: "test://quiet" } };
const windows = [{ start: "22:00", end: "08:00" }];

describe("instance-file quiet hours", () => {
  it.each([
    { label: "omitted", extra: {}, expected: undefined },
    { label: "null", extra: { quiet_hours: null }, expected: null },
    { label: "empty", extra: { quiet_hours: { windows: [] } }, expected: { windows: [] } },
  ])("preserves $label without enabling a gate", ({ extra, expected }) => {
    const { path } = instanceFile(extra);
    const config = bridgeConfigFromFile(path);
    expect(config.quiet_hours).toEqual(expected);
    if (expected === undefined) expect(config).not.toHaveProperty("quiet_hours");
    const bridge = new WakeBridge(config, { clock: () => new Date("2026-10-20T23:00:00Z") });
    try {
      expect(bridge.previewPolicy("test", event).gates.quiet_hours.active).toBe(false);
    } finally {
      bridge.close();
    }
  });

  it.each([
    ["2026-10-20T21:59:59Z", false, null],
    ["2026-10-20T22:00:00Z", true, "2026-10-21T08:00:00.000Z"],
    ["2026-10-21T00:00:00Z", true, "2026-10-21T08:00:00.000Z"],
    ["2026-10-21T07:59:59Z", true, "2026-10-21T08:00:00.000Z"],
    ["2026-10-21T08:00:00Z", false, null],
  ])("uses the top-level timezone for a cross-midnight window at %s", (now, active, until) => {
    const { path } = instanceFile({ quiet_hours: { windows } });
    const bridge = new WakeBridge(bridgeConfigFromFile(path), { clock: () => new Date(now) });
    try {
      expect(bridge.previewPolicy("test", event).gates.quiet_hours).toEqual({ action: "defer", active, defer_until: until });
    } finally {
      bridge.close();
    }
  });

  it("uses the quiet timezone override for real claims without changing scheduled-policy timezone", () => {
    const quietHours = { timezone: "Asia/Shanghai", windows };
    const { path } = instanceFile({ quiet_hours: quietHours });
    const config = bridgeConfigFromFile(path);
    expect(config.quiet_hours).toEqual(quietHours);
    const bridge = new WakeBridge(config, { clock: () => new Date("2026-10-20T14:00:00Z") });
    try {
      expect(bridge.config.timezone).toBe("UTC");
      expect(bridge.previewPolicy("test", event).gates.quiet_hours).toMatchObject({
        active: true, defer_until: "2026-10-21T00:00:00.000Z",
      });
      const result = bridge.emit("test", event);
      bridge.tick();
      expect(bridge.getClaim(result.claim!.id)).toMatchObject({
        eligible_after: "2026-10-21T00:00:00.000Z",
      });
      bridge.installPolicies([{
        id: "scheduled", version: 1, order: 100, match: { source: "scheduled" },
        delivery: { mode: "scheduled", scheduled_local_time: "15:00" },
      }]);
      expect(bridge.previewPolicy("scheduled", event).claim?.eligible_after).toBe("2026-10-20T15:00:00.000Z");
    } finally {
      bridge.close();
    }
  });

  it.each([
    [false, "invalid_quiet_hours"],
    [true, "invalid_quiet_hours"],
    [0, "invalid_quiet_hours"],
    [1, "invalid_quiet_hours"],
    ["", "invalid_quiet_hours"],
    ["quiet", "invalid_quiet_hours"],
    [[], "invalid_quiet_hours"],
    [{}, "invalid_quiet_hours"],
    [{ windows: null }, "invalid_quiet_hours"],
    [{ windows: "22:00" }, "invalid_quiet_hours"],
    [{ windows: [null] }, "invalid_quiet_hours"],
    [{ windows: [{ start: "24:00", end: "08:00" }] }, "invalid_quiet_hours"],
    [{ windows: [{ start: "22:00" }] }, "invalid_quiet_hours"],
    [{ windows: [{ start: "00:00", end: "00:00" }] }, "invalid_quiet_hours"],
    [{ windows: [{ start: "00:00", end: "12:00" }, { start: "12:00", end: "00:00" }] }, "invalid_quiet_hours"],
    [{ timezone: "Mars/Olympus_Mons", windows }, "invalid_timezone"],
    [{ timezone: "", windows }, "invalid_timezone"],
    [{ timezone: false, windows }, "invalid_timezone"],
    [{ timezone: null, windows }, "invalid_timezone"],
    [{ windows: [], resume_spread_ms: 1000 }, "unsupported_config_field"],
    [{ windows: [], default_action: "defer" }, "unsupported_config_field"],
  ])("rejects invalid file quiet_hours %j before creating a database", (quietHours, code) => {
    const { path, dbPath } = instanceFile({ quiet_hours: quietHours });
    expect(() => new WakeBridge(bridgeConfigFromFile(path)))
      .toThrowError(expect.objectContaining<Partial<BridgeError>>({ name: "BridgeError", code }));
    expect(existsSync(dbPath)).toBe(false);
    expect(existsSync(`${dbPath}-wal`)).toBe(false);
    expect(existsSync(`${dbPath}-shm`)).toBe(false);
  });
});
