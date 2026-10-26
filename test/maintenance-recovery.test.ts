import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { WakeBridge } from "../src/core.js";
import { initializeInstance } from "../src/instance-config.js";
import { backupInstance, databaseStatus, releasePreflight, restoreInstance } from "../src/release-lifecycle.js";
import { pruneHistory } from "../src/retention.js";

// Failure injection is limited to our temporary fixture. Never fill a real disk.
const faults = vi.hoisted(() => ({ manifestDirectory: "", partialCopyDirectory: "" }));
vi.mock("node:fs", async (importOriginal) => {
  const fs = await importOriginal<typeof import("node:fs")>();
  return {
    ...fs,
    writeFileSync: (...args: Parameters<typeof fs.writeFileSync>) => {
      if (faults.manifestDirectory && String(args[0]) === `${faults.manifestDirectory}/manifest.json`) {
        throw Object.assign(new Error("injected ENOSPC"), { code: "ENOSPC" });
      }
      return fs.writeFileSync(...args);
    },
    copyFileSync: (...args: Parameters<typeof fs.copyFileSync>) => {
      if (faults.partialCopyDirectory && String(args[1]).startsWith(`${faults.partialCopyDirectory}/`)
        && String(args[1]).endsWith(".restore")) {
        faults.partialCopyDirectory = "";
        fs.writeFileSync(args[1], "partial fixture copy", { mode: 0o600 });
        throw Object.assign(new Error("injected ENOSPC"), { code: "ENOSPC" });
      }
      return fs.copyFileSync(...args);
    },
  };
});

const roots: string[] = [];
afterEach(() => {
  faults.manifestDirectory = "";
  faults.partialCopyDirectory = "";
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "wakebridge-maintenance-"));
  roots.push(root);
  const initialized = initializeInstance({ data_dir: join(root, "original"), instance_id: "maintenance", owner_id: "fixture", timezone: "UTC" });
  const bridge = new WakeBridge(initialized.config);
  bridge.emit("fixture", { type: "original", dedupe_key: "original", resource: { uri: "fixture://original" } });
  bridge.close();
  const backup = join(root, "verified-backup");
  backupInstance(initialized.config_path, backup);
  return { root, backup, ...initialized };
}

function restore(f: ReturnType<typeof fixture>, rollback = join(f.root, "rollback")) {
  return restoreInstance({ config_path: f.config_path, backup_directory: f.backup, rollback_output: rollback,
    confirm_instance_id: f.config.instance_id, confirm_offline: true });
}

describe("maintenance and recovery failure boundaries", () => {
  it("rejects a malformed manifest without echoing its contents or changing the current database", () => {
    const f = fixture();
    const before = readFileSync(f.config.db_path);
    writeFileSync(join(f.backup, "manifest.json"), 'MANIFEST_PRIVATE_MARKER {');
    expect(() => restore(f)).toThrow(/^backup manifest is unreadable; check the file and JSON syntax$/u);
    expect(readFileSync(f.config.db_path)).toEqual(before);
    expect(existsSync(join(f.root, "rollback"))).toBe(false);
  });

  it("rejects a damaged backup before touching the current database", () => {
    const f = fixture();
    const before = readFileSync(f.config.db_path);
    writeFileSync(join(f.backup, "wake-bridge.sqlite"), "damaged backup");
    expect(() => restore(f)).toThrow(/checksum/u);
    expect(readFileSync(f.config.db_path)).toEqual(before);
    expect(existsSync(join(f.root, "rollback"))).toBe(false);
  });

  it("rejects corrupt backup contents even when the checksum has been recomputed", () => {
    const f = fixture();
    const before = readFileSync(f.config.db_path);
    const database = join(f.backup, "wake-bridge.sqlite");
    writeFileSync(database, "not a SQLite database");
    const manifestPath = join(f.backup, "manifest.json");
    const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
    manifest.database_sha256 = createHash("sha256").update(readFileSync(database)).digest("hex");
    writeFileSync(manifestPath, JSON.stringify(manifest));
    expect(() => restore(f)).toThrow();
    expect(readFileSync(f.config.db_path)).toEqual(before);
    expect(existsSync(join(f.root, "rollback"))).toBe(false);
  });

  it("does not leave a successful-looking backup after an injected manifest ENOSPC", () => {
    const f = fixture();
    const output = join(f.root, "incomplete-backup");
    faults.manifestDirectory = output;
    expect(() => backupInstance(f.config_path, output)).toThrow(/ENOSPC/u);
    expect(existsSync(output)).toBe(false);
    expect(databaseStatus(f.config.db_path).integrity).toBe("ok");
    expect(databaseStatus(join(f.backup, "wake-bridge.sqlite")).integrity).toBe("ok");
  });

  it("preserves the current database and removes a partial restore copy on ENOSPC", () => {
    const f = fixture();
    const bridge = new WakeBridge(f.config);
    bridge.emit("fixture", { type: "newer", dedupe_key: "newer", resource: { uri: "fixture://newer" } });
    bridge.close();
    faults.partialCopyDirectory = f.data_dir;
    expect(() => restore(f)).toThrow(/ENOSPC/u);
    const reopened = new WakeBridge(f.config);
    try { expect(reopened.listEvents().map((event) => event.type)).toEqual(["original", "newer"]); }
    finally { reopened.close(); }
    expect(readdirSync(f.data_dir).filter((name) => name.endsWith(".restore"))).toEqual([]);
    expect(databaseStatus(join(f.root, "rollback", "wake-bridge.sqlite")).integrity).toBe("ok");
  });

  it("preserves a corrupt original and proves recovery into an isolated healthy instance", () => {
    const f = fixture();
    writeFileSync(f.config.db_path, "corrupt original retained for diagnosis");
    const before = readFileSync(f.config.db_path);
    expect(releasePreflight(f.config_path)).toMatchObject({ ok: false, instance: { schema_state: "unreadable" } });
    expect(() => restore(f)).toThrow(); // Normal restore requires a verified current rollback snapshot.
    expect(readFileSync(f.config.db_path)).toEqual(before);
    const isolated = initializeInstance({ data_dir: join(f.root, "isolated-recovery"), instance_id: f.config.instance_id,
      owner_id: f.config.owner_id, timezone: "UTC" });
    restoreInstance({ config_path: isolated.config_path, backup_directory: f.backup, rollback_output: join(f.root, "isolated-empty-backup"),
      confirm_instance_id: f.config.instance_id, confirm_offline: true });
    const recovered = new WakeBridge(isolated.config);
    try { expect(recovered.listEvents().map((event) => event.type)).toEqual(["original"]); }
    finally { recovered.close(); }
    expect(readFileSync(f.config.db_path)).toEqual(before);
    expect(readFileSync(isolated.config_path, "utf8")).not.toContain(f.config.admin_token);
  });

  it("keeps event dedupe and unfinished work through prune, backup and restore", () => {
    const f = fixture();
    const bridge = new WakeBridge(f.config, { clock: () => new Date("2030-01-01T00:00:00Z") });
    const completed = bridge.emit("fixture", { type: "done", dedupe_key: "done", resource: { uri: "fixture://done" } });
    bridge.consumeClaim(completed.claim!.id);
    bridge.db.exec("UPDATE claims SET updated_at='2020-01-01T00:00:00.000Z' WHERE state='consumed';");
    expect(pruneHistory(bridge, { older_than_days: 30, apply: true }).deleted.claims).toBe(1);
    bridge.close();
    const retained = join(f.root, "after-prune");
    backupInstance(f.config_path, retained);
    const isolated = initializeInstance({ data_dir: join(f.root, "retention-drill"), instance_id: f.config.instance_id,
      owner_id: f.config.owner_id, timezone: "UTC" });
    restoreInstance({ config_path: isolated.config_path, backup_directory: retained, rollback_output: join(f.root, "retention-empty-backup"),
      confirm_instance_id: f.config.instance_id, confirm_offline: true });
    const recovered = new WakeBridge(isolated.config);
    try {
      expect(recovered.emit("fixture", { type: "done", dedupe_key: "done", resource: { uri: "fixture://done" } }))
        .toMatchObject({ duplicate: true, claim: undefined });
      expect(recovered.listClaims().some((claim) => claim.resource.uri === "fixture://original" && claim.state === "pending")).toBe(true);
    } finally { recovered.close(); }
  });
});
