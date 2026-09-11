import { execFileSync, spawnSync } from "node:child_process";
import { chmodSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { initializeInstance } from "../src/instance-config.js";
import {
  backupInstance,
  databaseStatus,
  installLinuxSystemdUserService,
  installMacLaunchAgent,
  releasePreflight,
  restoreInstance,
  uninstallLinuxSystemdUserService,
  uninstallMacLaunchAgent,
  upgradeInstance,
} from "../src/release-lifecycle.js";

function fixture(): { root: string; data: string; config: string } {
  const root = mkdtempSync(join(tmpdir(), "wake-bridge-release-"));
  const data = join(root, "data");
  const initialized = initializeInstance({ data_dir: data, instance_id: "release-test", owner_id: "owner", timezone: "UTC" });
  return { root, data, config: initialized.config_path };
}

function open(configPath: string): WakeBridge {
  const config = JSON.parse(readFileSync(configPath, "utf8")) as {
    instance_id: string; owner_id: string; db_path: string; timezone: string; admin_token: string;
  };
  return new WakeBridge(config);
}

describe("release lifecycle", () => {
  it("creates a verified online backup and restores it with an automatic rollback snapshot", () => {
    const current = fixture();
    try {
      const before = open(current.config);
      before.emit("fixture", { type: "before", dedupe_key: "before", resource: { uri: "fixture://before" } });
      before.close();

      const backupDirectory = join(current.root, "backup-before");
      const backup = backupInstance(current.config, backupDirectory);
      expect(backup).toMatchObject({ instance_id: "release-test", source_schema_version: 8 });
      expect(statSync(join(backupDirectory, "wake-bridge.sqlite")).mode & 0o077).toBe(0);

      const after = open(current.config);
      after.emit("fixture", { type: "after", dedupe_key: "after", resource: { uri: "fixture://after" } });
      expect(after.listEvents()).toHaveLength(2);
      after.close();

      expect(() => restoreInstance({
        config_path: current.config,
        backup_directory: backupDirectory,
        rollback_output: join(current.root, "not-created"),
        confirm_instance_id: "release-test",
        confirm_offline: false,
      })).toThrowError(/confirm-offline/u);

      const rollbackOutput = join(current.root, "rollback-after");
      expect(restoreInstance({
        config_path: current.config,
        backup_directory: backupDirectory,
        rollback_output: rollbackOutput,
        confirm_instance_id: "release-test",
        confirm_offline: true,
      })).toMatchObject({ restored: true, schema_version: 8, rollback_backup: rollbackOutput });

      const restored = open(current.config);
      expect(restored.listEvents().map((event) => event.type)).toEqual(["before"]);
      restored.close();
      expect(execFileSync("sqlite3", [join(rollbackOutput, "wake-bridge.sqlite"), "SELECT count(*) FROM events;"], { encoding: "utf8" }).trim()).toBe("2");
    } finally {
      rmSync(current.root, { recursive: true, force: true });
    }
  });

  it("gates schema migration behind upgrade, preserves the pre-upgrade schema, and supports rollback", () => {
    const current = fixture();
    try {
      const config = JSON.parse(readFileSync(current.config, "utf8")) as { db_path: string; instance_id: string; owner_id: string; timezone: string; admin_token: string };
      execFileSync("sqlite3", [config.db_path], {
        input: "DELETE FROM schema_migrations WHERE version > 6; PRAGMA user_version=6;\n",
        encoding: "utf8",
      });
      expect(databaseStatus(config.db_path)).toMatchObject({ schema_version: 6, integrity: "ok" });
      expect(() => new WakeBridge(config)).toThrowError(/requires explicit upgrade/u);

      const upgradeBackup = join(current.root, "pre-upgrade");
      expect(() => upgradeInstance(current.config, upgradeBackup)).toThrowError(/confirm-offline/u);
      expect(upgradeInstance(current.config, upgradeBackup, true)).toEqual({
        upgraded: true,
        from_schema: 6,
        to_schema: 8,
        backup_directory: upgradeBackup,
      });
      expect(databaseStatus(join(upgradeBackup, "wake-bridge.sqlite"))).toMatchObject({ schema_version: 6, integrity: "ok" });
      expect(databaseStatus(config.db_path)).toMatchObject({ schema_version: 8, integrity: "ok" });

      restoreInstance({
        config_path: current.config,
        backup_directory: upgradeBackup,
        rollback_output: join(current.root, "pre-rollback-current"),
        confirm_instance_id: "release-test",
        confirm_offline: true,
      });
      expect(databaseStatus(config.db_path)).toMatchObject({ schema_version: 6, integrity: "ok" });
      expect(() => new WakeBridge(config)).toThrowError(/requires explicit upgrade/u);
    } finally {
      rmSync(current.root, { recursive: true, force: true });
    }
  });

  it("automatically restores the verified backup when migration fails after it starts", () => {
    const current = fixture();
    try {
      const config = JSON.parse(readFileSync(current.config, "utf8")) as { db_path: string };
      execFileSync("sqlite3", [config.db_path], {
        input: "ALTER TABLE endpoints ADD COLUMN cold_routes_json TEXT; DELETE FROM schema_migrations WHERE version > 6; PRAGMA user_version=6;\n",
        encoding: "utf8",
      });
      const failedBackup = join(current.root, "failed-upgrade-backup");
      expect(() => upgradeInstance(current.config, failedBackup, true)).toThrowError(/ambiguous route migration/u);
      expect(databaseStatus(config.db_path)).toMatchObject({ schema_version: 6, integrity: "ok" });
      const columns = execFileSync("sqlite3", [config.db_path, "SELECT name FROM pragma_table_info('endpoints') ORDER BY name;"], { encoding: "utf8" });
      expect(columns).toContain("cold_routes_json");
      expect(columns).toContain("routes_json");
      expect(databaseStatus(join(failedBackup, "wake-bridge.sqlite"))).toMatchObject({ schema_version: 6, integrity: "ok" });
    } finally {
      rmSync(current.root, { recursive: true, force: true });
    }
  });

  it("reports runtime/schema readiness and installs a secret-free macOS LaunchAgent profile", () => {
    const current = fixture();
    try {
      const preflight = releasePreflight(current.config, { platform: "darwin", architecture: "arm64" });
      expect(preflight).toMatchObject({
        ok: true,
        release: { platform: "darwin", architecture: "arm64", platform_supported: true, architecture_supported: true },
        runtime: { node: { supported: true }, sqlite3: { supported: true, json_output: true } },
        instance: { schema_state: "current", integrity: "ok", security_ready: true },
      });

      const launchAgents = join(current.root, "LaunchAgents");
      const service = installMacLaunchAgent({
        config_path: current.config,
        launch_agents_directory: launchAgents,
        logs_directory: join(current.root, "logs"),
        cli_path: resolve("src/cli.ts"),
        port: 54321,
      }, { platform: "darwin", architecture: "arm64" });
      const plist = readFileSync(service.plist_path, "utf8");
      const config = JSON.parse(readFileSync(current.config, "utf8")) as { admin_token: string };
      expect(plist).toContain("<string>127.0.0.1</string>");
      expect(plist).toContain("<string>54321</string>");
      expect(plist).toContain(resolve(current.config));
      expect(plist).not.toContain(config.admin_token);
      expect(statSync(service.plist_path).mode & 0o077).toBe(0);
      expect(uninstallMacLaunchAgent({ config_path: current.config, launch_agents_directory: launchAgents }))
        .toMatchObject({ uninstalled: true, removed: true, data_preserved: true });
      expect(databaseStatus(JSON.parse(readFileSync(current.config, "utf8")).db_path).integrity).toBe("ok");
    } finally {
      chmodSync(current.root, 0o700);
      rmSync(current.root, { recursive: true, force: true });
    }
  });

  it("reports Linux readiness and installs a secret-free systemd user service profile", () => {
    const current = fixture();
    try {
      const preflight = releasePreflight(current.config, { platform: "linux", architecture: "x64" });
      expect(preflight).toMatchObject({
        ok: true,
        release: { platform: "linux", architecture: "x64", platform_supported: true, architecture_supported: true },
        runtime: { node: { supported: true }, sqlite3: { supported: true, json_output: true } },
        instance: { schema_state: "current", integrity: "ok", security_ready: true },
      });

      const systemdUserDirectory = join(current.root, "systemd", "user");
      const environmentFile = join(current.root, "host env $%.conf");
      const hostAdaptersPath = join(current.root, "host adapters $%.json");
      const cliPath = join(current.root, "wake bridge $% cli.js");
      writeFileSync(environmentFile, "WAKEBRIDGE_TEST_HOST_TOKEN=not-the-owner-token\n", { mode: 0o600 });
      writeFileSync(hostAdaptersPath, "{}\n", { mode: 0o600 });
      writeFileSync(cliPath, "// fixture\n", { mode: 0o700 });
      const service = installLinuxSystemdUserService({
        config_path: current.config,
        systemd_user_directory: systemdUserDirectory,
        environment_file: environmentFile,
        host_adapters_path: hostAdaptersPath,
        cli_path: cliPath,
        port: 54322,
      }, { platform: "linux", architecture: "x64" });
      const unit = readFileSync(service.unit_path, "utf8");
      const config = JSON.parse(readFileSync(current.config, "utf8")) as { admin_token: string; db_path: string };
      expect(service).toMatchObject({
        profile: "systemd_user",
        unit_name: "io.wakebridge.release-test.service",
        daemon_reload_command: "systemctl --user daemon-reload",
        enable_command: "systemctl --user enable io.wakebridge.release-test.service",
        start_command: "systemctl --user start io.wakebridge.release-test.service",
      });
      expect(unit).toContain("Type=exec");
      expect(unit).toContain("UMask=0077");
      expect(unit).toContain("Restart=on-failure");
      expect(unit).toContain('"--host" "127.0.0.1"');
      expect(unit).toContain('"--port" "54322"');
      expect(unit).toContain("EnvironmentFile=");
      expect(unit).not.toContain('EnvironmentFile="');
      expect(unit).toContain("host\\x20env\\x20\\x24%%.conf");
      expect(unit).toContain("wake bridge $$%% cli.js");
      expect(unit).toContain('"--host-adapters"');
      expect(unit).toContain("host adapters $$%%.json");
      expect(unit).toContain(resolve(current.config));
      expect(unit).not.toContain(config.admin_token);
      expect(statSync(service.unit_path).mode & 0o077).toBe(0);
      if (process.platform === "linux") {
        const verified = spawnSync("systemd-analyze", ["--user", "verify", service.unit_path], { encoding: "utf8" });
        expect(verified.status, verified.stderr || verified.stdout).toBe(0);
      }
      expect(() => installLinuxSystemdUserService({
        config_path: current.config,
        systemd_user_directory: systemdUserDirectory,
        cli_path: cliPath,
      }, { platform: "linux", architecture: "x64" })).toThrowError(/already exists/u);
      expect(uninstallLinuxSystemdUserService({
        config_path: current.config,
        systemd_user_directory: systemdUserDirectory,
      }, { platform: "linux" })).toMatchObject({
        uninstalled: true,
        profile: "systemd_user",
        removed: true,
        data_preserved: true,
      });
      expect(databaseStatus(config.db_path).integrity).toBe("ok");
      chmodSync(environmentFile, 0o644);
      expect(() => installLinuxSystemdUserService({
        config_path: current.config,
        systemd_user_directory: systemdUserDirectory,
        environment_file: environmentFile,
        cli_path: cliPath,
      }, { platform: "linux", architecture: "x64" })).toThrowError(/must not be accessible by group or other users/u);
    } finally {
      rmSync(current.root, { recursive: true, force: true });
    }
  });
});
