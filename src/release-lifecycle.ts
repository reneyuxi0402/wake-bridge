import { createHash, randomBytes } from "node:crypto";
import { spawnSync } from "node:child_process";
import {
  chmodSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  realpathSync,
  renameSync,
  rmSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { WakeBridge } from "./core.js";
import { CURRENT_SCHEMA_VERSION, MIN_SUPPORTED_SCHEMA_VERSION } from "./db.js";
import { instanceConfigSecurity, loadInstanceConfig } from "./instance-config.js";
import { RELEASE_VERSION } from "./version.js";

export { RELEASE_VERSION } from "./version.js";
export const MINIMUM_NODE_MAJOR = 20;
export const MINIMUM_SQLITE_VERSION = "3.33.0";
export const SUPPORTED_PLATFORMS = ["darwin", "linux"] as const;
export const SUPPORTED_ARCHITECTURES = ["arm64", "x64"] as const;

interface ReleaseRuntime {
  platform?: NodeJS.Platform;
  architecture?: string;
}

interface BackupManifest {
  format_version: 1;
  package_version: string;
  instance_id: string;
  owner_id: string;
  created_at: string;
  source_schema_version: number;
  database_file: "wake-bridge.sqlite";
  database_sha256: string;
}

export interface ReleasePreflight {
  ok: boolean;
  release: {
    package_version: string;
    platform: string;
    architecture: string;
    platform_supported: boolean;
    architecture_supported: boolean;
  };
  runtime: {
    node: { installed_version: string; minimum_major: number; supported: boolean };
    sqlite3: { available: boolean; installed_version: string | null; minimum_version: string; supported: boolean; json_output: boolean };
  };
  instance: {
    configured: boolean;
    config_path: string | null;
    security_ready: boolean;
    schema_version: number | null;
    current_schema_version: number;
    minimum_upgrade_schema_version: number;
    schema_state: "not_configured" | "current" | "upgrade_required" | "too_old" | "too_new" | "unreadable";
    integrity: "ok" | "failed" | "not_checked";
    error: string | null;
  };
}

function versionParts(value: string): number[] {
  const match = /^(\d+)\.(\d+)\.(\d+)/u.exec(value.trim());
  return match ? match.slice(1).map(Number) : [];
}

function versionAtLeast(actual: string, minimum: string): boolean {
  const left = versionParts(actual);
  const right = versionParts(minimum);
  if (left.length !== 3 || right.length !== 3) return false;
  for (let index = 0; index < 3; index += 1) {
    if (left[index] !== right[index]) return left[index] > right[index];
  }
  return true;
}

function sqlite(args: string[], input?: string): string {
  const result = spawnSync("sqlite3", args, {
    input,
    encoding: "utf8",
    maxBuffer: 32 * 1024 * 1024,
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`sqlite3 failed (${result.status}): ${(result.stderr || result.stdout || "").trim()}`);
  return (result.stdout || "").trim();
}

function sqliteVersion(): { available: boolean; version: string | null; json_output: boolean } {
  try {
    const raw = sqlite(["--version"]);
    const version = raw.split(/\s+/u)[0] || null;
    let jsonOutput = false;
    try {
      const parsed = JSON.parse(sqlite(["-batch", "-json", ":memory:"], "SELECT 1 AS ok;\n")) as Array<{ ok?: number }>;
      jsonOutput = parsed[0]?.ok === 1;
    } catch {
      jsonOutput = false;
    }
    return { available: true, version, json_output: jsonOutput };
  } catch {
    return { available: false, version: null, json_output: false };
  }
}

function sqliteScalar(dbPath: string, sql: string, field: string): string {
  const output = sqlite(["-batch", "-json", dbPath], `${sql}\n`);
  const parsed = JSON.parse(output) as Array<Record<string, unknown>>;
  return String(parsed[0]?.[field] ?? "");
}

export function databaseStatus(dbPath: string): { schema_version: number; integrity: "ok" | "failed" } {
  if (!existsSync(dbPath)) throw new Error(`instance database does not exist: ${dbPath}`);
  const schemaVersion = Number(sqliteScalar(dbPath, "PRAGMA user_version;", "user_version"));
  const quickCheck = sqliteScalar(dbPath, "PRAGMA quick_check;", "quick_check");
  return { schema_version: schemaVersion, integrity: quickCheck === "ok" ? "ok" : "failed" };
}

function checkpointDatabase(dbPath: string): void {
  const output = sqlite(["-batch", "-json", dbPath], ".timeout 5000\nPRAGMA wal_checkpoint(TRUNCATE);\n");
  const row = (JSON.parse(output) as Array<{ busy?: number; log?: number; checkpointed?: number }>)[0];
  if (!row || row.busy !== 0 || row.log !== row.checkpointed) {
    throw new Error("database WAL checkpoint is busy; stop the daemon before this operation");
  }
}

function schemaState(version: number): ReleasePreflight["instance"]["schema_state"] {
  if (version === CURRENT_SCHEMA_VERSION) return "current";
  if (version > CURRENT_SCHEMA_VERSION) return "too_new";
  if (version >= MIN_SUPPORTED_SCHEMA_VERSION) return "upgrade_required";
  return "too_old";
}

export function releasePreflight(configPath?: string, runtime: ReleaseRuntime = {}): ReleasePreflight {
  const nodeMajor = Number(process.versions.node.split(".")[0]);
  const sqliteRuntime = sqliteVersion();
  const platform = runtime.platform ?? process.platform;
  const architecture = runtime.architecture ?? process.arch;
  const platformSupported = (SUPPORTED_PLATFORMS as readonly string[]).includes(platform);
  const architectureSupported = (SUPPORTED_ARCHITECTURES as readonly string[]).includes(architecture);
  const sqliteSupported = Boolean(sqliteRuntime.version)
    && versionAtLeast(sqliteRuntime.version!, MINIMUM_SQLITE_VERSION)
    && sqliteRuntime.json_output;
  const instance: ReleasePreflight["instance"] = {
    configured: Boolean(configPath),
    config_path: configPath ? resolve(configPath) : null,
    security_ready: false,
    schema_version: null,
    current_schema_version: CURRENT_SCHEMA_VERSION,
    minimum_upgrade_schema_version: MIN_SUPPORTED_SCHEMA_VERSION,
    schema_state: configPath ? "unreadable" : "not_configured",
    integrity: "not_checked",
    error: null,
  };
  if (configPath) {
    try {
      const security = instanceConfigSecurity(configPath);
      instance.security_ready = security.ready;
      if (!security.ready) throw new Error(security.error || "instance security preflight failed");
      const config = loadInstanceConfig(configPath);
      const status = databaseStatus(config.db_path);
      instance.schema_version = status.schema_version;
      instance.schema_state = schemaState(status.schema_version);
      instance.integrity = status.integrity;
    } catch (error) {
      instance.error = error instanceof Error ? error.message : String(error);
      instance.schema_state = "unreadable";
    }
  }
  const nodeSupported = Number.isSafeInteger(nodeMajor) && nodeMajor >= MINIMUM_NODE_MAJOR;
  return {
    ok: platformSupported && architectureSupported && nodeSupported && sqliteSupported
      && (!configPath || (instance.security_ready && instance.schema_state === "current" && instance.integrity === "ok")),
    release: {
      package_version: RELEASE_VERSION,
      platform,
      architecture,
      platform_supported: platformSupported,
      architecture_supported: architectureSupported,
    },
    runtime: {
      node: { installed_version: process.versions.node, minimum_major: MINIMUM_NODE_MAJOR, supported: nodeSupported },
      sqlite3: {
        available: sqliteRuntime.available,
        installed_version: sqliteRuntime.version,
        minimum_version: MINIMUM_SQLITE_VERSION,
        supported: sqliteSupported,
        json_output: sqliteRuntime.json_output,
      },
    },
    instance,
  };
}

function sha256(path: string): string {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function sqlString(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function readBackupManifest(backupDir: string): { manifest: BackupManifest; databasePath: string } {
  const directory = resolve(backupDir);
  const manifestPath = join(directory, "manifest.json");
  let manifest: BackupManifest;
  try {
    manifest = JSON.parse(readFileSync(manifestPath, "utf8")) as BackupManifest;
  } catch (error) {
    throw new Error(`backup manifest is unreadable: ${String(error)}`);
  }
  if (manifest?.format_version !== 1 || manifest.database_file !== "wake-bridge.sqlite"
    || typeof manifest.instance_id !== "string" || typeof manifest.database_sha256 !== "string"
    || !Number.isSafeInteger(manifest.source_schema_version)) {
    throw new Error("backup manifest is invalid");
  }
  const databasePath = join(directory, manifest.database_file);
  if (!existsSync(databasePath) || sha256(databasePath) !== manifest.database_sha256) {
    throw new Error("backup database checksum does not match manifest");
  }
  const status = databaseStatus(databasePath);
  if (status.integrity !== "ok" || status.schema_version !== manifest.source_schema_version) {
    throw new Error("backup database failed schema or integrity verification");
  }
  return { manifest, databasePath };
}

export function backupInstance(configPath: string, outputDirectory: string): BackupManifest & { backup_directory: string } {
  const config = loadInstanceConfig(configPath);
  const security = instanceConfigSecurity(configPath);
  if (!security.ready) throw new Error(security.error || "instance security preflight failed");
  const source = databaseStatus(config.db_path);
  if (source.integrity !== "ok") throw new Error("source database failed quick_check");
  if (source.schema_version < MIN_SUPPORTED_SCHEMA_VERSION || source.schema_version > CURRENT_SCHEMA_VERSION) {
    throw new Error(`database schema ${source.schema_version} is outside the backup compatibility range`);
  }
  const output = resolve(outputDirectory);
  if (existsSync(output)) throw new Error(`backup output already exists: ${output}`);
  mkdirSync(output, { mode: 0o700 });
  let complete = false;
  try {
    const databasePath = join(output, "wake-bridge.sqlite");
    sqlite(["-batch", config.db_path], `VACUUM INTO ${sqlString(databasePath)};\n`);
    chmodSync(databasePath, 0o600);
    if (databaseStatus(databasePath).integrity !== "ok") throw new Error("backup database failed quick_check");
    const manifest: BackupManifest = {
      format_version: 1,
      package_version: RELEASE_VERSION,
      instance_id: config.instance_id,
      owner_id: config.owner_id,
      created_at: new Date().toISOString(),
      source_schema_version: source.schema_version,
      database_file: "wake-bridge.sqlite",
      database_sha256: sha256(databasePath),
    };
    const manifestPath = join(output, "manifest.json");
    writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, { encoding: "utf8", mode: 0o600, flag: "wx" });
    chmodSync(manifestPath, 0o600);
    complete = true;
    return { ...manifest, backup_directory: output };
  } finally {
    if (!complete) rmSync(output, { recursive: true, force: true });
  }
}

function replaceDatabase(dbPath: string, sourcePath: string): void {
  const destination = resolve(dbPath);
  const temporary = join(dirname(destination), `.${basename(destination)}.${randomBytes(8).toString("hex")}.restore`);
  copyFileSync(sourcePath, temporary);
  let replaced = false;
  try {
    chmodSync(temporary, 0o600);
    if (databaseStatus(temporary).integrity !== "ok") throw new Error("restore candidate failed quick_check");
    for (const suffix of ["-wal", "-shm"]) {
      if (existsSync(`${destination}${suffix}`)) unlinkSync(`${destination}${suffix}`);
    }
    renameSync(temporary, destination);
    replaced = true;
    chmodSync(destination, 0o600);
  } finally {
    if (!replaced && existsSync(temporary)) unlinkSync(temporary);
  }
}

export function restoreInstance(options: {
  config_path: string;
  backup_directory: string;
  rollback_output: string;
  confirm_instance_id: string;
  confirm_offline: boolean;
}): { restored: true; schema_version: number; rollback_backup: string } {
  if (!options.confirm_offline) throw new Error("restore requires --confirm-offline after stopping the daemon");
  const config = loadInstanceConfig(options.config_path);
  if (options.confirm_instance_id !== config.instance_id) throw new Error("restore instance confirmation does not match config");
  const backup = readBackupManifest(options.backup_directory);
  if (backup.manifest.instance_id !== config.instance_id || backup.manifest.owner_id !== config.owner_id) {
    throw new Error("backup identity does not match the target Agent Space");
  }
  checkpointDatabase(config.db_path);
  const rollback = backupInstance(options.config_path, options.rollback_output);
  try {
    replaceDatabase(config.db_path, backup.databasePath);
    const restored = databaseStatus(config.db_path);
    if (restored.integrity !== "ok") throw new Error("restored database failed quick_check");
    return { restored: true, schema_version: restored.schema_version, rollback_backup: rollback.backup_directory };
  } catch (error) {
    const rollbackSource = readBackupManifest(rollback.backup_directory);
    replaceDatabase(config.db_path, rollbackSource.databasePath);
    throw error;
  }
}

export function upgradeInstance(configPath: string, backupOutput: string, confirmOffline = false): {
  upgraded: boolean;
  from_schema: number;
  to_schema: number;
  backup_directory: string | null;
} {
  const config = loadInstanceConfig(configPath);
  const before = databaseStatus(config.db_path);
  if (before.integrity !== "ok") throw new Error("database failed quick_check; refusing upgrade");
  if (before.schema_version === CURRENT_SCHEMA_VERSION) {
    return { upgraded: false, from_schema: before.schema_version, to_schema: before.schema_version, backup_directory: null };
  }
  if (!confirmOffline) throw new Error("schema upgrade requires --confirm-offline after stopping the daemon");
  if (before.schema_version < MIN_SUPPORTED_SCHEMA_VERSION || before.schema_version > CURRENT_SCHEMA_VERSION) {
    throw new Error(`database schema ${before.schema_version} cannot upgrade to ${CURRENT_SCHEMA_VERSION}`);
  }
  checkpointDatabase(config.db_path);
  const backup = backupInstance(configPath, backupOutput);
  try {
    const bridge = new WakeBridge(config, {
      autoMockTransport: false,
      recoverDispatchLeases: false,
      allowSchemaUpgrade: true,
    });
    bridge.close();
    const after = databaseStatus(config.db_path);
    if (after.schema_version !== CURRENT_SCHEMA_VERSION || after.integrity !== "ok") {
      throw new Error("upgraded database failed schema or integrity verification");
    }
    return {
      upgraded: true,
      from_schema: before.schema_version,
      to_schema: after.schema_version,
      backup_directory: backup.backup_directory,
    };
  } catch (error) {
    const rollback = readBackupManifest(backup.backup_directory);
    replaceDatabase(config.db_path, rollback.databasePath);
    throw error;
  }
}

function xml(value: string): string {
  return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;").replaceAll("'", "&apos;");
}

function serviceLabel(instanceId: string): string {
  return `io.wakebridge.${instanceId.replaceAll(/[^A-Za-z0-9.-]/gu, "-")}`;
}

function servicePort(value: number | undefined, profile: string): number {
  const port = value ?? 4311;
  if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) {
    throw new Error(`${profile} port must be an integer between 1 and 65535`);
  }
  return port;
}

function systemdArgument(value: string): string {
  return `"${value
    .replaceAll("\\", "\\\\")
    .replaceAll('"', '\\"')
    .replaceAll("$", () => "$$")
    .replaceAll("%", "%%")
    .replaceAll("\n", "\\n")
    .replaceAll("\r", "\\r")
    .replaceAll("\t", "\\t")}"`;
}

function systemdDirectivePath(value: string): string {
  return [...Buffer.from(value, "utf8")].map((byte) => {
    const character = String.fromCharCode(byte);
    if (/[A-Za-z0-9/._:@+-]/u.test(character)) return character;
    if (character === "%") return "%%";
    return `\\x${byte.toString(16).padStart(2, "0")}`;
  }).join("");
}

function serviceFilePath(value: string | undefined, label: string, requirePrivate = false): string | null {
  if (!value) return null;
  const path = realpathSync(resolve(value));
  const status = statSync(path);
  if (!status.isFile()) throw new Error(`${label} must be a regular file: ${path}`);
  if (requirePrivate && (status.mode & 0o077) !== 0) {
    throw new Error(`${label} must not be accessible by group or other users: ${path}`);
  }
  return path;
}

export function installMacLaunchAgent(options: {
  config_path: string;
  launch_agents_directory?: string;
  logs_directory?: string;
  cli_path?: string;
  port?: number;
}, runtime: ReleaseRuntime = {}): { installed: true; profile: "launch_agent"; label: string; plist_path: string; load_command: string; unload_command: string } {
  const platform = runtime.platform ?? process.platform;
  if (platform !== "darwin") throw new Error("macOS LaunchAgent installation requires darwin");
  const configPath = realpathSync(resolve(options.config_path));
  const config = loadInstanceConfig(configPath);
  const preflight = releasePreflight(configPath, { ...runtime, platform });
  if (!preflight.ok) throw new Error(`release preflight failed; refusing service install: ${preflight.instance.error || preflight.instance.schema_state}`);
  const port = servicePort(options.port, "LaunchAgent");
  const label = serviceLabel(config.instance_id);
  const launchAgents = resolve(options.launch_agents_directory ?? join(homedir(), "Library", "LaunchAgents"));
  const logs = resolve(options.logs_directory ?? join(dirname(config.db_path), "logs"));
  mkdirSync(launchAgents, { recursive: true, mode: 0o700 });
  mkdirSync(logs, { recursive: true, mode: 0o700 });
  const plistPath = join(launchAgents, `${label}.plist`);
  if (existsSync(plistPath)) throw new Error(`LaunchAgent already exists: ${plistPath}`);
  const cliPath = realpathSync(resolve(options.cli_path ?? process.argv[1]));
  const contents = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>${xml(label)}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${xml(process.execPath)}</string>
    <string>${xml(cliPath)}</string>
    <string>daemon</string>
    <string>--config</string><string>${xml(configPath)}</string>
    <string>--host</string><string>127.0.0.1</string>
    <string>--port</string><string>${port}</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>${xml(join(logs, "daemon.stdout.log"))}</string>
  <key>StandardErrorPath</key><string>${xml(join(logs, "daemon.stderr.log"))}</string>
</dict>
</plist>
`;
  writeFileSync(plistPath, contents, { encoding: "utf8", mode: 0o600, flag: "wx" });
  chmodSync(plistPath, 0o600);
  const domain = `gui/${typeof process.getuid === "function" ? process.getuid() : "UID"}`;
  return {
    installed: true,
    profile: "launch_agent",
    label,
    plist_path: plistPath,
    load_command: `launchctl bootstrap ${domain} ${plistPath}`,
    unload_command: `launchctl bootout ${domain}/${label}`,
  };
}

export function uninstallMacLaunchAgent(options: {
  config_path: string;
  launch_agents_directory?: string;
}): { uninstalled: true; label: string; plist_path: string; removed: boolean; data_preserved: true } {
  const config = loadInstanceConfig(options.config_path);
  const label = serviceLabel(config.instance_id);
  const launchAgents = resolve(options.launch_agents_directory ?? join(homedir(), "Library", "LaunchAgents"));
  const plistPath = join(launchAgents, `${label}.plist`);
  const removed = existsSync(plistPath);
  if (removed) unlinkSync(plistPath);
  return { uninstalled: true, label, plist_path: plistPath, removed, data_preserved: true };
}

export function installLinuxSystemdUserService(options: {
  config_path: string;
  systemd_user_directory?: string;
  environment_file?: string;
  host_adapters_path?: string;
  source_connectors_path?: string;
  source_credentials_path?: string;
  cli_path?: string;
  port?: number;
}, runtime: ReleaseRuntime = {}): {
  installed: true;
  profile: "systemd_user";
  label: string;
  unit_name: string;
  unit_path: string;
  daemon_reload_command: string;
  enable_command: string;
  start_command: string;
  stop_command: string;
  disable_command: string;
} {
  const platform = runtime.platform ?? process.platform;
  if (platform !== "linux") throw new Error("systemd user service installation requires linux");
  const configPath = realpathSync(resolve(options.config_path));
  const config = loadInstanceConfig(configPath);
  const preflight = releasePreflight(configPath, { ...runtime, platform });
  if (!preflight.ok) throw new Error(`release preflight failed; refusing service install: ${preflight.instance.error || preflight.instance.schema_state}`);
  const port = servicePort(options.port, "systemd user service");
  const label = serviceLabel(config.instance_id);
  const unitName = `${label}.service`;
  const xdgConfigHome = process.env.XDG_CONFIG_HOME || join(homedir(), ".config");
  const systemdUserDirectory = resolve(options.systemd_user_directory ?? join(xdgConfigHome, "systemd", "user"));
  mkdirSync(systemdUserDirectory, { recursive: true, mode: 0o700 });
  const unitPath = join(systemdUserDirectory, unitName);
  if (existsSync(unitPath)) throw new Error(`systemd user unit already exists: ${unitPath}`);
  const cliPath = realpathSync(resolve(options.cli_path ?? process.argv[1]));
  const environmentFile = serviceFilePath(options.environment_file, "systemd environment file", true);
  const hostAdaptersPath = serviceFilePath(options.host_adapters_path, "Host Adapter manifest");
  const sourceConnectorsPath = serviceFilePath(options.source_connectors_path, "Source Connector manifest");
  const sourceCredentialsPath = serviceFilePath(options.source_credentials_path, "source credential file", true);
  const execStartArguments = [
    process.execPath,
    cliPath,
    "daemon",
    "--config",
    configPath,
    "--host",
    "127.0.0.1",
    "--port",
    String(port),
  ];
  if (hostAdaptersPath) execStartArguments.push("--host-adapters", hostAdaptersPath);
  if (sourceConnectorsPath) execStartArguments.push("--connectors", sourceConnectorsPath);
  if (sourceCredentialsPath) execStartArguments.push("--source-credentials", sourceCredentialsPath);
  const execStart = execStartArguments.map(systemdArgument).join(" ");
  const environmentFileDirective = environmentFile ? `EnvironmentFile=${systemdDirectivePath(environmentFile)}\n` : "";
  const contents = `[Unit]
Description=Wake Bridge ${label}

[Service]
Type=exec
UMask=0077
${environmentFileDirective}ExecStart=${execStart}
Restart=on-failure
RestartSec=3s
KillSignal=SIGTERM
TimeoutStopSec=30s

[Install]
WantedBy=default.target
`;
  writeFileSync(unitPath, contents, { encoding: "utf8", mode: 0o600, flag: "wx" });
  chmodSync(unitPath, 0o600);
  return {
    installed: true,
    profile: "systemd_user",
    label,
    unit_name: unitName,
    unit_path: unitPath,
    daemon_reload_command: "systemctl --user daemon-reload",
    enable_command: `systemctl --user enable ${unitName}`,
    start_command: `systemctl --user start ${unitName}`,
    stop_command: `systemctl --user stop ${unitName}`,
    disable_command: `systemctl --user disable ${unitName}`,
  };
}

export function uninstallLinuxSystemdUserService(options: {
  config_path: string;
  systemd_user_directory?: string;
}, runtime: ReleaseRuntime = {}): {
  uninstalled: true;
  profile: "systemd_user";
  label: string;
  unit_name: string;
  unit_path: string;
  removed: boolean;
  daemon_reload_command: string;
  data_preserved: true;
} {
  const platform = runtime.platform ?? process.platform;
  if (platform !== "linux") throw new Error("systemd user service uninstall requires linux");
  const config = loadInstanceConfig(options.config_path);
  const label = serviceLabel(config.instance_id);
  const unitName = `${label}.service`;
  const xdgConfigHome = process.env.XDG_CONFIG_HOME || join(homedir(), ".config");
  const systemdUserDirectory = resolve(options.systemd_user_directory ?? join(xdgConfigHome, "systemd", "user"));
  const unitPath = join(systemdUserDirectory, unitName);
  const removed = existsSync(unitPath);
  if (removed) unlinkSync(unitPath);
  return {
    uninstalled: true,
    profile: "systemd_user",
    label,
    unit_name: unitName,
    unit_path: unitPath,
    removed,
    daemon_reload_command: "systemctl --user daemon-reload",
    data_preserved: true,
  };
}

export function installService(options: {
  config_path: string;
  launch_agents_directory?: string;
  logs_directory?: string;
  systemd_user_directory?: string;
  environment_file?: string;
  host_adapters_path?: string;
  source_connectors_path?: string;
  source_credentials_path?: string;
  cli_path?: string;
  port?: number;
}) {
  if (process.platform === "darwin") return installMacLaunchAgent(options);
  if (process.platform === "linux") return installLinuxSystemdUserService(options);
  throw new Error(`service install is unsupported on ${process.platform}`);
}

export function uninstallService(options: {
  config_path: string;
  launch_agents_directory?: string;
  systemd_user_directory?: string;
}) {
  if (process.platform === "darwin") return uninstallMacLaunchAgent(options);
  if (process.platform === "linux") return uninstallLinuxSystemdUserService(options);
  throw new Error(`service uninstall is unsupported on ${process.platform}`);
}
