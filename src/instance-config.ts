import { randomBytes } from "node:crypto";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { basename, dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { WakeBridge, BridgeError } from "./core.js";
import type { BridgeConfig } from "./types.js";

export interface WakeBridgeInstanceConfigFile {
  version: 1;
  instance_id: string;
  owner_id: string;
  db_path: string;
  timezone: string;
  admin_token: string;
}

export interface InitializeInstanceOptions {
  data_dir: string;
  instance_id: string;
  owner_id: string;
  timezone?: string;
  db_path?: string;
  config_path?: string;
}

const INSTANCE_ID = /^[A-Za-z0-9_.:-]{1,128}$/u;

function assertPrivateMode(path: string, kind: string): void {
  if (process.platform === "win32") return;
  const mode = statSync(path).mode & 0o777;
  if ((mode & 0o077) !== 0) {
    throw new BridgeError(`${kind} must not be accessible by group or other users: ${path}`, "insecure_file_permissions", 400);
  }
}

function assertWithin(dataDir: string, path: string, kind: string): void {
  const relation = relative(dataDir, path);
  if (relation === "" || relation === ".." || relation.startsWith(`..${sep}`) || isAbsolute(relation)) {
    throw new BridgeError(`${kind} must be a file inside data_dir`, "invalid_instance_config", 400);
  }
}

export function loadInstanceConfig(path: string): WakeBridgeInstanceConfigFile {
  let parsed: unknown;
  try {
    assertPrivateMode(path, "instance config");
    parsed = JSON.parse(readFileSync(path, "utf8"));
  } catch (error) {
    if (error instanceof BridgeError) throw error;
    throw new BridgeError(`instance config is unreadable: ${String(error)}`, "invalid_instance_config", 400);
  }
  const config = parsed as Partial<WakeBridgeInstanceConfigFile>;
  if (config?.version !== 1 || !INSTANCE_ID.test(config.instance_id ?? "") || !INSTANCE_ID.test(config.owner_id ?? "")
    || typeof config.db_path !== "string" || !isAbsolute(config.db_path)
    || typeof config.timezone !== "string" || !config.timezone
    || typeof config.admin_token !== "string" || config.admin_token.length < 32) {
    throw new BridgeError("instance config is invalid", "invalid_instance_config", 400);
  }
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: config.timezone }).format(new Date());
  } catch {
    throw new BridgeError("instance config timezone is invalid", "invalid_instance_config", 400);
  }
  return config as WakeBridgeInstanceConfigFile;
}

export function initializeInstance(options: InitializeInstanceOptions): {
  config: WakeBridgeInstanceConfigFile;
  config_path: string;
  data_dir: string;
} {
  if (!INSTANCE_ID.test(options.instance_id) || !INSTANCE_ID.test(options.owner_id)) {
    throw new BridgeError("explicit valid instance_id and owner_id are required", "invalid_instance_config", 400);
  }
  const dataDir = resolve(options.data_dir);
  const configPath = resolve(options.config_path ?? resolve(dataDir, "wakebridge.config.json"));
  const dbPath = resolve(options.db_path ?? resolve(dataDir, "wake-bridge.sqlite"));
  assertWithin(dataDir, configPath, "config_path");
  assertWithin(dataDir, dbPath, "db_path");
  if (existsSync(configPath)) {
    throw new BridgeError(`instance config already exists: ${configPath}`, "instance_already_initialized", 409);
  }
  if (existsSync(dbPath)) {
    throw new BridgeError(`instance database already exists without a matching config: ${dbPath}`, "instance_database_exists", 409);
  }
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: options.timezone ?? "UTC" }).format(new Date());
  } catch {
    throw new BridgeError("timezone must be a valid IANA timezone", "invalid_instance_config", 400);
  }
  if (!existsSync(dataDir)) mkdirSync(dataDir, { recursive: true, mode: 0o700 });
  if (!statSync(dataDir).isDirectory()) throw new BridgeError("data_dir must be a directory", "invalid_instance_config", 400);
  assertPrivateMode(dataDir, "data_dir");
  mkdirSync(dirname(dbPath), { recursive: true, mode: 0o700 });
  mkdirSync(dirname(configPath), { recursive: true, mode: 0o700 });
  const config: WakeBridgeInstanceConfigFile = {
    version: 1,
    instance_id: options.instance_id,
    owner_id: options.owner_id,
    db_path: dbPath,
    timezone: options.timezone ?? "UTC",
    admin_token: randomBytes(32).toString("base64url"),
  };
  const bridge = new WakeBridge(config, { recoverDispatchLeases: false });
  bridge.close();
  chmodSync(dbPath, 0o600);
  for (const suffix of ["-wal", "-shm"]) {
    if (existsSync(`${dbPath}${suffix}`)) chmodSync(`${dbPath}${suffix}`, 0o600);
  }
  writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`, { encoding: "utf8", mode: 0o600, flag: "wx" });
  chmodSync(configPath, 0o600);
  return { config, config_path: configPath, data_dir: dataDir };
}

export function instanceConfigSecurity(path: string | undefined): {
  configured: boolean;
  path: string | null;
  private_permissions: boolean | null;
  database_exists: boolean | null;
  database_private_permissions: boolean | null;
  data_directory_private_permissions: boolean | null;
  ready: boolean;
  error: string | null;
} {
  if (!path) return {
    configured: false,
    path: null,
    private_permissions: null,
    database_exists: null,
    database_private_permissions: null,
    data_directory_private_permissions: null,
    ready: true,
    error: null,
  };
  try {
    assertPrivateMode(path, "instance config");
    const config = loadInstanceConfig(path);
    if (!existsSync(config.db_path)) throw new BridgeError(`instance database does not exist: ${config.db_path}`, "invalid_instance_config", 400);
    assertPrivateMode(config.db_path, "instance database");
    assertPrivateMode(dirname(config.db_path), "instance data directory");
    return {
      configured: true,
      path: resolve(path),
      private_permissions: true,
      database_exists: true,
      database_private_permissions: true,
      data_directory_private_permissions: true,
      ready: true,
      error: null,
    };
  } catch (error) {
    return {
      configured: true,
      path: resolve(path),
      private_permissions: false,
      database_exists: null,
      database_private_permissions: null,
      data_directory_private_permissions: null,
      ready: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

export function instanceDataSecurity(dbPath: string): {
  path: string;
  exists: boolean;
  private_permissions: boolean | null;
  directory_private_permissions: boolean | null;
  ready: boolean;
  error: string | null;
} {
  const path = resolve(dbPath);
  try {
    if (!existsSync(path)) throw new BridgeError(`instance database does not exist: ${path}`, "invalid_instance_config", 400);
    assertPrivateMode(path, "instance database");
    assertPrivateMode(dirname(path), "instance data directory");
    return { path, exists: true, private_permissions: true, directory_private_permissions: true, ready: true, error: null };
  } catch (error) {
    return {
      path,
      exists: existsSync(path),
      private_permissions: null,
      directory_private_permissions: null,
      ready: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

export function bridgeConfigFromFile(path: string): BridgeConfig {
  const config = loadInstanceConfig(path);
  return {
    instance_id: config.instance_id,
    owner_id: config.owner_id,
    db_path: config.db_path,
    timezone: config.timezone,
    admin_token: config.admin_token,
  };
}

export function rotateOwnerCredential(path: string): { config_path: string; rotated: true } {
  const configPath = resolve(path);
  const config = loadInstanceConfig(configPath);
  const next = { ...config, admin_token: randomBytes(32).toString("base64url") };
  const temporary = resolve(dirname(configPath), `.${basename(configPath)}.${randomBytes(8).toString("hex")}.tmp`);
  writeFileSync(temporary, `${JSON.stringify(next, null, 2)}\n`, { encoding: "utf8", mode: 0o600, flag: "wx" });
  chmodSync(temporary, 0o600);
  renameSync(temporary, configPath);
  return { config_path: configPath, rotated: true };
}
