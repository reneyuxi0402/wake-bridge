import { spawnSync } from "node:child_process";
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeError, WakeBridge } from "../src/core.js";
import { startDaemon } from "../src/daemon.js";
import { SqliteDatabase } from "../src/db.js";
import { initializeInstance, loadInstanceConfig } from "../src/instance-config.js";
import { backupInstance, databaseStatus, restoreInstance } from "../src/release-lifecycle.js";
import { WakeBridgeSdkError } from "../src/sdk-error.js";
import type { HostAdapter } from "../src/transport-sdk.js";

vi.mock("node:child_process", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawnSync: vi.fn(actual.spawnSync) };
});

const actualChildProcess = await vi.importActual<typeof import("node:child_process")>("node:child_process");
const spawn = vi.mocked(spawnSync);
const PRIVATE = "OPS_PRIVATE_FIXTURE_TOKEN";
const PRIVATE_PATH = `/fixture/${PRIVATE}/bridge.sqlite`;
const PRIVATE_SQL = `SELECT '${PRIVATE}' AS private_value;`;

afterEach(() => {
  vi.restoreAllMocks();
  spawn.mockReset().mockImplementation(actualChildProcess.spawnSync);
});

function result(overrides: Record<string, unknown> = {}): ReturnType<typeof spawnSync> {
  return { pid: 0, output: [], stdout: "", stderr: "", status: 0, signal: null, ...overrides } as ReturnType<typeof spawnSync>;
}

function failure(code: string): Error {
  return Object.assign(new Error(`${PRIVATE}: spawn sqlite3 ${PRIVATE_PATH}`), {
    code, path: PRIVATE_PATH, syscall: `spawnSync ${PRIVATE_PATH}`, spawnargs: [PRIVATE_SQL],
  });
}

function captured(action: () => unknown): Error & { error_class?: string } {
  let error: unknown;
  try { action(); } catch (caught) { error = caught; }
  expect(error).toBeInstanceOf(Error);
  return error as Error & { error_class?: string };
}

function expectSafe(error: Error, errorClass: string): void {
  expect(error).toMatchObject({ error_class: errorClass });
  expect(JSON.stringify(Object.getOwnPropertyDescriptors(error))).not.toContain(PRIVATE);
  expect(error).not.toHaveProperty("cause");
}

function databaseHandle(): SqliteDatabase {
  return Object.assign(Object.create(SqliteDatabase.prototype), { path: PRIVATE_PATH }) as SqliteDatabase;
}

const processFailures = [
  { name: "unavailable executable", response: () => result({ error: failure("ENOENT"), status: null }), errorClass: "database_unavailable" },
  { name: "inaccessible executable", response: () => result({ error: failure("EACCES"), status: null }), errorClass: "database_unavailable" },
  { name: "forbidden executable", response: () => result({ error: failure("EPERM"), status: null }), errorClass: "database_unavailable" },
  { name: "output limit", response: () => result({ error: failure("ENOBUFS"), stdout: PRIVATE, stderr: PRIVATE, status: null }), errorClass: "database_output_limit" },
  { name: "process timeout", response: () => result({ error: failure("ETIMEDOUT"), stdout: PRIVATE, status: null }), errorClass: "database_execution_failed" },
  { name: "other process error", response: () => result({ error: failure("ENOSPC"), stderr: PRIVATE, status: null }), errorClass: "database_execution_failed" },
  { name: "execution failure", response: () => result({ status: 1, stdout: PRIVATE, stderr: `${PRIVATE_SQL}: ${PRIVATE_PATH}` }), errorClass: "database_execution_failed" },
] as const;

describe("safe SQLite diagnostics", () => {
  describe.each(["exec", "query"] as const)("%s", (operation) => {
    it.each(processFailures)("classifies $name without retaining process content", ({ response, errorClass }) => {
      spawn.mockReturnValueOnce(response());
      expectSafe(captured(() => databaseHandle()[operation](PRIVATE_SQL)), errorClass);
    });
  });

  it.each(processFailures)("classifies schema probe $name without retaining process content", ({ response, errorClass }) => {
    spawn.mockReturnValueOnce(response());
    expectSafe(captured(() => new SqliteDatabase(":memory:")), errorClass);
  });

  it.each([`[{"private":"${PRIVATE}"`, JSON.stringify({ private: PRIVATE })])("does not retain invalid query JSON: %s", (stdout) => {
    spawn.mockReturnValueOnce(result({ stdout }));
    expectSafe(captured(() => databaseHandle().query(PRIVATE_SQL)), "database_invalid_json");
  });

  it("does not retain invalid schema probe JSON", () => {
    spawn.mockReturnValueOnce(result({ stdout: `[{"user_version":"${PRIVATE}"` }));
    expectSafe(captured(() => new SqliteDatabase(":memory:")), "database_invalid_json");
  });

  it("keeps the fixed unique-conflict signal and real dedupe-race recovery", () => {
    const bridge = fixtureBridge();
    try {
      bridge.db.exec(`CREATE TABLE ${PRIVATE}(value TEXT UNIQUE); INSERT INTO ${PRIVATE} VALUES('one');`);
      const error = captured(() => bridge.db.exec(`INSERT INTO ${PRIVATE} VALUES('one');`));
      expectSafe(error, "database_execution_failed");
      expect(error.message).toContain("UNIQUE constraint failed");
      const input = { type: "job.completed", dedupe_key: "race:1", resource: { uri: "job://race/1" } };
      const first = bridge.emit("manual", input);
      vi.spyOn(bridge as any, "duplicateEmitResult").mockReturnValueOnce(null);
      const second = bridge.emit("manual", input);
      expect(second).toMatchObject({ duplicate: true, event: { id: first.event.id } });
      expect(bridge.listClaims()).toHaveLength(1);
    } finally { bridge.close(); }
  });
});

describe("safe instance config diagnostics", () => {
  it("does not retain the admin token fragment from malformed JSON", () => {
    const directory = mkdtempSync(join(tmpdir(), "wakebridge-ops-config-"));
    try {
      const path = join(directory, "wakebridge.config.json");
      writeFileSync(path, '{"admin_token":OPS_LEAK_SYNTHETIC_ADMIN_TOKEN_ONLY}', { mode: 0o600 });
      const error = captured(() => loadInstanceConfig(path));
      expect(JSON.stringify(Object.getOwnPropertyDescriptors(error))).not.toContain("OPS_LEAK");
      expect(error).toMatchObject({
        code: "invalid_instance_config", status: 400,
        message: "instance config is unreadable; check file access and JSON syntax",
      });
      expect(error).not.toHaveProperty("cause");
    } finally { rmSync(directory, { recursive: true, force: true }); }
  });

  it("preserves the existing insecure-permissions contract before JSON parsing", () => {
    const directory = mkdtempSync(join(tmpdir(), "wakebridge-ops-config-"));
    try {
      const path = join(directory, "wakebridge.config.json");
      writeFileSync(path, '{"admin_token":OPS_LEAK}', { mode: 0o600 });
      chmodSync(path, 0o644);
      const error = captured(() => loadInstanceConfig(path));
      expect(error).toBeInstanceOf(BridgeError);
      expect(error).toMatchObject({ code: "insecure_file_permissions", status: 400 });
      expect(error.message).toContain("must not be accessible by group or other users");
      expect(error.message).not.toContain("OPS_LEAK");
    } finally { rmSync(directory, { recursive: true, force: true }); }
  });
});

describe("safe release maintenance SQLite diagnostics", () => {
  it.each(processFailures)("classifies maintenance $name without retaining process content", ({ response, errorClass }) => {
    const directory = mkdtempSync(join(tmpdir(), "wakebridge-ops-maintenance-"));
    try {
      const path = join(directory, "fixture.sqlite");
      writeFileSync(path, "synthetic SQLite fixture", { mode: 0o600 });
      spawn.mockReturnValueOnce(response());
      expectSafe(captured(() => databaseStatus(path)), errorClass);
    } finally { rmSync(directory, { recursive: true, force: true }); }
  });

  it.each(["schema", "integrity"])("does not retain malformed maintenance %s JSON", (phase) => {
    const directory = mkdtempSync(join(tmpdir(), "wakebridge-ops-maintenance-"));
    try {
      const path = join(directory, "fixture.sqlite");
      writeFileSync(path, "synthetic SQLite fixture", { mode: 0o600 });
      if (phase === "integrity") spawn.mockReturnValueOnce(result({ stdout: '[{"user_version":8}]' }));
      spawn.mockReturnValueOnce(result({ stdout: PRIVATE }));
      expectSafe(captured(() => databaseStatus(path)), "database_invalid_json");
    } finally { rmSync(directory, { recursive: true, force: true }); }
  });

  it("does not retain malformed restore checkpoint JSON", () => {
    const directory = mkdtempSync(join(tmpdir(), "wakebridge-ops-maintenance-"));
    try {
      const instance = initializeInstance({ data_dir: join(directory, "instance"), instance_id: "ops-fixture", owner_id: "owner", timezone: "UTC" });
      const backup = join(directory, "backup");
      backupInstance(instance.config_path, backup);
      // Backup verification succeeds; fail only the following current-DB
      // checkpoint parse, before rollback backup or database replacement.
      spawn.mockReturnValueOnce(result({ stdout: '[{"user_version":8}]' }));
      spawn.mockReturnValueOnce(result({ stdout: '[{"quick_check":"ok"}]' }));
      spawn.mockReturnValueOnce(result({ stdout: PRIVATE }));
      expectSafe(captured(() => restoreInstance({
        config_path: instance.config_path, backup_directory: backup,
        rollback_output: join(directory, "rollback"), confirm_instance_id: "ops-fixture", confirm_offline: true,
      })), "database_invalid_json");
    } finally { rmSync(directory, { recursive: true, force: true }); }
  });
});

function fixtureBridge(): WakeBridge {
  return new WakeBridge({ instance_id: "ops-fixture", owner_id: "owner", db_path: ":memory:", timezone: "UTC" });
}

async function daemonFor(bridge: WakeBridge, options: Parameters<typeof startDaemon>[1] = {}) {
  return startDaemon(bridge.config, {
    bridge, host: "127.0.0.1", port: 0, scheduler_interval_ms: 0, unsafe_no_auth: true, ...options,
  });
}

function origin(daemon: Awaited<ReturnType<typeof startDaemon>>): string {
  return `http://127.0.0.1:${(daemon.address as AddressInfo).port}`;
}

function adapter(overrides: Partial<HostAdapter>): HostAdapter {
  return {
    kind: "ops_fixture",
    manifest: {
      contract_version: 1, adapter_kind: "ops_fixture", adapter_version: "0.0.1", host_kinds: ["fixture"],
      support_tier: "mock", tested_host_versions: ["fixture"], capabilities: { warm_resume: true, cold_push: false },
      receipt_upper_bound: "accepted_to_live_pipe",
    },
    dispatch: () => ({ accepted: true }),
    ...overrides,
  };
}

describe("safe daemon failure surfaces", () => {
  it.each(["tick", "dispatchDue"] as const)("reports %s failures with a safe phase, class, and fixed type", async (method) => {
    const bridge = fixtureBridge();
    const warning = vi.spyOn(process, "emitWarning").mockImplementation(() => {});
    if (method === "tick") vi.spyOn(bridge, method).mockImplementation(() => { throw new Error(PRIVATE); });
    else vi.spyOn(bridge, method).mockRejectedValue(new Error(PRIVATE));
    const daemon = await daemonFor(bridge, { scheduler_interval_ms: 60_000 });
    try {
      await vi.waitFor(() => expect(warning).toHaveBeenCalledTimes(1));
      expect(JSON.parse(String(warning.mock.calls[0][0]))).toEqual({
        phase: method === "tick" ? "scheduler_tick" : "scheduler_dispatch", error_class: "internal_error", error_type: "Error",
      });
      expect(JSON.stringify(warning.mock.calls)).not.toContain(PRIVATE);
      expect((await fetch(`${origin(daemon)}/health`)).status).toBe(200);
    } finally { await daemon.close(); bridge.close(); }
  });

  it.each([
    ...[
      new TypeError(PRIVATE), new RangeError(PRIVATE), new ReferenceError(PRIVATE), new SyntaxError(PRIVATE),
      new URIError(PRIVATE), new EvalError(PRIVATE), new Error(PRIVATE), new AggregateError([], PRIVATE),
    ].map((error) => {
      const errorType = error.name;
      return {
        label: `renamed ${errorType}`, errorType,
        error: Object.assign(error, { name: PRIVATE, message: PRIVATE, stack: `${PRIVATE}\n at ${PRIVATE_PATH}`, path: PRIVATE_PATH }),
      };
    }),
    { label: "Error renamed to a different built-in", error: Object.assign(new Error(PRIVATE), { name: "TypeError" }), errorType: "Error" },
    ...[
      PRIVATE, 42, null, undefined, true, 1n, Symbol(PRIVATE),
      { name: "TypeError", message: PRIVATE, stack: PRIVATE_PATH },
      { name: PRIVATE, message: PRIVATE, stack: PRIVATE_PATH, toString: () => PRIVATE },
    ].map((error, index) => ({ label: `non-Error value ${index}`, error, errorType: "unknown" })),
  ])("uses only a fixed error type for $label", async ({ error, errorType }) => {
    const bridge = fixtureBridge();
    const warning = vi.spyOn(process, "emitWarning").mockImplementation(() => {});
    vi.spyOn(bridge, "tick").mockImplementation(() => { throw error; });
    const daemon = await daemonFor(bridge, { scheduler_interval_ms: 60_000 });
    try {
      await vi.waitFor(() => expect(warning).toHaveBeenCalledTimes(1));
      expect(JSON.parse(String(warning.mock.calls[0][0]))).toEqual({
        phase: "scheduler_tick", error_class: "internal_error", error_type: errorType,
      });
      expect(JSON.stringify(warning.mock.calls)).not.toContain(PRIVATE);
    } finally { await daemon.close(); bridge.close(); }
  });

  it("recognizes safe database classes in scheduler warnings", async () => {
    const bridge = fixtureBridge();
    const warning = vi.spyOn(process, "emitWarning").mockImplementation(() => {});
    vi.spyOn(bridge, "tick").mockImplementation(() => {
      spawn.mockReturnValueOnce(result({ stdout: `[{"private":"${PRIVATE}"` }));
      bridge.db.query(PRIVATE_SQL);
      return {} as ReturnType<WakeBridge["tick"]>;
    });
    const daemon = await daemonFor(bridge, { scheduler_interval_ms: 60_000 });
    try {
      await vi.waitFor(() => expect(warning).toHaveBeenCalledTimes(1));
      expect(JSON.parse(String(warning.mock.calls[0][0]))).toEqual({ phase: "scheduler_tick", error_class: "database_invalid_json" });
      expect(JSON.stringify(warning.mock.calls)).not.toContain(PRIVATE);
    } finally { await daemon.close(); bridge.close(); }
  });

  it("does not log adapter cleanup errors or replace the original startup failure", async () => {
    const bridge = fixtureBridge();
    const warning = vi.spyOn(process, "emitWarning").mockImplementation(() => {});
    const startupError = new Error("fixture startup failed");
    try {
      await expect(daemonFor(bridge, { host_adapters: [adapter({
        start: () => { throw startupError; }, stop: () => { throw new Error(PRIVATE); },
      })] })).rejects.toBe(startupError);
      expect(JSON.parse(String(warning.mock.calls[0][0]))).toEqual({ phase: "host_adapter_cleanup", error_class: "internal_error", error_type: "Error" });
      expect(JSON.stringify(warning.mock.calls)).not.toContain(PRIVATE);
    } finally { bridge.close(); }
  });

  it.each([
    new Error(PRIVATE),
    Object.assign(new Error(PRIVATE), { code: PRIVATE, status: 409 }),
    { code: "database_invalid_json", message: PRIVATE, status: 400, toString: () => PRIVATE },
  ])("returns a fixed HTTP response for a non-contract exception", async (error) => {
    const bridge = fixtureBridge();
    vi.spyOn(bridge, "inspect").mockImplementation(() => { throw error; });
    const daemon = await daemonFor(bridge);
    try {
      const response = await fetch(`${origin(daemon)}/v1/inspect`);
      expect(response.status).toBe(500);
      expect(await response.json()).toEqual({ error: "Wake Bridge operation failed", code: "internal_error", error_class: "internal_error" });
    } finally { await daemon.close(); bridge.close(); }
  });

  it("returns a safe database class over HTTP", async () => {
    const bridge = fixtureBridge();
    vi.spyOn(bridge, "inspect").mockImplementation(() => {
      spawn.mockReturnValueOnce(result({ status: 1, stderr: PRIVATE_SQL }));
      bridge.db.query(PRIVATE_SQL);
      return {} as ReturnType<WakeBridge["inspect"]>;
    });
    const daemon = await daemonFor(bridge);
    try {
      const response = await fetch(`${origin(daemon)}/v1/inspect`);
      expect(response.status).toBe(500);
      expect(await response.json()).toEqual({ error: "Wake Bridge operation failed", code: "internal_error", error_class: "database_execution_failed" });
    } finally { await daemon.close(); bridge.close(); }
  });

  it.each([new BridgeError("fixture validation needs correction", "invalid_arguments", 400), new WakeBridgeSdkError("fixture conflict needs correction", "fixture_conflict", 409)])("preserves genuine contract status, code, and actionable message", async (error) => {
    const bridge = fixtureBridge();
    vi.spyOn(bridge, "inspect").mockImplementation(() => { throw error; });
    const daemon = await daemonFor(bridge);
    try {
      const response = await fetch(`${origin(daemon)}/v1/inspect`);
      expect(response.status).toBe(error.status);
      expect(await response.json()).toEqual({ error: error.message, code: error.code });
    } finally { await daemon.close(); bridge.close(); }
  });

  it("keeps JSON validation useful without returning request fragments", async () => {
    const bridge = fixtureBridge();
    const daemon = await daemonFor(bridge);
    try {
      const requestFragment = "OPS_LEAK";
      const response = await fetch(`${origin(daemon)}/v1/tick`, { method: "POST", body: requestFragment });
      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body).toMatchObject({ code: "invalid_json" });
      expect(body.error).toMatch(/invalid JSON body/i);
      expect(JSON.stringify(body)).not.toContain(requestFragment);
    } finally { await daemon.close(); bridge.close(); }
  });

  it.each([new Error(PRIVATE), new WakeBridgeSdkError("route base_url is required", "invalid_endpoint", 400)])("does not turn arbitrary host validation text into a trusted contract", async (error) => {
    const bridge = fixtureBridge();
    bridge.config.admin_token = "owner-fixture-token-0123456789abcdef";
    const daemon = await daemonFor(bridge, {
      unsafe_no_auth: false,
      host_adapters: [adapter({ validateEndpoint: () => { throw error; } })],
      host_credentials: [{ id: "fixture", token: "host-fixture-token-0123456789abcdef", adapter_kind: "ops_fixture", host_kind: "fixture", attention_channels: ["default"] }],
    });
    try {
      const response = await fetch(`${origin(daemon)}/v1/host-sessions/open`, {
        method: "POST", headers: { authorization: "Bearer host-fixture-token-0123456789abcdef" },
        body: JSON.stringify({ adapter_kind: "ops_fixture", session_ref: "fixture", attention_channel: "default", route_address: {} }),
      });
      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.code).toBe("invalid_host_session");
      expect(JSON.stringify(body)).not.toContain(PRIVATE);
      if (error instanceof WakeBridgeSdkError) expect(body.error).toContain(error.message);
      else expect(body.error).toMatch(/route configuration/i);
    } finally { await daemon.close(); bridge.close(); }
  });
});
