import { spawnSync } from "node:child_process";
import { chmodSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { initializeInstance } from "../src/instance-config.js";
import type { TransportResult, WakeTransport } from "../src/types.js";

class CliPermanentFailure implements WakeTransport {
  readonly kind = "cli_failure";
  dispatch(): TransportResult {
    return { accepted: false, retryable: false, error_class: "cli_permanent_failure" };
  }
}

describe("Wake Bridge CLI", () => {
  it("rejects unknown and removed commands instead of opening a default Agent Space", () => {
    const dir = mkdtempSync(join(tmpdir(), "wake-bridge-cli-unknown-"));
    const db = join(dir, "must-not-exist.sqlite");
    try {
      const result = spawnSync(process.execPath, ["dist/src/cli.js", "old-product-host-command"], {
        cwd: process.cwd(),
        env: { ...process.env, WAKEBRIDGE_DB: db, WAKEBRIDGE_INSTANCE_ID: "i", WAKEBRIDGE_OWNER_ID: "o" },
        encoding: "utf8",
      });
      expect(result.status).toBe(1);
      expect(result.stderr).toContain("unknown command: old-product-host-command");
      expect(() => statSync(db)).toThrow();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rejects the removed cold-routes alias and accepts routes", () => {
    const dir = mkdtempSync(join(tmpdir(), "wake-bridge-cli-routes-"));
    const env = {
      ...process.env,
      WAKEBRIDGE_DB: join(dir, "bridge.sqlite"),
      WAKEBRIDGE_INSTANCE_ID: "i",
      WAKEBRIDGE_OWNER_ID: "o",
    };
    try {
      const removed = spawnSync(process.execPath, [
        "dist/src/cli.js", "endpoint-register",
        "--host-kind", "mock",
        "--session-ref", "legacy",
        "--cold-routes", "[]",
      ], { cwd: process.cwd(), env, encoding: "utf8" });
      expect(removed.status).toBe(1);
      expect(removed.stderr).toContain("--cold-routes has been removed; use --routes");

      const current = spawnSync(process.execPath, [
        "dist/src/cli.js", "endpoint-register",
        "--host-kind", "mock",
        "--session-ref", "current",
        "--routes", '[{"kind":"mock","address":{}}]',
      ], { cwd: process.cwd(), env, encoding: "utf8" });
      expect(current.status).toBe(0);
      expect(JSON.parse(current.stdout)).toMatchObject({
        host_kind: "mock",
        session_ref: "current",
        routes: [{ kind: "mock", address: {} }],
      });
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("reports the public Host Adapter contract without claiming product-host support", () => {
    const result = spawnSync(process.execPath, ["dist/src/cli.js", "doctor"], {
      cwd: process.cwd(), env: process.env, encoding: "utf8",
    });
    expect(result.status).toBe(0);
    const hosts = JSON.parse(result.stdout).hosts;
    expect(hosts).toMatchObject({
      contract: {
        protocol: "out_of_process_loopback_v1",
        product_hosts_bundled: false,
      },
      configured_adapters: { configured: false, adapter_count: 0, adapters: [] },
    });
    expect(Object.keys(hosts).sort()).toEqual(["configured_adapters", "contract"]);
  });

  it("lists a secret-free Source Connector catalog without enabling planned connectors", () => {
    const result = spawnSync(process.execPath, ["dist/src/cli.js", "connector-catalog"], {
      cwd: process.cwd(), env: process.env, encoding: "utf8",
    });
    expect(result.status).toBe(0);
    const catalog = JSON.parse(result.stdout);
    expect(catalog).toMatchObject({
      schema_version: 1,
      runtime_states: ["disabled", "enabled", "needs_attention"],
      connectors: [
        { id: "botlingknows", availability: "available", default_state: "disabled", enable_supported: true },
        { id: "gmail", availability: "planned", default_state: "disabled", enable_supported: false },
        { id: "group_chat_fixture", kind: "reference", default_state: "disabled" },
      ],
    });
    expect(result.stdout).not.toMatch(/token|credential|迎风/u);
  });

  it("initializes a private single-space config without printing or overwriting its owner credential", () => {
    const root = mkdtempSync(join(tmpdir(), "wake-bridge-cli-init-"));
    const dataDir = join(root, "agent-space");
    const args = [
      "dist/src/cli.js", "init",
      "--data-dir", dataDir,
      "--instance-id", "agent-a",
      "--owner-id", "agent-a-owner",
      "--timezone", "UTC",
    ];
    const { WAKEBRIDGE_CONFIG: _config, WAKEBRIDGE_ADMIN_TOKEN: _admin, ...cleanEnv } = process.env;
    try {
      const initialized = spawnSync(process.execPath, args, { cwd: process.cwd(), env: cleanEnv, encoding: "utf8" });
      expect(initialized.status).toBe(0);
      const result = JSON.parse(initialized.stdout);
      const stored = JSON.parse(readFileSync(result.config_path, "utf8"));
      expect(result).toMatchObject({
        ok: true,
        instance_id: "agent-a",
        owner_id: "agent-a-owner",
        credential: "stored_in_private_config",
      });
      expect(stored).toMatchObject({
        version: 1,
        instance_id: "agent-a",
        owner_id: "agent-a-owner",
        timezone: "UTC",
      });
      expect(stored.admin_token).toMatch(/^[A-Za-z0-9_-]{43}$/u);
      expect(initialized.stdout).not.toContain(stored.admin_token);
      if (process.platform !== "win32") {
        expect(statSync(dataDir).mode & 0o777).toBe(0o700);
        expect(statSync(result.config_path).mode & 0o777).toBe(0o600);
        expect(statSync(result.db_path).mode & 0o777).toBe(0o600);
      }

      const doctor = spawnSync(process.execPath, ["dist/src/cli.js", "doctor", "--config", result.config_path], {
        cwd: process.cwd(), env: cleanEnv, encoding: "utf8",
      });
      expect(doctor.status).toBe(0);
      expect(JSON.parse(doctor.stdout)).toMatchObject({
        ok: true,
        runtime: { sqlite3: { available: true } },
        security: {
          owner_api_authenticated: true,
          config_file: { configured: true, private_permissions: true },
          database: { exists: true, ready: true },
        },
      });

      const repeated = spawnSync(process.execPath, args, { cwd: process.cwd(), env: cleanEnv, encoding: "utf8" });
      expect(repeated.status).toBe(1);
      expect(repeated.stderr).toContain("instance config already exists");
      expect(JSON.parse(readFileSync(result.config_path, "utf8")).admin_token).toBe(stored.admin_token);

      const rotated = spawnSync(process.execPath, ["dist/src/cli.js", "owner-token-rotate", "--config", result.config_path], {
        cwd: process.cwd(), env: cleanEnv, encoding: "utf8",
      });
      expect(rotated.status).toBe(0);
      const rotatedToken = JSON.parse(readFileSync(result.config_path, "utf8")).admin_token;
      expect(rotatedToken).toMatch(/^[A-Za-z0-9_-]{43}$/u);
      expect(rotatedToken).not.toBe(stored.admin_token);
      expect(rotated.stdout).not.toContain(rotatedToken);
      if (process.platform !== "win32") expect(statSync(result.config_path).mode & 0o777).toBe(0o600);

      const overridden = spawnSync(process.execPath, [
        "dist/src/cli.js", "doctor", "--config", result.config_path, "--owner-id", "someone-else",
      ], { cwd: process.cwd(), env: cleanEnv, encoding: "utf8" });
      expect(overridden.status).toBe(0);
      expect(JSON.parse(overridden.stdout)).toMatchObject({ ok: false, security: { owner_api_authenticated: false } });
      expect(JSON.parse(overridden.stdout).security.config_load_error).toContain("cannot override the fixed Agent Space config");

      if (process.platform !== "win32") {
        chmodSync(result.config_path, 0o644);
        const insecure = spawnSync(process.execPath, ["dist/src/cli.js", "doctor", "--config", result.config_path], {
          cwd: process.cwd(), env: cleanEnv, encoding: "utf8",
        });
        expect(insecure.status).toBe(0);
        expect(JSON.parse(insecure.stdout)).toMatchObject({
          ok: false,
          security: { owner_api_authenticated: false, config_file: { configured: true, ready: false } },
        });
      }
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("reports secret-free operator status and reopens dead letters without dispatching", async () => {
    const root = mkdtempSync(join(tmpdir(), "wake-bridge-cli-operator-"));
    const initialized = initializeInstance({
      data_dir: join(root, "agent-space"),
      instance_id: "cli-operator",
      owner_id: "cli-owner",
      timezone: "UTC",
    });
    const bridge = new WakeBridge(initialized.config, {
      autoMockTransport: false,
      transports: [new CliPermanentFailure()],
      recoverDispatchLeases: false,
    });
    try {
      const endpoint = bridge.registerEndpoint({
        host_kind: "fixture",
        session_ref: "cli-secret-session",
        capabilities: { cold_push: true },
        routes: [{ kind: "cli_failure", address: { token: "cli-route-secret" } }],
      });
      bridge.takeover("default", endpoint.id, 0);
      bridge.emit("fixture", { type: "cli.operator", dedupe_key: "one", resource: { uri: "fixture://cli/one" } });
      bridge.tick();
      await bridge.dispatchDue();
      const batch = bridge.listBatches()[0];
      bridge.close();

      const status = spawnSync(process.execPath, ["dist/src/cli.js", "status", "--config", initialized.config_path], {
        cwd: process.cwd(), encoding: "utf8",
      });
      expect(status.status).toBe(2);
      expect(JSON.parse(status.stdout)).toMatchObject({
        health: "needs_attention",
        queue: { counts: { dead_letter: 1 }, dead_letters: [{ batch_id: batch.id, attempt: 1 }] },
      });
      expect(status.stdout).not.toContain("cli-route-secret");
      expect(status.stdout).not.toContain("cli-secret-session");

      const retry = spawnSync(process.execPath, [
        "dist/src/cli.js", "batch-retry", batch.id,
        "--config", initialized.config_path,
        "--expected-attempt", "1",
        "--reason", "operator repaired route",
      ], { cwd: process.cwd(), encoding: "utf8" });
      expect(retry.status, retry.stderr).toBe(0);
      expect(JSON.parse(retry.stdout)).toMatchObject({ retried: true, batch: { state: "retry_wait", attempt: 1 } });

      const after = spawnSync(process.execPath, ["dist/src/cli.js", "status", "--config", initialized.config_path], {
        cwd: process.cwd(), encoding: "utf8",
      });
      expect(after.status).toBe(0);
      expect(JSON.parse(after.stdout)).toMatchObject({ health: "degraded", queue: { counts: { retry_wait: 1, dead_letter: 0 } } });
    } finally {
      try { bridge.close(); } catch { /* already closed */ }
      rmSync(root, { recursive: true, force: true });
    }
  });
});
