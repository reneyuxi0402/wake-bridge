import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import { initializeInstance } from "../src/instance-config.js";

describe("ack timeout CLI configuration", () => {
  it.each([
    { name: "instance config", command: "dispatch", file: true, override: undefined, expected: "needs_attention" },
    { name: "environment overrides config", command: "dispatch", file: true, override: "1200000", expected: "dispatched" },
    { name: "environment without config", command: "dispatch", file: false, override: "300000", expected: "needs_attention" },
    { name: "status is read-only", command: "status", file: true, override: undefined, expected: "dispatched" },
  ])("honors $name", async ({ command, file, override, expected }) => {
    const dir = mkdtempSync(join(tmpdir(), "wake-bridge-ack-cli-"));
    try {
      const initialized = initializeInstance({ data_dir: dir, instance_id: "ack-cli", owner_id: "ack-owner" });
      const config = { ...initialized.config, ack_timeout_ms: 300_000 };
      writeFileSync(initialized.config_path, JSON.stringify(config), { mode: 0o600 });
      const past = new Date(Date.now() - 10 * 60_000);
      const seed = new WakeBridge(config, { clock: () => past });
      let batchId: string;
      try {
        const endpoint = seed.registerEndpoint({ host_kind: "mock", session_ref: "ack-cli-session" });
        seed.takeover("default", endpoint.id, 0);
        seed.emit("test", { type: "test.ready", dedupe_key: "one", resource: { uri: "test://one" } });
        seed.tick();
        await seed.dispatchDue();
        batchId = seed.listBatches()[0].id;
      } finally {
        seed.close();
      }
      const env: NodeJS.ProcessEnv = {
        ...process.env,
        WAKEBRIDGE_DB: config.db_path,
        WAKEBRIDGE_INSTANCE_ID: config.instance_id,
        WAKEBRIDGE_OWNER_ID: config.owner_id,
      };
      delete env.WAKEBRIDGE_CONFIG;
      delete env.WAKEBRIDGE_ACK_TIMEOUT_MS;
      if (override !== undefined) env.WAKEBRIDGE_ACK_TIMEOUT_MS = override;
      const result = spawnSync(process.execPath, ["dist/src/cli.js", command, ...(file ? ["--config", initialized.config_path] : [])], {
        cwd: process.cwd(), env, encoding: "utf8", timeout: 15_000,
      });
      expect(result.status, result.stderr).toBe(0);
      const observer = new WakeBridge(config, { recoverDispatchLeases: false });
      try {
        expect(observer.getBatch(batchId!)?.state).toBe(expected);
        expect(observer.listAttempts(batchId!)).toHaveLength(1);
        expect(observer.listReceipts(batchId!).map(receipt => receipt.stage)).toEqual(["transport_accepted"]);
      } finally {
        observer.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
