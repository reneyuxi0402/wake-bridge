import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { WakeBridge } from "../src/core.js";
import { operatorStatus } from "../src/operator-control.js";
import { SourceCheckpointStore, SourceControlStore } from "../src/source-adapter.js";
import { loadSourceConnectorFile, SourceSupervisor } from "../src/source-connector.js";

const source = { id: "explanation", base_url: "http://127.0.0.1:4391", token_env: "EXPLANATION_SECRET" };
const manifest = {
  contract_version: 1 as const, id: source.id, version: "1.0.0", subject_ref: "fixture:owner",
  binding_fingerprint: `sha256:${"3".repeat(64)}`, read_side_effects: "none" as const,
  credential_custody: "connector" as const, upstream_credential_breadth: "broad" as const,
  upstream_credential_scopes: ["provider.account"], connector_capabilities: ["notifications.read"],
};

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function temporaryRoot(prefix: string): string {
  const root = mkdtempSync(join(tmpdir(), prefix));
  roots.push(root);
  return root;
}

function config() {
  return { instance_id: "explanation", owner_id: "owner", db_path: join(temporaryRoot("source-explanation-"), "bridge.sqlite") };
}

describe("source effective enabled explanation", () => {
  it.each([undefined, false])("explains loaded configuration %s without persistent control", (enabled) => {
    const bridge = new WakeBridge(config());
    try {
      const loaded = { ...source, ...(enabled === undefined ? {} : { enabled }) };
      const supervisor = new SourceSupervisor(bridge, [loaded]);
      loaded.enabled = enabled === false;
      expect(supervisor.list()[0]).toMatchObject({
        enabled: enabled !== false, loaded_config_enabled: enabled !== false,
        persisted_control_enabled: null, enabled_source: "configuration",
      });
      expect(operatorStatus(bridge, { sources: supervisor.list() }).sources.items[0]).toMatchObject({
        enabled: enabled !== false, loaded_config_enabled: enabled !== false,
        persisted_control_enabled: null, enabled_source: "configuration",
      });
    } finally { bridge.close(); }
  });

  it.each([false, true])("explains persistent %s overriding the opposite loaded value across reopen", (persisted) => {
    const bridgeConfig = config();
    const bridge = new WakeBridge(bridgeConfig);
    new SourceControlStore(bridge).set(source.id, persisted);
    bridge.close();
    const reopened = new WakeBridge(bridgeConfig);
    try {
      expect(new SourceSupervisor(reopened, [{ ...source, enabled: !persisted }]).list()[0]).toMatchObject({
        enabled: persisted, loaded_config_enabled: !persisted,
        persisted_control_enabled: persisted, enabled_source: "persisted_control",
      });
    } finally { reopened.close(); }
  });

  it("updates live control explanations, leaves offline configuration unobserved, and excludes secrets", async () => {
    const bridge = new WakeBridge(config());
    new SourceCheckpointStore(bridge).commit(manifest, { secret_cursor: "private-cursor" }, 0);
    const fetcher = vi.fn(async () => new Response(JSON.stringify(manifest), { status: 200 }));
    const supervisor = new SourceSupervisor(bridge, [{ ...source, enabled: false }], { EXPLANATION_SECRET: "private-token" }, fetcher);
    await supervisor.stop();
    try {
      expect(await supervisor.enable(source.id)).toMatchObject({
        enabled: true, loaded_config_enabled: false, persisted_control_enabled: true, enabled_source: "persisted_control",
      });
      expect(new SourceControlStore(bridge).get(source.id)).toBe(true);
      expect(operatorStatus(bridge).sources.items[0]).toMatchObject({
        enabled: true, loaded_config_enabled: null, persisted_control_enabled: null, enabled_source: null,
      });
      expect(await supervisor.disable(source.id)).toMatchObject({
        enabled: false, loaded_config_enabled: false, persisted_control_enabled: false, enabled_source: "persisted_control",
      });
      expect(new SourceControlStore(bridge).get(source.id)).toBe(false);
      const live = operatorStatus(bridge, { sources: supervisor.list() });
      expect(live.sources).toMatchObject({ runtime_observed: true, items: [{
        enabled: false, loaded_config_enabled: false, persisted_control_enabled: false, enabled_source: "persisted_control",
      }] });
      const offline = operatorStatus(bridge);
      expect(offline.sources).toMatchObject({ runtime_observed: false, items: [{
        enabled: false, loaded_config_enabled: null, persisted_control_enabled: null, enabled_source: null, runtime_state: null,
      }] });
      const encoded = JSON.stringify({ live, offline, runtime: supervisor.list() });
      for (const secret of ["private-token", "private-cursor", "EXPLANATION_SECRET", source.base_url, "provider.account"]) {
        expect(encoded).not.toContain(secret);
      }
    } finally { await supervisor.stop(); bridge.close(); }
  });

  it("source-validate identifies normalized enabled values as file initial values, not runtime controls", () => {
    const file = join(temporaryRoot("source-validation-"), "sources.json");
    writeFileSync(file, JSON.stringify({ version: 1, sources: [source, { ...source, id: "explicit-false", enabled: false }] }));
    expect(loadSourceConnectorFile(file).map((item) => item.enabled)).toEqual([true, false]);
    const bridgeConfig = config();
    const bridge = new WakeBridge(bridgeConfig);
    new SourceControlStore(bridge).set(source.id, false);
    bridge.close();
    const result = spawnSync(process.execPath, [
      "--import", "tsx", "src/cli.ts", "source-validate", "--connectors", file,
      "--db", bridgeConfig.db_path, "--instance-id", bridgeConfig.instance_id,
    ], {
      cwd: process.cwd(), encoding: "utf8", env: { ...process.env, EXPLANATION_SECRET: "private-token" },
    });
    expect(result.status, result.stderr).toBe(0);
    expect(JSON.parse(result.stdout)).toMatchObject({
      ok: true, sources: [{ enabled: true }, { enabled: false }],
      enabled_semantics: {
        omitted_defaults_to: true, scope: "configuration_initial_value",
        runtime_control: "persisted enable/disable overrides this initial value",
      },
    });
    expect(result.stdout).not.toContain("private-token");
  });
});
