import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { WakeBridge, type WakeBridgeOptions } from "../src/core.js";
import { sqlJson, sqlValue } from "../src/db.js";
import { SourceRunner } from "../src/source-adapter.js";
import { SourceSupervisor } from "../src/source-connector.js";
import type { BridgeConfig, PullSourceAdapter, TransportContext, TransportResult, WakeTransport } from "../src/types.js";

const bridges: WakeBridge[] = [];
const directories: string[] = [];
const clock = () => new Date("2026-10-30T10:00:00.000Z");

function fixture(config: Partial<BridgeConfig> = {}, options: WakeBridgeOptions = {}): WakeBridge {
  const directory = mkdtempSync(join(tmpdir(), "wake-bridge-channels-"));
  directories.push(directory);
  const bridge = new WakeBridge({ instance_id: "i", owner_id: "o", db_path: join(directory, "bridge.sqlite"), timezone: "UTC", ...config }, { clock, ...options });
  bridges.push(bridge);
  return bridge;
}

function reopen(bridge: WakeBridge): WakeBridge {
  const next = new WakeBridge(bridge.config, { clock });
  bridges.push(next);
  return next;
}

function event(channel: string, key = channel) {
  return { type: "job", dedupe_key: key, attention_channel_hint: channel, resource: { uri: `job://${key}` } };
}

function registration(id = "ep") {
  return { id, host_kind: "mock", session_ref: id, capabilities: { cold_push: true, foreground_presence_observable: true, session_activity_observable: true } };
}

afterEach(() => {
  vi.restoreAllMocks();
  for (const bridge of bridges.splice(0)) bridge.close();
  for (const directory of directories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

describe("attention channel registry", () => {
  it("validates the configured limit and printable, bounded IDs and names", () => {
    for (const max_channels of [0, 1025, 1.5, NaN]) {
      expect(() => fixture({ max_channels })).toThrowError(expect.objectContaining({ code: "invalid_config" }));
    }
    const bridge = fixture();
    expect(bridge.config.max_channels).toBe(32);
    for (const name of ["", " \t", "x".repeat(201), "bad\nname"]) {
      expect(() => bridge.createChannel({ name })).toThrowError(expect.objectContaining({ code: "invalid_channel" }));
    }
    for (const attention_channel of ["", " ", "x".repeat(201), "bad\u0000id"]) {
      expect(() => bridge.createChannel({ name: "valid", attention_channel })).toThrowError(expect.objectContaining({ code: "invalid_channel" }));
    }
    expect(bridge.createChannel({ name: "  Life  ", attention_channel: "中文:life/1" })).toMatchObject({ name: "Life", attention_channel: "中文:life/1", deleted_at: null });
    expect(bridge.createChannel({ name: "x".repeat(200), attention_channel: "x".repeat(200) }).attention_channel).toHaveLength(200);
  });

  it("keeps stable routing and generations when renamed, permits duplicate names, and never reuses tombstones", () => {
    const bridge = fixture({ max_channels: 2 });
    const original = bridge.createChannel({ name: "Life", attention_channel: "life" });
    const endpoint = bridge.registerEndpoint(registration());
    const binding = bridge.takeover("life", endpoint.id);
    const emitted = bridge.emit("fixture", event("life"));
    const renamed = bridge.renameChannel("life", "  Shared name  ");
    expect(renamed).toEqual({ ...original, name: "Shared name" });
    expect(bridge.getBinding("life")).toEqual(binding);
    expect(bridge.getClaim(emitted.claim!.id)?.attention_channel).toBe("life");
    expect(bridge.createChannel({ name: "Shared name" }).attention_channel).toMatch(/^ch_/u);
    const deleted = bridge.deleteChannel("life", { force: true });
    expect(bridge.deleteChannel("life")).toEqual(deleted);
    expect(bridge.listChannels()).toHaveLength(1);
    expect(bridge.listChannels(true)).toHaveLength(2);
    expect(bridge.getChannel("life")).toEqual(deleted);
    expect(() => bridge.createChannel({ name: "Life again", attention_channel: "life" })).toThrowError(expect.objectContaining({ code: "channel_exists", status: 409 }));
    expect(bridge.createChannel({ name: "Life" }).attention_channel).not.toBe("life");
    expect(() => bridge.renameChannel("life", "Revive")).toThrowError(expect.objectContaining({ code: "channel_deleted", status: 409 }));
    expect(() => bridge.renameChannel("missing", "Name")).toThrowError(expect.objectContaining({ code: "channel_not_found", status: 404 }));
    expect(() => bridge.deleteChannel("missing")).toThrowError(expect.objectContaining({ code: "channel_not_found", status: 404 }));
  });

  it.each(["create", "schedule", "takeover", "host-session", "watch"])("keeps the same atomic quota error for explicit %s registration", (entry) => {
    const bridge = fixture({ max_channels: 1 });
    bridge.createChannel({ name: "Existing", attention_channel: "existing" });
    const endpoint = bridge.registerEndpoint(registration());
    const beforeEvents = bridge.listEvents();
    const beforeEndpoints = bridge.listEndpoints();
    const write = () => {
      if (entry === "create") return bridge.createChannel({ name: "New", attention_channel: "new" });
      if (entry === "schedule") return bridge.scheduleClaim({ attention_channel: "new", eligible_after: bridge.nowIso(), resource: { uri: "job://schedule" } });
      if (entry === "takeover") return bridge.takeover("new", endpoint.id);
      if (entry === "host-session") return bridge.registerHostSession(registration("new-endpoint"), ["existing", "new"]);
      return bridge.configureActivityWatch({ attention_channel: "new", enabled: true, idle_after_ms: 60_000 });
    };
    expect(write).toThrowError(expect.objectContaining({ code: "channel_limit_reached", status: 429 }));
    expect(bridge.getChannel("new")).toBeNull();
    expect(bridge.listEvents()).toEqual(beforeEvents);
    expect(bridge.listEndpoints()).toEqual(beforeEndpoints);
    expect(bridge.listBindings()).toEqual([]);
    expect(bridge.listActivityWatches()).toEqual([]);
    expect(bridge.emit("fixture", event("existing")).claim?.attention_channel).toBe("existing");
  });

  it("durably suppresses an over-quota source event and keeps dedupe and idempotency final after capacity is freed", () => {
    const bridge = fixture({ max_channels: 1 });
    bridge.createChannel({ name: "Existing", attention_channel: "existing" });
    const input = { ...event("new"), idempotency_key: "quota-idempotency" };
    expect(bridge.previewPolicy("fixture", input)).toMatchObject({ suppressed: true, claim: null, batch: null });
    expect(bridge.listEvents()).toEqual([]);
    const emitted = bridge.emit("fixture", input);
    expect(emitted).toMatchObject({ suppressed: true, duplicate: false, event: { state: "suppressed" } });
    expect(emitted.claim).toBeUndefined();
    expect(bridge.getChannel("new")).toBeNull();
    expect(bridge.listClaims()).toEqual([]);
    expect(bridge.db.query(`SELECT from_state, to_state, reason FROM event_transitions WHERE event_id=${sqlValue(emitted.event.id)} ORDER BY seq;`)).toEqual([
      { from_state: null, to_state: "received", reason: "ingest" },
      { from_state: "received", to_state: "suppressed", reason: "channel_limit_reached" },
    ]);
    expect(bridge.db.query("SELECT key, result_json FROM idempotency_keys")).toEqual([
      { key: input.idempotency_key, result_json: JSON.stringify({ event_id: emitted.event.id }) },
    ]);
    bridge.deleteChannel("existing");
    expect(bridge.emit("fixture", event("new"))).toMatchObject({ suppressed: true, duplicate: true, event: { id: emitted.event.id } });
    expect(bridge.emit("fixture", { ...input, dedupe_key: "different" })).toMatchObject({ suppressed: true, duplicate: true, event: { id: emitted.event.id } });
    expect(bridge.getChannel("new")).toBeNull();
    expect(bridge.listClaims()).toEqual([]);
    expect(bridge.emit("fixture", event("new", "new-event")).claim?.attention_channel).toBe("new");
  });

  it("suppresses implicit registration when another writer consumes the final slot before the write lock", () => {
    const bridge = fixture({ max_channels: 1 });
    const competitor = reopen(bridge);
    const query = bridge.db.query.bind(bridge.db);
    let injected = false;
    vi.spyOn(bridge.db, "query").mockImplementation(<T extends Record<string, unknown>>(sql: string): T[] => {
      if (!injected && sql.includes("BEGIN IMMEDIATE;") && sql.includes("channel_write_guard")) {
        injected = true;
        competitor.createChannel({ name: "Winner", attention_channel: "winner" });
      }
      return query<T>(sql);
    });
    const emitted = bridge.emit("fixture", event("new"));
    expect(injected).toBe(true);
    expect(emitted).toMatchObject({ suppressed: true, duplicate: false, event: { state: "suppressed" } });
    expect(emitted.claim).toBeUndefined();
    expect(bridge.listChannels().map((channel) => channel.attention_channel)).toEqual(["winner"]);
    expect(bridge.db.query(`SELECT reason FROM event_transitions WHERE event_id=${sqlValue(emitted.event.id)} AND to_state='suppressed';`)).toEqual([{ reason: "channel_limit_reached" }]);
  });

  it("rolls back the suppressed event and its idempotency record together on a later failure", () => {
    const bridge = fixture({ max_channels: 1 });
    bridge.createChannel({ name: "Existing", attention_channel: "existing" });
    bridge.db.exec(`CREATE TRIGGER fail_quota_suppression BEFORE INSERT ON idempotency_keys
      BEGIN SELECT RAISE(ABORT, 'fixture suppression failure'); END;`);
    expect(() => bridge.emit("fixture", { ...event("new"), idempotency_key: "quota" })).toThrowError(expect.objectContaining({ error_class: "database_execution_failed" }));
    expect(bridge.listEvents()).toEqual([]);
    expect(bridge.db.query("SELECT * FROM event_status")).toEqual([]);
    expect(bridge.db.query("SELECT * FROM event_transitions")).toEqual([]);
    expect(bridge.db.query("SELECT * FROM idempotency_keys")).toEqual([]);
    expect(bridge.getChannel("new")).toBeNull();
  });

  it("checks quotas under concurrent writers rather than a pre-read count", async () => {
    const bridge = fixture({ max_channels: 1 });
    const coreUrl = new URL("../src/core.ts", import.meta.url).href;
    const run = (channel: string) => new Promise<{ code: string }>((resolve, reject) => {
      const child = spawn(process.execPath, ["--import", "tsx", "--input-type=module", "-e", `
        import { WakeBridge } from ${JSON.stringify(coreUrl)};
        const bridge = new WakeBridge(${JSON.stringify(bridge.config)});
        try { bridge.createChannel({ name: ${JSON.stringify(channel)}, attention_channel: ${JSON.stringify(channel)} }); console.log(JSON.stringify({code:'ok'})); }
        catch (error) { console.log(JSON.stringify({code:error.code})); }
        bridge.close();
      `], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
      let stdout = "";
      let stderr = "";
      const timer = setTimeout(() => { child.kill("SIGTERM"); reject(new Error("channel quota writer timed out")); }, 15_000);
      child.stdout.on("data", (data) => { stdout += String(data); });
      child.stderr.on("data", (data) => { stderr += String(data); });
      child.on("error", (error) => { clearTimeout(timer); reject(error); });
      child.on("exit", (code) => {
        clearTimeout(timer);
        if (code !== 0) return reject(new Error(`writer exited ${code}: ${stderr}`));
        try { resolve(JSON.parse(stdout.trim())); } catch (error) { reject(error); }
      });
    });
    const results = await Promise.all([run("one"), run("two")]);
    expect(results.map((result) => result.code).sort()).toEqual(["channel_limit_reached", "ok"]);
    expect(bridge.listChannels()).toHaveLength(1);
  });

  it("does not consume registry quota when a multi-channel registration aborts partway through", () => {
    const bridge = fixture({ max_channels: 2 });
    bridge.db.exec(`CREATE TRIGGER fail_second_binding BEFORE INSERT ON bindings WHEN NEW.attention_channel='work'
      BEGIN SELECT RAISE(ABORT, 'fixture binding failure'); END;`);
    expect(() => bridge.registerHostSession(registration(), ["life", "work"])).toThrow();
    expect(bridge.listChannels(true)).toEqual([]);
    expect(bridge.listEndpoints()).toEqual([]);
    expect(bridge.listBindings()).toEqual([]);
  });

  it("registers policy-resolved targets rather than hints and keeps previews read-only and quota-equivalent", () => {
    const bridge = fixture({ max_channels: 1 });
    bridge.updatePolicy({ id: "routed", version: 1, match: { type: "job" }, delivery: { mode: "immediate" }, target: { attention_channel: "actual" } });
    expect(bridge.previewPolicy("fixture", event("ignored")).claim?.attention_channel).toBe("actual");
    expect(bridge.listChannels()).toEqual([]);
    expect(bridge.emit("fixture", event("ignored")).claim?.attention_channel).toBe("actual");
    expect(bridge.getChannel("ignored")).toBeNull();
    expect(bridge.previewPolicy("fixture", { ...event("new", "preview"), type: "other" })).toMatchObject({ suppressed: true, claim: null, batch: null });
    bridge.deleteChannel("actual", { force: true });
    expect(bridge.previewPolicy("fixture", event("ignored", "after")).suppressed).toBe(true);
    expect(bridge.previewPolicy("fixture", event("ignored", "after")).claim).toBeNull();
    expect(bridge.emit("fixture", event("ignored", "after"))).toMatchObject({ suppressed: true, event: { state: "suppressed" } });
  });

  it("rejects default deletion with all direct occupancy details and no durable writes", () => {
    const bridge = fixture();
    const opened = bridge.registerHostSession(registration(), ["life", "work"]);
    bridge.configureActivityWatch({ attention_channel: "life", enabled: true, idle_after_ms: 60_000 });
    for (const version of [1, 2]) bridge.updatePolicy({ id: "routed", version, match: { type: "routed" }, delivery: { mode: "immediate" }, target: { attention_channel: "life" } });
    bridge.updatePolicy({ id: "suppress", version: 1, match: { type: "quiet" }, delivery: { mode: "suppress" }, target: { attention_channel: "life" } });
    bridge.emit("fixture", event("life"));
    bridge.tick();
    const before = bridge.inspect();
    const channels = bridge.listChannels(true);
    const transitions = bridge.db.query("SELECT * FROM event_transitions UNION ALL SELECT * FROM claim_transitions UNION ALL SELECT * FROM batch_transitions;");
    for (const options of [undefined, { force: false }]) {
      expect(() => bridge.deleteChannel("life", options)).toThrowError(expect.objectContaining({
        code: "channel_in_use", status: 409,
        usage: { binding: { endpoint_id: opened.endpoint.id, generation: 1 }, activity_watch: true, policies: [{ id: "routed", version: 2 }, { id: "suppress", version: 1 }] },
      }));
      expect(bridge.inspect()).toEqual(before);
      expect(bridge.listChannels(true)).toEqual(channels);
      expect(bridge.db.query("SELECT * FROM event_transitions UNION ALL SELECT * FROM claim_transitions UNION ALL SELECT * FROM batch_transitions;")).toEqual(transitions);
    }
  });

  it.each(["true", 1, null, {}, []])("does not treat non-boolean force=%j as permission to delete", (force) => {
    const bridge = fixture();
    bridge.createChannel({ name: "Life", attention_channel: "life" });
    const before = bridge.listChannels(true);
    expect(() => bridge.deleteChannel("life", { force: force as boolean })).toThrowError(expect.objectContaining({ code: "invalid_arguments", status: 400 }));
    expect(bridge.listChannels(true)).toEqual(before);
  });

  it("does not block deletion for disabled watches, disabled or historical policies, legacy rules, or dynamic targets", () => {
    const bridge = fixture();
    bridge.createChannel({ name: "Life", attention_channel: "life" });
    bridge.configureActivityWatch({ attention_channel: "life", enabled: false, idle_after_ms: 60_000 });
    bridge.updatePolicy({ id: "disabled", version: 1, delivery: { mode: "immediate" }, target: { attention_channel: "life" } });
    bridge.updatePolicy({ id: "disabled", version: 2, enabled: false, delivery: { mode: "immediate" }, target: { attention_channel: "life" } });
    bridge.updatePolicy({ id: "historical", version: 1, delivery: { mode: "immediate" }, target: { attention_channel: "life" } });
    bridge.updatePolicy({ id: "historical", version: 2, delivery: { mode: "immediate" }, target: { attention_channel: "work" } });
    bridge.updatePolicy({ id: "dynamic", version: 1, delivery: { mode: "immediate" }, target: { attention_channel: "${attention_channel_hint}" } });
    for (const legacy of [
      { id: "legacy-mode", version: 1, enabled: true, delivery: { mode: "debounce" }, target: { attention_channel: "life" } },
      { id: "legacy-field", version: 1, enabled: true, delivery: { mode: "immediate", debounce_ms: 1 }, target: { attention_channel: "life" } },
      { id: "legacy-target", version: 1, enabled: true, delivery: { mode: "immediate" }, target: { attention_channel: "life", prefer_transport: "warm" } },
    ]) bridge.db.exec(`INSERT INTO policies(id, version, enabled, order_no, rule_json, installed_at)
      VALUES(${sqlValue(legacy.id)}, 1, 1, 0, ${sqlJson(legacy)}, ${sqlValue(bridge.nowIso())});`);
    expect(bridge.listPolicyStatuses().filter((policy) => policy.id.startsWith("legacy-")).map((policy) => policy.new_event_matching)).toEqual(["legacy_unsupported", "legacy_unsupported", "legacy_unsupported"]);
    expect(bridge.deleteChannel("life").deleted_at).not.toBeNull();
    bridge.createChannel({ name: "Default", attention_channel: "default" });
    expect(bridge.deleteChannel("default").deleted_at).not.toBeNull();
  });

  it.each(["binding", "watch", "policy"])("checks a competing %s insertion under the same writer lock as deletion", (entry) => {
    const bridge = fixture();
    bridge.createChannel({ name: "Life", attention_channel: "life" });
    const endpoint = bridge.registerEndpoint(registration());
    const competitor = reopen(bridge);
    const query = bridge.db.query.bind(bridge.db);
    let injected = false;
    let afterInsertion: ReturnType<WakeBridge["inspect"]> | undefined;
    vi.spyOn(bridge.db, "query").mockImplementation(<T extends Record<string, unknown>>(sql: string): T[] => {
      if (!injected && sql.includes("BEGIN IMMEDIATE;") && sql.includes("channel_delete_guard")) {
        injected = true;
        if (entry === "binding") competitor.takeover("life", endpoint.id);
        if (entry === "watch") competitor.configureActivityWatch({ attention_channel: "life", enabled: true, idle_after_ms: 60_000 });
        if (entry === "policy") competitor.updatePolicy({ id: "racing", version: 1, delivery: { mode: "immediate" }, target: { attention_channel: "life" } });
        afterInsertion = competitor.inspect();
      }
      return query<T>(sql);
    });
    expect(() => bridge.deleteChannel("life")).toThrowError(expect.objectContaining({
      code: "channel_in_use", status: 409,
      usage: {
        binding: entry === "binding" ? { endpoint_id: endpoint.id, generation: 1 } : null,
        activity_watch: entry === "watch",
        policies: entry === "policy" ? [{ id: "racing", version: 1 }] : [],
      },
    }));
    expect(injected).toBe(true);
    expect(bridge.getChannel("life")?.deleted_at).toBeNull();
    expect(bridge.inspect()).toEqual(afterInsertion);
  });

  it("suppresses a source event if deletion wins after routing but before its write transaction", () => {
    const bridge = fixture();
    bridge.createChannel({ name: "Life", attention_channel: "life" });
    const competitor = reopen(bridge);
    const query = bridge.db.query.bind(bridge.db);
    let injected = false;
    vi.spyOn(bridge.db, "query").mockImplementation(<T extends Record<string, unknown>>(sql: string): T[] => {
      if (!injected && sql.includes("BEGIN IMMEDIATE;") && sql.includes("channel_write_guard")) {
        injected = true;
        competitor.deleteChannel("life");
      }
      return query<T>(sql);
    });
    const emitted = bridge.emit("fixture", event("life"));
    expect(injected).toBe(true);
    expect(emitted).toMatchObject({ suppressed: true, duplicate: false, event: { state: "suppressed" } });
    expect(emitted.claim).toBeUndefined();
    expect(bridge.listClaims()).toEqual([]);
    expect(bridge.emit("fixture", event("life"))).toMatchObject({ suppressed: true, duplicate: true });
  });

  it("persists deleted-route source events without poisoning a page cursor or rerouting its other events", async () => {
    const bridge = fixture();
    bridge.createChannel({ name: "Life", attention_channel: "life" });
    bridge.deleteChannel("life");
    const adapter: PullSourceAdapter = {
      manifest: {
        contract_version: 1, id: "fixture", version: "1", subject_ref: "fixture:owner", binding_fingerprint: `sha256:${"1".repeat(64)}`,
        read_side_effects: "none", credential_custody: "none", upstream_credential_breadth: "none", upstream_credential_scopes: [], connector_capabilities: ["events.read"],
      },
      poll: () => ({ events: [event("life"), event("work")], next_cursor: { page: 2 } }),
    };
    const runner = new SourceRunner(bridge);
    expect(await runner.runOnce(adapter)).toMatchObject({ events_seen: 2, events_inserted: 2, checkpoint_revision: 1 });
    expect(runner.checkpoints.get("fixture")?.cursor).toEqual({ page: 2 });
    expect(bridge.listEvents().map((item) => item.state).sort()).toEqual(["matched", "suppressed"]);
    expect(bridge.listClaims()).toMatchObject([{ attention_channel: "work" }]);
    expect(await runner.runOnce(adapter)).toMatchObject({ events_duplicate: 2, checkpoint_revision: 2 });
  });

  it("persists an over-quota middle source event while continuing the page, advancing its cursor, and keeping source health", async () => {
    const bridge = fixture({ max_channels: 1 });
    bridge.createChannel({ name: "Life", attention_channel: "life" });
    const adapter: PullSourceAdapter = {
      manifest: {
        contract_version: 1, id: "fixture", version: "1", subject_ref: "fixture:owner", binding_fingerprint: `sha256:${"1".repeat(64)}`,
        read_side_effects: "none", credential_custody: "none", upstream_credential_breadth: "none", upstream_credential_scopes: [], connector_capabilities: ["events.read"],
      },
      poll: () => ({ events: [event("life", "first"), event("typo-life", "middle"), event("life", "last")], next_cursor: { page: 2 } }),
    };
    const runner = new SourceRunner(bridge);
    expect(await runner.runOnce(adapter)).toMatchObject({ events_seen: 3, events_inserted: 3, checkpoint_revision: 1 });
    expect(runner.checkpoints.get("fixture")?.cursor).toEqual({ page: 2 });
    expect(bridge.listEvents().map((item) => [item.dedupe_key, item.state]).sort()).toEqual([["first", "matched"], ["last", "matched"], ["middle", "suppressed"]]);
    expect(bridge.listClaims()).toHaveLength(2);
    expect(bridge.listClaims().every((claim) => claim.attention_channel === "life")).toBe(true);
    expect(bridge.getChannel("typo-life")).toBeNull();
    const supervisor = new SourceSupervisor(bridge, [{ id: "fixture", base_url: "http://127.0.0.1:4391", token_env: "FIXTURE_TOKEN", enabled: false }], { FIXTURE_TOKEN: "test-token" },
      async (input) => Response.json(String(input).endsWith("/v1/manifest") ? adapter.manifest : {
        events: [event("life", "supervised-first"), event("typo-life", "supervised-middle"), event("life", "supervised-last")], next_cursor: { page: 3 },
      }));
    try {
      expect(await supervisor.runOnce("fixture")).toMatchObject({ state: "healthy", last_error_class: null, consecutive_failures: 0, checkpoint_revision: 2, last_run: { events_seen: 3, events_inserted: 3, events_duplicate: 0 } });
    } finally { await supervisor.stop(); }
    expect(runner.checkpoints.get("fixture")?.cursor).toEqual({ page: 3 });
    expect(bridge.listClaims()).toHaveLength(4);
    expect(bridge.listEvents().filter((item) => item.state === "suppressed")).toHaveLength(2);
  });

  it.each(["schedule", "watch", "takeover", "presence", "activity", "ack", "consume"])("fences %s when deletion wins between its pre-read and writer lock", async (entry) => {
    const bridge = fixture();
    const opened = bridge.registerHostSession(registration(), ["life", "work"]);
    bridge.configureActivityWatch({ attention_channel: "life", enabled: true, idle_after_ms: 60_000 });
    const emitted = bridge.emit("fixture", event("life"));
    bridge.tick();
    await bridge.dispatchDue();
    const batch = bridge.listBatches({ channel: "life" })[0];
    const competitor = reopen(bridge);
    const query = bridge.db.query.bind(bridge.db);
    let afterDeletion: ReturnType<WakeBridge["inspect"]> | undefined;
    let injected = false;
    vi.spyOn(bridge.db, "query").mockImplementation(<T extends Record<string, unknown>>(sql: string): T[] => {
      if (!injected && sql.includes("BEGIN IMMEDIATE;") && sql.includes("channel_write_guard")) {
        injected = true;
        competitor.deleteChannel("life", { force: true });
        afterDeletion = competitor.inspect();
      }
      return query<T>(sql);
    });
    const write = () => {
      if (entry === "schedule") return bridge.scheduleClaim({ attention_channel: "life", eligible_after: bridge.nowIso(), resource: { uri: "job://race" } });
      if (entry === "watch") return bridge.configureActivityWatch({ attention_channel: "life", enabled: true, idle_after_ms: 120_000 });
      if (entry === "takeover") return bridge.takeover("life", opened.endpoint.id, 1);
      if (entry === "presence") return bridge.renewPresence({ attention_channel: "life", endpoint_id: opened.endpoint.id, generation: 1, lease_token: opened.endpoint.lease_token, observed_by: "racing" });
      if (entry === "activity") return bridge.observeActivity({ attention_channel: "life", endpoint_id: opened.endpoint.id, generation: 1, lease_token: opened.endpoint.lease_token, observation_id: "racing", kind: "activity_started" });
      if (entry === "ack") return bridge.ackBatch(batch.id, opened.endpoint.id, 1);
      return bridge.consumeClaim(emitted.claim!.id);
    };
    expect(write).toThrowError(expect.objectContaining({ code: "channel_deleted", status: 409 }));
    expect(injected).toBe(true);
    expect(bridge.inspect()).toEqual(afterDeletion);
  });

  it("does not rotate the shared endpoint or partially register another channel when deletion wins an open race", () => {
    const bridge = fixture();
    const opened = bridge.registerHostSession(registration(), ["life", "work"]);
    const competitor = reopen(bridge);
    const query = bridge.db.query.bind(bridge.db);
    let injected = false;
    vi.spyOn(bridge.db, "query").mockImplementation(<T extends Record<string, unknown>>(sql: string): T[] => {
      if (!injected && sql.includes("CREATE TEMP TABLE host_session_guard")) {
        injected = true;
        competitor.deleteChannel("life", { force: true });
      }
      return query<T>(sql);
    });
    expect(() => bridge.registerHostSession({ ...registration(), session_ref: "changed" }, ["life", "work", "extra"]))
      .toThrowError(expect.objectContaining({ code: "channel_deleted", status: 409 }));
    expect(bridge.getChannel("extra")).toBeNull();
    expect(bridge.getBinding("work")).toEqual(opened.bindings[1]);
    expect(bridge.renewEndpoint(opened.endpoint.id, opened.endpoint.lease_token!).session_ref).toBe("ep");
  });

  it("force-deletes only one shared-endpoint route, preserves history, and allows complete re-registration of the survivor", async () => {
    const bridge = fixture();
    const opened = bridge.registerHostSession(registration(), ["life", "work"]);
    const policy = bridge.updatePolicy({ id: "life-route", version: 1, match: { type: "guard-only" }, delivery: { mode: "immediate" }, target: { attention_channel: "life" } });
    for (const binding of opened.bindings) {
      bridge.configureActivityWatch({ attention_channel: binding.attention_channel, enabled: true, idle_after_ms: 60_000, mode: "repeat" });
      bridge.renewPresence({ attention_channel: binding.attention_channel, endpoint_id: opened.endpoint.id, generation: binding.generation, lease_token: opened.endpoint.lease_token, observed_by: "fixture" });
    }
    const life = bridge.emit("fixture", event("life"));
    const work = bridge.emit("fixture", event("work"));
    bridge.tick();
    const deleted = bridge.deleteChannel("life", { force: true });
    expect(bridge.deleteChannel("life")).toEqual(deleted);
    expect(bridge.getBinding("life")).toBeNull();
    expect(bridge.getPresence("life")).toBeNull();
    expect(bridge.getActivityWatch("life")).toMatchObject({ enabled: false, next_due_at: null, activity_lease_expires_at: null });
    expect(bridge.getClaim(life.claim!.id)).toMatchObject({ state: "dismissed", dismissed_reason: "channel_deleted" });
    expect(bridge.getEvent(life.event.id)?.state).toBe("expired");
    expect(bridge.getClaim(work.claim!.id)?.state).not.toBe("dismissed");
    expect(bridge.getBinding("work")).toEqual(opened.bindings[1]);
    expect(bridge.getPresence("work")).not.toBeNull();
    expect(bridge.getActivityWatch("work")?.enabled).toBe(true);
    expect(bridge.listPolicies().find((item) => item.id === policy.id)).toEqual(policy);
    expect(bridge.listReceipts().filter((receipt) => receipt.stage === "agent_consumed")).toEqual([]);
    const reopened = bridge.registerHostSession(registration(), ["work"]);
    expect(reopened.bindings).toMatchObject([{ attention_channel: "work", generation: 2 }]);
    expect(bridge.getChannel("life")?.deleted_at).not.toBeNull();
    expect(bridge.emit("fixture", event("life"))).toMatchObject({ duplicate: true, event: { id: life.event.id, state: "expired" } });
  });

  it("rolls the entire delete back if a later cleanup statement fails", () => {
    const bridge = fixture();
    bridge.registerHostSession(registration(), ["life", "work"]);
    bridge.emit("fixture", event("life"));
    bridge.tick();
    bridge.db.exec(`CREATE TRIGGER fail_delete BEFORE UPDATE ON claims WHEN NEW.dismissed_reason='channel_deleted'
      BEGIN SELECT RAISE(ABORT, 'fixture delete failure'); END;`);
    const channels = bridge.listChannels(true);
    const state = bridge.inspect();
    expect(() => bridge.deleteChannel("life", { force: true })).toThrowError(expect.objectContaining({ error_class: "database_execution_failed" }));
    expect(bridge.listChannels(true)).toEqual(channels);
    expect(bridge.inspect()).toEqual(state);
  });

  it("retains the survivor's outstanding nonce and accepts its ack and consumption after a shared-endpoint delete", async () => {
    const contexts: TransportContext[] = [];
    const transport: WakeTransport = {
      kind: "observable", capabilities: { session_activity_observable: true },
      dispatch(input) { contexts.push(input); return { accepted: true, transport_kind: "observable" }; },
    };
    const bridge = fixture({}, { transports: [transport], autoMockTransport: false });
    const opened = bridge.registerHostSession({ ...registration(), routes: [{ kind: "observable", address: {} }] }, ["life", "work"]);
    bridge.emit("fixture", event("life"));
    const work = bridge.emit("fixture", event("work"));
    bridge.tick();
    await bridge.dispatchDue();
    await bridge.dispatchDue();
    expect(contexts).toHaveLength(2);
    const workContext = contexts.find((item) => item.batch.attention_channel === "work")!;
    bridge.deleteChannel("life", { force: true });
    expect(bridge.listDeliveryCorrelations().find((item) => item.batch_id === workContext.batch.id)?.state).toBe("outstanding");
    expect(bridge.consumeWakeEcho({ attention_channel: "work", endpoint_id: opened.endpoint.id, generation: 1, lease_token: opened.endpoint.lease_token, observation_id: "work-echo", delivery_nonce: workContext.delivery_nonce }).duplicate).toBe(false);
    expect(bridge.ackBatch(workContext.batch.id, opened.endpoint.id, 1).state).toBe("seen");
    expect(bridge.consumeClaim(work.claim!.id).state).toBe("consumed");
    const lifeBatch = contexts.find((item) => item.batch.attention_channel === "life")!.batch.id;
    expect(bridge.listReceipts(lifeBatch).map((receipt) => receipt.stage)).toEqual(["transport_accepted"]);
  });

  it("fences in-flight transport completions and late acknowledgements without fabricating consumption", async () => {
    let context: TransportContext | undefined;
    let release: (result: TransportResult) => void = () => { throw new Error("transport not entered"); };
    const transport: WakeTransport = {
      kind: "held", capabilities: { session_activity_observable: true },
      dispatch(input) { context = input; return new Promise((resolve) => { release = resolve; }); },
    };
    const bridge = fixture({}, { transports: [transport], autoMockTransport: false });
    const opened = bridge.registerHostSession({ ...registration(), routes: [{ kind: "held", address: {} }] }, ["life", "work"]);
    const emitted = bridge.emit("fixture", event("life"));
    bridge.tick();
    const dispatching = bridge.dispatchDue();
    expect(context).toBeDefined();
    bridge.deleteChannel("life", { force: true });
    expect(bridge.getBatch(context!.batch.id)?.state).toBe("cancelled");
    expect(bridge.listAttempts(context!.batch.id)).toMatchObject([{ state: "failed", error_class: "channel_deleted" }]);
    expect(bridge.listDeliveryCorrelations()).toMatchObject([{ state: "discarded" }]);
    release({ accepted: true, transport_kind: "held" });
    expect(await dispatching).toMatchObject([{ status: "skipped" }]);
    expect(bridge.getBatch(context!.batch.id)?.state).toBe("cancelled");
    expect(bridge.getClaim(emitted.claim!.id)?.state).toBe("dismissed");
    expect(bridge.listReceipts(context!.batch.id)).toEqual([]);
    expect(() => bridge.ackBatch(context!.batch.id, opened.endpoint.id, 1)).toThrowError(expect.objectContaining({ code: "channel_deleted" }));
    expect(() => bridge.consumeWakeEcho({ attention_channel: "life", endpoint_id: opened.endpoint.id, generation: 1, lease_token: opened.endpoint.lease_token, observation_id: "late", delivery_nonce: context!.delivery_nonce }))
      .toThrowError(expect.objectContaining({ code: "channel_deleted" }));
    expect(bridge.getBinding("work")).toEqual(opened.bindings[1]);
  });

  it("rejects old owner and host writes after deletion and keeps tombstones effective after restart", () => {
    const bridge = fixture();
    const opened = bridge.registerHostSession(registration(), ["life", "work"]);
    const scheduled = bridge.scheduleClaim({ attention_channel: "life", resource: { uri: "job://later" }, eligible_after: bridge.nowIso(), idempotency_key: "same" });
    bridge.deleteChannel("life", { force: true });
    const restarted = reopen(bridge);
    const writes = [
      () => restarted.scheduleClaim({ attention_channel: "life", resource: { uri: "job://later" }, eligible_after: bridge.nowIso(), idempotency_key: "same" }),
      () => restarted.takeover("life", opened.endpoint.id),
      () => restarted.registerHostSession(registration(), ["life", "work"]),
      () => restarted.configureActivityWatch({ attention_channel: "life", enabled: true, idle_after_ms: 60_000 }),
      () => restarted.renewPresence({ attention_channel: "life", endpoint_id: opened.endpoint.id, generation: 1, lease_token: opened.endpoint.lease_token, observed_by: "late" }),
      () => restarted.observeActivity({ attention_channel: "life", endpoint_id: opened.endpoint.id, generation: 1, lease_token: opened.endpoint.lease_token, observation_id: "late", kind: "activity_started" }),
      () => restarted.consumeClaim(scheduled.claim.id),
    ];
    for (const write of writes) expect(write).toThrowError(expect.objectContaining({ code: "channel_deleted", status: 409 }));
    expect(restarted.emit("fixture", event("life", "after-restart"))).toMatchObject({ suppressed: true, event: { state: "suppressed" } });
    expect(restarted.tick().batches_created).toEqual([]);
    expect(restarted.listBindings().map((binding) => binding.attention_channel)).toEqual(["work"]);
  });
});
