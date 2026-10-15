import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WakeBridge } from "../src/core.js";
import {
  SourceCheckpointStore,
  SourceRunner,
} from "../src/source-adapter.js";
import {
  BotlingKnowsSourceAdapter,
  botlingKnowsCursorFromNow,
  type BotlingKnowsNotificationClient,
  type BotlingKnowsNotificationSummary,
  BOTLING_KNOWS_DEFAULT_WAKE_TYPES,
} from "../src/adapters/botlingknows.js";
import { validatePolicyFile } from "../src/policy-control.js";
import type { PullSourceAdapter, SourcePollContext } from "../src/types.js";

function dbPath(): string {
  return join(mkdtempSync(join(tmpdir(), "wake-bridge-source-")), "bridge.sqlite");
}

function bridge(path = dbPath()): WakeBridge {
  return new WakeBridge({
    instance_id: "source-instance",
    owner_id: "source-owner",
    db_path: path,
    timezone: "UTC",
  });
}

function manualAdapter(poll: PullSourceAdapter["poll"]): PullSourceAdapter {
  return {
    manifest: {
      contract_version: 1,
      id: "source_fixture",
      version: "1.0.0",
      subject_ref: "fixture:owner",
      binding_fingerprint: `sha256:${"1".repeat(64)}`,
      read_side_effects: "none",
      credential_custody: "connector",
      upstream_credential_breadth: "unknown",
      upstream_credential_scopes: [],
      connector_capabilities: ["events.read"],
    },
    poll,
  };
}

describe("source adapter contract", () => {
  it("commits a cursor only after every event is durable and replays a partial page safely", async () => {
    const b = bridge();
    const runner = new SourceRunner(b);
    let broken = true;
    const adapter = manualAdapter(() => ({
      events: [
        { type: "one", dedupe_key: "one", resource: { uri: "fixture://one" } },
        broken
          ? ({ type: "two", dedupe_key: "two", resource: { uri: "not-an-absolute-uri" } })
          : ({ type: "two", dedupe_key: "two", resource: { uri: "fixture://two" } }),
      ],
      next_cursor: "page-1",
    }));
    await expect(runner.runOnce(adapter)).rejects.toMatchObject({ code: "invalid_resource" });
    expect(b.listEvents()).toHaveLength(1);
    expect(runner.checkpoints.get("source_fixture")).toBeNull();

    broken = false;
    await expect(runner.runOnce(adapter)).resolves.toMatchObject({
      events_seen: 2,
      events_inserted: 1,
      events_duplicate: 1,
      checkpoint_revision: 1,
    });
    expect(b.listEvents()).toHaveLength(2);
    expect(runner.checkpoints.get("source_fixture")?.cursor).toBe("page-1");
    b.close();
  });

  it("persists an opaque bounded cursor across restart", async () => {
    const path = dbPath();
    const first = bridge(path);
    const firstRunner = new SourceRunner(first);
    await firstRunner.runOnce(manualAdapter(() => ({
      events: [],
      next_cursor: { page: "opaque", high: 42 },
    })));
    first.close();

    const second = bridge(path);
    let observed: SourcePollContext | undefined;
    const result = await new SourceRunner(second).runOnce(manualAdapter((context) => {
      observed = context;
      return { events: [], next_cursor: context.cursor };
    }));
    expect(observed?.cursor).toEqual({ page: "opaque", high: 42 });
    expect(result.checkpoint_revision).toBe(2);
    second.close();
  });

  it("fails closed when a pre-identity checkpoint is migrated", async () => {
    const b = bridge();
    b.db.exec(`INSERT INTO source_checkpoints(instance_id, source, adapter_version, cursor_json, revision, updated_at)
      VALUES('source-instance', 'source_fixture', '0.8.0', 'null', 14, '2026-08-29T00:00:00.000Z');`);
    let polls = 0;
    await expect(new SourceRunner(b).runOnce(manualAdapter(() => {
      polls += 1;
      return { events: [], next_cursor: null };
    }))).rejects.toMatchObject({ code: "source_identity_mismatch" });
    expect(polls).toBe(0);
    expect(new SourceCheckpointStore(b).get("source_fixture")).toMatchObject({
      revision: 14,
      subject_ref: "",
      binding_fingerprint: "",
    });
    b.close();
  });

  it("fails closed on side effects and in-process broad credentials, but accepts a narrow connector boundary", async () => {
    const b = bridge();
    const runner = new SourceRunner(b);
    let calls = 0;
    const unsafe: PullSourceAdapter = {
      manifest: {
        contract_version: 1,
        id: "unsafe",
        version: "1",
        subject_ref: "unsafe:owner",
        binding_fingerprint: `sha256:${"2".repeat(64)}`,
        read_side_effects: "marks_read",
        credential_custody: "wake_bridge_process",
        upstream_credential_breadth: "broad",
        upstream_credential_scopes: ["everything"],
        connector_capabilities: ["notifications.read"],
      },
      poll: () => { calls += 1; return { events: [], next_cursor: null }; },
    };
    await expect(runner.runOnce(unsafe)).rejects.toMatchObject({ code: "unsafe_source_read" });
    expect(calls).toBe(0);

    const inProcessBroad = { ...unsafe, manifest: { ...unsafe.manifest, read_side_effects: "none" as const } };
    await expect(runner.runOnce(inProcessBroad)).rejects.toMatchObject({ code: "unsafe_source_credential" });
    expect(calls).toBe(0);

    const connectorBroad = {
      ...unsafe,
      manifest: {
        ...unsafe.manifest,
        read_side_effects: "none" as const,
        credential_custody: "connector" as const,
      },
    };
    await expect(runner.runOnce(connectorBroad)).resolves.toMatchObject({ events_seen: 0 });
    expect(calls).toBe(1);

    await expect(runner.runOnce(inProcessBroad, { allow_in_process_broad_credentials: true }))
      .resolves.toMatchObject({ events_seen: 0 });
    expect(calls).toBe(2);

    const invalidCredentialFree: PullSourceAdapter = {
      ...unsafe,
      manifest: {
        ...unsafe.manifest,
        read_side_effects: "none",
        credential_custody: "none",
        upstream_credential_breadth: "broad",
      },
    };
    await expect(runner.runOnce(invalidCredentialFree)).rejects.toMatchObject({ code: "invalid_source_manifest" });
    expect(calls).toBe(2);

    await expect(runner.runOnce(manualAdapter(() => ({
      events: Array.from({ length: 2 }, (_, index) => ({ type: "x", dedupe_key: String(index), resource: { uri: `fixture://${index}` } })),
      next_cursor: null,
    })), { limit: 1 })).rejects.toMatchObject({ code: "invalid_source_page" });

    const left = new SourceCheckpointStore(b);
    const right = new SourceCheckpointStore(b);
    const raceManifest = { ...manualAdapter(() => ({ events: [], next_cursor: null })).manifest, id: "race", version: "1" };
    left.commit(raceManifest, "left", 0);
    expect(() => right.commit(raceManifest, "right", 0)).toThrowError(/concurrently/);
    expect(left.get("race")?.cursor).toBe("left");
    b.db.exec("UPDATE source_checkpoints SET cursor_json='{broken' WHERE source='race';");
    expect(() => left.get("race")).toThrowError(/corrupt/);
    b.close();
  });
});

describe("botlingknows connector-backed source", () => {
  const marker = (id: number, created_at = "2026-10-15T01:00:00.000001Z") => ({ id, created_at });
  const summary = (values: Record<string, [number, number | null]>): BotlingKnowsNotificationSummary => ({
    unread_by_type: Object.fromEntries(Object.entries(values).map(([type, [count]]) => [type, count])),
    latest_unread_by_type: Object.fromEntries(Object.entries(values).map(([type, [, id]]) => [type, id === null ? null : marker(id)])),
  });

  it("detects equal-count arrivals and never lowers per-type watermarks across read, withdrawal or restart", async () => {
    const calls: unknown[] = [];
    let current = summary({ new_reply: [3, 101], liked: [8, 100] });
    const client: BotlingKnowsNotificationClient = { notifications(input) { calls.push(input); return current; } };
    const path = dbPath();
    let b = bridge(path);
    let runner = new SourceRunner(b);
    let adapter = new BotlingKnowsSourceAdapter(client);
    await expect(runner.runOnce(adapter)).resolves.toMatchObject({ events_inserted: 2 });
    await expect(runner.runOnce(adapter)).resolves.toMatchObject({ events_inserted: 0 });
    current = summary({ new_reply: [3, 102], liked: [8, 100] });
    await expect(runner.runOnce(adapter)).resolves.toMatchObject({ events_inserted: 1 });
    current = summary({ new_reply: [2, 98], liked: [0, null] });
    await expect(runner.runOnce(adapter)).resolves.toMatchObject({ events_inserted: 0 });
    current = summary({ new_reply: [0, null], liked: [0, null] });
    await runner.runOnce(adapter);
    expect(runner.checkpoints.get("botlingknows")?.cursor).toEqual({
      v: 2, high_watermarks: { new_reply: marker(102), liked: marker(100) },
    });
    b.close();
    b = bridge(path);
    runner = new SourceRunner(b);
    adapter = new BotlingKnowsSourceAdapter(client);
    current = summary({ new_reply: [1, 102], liked: [1, 103] });
    await expect(runner.runOnce(adapter)).resolves.toMatchObject({ events_inserted: 1 });
    const event = b.listEvents({ source: "botlingknows" }).find((e) => e.dedupe_key === "notification:2026-10-15T01:00:00.000001Z:102");
    expect(event).toMatchObject({
      type: "new_reply", resource: { uri: "botlingknows://notifications", cursor: "2026-10-15T01:00:00.000001Z:102" },
      metadata: { unread_count: 3, latest_unread: marker(102), default_wake: true,
        next_action: { action: "notifications", payload: { mode: "list" } } },
    });
    expect(calls.every((call) => JSON.stringify(call) === '{"mode":"summary"}')).toBe(true);
    b.close();
  });

  it("keeps pending types eligible under a small Bridge limit and replays deterministically", async () => {
    const adapter = new BotlingKnowsSourceAdapter({ notifications: () => summary({
      new_reply: [1, 8], mention: [1, 10], invite_family: [2, 9],
    }) });
    const first = await adapter.poll({ cursor: null, limit: 1 });
    expect(first.events.map((e) => e.type)).toEqual(["new_reply"]);
    expect(first.has_more).toBe(true);
    const replay = await adapter.poll({ cursor: null, limit: 1 });
    expect(replay).toEqual(first);
    const second = await adapter.poll({ cursor: first.next_cursor, limit: 1 });
    expect(second.events.map((e) => e.type)).toEqual(["invite_family"]);
    const last = await adapter.poll({ cursor: second.next_cursor, limit: 1 });
    expect(last.events.map((e) => e.type)).toEqual(["mention"]);
    expect(last.has_more).toBe(false);
  });

  it("fails closed on ID-only checkpoints without overwriting the old baseline", async () => {
    const b = bridge();
    const runner = new SourceRunner(b);
    let polls = 0;
    const adapter = new BotlingKnowsSourceAdapter({ notifications: () => {
      polls++; return summary({ new_reply: [1, 9] });
    } });
    const previous = runner.checkpoints.commit({ ...adapter.manifest, version: "0.1.0" },
      { v: 1, high_watermark: 7, page_cursor: "page-2", pending_max_id: 9 }, 0);
    await expect(runner.runOnce(adapter)).rejects.toMatchObject({ code: "invalid_source_cursor" });
    expect(polls).toBe(0);
    expect(runner.checkpoints.get("botlingknows")).toEqual(previous);
    b.close();
  });

  it("uses microsecond timestamps ahead of IDs for both watermark and durable dedupe", async () => {
    const b = bridge();
    const runner = new SourceRunner(b);
    let current = summary({ new_reply: [1, 101] });
    const adapter = new BotlingKnowsSourceAdapter({ notifications: () => current });
    await runner.runOnce(adapter);
    current.latest_unread_by_type.new_reply = marker(101, "2026-10-15T01:00:00.000002Z");
    await expect(runner.runOnce(adapter)).resolves.toMatchObject({ events_inserted: 1 });
    current.latest_unread_by_type.new_reply = marker(1, "2026-10-15T01:00:00.000003Z");
    await expect(runner.runOnce(adapter)).resolves.toMatchObject({ events_inserted: 1 });
    await expect(runner.runOnce(adapter)).resolves.toMatchObject({ events_inserted: 0 });
    current.latest_unread_by_type.new_reply = marker(999, "2026-10-15T01:00:00.000002Z");
    await expect(runner.runOnce(adapter)).resolves.toMatchObject({ events_inserted: 0 });
    expect(new Set(b.listEvents().map((e) => e.dedupe_key)).size).toBe(3);
    expect(runner.checkpoints.get("botlingknows")?.cursor).toEqual({
      v: 2, high_watermarks: { new_reply: marker(1, "2026-10-15T01:00:00.000003Z") },
    });
    b.close();
  });

  it("bootstraps from-now using only the non-consuming summary", async () => {
    const calls: unknown[] = [];
    const client: BotlingKnowsNotificationClient = { notifications(input) {
      calls.push(input); return summary({ new_reply: [2, 22], liked: [0, null] });
    } };
    const cursor = await botlingKnowsCursorFromNow(client);
    expect(cursor).toEqual({ v: 2, high_watermarks: { new_reply: marker(22) } });
    expect((await new BotlingKnowsSourceAdapter(client).poll({ cursor, limit: 20 })).events).toEqual([]);
    expect(calls).toEqual([{ mode: "summary" }, { mode: "summary" }]);
  });

  it.each([
    { unread_by_type: { mention: 1 } },
    { unread_by_type: { mention: 1 }, latest_unread_by_type: { mention: null } },
    { unread_by_type: { mention: 0 }, latest_unread_by_type: { mention: 4 } },
    { unread_by_type: { mention: -1 }, latest_unread_by_type: { mention: 4 } },
    { unread_by_type: { mention: 1 }, latest_unread_by_type: { mention: "4" } },
    { unread_by_type: { mention: 1 }, latest_unread_by_type: { mention: marker(4, "2026-10-15T01:00:00.000Z") } },
  ])("does not advance checkpoints for unsupported or malformed summaries %j", async (value) => {
    const b = bridge();
    const runner = new SourceRunner(b);
    const adapter = new BotlingKnowsSourceAdapter({ notifications: () => value as any });
    await expect(runner.runOnce(adapter)).rejects.toMatchObject({ code: "invalid_source_page" });
    expect(runner.checkpoints.get("botlingknows")).toBeNull();
    b.close();
  });

  it("recommends immediate content, mention and human-invite delivery while allowing personal type overrides", async () => {
    const b = bridge();
    const recipe = validatePolicyFile(JSON.parse(readFileSync(
      new URL("../examples/policies/botlingknows-conservative.json", import.meta.url), "utf8",
    ))).policies;
    const types = [...BOTLING_KNOWS_DEFAULT_WAKE_TYPES, "liked", "useful", "invite_system", "affair_progress", "future_type"];
    const adapter = new BotlingKnowsSourceAdapter({ notifications: () => summary(
      Object.fromEntries(types.map((type, index) => [type, [1, index + 1]])),
    ) });
    const { events } = await adapter.poll({ cursor: null, limit: 100 });
    for (const event of events) {
      const immediate = BOTLING_KNOWS_DEFAULT_WAKE_TYPES.has(event.type);
      const preview = b.previewPolicy("botlingknows", event, recipe);
      expect(preview.suppressed).toBe(!immediate);
      expect(preview.matched_policy?.delivery.mode).toBe(immediate ? "immediate" : "suppress");
    }
    const personal = [...recipe, { id: "my-system-invites", version: 1, order: 200,
      match: { source: "botlingknows", type: "invite_system" }, delivery: { mode: "immediate" as const } }];
    const systemInvite = events.find((e) => e.type === "invite_system")!;
    expect(b.previewPolicy("botlingknows", systemInvite, personal).suppressed).toBe(false);
    b.close();
  });
});
