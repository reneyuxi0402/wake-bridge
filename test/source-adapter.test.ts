import { mkdtempSync } from "node:fs";
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
  type BotlingKnowsNotificationPage,
} from "../src/adapters/botlingknows.js";
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
  it("uses full + mark_read=false, separates page cursor from high-watermark, and maps no body", async () => {
    const calls: Array<Record<string, unknown>> = [];
    const pages: BotlingKnowsNotificationPage[] = [
      {
        notifications: [
          { id: 5, notif_type: "new_reply", category: "direct", created_at: "2026-08-28T10:05:00Z", question_id: 12, content_id: 50, actor_id: 7, next_action: { tool: "get_content", content_id: 50 } },
          { id: 4, notif_type: "liked", category: "activity", created_at: "2026-08-28T10:04:00Z", content_id: 40 },
        ],
        next_cursor: "page-2",
      },
      {
        notifications: [
          { id: 3, notif_type: "invited", category: "invitation", created_at: "2026-08-28T10:03:00Z", question_id: 12 },
          { id: 2, notif_type: "new_answer", category: "direct", created_at: "2026-08-28T10:02:00Z", question_id: 11 },
          { id: 1, notif_type: "system_notice", category: "system", created_at: "2026-08-28T10:01:00Z" },
        ],
      },
      {
        notifications: [
          { id: 7, notif_type: "mention", category: "direct", created_at: "2026-08-28T10:07:00Z", content_id: 70 },
          { id: 6, notif_type: "new_reply", category: "direct", created_at: "2026-08-28T10:06:00Z", content_id: 60 },
          { id: 5, notif_type: "new_reply", category: "direct", created_at: "2026-08-28T10:05:00Z", content_id: 50 },
        ],
        next_cursor: "must-not-follow-after-watermark",
      },
    ];
    const client: BotlingKnowsNotificationClient = {
      notifications(input) {
        calls.push(input);
        const page = pages.shift();
        if (!page) throw new Error("unexpected poll");
        return page;
      },
    };
    const b = bridge();
    const runner = new SourceRunner(b);
    const adapter = new BotlingKnowsSourceAdapter(client);
    expect(adapter.manifest).toMatchObject({
      credential_custody: "connector",
      upstream_credential_breadth: "unknown",
      connector_capabilities: ["notifications.read"],
    });
    await expect(runner.runOnce(adapter, { limit: 10 })).resolves.toMatchObject({ events_inserted: 2, has_more: true });
    expect(runner.checkpoints.get("botlingknows")?.cursor).toEqual({
      v: 1, high_watermark: 0, page_cursor: "page-2", pending_max_id: 5,
    });
    await expect(runner.runOnce(adapter, { limit: 10 })).resolves.toMatchObject({ events_inserted: 3, has_more: false });
    expect(runner.checkpoints.get("botlingknows")?.cursor).toEqual({
      v: 1, high_watermark: 5, page_cursor: null, pending_max_id: 5,
    });
    await expect(runner.runOnce(adapter, { limit: 10 })).resolves.toMatchObject({ events_inserted: 2, has_more: false });
    expect(runner.checkpoints.get("botlingknows")?.cursor).toEqual({
      v: 1, high_watermark: 7, page_cursor: null, pending_max_id: 7,
    });
    expect(calls).toEqual([
      { view: "full", mark_read: false, limit: 10 },
      { view: "full", mark_read: false, limit: 10, cursor: "page-2" },
      { view: "full", mark_read: false, limit: 10 },
    ]);
    expect(b.listEvents({ source: "botlingknows", limit: 20 }).map((event) => Number(event.resource.cursor))).toEqual([4, 5, 1, 2, 3, 6, 7]);
    const direct = b.listEvents({ source: "botlingknows", limit: 20 }).find((event) => event.dedupe_key === "notification:5");
    expect(direct).toMatchObject({
      type: "new_reply",
      priority_hint: "high",
      attention_channel_hint: "life",
      actor_ref: "botlingknows:principal:7",
      resource: { uri: "botlingknows://content/50", cursor: 5 },
      metadata: { notification_id: 5, category: "direct", notif_type: "new_reply", question_id: 12, content_id: 50 },
    });
    expect(JSON.stringify(direct)).not.toContain("summary");
    expect(JSON.stringify(direct)).not.toContain("body");
    b.close();
  });

  it("offers an explicit from-now bootstrap without marking notifications read", async () => {
    const calls: Array<Record<string, unknown>> = [];
    const client: BotlingKnowsNotificationClient = {
      notifications(input) {
        calls.push(input);
        return {
          notifications: [
            { id: 22, notif_type: "new_reply", category: "direct", created_at: "2026-08-28T10:22:00Z" },
            { id: 21, notif_type: "liked", category: "activity", created_at: "2026-08-28T10:21:00Z" },
          ],
        };
      },
    };
    await expect(botlingKnowsCursorFromNow(client, 20)).resolves.toEqual({
      v: 1, high_watermark: 22, page_cursor: null, pending_max_id: 22,
    });
    expect(calls).toEqual([{ view: "full", mark_read: false, limit: 20 }]);
  });
});
