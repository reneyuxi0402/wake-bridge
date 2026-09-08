import { describe, expect, it, vi } from "vitest";
import {
  GroupChatSourceAdapter,
  mapGroupChatEvent,
  type GroupChatEventFeedClient,
} from "../src/adapters/group-chat.js";
import { validateSourceManifest } from "../src/source-sdk.js";

describe("Group Chat reference source", () => {
  it("maps the public feed without exposing content or making policy decisions", () => {
    expect(mapGroupChatEvent({
      schema_version: 1,
      event_id: "gev_1",
      event_cursor: 955,
      type: "room.mention.created",
      occurred_at: "2026-08-30T03:21:00Z",
      room_id: "room_home",
      message_id: "msg_1",
      message_cursor: 184,
      actor_id: "yuxi",
      direct: true,
      metadata: { room_id: "untrusted-override", group_schema_version: 99 },
    })).toEqual({
      schema_version: 1,
      type: "mention",
      occurred_at: "2026-08-30T03:21:00Z",
      dedupe_key: "event:gev_1",
      coalesce_key: "group:room_home",
      priority_hint: "high",
      attention_channel_hint: "group:room_home",
      actor_ref: "group-member:yuxi",
      resource: { uri: "group-chat://room_home/messages/msg_1", cursor: 184 },
      metadata: {
        room_id: "room_home",
        group_event_cursor: 955,
        group_schema_version: 1,
        direct: true,
      },
      payload_preview: null,
    });
  });

  it("uses an opaque committed cursor and rejects malformed or duplicate pages", async () => {
    const readEvents = vi.fn<GroupChatEventFeedClient["readEvents"]>(() => ({
      events: [{
        schema_version: 1,
        event_id: "gev_2",
        event_cursor: "956",
        type: "room.reply.created",
        occurred_at: "2026-08-30T03:22:00Z",
        room_id: "room_home",
        message_id: "msg_2",
      }],
      next_cursor: "956",
      has_more: false,
    }));
    const adapter = new GroupChatSourceAdapter({ readEvents }, {
      subject_ref: "group-chat:principal:fixture",
      binding_fingerprint: `sha256:${"2".repeat(64)}`,
    });
    expect(validateSourceManifest(adapter.manifest)).toMatchObject({ contract_version: 1, id: "group_chat" });
    await expect(adapter.poll({ cursor: { v: 1, event_cursor: "955" }, limit: 20 })).resolves.toMatchObject({
      events: [{ type: "reply", dedupe_key: "event:gev_2" }],
      next_cursor: { v: 1, event_cursor: "956" },
      has_more: false,
    });
    expect(readEvents).toHaveBeenCalledWith(expect.objectContaining({ after_event_cursor: "955", limit: 20, wait_ms: 0 }));

    const duplicate = new GroupChatSourceAdapter({ readEvents: () => ({
      events: [
        { schema_version: 1, event_id: "same", event_cursor: 1, type: "room.message.created", occurred_at: "2026-08-30T00:00:00Z", room_id: "room" },
        { schema_version: 1, event_id: "same", event_cursor: 2, type: "room.message.created", occurred_at: "2026-08-30T00:00:01Z", room_id: "room" },
      ],
      next_cursor: 2,
    }) }, {
      subject_ref: "group-chat:principal:fixture",
      binding_fingerprint: `sha256:${"2".repeat(64)}`,
    });
    await expect(duplicate.poll({ cursor: null, limit: 20 })).rejects.toMatchObject({ code: "invalid_source_page" });
  });
});
