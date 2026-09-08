import {
  SOURCE_ADAPTER_CONTRACT_VERSION,
  type PullSourceAdapter,
  type SourceAdapterManifest,
  type SourceCursor,
  type SourcePollContext,
  type SourcePollResult,
} from "../source-sdk.js";
import { WakeBridgeSdkError, type JsonValue, type WakeEventInput } from "../event-sdk.js";

export const GROUP_CHAT_EVENT_FEED_SCHEMA_VERSION = 1 as const;
export const GROUP_CHAT_SOURCE_ID = "group_chat";
export const GROUP_CHAT_MAX_PAGE_SIZE = 250;

export type GroupChatEventType =
  | "room.mention.created"
  | "room.reply.created"
  | "room.message.created"
  | "room.message.tombstoned"
  | "room.membership.changed";

export interface GroupChatFeedEvent {
  schema_version: typeof GROUP_CHAT_EVENT_FEED_SCHEMA_VERSION;
  event_id: string;
  event_cursor: string | number;
  type: GroupChatEventType;
  occurred_at: string;
  room_id: string;
  message_id?: string | null;
  message_cursor?: string | number | null;
  actor_id?: string | null;
  direct?: boolean;
  metadata?: Record<string, JsonValue>;
}

export interface GroupChatEventPage {
  events: GroupChatFeedEvent[];
  next_cursor: string | number | null;
  has_more?: boolean;
}

export interface GroupChatEventFeedClient {
  readEvents(input: {
    after_event_cursor: string | number | null;
    limit: number;
    wait_ms: 0;
    types: GroupChatEventType[];
  }): Promise<GroupChatEventPage> | GroupChatEventPage;
}

interface GroupChatCursor {
  v: 1;
  event_cursor: string | number | null;
}

const EVENT_TYPES: GroupChatEventType[] = [
  "room.mention.created",
  "room.reply.created",
  "room.message.created",
  "room.message.tombstoned",
  "room.membership.changed",
];
const ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$/u;

function cursor(value: SourceCursor): GroupChatCursor {
  if (value === null) return { v: 1, event_cursor: null };
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new WakeBridgeSdkError("group chat cursor is invalid", "invalid_source_cursor", 400);
  }
  const candidate = value as Record<string, JsonValue>;
  const eventCursor = candidate.event_cursor;
  if (candidate.v !== 1 || (eventCursor !== null && typeof eventCursor !== "string" && typeof eventCursor !== "number")) {
    throw new WakeBridgeSdkError("group chat cursor is invalid", "invalid_source_cursor", 400);
  }
  return { v: 1, event_cursor: eventCursor };
}

function checkedId(value: unknown, field: string): string {
  if (typeof value !== "string" || !ID.test(value)) {
    throw new WakeBridgeSdkError(`group chat ${field} is invalid`, "invalid_source_page", 502);
  }
  return value;
}

export function mapGroupChatEvent(input: GroupChatFeedEvent): WakeEventInput {
  if (input?.schema_version !== GROUP_CHAT_EVENT_FEED_SCHEMA_VERSION || !EVENT_TYPES.includes(input.type)) {
    throw new WakeBridgeSdkError("group chat event schema or type is unsupported", "unsupported_source_schema", 502);
  }
  const eventId = checkedId(input.event_id, "event_id");
  const roomId = checkedId(input.room_id, "room_id");
  const messageId = input.message_id == null ? null : checkedId(input.message_id, "message_id");
  if (input.event_cursor == null || !["string", "number"].includes(typeof input.event_cursor)) {
    throw new WakeBridgeSdkError("group chat event_cursor is invalid", "invalid_source_page", 502);
  }
  const mapping: Record<GroupChatEventType, { type: string; priority: "high" | "normal" | "low" }> = {
    "room.mention.created": { type: "mention", priority: "high" },
    "room.reply.created": { type: "reply", priority: "normal" },
    "room.message.created": { type: "activity", priority: "low" },
    "room.message.tombstoned": { type: "tombstone", priority: "normal" },
    "room.membership.changed": { type: "membership", priority: "normal" },
  };
  const mapped = mapping[input.type];
  const roomUri = `group-chat://${encodeURIComponent(roomId)}`;
  return {
    schema_version: 1,
    type: mapped.type,
    occurred_at: input.occurred_at,
    dedupe_key: `event:${eventId}`,
    coalesce_key: `group:${roomId}`,
    priority_hint: mapped.priority,
    attention_channel_hint: input.type === "room.membership.changed" ? "default" : `group:${roomId}`,
    actor_ref: input.actor_id == null ? null : `group-member:${checkedId(input.actor_id, "actor_id")}`,
    resource: {
      uri: messageId ? `${roomUri}/messages/${encodeURIComponent(messageId)}` : roomUri,
      ...(input.message_cursor == null ? {} : { cursor: input.message_cursor }),
    },
    metadata: {
      ...(input.metadata ?? {}),
      room_id: roomId,
      group_event_cursor: input.event_cursor,
      group_schema_version: input.schema_version,
      direct: input.direct === true,
    },
    payload_preview: null,
  };
}

/** Reference pull adapter. It depends only on the public feed and source contracts. */
export class GroupChatSourceAdapter implements PullSourceAdapter {
  readonly manifest: SourceAdapterManifest;

  constructor(
    private readonly client: GroupChatEventFeedClient,
    identity: { subject_ref: string; binding_fingerprint: string },
  ) {
    this.manifest = {
      contract_version: SOURCE_ADAPTER_CONTRACT_VERSION,
      id: GROUP_CHAT_SOURCE_ID,
      version: "0.1.0",
      subject_ref: identity.subject_ref,
      binding_fingerprint: identity.binding_fingerprint,
      read_side_effects: "none",
      credential_custody: "connector",
      upstream_credential_breadth: "unknown",
      upstream_credential_scopes: [],
      connector_capabilities: ["events.read", "resources.read"],
    };
  }

  async poll(context: SourcePollContext): Promise<SourcePollResult> {
    const before = cursor(context.cursor);
    const page = await this.client.readEvents({
      after_event_cursor: before.event_cursor,
      limit: context.limit,
      wait_ms: 0,
      types: [...EVENT_TYPES],
    });
    if (!page || !Array.isArray(page.events) || page.events.length > context.limit
      || (page.next_cursor !== null && !["string", "number"].includes(typeof page.next_cursor))) {
      throw new WakeBridgeSdkError("group chat returned an invalid event page", "invalid_source_page", 502);
    }
    const ids = new Set<string>();
    for (const event of page.events) {
      if (ids.has(event.event_id)) {
        throw new WakeBridgeSdkError("group chat event page contains duplicate ids", "invalid_source_page", 502);
      }
      ids.add(event.event_id);
    }
    return {
      events: page.events.map(mapGroupChatEvent),
      next_cursor: { v: 1, event_cursor: page.next_cursor },
      has_more: page.has_more === true,
    };
  }
}
