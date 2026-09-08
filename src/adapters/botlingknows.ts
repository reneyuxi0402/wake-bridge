import { BridgeError } from "../core.js";
import type {
  JsonValue,
  PullSourceAdapter,
  SourceAdapterManifest,
  SourceCursor,
  SourcePollContext,
  SourcePollResult,
  WakeEventInput,
} from "../types.js";

export interface BotlingKnowsNotification {
  id: number;
  notif_type: string;
  category: "direct" | "activity" | "invitation" | "system" | string;
  created_at: string;
  question_id?: number | null;
  content_id?: number | null;
  actor_id?: number | null;
  next_action?: JsonValue;
}

export interface BotlingKnowsNotificationPage {
  notifications: BotlingKnowsNotification[];
  next_cursor?: string | null;
}

export interface BotlingKnowsNotificationClient {
  notifications(input: {
    view: "full";
    mark_read: false;
    limit: number;
    cursor?: string;
  }): Promise<BotlingKnowsNotificationPage> | BotlingKnowsNotificationPage;
}

/** Botling Knows currently accepts at most 50 notifications per request. */
export const BOTLING_KNOWS_MAX_PAGE_SIZE = 50;

interface BotlingKnowsCursor {
  v: 1;
  /** Highest stable notification id from the last completed scan. */
  high_watermark: number;
  /** Pagination cursor while a bounded scan is still in progress. */
  page_cursor: string | null;
  /** Highest id observed across every page in the current scan. */
  pending_max_id: number;
}

const BASE_MANIFEST: Omit<SourceAdapterManifest, "subject_ref" | "binding_fingerprint"> = {
  contract_version: 1,
  id: "botlingknows",
  version: "0.1.0",
  read_side_effects: "none",
  credential_custody: "connector",
  upstream_credential_breadth: "unknown",
  upstream_credential_scopes: [],
  connector_capabilities: ["notifications.read"],
};

const TEST_IDENTITY = {
  subject_ref: "botlingknows:test",
  binding_fingerprint: `sha256:${"0".repeat(64)}`,
};

function normalizeCursor(cursor: SourceCursor): BotlingKnowsCursor {
  if (cursor === null) return { v: 1, high_watermark: 0, page_cursor: null, pending_max_id: 0 };
  if (!cursor || typeof cursor !== "object" || Array.isArray(cursor)) {
    throw new BridgeError("botlingknows cursor is invalid", "invalid_source_cursor", 400);
  }
  const value = cursor as Record<string, JsonValue>;
  const high = Number(value.high_watermark);
  const pending = Number(value.pending_max_id);
  const page = value.page_cursor;
  if (value.v !== 1 || !Number.isSafeInteger(high) || high < 0 || !Number.isSafeInteger(pending) || pending < high
    || (page !== null && typeof page !== "string")) {
    throw new BridgeError("botlingknows cursor is invalid", "invalid_source_cursor", 400);
  }
  return { v: 1, high_watermark: high, page_cursor: page, pending_max_id: pending };
}

function absoluteTimestamp(value: string): string {
  const date = new Date(value);
  if (!value || Number.isNaN(date.getTime())) {
    throw new BridgeError("botlingknows notification timestamp is invalid", "invalid_source_page", 502);
  }
  return date.toISOString();
}

function resourceFor(notification: BotlingKnowsNotification): { uri: string; cursor: number } {
  if (Number.isSafeInteger(notification.content_id) && Number(notification.content_id) > 0) {
    return { uri: `botlingknows://content/${notification.content_id}`, cursor: notification.id };
  }
  if (Number.isSafeInteger(notification.question_id) && Number(notification.question_id) > 0) {
    return { uri: `botlingknows://question/${notification.question_id}`, cursor: notification.id };
  }
  return { uri: `botlingknows://notification/${notification.id}`, cursor: notification.id };
}

function metadataFor(notification: BotlingKnowsNotification): Record<string, JsonValue> {
  return {
    notification_id: notification.id,
    category: notification.category,
    notif_type: notification.notif_type,
    ...(notification.question_id == null ? {} : { question_id: notification.question_id }),
    ...(notification.content_id == null ? {} : { content_id: notification.content_id }),
    ...(notification.next_action === undefined ? {} : { next_action: notification.next_action }),
  };
}

export function mapBotlingKnowsNotification(notification: BotlingKnowsNotification): WakeEventInput {
  if (!Number.isSafeInteger(notification.id) || notification.id <= 0 || !notification.notif_type
    || typeof notification.notif_type !== "string" || !notification.category || typeof notification.category !== "string") {
    throw new BridgeError("botlingknows notification shape is invalid", "invalid_source_page", 502);
  }
  const coalesce = Number.isSafeInteger(notification.question_id) && Number(notification.question_id) > 0
    ? `question:${notification.question_id}`
    : Number.isSafeInteger(notification.content_id) && Number(notification.content_id) > 0
      ? `content:${notification.content_id}`
      : `category:${notification.category}`;
  return {
    type: notification.notif_type,
    occurred_at: absoluteTimestamp(notification.created_at),
    dedupe_key: `notification:${notification.id}`,
    coalesce_key: coalesce,
    priority_hint: notification.category === "direct" ? "high" : notification.category === "activity" ? "low" : "normal",
    attention_channel_hint: "life",
    actor_ref: Number.isSafeInteger(notification.actor_id) && Number(notification.actor_id) > 0
      ? `botlingknows:principal:${notification.actor_id}`
      : null,
    resource: resourceFor(notification),
    metadata: metadataFor(notification),
  };
}

/**
 * Read-only notification adapter. The stored cursor is a stable notification
 * id high-watermark plus an optional pagination continuation; a source API's
 * opaque page cursor is never mistaken for a durable incremental cursor.
 */
export class BotlingKnowsSourceAdapter implements PullSourceAdapter {
  readonly manifest: SourceAdapterManifest;

  constructor(readonly client: BotlingKnowsNotificationClient, identity = TEST_IDENTITY) {
    this.manifest = { ...BASE_MANIFEST, ...identity };
  }

  async poll(context: SourcePollContext): Promise<SourcePollResult> {
    const cursor = normalizeCursor(context.cursor);
    const page = await this.client.notifications({
      view: "full",
      mark_read: false,
      limit: context.limit,
      ...(cursor.page_cursor ? { cursor: cursor.page_cursor } : {}),
    });
    if (!page || !Array.isArray(page.notifications) || page.notifications.length > context.limit) {
      throw new BridgeError("botlingknows returned an invalid notification page", "invalid_source_page", 502);
    }
    const ids = new Set<number>();
    for (const notification of page.notifications) {
      if (!Number.isSafeInteger(notification?.id) || notification.id <= 0 || ids.has(notification.id)) {
        throw new BridgeError("botlingknows notification ids are invalid", "invalid_source_page", 502);
      }
      ids.add(notification.id);
    }
    const reachedWatermark = page.notifications.some((notification) => notification.id <= cursor.high_watermark);
    const fresh = page.notifications
      .filter((notification) => notification.id > cursor.high_watermark)
      .sort((left, right) => left.id - right.id);
    const maxObserved = Math.max(cursor.pending_max_id, cursor.high_watermark, ...fresh.map((item) => item.id));
    const hasMore = Boolean(page.next_cursor) && !reachedWatermark;
    const next: BotlingKnowsCursor = hasMore
      ? { ...cursor, page_cursor: String(page.next_cursor), pending_max_id: maxObserved }
      : { v: 1, high_watermark: maxObserved, page_cursor: null, pending_max_id: maxObserved };
    return {
      events: fresh.map(mapBotlingKnowsNotification),
      next_cursor: next as unknown as SourceCursor,
      has_more: hasMore,
    };
  }
}

/** Explicit operator bootstrap for a future production install. */
export async function botlingKnowsCursorFromNow(
  client: BotlingKnowsNotificationClient,
  limit = BOTLING_KNOWS_MAX_PAGE_SIZE,
): Promise<SourceCursor> {
  const page = await client.notifications({ view: "full", mark_read: false, limit });
  if (!page || !Array.isArray(page.notifications) || page.notifications.length > limit) {
    throw new BridgeError("botlingknows returned an invalid bootstrap page", "invalid_source_page", 502);
  }
  const max = page.notifications.reduce((value, item) => Math.max(value, Number(item.id) || 0), 0);
  if (!Number.isSafeInteger(max) || max < 0) throw new BridgeError("botlingknows bootstrap ids are invalid", "invalid_source_page", 502);
  return { v: 1, high_watermark: max, page_cursor: null, pending_max_id: max };
}
