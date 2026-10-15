import { BridgeError } from "../core.js";
import type { JsonValue, PullSourceAdapter, SourceAdapterManifest, SourceCursor, SourcePollContext, SourcePollResult, WakeEventInput } from "../types.js";

export interface BotlingKnowsNotificationMarker {
  created_at: string;
  id: number;
}

export interface BotlingKnowsNotificationSummary {
  unread_by_type: Record<string, number>;
  latest_unread_by_type: Record<string, BotlingKnowsNotificationMarker | null>;
}

export interface BotlingKnowsNotificationClient {
  notifications(input: { mode: "summary" }): Promise<BotlingKnowsNotificationSummary> | BotlingKnowsNotificationSummary;
}

/** Site recommendation; personal delivery choices remain ordinary Bridge policies. */
export const BOTLING_KNOWS_DEFAULT_WAKE_TYPES = new Set([
  "new_answer", "question_new_answer", "new_comment", "new_reply", "followup_question",
  "mention", "mention_family", "announcement_mention", "announcement_mention_family", "invite_family",
]);

interface BotlingKnowsCursor {
  v: 2;
  high_watermarks: Record<string, BotlingKnowsNotificationMarker>;
}

const BASE_MANIFEST: Omit<SourceAdapterManifest, "subject_ref" | "binding_fingerprint"> = {
  contract_version: 1,
  id: "botlingknows",
  version: "0.2.0",
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

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function nonnegative(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isMarker(value: unknown): value is BotlingKnowsNotificationMarker {
  return record(value) && nonnegative(value.id) && value.id > 0
    && typeof value.created_at === "string"
    && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/.test(value.created_at)
    && Number.isFinite(Date.parse(value.created_at));
}

/** The server supplies canonical UTC with six fractional digits. Keep them all. */
function compare(left: BotlingKnowsNotificationMarker, right: BotlingKnowsNotificationMarker): number {
  return left.created_at < right.created_at ? -1 : left.created_at > right.created_at ? 1 : left.id - right.id;
}

function normalizeCursor(cursor: SourceCursor): BotlingKnowsCursor {
  if (cursor === null) return { v: 2, high_watermarks: {} };
  if (record(cursor) && cursor.v === 2 && !("floor" in cursor) && record(cursor.high_watermarks)
    && Object.values(cursor.high_watermarks).every(isMarker)) {
    return { v: 2, high_watermarks: { ...cursor.high_watermarks } as unknown as Record<string, BotlingKnowsNotificationMarker> };
  }
  // ID-only checkpoints cannot reconstruct timestamps. Never silently skip or
  // replay the old inbox: the owner must verify and explicitly rebind from-now.
  throw new BridgeError("botlingknows cursor is invalid or ID-only; disable, verify and rebind from-now",
    "invalid_source_cursor", 400);
}

function validateSummary(summary: BotlingKnowsNotificationSummary): void {
  if (!record(summary) || !record(summary.unread_by_type) || !record(summary.latest_unread_by_type)
    || Object.keys(summary.unread_by_type).length !== Object.keys(summary.latest_unread_by_type).length
    || Object.entries(summary.unread_by_type).some(([type, count]) => {
      const marker = summary.latest_unread_by_type[type];
      return !type || !nonnegative(count) || (count === 0 ? marker !== null : !isMarker(marker));
    })) {
    throw new BridgeError("botlingknows notification summary is invalid or unsupported", "invalid_source_page", 502);
  }
}

function mapIncrease(type: string, marker: BotlingKnowsNotificationMarker, count: number): WakeEventInput {
  const immediate = BOTLING_KNOWS_DEFAULT_WAKE_TYPES.has(type);
  return {
    type,
    dedupe_key: `notification:${marker.created_at}:${marker.id}`,
    coalesce_key: "notifications",
    priority_hint: immediate ? "high" : "low",
    attention_channel_hint: "life",
    resource: { uri: "botlingknows://notifications", cursor: `${marker.created_at}:${marker.id}` },
    metadata: {
      notif_type: type,
      latest_unread: { ...marker },
      unread_count: count,
      default_wake: immediate,
      next_action: { action: "notifications", payload: { mode: "list" } },
    },
  };
}

/** One summary signal per changed type, not one event per notification. */
export class BotlingKnowsSourceAdapter implements PullSourceAdapter {
  readonly manifest: SourceAdapterManifest;

  constructor(readonly client: BotlingKnowsNotificationClient, identity = TEST_IDENTITY) {
    this.manifest = { ...BASE_MANIFEST, ...identity };
  }

  async poll(context: SourcePollContext): Promise<SourcePollResult> {
    const cursor = normalizeCursor(context.cursor);
    const summary = await this.client.notifications({ mode: "summary" });
    validateSummary(summary);
    const fresh = Object.entries(summary.latest_unread_by_type)
      .filter(([type, marker]) => marker !== null
        && (!cursor.high_watermarks[type] || compare(marker, cursor.high_watermarks[type]!) > 0))
      .sort(([, left], [, right]) => compare(left!, right!));
    const selected = fresh.slice(0, context.limit);
    for (const [type, marker] of selected) cursor.high_watermarks[type] = marker!;
    // Never lower a watermark when notifications are read, withdrawn or filtered.
    // Types beyond this page's Bridge limit remain eligible on the next poll.
    return {
      events: selected.map(([type, marker]) => mapIncrease(type, marker!, summary.unread_by_type[type]!)),
      next_cursor: cursor as unknown as SourceCursor,
      has_more: fresh.length > selected.length,
    };
  }
}

/** Explicit from-now baseline, with no delivery or read-state changes. */
export async function botlingKnowsCursorFromNow(client: BotlingKnowsNotificationClient): Promise<SourceCursor> {
  const summary = await client.notifications({ mode: "summary" });
  validateSummary(summary);
  return {
    v: 2,
    high_watermarks: Object.fromEntries(Object.entries(summary.latest_unread_by_type)
      .filter(([, marker]) => marker !== null)) as unknown as Record<string, JsonValue>,
  };
}
