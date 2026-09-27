import { BridgeError } from "../core.js";
import type {
  JsonValue,
  PullSourceAdapter,
  SourceAdapterManifest,
  SourceCursor,
  SourcePollContext,
  SourcePollResult,
  SourceUpstreamCredentialBreadth,
  WakeEventInput,
} from "../types.js";

export const GMAIL_SOURCE_ID = "gmail";
export const GMAIL_MAX_PAGE_SIZE = 100;

const PROVIDER_ID = /^[A-Za-z0-9_-]{1,128}$/u;
const HISTORY_ID = /^[0-9]{1,32}$/u;

export interface GmailProfile {
  emailAddress: string;
  historyId: string;
}

export interface GmailHistoryMessage {
  id: string;
  threadId?: string;
}

export interface GmailHistoryRecord {
  id: string;
  messagesAdded?: Array<{ message?: GmailHistoryMessage }>;
}

export interface GmailHistoryPage {
  history?: GmailHistoryRecord[];
  nextPageToken?: string;
  historyId: string;
}

export interface GmailMessageMetadata {
  id: string;
  threadId?: string;
  labelIds?: string[];
  internalDate?: string;
}

export interface GmailNotificationClient {
  profile(): Promise<GmailProfile> | GmailProfile;
  history(input: {
    start_history_id: string;
    page_token: string | null;
    max_results: number;
  }): Promise<GmailHistoryPage> | GmailHistoryPage;
  messageMetadata(messageId: string): Promise<GmailMessageMetadata> | GmailMessageMetadata;
}

interface GmailCursor {
  v: 1;
  /** Stable Gmail history waterline for the whole bounded scan. */
  history_id: string;
  /** Gmail's opaque continuation for the provider page being read. */
  page_token: string | null;
  /** Offset within a provider page when one history page expands past the event limit. */
  message_offset: number;
}

export interface GmailSourceAdapterOptions {
  subject_ref: string;
  binding_fingerprint: string;
  upstream_credential_scopes: string[];
  upstream_credential_breadth: SourceUpstreamCredentialBreadth;
  attention_channel?: string;
  now?: () => Date;
}

function gmailCursor(cursor: SourceCursor): GmailCursor {
  if (!cursor || typeof cursor !== "object" || Array.isArray(cursor)) {
    throw new BridgeError("gmail requires an explicit from-now bootstrap", "invalid_source_cursor", 400);
  }
  const value = cursor as Record<string, JsonValue>;
  const offset = Number(value.message_offset);
  if (
    value.v !== 1
    || typeof value.history_id !== "string"
    || !HISTORY_ID.test(value.history_id)
    || (value.page_token !== null && (typeof value.page_token !== "string" || value.page_token.length > 2048))
    || !Number.isSafeInteger(offset)
    || offset < 0
  ) {
    throw new BridgeError("gmail cursor is invalid", "invalid_source_cursor", 400);
  }
  return {
    v: 1,
    history_id: value.history_id,
    page_token: value.page_token as string | null,
    message_offset: offset,
  };
}

function validateHistoryPage(page: GmailHistoryPage): GmailHistoryRecord[] {
  if (
    !page
    || !HISTORY_ID.test(page.historyId || "")
    || (page.nextPageToken !== undefined && (typeof page.nextPageToken !== "string" || page.nextPageToken.length > 2048))
    || (page.history !== undefined && !Array.isArray(page.history))
  ) {
    throw new BridgeError("gmail returned an invalid history page", "invalid_source_page", 502);
  }
  const records = page.history ?? [];
  for (const record of records) {
    if (!record || !HISTORY_ID.test(record.id || "") || (record.messagesAdded !== undefined && !Array.isArray(record.messagesAdded))) {
      throw new BridgeError("gmail returned an invalid history record", "invalid_source_page", 502);
    }
  }
  return records;
}

function historyMessages(records: GmailHistoryRecord[]): Array<{ message_id: string; thread_id: string | null; history_id: string }> {
  const seen = new Set<string>();
  const messages: Array<{ message_id: string; thread_id: string | null; history_id: string }> = [];
  for (const record of records) {
    for (const added of record.messagesAdded ?? []) {
      const message = added?.message;
      if (!message || !PROVIDER_ID.test(message.id || "") || (message.threadId !== undefined && !PROVIDER_ID.test(message.threadId))) {
        throw new BridgeError("gmail returned an invalid message reference", "invalid_source_page", 502);
      }
      if (seen.has(message.id)) continue;
      seen.add(message.id);
      messages.push({ message_id: message.id, thread_id: message.threadId ?? null, history_id: record.id });
    }
  }
  return messages;
}

function occurredAt(internalDate: string | undefined, now: () => Date): string {
  const milliseconds = Number(internalDate);
  if (Number.isFinite(milliseconds) && milliseconds > 0) return new Date(milliseconds).toISOString();
  return now().toISOString();
}

function gmailEvent(
  reference: { message_id: string; thread_id: string | null; history_id: string },
  message: GmailMessageMetadata,
  attentionChannel: string,
  now: () => Date,
): WakeEventInput {
  if (!message || message.id !== reference.message_id
    || (message.threadId !== undefined && !PROVIDER_ID.test(message.threadId))) {
    throw new BridgeError("gmail returned mismatched message metadata", "invalid_source_page", 502);
  }
  const threadId = message.threadId ?? reference.thread_id ?? message.id;
  return {
    type: "message.received",
    occurred_at: occurredAt(message.internalDate, now),
    dedupe_key: `message:${message.id}`,
    coalesce_key: `thread:${threadId}`,
    priority_hint: "normal",
    attention_channel_hint: attentionChannel,
    actor_ref: null,
    resource: { uri: `gmail://message/${message.id}`, cursor: reference.history_id },
    // Deliberately exclude Subject, From, snippet, body, and headers. The
    // awakened agent reads authoritative content through its own Gmail tool.
    metadata: { message_id: message.id, thread_id: threadId },
  };
}

export class GmailSourceAdapter implements PullSourceAdapter {
  readonly manifest: SourceAdapterManifest;
  private readonly attentionChannel: string;
  private readonly now: () => Date;

  constructor(readonly client: GmailNotificationClient, options: GmailSourceAdapterOptions) {
    this.attentionChannel = options.attention_channel ?? "life";
    this.now = options.now ?? (() => new Date());
    this.manifest = {
      contract_version: 1,
      id: GMAIL_SOURCE_ID,
      version: "0.1.0",
      subject_ref: options.subject_ref,
      binding_fingerprint: options.binding_fingerprint,
      read_side_effects: "none",
      credential_custody: "connector",
      upstream_credential_breadth: options.upstream_credential_breadth,
      upstream_credential_scopes: [...options.upstream_credential_scopes],
      connector_capabilities: ["messages.read_metadata"],
    };
  }

  async poll(context: SourcePollContext): Promise<SourcePollResult> {
    if (!Number.isSafeInteger(context.limit) || context.limit < 1 || context.limit > GMAIL_MAX_PAGE_SIZE) {
      throw new BridgeError("gmail poll limit is invalid", "invalid_source_poll", 400);
    }
    const cursor = gmailCursor(context.cursor);
    const page = await this.client.history({
      start_history_id: cursor.history_id,
      page_token: cursor.page_token,
      max_results: GMAIL_MAX_PAGE_SIZE,
    });
    const messages = historyMessages(validateHistoryPage(page));
    if (cursor.message_offset > messages.length) {
      throw new BridgeError("gmail cursor offset is outside the provider page", "invalid_source_cursor", 400);
    }

    const events: WakeEventInput[] = [];
    let offset = cursor.message_offset;
    while (offset < messages.length && events.length < context.limit) {
      const reference = messages[offset];
      offset += 1;
      let message: GmailMessageMetadata;
      try {
        message = await this.client.messageMetadata(reference.message_id);
      } catch (error) {
        if (error instanceof BridgeError && error.code === "gmail_message_not_found") continue;
        throw error;
      }
      const labels = Array.isArray(message.labelIds) ? message.labelIds : [];
      if (!labels.includes("INBOX") || labels.includes("DRAFT")) continue;
      events.push(gmailEvent(reference, message, this.attentionChannel, this.now));
    }

    if (offset < messages.length) {
      return {
        events,
        next_cursor: { ...cursor, message_offset: offset },
        has_more: true,
      };
    }
    if (page.nextPageToken) {
      return {
        events,
        next_cursor: { ...cursor, page_token: page.nextPageToken, message_offset: 0 },
        has_more: true,
      };
    }
    return {
      events,
      next_cursor: { v: 1, history_id: page.historyId, page_token: null, message_offset: 0 },
      has_more: false,
    };
  }
}

export async function gmailCursorFromNow(client: GmailNotificationClient): Promise<SourceCursor> {
  const profile = await client.profile();
  if (!profile || !HISTORY_ID.test(profile.historyId || "")) {
    throw new BridgeError("gmail returned an invalid profile", "invalid_source_page", 502);
  }
  return { v: 1, history_id: profile.historyId, page_token: null, message_offset: 0 };
}
