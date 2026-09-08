import { WakeBridgeSdkError } from "./sdk-error.js";
import type { JsonValue, ResourceRef, WakeEventInput } from "./types.js";

export const EVENT_CONTRACT_VERSION = 1 as const;
export const SUPPORTED_EVENT_CONTRACT_VERSIONS = [EVENT_CONTRACT_VERSION] as const;

const ABSOLUTE_RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/u;
const FORBIDDEN_INPUT_KEYS = new Set([
  "owner_id", "instance_id", "policy", "policy_id", "endpoint", "endpoint_id",
  "wake_prompt", "wakePrompt", "system_instruction", "systemInstruction", "instructions", "prompt",
]);

function validateNoPrompt(value: unknown, path = "input"): void {
  if (!value || typeof value !== "object") return;
  if (Array.isArray(value)) {
    value.forEach((item, index) => validateNoPrompt(item, `${path}[${index}]`));
    return;
  }
  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    if (FORBIDDEN_INPUT_KEYS.has(key)) {
      throw new WakeBridgeSdkError(`${path}.${key} is not accepted by event ingress`, "forbidden_field", 400);
    }
    validateNoPrompt(child, `${path}.${key}`);
  }
}

function validateResource(resource: ResourceRef): void {
  if (!resource || typeof resource !== "object" || typeof resource.uri !== "string") {
    throw new WakeBridgeSdkError("resource.uri is required", "invalid_resource", 400);
  }
  if (resource.uri.length === 0 || resource.uri.length > 2048 || !/^[A-Za-z][A-Za-z0-9+.-]*:/u.test(resource.uri)) {
    throw new WakeBridgeSdkError("resource.uri must be a bounded absolute URI", "invalid_resource", 400);
  }
  if (JSON.stringify(resource).length > 4096) {
    throw new WakeBridgeSdkError("resource is too large", "oversized_event", 413);
  }
}

function absoluteTimestamp(value: string, field: string): string {
  if (typeof value !== "string" || !ABSOLUTE_RFC3339.test(value)) {
    throw new WakeBridgeSdkError(`${field} must be an absolute RFC3339 timestamp with Z or numeric offset`, "invalid_timestamp", 400);
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new WakeBridgeSdkError(`${field} is invalid`, "invalid_timestamp", 400);
  }
  return date.toISOString();
}

/** Validate and normalize the stable v1 producer payload. */
export function validateWakeEventInput(raw: WakeEventInput, now = new Date()): WakeEventInput & { schema_version: 1; occurred_at: string } {
  if (!raw || typeof raw.type !== "string" || !raw.type || raw.type.length > 200) {
    throw new WakeBridgeSdkError("event type is required", "invalid_event", 400);
  }
  if (raw.schema_version != null && raw.schema_version !== EVENT_CONTRACT_VERSION) {
    throw new WakeBridgeSdkError(`unsupported event schema_version: ${String(raw.schema_version)}`, "unsupported_schema", 400);
  }
  if (!raw.dedupe_key || typeof raw.dedupe_key !== "string" || raw.dedupe_key.length > 500) {
    throw new WakeBridgeSdkError("dedupe_key is required and bounded", "invalid_event", 400);
  }
  validateNoPrompt(raw);
  validateResource(raw.resource);
  const metadata = raw.metadata ?? {};
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata) || JSON.stringify(metadata).length > 8192) {
    throw new WakeBridgeSdkError("metadata must be a bounded object", "oversized_event", 413);
  }
  if (raw.payload_preview != null && (typeof raw.payload_preview !== "string" || raw.payload_preview.length > 500)) {
    throw new WakeBridgeSdkError("payload_preview is limited to 500 characters", "oversized_event", 413);
  }
  if (Number.isNaN(now.getTime())) {
    throw new WakeBridgeSdkError("event clock is invalid", "invalid_event_clock", 500);
  }
  return {
    schema_version: EVENT_CONTRACT_VERSION,
    type: raw.type,
    occurred_at: raw.occurred_at ? absoluteTimestamp(raw.occurred_at, "occurred_at") : now.toISOString(),
    dedupe_key: raw.dedupe_key,
    coalesce_key: raw.coalesce_key ?? null,
    priority_hint: raw.priority_hint ?? null,
    attention_channel_hint: raw.attention_channel_hint ?? null,
    actor_ref: raw.actor_ref ?? null,
    resource: raw.resource,
    metadata,
    payload_preview: raw.payload_preview ?? null,
    idempotency_key: raw.idempotency_key ?? null,
  };
}

export interface SourcePushClientOptions {
  base_url: string;
  source: string;
  token: string;
  fetch?: typeof fetch;
  timeout_ms?: number;
}

export interface SourcePushReceipt {
  event: {
    id: string;
    source: string;
    type: string;
    dedupe_key: string;
    schema_version: number;
    occurred_at: string;
    received_at: string;
  };
  duplicate: boolean;
  suppressed: boolean;
  claim?: { id: string };
  [key: string]: JsonValue | object | undefined;
}

const SOURCE_ID = /^[A-Za-z0-9_.:-]{1,128}$/u;

function localOrigin(raw: string): URL {
  let url: URL;
  try { url = new URL(raw); } catch {
    throw new WakeBridgeSdkError("source push base_url is invalid", "invalid_source_push_config", 400);
  }
  if (url.protocol !== "http:" || !["127.0.0.1", "[::1]", "localhost"].includes(url.hostname)
    || url.username || url.password || url.pathname !== "/" || url.search || url.hash) {
    throw new WakeBridgeSdkError("source push base_url must be a credential-free loopback HTTP origin", "invalid_source_push_config", 400);
  }
  return url;
}

/** Narrow client for the credential-bound generic push ingress. */
export class SourcePushClient {
  private readonly origin: URL;
  private readonly fetchImpl: typeof fetch;
  private readonly timeoutMs: number;

  constructor(private readonly options: SourcePushClientOptions) {
    this.origin = localOrigin(options.base_url);
    if (!SOURCE_ID.test(options.source)) {
      throw new WakeBridgeSdkError("source push source is invalid", "invalid_source_push_config", 400);
    }
    if (typeof options.token !== "string" || options.token.length < 32) {
      throw new WakeBridgeSdkError("source push token must contain at least 32 characters", "invalid_source_push_config", 400);
    }
    this.fetchImpl = options.fetch ?? fetch;
    this.timeoutMs = options.timeout_ms ?? 15_000;
    if (!Number.isSafeInteger(this.timeoutMs) || this.timeoutMs < 1 || this.timeoutMs > 300_000) {
      throw new WakeBridgeSdkError("source push timeout_ms is invalid", "invalid_source_push_config", 400);
    }
  }

  async emit(input: WakeEventInput): Promise<SourcePushReceipt> {
    const event = validateWakeEventInput(input);
    let response: Response;
    try {
      response = await this.fetchImpl(new URL(`v1/ingress/${encodeURIComponent(this.options.source)}/events`, this.origin), {
        method: "POST",
        headers: { authorization: `Bearer ${this.options.token}`, "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify(event),
        redirect: "error",
        signal: AbortSignal.timeout(this.timeoutMs),
      });
    } catch (error) {
      throw new WakeBridgeSdkError(`source push is unavailable: ${String(error)}`, "source_push_unavailable", 503);
    }
    let body: unknown;
    try { body = await response.json(); } catch { body = null; }
    if (!response.ok) {
      const remote = body && typeof body === "object" ? body as Record<string, unknown> : {};
      throw new WakeBridgeSdkError(
        typeof remote.error === "string" ? remote.error : `source push returned HTTP ${response.status}`,
        typeof remote.code === "string" ? remote.code : "source_push_rejected",
        response.status,
      );
    }
    const receipt = body as Partial<SourcePushReceipt> | null;
    if (!receipt?.event || receipt.event.source !== this.options.source || receipt.event.schema_version !== EVENT_CONTRACT_VERSION) {
      throw new WakeBridgeSdkError("source push returned an invalid receipt", "invalid_source_push_receipt", 502);
    }
    return receipt as SourcePushReceipt;
  }
}

export type { JsonPrimitive, JsonValue, PriorityHint, ResourceRef, WakeEventInput } from "./types.js";
export { WakeBridgeSdkError } from "./sdk-error.js";
