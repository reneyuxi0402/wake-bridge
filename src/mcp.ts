import { createInterface } from "node:readline";
import { BridgeError, ChannelInUseError, WakeBridge } from "./core.js";
import { sqlValue } from "./db.js";
import type {
  AttentionClaim,
  BatchState,
  ClaimState,
  EventState,
  JsonValue,
  WakeEvent,
  WakeBatch,
} from "./types.js";
import { BATCH_STATES, CLAIM_STATES, FINALIZED_CLAIM_STATES, OPEN_BATCH_STATES } from "./types.js";
import type { AgentSourceControl } from "./source-control-client.js";
import { RELEASE_VERSION } from "./version.js";
import { ABSOLUTE_RFC3339 } from "./validation.js";

/** MCP protocol version understood by this local stdio server. */
export const MCP_PROTOCOL_VERSION = "2025-06-18";
export const MCP_SERVER_NAME = "wake-bridge";
export const MCP_SERVER_VERSION = RELEASE_VERSION;
const SUPPORTED_PROTOCOL_VERSIONS = new Set([MCP_PROTOCOL_VERSION, "2025-03-26", "2024-11-05"]);

type RpcId = string | number | null;
type JsonObject = Record<string, unknown>;
type StdioInput = NodeJS.ReadableStream;
type StdioOutput = NodeJS.WritableStream;

export interface McpRequest {
  jsonrpc: "2.0";
  id?: RpcId;
  method: string;
  params?: JsonObject;
}

export interface McpResponse {
  jsonrpc: "2.0";
  id: RpcId;
  result?: JsonObject;
  error?: {
    code: number;
    message: string;
    data?: JsonValue;
  };
}

export interface McpServerOptions {
  bridge: WakeBridge;
  source_control?: AgentSourceControl;
}

const EVENT_STATES: EventState[] = ["received", "matched", "suppressed", "batched", "consumed", "expired"];
type ChannelSuppressionReason = "channel_deleted" | "channel_limit_reached";

function isPlainObject(value: unknown): value is JsonObject {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function compactClaim(claim: AttentionClaim): JsonObject {
  return {
    id: claim.id,
    claim_id: claim.id,
    origin: claim.origin,
    state: claim.state,
    resource: claim.resource as unknown as JsonValue,
    attention_channel: claim.attention_channel,
    eligible_after: claim.eligible_after,
    expires_at: claim.expires_at ?? null,
    defer_while_presence: claim.defer_while_presence,
    reason_code: claim.reason_code,
    ...(claim.note == null ? {} : { note: claim.note }),
    ...(claim.snooze_until == null ? {} : { snooze_until: claim.snooze_until }),
    event_ids: claim.event_ids,
  };
}

function compactBatch(batch: WakeBatch): JsonObject {
  return {
    id: batch.id,
    batch_id: batch.id,
    state: batch.state,
    attention_channel: batch.attention_channel,
    claim_ids: batch.claim_ids,
    event_ids: batch.event_ids,
    not_before: batch.not_before,
    deadline: batch.deadline ?? null,
    attempt: batch.attempt,
    binding_generation: batch.binding_generation ?? null,
    ...(batch.last_error == null ? {} : { last_error: batch.last_error }),
  };
}

function compactEvent(event: WakeEvent, suppressionReason: ChannelSuppressionReason | null): JsonObject {
  return {
    event_id: event.id,
    source: event.source,
    type: event.type,
    state: event.state,
    occurred_at: event.occurred_at,
    received_at: event.received_at,
    resource: event.resource as unknown as JsonValue,
    attention_channel_hint: event.attention_channel_hint ?? null,
    matched_policy_id: event.matched_policy_id ?? null,
    matched_policy_version: event.matched_policy_version ?? null,
    suppression_reason: suppressionReason,
  };
}

function compactEvents(bridge: WakeBridge, events: WakeEvent[]): JsonObject[] {
  if (!events.length) return [];
  // Hydrate only the selected page, once. Ingest suppression is distinct from
  // channel-deletion expiry and from an intentional policy suppression.
  const reasons = new Map(bridge.db.query<{ event_id: string; reason: ChannelSuppressionReason }>(`
    SELECT t.event_id, t.reason FROM event_transitions t
    JOIN events e ON e.id=t.event_id JOIN event_status s ON s.event_id=e.id
    WHERE e.instance_id=${sqlValue(bridge.config.instance_id)}
      AND e.id IN (${events.map((event) => sqlValue(event.id)).join(",")})
      AND s.state='suppressed' AND t.from_state='received' AND t.to_state='suppressed'
      AND t.reason IN ('channel_deleted','channel_limit_reached')
    ORDER BY t.seq;
  `).map((row) => [row.event_id, row.reason]));
  return events.map((event) => compactEvent(event, reasons.get(event.id) ?? null));
}

function deliveryNextStep(batch: WakeBatch, agentSeen: boolean, hostAttested: boolean, ackOverdue: boolean): string {
  if (ackOverdue) return "Agent acknowledgement is overdue. Verify actual host injection and whether the agent read the wake; host injection is not agent acknowledgement. After reading, the agent should call attention_ack with wake_batch_id; a genuine late attention_ack may still succeed. Core does not automatically resend this batch; the adapter must not acknowledge for the agent.";
  switch (batch.state) {
    case "pending":
      return "Wait for the dispatch time and a live binding on this channel; inspect the daemon if overdue.";
    case "waiting_for_endpoint":
      return "Register or renew the exact host session on this channel and check its compatible route. Core can dispatch when a live binding is available.";
    case "waiting_for_waiter":
      return "Inspect the instance version and host route; this legacy state is not selected by the current automatic dispatcher.";
    case "dispatching":
      return "The current dispatch attempt has not settled. Let it settle and inspect again; if the worker stopped, check daemon recovery. An early attention_ack can return batch_not_dispatched before acceptance is recorded. Do not substitute consume for acknowledgement.";
    case "retry_wait":
      return batch.last_error === "host_timeout" || batch.last_error === "transport_timeout"
        ? "Acceptance is unproven, but the host may already have received the wake. Inspect host injection and deduplication before further retries; never turn a timeout into a synthetic 202."
        : "Inspect the latest failed attempt and adapter health; Core can retry after not_before. A failed response does not by itself prove the host received nothing.";
    case "dispatched":
      return "Transport accepted; verify actual session injection. After reading the wake, the agent should call attention_ack with wake_batch_id. Missing acknowledgement alone does not cause an automatic resend.";
    case "seen":
      if (agentSeen) return "The agent acknowledged this batch. Consume remaining claims only after their work is handled; no automatic resend is scheduled.";
      if (hostAttested) return "The host proved session injection, not that the agent read it. After reading, the agent should call attention_ack with wake_batch_id; the adapter must not acknowledge for it.";
      return "Inspect receipts: the batch state alone does not establish agent_seen. No automatic resend is scheduled.";
    case "cancelled":
      if (batch.last_error === "channel_deleted") return "This channel was deleted. Stop retrying and inspect the terminal state with attention_status; deletion does not mean the claims were consumed. Messages already accepted by the host cannot be recalled.";
      return "This batch will not be dispatched again. Cancellation does not recall a message already accepted by the host.";
    case "needs_attention":
      if (batch.last_error === "ack_timeout") return "No timely agent acknowledgement was recorded. Inspect receipts and real session delivery; a genuine late attention_ack may still succeed. Core does not automatically resend this batch.";
      if (batch.last_error === "empty_batch") return "Inspect claim states, missing_claim_ids and the running daemon version. No deliverable claims were found, and needs_attention does not automatically retry. Do not acknowledge a never-accepted batch or edit the database to hide it.";
      return "Inspect attempts, receipts and claim states to resolve this issue. needs_attention does not automatically retry.";
    case "dead_letter":
      return "Correct the rejection before considering operator batch-retry with the inspected expected-attempt and a reason; only batches with retryable claims can reopen.";
  }
}

function asString(args: JsonObject, key: string, required = true, max = 4096): string | undefined {
  const value = args[key];
  if (value === undefined && !required) return undefined;
  if (typeof value !== "string" || !value.trim() || value.length > max) {
    throw new BridgeError(`${key} is required and bounded`, "invalid_arguments", 400);
  }
  return value;
}

function asOptionalString(args: JsonObject, key: string, max = 4096): string | undefined {
  return asString(args, key, false, max);
}

function asOptionalBoolean(args: JsonObject, key: string): boolean | undefined {
  const value = args[key];
  if (value === undefined) return undefined;
  if (typeof value !== "boolean") throw new BridgeError(`${key} must be boolean`, "invalid_arguments", 400);
  return value;
}

function asInteger(args: JsonObject, key: string, minimum: number, maximum: number): number {
  const value = args[key];
  if (!Number.isSafeInteger(value) || Number(value) < minimum || Number(value) > maximum) {
    throw new BridgeError(`${key} must be an integer between ${minimum} and ${maximum}`, "invalid_arguments", 400);
  }
  return Number(value);
}

function asAbsoluteTimestamp(args: JsonObject, key: string): string {
  const value = asString(args, key, true, 80)!;
  if (!ABSOLUTE_RFC3339.test(value) || Number.isNaN(new Date(value).getTime())) {
    throw new BridgeError(`${key} must be an absolute RFC3339 timestamp with Z or numeric offset`, "invalid_arguments", 400);
  }
  return value;
}

function normalizeResource(value: unknown): { uri: string; cursor?: string | number | null } {
  if (typeof value === "string") {
    if (!value.trim() || value.length > 2048) throw new BridgeError("resource URI is required and bounded", "invalid_arguments", 400);
    return { uri: value };
  }
  if (!isPlainObject(value)) throw new BridgeError("resource must be a URI or object", "invalid_arguments", 400);
  const unknown = Object.keys(value).filter((key) => !["uri", "cursor"].includes(key));
  if (unknown.length) throw new BridgeError("resource has unsupported fields", "invalid_arguments", 400);
  const uri = asString(value, "uri", true, 2048)!;
  const cursor = value.cursor;
  if (cursor !== undefined && cursor !== null && typeof cursor !== "string" && !(typeof cursor === "number" && Number.isFinite(cursor))) {
    throw new BridgeError("resource.cursor must be a string, number, or null", "invalid_arguments", 400);
  }
  return { uri, ...(cursor === undefined ? {} : { cursor }) };
}

function jsonSize(value: unknown): number {
  try { return JSON.stringify(value).length; } catch { return Number.POSITIVE_INFINITY; }
}

function asJsonValue(value: unknown, key: string, max = 4096): JsonValue | undefined {
  if (value === undefined) return undefined;
  if (jsonSize(value) > max) throw new BridgeError(`${key} is too large`, "oversized_arguments", 413);
  try {
    JSON.stringify(value);
  } catch {
    throw new BridgeError(`${key} must be JSON`, "invalid_arguments", 400);
  }
  return value as JsonValue;
}

function assertAllowed(args: JsonObject, allowed: string[]): void {
  const unknown = Object.keys(args).filter((key) => !allowed.includes(key));
  if (unknown.length) throw new BridgeError("unsupported tool arguments", "invalid_arguments", 400);
}

function toolText(value: JsonObject, isError = false): JsonObject {
  return {
    content: [{ type: "text", text: JSON.stringify(value) }],
    structuredContent: value as unknown as JsonValue,
    ...(isError ? { isError: true } : {}),
  };
}

function rpcError(id: RpcId, code: number, message: string): McpResponse {
  return { jsonrpc: "2.0", id, error: { code, message } };
}

function requestId(value: unknown): RpcId {
  return typeof value === "string" || (typeof value === "number" && Number.isFinite(value)) || value === null
    ? value as RpcId
    : null;
}

function toolDefinitions(): JsonObject[] {
  const resourceSchema = {
    anyOf: [
      { type: "string", minLength: 1, maxLength: 2048 },
      {
        type: "object",
        properties: {
          uri: { type: "string", minLength: 1, maxLength: 2048 },
          cursor: { anyOf: [{ type: "string" }, { type: "number" }, { type: "null" }] },
        },
        required: ["uri"],
        additionalProperties: false,
      },
    ],
  };
  return [
    {
      name: "attention_channel_create",
      description: "Create a named attention channel in this agent Space. The stable ID is generated unless attention_channel is supplied. Channel names need not be unique.",
      inputSchema: { type: "object", properties: { name: { type: "string", minLength: 1, maxLength: 200 }, attention_channel: { type: "string", minLength: 1, maxLength: 200 } }, required: ["name"], additionalProperties: false },
    },
    {
      name: "attention_channel_list",
      description: "List all active channels in this agent Space and max_channels, the configured active-channel limit (not a pagination limit).",
      inputSchema: { type: "object", properties: {}, additionalProperties: false },
    },
    {
      name: "attention_channel_rename",
      description: "Rename a channel without changing its stable ID, bindings, or history.",
      inputSchema: { type: "object", properties: { attention_channel: { type: "string", minLength: 1, maxLength: 200 }, name: { type: "string", minLength: 1, maxLength: 200 } }, required: ["attention_channel", "name"], additionalProperties: false },
    },
    {
      name: "attention_channel_delete",
      description: "Delete a channel. By default, a current binding, enabled inactivity watch, or current enabled supported policy with this literal target returns channel_in_use with safe usage details. Explicit force=true allows irreversible deletion of an in-use channel: detach its session and stop pending attention work. History is retained and the ID cannot be reused. In-flight messages or messages already sent to a host cannot be recalled.",
      inputSchema: { type: "object", properties: { attention_channel: { type: "string", minLength: 1, maxLength: 200 }, force: { type: "boolean", default: false } }, required: ["attention_channel"], additionalProperties: false },
    },
    {
      name: "attention_schedule",
      description: "Schedule a durable owner attention claim for a resource. eligible_after must be RFC3339 with Z or numeric offset. defer_while_presence defaults to true: while the owner is actively present on the bound channel, delivery waits instead of interrupting.",
      inputSchema: {
        type: "object",
        properties: {
          resource: resourceSchema,
          eligible_after: { type: "string", minLength: 1, pattern: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z|[+-]\\d{2}:\\d{2})$" },
          attention_channel: { type: "string", minLength: 1, maxLength: 200 },
          reason_code: { type: "string", maxLength: 256 },
          note: { type: ["string", "null"], maxLength: 500 },
          idempotency_key: { type: "string", maxLength: 500 },
          defer_while_presence: { type: "boolean", default: true },
        },
        required: ["resource", "eligible_after"],
        additionalProperties: false,
      },
    },
    {
      name: "attention_ack",
      description: "Acknowledge a dispatched wake batch. Pass wake_batch_id from the Wake Bridge wake envelope. If channel_deleted is returned, stop retrying and inspect the terminal state with attention_status; deletion is not consumption and must not be reported as consumed.",
      inputSchema: {
        type: "object",
        properties: {
          wake_batch_id: { type: "string", minLength: 1, maxLength: 200 },
          // batch_id remains a backwards-compatible alias for non-envelope
          // callers; endpoint/generation are intentionally not agent inputs.
          batch_id: { type: "string", minLength: 1, maxLength: 200 },
        },
        anyOf: [{ required: ["wake_batch_id"] }, { required: ["batch_id"] }],
        additionalProperties: false,
      },
    },
    {
      name: "attention_consume",
      description: "Mark one attention claim consumed after it was reviewed or handled. If channel_deleted is returned, stop retrying and inspect the terminal state with attention_status; deletion is not consumption and must not be reported as consumed.",
      inputSchema: {
        type: "object",
        properties: {
          claim_id: { type: "string", minLength: 1, maxLength: 200 },
          result: {},
        },
        required: ["claim_id"],
        additionalProperties: false,
      },
    },
    {
      name: "attention_snooze",
      description: "Defer one attention claim until a future ISO timestamp. If channel_deleted is returned, stop retrying and inspect the terminal state with attention_status; deletion is not consumption and must not be reported as consumed.",
      inputSchema: {
        type: "object",
        properties: {
          claim_id: { type: "string", minLength: 1, maxLength: 200 },
          until: { type: "string", minLength: 1, pattern: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z|[+-]\\d{2}:\\d{2})$" },
        },
        required: ["claim_id", "until"],
        additionalProperties: false,
      },
    },
    {
      name: "attention_dismiss",
      description: "Dismiss one attention claim without changing its authoritative source. Stale claims are expected when work was handled elsewhere and are cheap to dismiss; repeated irrelevant or identity-mismatched claims can still indicate a source or policy fault. If channel_deleted is returned, stop retrying and inspect the terminal state with attention_status; deletion is not consumption and must not be reported as consumed.",
      inputSchema: {
        type: "object",
        properties: {
          claim_id: { type: "string", minLength: 1, maxLength: 200 },
          reason: { type: "string", maxLength: 500 },
        },
        required: ["claim_id"],
        additionalProperties: false,
      },
    },
    {
      name: "attention_status",
      description: "Inspect compact claim and wake-batch counts, or one requested item.",
      inputSchema: {
        type: "object",
        properties: {
          claim_id: { type: "string", minLength: 1, maxLength: 200 },
          batch_id: { type: "string", minLength: 1, maxLength: 200 },
          channel: { type: "string", minLength: 1, maxLength: 200 },
        },
        additionalProperties: false,
      },
    },
    {
      name: "attention_list",
      description: "List compact attention claims by state, channel, or source. Returns the most recent `limit` matching claims (default 1000), oldest first.",
      inputSchema: {
        type: "object",
        properties: {
          state: { type: "string", enum: CLAIM_STATES },
          channel: { type: "string", minLength: 1, maxLength: 200 },
          source: { type: "string", minLength: 1, maxLength: 128 },
          limit: { type: "integer", minimum: 1, maximum: 1000 },
        },
        additionalProperties: false,
      },
    },
    {
      name: "attention_event_list",
      description: "List compact durable source events and their current match/consume state. suppression_reason is channel_deleted or channel_limit_reached only for channel suppression at ingest, otherwise null; attention_channel_hint is the original hint, not proof of the resolved route. Without `after`, returns the most recent `limit` events (default 100); with `after`, pages forward from that time. Results are oldest first. Provider payloads and credentials are not returned.",
      inputSchema: {
        type: "object",
        properties: {
          state: { type: "string", enum: EVENT_STATES },
          source: { type: "string", minLength: 1, maxLength: 128 },
          after: { type: "string", minLength: 1, pattern: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z|[+-]\\d{2}:\\d{2})$" },
          limit: { type: "integer", minimum: 1, maximum: 1000 },
        },
        additionalProperties: false,
      },
    },
    {
      name: "attention_binding_status",
      description: "Inspect the current channel-to-host binding, lease liveness, and public host capabilities without route credentials.",
      inputSchema: {
        type: "object",
        properties: { channel: { type: "string", minLength: 1, maxLength: 200 } },
        additionalProperties: false,
      },
    },
    {
      name: "attention_wake_health",
      description: "Inspect delivery evidence, automatic dispatch eligibility and next steps without changing state. Pass batch_id for claims, events, attempts, receipts and recent transitions; omit it for compact actionable issues.",
      inputSchema: {
        type: "object",
        properties: {
          batch_id: { type: "string", minLength: 1, maxLength: 200 },
          channel: { type: "string", minLength: 1, maxLength: 200 },
          limit: { type: "integer", minimum: 1, maximum: 100 },
        },
        additionalProperties: false,
      },
    },
    {
      name: "attention_watch_status",
      description: "Inspect persistent inactivity watches, current binding generation, epoch, sequence, activity baseline, and next due time. Omit attention_channel to list all watches. enabled does not mean armed: waiting_for_activity requires trusted independent activity from the current binding generation; binding alone is not a baseline, and Wake activity cannot arm the watch. This is not startup-liveness or endpoint-disconnection monitoring.",
      inputSchema: {
        type: "object",
        properties: { attention_channel: { type: "string", minLength: 1, maxLength: 200 } },
        additionalProperties: false,
      },
    },
    {
      name: "attention_watch_configure",
      description: "Enable, update, or disable a persistent inactivity watch for one attention channel. mode=once triggers once per inactive epoch; mode=repeat continues at repeat_after_seconds while the inactivity condition remains true. Repeating the same configuration is idempotent. enabled does not mean armed: waiting_for_activity requires trusted independent activity from the current binding generation, supplied by the bound host; binding alone is not a baseline, and Wake activity cannot arm the watch. This is not startup-liveness or endpoint-disconnection monitoring.",
      inputSchema: {
        type: "object",
        properties: {
          attention_channel: { type: "string", minLength: 1, maxLength: 200 },
          enabled: { type: "boolean" },
          idle_after_seconds: { type: "integer", minimum: 60, maximum: 604800 },
          mode: { type: "string", enum: ["once", "repeat"], default: "once" },
          repeat_after_seconds: { type: "integer", minimum: 60, maximum: 604800 },
        },
        required: ["attention_channel", "enabled", "idle_after_seconds"],
        additionalProperties: false,
      },
    },
    {
      name: "attention_source_status",
      description: "List installed source connector slots, durable identity checkpoints, and current enable/health state. Provider credentials are never returned.",
      inputSchema: { type: "object", properties: {}, additionalProperties: false },
    },
    {
      name: "attention_source_verify",
      description: "Read one installed connector manifest and compare its non-secret subject/binding identity with the durable checkpoint. This does not enable or poll the source.",
      inputSchema: { type: "object", properties: { source: { type: "string", minLength: 1, maxLength: 128 } }, required: ["source"], additionalProperties: false },
    },
    {
      name: "attention_source_bootstrap",
      description: "Create a first from-now checkpoint for a disabled source. Pass the exact subject_ref and binding_fingerprint returned by attention_source_verify; no historical events are created.",
      inputSchema: {
        type: "object",
        properties: {
          source: { type: "string", minLength: 1, maxLength: 128 },
          expected_subject_ref: { type: "string", minLength: 1, maxLength: 200 },
          expected_binding_fingerprint: { type: "string", pattern: "^sha256:[0-9a-f]{64}$" },
        },
        required: ["source", "expected_subject_ref", "expected_binding_fingerprint"], additionalProperties: false,
      },
    },
    {
      name: "attention_source_rebind",
      description: "Replace a disabled source checkpoint after connector identity changes. The previous checkpoint is archived; pass its exact revision and the newly verified subject/binding.",
      inputSchema: {
        type: "object",
        properties: {
          source: { type: "string", minLength: 1, maxLength: 128 },
          expected_checkpoint_revision: { type: "integer", minimum: 1 },
          expected_subject_ref: { type: "string", minLength: 1, maxLength: 200 },
          expected_binding_fingerprint: { type: "string", pattern: "^sha256:[0-9a-f]{64}$" },
          reason: { type: "string", minLength: 1, maxLength: 300 },
        },
        required: ["source", "expected_checkpoint_revision", "expected_subject_ref", "expected_binding_fingerprint", "reason"], additionalProperties: false,
      },
    },
    {
      name: "attention_source_enable",
      description: "Enable polling for a source only when its live connector identity matches its durable checkpoint. Enable intent survives daemon restart.",
      inputSchema: { type: "object", properties: { source: { type: "string", minLength: 1, maxLength: 128 } }, required: ["source"], additionalProperties: false },
    },
    {
      name: "attention_source_disable",
      description: "Stop future polling for a source without deleting events, claims, receipts, or checkpoint history. Disable intent survives daemon restart.",
      inputSchema: { type: "object", properties: { source: { type: "string", minLength: 1, maxLength: 128 } }, required: ["source"], additionalProperties: false },
    },
  ];
}

/**
 * In-process MCP implementation. It deliberately has no wait/warm tool: a
 * stdio invocation must finish so normal turns can settle and cold transport
 * remains the lifecycle baseline.
 */
export class WakeBridgeMcpServer {
  readonly bridge: WakeBridge;
  private readonly sourceControl?: AgentSourceControl;

  constructor(options: McpServerOptions | WakeBridge) {
    this.bridge = options instanceof WakeBridge ? options : options.bridge;
    this.sourceControl = options instanceof WakeBridge ? undefined : options.source_control;
  }

  async handleRequest(request: unknown): Promise<McpResponse | null> {
    if (!isPlainObject(request) || request.jsonrpc !== "2.0" || typeof request.method !== "string") {
      return rpcError(requestId(isPlainObject(request) ? request.id : null), -32600, "Invalid Request");
    }
    const idPresent = Object.prototype.hasOwnProperty.call(request, "id");
    const id = requestId(request.id);
    const method = request.method;
    if (!idPresent) {
      // MCP notifications, including initialized and cancellation notices,
      // never receive a response. Unknown notifications are ignored.
      return null;
    }
    if (method === "initialize") {
      const initializeParams = isPlainObject(request.params) ? request.params : null;
      const requestedProtocol = typeof initializeParams?.protocolVersion === "string"
        ? initializeParams.protocolVersion
        : MCP_PROTOCOL_VERSION;
      return {
        jsonrpc: "2.0",
        id,
        result: {
          protocolVersion: SUPPORTED_PROTOCOL_VERSIONS.has(requestedProtocol) ? requestedProtocol : MCP_PROTOCOL_VERSION,
          capabilities: { tools: { listChanged: false } } as unknown as JsonValue,
          serverInfo: { name: MCP_SERVER_NAME, version: MCP_SERVER_VERSION } as unknown as JsonValue,
        },
      };
    }
    if (method === "ping") return { jsonrpc: "2.0", id, result: {} };
    if (method === "tools/list") {
      return { jsonrpc: "2.0", id, result: { tools: toolDefinitions() as unknown as JsonValue } };
    }
    if (method !== "tools/call") return rpcError(id, -32601, "Method not found");
    const params = request.params;
    if (!isPlainObject(params) || typeof params.name !== "string") return rpcError(id, -32602, "Invalid tool call");
    const args = params.arguments === undefined ? {} : params.arguments;
    if (!isPlainObject(args)) return rpcError(id, -32602, "Tool arguments must be an object");
    try {
      const result = await this.callTool(params.name, args);
      return { jsonrpc: "2.0", id, result: toolText(result) };
    } catch (error) {
      const normalized = this.errorValue(error);
      return { jsonrpc: "2.0", id, result: toolText(normalized, true) };
    }
  }

  async callTool(name: string, args: JsonObject): Promise<JsonObject> {
    switch (name) {
      case "attention_channel_create": {
        assertAllowed(args, ["name", "attention_channel"]);
        return { channel: this.bridge.createChannel({ name: asString(args, "name", true, 200)!, attention_channel: asOptionalString(args, "attention_channel", 200) }) };
      }
      case "attention_channel_list": {
        assertAllowed(args, []);
        return { max_channels: this.bridge.config.max_channels, channels: this.bridge.listChannels() };
      }
      case "attention_channel_rename": {
        assertAllowed(args, ["attention_channel", "name"]);
        return { channel: this.bridge.renameChannel(asString(args, "attention_channel", true, 200)!, asString(args, "name", true, 200)!) };
      }
      case "attention_channel_delete": {
        assertAllowed(args, ["attention_channel", "force"]);
        return { channel: this.bridge.deleteChannel(asString(args, "attention_channel", true, 200)!, { force: asOptionalBoolean(args, "force") ?? false }) };
      }
      case "attention_schedule": {
        assertAllowed(args, ["resource", "eligible_after", "attention_channel", "reason_code", "note", "idempotency_key", "defer_while_presence"]);
        const scheduled = this.bridge.scheduleClaim({
          resource: normalizeResource(args.resource),
          eligible_after: asAbsoluteTimestamp(args, "eligible_after"),
          attention_channel: asOptionalString(args, "attention_channel", 200),
          reason_code: asOptionalString(args, "reason_code", 256),
          note: args.note === null ? null : asOptionalString(args, "note", 500),
          idempotency_key: asOptionalString(args, "idempotency_key", 500),
          defer_while_presence: asOptionalBoolean(args, "defer_while_presence"),
        });
        return { claim: compactClaim(scheduled.claim), event_id: scheduled.event.id, duplicate: scheduled.duplicate, warnings: scheduled.warnings };
      }
      case "attention_ack": {
        assertAllowed(args, ["wake_batch_id", "batch_id"]);
        const envelopeBatchId = asOptionalString(args, "wake_batch_id", 200);
        const aliasBatchId = asOptionalString(args, "batch_id", 200);
        if (!envelopeBatchId && !aliasBatchId) throw new BridgeError("wake_batch_id is required", "invalid_arguments", 400);
        if (envelopeBatchId && aliasBatchId && envelopeBatchId !== aliasBatchId) {
          throw new BridgeError("wake_batch_id and batch_id disagree", "invalid_arguments", 400);
        }
        const batchId = envelopeBatchId || aliasBatchId!;
        const batch = this.bridge.getBatch(batchId);
        if (!batch) throw new BridgeError("batch not found", "batch_not_found", 404);
        const accepted = this.bridge.listAttempts(batchId)
          .filter((attempt) => String(attempt.state) === "accepted")
          .sort((left, right) => String(right.started_at).localeCompare(String(left.started_at)))[0];
        if (!accepted?.endpoint_id || accepted.binding_generation == null) {
          throw new BridgeError("batch has no accepted transport attempt", "batch_not_dispatched", 409);
        }
        // Core re-checks the current binding and generation. Deriving these
        // values from the durable attempt prevents an agent from inventing an
        // endpoint while keeping the public tool usable from WakePayload alone.
        const acknowledged = this.bridge.ackBatch(batchId, String(accepted.endpoint_id), Number(accepted.binding_generation));
        return { batch: compactBatch(acknowledged) };
      }
      case "attention_consume": {
        assertAllowed(args, ["claim_id", "result"]);
        const claim = this.bridge.consumeClaim(asString(args, "claim_id", true, 200)!, asJsonValue(args.result, "result"));
        return { claim: compactClaim(claim) };
      }
      case "attention_snooze": {
        assertAllowed(args, ["claim_id", "until"]);
        const claim = this.bridge.snoozeClaim(asString(args, "claim_id", true, 200)!, asAbsoluteTimestamp(args, "until"));
        return { claim: compactClaim(claim) };
      }
      case "attention_dismiss": {
        assertAllowed(args, ["claim_id", "reason"]);
        const claim = this.bridge.dismissClaim(asString(args, "claim_id", true, 200)!, asOptionalString(args, "reason", 500) || "dismissed");
        return { claim: compactClaim(claim) };
      }
      case "attention_status": {
        assertAllowed(args, ["claim_id", "batch_id", "channel"]);
        const claimId = asOptionalString(args, "claim_id", 200);
        const batchId = asOptionalString(args, "batch_id", 200);
        const channel = asOptionalString(args, "channel", 200);
        if (claimId) {
          const claim = this.bridge.getClaim(claimId);
          if (!claim) throw new BridgeError("claim not found", "claim_not_found", 404);
          return { claim: compactClaim(claim) };
        }
        if (batchId) {
          const batch = this.bridge.getBatch(batchId);
          if (!batch) throw new BridgeError("batch not found", "batch_not_found", 404);
          return { batch: compactBatch(batch) };
        }
        const claimCounts = this.bridge.countClaimsByState(channel ? { channel } : {});
        const batchCounts = this.bridge.countBatchesByState(channel ? { channel } : {});
        return { now: this.bridge.nowIso(), claims: claimCounts, batches: batchCounts };
      }
      case "attention_list": {
        assertAllowed(args, ["state", "channel", "source", "limit"]);
        const state = asOptionalString(args, "state", 32) as ClaimState | undefined;
        if (state && !CLAIM_STATES.includes(state)) throw new BridgeError("invalid claim state", "invalid_arguments", 400);
        const source = asOptionalString(args, "source", 128);
        const channel = asOptionalString(args, "channel", 200);
        const rawLimit = args.limit;
        if (rawLimit !== undefined && (!Number.isInteger(rawLimit) || Number(rawLimit) < 1 || Number(rawLimit) > 1000)) {
          throw new BridgeError("limit must be an integer between 1 and 1000", "invalid_arguments", 400);
        }
        const claims = this.bridge.listClaims({ state, channel, source, limit: rawLimit as number | undefined, newest_first: true });
        return { claims: claims.map(compactClaim) };
      }
      case "attention_event_list": {
        assertAllowed(args, ["state", "source", "after", "limit"]);
        const state = asOptionalString(args, "state", 32) as EventState | undefined;
        if (state && !EVENT_STATES.includes(state)) throw new BridgeError("invalid event state", "invalid_arguments", 400);
        const source = asOptionalString(args, "source", 128);
        const after = args.after === undefined ? undefined : asAbsoluteTimestamp(args, "after");
        const limit = args.limit === undefined ? undefined : asInteger(args, "limit", 1, 1000);
        // Without `after` this is "show me what is going on", so take the newest page.
        return { events: compactEvents(this.bridge, this.bridge.listEvents({ state, source, after, limit, newest_first: after === undefined })) };
      }
      case "attention_binding_status": {
        assertAllowed(args, ["channel"]);
        const channel = asOptionalString(args, "channel", 200);
        const now = this.bridge.nowIso();
        const bindings = (channel ? [this.bridge.getBinding(channel)].filter(Boolean) : this.bridge.listBindings()).map((binding) => {
          const endpoint = this.bridge.getEndpoint(binding!.endpoint_id);
          return {
            attention_channel: binding!.attention_channel,
            endpoint_id: binding!.endpoint_id,
            generation: binding!.generation,
            bound_at: binding!.bound_at,
            live: Boolean(endpoint && endpoint.lease_expires_at > now),
            endpoint: endpoint ? {
              host_kind: endpoint.host_kind,
              session_ref: endpoint.session_ref,
              lease_expires_at: endpoint.lease_expires_at,
              capabilities: endpoint.capabilities as unknown as JsonValue,
            } : null,
          };
        });
        if (channel) return { now, binding: bindings[0] ?? null };
        return { now, bindings };
      }
      case "attention_wake_health": {
        assertAllowed(args, ["batch_id", "channel", "limit"]);
        const batchId = asOptionalString(args, "batch_id", 200);
        const channel = asOptionalString(args, "channel", 200);
        const limit = args.limit === undefined ? 50 : asInteger(args, "limit", 1, 100);
        const now = this.bridge.nowIso();
        // Missing refs remain actionable; an empty array is not ack debt.
        // Use the same predicate before LIMIT and for per-batch diagnostics.
        const unfinishedClaims = `EXISTS (
          SELECT 1 FROM json_each(b.claim_ids_json) j LEFT JOIN claims c ON c.id=j.value AND c.instance_id=b.instance_id
          WHERE c.id IS NULL OR c.state NOT IN (${FINALIZED_CLAIM_STATES.map(sqlValue).join(",")})
        )`;
        const deliveryState = (batch: WakeBatch): string => batch.state === "dispatched" ? "accepted_but_unseen" : batch.state;
        const deliverySummary = (batch: WakeBatch): JsonObject => {
          const receipts = this.bridge.listReceipts(batch.id);
          const accepted = receipts.filter((receipt) => receipt.stage === "transport_accepted").at(-1);
          const seen = receipts.some((receipt) => receipt.stage === "agent_seen");
          const hostAttested = receipts.some((receipt) => receipt.stage === "host_attested");
          const eligible = OPEN_BATCH_STATES.includes(batch.state);
          const notBefore = batch.state !== "waiting_for_endpoint" && batch.deadline && batch.deadline > batch.not_before
            ? batch.deadline : batch.not_before;
          const awaitingAck = !seen && (batch.state === "dispatched" || batch.state === "seen"
            || (batch.state === "needs_attention" && batch.last_error === "ack_timeout"));
          // Match the current accepted attempt: either its completion/route
          // receipt or its consumed nonce correlation proves acceptance/injection.
          const currentAccepted = awaitingAck ? this.bridge.db.query<{ finished_at: string }>(`
            SELECT a.finished_at FROM batches b JOIN outbox_attempts a ON a.batch_id=b.id
            WHERE b.id=${sqlValue(batch.id)} AND b.instance_id=${sqlValue(this.bridge.config.instance_id)}
              AND a.attempt_no=b.attempt AND a.state='accepted' AND a.binding_generation=b.binding_generation
              AND a.finished_at IS NOT NULL AND ${unfinishedClaims}
              AND (EXISTS (SELECT 1 FROM receipts r WHERE r.batch_id=b.id AND r.stage='transport_accepted'
                AND r.at=a.finished_at AND r.endpoint_id=a.endpoint_id AND r.binding_generation=a.binding_generation
                AND r.transport_kind=a.transport_kind)
                OR EXISTS (SELECT 1 FROM delivery_correlations dc WHERE dc.attempt_id=a.id AND dc.batch_id=b.id
                  AND dc.state='consumed' AND dc.endpoint_id=a.endpoint_id AND dc.binding_generation=a.binding_generation
                  AND dc.transport_kind=a.transport_kind))
            LIMIT 1;
          `)[0] : undefined;
          const deadlineMs = currentAccepted ? Date.parse(currentAccepted.finished_at) + this.bridge.config.ack_timeout_ms! : null;
          const deadlineDate = deadlineMs === null ? null : new Date(deadlineMs);
          // Valid extreme timeouts can exceed Date's representable range.
          // Keep the pending evidence and numeric comparison, without inventing a date.
          const deadlineAt = deadlineDate && !Number.isNaN(deadlineDate.getTime()) ? deadlineDate.toISOString() : null;
          const overdue = deadlineMs !== null && Date.parse(now) >= deadlineMs;
          return {
            ...compactBatch(batch),
            delivery_state: seen ? "seen" : deliveryState(batch),
            unseen_since: !seen && accepted ? accepted.at : null,
            health_evaluated_at: now,
            // Eligibility is a state snapshot, not a promise that the daemon is
            // running or that a current endpoint/claim will pass dispatch checks.
            core_dispatch: { eligible, in_flight: batch.state === "dispatching", not_before: eligible ? notBefore : null },
            evidence: { transport_accepted: Boolean(accepted), host_attested: hostAttested, agent_seen: seen },
            agent_ack: { pending: Boolean(currentAccepted), overdue, deadline_at: deadlineAt },
            next_step: deliveryNextStep(batch, seen, hostAttested, overdue),
          };
        };
        if (batchId) {
          const batch = this.bridge.getBatch(batchId);
          if (!batch) throw new BridgeError("batch not found", "batch_not_found", 404);
          const claims = batch.claim_ids.map((claimId) => this.bridge.getClaim(claimId)).filter((claim): claim is AttentionClaim => Boolean(claim));
          const events = batch.event_ids.map((eventId) => this.bridge.getEvent(eventId)).filter((event): event is WakeEvent => Boolean(event));
          const attempts = this.bridge.listAttempts(batch.id).map((attempt) => ({
            attempt_id: String(attempt.id),
            attempt_no: Number(attempt.attempt_no),
            state: String(attempt.state),
            transport_kind: attempt.transport_kind == null ? null : String(attempt.transport_kind),
            endpoint_id: attempt.endpoint_id == null ? null : String(attempt.endpoint_id),
            binding_generation: attempt.binding_generation == null ? null : Number(attempt.binding_generation),
            started_at: String(attempt.started_at),
            finished_at: attempt.finished_at == null ? null : String(attempt.finished_at),
            error_class: attempt.error_class == null ? null : String(attempt.error_class),
          }));
          const receipts = this.bridge.listReceipts(batch.id).map((receipt) => ({
            receipt_id: receipt.id,
            claim_id: receipt.claim_id,
            stage: receipt.stage,
            at: receipt.at,
            endpoint_id: receipt.endpoint_id,
            binding_generation: receipt.binding_generation,
            transport_kind: receipt.transport_kind,
          }));
          // Reasons can contain operator/provider free text. Only project the
          // state edges and timestamps; attempts already expose error classes.
          const recentTransitions = this.bridge.db.query<{ seq: number; from_state: string | null; to_state: string; at: string }>(
            `SELECT seq, from_state, to_state, at FROM batch_transitions WHERE batch_id=${sqlValue(batch.id)} ORDER BY seq DESC LIMIT 101;`,
          );
          const presentClaims = new Set(claims.map((claim) => claim.id));
          return {
            delivery: deliverySummary(batch), claims: claims.map(compactClaim), events: compactEvents(this.bridge, events), attempts, receipts,
            missing_claim_ids: batch.claim_ids.filter((claimId) => !presentClaims.has(claimId)),
            transitions: recentTransitions.slice(0, 100).reverse(),
            transitions_truncated: recentTransitions.length > 100,
          };
        }
        const actionable: BatchState[] = ["waiting_for_endpoint", "waiting_for_waiter", "retry_wait", "dispatched", "needs_attention", "dead_letter"];
        // Select the newest actionable ids first, then hydrate only that page
        // in the existing oldest-first response order.
        const batchIds = this.bridge.db.query<{ id: string }>(`
          SELECT b.id FROM batches b WHERE b.instance_id=${sqlValue(this.bridge.config.instance_id)}
            ${channel ? `AND b.attention_channel=${sqlValue(channel)}` : ""}
            AND (b.state IN (${actionable.map(sqlValue).join(",")}) OR (
              b.state='seen' AND ${unfinishedClaims}
              AND EXISTS (SELECT 1 FROM receipts r WHERE r.batch_id=b.id AND r.stage='host_attested')
              AND NOT EXISTS (SELECT 1 FROM receipts r WHERE r.batch_id=b.id AND r.stage='agent_seen')
            ))
          ORDER BY b.created_at DESC LIMIT ${limit};
        `);
        const batches = batchIds.reverse().map((row) => this.bridge.getBatch(row.id)).filter((batch): batch is WakeBatch => Boolean(batch));
        return {
          now,
          deliveries: batches.map(deliverySummary),
          activity_watches: channel ? [this.bridge.getActivityWatch(channel)].filter(Boolean) : this.bridge.listActivityWatches(),
        };
      }
      case "attention_watch_status": {
        assertAllowed(args, ["attention_channel"]);
        const channel = asOptionalString(args, "attention_channel", 200);
        if (channel) return { activity_watch: this.bridge.getActivityWatch(channel) };
        return { activity_watches: this.bridge.listActivityWatches() };
      }
      case "attention_watch_configure": {
        assertAllowed(args, ["attention_channel", "enabled", "idle_after_seconds", "mode", "repeat_after_seconds"]);
        if (typeof args.enabled !== "boolean") throw new BridgeError("enabled must be boolean", "invalid_arguments", 400);
        const watch = this.bridge.configureActivityWatch({
          attention_channel: asString(args, "attention_channel", true, 200)!,
          enabled: args.enabled,
          idle_after_ms: asInteger(args, "idle_after_seconds", 60, 604800) * 1000,
          mode: asOptionalString(args, "mode", 32) as "once" | "repeat" | undefined,
          repeat_after_ms: args.repeat_after_seconds === undefined
            ? undefined
            : asInteger(args, "repeat_after_seconds", 60, 604800) * 1000,
        });
        return { activity_watch: watch };
      }
      case "attention_source_status":
        assertAllowed(args, []);
        return await this.sources().status();
      case "attention_source_verify":
        assertAllowed(args, ["source"]);
        return await this.sources().verify(asString(args, "source", true, 128)!);
      case "attention_source_bootstrap":
        assertAllowed(args, ["source", "expected_subject_ref", "expected_binding_fingerprint"]);
        return await this.sources().bootstrap(asString(args, "source", true, 128)!, {
          expected_subject_ref: asString(args, "expected_subject_ref", true, 200)!,
          expected_binding_fingerprint: asString(args, "expected_binding_fingerprint", true, 80)!,
        });
      case "attention_source_rebind": {
        assertAllowed(args, ["source", "expected_checkpoint_revision", "expected_subject_ref", "expected_binding_fingerprint", "reason"]);
        const revision = Number(args.expected_checkpoint_revision);
        if (!Number.isSafeInteger(revision) || revision < 1) throw new BridgeError("expected_checkpoint_revision is invalid", "invalid_arguments", 400);
        return await this.sources().rebind(asString(args, "source", true, 128)!, {
          expected_checkpoint_revision: revision,
          expected_subject_ref: asString(args, "expected_subject_ref", true, 200)!,
          expected_binding_fingerprint: asString(args, "expected_binding_fingerprint", true, 80)!,
          reason: asString(args, "reason", true, 300)!,
        });
      }
      case "attention_source_enable":
        assertAllowed(args, ["source"]);
        return await this.sources().enable(asString(args, "source", true, 128)!);
      case "attention_source_disable":
        assertAllowed(args, ["source"]);
        return await this.sources().disable(asString(args, "source", true, 128)!);
      default:
        throw new BridgeError("unknown tool", "unknown_tool", 404);
    }
  }

  private sources(): AgentSourceControl {
    if (!this.sourceControl) throw new BridgeError("Wake Bridge source control daemon is not configured", "source_control_unavailable", 503);
    return this.sourceControl;
  }

  private errorValue(error: unknown): JsonObject {
    if (error instanceof BridgeError) {
      return { error: error.code, message: error.message.slice(0, 300), ...(error instanceof ChannelInUseError ? { usage: error.usage } : {}) };
    }
    // Do not send stack traces, SQL, credentials, or arbitrary provider text
    // over the agent protocol. The detailed failure remains local to Core.
    return { error: "internal_error", message: "Wake Bridge operation failed" };
  }
}

/** Run newline-delimited JSON-RPC over stdin/stdout, with no stdout logging. */
export async function runMcpStdio(server: WakeBridgeMcpServer, input: StdioInput = process.stdin, output: StdioOutput = process.stdout): Promise<void> {
  const lines = createInterface({ input, crlfDelay: Infinity });
  for await (const line of lines) {
    if (!String(line).trim()) continue;
    let request: unknown;
    try {
      request = JSON.parse(String(line));
    } catch {
      output.write(`${JSON.stringify(rpcError(null, -32700, "Parse error"))}\n`);
      continue;
    }
    const response = await server.handleRequest(request);
    if (response) output.write(`${JSON.stringify(response)}\n`);
  }
}

export default WakeBridgeMcpServer;
