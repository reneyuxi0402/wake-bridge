import { createInterface } from "node:readline";
import { BridgeError, WakeBridge } from "./core.js";
import type {
  AttentionClaim,
  BridgeConfig,
  BatchState,
  ClaimState,
  EventState,
  JsonValue,
  WakeEvent,
  WakeBatch,
} from "./types.js";
import type { AgentSourceControl } from "./source-control-client.js";
import { RELEASE_VERSION } from "./version.js";

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

const CLAIM_STATES: ClaimState[] = ["pending", "deferred", "eligible", "batched", "consumed", "dismissed", "expired"];
const BATCH_STATES: BatchState[] = ["pending", "waiting_for_endpoint", "waiting_for_waiter", "dispatching", "retry_wait", "dispatched", "seen", "cancelled", "needs_attention", "dead_letter"];
const EVENT_STATES: EventState[] = ["received", "matched", "suppressed", "batched", "consumed", "expired"];
const ABSOLUTE_RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/u;

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

function compactEvent(event: WakeEvent): JsonObject {
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
  };
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
      description: "Acknowledge a dispatched wake batch. Pass wake_batch_id from the Wake Bridge wake envelope.",
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
      description: "Mark one attention claim consumed after it was reviewed or handled.",
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
      description: "Defer one attention claim until a future ISO timestamp.",
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
      description: "Dismiss one attention claim without changing its authoritative source. Stale claims are expected when work was handled elsewhere and are cheap to dismiss; repeated irrelevant or identity-mismatched claims can still indicate a source or policy fault.",
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
      description: "List compact attention claims by state, channel, or source.",
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
      description: "List compact durable source events and their current match/consume state. Provider payloads and credentials are not returned.",
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
      description: "Inspect delivery health. Pass batch_id to drill into its claims, events, attempts, and receipts; omit it for compact actionable delivery issues.",
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
      description: "Inspect persistent inactivity watches, current binding generation, epoch, sequence, activity baseline, and next due time. Omit attention_channel to list all watches.",
      inputSchema: {
        type: "object",
        properties: { attention_channel: { type: "string", minLength: 1, maxLength: 200 } },
        additionalProperties: false,
      },
    },
    {
      name: "attention_watch_configure",
      description: "Enable, update, or disable a persistent inactivity watch for one attention channel. mode=once triggers once per inactive epoch; mode=repeat continues at repeat_after_seconds while the inactivity condition remains true. Repeating the same configuration is idempotent. The bound host supplies activity evidence.",
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
        return { claim: compactClaim(scheduled.claim), event_id: scheduled.event.id, duplicate: scheduled.duplicate };
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
        const claims = this.bridge.listClaims(channel ? { channel } : {});
        const batches = this.bridge.listBatches(channel ? { channel } : {});
        const claimCounts = Object.fromEntries(CLAIM_STATES.map((state) => [state, claims.filter((claim) => claim.state === state).length]));
        const batchCounts = Object.fromEntries(BATCH_STATES.map((state) => [state, batches.filter((batch) => batch.state === state).length]));
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
        const claims = this.bridge.listClaims({ state, channel, source, limit: rawLimit as number | undefined });
        return { claims: claims.map(compactClaim) };
      }
      case "attention_event_list": {
        assertAllowed(args, ["state", "source", "after", "limit"]);
        const state = asOptionalString(args, "state", 32) as EventState | undefined;
        if (state && !EVENT_STATES.includes(state)) throw new BridgeError("invalid event state", "invalid_arguments", 400);
        const source = asOptionalString(args, "source", 128);
        const after = args.after === undefined ? undefined : asAbsoluteTimestamp(args, "after");
        const limit = args.limit === undefined ? undefined : asInteger(args, "limit", 1, 1000);
        return { events: this.bridge.listEvents({ state, source, after, limit }).map(compactEvent) };
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
        const deliveryState = (batch: WakeBatch): string => batch.state === "dispatched" ? "accepted_but_unseen" : batch.state;
        const deliverySummary = (batch: WakeBatch): JsonObject => {
          const receipts = this.bridge.listReceipts(batch.id);
          const accepted = receipts.filter((receipt) => receipt.stage === "transport_accepted").at(-1);
          const seen = receipts.some((receipt) => receipt.stage === "agent_seen");
          return {
            ...compactBatch(batch),
            delivery_state: seen ? "seen" : deliveryState(batch),
            unseen_since: !seen && accepted ? accepted.at : null,
            health_evaluated_at: now,
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
          return { delivery: deliverySummary(batch), claims: claims.map(compactClaim), events: events.map(compactEvent), attempts, receipts };
        }
        const actionable = new Set<BatchState>(["waiting_for_endpoint", "waiting_for_waiter", "retry_wait", "dispatched", "needs_attention", "dead_letter"]);
        const batches = this.bridge.listBatches({ channel, limit: 1000 }).filter((batch) => actionable.has(batch.state)).slice(-limit);
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
      return { error: error.code, message: error.message.slice(0, 300) };
    }
    // Do not send stack traces, SQL, credentials, or arbitrary provider text
    // over the agent protocol. The detailed failure remains local to Core.
    return { error: "internal_error", message: "Wake Bridge operation failed" };
  }
}

export function createMcpServer(options: McpServerOptions | WakeBridge): WakeBridgeMcpServer {
  return new WakeBridgeMcpServer(options);
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

/** Construct a bridge and serve it until stdin closes. Used by the CLI. */
export async function runMcp(config: BridgeConfig): Promise<void> {
  const bridge = new WakeBridge(config);
  try {
    await runMcpStdio(new WakeBridgeMcpServer(bridge));
  } finally {
    bridge.close();
  }
}

export default WakeBridgeMcpServer;
