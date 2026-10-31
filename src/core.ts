import { randomBytes } from "node:crypto";
import { performance } from "node:perf_hooks";
import { setImmediate } from "node:timers/promises";
import { SqliteDatabase, sqlJson, sqlValue } from "./db.js";
import {
  addMs,
  asDate,
  assertFiniteMs,
  getPath,
  id,
  isoNow,
  nextLocalClock,
  parseJson,
  quietEndAfter,
  quietWindowAt,
  sha256,
  stableJson,
} from "./util.js";
import { MockTransport } from "./transport.js";
import { validateWakeEventInput } from "./event-sdk.js";
import { WakeBridgeSdkError } from "./sdk-error.js";
import { ABSOLUTE_RFC3339, LOCAL_TIME, SECRET_ADDRESS_KEY, SOURCE_ID } from "./validation.js";
import { ACTIVE_CLAIM_STATES, BATCH_STATES, CLAIM_STATES, FINALIZED_CLAIM_STATES, OPEN_BATCH_STATES } from "./types.js";
import type {
  ActivityObservation,
  ActivityObservationResult,
  ActivityWatch,
  ActivityWatchConfiguration,
  AttentionChannel,
  AttentionClaim,
  BatchState,
  Binding,
  BridgeConfig,
  ChannelUsage,
  ClaimOrigin,
  ClaimState,
  DispatchResult,
  DeliveryCorrelation,
  Endpoint,
  EndpointCapabilities,
  EndpointRegistration,
  EventState,
  InstalledPolicyRule,
  JsonValue,
  PolicyMatchResult,
  PolicyPreviewResult,
  PolicyRule,
  PolicyVersionStatus,
  PresenceLease,
  PresenceRenewal,
  Receipt,
  ReceiptStage,
  ResourceRef,
  TickResult,
  TransportContext,
  TransportResult,
  WakeBatch,
  WakeEvent,
  WakeEventInput,
  WakeEchoObservation,
  WakePayload,
  WakeTransport,
} from "./types.js";

export interface WakeBridgeOptions {
  clock?: () => Date;
  transports?: WakeTransport[];
  autoMockTransport?: boolean;
  /** Only the canonical dispatcher owner may reclaim leases and reconcile overdue acknowledgements. */
  recoverDispatchLeases?: boolean;
  /** Schema changes are only authorized by the release upgrade workflow. */
  allowSchemaUpgrade?: boolean;
}

export interface EmitResult {
  event: WakeEvent;
  claim?: AttentionClaim;
  duplicate: boolean;
  suppressed: boolean;
}

export interface ScheduleClaimInput {
  resource: ResourceRef;
  eligible_after: string;
  attention_channel?: string;
  reason_code?: string;
  note?: string | null;
  expires_at?: string | null;
  idempotency_key?: string | null;
  defer_while_presence?: boolean;
}

export interface ScheduleClaimWarning {
  code: "no_current_binding";
  attention_channel: string;
}

export interface ScheduleClaimResult {
  event: WakeEvent;
  claim: AttentionClaim;
  duplicate: boolean;
  warnings: ScheduleClaimWarning[];
}

export class BridgeError extends WakeBridgeSdkError {
  constructor(
    message: string,
    readonly code: string = "invalid_request",
    readonly status: number = 400,
  ) {
    super(message, code, status);
    this.name = "BridgeError";
  }
}

export class ChannelInUseError extends BridgeError {
  constructor(readonly usage: ChannelUsage) {
    super("attention channel is in use", "channel_in_use", 409);
    this.name = "ChannelInUseError";
  }
}

type DbRow = Record<string, any>;

const DEFAULT_POLICY: PolicyRule = {
  id: "default",
  version: 1,
  enabled: true,
  order: -1_000_000,
  match: {},
  delivery: {
    mode: "immediate",
    quiet_hours_policy: "defer",
    foreground_presence_policy: "defer",
  },
  batch: { coalesce_by: "coalesce_key", max_events: 20, window_ms: 0 },
  target: { attention_channel: "${attention_channel_hint}" },
  reason_code: "event",
};

const DEFAULT_ACK_TIMEOUT_MS = 30 * 60_000;
// Date accepts millisecond values through ±8.64e15. Keep the configured
// timeout inside that range so computing a cutoff cannot produce an invalid
// timestamp even though Number.MAX_SAFE_INTEGER is a little larger.
const MAX_ACK_TIMEOUT_MS = 8_640_000_000_000_000;
const CHANNEL_WRITE_ALLOWED = "(SELECT status='ok' FROM channel_write_guard)";

function validateChannelId(channel: string): void {
  if (typeof channel !== "string" || !channel.trim() || channel.length > 200 || /[\u0000-\u001f\u007f]/u.test(channel)) {
    throw new BridgeError("attention channel must be a printable string of 1 to 200 characters", "invalid_channel", 400);
  }
}

function channelName(name: string): string {
  const clean = typeof name === "string" ? name.trim() : "";
  if (!clean || clean.length > 200 || /[\u0000-\u001f\u007f]/u.test(clean)) {
    throw new BridgeError("channel name must be a printable string of 1 to 200 characters", "invalid_channel", 400);
  }
  return clean;
}

function validateAckTimeout(value: unknown): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0 || value > MAX_ACK_TIMEOUT_MS) {
    throw new BridgeError("ack_timeout_ms must be a positive safe integer within the supported date range", "invalid_config", 400);
  }
}

function validateResource(resource: ResourceRef): void {
  if (!resource || typeof resource !== "object" || typeof resource.uri !== "string") {
    throw new BridgeError("resource.uri is required", "invalid_resource", 400);
  }
  if (resource.uri.length === 0 || resource.uri.length > 2048 || !/^[A-Za-z][A-Za-z0-9+.-]*:/.test(resource.uri)) {
    throw new BridgeError("resource.uri must be a bounded absolute URI", "invalid_resource", 400);
  }
  if (JSON.stringify(resource).length > 4096) throw new BridgeError("resource is too large", "oversized_event", 413);
}

const POLICY_ID = /^[A-Za-z][A-Za-z0-9._-]{0,127}$/u;

export function normalizePolicy(policy: PolicyRule): PolicyRule {
  if (!policy || typeof policy.id !== "string" || !POLICY_ID.test(policy.id)
    || !Number.isSafeInteger(policy.version) || policy.version < 1) {
    throw new BridgeError("policy id and positive integer version are required", "invalid_policy", 400);
  }
  if (policy.enabled != null && typeof policy.enabled !== "boolean") {
    throw new BridgeError("policy enabled must be boolean", "invalid_policy", 400);
  }
  const delivery = policy.delivery;
  if (!delivery || !["immediate", "scheduled", "suppress"].includes(String(delivery.mode))) {
    throw new BridgeError(`unsupported delivery mode: ${String(delivery?.mode)}`, "unsupported_policy_mode", 400);
  }
  const rawDelivery = delivery as unknown as Record<string, unknown>;
  const unsupportedDelivery = ["debounce_ms", "cadence_ms", "max_delay_ms"].filter((key) => Object.prototype.hasOwnProperty.call(rawDelivery, key));
  const rawTarget = (policy.target ?? {}) as unknown as Record<string, unknown>;
  const unsupportedTarget = ["prefer_transport", "busy_behavior"].filter((key) => Object.prototype.hasOwnProperty.call(rawTarget, key));
  if (unsupportedDelivery.length || unsupportedTarget.length) {
    throw new BridgeError(`unsupported policy field(s): ${[...unsupportedDelivery, ...unsupportedTarget].join(", ")}`, "unsupported_policy_field", 400);
  }
  if (delivery.mode === "scheduled" && (typeof delivery.scheduled_local_time !== "string"
    || !LOCAL_TIME.test(delivery.scheduled_local_time))) {
    throw new BridgeError("scheduled policy requires scheduled_local_time in HH:MM form", "invalid_policy", 400);
  }
  if (delivery.mode !== "scheduled" && delivery.scheduled_local_time != null) {
    throw new BridgeError("scheduled_local_time is only valid for scheduled mode", "invalid_policy", 400);
  }
  if (delivery.quiet_hours_policy != null && !["defer", "bypass"].includes(delivery.quiet_hours_policy)) {
    throw new BridgeError("quiet_hours_policy must be defer or bypass", "invalid_policy", 400);
  }
  if (delivery.foreground_presence_policy != null && !["defer", "bypass"].includes(delivery.foreground_presence_policy)) {
    throw new BridgeError("foreground_presence_policy must be defer or bypass", "invalid_policy", 400);
  }
  if (policy.order != null && (!Number.isSafeInteger(policy.order) || Math.abs(policy.order) > 1_000_000)) {
    throw new BridgeError("policy order must be an integer between -1000000 and 1000000", "invalid_policy", 400);
  }
  if (policy.match != null && (typeof policy.match !== "object" || Array.isArray(policy.match) || JSON.stringify(policy.match).length > 8192)) {
    throw new BridgeError("policy match must be a bounded object", "invalid_policy", 400);
  }
  if (policy.batch?.max_events != null && (!Number.isSafeInteger(policy.batch.max_events)
    || policy.batch.max_events < 1 || policy.batch.max_events > 1000)) {
    throw new BridgeError("batch.max_events must be an integer between 1 and 1000", "invalid_policy", 400);
  }
  if (policy.batch?.window_ms != null && (!Number.isSafeInteger(policy.batch.window_ms)
    || policy.batch.window_ms < 0 || policy.batch.window_ms > 7 * 24 * 60 * 60_000)) {
    throw new BridgeError("batch.window_ms must be an integer between 0 and 604800000", "invalid_policy", 400);
  }
  if (policy.batch?.coalesce_by != null && (typeof policy.batch.coalesce_by !== "string"
    || !policy.batch.coalesce_by || policy.batch.coalesce_by.length > 256)) {
    throw new BridgeError("batch.coalesce_by must be a non-empty bounded string", "invalid_policy", 400);
  }
  if (policy.target?.attention_channel != null && (typeof policy.target.attention_channel !== "string"
    || !policy.target.attention_channel || policy.target.attention_channel.length > 200)) {
    throw new BridgeError("target.attention_channel must be a non-empty string of at most 200 characters", "invalid_policy", 400);
  }
  if (policy.reason_code != null && (typeof policy.reason_code !== "string" || !policy.reason_code || policy.reason_code.length > 128)) {
    throw new BridgeError("reason_code must be a non-empty string of at most 128 characters", "invalid_policy", 400);
  }
  if (policy.expires_after_ms != null && (!Number.isSafeInteger(policy.expires_after_ms) || policy.expires_after_ms < 0)) {
    throw new BridgeError("expires_after_ms must be a non-negative safe integer", "invalid_policy", 400);
  }
  if (policy.id === DEFAULT_POLICY.id && (policy.enabled === false || stableJson(policy.match ?? {}) !== "{}")) {
    throw new BridgeError("default policy must be enabled and have an empty match", "invalid_policy", 400);
  }
  return withPolicyDefaults(policy);
}

function normalizeStoredPolicy(policy: InstalledPolicyRule): InstalledPolicyRule {
  if (!policy || typeof policy.id !== "string" || !POLICY_ID.test(policy.id)
    || !Number.isSafeInteger(policy.version) || policy.version < 1) {
    throw new BridgeError("stored policy id or version is invalid", "invalid_policy", 500);
  }
  const delivery = policy.delivery;
  if (!delivery || !["immediate", "debounce", "digest", "scheduled", "suppress"].includes(String(delivery.mode))) {
    throw new BridgeError(`stored policy has unsupported delivery mode: ${String(delivery?.mode)}`, "invalid_policy", 500);
  }
  if (delivery.mode === "scheduled" && (typeof delivery.scheduled_local_time !== "string"
    || !LOCAL_TIME.test(delivery.scheduled_local_time))) {
    throw new BridgeError("stored scheduled policy has invalid local time", "invalid_policy", 500);
  }
  const values = [delivery.debounce_ms, delivery.cadence_ms, delivery.max_delay_ms, policy.batch?.max_events, policy.batch?.window_ms];
  if (values.some((value) => value !== undefined && value !== null && (!Number.isFinite(value) || value < 0))) {
    throw new BridgeError("stored policy has invalid durations or batch limits", "invalid_policy", 500);
  }
  return withPolicyDefaults(policy);
}

/**
 * Defaults shared by newly installed and stored policies.  Legacy target fields
 * survive only on stored rows; normalizePolicy has already rejected them.
 */
function withPolicyDefaults<T extends InstalledPolicyRule>(policy: T): T {
  const delivery = policy.delivery;
  return {
    ...policy,
    enabled: policy.enabled !== false,
    order: policy.order ?? 0,
    match: policy.match ?? {},
    delivery: {
      ...delivery,
      quiet_hours_policy: delivery.quiet_hours_policy ?? "defer",
      foreground_presence_policy: delivery.foreground_presence_policy ?? "defer",
    },
    batch: {
      coalesce_by: policy.batch?.coalesce_by ?? "coalesce_key",
      max_events: policy.batch?.max_events ?? 20,
      window_ms: policy.batch?.window_ms ?? 0,
    },
    target: {
      attention_channel: policy.target?.attention_channel ?? "${attention_channel_hint}",
      ...(policy.target?.prefer_transport == null ? {} : { prefer_transport: policy.target.prefer_transport }),
      ...(policy.target?.busy_behavior == null ? {} : { busy_behavior: policy.target.busy_behavior }),
    },
    reason_code: policy.reason_code ?? policy.id,
  };
}

function currentInstalledPolicy(policy: InstalledPolicyRule): policy is PolicyRule {
  if (!["immediate", "scheduled", "suppress"].includes(policy.delivery.mode)) return false;
  try {
    normalizePolicy(policy as PolicyRule);
    return true;
  } catch {
    return false;
  }
}

/** Highest version of each policy id, in matching order (order desc, then id). */
function latestPolicyVersions<T extends InstalledPolicyRule>(policies: T[], normalize: (policy: T) => T): T[] {
  const latest = new Map<string, T>();
  for (const raw of policies) {
    const policy = normalize(raw);
    const current = latest.get(policy.id);
    if (!current || policy.version > current.version) latest.set(policy.id, policy);
  }
  return [...latest.values()].sort((a, b) => (b.order ?? 0) - (a.order ?? 0) || a.id.localeCompare(b.id));
}

function activeInstalledPolicyVersions(policies: InstalledPolicyRule[]): InstalledPolicyRule[] {
  return latestPolicyVersions(policies, normalizeStoredPolicy);
}

export function activePolicyVersions(policies: PolicyRule[]): PolicyRule[] {
  return latestPolicyVersions(policies, normalizePolicy);
}

function jsonRecord<T>(value: unknown, fallback: T): T {
  return parseJson<T>(value, fallback);
}


function isoOrThrow(value: string, field: string): string {
  if (typeof value !== "string" || !ABSOLUTE_RFC3339.test(value)) {
    throw new BridgeError(`${field} must be an absolute RFC3339 timestamp with Z or numeric offset`, "invalid_timestamp", 400);
  }
  const date = asDate(value);
  return date.toISOString();
}

function arrayWithoutDuplicates(values: string[]): string[] {
  return [...new Set(values)];
}

export class WakeBridge {
  readonly config: Required<Pick<BridgeConfig, "instance_id" | "owner_id" | "db_path">> & BridgeConfig;
  readonly db: SqliteDatabase;
  readonly transports = new Map<string, WakeTransport>();
  readonly mockTransport: MockTransport;
  private readonly clock: () => Date;

  constructor(config: BridgeConfig, options: WakeBridgeOptions = {}) {
    if (!config?.instance_id || !config.owner_id || !config.db_path) {
      throw new BridgeError("instance_id, owner_id and db_path are required", "invalid_config", 400);
    }
    const ackTimeoutMs = config.ack_timeout_ms ?? DEFAULT_ACK_TIMEOUT_MS;
    validateAckTimeout(ackTimeoutMs);
    const maxChannels = config.max_channels === undefined ? 32 : config.max_channels;
    if (!Number.isSafeInteger(maxChannels) || maxChannels < 1 || maxChannels > 1024) {
      throw new BridgeError("max_channels must be an integer between 1 and 1024", "invalid_config", 400);
    }
    if (config.incident_attention_channel != null && (typeof config.incident_attention_channel !== "string"
      || !config.incident_attention_channel || config.incident_attention_channel.length > 200)) {
      throw new BridgeError("incident_attention_channel must be a non-empty string of at most 200 characters", "invalid_config", 400);
    }
    const quietConfig = config.quiet_hours as unknown as Record<string, unknown> | null | undefined;
    if (quietConfig != null && (typeof quietConfig !== "object" || Array.isArray(quietConfig))) {
      throw new BridgeError("quiet_hours must be an object or null", "invalid_quiet_hours", 400);
    }
    const unsupportedQuiet = ["resume_spread_ms", "default_action"].filter((field) => quietConfig && Object.prototype.hasOwnProperty.call(quietConfig, field));
    if (unsupportedQuiet.length) {
      throw new BridgeError(`unsupported quiet_hours field(s): ${unsupportedQuiet.join(", ")}`, "unsupported_config_field", 400);
    }
    const timezone = config.timezone ?? config.quiet_hours?.timezone ?? "UTC";
    for (const candidate of new Set([timezone, config.quiet_hours?.timezone].filter((value) => value !== undefined))) {
      try {
        if (typeof candidate !== "string" || !candidate) throw new Error("invalid timezone");
        new Intl.DateTimeFormat("en", { timeZone: candidate }).format(new Date(0));
      } catch {
        throw new BridgeError(`timezone is invalid: ${candidate}`, "invalid_timezone", 400);
      }
    }
    if (config.quiet_hours && (!Array.isArray(config.quiet_hours.windows)
      || config.quiet_hours.windows.some((window) => !window || !LOCAL_TIME.test(window.start)
        || !LOCAL_TIME.test(window.end)))) {
      throw new BridgeError("quiet_hours.windows must contain HH:MM start/end values", "invalid_quiet_hours", 400);
    }
    if (config.quiet_hours?.windows.length) {
      const covered = new Array<boolean>(24 * 60).fill(false);
      for (const window of config.quiet_hours.windows) {
        const [startHour, startMinute] = window.start.split(":").map(Number);
        const [endHour, endMinute] = window.end.split(":").map(Number);
        const start = startHour * 60 + startMinute;
        const end = endHour * 60 + endMinute;
        for (let minute = 0; minute < covered.length; minute += 1) {
          if (start === end || (start < end ? minute >= start && minute < end : minute >= start || minute < end)) covered[minute] = true;
        }
      }
      if (covered.every(Boolean)) {
        throw new BridgeError("quiet_hours must leave at least one open local minute", "invalid_quiet_hours", 400);
      }
    }
    this.config = {
      ...config,
      timezone,
      max_channels: maxChannels,
      presence_ttl_ms: config.presence_ttl_ms ?? 20 * 60_000,
      activity_ttl_ms: config.activity_ttl_ms ?? 2 * 60_000,
      endpoint_lease_ms: config.endpoint_lease_ms ?? 60 * 60_000,
      dispatch_lease_ms: config.dispatch_lease_ms ?? 30_000,
      ack_timeout_ms: ackTimeoutMs,
      retry_delay_ms: config.retry_delay_ms ?? 5_000,
      batch_window_ms: config.batch_window_ms ?? 0,
    };
    this.clock = options.clock ?? (() => new Date());
    this.db = new SqliteDatabase(config.db_path, { allowSchemaUpgrade: options.allowSchemaUpgrade, instanceId: config.instance_id });
    this.mockTransport = new MockTransport();
    if (options.autoMockTransport !== false) this.registerTransport(this.mockTransport);
    for (const transport of options.transports ?? []) this.registerTransport(transport);
    this.installPolicies([...(config.policies ?? []), ...(config.default_policy ? [config.default_policy] : [])]);
    if (options.recoverDispatchLeases !== false) this.recover();
  }

  close(): void {
    this.db.close();
  }

  now(): Date {
    const date = new Date(this.clock().getTime());
    if (Number.isNaN(date.getTime())) throw new BridgeError("clock returned invalid date", "invalid_clock", 500);
    return date;
  }

  nowIso(): string {
    return isoNow(this.now());
  }

  private channelGuardSql(channels: string[], register = true, explicit = false): string {
    const requested = `SELECT value FROM json_each(${sqlJson(channels)})`;
    const scope = `instance_id=${sqlValue(this.config.instance_id)}`;
    return `CREATE TEMP TABLE channel_write_guard(status TEXT NOT NULL);
      INSERT INTO channel_write_guard SELECT CASE
        ${explicit ? `WHEN EXISTS(SELECT 1 FROM attention_channels WHERE ${scope} AND attention_channel IN (${requested})) THEN 'channel_exists'` : ""}
        WHEN EXISTS(SELECT 1 FROM attention_channels WHERE ${scope} AND attention_channel IN (${requested}) AND deleted_at IS NOT NULL) THEN 'channel_deleted'
        ${register
          ? `WHEN EXISTS(SELECT 1 FROM json_each(${sqlJson(channels)}) j WHERE NOT EXISTS
                (SELECT 1 FROM attention_channels c WHERE c.instance_id=${sqlValue(this.config.instance_id)} AND c.attention_channel=j.value))
              AND (SELECT count(*) FROM attention_channels WHERE ${scope} AND deleted_at IS NULL)
              + (SELECT count(*) FROM json_each(${sqlJson(channels)}) j WHERE NOT EXISTS
                (SELECT 1 FROM attention_channels c WHERE c.instance_id=${sqlValue(this.config.instance_id)} AND c.attention_channel=j.value))>${this.config.max_channels}
              THEN 'channel_limit_reached'`
          : `WHEN EXISTS(SELECT 1 FROM json_each(${sqlJson(channels)}) j WHERE NOT EXISTS
                (SELECT 1 FROM attention_channels c WHERE c.instance_id=${sqlValue(this.config.instance_id)} AND c.attention_channel=j.value)) THEN 'channel_not_found'`}
        ELSE 'ok' END;`;
  }

  private channelRegistrationSql(channels: string[], now: string, condition = CHANNEL_WRITE_ALLOWED, name?: string): string {
    return `INSERT INTO attention_channels(instance_id, attention_channel, name, created_at, updated_at, deleted_at)
      SELECT ${sqlValue(this.config.instance_id)}, j.value, ${name == null ? "j.value" : sqlValue(name)}, ${sqlValue(now)}, ${sqlValue(now)}, NULL
      FROM json_each(${sqlJson(channels)}) j WHERE ${condition} AND NOT EXISTS
        (SELECT 1 FROM attention_channels c WHERE c.instance_id=${sqlValue(this.config.instance_id)} AND c.attention_channel=j.value);`;
  }

  private checkChannelStatus(status: string): void {
    if (status === "ok") return;
    if (status === "channel_limit_reached") throw new BridgeError("active channel limit reached", status, 429);
    if (status === "channel_not_found") throw new BridgeError("attention channel is unknown", status, 404);
    if (status === "channel_exists") throw new BridgeError("attention channel ID already exists", status, 409);
    if (status === "channel_deleted") throw new BridgeError("attention channel was deleted", status, 409);
    if (status === "stale_generation") throw new BridgeError("operation targets a stale channel binding", status, 409);
    if (status === "endpoint_fenced") throw new BridgeError("endpoint lease is no longer current", status, 409);
  }

  /** The guard is connection-local; only this script can observe it, and every durable write is conditional on it. */
  private channelTransaction(channels: string[], now: string, statements: string[], register = true, allowSuppressedEvent = false): void {
    const row = this.db.query<{ status: string }>([
      "BEGIN IMMEDIATE;", this.channelGuardSql(channels, register),
      ...(register ? [this.channelRegistrationSql(channels, now)] : []),
      ...statements, "SELECT status FROM channel_write_guard;", "COMMIT;",
    ].join("\n"))[0];
    // Only event ingestion opts into a durable suppressed result. Explicit
    // registration and host writes still surface all channel guard failures.
    if (!allowSuppressedEvent || !["channel_deleted", "channel_limit_reached"].includes(row.status)) this.checkChannelStatus(row.status);
  }

  private assertChannelNotDeleted(channel: string): void {
    if (this.getChannel(channel)?.deleted_at) this.checkChannelStatus("channel_deleted");
  }

  private channelBindingFenceSql(channel: string, endpointId: string, generation: number, now: string, token?: string): string {
    return `UPDATE channel_write_guard SET status=CASE
      ${token == null ? "" : `WHEN NOT EXISTS(SELECT 1 FROM endpoints WHERE instance_id=${sqlValue(this.config.instance_id)}
        AND id=${sqlValue(endpointId)} AND lease_token_hash=${sqlValue(sha256(token))} AND lease_expires_at>${sqlValue(now)}) THEN 'endpoint_fenced'`}
      WHEN NOT EXISTS(SELECT 1 FROM bindings WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)}
        AND endpoint_id=${sqlValue(endpointId)} AND generation=${sqlValue(generation)}) THEN 'stale_generation'
      ELSE 'ok' END WHERE status='ok';`;
  }

  createChannel(input: { name: string; attention_channel?: string }): AttentionChannel {
    const name = channelName(input?.name);
    const channel = input.attention_channel ?? id("ch");
    validateChannelId(channel);
    if (channel !== channel.trim()) {
      throw new BridgeError("attention channel ID must not have leading or trailing whitespace", "invalid_channel", 400);
    }
    const now = this.nowIso();
    const row = this.db.query<DbRow>([
      "BEGIN IMMEDIATE;", this.channelGuardSql([channel], true, true),
      this.channelRegistrationSql([channel], now, CHANNEL_WRITE_ALLOWED, name),
      `SELECT g.status, c.* FROM channel_write_guard g LEFT JOIN attention_channels c
        ON c.instance_id=${sqlValue(this.config.instance_id)} AND c.attention_channel=${sqlValue(channel)};`, "COMMIT;",
    ].join("\n"))[0];
    this.checkChannelStatus(row.status);
    return this.channelFromRow(row);
  }

  listChannels(includeDeleted = false): AttentionChannel[] {
    return this.db.query<DbRow>(`SELECT * FROM attention_channels WHERE instance_id=${sqlValue(this.config.instance_id)}
      ${includeDeleted ? "" : "AND deleted_at IS NULL"} ORDER BY created_at, attention_channel;`).map((row) => this.channelFromRow(row));
  }

  getChannel(channel: string): AttentionChannel | null {
    const row = this.db.query<DbRow>(`SELECT * FROM attention_channels WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)};`)[0];
    return row ? this.channelFromRow(row) : null;
  }

  private channelFromRow(row: DbRow): AttentionChannel {
    return { instance_id: String(row.instance_id), attention_channel: String(row.attention_channel), name: String(row.name),
      created_at: String(row.created_at), updated_at: String(row.updated_at), deleted_at: row.deleted_at == null ? null : String(row.deleted_at) };
  }

  renameChannel(channel: string, name: string): AttentionChannel {
    validateChannelId(channel);
    const clean = channelName(name);
    this.channelTransaction([channel], this.nowIso(), [
      `UPDATE attention_channels SET name=${sqlValue(clean)}, updated_at=${sqlValue(this.nowIso())}
       WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)} AND ${CHANNEL_WRITE_ALLOWED};`,
    ], false);
    return this.getChannel(channel)!;
  }

  deleteChannel(channel: string, options: { force?: boolean } = {}): AttentionChannel {
    validateChannelId(channel);
    if (options.force !== undefined && typeof options.force !== "boolean") {
      throw new BridgeError("force must be boolean", "invalid_arguments", 400);
    }
    const now = this.nowIso();
    const scope = `instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)}`;
    const batchIds = `SELECT id FROM batches WHERE ${scope}`;
    const eventIds = `SELECT j.value FROM claims c, json_each(c.event_ids_json) j WHERE c.instance_id=${sqlValue(this.config.instance_id)}
      AND c.attention_channel=${sqlValue(channel)} AND c.state NOT IN (${FINALIZED_CLAIM_STATES.map(sqlValue).join(",")})`;
    const active = "(SELECT active FROM channel_delete_guard)";
    // Normal installs validate policy contents. Under this same writer lock,
    // select only the latest enabled, supported rules with literal targets.
    const row = this.db.query<DbRow>([
      "BEGIN IMMEDIATE;",
      `CREATE TEMP TABLE channel_delete_guard AS SELECT
        CASE WHEN EXISTS(SELECT 1 FROM attention_channels WHERE ${scope}) THEN 'ok' ELSE 'channel_not_found' END AS status,
        EXISTS(SELECT 1 FROM attention_channels WHERE ${scope} AND deleted_at IS NULL) AS active,
        (SELECT json_object('endpoint_id', endpoint_id, 'generation', generation) FROM bindings WHERE ${scope}) AS binding_json,
        EXISTS(SELECT 1 FROM activity_watches WHERE ${scope} AND enabled=1) AS activity_watch,
        (SELECT json_group_array(json_object('id', id, 'version', version)) FROM (
          SELECT p.id, p.version FROM policies p
          WHERE p.version=(SELECT max(latest.version) FROM policies latest WHERE latest.id=p.id)
            AND json_extract(p.rule_json, '$.enabled') IS NOT 0
            AND json_extract(p.rule_json, '$.target.attention_channel')=${sqlValue(channel)}
            AND instr(json_extract(p.rule_json, '$.target.attention_channel'), ${sqlValue("${")})=0
            AND json_extract(p.rule_json, '$.delivery.mode') IN ('immediate','scheduled','suppress')
            AND NOT EXISTS(SELECT 1 FROM json_each(p.rule_json, '$.delivery')
              WHERE key IN ('debounce_ms','cadence_ms','max_delay_ms'))
            AND NOT EXISTS(SELECT 1 FROM json_each(p.rule_json, '$.target')
              WHERE key IN ('prefer_transport','busy_behavior') AND value IS NOT NULL)
          ORDER BY p.id
        )) AS policies_json;`,
      `UPDATE channel_delete_guard SET status='channel_in_use', active=0
        WHERE active AND ${options.force === true ? 0 : 1} AND (binding_json IS NOT NULL OR activity_watch OR json_array_length(policies_json)>0);`,
      `UPDATE attention_channels SET deleted_at=${sqlValue(now)}, updated_at=${sqlValue(now)} WHERE ${scope} AND ${active};`,
      `DELETE FROM bindings WHERE ${scope} AND ${active};`,
      `DELETE FROM presence_leases WHERE ${scope} AND ${active};`,
      `UPDATE activity_watches SET enabled=0, endpoint_id=NULL, binding_generation=NULL, epoch=epoch+1, armed_at=NULL,
        last_nonwake_activity_at=NULL, activity_lease_expires_at=NULL, fired_epoch=NULL, next_due_at=NULL, updated_at=${sqlValue(now)}
        WHERE ${scope} AND ${active};`,
      `UPDATE delivery_correlations SET state='discarded' WHERE batch_id IN (${batchIds}) AND state='outstanding' AND ${active};`,
      `UPDATE outbox_attempts SET state='failed', finished_at=${sqlValue(now)}, error_class='channel_deleted', error_message=NULL
        WHERE batch_id IN (${batchIds}) AND state='leased' AND ${active};`,
      `INSERT INTO event_transitions(event_id, from_state, to_state, reason, at)
        SELECT event_id, state, 'expired', 'channel_deleted', ${sqlValue(now)} FROM event_status
        WHERE event_id IN (${eventIds}) AND state IN ('received','matched','batched') AND ${active};`,
      `UPDATE event_status SET state='expired', updated_at=${sqlValue(now)}
        WHERE event_id IN (${eventIds}) AND state IN ('received','matched','batched') AND ${active};`,
      `INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at)
        SELECT id, state, 'dismissed', 'channel_deleted', ${sqlValue(now)} FROM claims
        WHERE ${scope} AND state NOT IN (${FINALIZED_CLAIM_STATES.map(sqlValue).join(",")}) AND ${active};`,
      `UPDATE claims SET state='dismissed', dismissed_reason='channel_deleted', updated_at=${sqlValue(now)}
        WHERE ${scope} AND state NOT IN (${FINALIZED_CLAIM_STATES.map(sqlValue).join(",")}) AND ${active};`,
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
        SELECT id, state, 'cancelled', 'channel_deleted', ${sqlValue(now)} FROM batches WHERE ${scope} AND state NOT IN ('seen','cancelled') AND ${active};`,
      `UPDATE batches SET state='cancelled', lease_expires_at=NULL, last_error='channel_deleted', updated_at=${sqlValue(now)}
        WHERE ${scope} AND state NOT IN ('seen','cancelled') AND ${active};`,
      `SELECT g.status, g.binding_json, g.activity_watch, g.policies_json, c.* FROM channel_delete_guard g LEFT JOIN attention_channels c
        ON c.instance_id=${sqlValue(this.config.instance_id)} AND c.attention_channel=${sqlValue(channel)};`, "COMMIT;",
    ].join("\n"))[0];
    if (row.status === "channel_in_use") throw new ChannelInUseError({
      binding: jsonRecord<ChannelUsage["binding"]>(row.binding_json, null),
      activity_watch: Boolean(row.activity_watch),
      policies: jsonRecord<ChannelUsage["policies"]>(row.policies_json, []),
    });
    this.checkChannelStatus(row.status);
    return this.channelFromRow(row);
  }

  /** Create one policy-routed, deduplicated operator incident when explicitly configured. */
  reportCoreIncident(input: {
    kind: "ack_timeout" | "source_needs_attention";
    identity: Record<string, JsonValue>;
    resource: ResourceRef;
  }): EmitResult | null {
    const channel = this.config.incident_attention_channel;
    if (!channel) return null;
    const fingerprint = sha256(stableJson({ kind: input.kind, identity: input.identity }));
    return this.emitEvent("wakebridge.core", {
      type: "core.incident",
      occurred_at: this.nowIso(),
      dedupe_key: `incident:${fingerprint}`,
      coalesce_key: `incident:${input.kind}`,
      priority_hint: "high",
      attention_channel_hint: channel,
      actor_ref: "wakebridge:core",
      resource: input.resource,
      metadata: { origin: "core_incident", reason_code: input.kind, ...input.identity },
      payload_preview: null,
    });
  }

  registerTransport(transport: WakeTransport): void {
    if (!transport?.kind) throw new BridgeError("transport.kind is required", "invalid_transport", 400);
    this.transports.set(transport.kind, transport);
  }

  unregisterTransport(kind: string, expected?: WakeTransport): void {
    if (expected && this.transports.get(kind) !== expected) return;
    this.transports.delete(kind);
  }

  listPolicies(): InstalledPolicyRule[] {
    return this.db.query<DbRow>("SELECT rule_json FROM policies ORDER BY order_no DESC, id ASC, version DESC;")
      .map((row) => jsonRecord<InstalledPolicyRule>(row.rule_json, DEFAULT_POLICY));
  }

  listActivePolicies(): InstalledPolicyRule[] {
    return activeInstalledPolicyVersions(this.listPolicies());
  }

  listPolicyStatuses(): PolicyVersionStatus[] {
    const policies = this.listPolicies();
    const active = new Map(this.listActivePolicies().map((policy) => [policy.id, policy.version]));
    return policies.map((policy) => {
      const isActive = active.get(policy.id) === policy.version;
      return {
        id: policy.id,
        version: policy.version,
        active: isActive,
        enabled: policy.enabled !== false,
        new_event_matching: !isActive ? "historical"
          : policy.enabled === false ? "disabled"
          : currentInstalledPolicy(policy) ? "eligible"
          : "legacy_unsupported",
      };
    });
  }

  installPolicies(policies: PolicyRule[]): void {
    const additions = this.policyAdditions(policies, this.listPolicies());
    if (additions.length) {
      const installedAt = this.nowIso();
      this.db.transaction(additions.map((policy) => `INSERT INTO policies(id, version, enabled, order_no, rule_json, installed_at)
        VALUES(${sqlValue(policy.id)}, ${policy.version}, ${policy.enabled === false ? 0 : 1}, ${Number(policy.order ?? 0)}, ${sqlJson(policy)}, ${sqlValue(installedAt)});`));
    }
    this.ensureDefaultPolicy();
  }

  // Installation and its read-only preview share immutable-version checks.
  private policyAdditions(policies: PolicyRule[], installed: InstalledPolicyRule[]): PolicyRule[] {
    const all = policies.map(normalizePolicy);
    const requested = new Map<string, PolicyRule>();
    for (const policy of all) {
      const key = `${policy.id}\u0000${policy.version}`;
      const duplicate = requested.get(key);
      if (duplicate && stableJson(duplicate) !== stableJson(policy)) {
        throw new BridgeError(`policy ${policy.id}@${policy.version} appears more than once with different contents`, "policy_version_conflict", 409);
      }
      requested.set(key, policy);
    }

    const installedByVersion = new Map(installed.map((policy) => [`${policy.id}\u0000${policy.version}`, normalizeStoredPolicy(policy)]));
    const maxVersion = new Map<string, number>();
    for (const policy of installed) maxVersion.set(policy.id, Math.max(maxVersion.get(policy.id) ?? 0, policy.version));

    const additions: PolicyRule[] = [];
    for (const policy of [...requested.values()].sort((a, b) => a.id.localeCompare(b.id) || a.version - b.version)) {
      const key = `${policy.id}\u0000${policy.version}`;
      const existing = installedByVersion.get(key);
      if (existing) {
        if (stableJson(existing) !== stableJson(policy)) {
          throw new BridgeError(`policy ${policy.id}@${policy.version} is immutable and already has different contents`, "policy_version_conflict", 409);
        }
        continue;
      }
      const highest = maxVersion.get(policy.id) ?? 0;
      if (policy.version <= highest) {
        throw new BridgeError(`policy ${policy.id}@${policy.version} is older than installed version ${highest}`, "policy_version_regression", 409);
      }
      additions.push(policy);
      maxVersion.set(policy.id, policy.version);
    }

    return additions;
  }

  private ensureDefaultPolicy(): void {
    if (this.listPolicies().some((policy) => policy.id === DEFAULT_POLICY.id)) return;
    const policy = normalizePolicy(DEFAULT_POLICY);
    this.db.exec(`INSERT INTO policies(id, version, enabled, order_no, rule_json, installed_at)
      VALUES(${sqlValue(policy.id)}, ${policy.version}, 1, ${Number(policy.order ?? 0)}, ${sqlJson(policy)}, ${sqlValue(this.nowIso())});`);
  }

  updatePolicy(policy: PolicyRule): PolicyRule {
    const normalized = normalizePolicy(policy);
    this.installPolicies([normalized]);
    return normalized;
  }

  private policyRows(): PolicyRule[] {
    return this.listActivePolicies().filter((policy) => policy.enabled !== false).filter(currentInstalledPolicy);
  }

  private policyForEvent(source: string, input: WakeEventInput, candidates?: PolicyRule[]): PolicyRule {
    const event = { ...input, source } as Record<string, unknown>;
    const installed = candidates ? this.listPolicies() : null;
    const rows = installed && candidates
      ? activeInstalledPolicyVersions([...installed, ...this.policyAdditions(candidates, installed)])
        .filter((policy) => policy.enabled !== false).filter(currentInstalledPolicy)
      : this.policyRows();
    for (const policy of rows) {
      if (this.matches(policy.match ?? {}, event)) return policy;
    }
    // A legacy default row may remain append-only but cannot match new events.
    // The clean built-in default preserves current behavior without rewriting
    // that history; it also guards a manually edited database.
    return normalizePolicy(DEFAULT_POLICY);
  }

  testPolicy(source: string, input: WakeEventInput, candidates?: PolicyRule[]): PolicyMatchResult {
    const clean = this.validateEvent(source, input);
    const matched = this.policyForEvent(source, clean, candidates);
    return { source, type: clean.type, matched_policy: matched };
  }

  previewPolicy(source: string, input: WakeEventInput, candidates?: PolicyRule[]): PolicyPreviewResult {
    const clean = this.validateEvent(source, input);
    const policy = this.policyForEvent(source, clean, candidates);
    const nowDate = this.now();
    const now = nowDate.toISOString();
    let suppressed = policy.delivery.mode === "suppress";
    const channel = suppressed ? null : this.targetChannel(policy, clean);
    if (channel) {
      const status = this.db.query<{ status: string }>(`${this.channelGuardSql([channel])}\nSELECT status FROM channel_write_guard;`)[0].status;
      if (status === "channel_deleted" || status === "channel_limit_reached") suppressed = true;
      else this.checkChannelStatus(status);
    }
    const quietActive = Boolean(quietWindowAt(nowDate, this.config.quiet_hours, this.config.timezone ?? "UTC"));
    const quietAction = policy.delivery.quiet_hours_policy ?? "defer";
    const quietUntil = quietActive && quietAction === "defer"
      ? quietEndAfter(nowDate, this.config.quiet_hours, this.config.timezone ?? "UTC").toISOString()
      : null;
    const presence = channel ? this.presenceActive(channel, now) : null;
    const presenceAction = policy.delivery.foreground_presence_policy ?? "defer";
    const coalesceBy = policy.batch?.coalesce_by ?? "coalesce_key";
    let coalesceKey: string | null = null;
    if (!suppressed && coalesceBy !== "none") {
      if (coalesceBy === "type") coalesceKey = clean.type;
      else if (coalesceBy === "source") coalesceKey = source;
      else if (coalesceBy.startsWith("metadata.")) coalesceKey = String(getPath(clean.metadata, coalesceBy.slice("metadata.".length)) ?? "");
      else coalesceKey = clean.coalesce_key ?? null;
    }
    return {
      source,
      type: clean.type,
      evaluated_at: now,
      matched_policy: policy,
      suppressed,
      claim: suppressed || !channel ? null : {
        attention_channel: channel,
        eligible_after: this.claimEligibleAfter(policy, nowDate),
        expires_at: this.claimExpiresAt(policy, nowDate),
        reason_code: policy.reason_code ?? policy.id,
        defer_while_presence: presenceAction !== "bypass",
      },
      gates: {
        quiet_hours: { action: quietAction, active: quietActive, defer_until: quietUntil },
        foreground_presence: {
          action: presenceAction,
          active: Boolean(presence),
          defer_until: presence && presenceAction === "defer" ? presence.expires_at : null,
        },
      },
      batch: suppressed ? null : {
        coalesce_by: coalesceBy,
        coalesce_key: coalesceKey,
        max_events: policy.batch?.max_events ?? 20,
        window_ms: policy.batch?.window_ms ?? 0,
      },
    };
  }

  private matches(selector: Record<string, unknown>, event: Record<string, unknown>): boolean {
    for (const [key, expected] of Object.entries(selector)) {
      if (key === "metadata" && expected && typeof expected === "object") {
        for (const [metadataKey, metadataExpected] of Object.entries(expected as Record<string, unknown>)) {
          if (stableJson(getPath(event.metadata, metadataKey)) !== stableJson(metadataExpected)) return false;
        }
        continue;
      }
      const actual = key.startsWith("metadata.") ? getPath(event.metadata, key.slice("metadata.".length)) : getPath(event, key);
      if (stableJson(actual) !== stableJson(expected)) return false;
    }
    return true;
  }

  private targetChannel(policy: PolicyRule, input: WakeEventInput): string {
    const configured = policy.target?.attention_channel ?? "${attention_channel_hint}";
    const channel = configured.replaceAll("${attention_channel_hint}", input.attention_channel_hint || "default");
    validateChannelId(channel);
    return channel;
  }

  private claimEligibleAfter(policy: PolicyRule, now: Date): string {
    const mode = policy.delivery.mode;
    if (mode === "scheduled") return nextLocalClock(now, policy.delivery.scheduled_local_time!, this.config.timezone ?? "UTC").toISOString();
    return now.toISOString();
  }

  private claimExpiresAt(policy: PolicyRule, now: Date): string | null {
    return policy.expires_after_ms == null ? null : addMs(now, assertFiniteMs(policy.expires_after_ms, "expires_after_ms", 0));
  }

  private validateEvent(source: string, raw: WakeEventInput): WakeEventInput {
    if (!source || !SOURCE_ID.test(source)) throw new BridgeError("source identity is invalid", "invalid_source", 400);
    try {
      return validateWakeEventInput(raw, this.now());
    } catch (error) {
      if (error instanceof WakeBridgeSdkError) throw new BridgeError(error.message, error.code, error.status);
      throw error;
    }
  }

  private duplicateEmitResult(source: string, dedupeKey: string, eventId?: string): EmitResult | null {
    const row = (eventId
      ? this.db.query<DbRow>(`SELECT id FROM events WHERE instance_id=${sqlValue(this.config.instance_id)} AND id=${sqlValue(eventId)} LIMIT 1;`)[0]
      : undefined)
      || this.db.query<DbRow>(`SELECT id FROM events WHERE instance_id=${sqlValue(this.config.instance_id)} AND source=${sqlValue(source)} AND dedupe_key=${sqlValue(dedupeKey)} LIMIT 1;`)[0];
    if (!row) return null;
    const event = this.getEvent(String(row.id));
    if (!event) throw new BridgeError("dedupe record points to missing event", "storage_corrupt", 500);
    const claim = this.findClaimByEventId(event.id);
    return { event, claim, duplicate: true, suppressed: event.state === "suppressed" };
  }

  private isUniqueConflict(error: unknown): boolean {
    return /UNIQUE constraint failed/i.test(error instanceof Error ? error.message : String(error));
  }

  emitEvent(source: string, input: WakeEventInput): EmitResult {
    const clean = this.validateEvent(source, input);
    const idempotencyRow = clean.idempotency_key
      ? this.db.query<DbRow>(`SELECT result_json FROM idempotency_keys WHERE scope=${sqlValue(`event:${source}`)} AND key=${sqlValue(clean.idempotency_key)} LIMIT 1;`)[0]
      : undefined;
    const idempotentEventId = idempotencyRow ? jsonRecord<{ event_id?: string }>(idempotencyRow.result_json, {}).event_id : undefined;
    const deterministicEventId = clean.idempotency_key ? `evt_${sha256(`event:${this.config.instance_id}:${source}:${clean.idempotency_key}`).slice(0, 32)}` : undefined;
    const existing = this.duplicateEmitResult(source, clean.dedupe_key, idempotentEventId || deterministicEventId);
    if (existing) return existing;
    const now = this.nowIso();
    const policy = this.policyForEvent(source, clean);
    const eventId = deterministicEventId || id("evt");
    const eventState: EventState = policy.delivery.mode === "suppress" ? "suppressed" : "matched";
    const claimId = eventState === "matched" ? id("ac") : null;
    const channel = claimId ? this.targetChannel(policy, clean) : null;
    const eventAllowed = "(SELECT status IN ('ok','channel_deleted','channel_limit_reached') FROM channel_write_guard)";
    const eventStateSql = channel ? `CASE WHEN ${CHANNEL_WRITE_ALLOWED} THEN 'matched' ELSE 'suppressed' END` : "'suppressed'";
    const claimState: ClaimState = "pending";
    let mergedClaim: { id: string } | null = null;
    if (claimId && channel) {
      let resourceJson: string | null = stableJson(clean.resource);
      try { JSON.parse(resourceJson); } catch { resourceJson = null; }
      const mergeSemantics = stableJson({ delivery: policy.delivery, batch: policy.batch, expires_after_ms: policy.expires_after_ms ?? null });
      const mergePolicies = this.listPolicies().map(normalizeStoredPolicy).filter((candidate) =>
        stableJson({ delivery: candidate.delivery, batch: candidate.batch, expires_after_ms: candidate.expires_after_ms ?? null }) === mergeSemantics)
        .map((candidate) => [candidate.id, candidate.version]);
      // Compare JSON structure in SQLite before returning a candidate. Text
      // equality would split resources merely because object keys were reordered;
      // fullkey keeps array order, and kind keeps true/1 and null/missing distinct.
      // Optional undefined properties make stableJson non-JSON: no stored JSON
      // resource can match that canonical string, but ingestion still succeeds.
      if (resourceJson !== null) mergedClaim = this.db.query<{ id: string }>(`WITH merge_policies AS (
          SELECT json_extract(value, '$[0]') AS id, json_extract(value, '$[1]') AS version FROM json_each(${sqlJson(mergePolicies)})
        ), resource_tree AS (
          SELECT fullkey, CASE WHEN type IN ('integer','real') THEN 'number' ELSE type END AS kind, atom
          FROM json_tree(${sqlValue(resourceJson)})
        )
        SELECT c.id FROM claims c JOIN merge_policies p ON p.id=c.policy_id AND p.version=c.policy_version
        JOIN events e ON e.id=json_extract(c.event_ids_json, '$[0]') AND e.instance_id=c.instance_id
        JOIN event_status s ON s.event_id=e.id
        WHERE c.instance_id=${sqlValue(this.config.instance_id)} AND c.attention_channel=${sqlValue(channel)}
          AND c.state IN (${ACTIVE_CLAIM_STATES.map(sqlValue).join(",")}) AND e.source=${sqlValue(source)}
          AND json_extract(c.resource_json, '$.uri')=${sqlValue(clean.resource.uri)}
          AND NOT EXISTS (
            SELECT fullkey, CASE WHEN type IN ('integer','real') THEN 'number' ELSE type END, atom FROM json_tree(c.resource_json)
            EXCEPT SELECT fullkey, kind, atom FROM resource_tree
          ) AND NOT EXISTS (
            SELECT fullkey, kind, atom FROM resource_tree
            EXCEPT SELECT fullkey, CASE WHEN type IN ('integer','real') THEN 'number' ELSE type END, atom FROM json_tree(c.resource_json)
          )
        ORDER BY c.created_at ASC, c.rowid ASC LIMIT 1;`)[0] ?? null;
    }
    const statements = [
      `INSERT INTO events(id, instance_id, owner_id, source, type, schema_version, occurred_at, received_at, dedupe_key, coalesce_key, priority_hint, attention_channel_hint, actor_ref, resource_json, metadata_json, payload_preview, matched_policy_id, matched_policy_version)
       SELECT ${sqlValue(eventId)}, ${sqlValue(this.config.instance_id)}, ${sqlValue(this.config.owner_id)}, ${sqlValue(source)}, ${sqlValue(clean.type)}, ${Number(clean.schema_version ?? 1)}, ${sqlValue(clean.occurred_at)}, ${sqlValue(now)}, ${sqlValue(clean.dedupe_key)}, ${sqlValue(clean.coalesce_key)}, ${sqlValue(clean.priority_hint)}, ${sqlValue(clean.attention_channel_hint)}, ${sqlValue(clean.actor_ref)}, ${sqlJson(clean.resource)}, ${sqlJson(clean.metadata ?? {})}, ${sqlValue(clean.payload_preview)}, ${sqlValue(policy.id)}, ${policy.version} WHERE ${eventAllowed};`,
      `INSERT INTO event_status(event_id, state, updated_at) SELECT ${sqlValue(eventId)}, 'received', ${sqlValue(now)} WHERE ${eventAllowed};`,
      `INSERT INTO event_transitions(event_id, from_state, to_state, reason, at) SELECT ${sqlValue(eventId)}, NULL, 'received', 'ingest', ${sqlValue(now)} WHERE ${eventAllowed};`,
      `UPDATE event_status SET state=${eventStateSql}, updated_at=${sqlValue(now)} WHERE event_id=${sqlValue(eventId)} AND ${eventAllowed};`,
      `INSERT INTO event_transitions(event_id, from_state, to_state, reason, at) SELECT ${sqlValue(eventId)}, 'received', ${eventStateSql},
        CASE WHEN NOT ${CHANNEL_WRITE_ALLOWED} THEN (SELECT status FROM channel_write_guard) ELSE ${sqlValue(policy.delivery.mode)} END, ${sqlValue(now)} WHERE ${eventAllowed};`,
    ];
    if (claimId && channel) {
      if (mergedClaim) {
        // Append to the durable array, not the candidate snapshot: another
        // source worker may have merged since the pre-read. Only unfrozen
        // claims can accept new events, even if an active gate state changed.
        statements.push(`UPDATE claims SET event_ids_json=json_insert(event_ids_json, '$[#]', ${sqlValue(eventId)}), updated_at=${sqlValue(now)} WHERE id=${sqlValue(mergedClaim.id)} AND state IN (${ACTIVE_CLAIM_STATES.map(sqlValue).join(",")}) AND ${CHANNEL_WRITE_ALLOWED};`);
        statements.push(`INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at) SELECT id, state, state, 'merge_attention_duplicate', ${sqlValue(now)} FROM claims WHERE id=${sqlValue(mergedClaim.id)} AND changes()>0;`);
      }
      const eligibleAfter = this.claimEligibleAfter(policy, this.now());
      const deferPresence = policy.delivery.foreground_presence_policy !== "bypass";
      const expiresAt = this.claimExpiresAt(policy, this.now());
      // A concurrently frozen/finalized candidate cannot own this event.
      // Create its replacement in the same transaction as ingestion so a
      // matched event is never left without an attention claim.
      const fallback = mergedClaim
        ? ` WHERE ${CHANNEL_WRITE_ALLOWED} AND NOT EXISTS (SELECT 1 FROM claims c, json_each(c.event_ids_json) j WHERE c.id=${sqlValue(mergedClaim.id)} AND j.value=${sqlValue(eventId)})`
        : ` WHERE ${CHANNEL_WRITE_ALLOWED}`;
      statements.push(
        `INSERT INTO claims(id, instance_id, origin, event_ids_json, resource_json, policy_id, policy_version, attention_channel, eligible_after, expires_at, defer_while_presence, state, reason_code, note, created_at, updated_at, snooze_until, consumed_result_json, dismissed_reason)
         SELECT ${sqlValue(claimId)}, ${sqlValue(this.config.instance_id)}, ${sqlValue("policy")}, ${sqlJson([eventId])}, ${sqlJson(clean.resource)}, ${sqlValue(policy.id)}, ${policy.version}, ${sqlValue(channel)}, ${sqlValue(eligibleAfter)}, ${sqlValue(expiresAt)}, ${deferPresence ? 1 : 0}, ${sqlValue(claimState)}, ${sqlValue(policy.reason_code ?? policy.id)}, NULL, ${sqlValue(now)}, ${sqlValue(now)}, NULL, NULL, NULL${fallback};`,
      );
      statements.push(`INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at) SELECT ${sqlValue(claimId)}, NULL, ${sqlValue(claimState)}, ${sqlValue("policy")}, ${sqlValue(now)} WHERE changes()>0;`);
    }
    if (clean.idempotency_key) {
      statements.push(`INSERT OR IGNORE INTO idempotency_keys(scope, key, result_json, created_at) SELECT ${sqlValue(`event:${source}`)}, ${sqlValue(clean.idempotency_key)}, ${sqlJson({ event_id: eventId })}, ${sqlValue(now)} WHERE ${eventAllowed};`);
    }
    try {
      this.channelTransaction(channel ? [channel] : [], now, statements, true, true);
    } catch (error) {
      // Two source workers may pass the pre-read concurrently.  The unique
      // event key is the arbiter; return its durable result instead of
      // surfacing a raw sqlite constraint error.
      if (this.isUniqueConflict(error)) {
        const duplicate = this.duplicateEmitResult(source, clean.dedupe_key, deterministicEventId);
        if (duplicate) return duplicate;
      }
      throw error;
    }
    const event = this.getEvent(eventId)!;
    const claim = mergedClaim ? this.findClaimByEventId(eventId) : (claimId ? this.getClaim(claimId) ?? undefined : undefined);
    return { event, claim, duplicate: false, suppressed: event.state === "suppressed" };
  }

  emit(source: string, input: WakeEventInput): EmitResult {
    return this.emitEvent(source, input);
  }

  scheduleClaim(input: ScheduleClaimInput): ScheduleClaimResult {
    validateResource(input.resource);
    const channel = input.attention_channel || "default";
    validateChannelId(channel);
    this.assertChannelNotDeleted(channel);
    const eligibleAfter = isoOrThrow(input.eligible_after, "eligible_after");
    const expiresAt = input.expires_at == null ? null : isoOrThrow(input.expires_at, "expires_at");
    const key = input.idempotency_key || `resource:${sha256(stableJson({ resource: input.resource, eligibleAfter, channel: input.attention_channel || "default", reason: input.reason_code || "reconsider" }))}`;
    const source = "self_commitment";
    const dedupeKey = `schedule:${key}`;
    const existing = this.db.query<DbRow>(`SELECT id FROM events WHERE instance_id=${sqlValue(this.config.instance_id)} AND source=${sqlValue(source)} AND dedupe_key=${sqlValue(dedupeKey)} LIMIT 1;`)[0];
    if (existing) {
      const event = this.getEvent(String(existing.id))!;
      const claim = this.findClaimByEventId(event.id);
      if (!claim) {
        // Retention (pruneHistory) removes finished claims but keeps their events as the dedupe record.
        if (event.state === "consumed" || event.state === "expired") {
          throw new BridgeError("this idempotency key belongs to a finished self commitment whose history was pruned; use a new key", "claim_pruned", 409);
        }
        throw new BridgeError("self commitment event has no claim", "storage_corrupt", 500);
      }
      this.assertChannelNotDeleted(claim.attention_channel);
      return { event, claim, duplicate: true, warnings: this.scheduleWarnings(claim.attention_channel) };
    }
    const now = this.nowIso();
    const eventId = `evt_${sha256(`schedule:${this.config.instance_id}:${key}`).slice(0, 32)}`;
    const claimId = `ac_${sha256(`schedule-claim:${this.config.instance_id}:${key}`).slice(0, 32)}`;
    const metadata = { origin: "self_commitment", reason_code: input.reason_code || "reconsider" };
    const statements = [
      `INSERT INTO events(id, instance_id, owner_id, source, type, schema_version, occurred_at, received_at, dedupe_key, coalesce_key, priority_hint, attention_channel_hint, actor_ref, resource_json, metadata_json, payload_preview, matched_policy_id, matched_policy_version)
       SELECT ${sqlValue(eventId)}, ${sqlValue(this.config.instance_id)}, ${sqlValue(this.config.owner_id)}, ${sqlValue(source)}, ${sqlValue("attention.schedule")}, 1, ${sqlValue(now)}, ${sqlValue(now)}, ${sqlValue(dedupeKey)}, ${sqlValue(channel)}, ${sqlValue("normal")}, ${sqlValue(channel)}, ${sqlValue(this.config.owner_id)}, ${sqlJson(input.resource)}, ${sqlJson(metadata)}, NULL, ${sqlValue("self_commitment")}, 1 WHERE ${CHANNEL_WRITE_ALLOWED};`,
      `INSERT INTO event_status(event_id, state, updated_at) SELECT ${sqlValue(eventId)}, 'matched', ${sqlValue(now)} WHERE ${CHANNEL_WRITE_ALLOWED};`,
      `INSERT INTO event_transitions(event_id, from_state, to_state, reason, at) SELECT ${sqlValue(eventId)}, NULL, 'matched', 'self_commitment', ${sqlValue(now)} WHERE ${CHANNEL_WRITE_ALLOWED};`,
      `INSERT INTO claims(id, instance_id, origin, event_ids_json, resource_json, policy_id, policy_version, attention_channel, eligible_after, expires_at, defer_while_presence, state, reason_code, note, created_at, updated_at, snooze_until, consumed_result_json, dismissed_reason)
       SELECT ${sqlValue(claimId)}, ${sqlValue(this.config.instance_id)}, ${sqlValue("self_commitment")}, ${sqlJson([eventId])}, ${sqlJson(input.resource)}, ${sqlValue("self_commitment")}, 1, ${sqlValue(channel)}, ${sqlValue(eligibleAfter)}, ${sqlValue(expiresAt)}, ${sqlValue(input.defer_while_presence !== false)}, 'pending', ${sqlValue(input.reason_code || "reconsider")}, ${sqlValue(input.note)}, ${sqlValue(now)}, ${sqlValue(now)}, NULL, NULL, NULL WHERE ${CHANNEL_WRITE_ALLOWED};`,
      `INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at) SELECT ${sqlValue(claimId)}, NULL, 'pending', 'self_commitment', ${sqlValue(now)} WHERE ${CHANNEL_WRITE_ALLOWED};`,
      `INSERT OR IGNORE INTO idempotency_keys(scope, key, result_json, created_at) SELECT ${sqlValue("claim.schedule")}, ${sqlValue(key)}, ${sqlJson({ event_id: eventId, claim_id: claimId })}, ${sqlValue(now)} WHERE ${CHANNEL_WRITE_ALLOWED};`,
    ];
    try {
      this.channelTransaction([channel], now, statements);
    } catch (error) {
      if (this.isUniqueConflict(error)) {
        const duplicateEvent = this.db.query<DbRow>(`SELECT id FROM events WHERE instance_id=${sqlValue(this.config.instance_id)} AND source=${sqlValue(source)} AND dedupe_key=${sqlValue(dedupeKey)} LIMIT 1;`)[0];
        if (duplicateEvent) {
          const event = this.getEvent(String(duplicateEvent.id))!;
          const claim = this.findClaimByEventId(event.id);
          if (claim) return { event, claim, duplicate: true, warnings: this.scheduleWarnings(claim.attention_channel) };
        }
      }
      throw error;
    }
    return { event: this.getEvent(eventId)!, claim: this.getClaim(claimId)!, duplicate: false, warnings: this.scheduleWarnings(channel) };
  }

  private scheduleWarnings(channel: string): ScheduleClaimWarning[] {
    return this.resolveBinding(channel, this.nowIso()) ? [] : [{ code: "no_current_binding", attention_channel: channel }];
  }

  getEvent(eventId: string): WakeEvent | null {
    const row = this.db.query<DbRow>(`SELECT e.*, s.state FROM events e JOIN event_status s ON s.event_id=e.id WHERE e.id=${sqlValue(eventId)} AND e.instance_id=${sqlValue(this.config.instance_id)} LIMIT 1;`)[0];
    return row ? this.eventFromRow(row) : null;
  }

  /**
   * Oldest-first by default, which pages through history from the beginning.  Pass
   * `newest_first` to take the most recent `limit` rows instead — still returned in
   * chronological order — so a capped read of a growing table shows current activity
   * rather than the first page ever written.
   */
  listEvents(filters: { state?: EventState; source?: string; after?: string; limit?: number; newest_first?: boolean } = {}): WakeEvent[] {
    const clauses = [`e.instance_id=${sqlValue(this.config.instance_id)}`];
    if (filters.state) clauses.push(`s.state=${sqlValue(filters.state)}`);
    if (filters.source) clauses.push(`e.source=${sqlValue(filters.source)}`);
    if (filters.after) clauses.push(`e.received_at>${sqlValue(filters.after)}`);
    const limit = Math.min(100_000, Math.max(1, Math.floor(filters.limit ?? 100)));
    const order = filters.newest_first ? "DESC" : "ASC";
    const events = this.db.query<DbRow>(`SELECT e.*, s.state FROM events e JOIN event_status s ON s.event_id=e.id WHERE ${clauses.join(" AND ")} ORDER BY e.received_at ${order} LIMIT ${limit};`).map((row) => this.eventFromRow(row));
    return filters.newest_first ? events.reverse() : events;
  }

  private eventFromRow(row: DbRow): WakeEvent {
    return {
      id: String(row.id),
      instance_id: String(row.instance_id),
      owner_id: String(row.owner_id),
      source: String(row.source),
      type: String(row.type),
      schema_version: Number(row.schema_version),
      occurred_at: String(row.occurred_at),
      received_at: String(row.received_at),
      dedupe_key: String(row.dedupe_key),
      coalesce_key: row.coalesce_key == null ? null : String(row.coalesce_key),
      priority_hint: row.priority_hint == null ? null : String(row.priority_hint),
      attention_channel_hint: row.attention_channel_hint == null ? null : String(row.attention_channel_hint),
      actor_ref: row.actor_ref == null ? null : String(row.actor_ref),
      resource: jsonRecord<ResourceRef>(row.resource_json, { uri: "unknown:" }),
      metadata: jsonRecord<Record<string, JsonValue>>(row.metadata_json, {}),
      payload_preview: row.payload_preview == null ? null : String(row.payload_preview),
      idempotency_key: null,
      state: String(row.state) as EventState,
      matched_policy_id: row.matched_policy_id == null ? null : String(row.matched_policy_id),
      matched_policy_version: row.matched_policy_version == null ? null : Number(row.matched_policy_version),
    };
  }

  getClaim(claimId: string): AttentionClaim | null {
    const row = this.db.query<DbRow>(`SELECT * FROM claims WHERE id=${sqlValue(claimId)} AND instance_id=${sqlValue(this.config.instance_id)} LIMIT 1;`)[0];
    return row ? this.claimFromRow(row) : null;
  }

  /** Same paging contract as listEvents: oldest-first unless `newest_first`. */
  listClaims(filters: { state?: ClaimState; states?: ClaimState[]; channel?: string; source?: string; limit?: number; newest_first?: boolean } = {}): AttentionClaim[] {
    const clauses = [`c.instance_id=${sqlValue(this.config.instance_id)}`];
    if (filters.state) clauses.push(`c.state=${sqlValue(filters.state)}`);
    if (filters.states?.length) clauses.push(`c.state IN (${filters.states.map(sqlValue).join(",")})`);
    if (filters.channel) clauses.push(`c.attention_channel=${sqlValue(filters.channel)}`);
    // Filter by source in SQL: filtering after LIMIT would return a short or
    // empty page whenever the matching claims sit beyond the first page.
    if (filters.source) clauses.push(`EXISTS (SELECT 1 FROM json_each(c.event_ids_json) j JOIN events e ON e.id=j.value WHERE e.source=${sqlValue(filters.source)})`);
    const limit = Math.min(100_000, Math.max(1, Math.floor(filters.limit ?? 1000)));
    const order = filters.newest_first ? "DESC" : "ASC";
    const claims = this.db.query<DbRow>(`SELECT c.* FROM claims c WHERE ${clauses.join(" AND ")} ORDER BY c.created_at ${order} LIMIT ${limit};`).map((row) => this.claimFromRow(row));
    return filters.newest_first ? claims.reverse() : claims;
  }

  countClaimsByState(filters: { channel?: string } = {}): Record<ClaimState, number> {
    const clauses = [`instance_id=${sqlValue(this.config.instance_id)}`];
    if (filters.channel) clauses.push(`attention_channel=${sqlValue(filters.channel)}`);
    const counts = Object.fromEntries(CLAIM_STATES.map((state) => [state, 0])) as Record<ClaimState, number>;
    for (const row of this.db.query<DbRow>(`SELECT state, COUNT(*) AS total FROM claims WHERE ${clauses.join(" AND ")} GROUP BY state;`)) {
      counts[String(row.state) as ClaimState] = Number(row.total);
    }
    return counts;
  }

  countBatchesByState(filters: { channel?: string } = {}): Record<BatchState, number> {
    const clauses = [`instance_id=${sqlValue(this.config.instance_id)}`];
    if (filters.channel) clauses.push(`attention_channel=${sqlValue(filters.channel)}`);
    const counts = Object.fromEntries(BATCH_STATES.map((state) => [state, 0])) as Record<BatchState, number>;
    for (const row of this.db.query<DbRow>(`SELECT state, COUNT(*) AS total FROM batches WHERE ${clauses.join(" AND ")} GROUP BY state;`)) {
      counts[String(row.state) as BatchState] = Number(row.total);
    }
    return counts;
  }

  private findClaimByEventId(eventId: string): AttentionClaim | undefined {
    const row = this.db.query<DbRow>(`SELECT c.* FROM claims c WHERE c.instance_id=${sqlValue(this.config.instance_id)} AND EXISTS (SELECT 1 FROM json_each(c.event_ids_json) WHERE json_each.value=${sqlValue(eventId)}) ORDER BY c.created_at ASC LIMIT 1;`)[0];
    return row ? this.claimFromRow(row) : undefined;
  }

  private claimFromRow(row: DbRow): AttentionClaim {
    return {
      id: String(row.id),
      instance_id: String(row.instance_id),
      origin: String(row.origin) as ClaimOrigin,
      event_ids: jsonRecord<string[]>(row.event_ids_json, []),
      resource: jsonRecord<ResourceRef>(row.resource_json, { uri: "unknown:" }),
      policy_id: String(row.policy_id),
      policy_version: Number(row.policy_version),
      attention_channel: String(row.attention_channel),
      eligible_after: String(row.eligible_after),
      expires_at: row.expires_at == null ? null : String(row.expires_at),
      defer_while_presence: Boolean(Number(row.defer_while_presence)),
      state: String(row.state) as ClaimState,
      reason_code: String(row.reason_code),
      note: row.note == null ? null : String(row.note),
      created_at: String(row.created_at),
      updated_at: String(row.updated_at),
      snooze_until: row.snooze_until == null ? null : String(row.snooze_until),
      consumed_result: row.consumed_result_json == null ? null : jsonRecord<JsonValue>(row.consumed_result_json, null),
      dismissed_reason: row.dismissed_reason == null ? null : String(row.dismissed_reason),
    };
  }

  private policy(policyId: string, version: number): InstalledPolicyRule | null {
    const row = this.db.query<DbRow>(`SELECT rule_json FROM policies WHERE id=${sqlValue(policyId)} AND version=${version} LIMIT 1;`)[0];
    return row ? normalizeStoredPolicy(jsonRecord<InstalledPolicyRule>(row.rule_json, DEFAULT_POLICY)) : null;
  }

  getBinding(channel: string): Binding | null {
    const row = this.db.query<DbRow>(`SELECT * FROM bindings WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)} LIMIT 1;`)[0];
    return row ? { instance_id: String(row.instance_id), attention_channel: String(row.attention_channel), endpoint_id: String(row.endpoint_id), generation: Number(row.generation), bound_at: String(row.bound_at) } : null;
  }

  listBindings(): Binding[] {
    return this.db.query<DbRow>(`SELECT * FROM bindings WHERE instance_id=${sqlValue(this.config.instance_id)} ORDER BY attention_channel;`).map((row) => ({ instance_id: String(row.instance_id), attention_channel: String(row.attention_channel), endpoint_id: String(row.endpoint_id), generation: Number(row.generation), bound_at: String(row.bound_at) }));
  }

  private endpointFromRow(row: DbRow, includeToken = false, token?: string, includeRouteSecrets = true): Endpoint {
    const routes = jsonRecord<Endpoint["routes"]>(row.routes_json, []);
    return {
      id: String(row.id),
      instance_id: String(row.instance_id),
      host_kind: String(row.host_kind),
      session_ref: String(row.session_ref),
      lease_token: includeToken ? token : undefined,
      lease_expires_at: String(row.lease_expires_at),
      capabilities: jsonRecord<EndpointCapabilities>(row.capabilities_json, { cold_push: false }),
      routes: includeRouteSecrets ? routes : routes.map((route) => ({
        ...route,
        address: Object.fromEntries(Object.entries(route.address).map(([key, value]) => [
          key,
          SECRET_ADDRESS_KEY.test(key) ? "[redacted]" : value,
        ])),
      })),
      registered_at: String(row.registered_at),
      updated_at: String(row.updated_at),
    };
  }

  getEndpoint(endpointId: string): Endpoint | null {
    const row = this.db.query<DbRow>(`SELECT * FROM endpoints WHERE id=${sqlValue(endpointId)} AND instance_id=${sqlValue(this.config.instance_id)} LIMIT 1;`)[0];
    return row ? this.endpointFromRow(row) : null;
  }

  listEndpoints(): Endpoint[] {
    return this.db.query<DbRow>(`SELECT * FROM endpoints WHERE instance_id=${sqlValue(this.config.instance_id)} ORDER BY id;`).map((row) => this.endpointFromRow(row, false, undefined, false));
  }

  registerEndpoint(registration: EndpointRegistration): Endpoint {
    const write = this.endpointRegistrationWrite(registration);
    this.db.transaction([write.statement]);
    const row = this.db.query<DbRow>(`SELECT * FROM endpoints WHERE id=${sqlValue(write.endpointId)};`)[0];
    return this.endpointFromRow(row, true, write.token);
  }

  private endpointRegistrationWrite(registration: EndpointRegistration, condition = "1") {
    if (!registration?.host_kind || !registration.session_ref) throw new BridgeError("host_kind and session_ref are required", "invalid_endpoint", 400);
    const now = this.nowIso();
    const endpointId = registration.id || id("ep");
    const token = randomBytes(32).toString("base64url");
    const leaseMs = assertFiniteMs(registration.lease_ms, "lease_ms", this.config.endpoint_lease_ms!);
    const capabilities: EndpointCapabilities = {
      cold_push: registration.host_kind === "mock",
      warm_resume: false,
      ...(registration.capabilities ?? {}),
    };
    const routes = registration.routes ?? (registration.host_kind === "mock"
      ? [{ kind: "mock", priority: 100, address: {} }]
      : []);
    return {
      endpointId, token, now,
      statement: `INSERT INTO endpoints(id, instance_id, host_kind, session_ref, lease_token_hash, lease_expires_at, capabilities_json, routes_json, registered_at, updated_at)
       SELECT ${sqlValue(endpointId)}, ${sqlValue(this.config.instance_id)}, ${sqlValue(registration.host_kind)}, ${sqlValue(registration.session_ref)}, ${sqlValue(sha256(token))}, ${sqlValue(addMs(now, leaseMs))}, ${sqlJson(capabilities)}, ${sqlJson(routes)}, ${sqlValue(now)}, ${sqlValue(now)} WHERE ${condition}
       ON CONFLICT(id) DO UPDATE SET host_kind=excluded.host_kind, session_ref=excluded.session_ref, lease_token_hash=excluded.lease_token_hash, lease_expires_at=excluded.lease_expires_at, capabilities_json=excluded.capabilities_json, routes_json=excluded.routes_json, updated_at=excluded.updated_at;`,
    };
  }

  /** Rotate one real session's endpoint lease and all requested bindings atomically. */
  registerHostSession(registration: EndpointRegistration, attentionChannels: string[]): { endpoint: Endpoint; bindings: Binding[] } {
    if (!Array.isArray(attentionChannels) || !attentionChannels.length
      || attentionChannels.some((channel) => typeof channel !== "string" || !channel.trim() || channel.length > 200 || /[\u0000-\u001f\u007f]/u.test(channel))
      || new Set(attentionChannels).size !== attentionChannels.length) {
      throw new BridgeError("host attention_channels must be non-empty, distinct bounded strings", "invalid_host_session", 400);
    }
    const guard = "(SELECT status='ok' FROM host_session_guard)";
    const write = this.endpointRegistrationWrite(registration, guard);
    const bindings = this.listBindings();
    const expected = attentionChannels.map((channel) => bindings.find((binding) => binding.attention_channel === channel)?.generation ?? 0);
    const channelsSql = attentionChannels.map(sqlValue).join(", ");
    const cas = attentionChannels.map((channel, index) => `COALESCE((SELECT generation FROM bindings WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)}), 0)=${expected[index]}`).join(" AND ");
    // The status guard is evaluated under the same writer lock as every write.
    // Returning it avoids depending on SQLite diagnostic text for API errors.
    const row = this.db.query<DbRow>([
      "BEGIN IMMEDIATE;",
      this.channelGuardSql(attentionChannels),
      "CREATE TEMP TABLE host_session_guard(status TEXT NOT NULL);",
      `INSERT INTO host_session_guard SELECT CASE
        WHEN NOT ${CHANNEL_WRITE_ALLOWED} THEN (SELECT status FROM channel_write_guard)
        WHEN EXISTS(SELECT 1 FROM bindings WHERE instance_id=${sqlValue(this.config.instance_id)} AND endpoint_id=${sqlValue(write.endpointId)} AND attention_channel NOT IN (${channelsSql})) THEN 'host_session_channels_required'
        WHEN NOT (${cas}) THEN 'stale_generation' ELSE 'ok' END;`,
      this.channelRegistrationSql(attentionChannels, write.now, guard),
      write.statement,
      ...attentionChannels.flatMap((channel, index) => this.bindingTakeoverStatements(channel, write.endpointId, expected[index] + 1, write.now, guard)),
      `SELECT g.status, e.*, (SELECT json_group_array(json_object('instance_id', instance_id, 'attention_channel', attention_channel, 'endpoint_id', endpoint_id, 'generation', generation, 'bound_at', bound_at)) FROM bindings WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel IN (${channelsSql})) AS bindings_json
        FROM host_session_guard g LEFT JOIN endpoints e ON e.id=${sqlValue(write.endpointId)} AND e.instance_id=${sqlValue(this.config.instance_id)};`,
      "COMMIT;",
    ].join("\n"))[0];
    this.checkChannelStatus(row.status);
    if (row.status === "host_session_channels_required") {
      throw new BridgeError("all channels bound to this host session must be included", "host_session_channels_required", 409);
    }
    if (row.status !== "ok") throw new BridgeError("host session binding generation changed during registration", "stale_generation", 409);
    const committed = jsonRecord<Binding[]>(row.bindings_json, []);
    return {
      endpoint: this.endpointFromRow(row, true, write.token),
      bindings: attentionChannels.map((channel) => committed.find((binding) => binding.attention_channel === channel)!),
    };
  }

  renewEndpoint(endpointId: string, leaseToken: string, leaseMs?: number): Endpoint {
    const now = this.nowIso();
    const duration = assertFiniteMs(leaseMs, "lease_ms", this.config.endpoint_lease_ms!);
    const result = this.db.query<DbRow>(`UPDATE endpoints SET lease_expires_at=${sqlValue(addMs(now, duration))}, updated_at=${sqlValue(now)} WHERE id=${sqlValue(endpointId)} AND instance_id=${sqlValue(this.config.instance_id)} AND lease_token_hash=${sqlValue(sha256(leaseToken))} RETURNING *;`)[0];
    if (!result) throw new BridgeError("endpoint lease token is invalid or endpoint is unknown", "endpoint_fenced", 409);
    return this.endpointFromRow(result);
  }

  renewBoundEndpoint(endpointId: string, channel: string, expectedGeneration: number, leaseMs?: number, allowedChannels?: string[]): Endpoint {
    const now = this.nowIso();
    const duration = assertFiniteMs(leaseMs, "lease_ms", this.config.endpoint_lease_ms!);
    const row = this.db.query<DbRow>([
      "BEGIN IMMEDIATE;",
      this.boundEndpointGuardSql(endpointId, channel, expectedGeneration, allowedChannels),
      `UPDATE endpoints SET lease_expires_at=${sqlValue(addMs(now, duration))}, updated_at=${sqlValue(now)} WHERE id=${sqlValue(endpointId)} AND instance_id=${sqlValue(this.config.instance_id)} AND (SELECT status='ok' FROM bound_endpoint_guard);`,
      `SELECT g.status, e.* FROM bound_endpoint_guard g LEFT JOIN endpoints e ON e.id=${sqlValue(endpointId)} AND e.instance_id=${sqlValue(this.config.instance_id)};`,
      "COMMIT;",
    ].join("\n"))[0];
    return this.boundEndpointResult(row);
  }

  revokeEndpoint(endpointId: string, leaseToken: string): Endpoint {
    this.endpointAuthorized(endpointId, leaseToken);
    return this.revokeEndpointState(endpointId);
  }

  revokeBoundEndpoint(endpointId: string, channel: string, expectedGeneration: number, allowedChannels?: string[]): Endpoint {
    return this.revokeEndpointState(endpointId, this.boundEndpointGuardSql(endpointId, channel, expectedGeneration, allowedChannels));
  }

  private boundEndpointGuardSql(endpointId: string, channel: string, expectedGeneration: number, allowedChannels?: string[]): string {
    return `CREATE TEMP TABLE bound_endpoint_guard(status TEXT NOT NULL);
      INSERT INTO bound_endpoint_guard SELECT CASE
        WHEN EXISTS(SELECT 1 FROM attention_channels WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)} AND deleted_at IS NOT NULL) THEN 'channel_deleted'
        WHEN NOT EXISTS(SELECT 1 FROM bindings WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)} AND endpoint_id=${sqlValue(endpointId)} AND generation=${sqlValue(expectedGeneration)}) THEN 'stale_generation'
        WHEN NOT EXISTS(SELECT 1 FROM endpoints WHERE instance_id=${sqlValue(this.config.instance_id)} AND id=${sqlValue(endpointId)}) THEN 'endpoint_not_found'
        ${allowedChannels ? `WHEN EXISTS(SELECT 1 FROM bindings WHERE instance_id=${sqlValue(this.config.instance_id)} AND endpoint_id=${sqlValue(endpointId)} AND attention_channel NOT IN (${allowedChannels.map(sqlValue).join(", ")})) THEN 'invalid_host_session'` : ""}
        ELSE 'ok' END;`;
  }

  private boundEndpointResult(row: DbRow): Endpoint {
    this.checkChannelStatus(row.status);
    if (row.status === "stale_generation") throw new BridgeError("endpoint operation does not match current binding generation", "stale_generation", 409);
    if (row.status === "endpoint_not_found") throw new BridgeError("endpoint is unknown", "endpoint_not_found", 404);
    if (row.status === "invalid_host_session") throw new BridgeError("host credential must cover all channels bound to this endpoint", "invalid_host_session", 400);
    return this.endpointFromRow(row);
  }

  private revokeEndpointState(endpointId: string, guardSql?: string): Endpoint {
    const now = this.nowIso();
    const condition = guardSql ? "(SELECT status='ok' FROM bound_endpoint_guard)" : "1";
    const row = this.db.query<DbRow>([
      "BEGIN IMMEDIATE;",
      guardSql ?? "",
      `UPDATE endpoints SET lease_expires_at=${sqlValue(now)}, updated_at=${sqlValue(now)} WHERE id=${sqlValue(endpointId)} AND instance_id=${sqlValue(this.config.instance_id)} AND ${condition};`,
      `DELETE FROM presence_leases WHERE instance_id=${sqlValue(this.config.instance_id)} AND endpoint_id=${sqlValue(endpointId)} AND ${condition};`,
      `UPDATE activity_watches SET activity_lease_expires_at=NULL, updated_at=${sqlValue(now)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND endpoint_id=${sqlValue(endpointId)} AND ${condition};`,
      `UPDATE delivery_correlations SET state='discarded' WHERE endpoint_id=${sqlValue(endpointId)} AND state='outstanding' AND ${condition};`,
      guardSql
        ? `SELECT g.status, e.* FROM bound_endpoint_guard g LEFT JOIN endpoints e ON e.id=${sqlValue(endpointId)} AND e.instance_id=${sqlValue(this.config.instance_id)};`
        : `SELECT 'ok' AS status, e.* FROM endpoints e WHERE e.id=${sqlValue(endpointId)} AND e.instance_id=${sqlValue(this.config.instance_id)};`,
      "COMMIT;",
    ].join("\n"))[0];
    return this.boundEndpointResult(row);
  }

  takeover(channel: string, endpointId: string, expectedGeneration?: number): Binding {
    validateChannelId(channel);
    this.assertChannelNotDeleted(channel);
    const endpoint = this.getEndpoint(endpointId);
    if (!endpoint) throw new BridgeError("endpoint is unknown", "endpoint_not_found", 404);
    if (new Date(endpoint.lease_expires_at).getTime() <= this.now().getTime()) throw new BridgeError("endpoint lease has expired", "endpoint_fenced", 409);
    const current = this.getBinding(channel);
    const currentGeneration = current?.generation ?? 0;
    if (expectedGeneration != null && expectedGeneration !== currentGeneration) {
      throw new BridgeError(`generation mismatch: expected ${expectedGeneration}, current ${currentGeneration}`, "stale_generation", 409);
    }
    const generation = currentGeneration + 1;
    const now = this.nowIso();
    const guard = "(SELECT status='ok' FROM takeover_guard)";
    const row = this.db.query<{ status: string }>([
      "BEGIN IMMEDIATE;", this.channelGuardSql([channel]),
      `CREATE TEMP TABLE takeover_guard AS SELECT CASE
        WHEN NOT ${CHANNEL_WRITE_ALLOWED} THEN (SELECT status FROM channel_write_guard)
        WHEN NOT EXISTS(SELECT 1 FROM endpoints WHERE id=${sqlValue(endpointId)} AND instance_id=${sqlValue(this.config.instance_id)}) THEN 'endpoint_not_found'
        WHEN NOT EXISTS(SELECT 1 FROM endpoints WHERE id=${sqlValue(endpointId)} AND instance_id=${sqlValue(this.config.instance_id)} AND lease_expires_at>${sqlValue(now)}) THEN 'endpoint_fenced'
        WHEN COALESCE((SELECT generation FROM bindings WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)}), 0)<>${currentGeneration} THEN 'stale_generation'
        ELSE 'ok' END AS status;`,
      this.channelRegistrationSql([channel], now, guard),
      ...this.bindingTakeoverStatements(channel, endpointId, generation, now, guard),
      "SELECT status FROM takeover_guard;", "COMMIT;",
    ].join("\n"))[0];
    this.checkChannelStatus(row.status);
    if (row.status === "endpoint_not_found") throw new BridgeError("endpoint is unknown", row.status, 404);
    if (row.status === "endpoint_fenced") throw new BridgeError("endpoint lease has expired", row.status, 409);
    if (row.status === "stale_generation") throw new BridgeError("binding generation changed during takeover", row.status, 409);
    return this.getBinding(channel)!;
  }

  private bindingTakeoverStatements(channel: string, endpointId: string, generation: number, now: string, condition = "1"): string[] {
    return [
      `INSERT INTO bindings(instance_id, attention_channel, endpoint_id, generation, bound_at)
       SELECT ${sqlValue(this.config.instance_id)}, ${sqlValue(channel)}, ${sqlValue(endpointId)}, ${generation}, ${sqlValue(now)} WHERE ${condition}
       ON CONFLICT(instance_id, attention_channel) DO UPDATE SET endpoint_id=excluded.endpoint_id, generation=excluded.generation, bound_at=excluded.bound_at;`,
      `DELETE FROM presence_leases WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)} AND ${condition};`,
      `UPDATE activity_watches SET endpoint_id=NULL, binding_generation=NULL, epoch=epoch+1, armed_at=NULL, last_nonwake_activity_at=NULL, activity_lease_expires_at=NULL, fired_epoch=NULL, sequence=0, last_triggered_at=NULL, next_due_at=NULL, updated_at=${sqlValue(now)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)} AND ${condition};`,
      `UPDATE delivery_correlations SET state='discarded' WHERE state='outstanding' AND batch_id IN (SELECT id FROM batches WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)}) AND ${condition};`,
      `UPDATE batches SET state='pending', binding_generation=NULL, updated_at=${sqlValue(now)}, last_error=NULL WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)} AND state='waiting_for_endpoint' AND ${condition};`,
    ];
  }

  private endpointAuthorized(endpointId: string, token: string): DbRow {
    const row = this.db.query<DbRow>(`SELECT * FROM endpoints WHERE id=${sqlValue(endpointId)} AND instance_id=${sqlValue(this.config.instance_id)} AND lease_token_hash=${sqlValue(sha256(token))} LIMIT 1;`)[0];
    if (!row) throw new BridgeError("endpoint lease token is invalid", "endpoint_fenced", 409);
    if (new Date(String(row.lease_expires_at)).getTime() <= this.now().getTime()) throw new BridgeError("endpoint lease has expired", "endpoint_fenced", 409);
    return row;
  }

  renewPresence(input: PresenceRenewal): PresenceLease {
    if (!input?.attention_channel || !input.endpoint_id || !input.observed_by) throw new BridgeError("presence channel, endpoint and observed_by are required", "invalid_presence", 400);
    this.assertChannelNotDeleted(input.attention_channel);
    const endpoint = this.endpointAuthorized(input.endpoint_id, input.lease_token || "");
    const capabilities = jsonRecord<EndpointCapabilities>(endpoint.capabilities_json, { cold_push: false });
    if (!capabilities.foreground_presence_observable) {
      throw new BridgeError("bound host cannot observe foreground presence", "presence_unsupported", 409);
    }
    const binding = this.getBinding(input.attention_channel);
    if (!binding || binding.endpoint_id !== input.endpoint_id || binding.generation !== input.generation) {
      throw new BridgeError("presence renewal does not match current binding generation", "stale_generation", 409);
    }
    const now = this.nowIso();
    const ttl = Math.min(assertFiniteMs(input.ttl_ms, "ttl_ms", this.config.presence_ttl_ms!), 24 * 60 * 60_000);
    const expires = addMs(now, ttl);
    this.channelTransaction([input.attention_channel], now, [
      this.channelBindingFenceSql(input.attention_channel, input.endpoint_id, input.generation, now, input.lease_token || ""),
      `INSERT INTO presence_leases(instance_id, attention_channel, endpoint_id, binding_generation, renewed_at, expires_at, observed_by, observation)
       SELECT ${sqlValue(this.config.instance_id)}, ${sqlValue(input.attention_channel)}, ${sqlValue(input.endpoint_id)}, ${input.generation}, ${sqlValue(now)}, ${sqlValue(expires)}, ${sqlValue(input.observed_by)}, ${sqlValue(input.observation || "user_message_accepted")} WHERE ${CHANNEL_WRITE_ALLOWED}
       ON CONFLICT(instance_id, attention_channel) DO UPDATE SET endpoint_id=excluded.endpoint_id, binding_generation=excluded.binding_generation, renewed_at=excluded.renewed_at, expires_at=excluded.expires_at, observed_by=excluded.observed_by, observation=excluded.observation;`,
    ], false);
    return this.getPresence(input.attention_channel)!;
  }

  configureActivityWatch(input: ActivityWatchConfiguration): ActivityWatch {
    const channel = input?.attention_channel;
    validateChannelId(channel);
    this.assertChannelNotDeleted(channel);
    const idleAfter = Number(input.idle_after_ms);
    if (!Number.isSafeInteger(idleAfter) || idleAfter < 60_000 || idleAfter > 7 * 24 * 60 * 60_000) {
      throw new BridgeError("idle_after_ms must be an integer between 60000 and 604800000", "invalid_activity_watch", 400);
    }
    const existing = this.getActivityWatch(channel);
    const mode = input.mode ?? existing?.mode ?? "once";
    if (!["once", "repeat"].includes(mode)) {
      throw new BridgeError("mode must be once or repeat", "invalid_activity_watch", 400);
    }
    const repeatAfter = Number(input.repeat_after_ms ?? existing?.repeat_after_ms ?? idleAfter);
    if (!Number.isSafeInteger(repeatAfter) || repeatAfter < 60_000 || repeatAfter > 7 * 24 * 60 * 60_000) {
      throw new BridgeError("repeat_after_ms must be an integer between 60000 and 604800000", "invalid_activity_watch", 400);
    }
    if (existing && existing.enabled === input.enabled && existing.idle_after_ms === idleAfter
      && existing.mode === mode && existing.repeat_after_ms === repeatAfter) {
      this.channelTransaction([channel], this.nowIso(), [], false);
      return existing;
    }
    const now = this.nowIso();
    const binding = this.getBinding(channel);
    if (!existing) {
      this.channelTransaction([channel], now, [
        `INSERT INTO activity_watches(instance_id, attention_channel, enabled, idle_after_ms, endpoint_id, binding_generation, epoch, armed_at, last_nonwake_activity_at, activity_lease_expires_at, fired_epoch, mode, repeat_after_ms, sequence, last_triggered_at, next_due_at, updated_at)
         SELECT ${sqlValue(this.config.instance_id)}, ${sqlValue(channel)}, ${sqlValue(input.enabled)}, ${idleAfter}, ${sqlValue(binding?.endpoint_id)}, ${sqlValue(binding?.generation)}, 1, NULL, NULL, NULL, NULL, ${sqlValue(mode)}, ${repeatAfter}, 0, NULL, NULL, ${sqlValue(now)} WHERE ${CHANNEL_WRITE_ALLOWED};`,
      ]);
    } else if (!existing.enabled && input.enabled) {
      this.channelTransaction([channel], now, [
        `UPDATE activity_watches SET enabled=1, idle_after_ms=${idleAfter}, mode=${sqlValue(mode)}, repeat_after_ms=${repeatAfter}, endpoint_id=${sqlValue(binding?.endpoint_id)}, binding_generation=${sqlValue(binding?.generation)}, epoch=epoch+1, armed_at=NULL, last_nonwake_activity_at=NULL, activity_lease_expires_at=NULL, fired_epoch=NULL, sequence=0, last_triggered_at=NULL, next_due_at=NULL, updated_at=${sqlValue(now)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)} AND ${CHANNEL_WRITE_ALLOWED};`,
      ]);
    } else {
      const nextDueAt = input.enabled && mode === "repeat" && existing.fired_epoch === existing.epoch
        ? addMs(now, repeatAfter)
        : null;
      this.channelTransaction([channel], now, [
        `UPDATE activity_watches SET enabled=${sqlValue(input.enabled)}, idle_after_ms=${idleAfter}, mode=${sqlValue(mode)}, repeat_after_ms=${repeatAfter}, activity_lease_expires_at=${sqlValue(input.enabled ? existing.activity_lease_expires_at : null)}, next_due_at=${sqlValue(nextDueAt)}, updated_at=${sqlValue(now)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)} AND ${CHANNEL_WRITE_ALLOWED};`,
      ]);
    }
    return this.getActivityWatch(channel)!;
  }

  getActivityWatch(channel: string): ActivityWatch | null {
    const row = this.db.query<DbRow>(`SELECT * FROM activity_watches WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)} LIMIT 1;`)[0];
    return row ? this.activityWatchFromRow(row) : null;
  }

  listActivityWatches(): ActivityWatch[] {
    return this.db.query<DbRow>(`SELECT * FROM activity_watches WHERE instance_id=${sqlValue(this.config.instance_id)} ORDER BY attention_channel;`).map((row) => this.activityWatchFromRow(row));
  }

  private activityWatchFromRow(row: DbRow): ActivityWatch {
    const enabled = Number(row.enabled) === 1;
    const binding = this.getBinding(String(row.attention_channel));
    const endpoint = binding ? this.getEndpoint(binding.endpoint_id) : null;
    const matchesBinding = Boolean(binding && row.endpoint_id === binding.endpoint_id && Number(row.binding_generation) === binding.generation);
    const lastActivity = row.last_nonwake_activity_at == null ? null : String(row.last_nonwake_activity_at);
    const leaseExpires = row.activity_lease_expires_at == null ? null : String(row.activity_lease_expires_at);
    const now = this.nowIso();
    const conditionStatus: ActivityWatch["condition_status"] = !enabled ? "disabled"
      : !binding || !endpoint || endpoint.lease_expires_at <= now ? "waiting_for_endpoint"
      : !endpoint.capabilities.session_activity_observable ? "unsupported"
      : !matchesBinding || !lastActivity ? "waiting_for_activity"
      : leaseExpires && leaseExpires > now ? "watching"
      : new Date(lastActivity).getTime() + Number(row.idle_after_ms) <= new Date(now).getTime() ? "inactive"
      : "watching";
    return {
      instance_id: String(row.instance_id),
      attention_channel: String(row.attention_channel),
      enabled,
      idle_after_ms: Number(row.idle_after_ms),
      endpoint_id: row.endpoint_id == null ? null : String(row.endpoint_id),
      binding_generation: row.binding_generation == null ? null : Number(row.binding_generation),
      epoch: Number(row.epoch),
      armed_at: row.armed_at == null ? null : String(row.armed_at),
      last_nonwake_activity_at: row.last_nonwake_activity_at == null ? null : String(row.last_nonwake_activity_at),
      activity_lease_expires_at: row.activity_lease_expires_at == null ? null : String(row.activity_lease_expires_at),
      fired_epoch: row.fired_epoch == null ? null : Number(row.fired_epoch),
      mode: String(row.mode) as ActivityWatch["mode"],
      repeat_after_ms: Number(row.repeat_after_ms),
      sequence: Number(row.sequence),
      last_triggered_at: row.last_triggered_at == null ? null : String(row.last_triggered_at),
      next_due_at: row.next_due_at == null ? null : String(row.next_due_at),
      condition_status: conditionStatus,
      limits: { min_interval_ms: 60_000, max_claims_per_tick: 16 },
      updated_at: String(row.updated_at),
    };
  }

  observeActivity(input: ActivityObservation): ActivityObservationResult {
    if (!input?.attention_channel || !input.endpoint_id || !input.observation_id) throw new BridgeError("activity channel, endpoint and observation_id are required", "invalid_activity", 400);
    this.assertChannelNotDeleted(input.attention_channel);
    if (input.observation_id.length > 256) throw new BridgeError("observation_id is too long", "invalid_activity", 400);
    const kinds = new Set(["owner_message_accepted", "activity_started", "activity_progress", "activity_settled", "wake_started", "wake_progress", "wake_settled"]);
    if (!kinds.has(input.kind)) throw new BridgeError("activity kind is invalid", "invalid_activity", 400);
    this.endpointAuthorized(input.endpoint_id, input.lease_token || "");
    const binding = this.getBinding(input.attention_channel);
    if (!binding || binding.endpoint_id !== input.endpoint_id || binding.generation !== input.generation) throw new BridgeError("activity observation does not match current binding generation", "stale_generation", 409);
    const receivedAt = this.nowIso();
    const observedAt = input.observed_at == null ? receivedAt : isoOrThrow(input.observed_at, "observed_at");
    const skew = new Date(observedAt).getTime() - new Date(receivedAt).getTime();
    if (skew > 5 * 60_000 || skew < -7 * 24 * 60 * 60_000) throw new BridgeError("observed_at is outside the accepted window", "invalid_activity", 400);
    const ttl = Math.min(assertFiniteMs(input.ttl_ms, "ttl_ms", this.config.activity_ttl_ms!), 60 * 60_000);
    const leaseExpires = addMs(receivedAt, ttl);
    const configuredWatch = this.getActivityWatch(input.attention_channel);
    if (!configuredWatch?.enabled) return { duplicate: false, watch: configuredWatch };
    const insert = `INSERT OR IGNORE INTO activity_observations(instance_id, endpoint_id, binding_generation, observation_id, attention_channel, kind, observed_at, received_at)
      SELECT ${sqlValue(this.config.instance_id)}, ${sqlValue(input.endpoint_id)}, ${input.generation}, ${sqlValue(input.observation_id)}, ${sqlValue(input.attention_channel)}, ${sqlValue(input.kind)}, ${sqlValue(observedAt)}, ${sqlValue(receivedAt)} WHERE ${CHANNEL_WRITE_ALLOWED};`;
    const nonWakeActivity = input.kind === "owner_message_accepted"
      || input.kind === "activity_started"
      || input.kind === "activity_progress"
      || input.kind === "activity_settled";
    // A host is not required to observe the beginning of every independent
    // turn.  In particular, polling adapters may first see a progress or
    // settled sample after takeover.  Any trusted non-Wake observation can
    // therefore establish the current-generation baseline (and start a new
    // epoch after a previous one fired).  Wake-origin observations remain
    // unable to arm or rearm the watch.
    const establishBaseline = nonWakeActivity && (
      configuredWatch.endpoint_id !== input.endpoint_id
      || configuredWatch.binding_generation !== input.generation
      || !configuredWatch.last_nonwake_activity_at
      || configuredWatch.fired_epoch === configuredWatch.epoch
    );
    const repeatNotBefore = addMs(receivedAt, configuredWatch.repeat_after_ms);
    let update = "";
    if (establishBaseline) {
      update = `UPDATE activity_watches SET endpoint_id=${sqlValue(input.endpoint_id)}, binding_generation=${input.generation}, epoch=CASE WHEN fired_epoch=epoch THEN epoch+1 ELSE epoch END, armed_at=${sqlValue(observedAt)}, last_nonwake_activity_at=${sqlValue(observedAt)}, activity_lease_expires_at=${sqlValue(input.kind === "activity_started" || input.kind === "activity_progress" ? leaseExpires : null)}, fired_epoch=NULL, sequence=0, last_triggered_at=NULL, next_due_at=NULL, updated_at=${sqlValue(receivedAt)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(input.attention_channel)} AND enabled=1 AND changes()>0;`;
    } else if (input.kind === "owner_message_accepted" || input.kind === "activity_started") {
      update = `UPDATE activity_watches SET last_nonwake_activity_at=${sqlValue(observedAt)}, activity_lease_expires_at=${sqlValue(input.kind === "activity_started" ? leaseExpires : null)}, updated_at=${sqlValue(receivedAt)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(input.attention_channel)} AND enabled=1 AND endpoint_id=${sqlValue(input.endpoint_id)} AND binding_generation=${input.generation} AND changes()>0;`;
    } else if (input.kind === "activity_progress") {
      update = `UPDATE activity_watches SET activity_lease_expires_at=${sqlValue(leaseExpires)}, updated_at=${sqlValue(receivedAt)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(input.attention_channel)} AND enabled=1 AND endpoint_id=${sqlValue(input.endpoint_id)} AND binding_generation=${input.generation} AND fired_epoch IS NOT epoch AND changes()>0;`;
    } else if (input.kind === "activity_settled") {
      update = `UPDATE activity_watches SET last_nonwake_activity_at=CASE WHEN fired_epoch IS NOT epoch THEN ${sqlValue(observedAt)} ELSE last_nonwake_activity_at END, activity_lease_expires_at=NULL, updated_at=${sqlValue(receivedAt)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(input.attention_channel)} AND enabled=1 AND endpoint_id=${sqlValue(input.endpoint_id)} AND binding_generation=${input.generation} AND changes()>0;`;
    } else if (input.kind === "wake_started" || input.kind === "wake_progress") {
      update = `UPDATE activity_watches SET activity_lease_expires_at=${sqlValue(leaseExpires)}, updated_at=${sqlValue(receivedAt)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(input.attention_channel)} AND enabled=1 AND endpoint_id=${sqlValue(input.endpoint_id)} AND binding_generation=${input.generation} AND changes()>0;`;
    } else if (input.kind === "wake_settled") {
      update = `UPDATE activity_watches SET activity_lease_expires_at=NULL, next_due_at=CASE WHEN mode='repeat' AND fired_epoch=epoch THEN ${sqlValue(repeatNotBefore)} ELSE next_due_at END, updated_at=${sqlValue(receivedAt)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(input.attention_channel)} AND enabled=1 AND endpoint_id=${sqlValue(input.endpoint_id)} AND binding_generation=${input.generation} AND changes()>0;`;
    }
    const before = this.db.query<DbRow>(`SELECT 1 AS present FROM activity_observations WHERE instance_id=${sqlValue(this.config.instance_id)} AND endpoint_id=${sqlValue(input.endpoint_id)} AND binding_generation=${input.generation} AND observation_id=${sqlValue(input.observation_id)} LIMIT 1;`)[0];
    this.channelTransaction([input.attention_channel], receivedAt, [
      this.channelBindingFenceSql(input.attention_channel, input.endpoint_id, input.generation, receivedAt, input.lease_token || ""),
      `DELETE FROM activity_observations WHERE instance_id=${sqlValue(this.config.instance_id)} AND received_at<${sqlValue(addMs(receivedAt, -7 * 24 * 60 * 60_000))} AND ${CHANNEL_WRITE_ALLOWED};`,
      insert,
      update,
    ], false);
    return { duplicate: Boolean(before), watch: this.getActivityWatch(input.attention_channel) };
  }

  consumeWakeEcho(input: WakeEchoObservation): ActivityObservationResult {
    if (!input?.attention_channel || !input.endpoint_id || !input.observation_id || input.observation_id.length > 256
      || !Number.isSafeInteger(input.generation) || input.generation < 1) {
      throw new BridgeError("wake echo identity is invalid", "invalid_activity", 400);
    }
    this.assertChannelNotDeleted(input.attention_channel);
    if (!input?.delivery_nonce || input.delivery_nonce.length > 256) {
      throw new BridgeError("delivery nonce is required and bounded", "invalid_delivery_nonce", 400);
    }
    const now = this.nowIso();
    const observedAt = input.observed_at == null ? now : isoOrThrow(input.observed_at, "observed_at");
    const skew = new Date(observedAt).getTime() - new Date(now).getTime();
    if (skew > 5 * 60_000 || skew < -7 * 24 * 60 * 60_000) {
      throw new BridgeError("observed_at is outside the accepted window", "invalid_activity", 400);
    }
    if (input.ttl_ms != null) assertFiniteMs(input.ttl_ms, "ttl_ms", this.config.activity_ttl_ms!);
    this.endpointAuthorized(input.endpoint_id, input.lease_token || "");
    const binding = this.getBinding(input.attention_channel);
    if (!binding || binding.endpoint_id !== input.endpoint_id || binding.generation !== input.generation) {
      throw new BridgeError("wake echo does not match current binding generation", "stale_generation", 409);
    }
    const nonceHash = sha256(input.delivery_nonce);
    const correlation = this.db.query<DbRow>(`SELECT dc.*, b.state AS batch_state, b.attempt AS batch_attempt,
        b.last_error AS batch_last_error, a.attempt_no AS attempt_no
      FROM delivery_correlations dc JOIN batches b ON b.id=dc.batch_id JOIN outbox_attempts a ON a.id=dc.attempt_id
      WHERE b.instance_id=${sqlValue(this.config.instance_id)} AND b.attention_channel=${sqlValue(input.attention_channel)} AND dc.endpoint_id=${sqlValue(input.endpoint_id)}
        AND dc.binding_generation=${input.generation} AND dc.nonce_hash=${sqlValue(nonceHash)} LIMIT 1;`)[0];
    if (!correlation) throw new BridgeError("delivery nonce is unknown", "unknown_delivery_nonce", 409);
    if (String(correlation.state) !== "outstanding") throw new BridgeError("delivery nonce was already consumed or discarded", "delivery_nonce_replayed", 409);
    if (String(correlation.expires_at) <= now) {
      this.db.exec(`UPDATE delivery_correlations SET state='expired' WHERE attempt_id=${sqlValue(correlation.attempt_id)} AND state='outstanding';`);
      throw new BridgeError("delivery nonce has expired", "delivery_nonce_expired", 409);
    }
    if (Number(correlation.batch_attempt) !== Number(correlation.attempt_no)) {
      throw new BridgeError("delivery nonce belongs to a stale dispatch attempt", "stale_delivery_correlation", 409);
    }
    const batchState = String(correlation.batch_state) as BatchState;
    const lateAck = batchState === "needs_attention" && String(correlation.batch_last_error) === "ack_timeout";
    const canSetSeen = ["dispatching", "retry_wait", "dead_letter", "dispatched"].includes(batchState) || lateAck;
    const details = { attested_by: "host", evidence: "delivery_nonce_echo" };
    const statements = [
      this.channelBindingFenceSql(input.attention_channel, input.endpoint_id, input.generation, now, input.lease_token || ""),
      `UPDATE delivery_correlations SET state='consumed', consumed_at=${sqlValue(now)}
       WHERE attempt_id=${sqlValue(correlation.attempt_id)} AND state='outstanding' AND ${CHANNEL_WRITE_ALLOWED};`,
      `INSERT INTO receipts(id, batch_id, claim_id, stage, at, endpoint_id, binding_generation, transport_kind, details_json)
       SELECT ${sqlValue(id("rcpt"))}, ${sqlValue(correlation.batch_id)}, NULL, 'host_attested', ${sqlValue(now)}, ${sqlValue(input.endpoint_id)}, ${input.generation}, ${sqlValue(correlation.transport_kind)}, ${sqlJson(details)} WHERE changes()>0;`,
    ];
    if (canSetSeen && batchState !== "dispatching") {
      statements.push(
        `UPDATE batches SET state='seen', updated_at=${sqlValue(now)}, lease_expires_at=NULL, last_error=NULL
         WHERE id=${sqlValue(correlation.batch_id)} AND instance_id=${sqlValue(this.config.instance_id)}
           AND state=${sqlValue(batchState)} AND attempt=${Number(correlation.attempt_no)}
           AND ${CHANNEL_WRITE_ALLOWED}
           ${lateAck ? "AND last_error='ack_timeout'" : ""};`,
        `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
         SELECT ${sqlValue(correlation.batch_id)}, ${sqlValue(batchState)}, 'seen', 'host_attested', ${sqlValue(now)} WHERE changes()>0;`,
      );
    }
    this.channelTransaction([input.attention_channel], now, statements, false);
    return this.observeActivity({
      attention_channel: input.attention_channel,
      endpoint_id: input.endpoint_id,
      generation: input.generation,
      lease_token: input.lease_token,
      observation_id: input.observation_id,
      kind: "wake_started",
      observed_at: observedAt,
      ttl_ms: input.ttl_ms,
    });
  }

  listDeliveryCorrelations(): DeliveryCorrelation[] {
    return this.db.query<DbRow>(`SELECT dc.* FROM delivery_correlations dc JOIN batches b ON b.id=dc.batch_id
      WHERE b.instance_id=${sqlValue(this.config.instance_id)} ORDER BY dc.created_at ASC;`).map((row) => ({
      attempt_id: String(row.attempt_id),
      batch_id: String(row.batch_id),
      endpoint_id: String(row.endpoint_id),
      binding_generation: Number(row.binding_generation),
      transport_kind: String(row.transport_kind),
      nonce_hash: String(row.nonce_hash),
      state: String(row.state) as DeliveryCorrelation["state"],
      created_at: String(row.created_at),
      expires_at: String(row.expires_at),
      consumed_at: row.consumed_at == null ? null : String(row.consumed_at),
    }));
  }

  getPresence(channel: string): PresenceLease | null {
    const row = this.db.query<DbRow>(`SELECT * FROM presence_leases WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)} LIMIT 1;`)[0];
    return row ? this.presenceFromRow(row) : null;
  }

  listPresence(): PresenceLease[] {
    return this.db.query<DbRow>(`SELECT * FROM presence_leases WHERE instance_id=${sqlValue(this.config.instance_id)} ORDER BY attention_channel;`).map((row) => this.presenceFromRow(row));
  }

  private presenceFromRow(row: DbRow): PresenceLease {
    return {
      instance_id: String(row.instance_id),
      attention_channel: String(row.attention_channel),
      endpoint_id: String(row.endpoint_id),
      binding_generation: Number(row.binding_generation),
      renewed_at: String(row.renewed_at),
      expires_at: String(row.expires_at),
      observed_by: String(row.observed_by),
      observation: String(row.observation),
    };
  }

  private presenceActive(channel: string, now: string): PresenceLease | null {
    const presence = this.getPresence(channel);
    if (!presence || presence.expires_at <= now) return null;
    const binding = this.getBinding(channel);
    if (!binding || binding.endpoint_id !== presence.endpoint_id || binding.generation !== presence.binding_generation) return null;
    const endpoint = this.getEndpoint(presence.endpoint_id);
    if (!endpoint || endpoint.lease_expires_at <= now) return null;
    return presence;
  }

  private recover(): void {
    const now = this.nowIso();
    const ackTimeoutCutoff = addMs(now, -this.config.ack_timeout_ms!);
    const timedOut = this.db.query<DbRow>(`SELECT b.id FROM batches b JOIN outbox_attempts a ON a.batch_id=b.id
      WHERE b.instance_id=${sqlValue(this.config.instance_id)} AND b.state='dispatched'
        AND b.binding_generation IS NOT NULL AND a.state='accepted' AND a.attempt_no=b.attempt
        AND a.binding_generation=b.binding_generation AND a.finished_at IS NOT NULL
        AND a.finished_at<=${sqlValue(ackTimeoutCutoff)};`);
    // An ack_timeout item exists so that a missing acknowledgement is noticed.
    // Once every claim the batch carried is consumed, dismissed or expired, the
    // agent has dealt with it by some route and the item is no longer
    // actionable; leaving it open only buries newer incidents.  This runs before
    // the timeout transition below, so a batch always spends at least one sweep
    // in needs_attention and still raises its incident.  A claim row that is
    // missing entirely keeps the item open.
    const allClaimsFinalized = `json_array_length(claim_ids_json)>0
         AND NOT EXISTS (
           SELECT 1 FROM json_each(batches.claim_ids_json) j LEFT JOIN claims c ON c.id=j.value
           WHERE c.id IS NULL OR c.state NOT IN (${FINALIZED_CLAIM_STATES.map(sqlValue).join(",")})
         )`;
    const settledAckTimeout = `instance_id=${sqlValue(this.config.instance_id)} AND state='needs_attention' AND last_error='ack_timeout'
         AND ${allClaimsFinalized}`;
    this.db.transaction([
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
       SELECT id, 'needs_attention', 'cancelled', 'all_claims_finalized_reconciled', ${sqlValue(now)} FROM batches WHERE ${settledAckTimeout};`,
      `UPDATE batches SET state='cancelled', updated_at=${sqlValue(now)} WHERE ${settledAckTimeout};`,
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
       SELECT id, 'needs_attention', 'cancelled', 'all_claims_finalized_reconciled', ${sqlValue(now)} FROM batches
       WHERE instance_id=${sqlValue(this.config.instance_id)} AND state='needs_attention' AND last_error='all_claims_finalized'
         AND claim_ids_json='[]' AND event_ids_json='[]';`,
      `UPDATE batches SET state='cancelled', updated_at=${sqlValue(now)}
       WHERE instance_id=${sqlValue(this.config.instance_id)} AND state='needs_attention' AND last_error='all_claims_finalized'
         AND claim_ids_json='[]' AND event_ids_json='[]';`,
      `UPDATE batches SET state='retry_wait', not_before=${sqlValue(now)}, lease_expires_at=NULL, last_error='dispatcher_recovered', updated_at=${sqlValue(now)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND state='dispatching' AND lease_expires_at IS NOT NULL AND lease_expires_at<${sqlValue(now)};`,
      `UPDATE outbox_attempts SET state='expired', finished_at=${sqlValue(now)}, error_class='dispatcher_recovered' WHERE state='leased' AND leased_until IS NOT NULL AND leased_until<${sqlValue(now)};`,
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
       SELECT b.id, b.state, 'needs_attention', 'ack_timeout', ${sqlValue(now)}
       FROM batches b JOIN outbox_attempts a ON a.batch_id=b.id
       WHERE b.instance_id=${sqlValue(this.config.instance_id)} AND b.state='dispatched'
         AND b.binding_generation IS NOT NULL AND a.state='accepted' AND a.attempt_no=b.attempt
         AND a.binding_generation=b.binding_generation AND a.finished_at IS NOT NULL
         AND a.finished_at<=${sqlValue(ackTimeoutCutoff)};`,
      `UPDATE batches SET state='needs_attention', lease_expires_at=NULL, last_error='ack_timeout', updated_at=${sqlValue(now)}
       WHERE instance_id=${sqlValue(this.config.instance_id)} AND state='dispatched'
         AND binding_generation IS NOT NULL
         AND EXISTS (
           SELECT 1 FROM outbox_attempts a WHERE a.batch_id=batches.id AND a.state='accepted'
             AND a.attempt_no=batches.attempt AND a.binding_generation=batches.binding_generation
             AND a.finished_at IS NOT NULL AND a.finished_at<=${sqlValue(ackTimeoutCutoff)}
         );`,
      `UPDATE delivery_correlations SET state='expired' WHERE state='outstanding' AND expires_at<${sqlValue(now)};`,
    ]);
    // Finalization during an in-flight request cannot detach its frozen refs.
    // After failure (or lease recovery above), retire that work before another
    // attempt is acquired. Also reconcile historical empty_batch incidents,
    // but never infer completion from an empty payload or a missing claim.
    const settledUnsent = `instance_id=${sqlValue(this.config.instance_id)}
      AND (state IN (${OPEN_BATCH_STATES.map(sqlValue).join(",")}) OR (state='needs_attention' AND last_error='empty_batch'))
      AND ${allClaimsFinalized}`;
    const settled = this.db.query<DbRow>(`SELECT id, state FROM batches WHERE ${settledUnsent};`);
    if (settled.length) this.db.transaction(settled.flatMap((batch) => [
      `UPDATE batches SET state='cancelled', lease_expires_at=NULL, updated_at=${sqlValue(now)}
       WHERE id=${sqlValue(batch.id)} AND state=${sqlValue(batch.state)} AND ${settledUnsent};`,
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
       SELECT ${sqlValue(batch.id)}, ${sqlValue(batch.state)}, 'cancelled', 'all_claims_finalized_reconciled', ${sqlValue(now)} WHERE changes()>0;`,
    ]));
    for (const row of timedOut) {
      const batch = this.getBatch(String(row.id));
      if (!batch || batch.state !== "needs_attention" || batch.last_error !== "ack_timeout") continue;
      if (batch.event_ids.some((eventId) => this.getEvent(eventId)?.source === "wakebridge.core")) continue;
      try {
        this.reportCoreIncident({
          kind: "ack_timeout",
          identity: { batch_id: batch.id },
          resource: { uri: `wakebridge://batches/${encodeURIComponent(batch.id)}` },
        });
      } catch {
        // Incident projection is optional observability. The durable timeout
        // transition remains authoritative and must not be rolled back or
        // make the dispatcher unavailable when projection itself fails.
      }
    }
  }

  private materializeDueActivityWatches(now: string): string[] {
    const watches = this.listActivityWatches().filter((watch) => {
      if (!watch.enabled || !watch.last_nonwake_activity_at) return false;
      if (watch.activity_lease_expires_at && watch.activity_lease_expires_at > now) return false;
      if (watch.fired_epoch !== watch.epoch) {
        return new Date(watch.last_nonwake_activity_at).getTime() + watch.idle_after_ms <= new Date(now).getTime();
      }
      return watch.mode === "repeat" && Boolean(watch.next_due_at) && watch.next_due_at! <= now;
    }).slice(0, 16);
    const created: string[] = [];
    for (const watch of watches) {
      const binding = this.getBinding(watch.attention_channel);
      if (!binding || binding.endpoint_id !== watch.endpoint_id || binding.generation !== watch.binding_generation) continue;
      const endpoint = this.getEndpoint(binding.endpoint_id);
      if (!endpoint || endpoint.lease_expires_at <= now || !endpoint.capabilities.session_activity_observable) continue;
      const sequence = watch.fired_epoch === watch.epoch ? watch.sequence + 1 : 1;
      const identity = `${this.config.instance_id}:${watch.attention_channel}:${binding.generation}:${watch.epoch}:${sequence}`;
      const eventId = `evt_${sha256(`activity-watch:${identity}`).slice(0, 32)}`;
      const claimId = `ac_${sha256(`activity-watch-claim:${identity}`).slice(0, 32)}`;
      const dedupeKey = `activity-watch:${watch.attention_channel}:${binding.generation}:${watch.epoch}:${sequence}`;
      const resource = { uri: `wakebridge://activity-watch/${encodeURIComponent(watch.attention_channel)}/${watch.epoch}/${sequence}` };
      const initialPredicate = `(fired_epoch IS NULL OR fired_epoch<>epoch) AND sequence=${watch.sequence}`;
      const repeatPredicate = `fired_epoch=epoch AND mode='repeat' AND sequence=${watch.sequence} AND next_due_at IS NOT NULL AND next_due_at<=${sqlValue(now)}`;
      const activeChannel = `EXISTS(SELECT 1 FROM attention_channels c WHERE c.instance_id=${sqlValue(this.config.instance_id)} AND c.attention_channel=${sqlValue(watch.attention_channel)} AND c.deleted_at IS NULL)`;
      const predicate = `instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(watch.attention_channel)} AND enabled=1 AND epoch=${watch.epoch} AND ${watch.fired_epoch === watch.epoch ? repeatPredicate : initialPredicate} AND endpoint_id=${sqlValue(binding.endpoint_id)} AND binding_generation=${binding.generation} AND ${activeChannel}`;
      this.db.transaction([
        `INSERT OR IGNORE INTO events(id, instance_id, owner_id, source, type, schema_version, occurred_at, received_at, dedupe_key, coalesce_key, priority_hint, attention_channel_hint, actor_ref, resource_json, metadata_json, payload_preview, matched_policy_id, matched_policy_version)
         SELECT ${sqlValue(eventId)}, ${sqlValue(this.config.instance_id)}, ${sqlValue(this.config.owner_id)}, 'wakebridge.core', 'channel.inactive', 1, ${sqlValue(now)}, ${sqlValue(now)}, ${sqlValue(dedupeKey)}, ${sqlValue(watch.attention_channel)}, 'normal', ${sqlValue(watch.attention_channel)}, ${sqlValue(this.config.owner_id)}, ${sqlJson(resource)}, ${sqlJson({ origin: "inactivity_watch", reason_code: "host_inactive", epoch: watch.epoch, idle_after_ms: watch.idle_after_ms, mode: watch.mode, sequence })}, NULL, 'inactivity_watch', 1 FROM activity_watches WHERE ${predicate};`,
        `INSERT OR IGNORE INTO event_status(event_id, state, updated_at) SELECT ${sqlValue(eventId)}, 'matched', ${sqlValue(now)} FROM events WHERE id=${sqlValue(eventId)} AND ${activeChannel};`,
        `INSERT INTO event_transitions(event_id, from_state, to_state, reason, at) SELECT ${sqlValue(eventId)}, NULL, 'matched', 'inactivity_watch', ${sqlValue(now)} WHERE EXISTS (SELECT 1 FROM events WHERE id=${sqlValue(eventId)}) AND NOT EXISTS (SELECT 1 FROM event_transitions WHERE event_id=${sqlValue(eventId)}) AND ${activeChannel};`,
        `INSERT OR IGNORE INTO claims(id, instance_id, origin, event_ids_json, resource_json, policy_id, policy_version, attention_channel, eligible_after, expires_at, defer_while_presence, state, reason_code, note, created_at, updated_at, snooze_until, consumed_result_json, dismissed_reason)
         SELECT ${sqlValue(claimId)}, ${sqlValue(this.config.instance_id)}, 'inactivity_watch', ${sqlJson([eventId])}, ${sqlJson(resource)}, 'inactivity_watch', 1, ${sqlValue(watch.attention_channel)}, ${sqlValue(now)}, NULL, 1, 'pending', 'host_inactive', NULL, ${sqlValue(now)}, ${sqlValue(now)}, NULL, NULL, NULL FROM events WHERE id=${sqlValue(eventId)} AND ${activeChannel};`,
        `INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at) SELECT ${sqlValue(claimId)}, NULL, 'pending', 'inactivity_watch', ${sqlValue(now)} WHERE EXISTS (SELECT 1 FROM claims WHERE id=${sqlValue(claimId)}) AND NOT EXISTS (SELECT 1 FROM claim_transitions WHERE claim_id=${sqlValue(claimId)}) AND ${activeChannel};`,
        `UPDATE activity_watches SET fired_epoch=epoch, sequence=${sequence}, last_triggered_at=${sqlValue(now)}, next_due_at=${sqlValue(watch.mode === "repeat" ? addMs(now, watch.repeat_after_ms) : null)}, activity_lease_expires_at=NULL, updated_at=${sqlValue(now)} WHERE ${predicate} AND EXISTS (SELECT 1 FROM claims WHERE id=${sqlValue(claimId)});`,
      ]);
      const after = this.getActivityWatch(watch.attention_channel);
      if (after?.sequence === sequence && this.getClaim(claimId)) created.push(claimId);
    }
    return created;
  }

  /** Advance up to 1000 due claims and freeze up to 1000 eligible claims per pass. */
  tick(): TickResult {
    const nowDate = this.now();
    const now = nowDate.toISOString();
    const result: TickResult = { inactivity_claims_created: this.materializeDueActivityWatches(now), expired_claims: [], deferred_claims: [], eligible_claims: [], batches_created: [], batches_updated: [] };
    const claims = this.db.query<Pick<AttentionClaim, "id" | "state" | "policy_id" | "policy_version" | "attention_channel" | "eligible_after" | "expires_at" | "defer_while_presence">>(
      `SELECT id, state, policy_id, policy_version, attention_channel, eligible_after, expires_at, defer_while_presence
       FROM claims WHERE instance_id=${sqlValue(this.config.instance_id)} AND state IN (${ACTIVE_CLAIM_STATES.map(sqlValue).join(",")})
         AND (eligible_after<=${sqlValue(now)} OR expires_at<=${sqlValue(now)})
       ORDER BY created_at ASC, id ASC LIMIT 1000;`,
    );
    const transitions: string[] = [];
    for (const claim of claims) {
      if (!ACTIVE_CLAIM_STATES.includes(claim.state)) continue;
      if (claim.expires_at && claim.expires_at <= now) {
        transitions.push(...this.claimStateSql(claim, "expired", now, "expiry"));
        transitions.push(...this.claimEventStateSql(claim.id, "expired", "expired", now, "claim_expired"));
        result.expired_claims.push(claim.id);
        continue;
      }
      if (claim.eligible_after > now) continue;
      const policy = this.policy(claim.policy_id, claim.policy_version) ?? normalizePolicy(DEFAULT_POLICY);
      if (policy && policy.delivery.quiet_hours_policy !== "bypass" && quietWindowAt(nowDate, this.config.quiet_hours, this.config.timezone ?? "UTC")) {
        const end = quietEndAfter(nowDate, this.config.quiet_hours, this.config.timezone ?? "UTC").toISOString();
        transitions.push(...this.claimDeferSql(claim, end, now, "quiet_hours"));
        result.deferred_claims.push(claim.id);
        continue;
      }
      if (claim.defer_while_presence) {
        const presence = this.presenceActive(claim.attention_channel, now);
        if (presence) {
          transitions.push(...this.claimDeferSql(claim, presence.expires_at, now, "foreground_presence"));
          result.deferred_claims.push(claim.id);
          continue;
        }
      }
      if (claim.state !== "eligible") {
        transitions.push(...this.claimStateSql(claim, "eligible", now, "gates_open"));
        result.eligible_claims.push(claim.id);
      }
    }
    if (transitions.length) this.db.transaction(transitions);
    if (result.expired_claims.length) {
      const expired = new Set(this.db.query<DbRow>(`SELECT id FROM claims WHERE state='expired' AND id IN (${result.expired_claims.map(sqlValue).join(",")});`).map((row) => String(row.id)));
      result.expired_claims = result.expired_claims.filter((claimId) => expired.has(claimId));
    }

    const eligible = this.listClaims({ state: "eligible", limit: 1000 });
    const openByKey = new Map<string, WakeBatch | null>();
    const batchStatements: string[] = [];
    const appendedClaims: Array<{ batchId: string; claimId: string }> = [];
    for (const claim of eligible) {
      const policy = this.policy(claim.policy_id, claim.policy_version) ?? normalizePolicy(DEFAULT_POLICY);
      const event = this.getEvent(claim.event_ids[0]) ?? undefined;
      const coalesce = this.coalesceKey(policy, claim, event);
      const key = this.batchKey(claim.policy_id, claim.policy_version, claim.attention_channel, coalesce);
      const maxEvents = Math.max(1, Math.floor(policy.batch?.max_events ?? 20));
      let batch = openByKey.get(key);
      if (batch === undefined) {
        const row = this.db.query<DbRow>(`SELECT * FROM batches
          WHERE instance_id=${sqlValue(this.config.instance_id)} AND policy_id=${sqlValue(claim.policy_id)} AND policy_version=${claim.policy_version}
            AND attention_channel=${sqlValue(claim.attention_channel)} AND coalesce_key IS ${sqlValue(coalesce)}
            AND state IN (${OPEN_BATCH_STATES.map(sqlValue).join(",")}) AND deadline>${sqlValue(now)}
            AND json_array_length(claim_ids_json)<${maxEvents}
          ORDER BY created_at DESC, rowid DESC LIMIT 1;`)[0];
        batch = row ? this.batchFromRow(row) : null;
        openByKey.set(key, batch);
      }
      if (batch && batch.claim_ids.length < maxEvents && batch.deadline && batch.deadline > now) {
        // Both arrays may have grown since planning. Append the current
        // claim's refs only while the destination can still accept it.
        batchStatements.push(`UPDATE batches SET claim_ids_json=json_insert(claim_ids_json, '$[#]', ${sqlValue(claim.id)}),
          event_ids_json=(SELECT json_group_array(value) FROM (
            SELECT value FROM json_each(batches.event_ids_json)
            UNION ALL
            SELECT j.value FROM claims c, json_each(c.event_ids_json) j WHERE c.id=${sqlValue(claim.id)}
              AND NOT EXISTS (SELECT 1 FROM json_each(batches.event_ids_json) existing WHERE existing.value=j.value)
          )), updated_at=${sqlValue(now)}
          WHERE id=${sqlValue(batch.id)} AND state IN (${OPEN_BATCH_STATES.map(sqlValue).join(",")})
            AND deadline>${sqlValue(now)} AND json_array_length(claim_ids_json)<${maxEvents}
            AND NOT EXISTS (SELECT 1 FROM json_each(batches.claim_ids_json) WHERE value=${sqlValue(claim.id)})
            AND EXISTS (SELECT 1 FROM claims WHERE id=${sqlValue(claim.id)} AND state='eligible');`);
        batchStatements.push(`INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) SELECT id, state, state, 'append_claim', ${sqlValue(now)} FROM batches WHERE id=${sqlValue(batch.id)} AND changes()>0;`);
        batch.claim_ids = arrayWithoutDuplicates([...batch.claim_ids, claim.id]);
        appendedClaims.push({ batchId: batch.id, claimId: claim.id });
      } else {
        const windowMs = assertFiniteMs(policy.batch?.window_ms, "batch.window_ms", this.config.batch_window_ms!);
        const batchId = id("wb");
        batch = {
          id: batchId,
          instance_id: this.config.instance_id,
          policy_id: claim.policy_id,
          policy_version: claim.policy_version,
          attention_channel: claim.attention_channel,
          claim_ids: [claim.id],
          event_ids: [...claim.event_ids],
          coalesce_key: coalesce,
          state: "pending",
          not_before: now,
          deadline: addMs(nowDate, windowMs),
          attempt: 0,
          binding_generation: null,
          created_at: now,
          updated_at: now,
          lease_expires_at: null,
          last_error: null,
        };
        batchStatements.push(this.batchInsertSql(batch, claim.id));
        batchStatements.push(`INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) SELECT ${sqlValue(batchId)}, NULL, 'pending', 'freeze', ${sqlValue(now)} WHERE changes()>0;`);
        openByKey.set(key, batch);
        result.batches_created.push(batchId);
      }
      // Keep this CAS adjacent to the conditional batch transition. If the
      // insertion/append lost a race, the claim stays eligible for the next tick.
      batchStatements.push(`UPDATE claims SET state='batched', updated_at=${sqlValue(now)} WHERE id=${sqlValue(claim.id)} AND state='eligible' AND changes()>0;`);
      batchStatements.push(`INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at) SELECT ${sqlValue(claim.id)}, 'eligible', 'batched', ${sqlValue(`batch:${batch.id}`)}, ${sqlValue(now)} WHERE changes()>0;`);
      batchStatements.push(...this.claimEventStateSql(claim.id, "batched", "batched", now, `batch:${batch.id}`));
    }
    if (batchStatements.length) {
      this.db.transaction(batchStatements);
      const batchIds = arrayWithoutDuplicates([...result.batches_created, ...appendedClaims.map((candidate) => candidate.batchId)]);
      const current = new Map(this.db.query<DbRow>(`SELECT id, claim_ids_json FROM batches WHERE id IN (${batchIds.map(sqlValue).join(",")});`)
        .map((row) => [String(row.id), jsonRecord<string[]>(row.claim_ids_json, [])]));
      result.batches_created = result.batches_created.filter((batchId) => current.has(batchId));
      result.batches_updated = appendedClaims.filter(({ batchId, claimId }) => current.get(batchId)?.includes(claimId)).map(({ batchId }) => batchId);
    }
    return result;
  }

  private claimStateSql(claim: Pick<AttentionClaim, "id" | "state">, state: ClaimState, now: string, reason: string): string[] {
    return [
      `UPDATE claims SET state=${sqlValue(state)}, updated_at=${sqlValue(now)} WHERE id=${sqlValue(claim.id)} AND state=${sqlValue(claim.state)};`,
      `INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at) SELECT ${sqlValue(claim.id)}, ${sqlValue(claim.state)}, ${sqlValue(state)}, ${sqlValue(reason)}, ${sqlValue(now)} WHERE changes()>0;`,
    ];
  }

  private claimDeferSql(claim: Pick<AttentionClaim, "id" | "state">, eligibleAfter: string, now: string, reason: string): string[] {
    return [
      `UPDATE claims SET state='deferred', eligible_after=${sqlValue(eligibleAfter)}, updated_at=${sqlValue(now)} WHERE id=${sqlValue(claim.id)} AND state IN ('pending','deferred','eligible');`,
      `INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at) SELECT ${sqlValue(claim.id)}, ${sqlValue(claim.state)}, 'deferred', ${sqlValue(reason)}, ${sqlValue(now)} WHERE changes()>0;`,
    ];
  }

  private claimEventStateSql(claimId: string, claimState: ClaimState, state: EventState, now: string, reason: string, condition = "1"): string[] {
    // Read membership and the audit's from_state inside the writing transaction.
    // A failed claim CAS must not finalize or batch its events independently.
    const membership = `SELECT j.value FROM claims c, json_each(c.event_ids_json) j WHERE c.id=${sqlValue(claimId)} AND c.state=${sqlValue(claimState)} AND ${condition}`;
    return [
      `INSERT INTO event_transitions(event_id, from_state, to_state, reason, at) SELECT event_id, state, ${sqlValue(state)}, ${sqlValue(reason)}, ${sqlValue(now)} FROM event_status WHERE event_id IN (${membership}) AND state<>${sqlValue(state)};`,
      `UPDATE event_status SET state=${sqlValue(state)}, updated_at=${sqlValue(now)} WHERE event_id IN (${membership}) AND state<>${sqlValue(state)};`,
    ];
  }

  private coalesceKey(policy: InstalledPolicyRule, claim: AttentionClaim, event?: WakeEvent): string | null {
    const by = policy.batch?.coalesce_by ?? "coalesce_key";
    if (by === "none") return claim.id;
    if (by === "type") return event?.type ?? claim.id;
    if (by === "source") return event?.source ?? claim.id;
    if (by.startsWith("metadata.") && event) return String(getPath(event.metadata, by.slice("metadata.".length)) ?? claim.id);
    return event?.coalesce_key ?? claim.id;
  }

  private batchKey(policyId: string, version: number, channel: string, coalesce: string | null): string {
    return `${policyId}:${version}:${channel}:${coalesce ?? ""}`;
  }

  private batchInsertSql(batch: WakeBatch, claimId: string): string {
    return `INSERT INTO batches(id, instance_id, policy_id, policy_version, attention_channel, claim_ids_json, event_ids_json, coalesce_key, state, not_before, deadline, attempt, binding_generation, created_at, updated_at, lease_expires_at, last_error)
      SELECT ${sqlValue(batch.id)}, ${sqlValue(batch.instance_id)}, ${sqlValue(batch.policy_id)}, ${batch.policy_version}, ${sqlValue(batch.attention_channel)}, ${sqlJson(batch.claim_ids)}, event_ids_json, ${sqlValue(batch.coalesce_key)}, ${sqlValue(batch.state)}, ${sqlValue(batch.not_before)}, ${sqlValue(batch.deadline)}, ${batch.attempt}, NULL, ${sqlValue(batch.created_at)}, ${sqlValue(batch.updated_at)}, NULL, NULL FROM claims WHERE id=${sqlValue(claimId)} AND state='eligible';`;
  }

  getBatch(batchId: string): WakeBatch | null {
    const row = this.db.query<DbRow>(`SELECT * FROM batches WHERE id=${sqlValue(batchId)} AND instance_id=${sqlValue(this.config.instance_id)} LIMIT 1;`)[0];
    return row ? this.batchFromRow(row) : null;
  }

  /** Same paging contract as listEvents: oldest-first unless `newest_first`. */
  listBatches(filters: { states?: BatchState[]; channel?: string; limit?: number; newest_first?: boolean } = {}): WakeBatch[] {
    const clauses = [`instance_id=${sqlValue(this.config.instance_id)}`];
    if (filters.states?.length) clauses.push(`state IN (${filters.states.map(sqlValue).join(",")})`);
    if (filters.channel) clauses.push(`attention_channel=${sqlValue(filters.channel)}`);
    const limit = Math.min(100_000, Math.max(1, Math.floor(filters.limit ?? 1000)));
    const order = filters.newest_first ? "DESC" : "ASC";
    const batches = this.db.query<DbRow>(`SELECT * FROM batches WHERE ${clauses.join(" AND ")} ORDER BY created_at ${order} LIMIT ${limit};`).map((row) => this.batchFromRow(row));
    return filters.newest_first ? batches.reverse() : batches;
  }

  /** Every batch that references the claim, oldest first.  A snoozed claim can appear in several. */
  private batchesContainingClaim(claimId: string, states?: BatchState[]): WakeBatch[] {
    const clauses = [
      `instance_id=${sqlValue(this.config.instance_id)}`,
      `EXISTS (SELECT 1 FROM json_each(batches.claim_ids_json) WHERE json_each.value=${sqlValue(claimId)})`,
    ];
    if (states?.length) clauses.push(`state IN (${states.map(sqlValue).join(",")})`);
    return this.db.query<DbRow>(`SELECT * FROM batches WHERE ${clauses.join(" AND ")} ORDER BY created_at ASC;`).map((row) => this.batchFromRow(row));
  }

  private batchFromRow(row: DbRow): WakeBatch {
    return {
      id: String(row.id),
      instance_id: String(row.instance_id),
      policy_id: String(row.policy_id),
      policy_version: Number(row.policy_version),
      attention_channel: String(row.attention_channel),
      claim_ids: jsonRecord<string[]>(row.claim_ids_json, []),
      event_ids: jsonRecord<string[]>(row.event_ids_json, []),
      coalesce_key: row.coalesce_key == null ? null : String(row.coalesce_key),
      state: String(row.state) as BatchState,
      not_before: String(row.not_before),
      deadline: row.deadline == null ? null : String(row.deadline),
      attempt: Number(row.attempt),
      binding_generation: row.binding_generation == null ? null : Number(row.binding_generation),
      created_at: String(row.created_at),
      updated_at: String(row.updated_at),
      lease_expires_at: row.lease_expires_at == null ? null : String(row.lease_expires_at),
      last_error: row.last_error == null ? null : String(row.last_error),
    };
  }

  private resolveBinding(channel: string, now: string): { binding: Binding; endpoint: Endpoint } | null {
    const row = this.db.query<DbRow>(`SELECT b.*, e.* FROM bindings b JOIN endpoints e ON e.id=b.endpoint_id WHERE b.instance_id=${sqlValue(this.config.instance_id)} AND b.attention_channel=${sqlValue(channel)} AND e.lease_expires_at>${sqlValue(now)} LIMIT 1;`)[0];
    if (!row) return null;
    return {
      binding: { instance_id: String(row.instance_id), attention_channel: String(row.attention_channel), endpoint_id: String(row.endpoint_id), generation: Number(row.generation), bound_at: String(row.bound_at) },
      endpoint: this.endpointFromRow(row),
    };
  }

  private chooseTransport(_batch: WakeBatch, endpoint: Endpoint): WakeTransport | null {
    const routeKinds = [...endpoint.routes].sort((a, b) => (b.priority ?? 0) - (a.priority ?? 0)).map((route) => route.kind);
    for (const kind of routeKinds) {
      const transport = this.transports.get(kind);
      if (transport && (endpoint.capabilities.cold_push !== false || transport.capabilities?.warm_resume)) return transport;
    }
    return null;
  }

  private buildPayload(batch: WakeBatch, binding: Binding): WakePayload {
    // A batch freezes exact claim/event references.  Resolve those references
    // directly rather than through a bounded event index: an older event must
    // not disappear from a wake payload merely because the instance has more
    // than 1,000 retained events.
    // Each database call starts sqlite3. Hydrate the exact sets in two reads,
    // then restore batch order (including repeated claim references) in memory.
    const claimsById = new Map(this.db.query<DbRow>(`SELECT * FROM claims
      WHERE instance_id=${sqlValue(this.config.instance_id)} AND id IN (SELECT value FROM json_each(${sqlJson(batch.claim_ids)}));`)
      .map((row) => { const claim = this.claimFromRow(row); return [claim.id, claim] as const; }));
    const claims = batch.claim_ids.map((claimId) => claimsById.get(claimId))
      .filter((claim): claim is AttentionClaim => Boolean(claim) && claim!.state === "batched");
    const eventIds = arrayWithoutDuplicates(claims.flatMap((claim) => claim.event_ids));
    // Payload refs need no metadata or previews; do not accumulate those large
    // columns in one sqlite3 JSON response when hydrating a full batch.
    const events = new Map(this.db.query<Pick<WakeEvent, "id" | "source" | "type">>(`SELECT e.id, e.source, e.type FROM events e JOIN event_status s ON s.event_id=e.id
      WHERE e.instance_id=${sqlValue(this.config.instance_id)} AND e.id IN (SELECT value FROM json_each(${sqlJson(eventIds)}));`)
      .map((event) => [event.id, event] as const));
    return {
      schema_version: 1,
      instance_id: this.config.instance_id,
      wake_batch_id: batch.id,
      attention_channel: batch.attention_channel,
      claim_refs: claims.map((claim) => {
        const event = events.get(claim.event_ids[0]);
        return {
          claim_id: claim.id,
          event_id: event?.id,
          source: event?.source ?? claim.origin,
          type: event?.type ?? "attention.claim",
          resource: claim.resource,
          origin: claim.origin,
          reason_code: claim.reason_code,
          ...(claim.origin === "self_commitment" && claim.note != null ? { note: claim.note } : {}),
        };
      }),
      binding_generation: binding.generation,
    };
  }

  private markNoEndpoint(batch: WakeBatch, now: string): DispatchResult {
    if (batch.state === "waiting_for_endpoint") {
      // A waiting item with no endpoint still gets a turn, but must not keep
      // the oldest timestamp forever or create another audit on every sweep.
      this.db.exec(`UPDATE batches SET updated_at=${sqlValue(now)}
        WHERE id=${sqlValue(batch.id)} AND instance_id=${sqlValue(this.config.instance_id)}
          AND state='waiting_for_endpoint' AND updated_at=${sqlValue(batch.updated_at)};`);
      return { batch_id: batch.id, status: "waiting_for_endpoint" };
    }
    this.db.transaction([
      `UPDATE batches SET state='waiting_for_endpoint', updated_at=${sqlValue(now)}, last_error='no_current_binding' WHERE id=${sqlValue(batch.id)} AND state IN ('pending','retry_wait');`,
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) SELECT ${sqlValue(batch.id)}, state, 'waiting_for_endpoint', 'no_endpoint', ${sqlValue(now)} FROM batches WHERE id=${sqlValue(batch.id)} AND changes()>0;`,
    ]);
    return { batch_id: batch.id, status: "waiting_for_endpoint" };
  }

  private dispatchClaimsSnapshotSql(snapshots: Array<[string, AttentionClaim | null]>): string {
    const expected = snapshots.map(([claimId, claim]) => claim ? {
      id: claimId, state: claim.state, policy_id: claim.policy_id, policy_version: claim.policy_version,
      attention_channel: claim.attention_channel, event_ids: claim.event_ids, eligible_after: claim.eligible_after,
      expires_at: claim.expires_at ?? null, defer_while_presence: claim.defer_while_presence, updated_at: claim.updated_at,
    } : { id: claimId, missing: true });
    // One JSON relation keeps the fence below SQLite's expression-depth limit
    // even for the public maximum of 1000 claims in one batch.
    return `NOT EXISTS (SELECT 1 FROM json_each(${sqlJson(expected)}) s LEFT JOIN claims c
      ON c.id=json_extract(s.value, '$.id') AND c.instance_id=${sqlValue(this.config.instance_id)}
      WHERE CASE WHEN json_extract(s.value, '$.missing') THEN c.id IS NOT NULL ELSE c.id IS NULL
        OR c.state IS NOT json_extract(s.value, '$.state') OR c.policy_id IS NOT json_extract(s.value, '$.policy_id')
        OR c.policy_version IS NOT json_extract(s.value, '$.policy_version') OR c.attention_channel IS NOT json_extract(s.value, '$.attention_channel')
        OR c.event_ids_json IS NOT json_extract(s.value, '$.event_ids') OR c.eligible_after IS NOT json_extract(s.value, '$.eligible_after')
        OR c.expires_at IS NOT json_extract(s.value, '$.expires_at') OR c.defer_while_presence IS NOT json_extract(s.value, '$.defer_while_presence')
        OR c.updated_at IS NOT json_extract(s.value, '$.updated_at') END)`;
  }

  private dispatchPresenceSnapshotSql(channel: string, presence: PresenceLease | null, now: string): string {
    const active = `SELECT 1 FROM presence_leases p JOIN bindings b
      ON b.instance_id=p.instance_id AND b.attention_channel=p.attention_channel
        AND b.endpoint_id=p.endpoint_id AND b.generation=p.binding_generation
      JOIN endpoints e ON e.id=p.endpoint_id AND e.instance_id=p.instance_id
      WHERE p.instance_id=${sqlValue(this.config.instance_id)} AND p.attention_channel=${sqlValue(channel)}
        AND p.expires_at>${sqlValue(now)} AND e.lease_expires_at>${sqlValue(now)}`;
    return presence
      ? `EXISTS (${active} AND p.endpoint_id=${sqlValue(presence.endpoint_id)} AND p.binding_generation=${presence.binding_generation}
          AND p.renewed_at=${sqlValue(presence.renewed_at)} AND p.expires_at=${sqlValue(presence.expires_at)})`
      : `NOT EXISTS (${active})`;
  }

  /** Recheck only unsent work. An acquired/accepted batch is never retracted by a gate. */
  private recheckDispatchGates(batch: WakeBatch, nowDate: Date): { batch: WakeBatch; fenceSql: string; quietActive: boolean } | null {
    const now = nowDate.toISOString();
    const claims = this.db.query<DbRow>(`SELECT c.* FROM json_each(${sqlJson(batch.claim_ids)}) j JOIN claims c ON c.id=j.value
      WHERE c.instance_id=${sqlValue(this.config.instance_id)};`).map((row) => this.claimFromRow(row));
    const byId = new Map(claims.map((claim) => [claim.id, claim]));
    const snapshots = new Map(batch.claim_ids.map((claimId) => [claimId, byId.get(claimId) ?? null]));
    const presences = new Map<string, PresenceLease | null>();
    const quietActive = quietWindowAt(nowDate, this.config.quiet_hours, this.config.timezone ?? "UTC");
    const policies = new Map<string, InstalledPolicyRule>();
    let quietEnd: string | undefined;
    const gated: Array<{ claim: AttentionClaim; state: "expired" | "deferred"; eligibleAfter: string; reason: string }> = [];
    for (const claim of snapshots.values()) {
      if (!claim || claim.state !== "batched") continue;
      if (claim.expires_at && claim.expires_at <= now) {
        gated.push({ claim, state: "expired", eligibleAfter: claim.eligible_after, reason: "expiry" });
        continue;
      }
      if (quietActive) {
        const policyKey = `${claim.policy_id}:${claim.policy_version}`;
        const policy = policies.get(policyKey) ?? this.policy(claim.policy_id, claim.policy_version) ?? normalizePolicy(DEFAULT_POLICY);
        policies.set(policyKey, policy);
        if (policy.delivery.quiet_hours_policy !== "bypass") {
          quietEnd ??= quietEndAfter(nowDate, this.config.quiet_hours, this.config.timezone ?? "UTC").toISOString();
          gated.push({ claim, state: "deferred", eligibleAfter: quietEnd, reason: "quiet_hours" });
          continue;
        }
      }
      if (claim.defer_while_presence && !presences.has(claim.attention_channel)) {
        presences.set(claim.attention_channel, this.presenceActive(claim.attention_channel, now));
      }
      const presence = claim.defer_while_presence ? presences.get(claim.attention_channel) : null;
      if (presence) gated.push({ claim, state: "deferred", eligibleAfter: presence.expires_at, reason: "foreground_presence" });
    }
    const presenceFence = [...presences].map(([channel, presence]) => this.dispatchPresenceSnapshotSql(channel, presence, now)).join(" AND ") || "1";
    const removed = new Set(gated.map(({ claim }) => claim.id));
    const claimIds = batch.claim_ids.filter((claimId) => !removed.has(claimId));
    const removedEvents = new Set(gated.flatMap(({ claim }) => claim.event_ids));
    const retainedEvents = new Set(claimIds.flatMap((claimId) => snapshots.get(claimId)?.event_ids ?? []));
    const eventIds = batch.event_ids.filter((eventId) => !removedEvents.has(eventId) || retainedEvents.has(eventId));
    if (gated.length) {
      const inputFence = this.dispatchClaimsSnapshotSql([...snapshots]);
      const nextState = claimIds.length ? batch.state : "cancelled";
      const reasons = arrayWithoutDuplicates(gated.map(({ reason }) => reason));
      const reason = reasons.length === 1 ? reasons[0] : "dispatch_gates";
      const affectedIds = gated.map(({ claim }) => sqlValue(claim.id)).join(",");
      const stateCases = gated.map(({ claim, state }) => `WHEN ${sqlValue(claim.id)} THEN ${sqlValue(state)}`).join(" ");
      const eligibleCases = gated.map(({ claim, eligibleAfter }) => `WHEN ${sqlValue(claim.id)} THEN ${sqlValue(eligibleAfter)}`).join(" ");
      const reasonCases = gated.map(({ claim, reason: claimReason }) => `WHEN ${sqlValue(claim.id)} THEN ${sqlValue(claimReason)}`).join(" ");
      // The first CAS fences the whole snapshot. With the write lock held,
      // changes() then carries that success through the remaining statements.
      // A concurrent append/state change skips this sweep without losing refs.
      const statements = [
        `UPDATE batches SET claim_ids_json=${sqlJson(claimIds)}, event_ids_json=${sqlJson(eventIds)}, state=${sqlValue(nextState)},
           updated_at=${sqlValue(now)}, lease_expires_at=NULL, last_error=${claimIds.length ? "last_error" : sqlValue(reason)}
         WHERE id=${sqlValue(batch.id)} AND instance_id=${sqlValue(this.config.instance_id)}
           AND state=${sqlValue(batch.state)} AND state IN (${OPEN_BATCH_STATES.map(sqlValue).join(",")}) AND attempt=${batch.attempt}
           AND claim_ids_json=${sqlJson(batch.claim_ids)} AND event_ids_json=${sqlJson(batch.event_ids)}
           AND policy_id=${sqlValue(batch.policy_id)} AND policy_version=${batch.policy_version}
           AND attention_channel=${sqlValue(batch.attention_channel)} AND not_before=${sqlValue(batch.not_before)} AND deadline IS ${sqlValue(batch.deadline)}
           AND (${inputFence}) AND (${presenceFence});`,
        `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
         SELECT ${sqlValue(batch.id)}, ${sqlValue(batch.state)}, ${sqlValue(nextState)}, ${sqlValue(reason)}, ${sqlValue(now)} WHERE changes()>0;`,
        `INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at)
         SELECT id, state, CASE id ${stateCases} END, CASE id ${reasonCases} END, ${sqlValue(now)}
         FROM claims WHERE id IN (${affectedIds}) AND changes()>0;`,
        `UPDATE claims SET state=CASE id ${stateCases} END, eligible_after=CASE id ${eligibleCases} END, updated_at=${sqlValue(now)}
         WHERE id IN (${affectedIds}) AND changes()>0;`,
      ];
      const expiredEvents = arrayWithoutDuplicates(gated.filter(({ state }) => state === "expired").flatMap(({ claim }) => claim.event_ids));
      if (expiredEvents.length) statements.push(
        `INSERT INTO event_transitions(event_id, from_state, to_state, reason, at)
         SELECT event_id, state, 'expired', 'claim_expired', ${sqlValue(now)} FROM event_status
         WHERE event_id IN (${expiredEvents.map(sqlValue).join(",")}) AND state<>'expired' AND changes()>0;`,
        `UPDATE event_status SET state='expired', updated_at=${sqlValue(now)}
         WHERE event_id IN (${expiredEvents.map(sqlValue).join(",")}) AND state<>'expired' AND changes()>0;`,
      );
      this.db.transaction(statements);
    }
    const refreshed = this.getBatch(batch.id);
    if (!refreshed || JSON.stringify(refreshed.claim_ids) !== JSON.stringify(claimIds)
      || JSON.stringify(refreshed.event_ids) !== JSON.stringify(eventIds)) return null;
    const remainingFence = this.dispatchClaimsSnapshotSql(claimIds.map((claimId) => [claimId, snapshots.get(claimId) ?? null]));
    return { batch: refreshed, fenceSql: `(${remainingFence}) AND (${presenceFence})`, quietActive };
  }

  private acquireBatch(batch: WakeBatch, binding: Binding, now: string, gateFence: string): { batch: WakeBatch; attemptId: string } | null {
    const attemptId = id("attempt");
    const leaseUntil = addMs(now, this.config.dispatch_lease_ms!);
    const fromState = batch.state;
    const rows = this.db.query<DbRow>([
      "BEGIN IMMEDIATE;",
      `UPDATE batches SET state='dispatching', attempt=attempt+1, binding_generation=${binding.generation}, lease_expires_at=${sqlValue(leaseUntil)}, updated_at=${sqlValue(now)}, last_error=${sqlValue(attemptId)}
       WHERE id=${sqlValue(batch.id)} AND instance_id=${sqlValue(this.config.instance_id)} AND state IN ('pending','retry_wait','waiting_for_endpoint')
       AND not_before<=${sqlValue(now)} AND (state='waiting_for_endpoint' OR deadline IS NULL OR deadline<=${sqlValue(now)})
       AND claim_ids_json=${sqlJson(batch.claim_ids)} AND event_ids_json=${sqlJson(batch.event_ids)} AND (${gateFence})
       AND EXISTS (SELECT 1 FROM attention_channels c WHERE c.instance_id=batches.instance_id AND c.attention_channel=batches.attention_channel AND c.deleted_at IS NULL)
       AND NOT EXISTS (SELECT 1 FROM json_each(batches.claim_ids_json) j JOIN claims c ON c.id=j.value
         WHERE c.state='batched' AND c.expires_at IS NOT NULL AND c.expires_at<=${sqlValue(now)});`,
      `INSERT INTO outbox_attempts(id, batch_id, attempt_no, state, transport_kind, endpoint_id, binding_generation, leased_until, started_at)
       SELECT ${sqlValue(attemptId)}, id, attempt, 'leased', NULL, ${sqlValue(binding.endpoint_id)}, ${binding.generation}, ${sqlValue(leaseUntil)}, ${sqlValue(now)} FROM batches
       WHERE id=${sqlValue(batch.id)} AND state='dispatching' AND lease_expires_at=${sqlValue(leaseUntil)} AND last_error=${sqlValue(attemptId)};`,
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
       SELECT ${sqlValue(batch.id)}, ${sqlValue(fromState)}, 'dispatching', 'worker_lease', ${sqlValue(now)} FROM batches WHERE id=${sqlValue(batch.id)} AND state='dispatching' AND lease_expires_at=${sqlValue(leaseUntil)} AND last_error=${sqlValue(attemptId)};`,
      `SELECT id, instance_id, policy_id, policy_version, attention_channel, claim_ids_json, event_ids_json, coalesce_key, state, not_before, deadline, attempt, binding_generation, created_at, updated_at, lease_expires_at, last_error FROM batches WHERE id=${sqlValue(batch.id)} AND state='dispatching' AND lease_expires_at=${sqlValue(leaseUntil)} AND last_error=${sqlValue(attemptId)};`,
      "COMMIT;",
    ].join("\n"));
    if (!rows.length) return null;
    return { batch: this.batchFromRow({ ...rows[0], state: "dispatching" }), attemptId };
  }

  private bindingStillCurrent(binding: Binding, now: string): boolean {
    const current = this.resolveBinding(binding.attention_channel, now);
    return Boolean(current && current.binding.endpoint_id === binding.endpoint_id && current.binding.generation === binding.generation);
  }

  private finalizeAttempt(attemptId: string, batch: WakeBatch, binding: Binding, result: TransportResult, transportKind: string, now: string): DispatchResult {
    const accepted = result.accepted === true;
    const hostAttested = Boolean(this.db.query<DbRow>(`SELECT 1 AS present FROM delivery_correlations
      WHERE attempt_id=${sqlValue(attemptId)} AND state='consumed' LIMIT 1;`)[0]);
    const effectiveAccepted = accepted || hostAttested;
    const retryable = result.retryable !== false;
    const nextState: BatchState = hostAttested ? "seen" : accepted ? "dispatched" : retryable ? "retry_wait" : "dead_letter";
    const status: DispatchResult["status"] = effectiveAccepted ? "accepted" : retryable ? "retry_wait" : "dead_letter";
    const errorClass = effectiveAccepted ? null : result.error_class || "transport_rejected";
    const errorMessage = result.error_message || null;
    const requestedRetry = result.retry_after_ms;
    // attempt is the durable cumulative acquisition count, including retries
    // across restarts and binding generations; it is not a failure streak.
    const maxRetryDelay = Math.max(this.config.retry_delay_ms!, 5 * 60_000);
    let backedOffRetryDelay = this.config.retry_delay_ms!;
    for (let attempt = 1; attempt < batch.attempt && backedOffRetryDelay > 0 && backedOffRetryDelay < maxRetryDelay; attempt += 1) {
      backedOffRetryDelay = Math.min(backedOffRetryDelay * 2, maxRetryDelay);
    }
    const retryDelay = requestedRetry !== undefined
      && Number.isFinite(requestedRetry)
      && requestedRetry >= 0
      ? Math.min(requestedRetry, 24 * 60 * 60_000)
      : backedOffRetryDelay;
    const nextNotBefore = effectiveAccepted ? batch.not_before : addMs(now, retryDelay);
    const statements = [
      `UPDATE outbox_attempts SET state=${sqlValue(effectiveAccepted ? "accepted" : "failed")}, transport_kind=${sqlValue(result.transport_kind || transportKind)}, finished_at=${sqlValue(now)}, error_class=${sqlValue(errorClass)}, error_message=${sqlValue(hostAttested ? null : errorMessage)} WHERE id=${sqlValue(attemptId)} AND state='leased';`,
    ];
    if (!effectiveAccepted) statements.push(`UPDATE delivery_correlations SET state='discarded' WHERE attempt_id=${sqlValue(attemptId)} AND state='outstanding';`);
    if (accepted) {
      // Insert the accepted receipt while the attempt id still owns the
      // dispatching row.  A late worker whose lease was reclaimed cannot
      // satisfy this EXISTS predicate and therefore cannot insert a receipt
      // for a newer attempt.
      statements.push(`INSERT INTO receipts(id, batch_id, claim_id, stage, at, endpoint_id, binding_generation, transport_kind, details_json)
        SELECT ${sqlValue(id("rcpt"))}, ${sqlValue(batch.id)}, NULL, 'transport_accepted', ${sqlValue(now)}, ${sqlValue(binding.endpoint_id)}, ${binding.generation}, ${sqlValue(result.transport_kind || transportKind)}, ${sqlJson(result.receipt_details ?? null)}
        WHERE EXISTS (SELECT 1 FROM batches WHERE id=${sqlValue(batch.id)} AND state='dispatching' AND last_error=${sqlValue(attemptId)})
          AND EXISTS (SELECT 1 FROM outbox_attempts WHERE id=${sqlValue(attemptId)} AND state='accepted');`);
    }
    statements.push(`UPDATE batches SET state=${sqlValue(nextState)}, not_before=${sqlValue(nextNotBefore)}, lease_expires_at=NULL, updated_at=${sqlValue(now)}, last_error=${sqlValue(errorClass)} WHERE id=${sqlValue(batch.id)} AND state='dispatching' AND last_error=${sqlValue(attemptId)};`);
    statements.push(
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) SELECT ${sqlValue(batch.id)}, 'dispatching', ${sqlValue(nextState)}, ${sqlValue(hostAttested ? "host_attested" : errorClass || "transport_accepted")}, ${sqlValue(now)} WHERE changes()>0;`,
    );
    this.db.transaction(statements);
    const current = this.getBatch(batch.id);
    if (!current || current.state !== nextState || current.last_error !== errorClass) {
      return { batch_id: batch.id, status: "skipped", attempt_id: attemptId, error_class: "stale_attempt" };
    }
    return { batch_id: batch.id, status, attempt_id: attemptId, endpoint_id: binding.endpoint_id, generation: binding.generation, error_class: errorClass ?? undefined };
  }

  /** Visit at most 1000 due batches, stopping between batches after a 1s soft budget. */
  async dispatchDue(nowDate = this.now()): Promise<DispatchResult[]> {
    const sweepStartedAt = performance.now();
    const clockStartedAt = this.now().getTime();
    // An explicit date remains a not-earlier-than sweep start, never a way to
    // create historical leases. Advance that start with actual clock elapsed
    // time, including time spent awaiting earlier transports in this sweep.
    const startedAt = Math.max(nowDate.getTime(), clockStartedAt);
    const dispatchNow = () => new Date(startedAt + Math.max(0, this.now().getTime() - clockStartedAt));
    // A worker can disappear without a process restart in the daemon (for
    // example, an adapter promise is rejected).  Expired leases are safe to
    // reclaim on every sweep; the attempt id CAS below still protects a live
    // worker from a second dispatcher.
    this.recover();
    const results: DispatchResult[] = [];
    const visited = new Set<string>();
    let candidates: Array<{ id: string }> = [];
    while (visited.size < 1000) {
      // Use a monotonic runtime clock, not the injectable event/lease clock.
      // Always allow one candidate to progress. This cannot interrupt recovery,
      // an individual SQL call or an in-flight transport, so it is not a timeout.
      if (visited.size > 0 && performance.now() - sweepStartedAt >= 1000) {
        await setImmediate();
        break;
      }
      if (!candidates.length) {
        const candidatesAt = dispatchNow().toISOString();
        // Return IDs only. Live bindings take priority; other waiters rotate
        // using updated_at. rowid retains same-tick batch insertion order.
        // Refill against the current clock after preceding slow transports,
        // but never revisit a zero-delay retry within this pass.
        candidates = this.db.query<{ id: string }>(`SELECT b.id FROM batches b
          WHERE b.instance_id=${sqlValue(this.config.instance_id)} AND b.state IN (${OPEN_BATCH_STATES.map(sqlValue).join(",")})
            AND b.not_before<=${sqlValue(candidatesAt)} AND (b.state='waiting_for_endpoint' OR b.deadline IS NULL OR b.deadline<=${sqlValue(candidatesAt)})
            AND b.id NOT IN (SELECT value FROM json_each(${sqlJson([...visited])}))
          ORDER BY EXISTS (SELECT 1 FROM bindings binding JOIN endpoints endpoint ON endpoint.id=binding.endpoint_id
            WHERE binding.instance_id=b.instance_id AND binding.attention_channel=b.attention_channel
              AND endpoint.lease_expires_at>${sqlValue(candidatesAt)}) DESC,
            b.updated_at ASC, b.created_at ASC, b.rowid ASC
          LIMIT ${1000 - visited.size};`);
      }
      const listed = candidates.shift();
      if (!listed) break;
      visited.add(listed.id);
      const current = this.getBatch(listed.id);
      if (!current || !OPEN_BATCH_STATES.includes(current.state)) continue;
      const gateDate = dispatchNow();
      const gateAt = gateDate.toISOString();
      if (current.not_before > gateAt) continue;
      if (current.state !== "waiting_for_endpoint" && current.deadline && current.deadline > gateAt) continue;
      const prepared = this.recheckDispatchGates(current, gateDate);
      if (!prepared || !OPEN_BATCH_STATES.includes(prepared.batch.state)) {
        results.push({ batch_id: listed.id, status: "skipped" });
        continue;
      }
      const batch = prepared.batch;
      const acquireDate = dispatchNow();
      const now = acquireDate.toISOString();
      if (quietWindowAt(acquireDate, this.config.quiet_hours, this.config.timezone ?? "UTC") !== prepared.quietActive) {
        results.push({ batch_id: listed.id, status: "skipped" });
        continue;
      }
      const resolved = this.resolveBinding(batch.attention_channel, now);
      if (!resolved) {
        results.push(this.markNoEndpoint(batch, now));
        continue;
      }
      const transport = this.chooseTransport(batch, resolved.endpoint);
      if (!transport) {
        if (batch.state === "waiting_for_endpoint") {
          this.db.exec(`UPDATE batches SET updated_at=${sqlValue(now)}
            WHERE id=${sqlValue(batch.id)} AND instance_id=${sqlValue(this.config.instance_id)}
              AND state='waiting_for_endpoint' AND updated_at=${sqlValue(batch.updated_at)};`);
        } else this.db.transaction([
          `UPDATE batches SET state='waiting_for_endpoint', last_error='no_compatible_transport', updated_at=${sqlValue(now)} WHERE id=${sqlValue(listed.id)} AND state IN ('pending','retry_wait');`,
          `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) SELECT ${sqlValue(listed.id)}, state, 'waiting_for_endpoint', 'no_transport', ${sqlValue(now)} FROM batches WHERE id=${sqlValue(listed.id)} AND changes()>0;`,
        ]);
        results.push({ batch_id: listed.id, status: "waiting_for_endpoint", error_class: "no_compatible_transport" });
        continue;
      }
      const acquired = this.acquireBatch(batch, resolved.binding, now, prepared.fenceSql);
      if (!acquired) {
        results.push({ batch_id: listed.id, status: "skipped" });
        continue;
      }
      const payload = this.buildPayload(acquired.batch, resolved.binding);
      if (!payload.claim_refs.length) {
        this.db.transaction([
          `UPDATE outbox_attempts SET state='failed', finished_at=${sqlValue(now)}, error_class='empty_batch', error_message='batch has no deliverable claims' WHERE id=${sqlValue(acquired.attemptId)} AND state='leased';`,
          `UPDATE batches SET state='needs_attention', lease_expires_at=NULL, updated_at=${sqlValue(now)}, last_error='empty_batch' WHERE id=${sqlValue(listed.id)} AND state='dispatching' AND last_error=${sqlValue(acquired.attemptId)};`,
          `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
           SELECT ${sqlValue(listed.id)}, 'dispatching', 'needs_attention', 'empty_batch', ${sqlValue(now)} WHERE changes()>0;`,
        ]);
        results.push({ batch_id: listed.id, status: "needs_attention", attempt_id: acquired.attemptId, error_class: "empty_batch" });
        continue;
      }
      const dispatchAt = dispatchNow().toISOString();
      if (!this.bindingStillCurrent(resolved.binding, dispatchAt)) {
        this.db.transaction([
          `UPDATE outbox_attempts SET state='failed', finished_at=${sqlValue(dispatchAt)}, error_class='stale_generation', error_message='binding changed before dispatch' WHERE id=${sqlValue(acquired.attemptId)} AND state='leased';`,
          `UPDATE batches SET state='retry_wait', not_before=${sqlValue(addMs(dispatchAt, this.config.retry_delay_ms!))}, lease_expires_at=NULL, updated_at=${sqlValue(dispatchAt)}, last_error='stale_generation' WHERE id=${sqlValue(listed.id)} AND state='dispatching' AND last_error=${sqlValue(acquired.attemptId)};`,
          `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) SELECT ${sqlValue(listed.id)}, 'dispatching', 'retry_wait', 'stale_generation', ${sqlValue(dispatchAt)} WHERE changes()>0;`,
        ]);
        results.push({ batch_id: listed.id, status: "stale_generation", attempt_id: acquired.attemptId, endpoint_id: resolved.binding.endpoint_id, generation: resolved.binding.generation, error_class: "stale_generation" });
        continue;
      }
      const deliveryNonce = randomBytes(24).toString("base64url");
      const correlationCreatedAt = dispatchNow().toISOString();
      const dispatchFence = `EXISTS (SELECT 1 FROM batches b JOIN outbox_attempts a ON a.batch_id=b.id
        JOIN attention_channels c ON c.instance_id=b.instance_id AND c.attention_channel=b.attention_channel
        JOIN bindings binding ON binding.instance_id=b.instance_id AND binding.attention_channel=b.attention_channel
        WHERE b.id=${sqlValue(acquired.batch.id)} AND b.state='dispatching' AND b.last_error=${sqlValue(acquired.attemptId)}
          AND a.id=${sqlValue(acquired.attemptId)} AND a.state='leased' AND c.deleted_at IS NULL
          AND binding.endpoint_id=${sqlValue(resolved.binding.endpoint_id)} AND binding.generation=${resolved.binding.generation})`;
      const deliveryStatements: string[] = [];
      if (transport.capabilities?.session_activity_observable) {
        deliveryStatements.push(
          `INSERT INTO delivery_correlations(attempt_id, batch_id, endpoint_id, binding_generation, transport_kind, nonce_hash, state, created_at, expires_at, consumed_at)
           SELECT ${sqlValue(acquired.attemptId)}, ${sqlValue(acquired.batch.id)}, ${sqlValue(resolved.binding.endpoint_id)}, ${resolved.binding.generation}, ${sqlValue(transport.kind)}, ${sqlValue(sha256(deliveryNonce))}, 'outstanding', ${sqlValue(correlationCreatedAt)}, ${sqlValue(addMs(correlationCreatedAt, 60 * 60_000))}, NULL WHERE ${dispatchFence};`,
        );
      }
      const dispatchable = this.db.query<{ current: number }>([
        "BEGIN IMMEDIATE;", ...deliveryStatements, `SELECT 1 AS current WHERE ${dispatchFence};`, "COMMIT;",
      ].join("\n"));
      if (!dispatchable.length) {
        results.push({ batch_id: listed.id, status: "skipped", attempt_id: acquired.attemptId, error_class: "stale_attempt" });
        continue;
      }
      const context: TransportContext = {
        batch: acquired.batch,
        endpoint: resolved.endpoint,
        binding: resolved.binding,
        payload,
        attempt_id: acquired.attemptId,
        delivery_nonce: deliveryNonce,
      };
      let transportResult: TransportResult;
      try {
        transportResult = await Promise.resolve(transport.dispatch(context));
      } catch (error) {
        transportResult = { accepted: false, retryable: true, error_class: "transport_exception", error_message: String(error) };
      }
      // A transport may legitimately remain open for minutes. Retry windows,
      // receipts and transitions must be based on completion time, not the
      // timestamp captured before the network/provider turn began.
      const finalizedAt = dispatchNow().toISOString();
      const final = this.finalizeAttempt(acquired.attemptId, acquired.batch, resolved.binding, transportResult, transport.kind, finalizedAt);
      final.endpoint_id = resolved.binding.endpoint_id;
      final.generation = resolved.binding.generation;
      results.push(final);
    }
    return results;
  }

  private receiptInsertSql(receipt: Receipt, condition = "1"): string {
    return `INSERT INTO receipts(id, batch_id, claim_id, stage, at, endpoint_id, binding_generation, transport_kind, details_json)
      SELECT ${sqlValue(receipt.id)}, ${sqlValue(receipt.batch_id)}, ${sqlValue(receipt.claim_id)}, ${sqlValue(receipt.stage)}, ${sqlValue(receipt.at)}, ${sqlValue(receipt.endpoint_id)}, ${sqlValue(receipt.binding_generation)}, ${sqlValue(receipt.transport_kind)}, ${sqlJson(receipt.details)} WHERE ${condition};`;
  }

  private detachClaimFromOpenBatches(claimId: string, state: ClaimState, now: string, snoozeUntil: string | null, condition = "1"): string[] {
    const claimFence = `EXISTS (SELECT 1 FROM claims WHERE id=${sqlValue(claimId)} AND state=${sqlValue(state)}${snoozeUntil == null ? "" : ` AND snooze_until=${sqlValue(snoozeUntil)} AND eligible_after=${sqlValue(snoozeUntil)}`})`;
    const predicate = `instance_id=${sqlValue(this.config.instance_id)} AND state IN (${OPEN_BATCH_STATES.map(sqlValue).join(",")})
      AND EXISTS (SELECT 1 FROM json_each(batches.claim_ids_json) WHERE value=${sqlValue(claimId)}) AND ${claimFence} AND ${condition}`;
    const emptied = "json_array_length(claim_ids_json)=1";
    const reason = snoozeUntil == null ? "all_claims_finalized" : "all_claims_snoozed";
    // Inspect and filter the arrays under the transaction lock. Other claims
    // may have joined the batch since the action's pre-read.
    return [
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
       SELECT id, state, CASE WHEN ${emptied} THEN 'cancelled' ELSE state END,
         CASE WHEN ${emptied} THEN ${sqlValue(reason)} ELSE 'detach_claim' END, ${sqlValue(now)} FROM batches WHERE ${predicate};`,
      `UPDATE batches SET event_ids_json=CASE WHEN ${emptied} THEN '[]' ELSE (
          SELECT json_group_array(j.value) FROM json_each(batches.event_ids_json) j
          WHERE NOT EXISTS (SELECT 1 FROM claims c, json_each(c.event_ids_json) own WHERE c.id=${sqlValue(claimId)} AND own.value=j.value)
            OR EXISTS (SELECT 1 FROM json_each(batches.claim_ids_json) other JOIN claims c ON c.id=other.value, json_each(c.event_ids_json) ref
              WHERE other.value<>${sqlValue(claimId)} AND ref.value=j.value)
        ) END,
        claim_ids_json=(SELECT json_group_array(value) FROM json_each(batches.claim_ids_json) WHERE value<>${sqlValue(claimId)}),
        state=CASE WHEN ${emptied} THEN 'cancelled' ELSE state END,
        last_error=CASE WHEN ${emptied} THEN ${sqlValue(reason)} ELSE last_error END, updated_at=${sqlValue(now)} WHERE ${predicate};`,
    ];
  }

  /** Unbounded unless `limit` is given; `newest_first` then keeps the most recent rows, still in order. */
  listReceipts(batchId?: string, options: { limit?: number; newest_first?: boolean } = {}): Receipt[] {
    const where = [`b.instance_id=${sqlValue(this.config.instance_id)}`];
    if (batchId) where.push(`r.batch_id=${sqlValue(batchId)}`);
    const order = options.newest_first ? "DESC" : "ASC";
    const limit = options.limit == null ? "" : ` LIMIT ${Math.min(100_000, Math.max(1, Math.floor(options.limit)))}`;
    const rows = this.db.query<DbRow>(`SELECT r.* FROM receipts r JOIN batches b ON b.id=r.batch_id WHERE ${where.join(" AND ")} ORDER BY r.at ${order}, r.id ${order}${limit};`);
    if (options.newest_first) rows.reverse();
    return rows.map((row) => ({
      id: String(row.id),
      batch_id: String(row.batch_id),
      claim_id: row.claim_id == null ? null : String(row.claim_id),
      stage: String(row.stage) as ReceiptStage,
      at: String(row.at),
      endpoint_id: row.endpoint_id == null ? null : String(row.endpoint_id),
      binding_generation: row.binding_generation == null ? null : Number(row.binding_generation),
      transport_kind: row.transport_kind == null ? null : String(row.transport_kind),
      details: row.details_json == null ? null : jsonRecord<Record<string, JsonValue>>(row.details_json, {}),
    }));
  }

  listAttempts(batchId?: string): DbRow[] {
    const where = [`b.instance_id=${sqlValue(this.config.instance_id)}`];
    if (batchId) where.push(`a.batch_id=${sqlValue(batchId)}`);
    return this.db.query<DbRow>(`SELECT a.* FROM outbox_attempts a JOIN batches b ON b.id=a.batch_id WHERE ${where.join(" AND ")} ORDER BY a.started_at ASC, a.attempt_no ASC;`);
  }

  private acceptedAttempt(batchId: string, attemptNo?: number): DbRow | null {
    const clauses = [`batch_id=${sqlValue(batchId)}`, "state='accepted'"];
    if (attemptNo != null) clauses.push(`attempt_no=${attemptNo}`);
    return this.db.query<DbRow>(`SELECT * FROM outbox_attempts WHERE ${clauses.join(" AND ")} ORDER BY attempt_no DESC LIMIT 1;`)[0] ?? null;
  }

  ackBatch(batchId: string, endpointId: string, generation: number): WakeBatch {
    const batch = this.getBatch(batchId);
    if (!batch) throw new BridgeError("batch not found", "batch_not_found", 404);
    this.assertChannelNotDeleted(batch.attention_channel);
    const accepted = this.acceptedAttempt(batchId, batch.attempt);
    const acceptedMatches = Boolean(accepted && String(accepted.endpoint_id) === endpointId && Number(accepted.binding_generation) === generation);
    const binding = this.getBinding(batch.attention_channel);
    const bindingMatches = Boolean(binding && binding.endpoint_id === endpointId && binding.generation === generation);
    if (batch.state === "seen") {
      const receipts = this.listReceipts(batchId);
      if (receipts.some((receipt) => receipt.stage === "agent_seen")) return batch;
      if (!receipts.some((receipt) => receipt.stage === "host_attested")) return batch;
      if (!acceptedMatches || !bindingMatches) throw new BridgeError("batch acknowledgement is fenced by endpoint/generation", "stale_generation", 409);
      const now = this.nowIso();
      this.channelTransaction([batch.attention_channel], now, [
        this.channelBindingFenceSql(batch.attention_channel, endpointId, generation, now),
        `INSERT INTO receipts(id, batch_id, claim_id, stage, at, endpoint_id, binding_generation, transport_kind, details_json)
         SELECT ${sqlValue(id("rcpt"))}, ${sqlValue(batchId)}, NULL, 'agent_seen', ${sqlValue(now)}, ${sqlValue(endpointId)}, ${generation}, ${sqlValue(String(accepted!.transport_kind || "unknown"))}, ${sqlJson({ after: "host_attested" })}
         WHERE ${CHANNEL_WRITE_ALLOWED} AND NOT EXISTS (SELECT 1 FROM receipts WHERE batch_id=${sqlValue(batchId)} AND stage='agent_seen');`,
        `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
         SELECT ${sqlValue(batchId)}, 'seen', 'seen', 'agent_ack', ${sqlValue(now)} WHERE changes()>0;`,
      ], false);
      return this.getBatch(batchId)!;
    }
    const lateAck = batch.state === "needs_attention" && batch.last_error === "ack_timeout";
    if (batch.state !== "dispatched" && !lateAck) throw new BridgeError("batch acknowledgement is not available", "batch_not_ready", 409);
    // The accepted attempt must be the one that finalized this batch.  A
    // previous accepted attempt may still be present after lease recovery;
    // accepting its late acknowledgement would incorrectly mark a retry as
    // seen.
    if (!accepted || !acceptedMatches) {
      throw new BridgeError("batch acknowledgement is fenced by endpoint/generation", "stale_generation", 409);
    }
    if (!bindingMatches) throw new BridgeError("batch acknowledgement targets stale binding", "stale_generation", 409);
    const now = this.nowIso();
    this.channelTransaction([batch.attention_channel], now, [
      this.channelBindingFenceSql(batch.attention_channel, endpointId, generation, now),
      `UPDATE batches SET state='seen', updated_at=${sqlValue(now)}, lease_expires_at=NULL, last_error=${lateAck ? "NULL" : "last_error"}
       WHERE id=${sqlValue(batchId)} AND instance_id=${sqlValue(this.config.instance_id)}
         AND state=${sqlValue(batch.state)} ${lateAck ? "AND last_error='ack_timeout'" : ""}
         AND attempt=${Number(accepted.attempt_no)} AND binding_generation=${generation} AND ${CHANNEL_WRITE_ALLOWED};`,
      // Keep the transition INSERT immediately after the CAS so changes()
      // refers to that UPDATE.  The receipt follows it and is conditional on
      // the transition INSERT, so a stale/concurrent ack cannot leave a
      // misleading agent_seen receipt behind.  The captured state is the
      // state fenced by the CAS, including needs_attention for a late ack.
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) SELECT ${sqlValue(batchId)}, ${sqlValue(batch.state)}, 'seen', 'agent_ack', ${sqlValue(now)} WHERE changes()>0;`,
      `INSERT INTO receipts(id, batch_id, claim_id, stage, at, endpoint_id, binding_generation, transport_kind, details_json)
        SELECT ${sqlValue(id("rcpt"))}, ${sqlValue(batchId)}, NULL, 'agent_seen', ${sqlValue(now)}, ${sqlValue(endpointId)}, ${generation}, ${sqlValue(String(accepted.transport_kind || "unknown"))}, NULL
        WHERE changes()>0;`,
    ], false);
    const current = this.getBatch(batchId);
    if (!current) throw new BridgeError("batch not found", "batch_not_found", 404);
    if (current.state === "seen") return current;
    throw new BridgeError("batch acknowledgement was fenced by a concurrent state change", "batch_not_ready", 409);
  }

  recordReceipt(batchId: string, stage: ReceiptStage, details?: Record<string, JsonValue> | null, endpointId?: string, generation?: number): Receipt {
    const batch = this.getBatch(batchId);
    if (!batch) throw new BridgeError("batch not found", "batch_not_found", 404);
    this.assertChannelNotDeleted(batch.attention_channel);
    if (["agent_seen", "agent_consumed", "agent_acted"].includes(stage) && endpointId && generation != null) {
      const binding = this.getBinding(batch.attention_channel);
      if (!binding || binding.endpoint_id !== endpointId || binding.generation !== generation) throw new BridgeError("receipt target is stale", "stale_generation", 409);
    }
    const receipt: Receipt = { id: id("rcpt"), batch_id: batchId, stage, at: this.nowIso(), endpoint_id: endpointId ?? null, binding_generation: generation ?? null, transport_kind: null, details: details ?? null };
    this.channelTransaction([batch.attention_channel], receipt.at, [
      ...(["agent_seen", "agent_consumed", "agent_acted"].includes(stage) && endpointId && generation != null
        ? [this.channelBindingFenceSql(batch.attention_channel, endpointId, generation, receipt.at)] : []),
      this.receiptInsertSql(receipt, CHANNEL_WRITE_ALLOWED),
    ], false);
    return receipt;
  }

  private claimAction(claimId: string, action: "snooze" | "dismiss" | "consume", value?: string | JsonValue | null): AttentionClaim {
    const claim = this.getClaim(claimId);
    if (!claim) throw new BridgeError("claim not found", "claim_not_found", 404);
    this.assertChannelNotDeleted(claim.attention_channel);
    if (FINALIZED_CLAIM_STATES.includes(claim.state)) return claim;
    const now = this.nowIso();
    const nextState: ClaimState = action === "snooze" ? "pending" : action === "dismiss" ? "dismissed" : "consumed";
    // The consume receipt belongs to the delivery the agent is answering,
    // which is the most recent batch when a snoozed claim was re-batched.
    const batch = this.batchesContainingClaim(claimId).at(-1);
    const statements = [
      `UPDATE claims SET state=${sqlValue(nextState)}, eligible_after=${sqlValue(action === "snooze" ? String(value) : claim.eligible_after)}, snooze_until=${sqlValue(action === "snooze" ? String(value) : null)}, consumed_result_json=${sqlJson(action === "consume" ? value : null)}, dismissed_reason=${sqlValue(action === "dismiss" ? String(value || "dismissed") : null)}, updated_at=${sqlValue(now)} WHERE id=${sqlValue(claimId)} AND state=${sqlValue(claim.state)} AND eligible_after=${sqlValue(claim.eligible_after)} AND snooze_until IS ${sqlValue(claim.snooze_until)} AND ${CHANNEL_WRITE_ALLOWED};`,
      `INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at) SELECT ${sqlValue(claimId)}, ${sqlValue(claim.state)}, ${sqlValue(nextState)}, ${sqlValue(action)}, ${sqlValue(now)} WHERE changes()>0;`,
    ];
    if (batch && action !== "snooze") {
      const details = action === "dismiss" ? { reason: String(value || "dismissed") } : { result: (value as JsonValue) ?? null };
      statements.push(`INSERT INTO receipts(id, batch_id, claim_id, stage, at, endpoint_id, binding_generation, transport_kind, details_json)
        SELECT ${sqlValue(id("rcpt"))}, ${sqlValue(batch.id)}, ${sqlValue(claimId)}, 'agent_consumed', ${sqlValue(now)}, NULL, NULL, NULL, ${sqlJson(details)} WHERE changes()>0;`);
    }
    statements.push(...this.detachClaimFromOpenBatches(claimId, nextState, now, action === "snooze" ? String(value) : null, CHANNEL_WRITE_ALLOWED));
    if (action !== "snooze") {
      statements.push(...this.claimEventStateSql(claimId, nextState, "consumed", now, action, CHANNEL_WRITE_ALLOWED));
    }
    this.channelTransaction([claim.attention_channel], now, statements, false);
    return this.getClaim(claimId)!;
  }

  snoozeClaim(claimId: string, until: string): AttentionClaim {
    const due = isoOrThrow(until, "until");
    if (new Date(due).getTime() <= this.now().getTime()) throw new BridgeError("snooze until must be in the future", "invalid_claim", 400);
    return this.claimAction(claimId, "snooze", due);
  }

  dismissClaim(claimId: string, reason = "dismissed"): AttentionClaim {
    if (reason.length > 500) throw new BridgeError("dismiss reason is too long", "invalid_claim", 400);
    return this.claimAction(claimId, "dismiss", reason);
  }

  consumeClaim(claimId: string, result?: JsonValue | null): AttentionClaim {
    if (result !== undefined && JSON.stringify(result).length > 4096) throw new BridgeError("consume result is too large", "oversized_event", 413);
    return this.claimAction(claimId, "consume", result ?? null);
  }

  inspect(): {
    instance: Pick<BridgeConfig, "instance_id" | "owner_id" | "timezone">;
    events: WakeEvent[];
    claims: AttentionClaim[];
    batches: WakeBatch[];
    bindings: Binding[];
    endpoints: Endpoint[];
    presence: PresenceLease[];
    activity_watches: ActivityWatch[];
    delivery_correlations: DeliveryCorrelation[];
    receipts: Receipt[];
    attempts: DbRow[];
  } {
    return {
      instance: { instance_id: this.config.instance_id, owner_id: this.config.owner_id, timezone: this.config.timezone },
      events: this.listEvents({ limit: 1000, newest_first: true }),
      claims: this.listClaims({ limit: 100_000 }),
      batches: this.listBatches({ limit: 100_000 }),
      bindings: this.listBindings(),
      endpoints: this.listEndpoints(),
      presence: this.listPresence(),
      activity_watches: this.listActivityWatches(),
      delivery_correlations: this.listDeliveryCorrelations(),
      receipts: this.listReceipts(),
      attempts: this.listAttempts(),
    };
  }
}

export { DEFAULT_POLICY };
