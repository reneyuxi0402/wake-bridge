import { randomBytes } from "node:crypto";
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
import type {
  ActivityObservation,
  ActivityObservationResult,
  ActivityWatch,
  ActivityWatchConfiguration,
  AttentionClaim,
  BatchState,
  Binding,
  BridgeConfig,
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
  /** Only the canonical dispatcher owner may reclaim expired dispatch leases. */
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

function validateResource(resource: ResourceRef): void {
  if (!resource || typeof resource !== "object" || typeof resource.uri !== "string") {
    throw new BridgeError("resource.uri is required", "invalid_resource", 400);
  }
  if (resource.uri.length === 0 || resource.uri.length > 2048 || !/^[A-Za-z][A-Za-z0-9+.-]*:/.test(resource.uri)) {
    throw new BridgeError("resource.uri must be a bounded absolute URI", "invalid_resource", 400);
  }
  if (JSON.stringify(resource).length > 4096) throw new BridgeError("resource is too large", "oversized_event", 413);
}

export function normalizePolicy(policy: PolicyRule): PolicyRule {
  if (!policy || typeof policy.id !== "string" || !/^[A-Za-z][A-Za-z0-9._-]{0,127}$/u.test(policy.id)
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
    || !/^(?:[01]\d|2[0-3]):[0-5]\d$/u.test(delivery.scheduled_local_time))) {
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
    },
    reason_code: policy.reason_code ?? policy.id,
  };
}

function normalizeStoredPolicy(policy: InstalledPolicyRule): InstalledPolicyRule {
  if (!policy || typeof policy.id !== "string" || !/^[A-Za-z][A-Za-z0-9._-]{0,127}$/u.test(policy.id)
    || !Number.isSafeInteger(policy.version) || policy.version < 1) {
    throw new BridgeError("stored policy id or version is invalid", "invalid_policy", 500);
  }
  const delivery = policy.delivery;
  if (!delivery || !["immediate", "debounce", "digest", "scheduled", "suppress"].includes(String(delivery.mode))) {
    throw new BridgeError(`stored policy has unsupported delivery mode: ${String(delivery?.mode)}`, "invalid_policy", 500);
  }
  if (delivery.mode === "scheduled" && (typeof delivery.scheduled_local_time !== "string"
    || !/^(?:[01]\d|2[0-3]):[0-5]\d$/u.test(delivery.scheduled_local_time))) {
    throw new BridgeError("stored scheduled policy has invalid local time", "invalid_policy", 500);
  }
  const values = [delivery.debounce_ms, delivery.cadence_ms, delivery.max_delay_ms, policy.batch?.max_events, policy.batch?.window_ms];
  if (values.some((value) => value !== undefined && value !== null && (!Number.isFinite(value) || value < 0))) {
    throw new BridgeError("stored policy has invalid durations or batch limits", "invalid_policy", 500);
  }
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

function activeInstalledPolicyVersions(policies: InstalledPolicyRule[]): InstalledPolicyRule[] {
  const latest = new Map<string, InstalledPolicyRule>();
  for (const raw of policies) {
    const policy = normalizeStoredPolicy(raw);
    const current = latest.get(policy.id);
    if (!current || policy.version > current.version) latest.set(policy.id, policy);
  }
  return [...latest.values()].sort((a, b) => (b.order ?? 0) - (a.order ?? 0) || a.id.localeCompare(b.id));
}

export function activePolicyVersions(policies: PolicyRule[]): PolicyRule[] {
  const latest = new Map<string, PolicyRule>();
  for (const raw of policies) {
    const policy = normalizePolicy(raw);
    const current = latest.get(policy.id);
    if (!current || policy.version > current.version) latest.set(policy.id, policy);
  }
  return [...latest.values()].sort((a, b) => (b.order ?? 0) - (a.order ?? 0) || a.id.localeCompare(b.id));
}

function jsonRecord<T>(value: unknown, fallback: T): T {
  return parseJson<T>(value, fallback);
}

const ABSOLUTE_RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/u;

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
    const quietConfig = config.quiet_hours as unknown as Record<string, unknown> | null | undefined;
    const unsupportedQuiet = ["resume_spread_ms", "default_action"].filter((field) => quietConfig && Object.prototype.hasOwnProperty.call(quietConfig, field));
    if (unsupportedQuiet.length) {
      throw new BridgeError(`unsupported quiet_hours field(s): ${unsupportedQuiet.join(", ")}`, "unsupported_config_field", 400);
    }
    const timezone = config.timezone ?? config.quiet_hours?.timezone ?? "UTC";
    for (const candidate of new Set([timezone, config.quiet_hours?.timezone].filter((value): value is string => Boolean(value)))) {
      try {
        new Intl.DateTimeFormat("en", { timeZone: candidate }).format(new Date(0));
      } catch {
        throw new BridgeError(`timezone is invalid: ${candidate}`, "invalid_timezone", 400);
      }
    }
    if (config.quiet_hours && (!Array.isArray(config.quiet_hours.windows)
      || config.quiet_hours.windows.some((window) => !window || !/^(?:[01]\d|2[0-3]):[0-5]\d$/u.test(window.start)
        || !/^(?:[01]\d|2[0-3]):[0-5]\d$/u.test(window.end)))) {
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
      presence_ttl_ms: config.presence_ttl_ms ?? 20 * 60_000,
      activity_ttl_ms: config.activity_ttl_ms ?? 2 * 60_000,
      endpoint_lease_ms: config.endpoint_lease_ms ?? 60 * 60_000,
      dispatch_lease_ms: config.dispatch_lease_ms ?? 30_000,
      retry_delay_ms: config.retry_delay_ms ?? 5_000,
      batch_window_ms: config.batch_window_ms ?? 0,
    };
    this.clock = options.clock ?? (() => new Date());
    this.db = new SqliteDatabase(config.db_path, { allowSchemaUpgrade: options.allowSchemaUpgrade });
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

    const installed = this.listPolicies();
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

    if (additions.length) {
      const installedAt = this.nowIso();
      this.db.transaction(additions.map((policy) => `INSERT INTO policies(id, version, enabled, order_no, rule_json, installed_at)
        VALUES(${sqlValue(policy.id)}, ${policy.version}, ${policy.enabled === false ? 0 : 1}, ${Number(policy.order ?? 0)}, ${sqlJson(policy)}, ${sqlValue(installedAt)});`));
    }
    this.ensureDefaultPolicy();
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
    const rows = candidates ? activePolicyVersions(candidates).filter((policy) => policy.enabled !== false) : this.policyRows();
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
    const suppressed = policy.delivery.mode === "suppress";
    const channel = suppressed ? null : this.targetChannel(policy, clean);
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
    if (!channel || channel.length > 200) throw new BridgeError("attention channel is invalid", "invalid_policy", 400);
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
    if (!source || !/^[A-Za-z0-9_.:-]{1,128}$/.test(source)) throw new BridgeError("source identity is invalid", "invalid_source", 400);
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
    const claim = this.listClaims().find((item) => item.event_ids.includes(event.id));
    return { event, claim, duplicate: true, suppressed: event.state === "suppressed" };
  }

  private isUniqueConflict(error: unknown): boolean {
    return /UNIQUE constraint failed/i.test(error instanceof Error ? error.message : String(error));
  }

  /** Accept both emitEvent(source, input) and emitEvent(input, source). */
  emitEvent(sourceOrInput: string | WakeEventInput, inputOrSource?: WakeEventInput | string): EmitResult {
    const source = typeof sourceOrInput === "string" ? sourceOrInput : typeof inputOrSource === "string" ? inputOrSource : "manual";
    const input = (typeof sourceOrInput === "string" ? inputOrSource : sourceOrInput) as WakeEventInput;
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
    const claimState: ClaimState = "pending";
    const statements = [
      `INSERT INTO events(id, instance_id, owner_id, source, type, schema_version, occurred_at, received_at, dedupe_key, coalesce_key, priority_hint, attention_channel_hint, actor_ref, resource_json, metadata_json, payload_preview, matched_policy_id, matched_policy_version)
       VALUES(${sqlValue(eventId)}, ${sqlValue(this.config.instance_id)}, ${sqlValue(this.config.owner_id)}, ${sqlValue(source)}, ${sqlValue(clean.type)}, ${Number(clean.schema_version ?? 1)}, ${sqlValue(clean.occurred_at)}, ${sqlValue(now)}, ${sqlValue(clean.dedupe_key)}, ${sqlValue(clean.coalesce_key)}, ${sqlValue(clean.priority_hint)}, ${sqlValue(clean.attention_channel_hint)}, ${sqlValue(clean.actor_ref)}, ${sqlJson(clean.resource)}, ${sqlJson(clean.metadata ?? {})}, ${sqlValue(clean.payload_preview)}, ${sqlValue(policy.id)}, ${policy.version});`,
      `INSERT INTO event_status(event_id, state, updated_at) VALUES(${sqlValue(eventId)}, ${sqlValue("received")}, ${sqlValue(now)});`,
      `INSERT INTO event_transitions(event_id, from_state, to_state, reason, at) VALUES(${sqlValue(eventId)}, NULL, ${sqlValue("received")}, ${sqlValue("ingest")}, ${sqlValue(now)});`,
      `UPDATE events SET matched_policy_id=${sqlValue(policy.id)}, matched_policy_version=${policy.version} WHERE id=${sqlValue(eventId)};`,
      `UPDATE event_status SET state=${sqlValue(eventState)}, updated_at=${sqlValue(now)} WHERE event_id=${sqlValue(eventId)};`,
      `INSERT INTO event_transitions(event_id, from_state, to_state, reason, at) VALUES(${sqlValue(eventId)}, ${sqlValue("received")}, ${sqlValue(eventState)}, ${sqlValue(policy.delivery.mode)}, ${sqlValue(now)});`,
    ];
    if (claimId && channel) {
      const eligibleAfter = this.claimEligibleAfter(policy, this.now());
      const deferPresence = policy.delivery.foreground_presence_policy !== "bypass";
      const expiresAt = this.claimExpiresAt(policy, this.now());
      statements.push(
        `INSERT INTO claims(id, instance_id, origin, event_ids_json, resource_json, policy_id, policy_version, attention_channel, eligible_after, expires_at, defer_while_presence, state, reason_code, note, created_at, updated_at, snooze_until, consumed_result_json, dismissed_reason)
         VALUES(${sqlValue(claimId)}, ${sqlValue(this.config.instance_id)}, ${sqlValue("policy")}, ${sqlJson([eventId])}, ${sqlJson(clean.resource)}, ${sqlValue(policy.id)}, ${policy.version}, ${sqlValue(channel)}, ${sqlValue(eligibleAfter)}, ${sqlValue(expiresAt)}, ${deferPresence ? 1 : 0}, ${sqlValue(claimState)}, ${sqlValue(policy.reason_code ?? policy.id)}, NULL, ${sqlValue(now)}, ${sqlValue(now)}, NULL, NULL, NULL);`,
      );
      statements.push(`INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at) VALUES(${sqlValue(claimId)}, NULL, ${sqlValue(claimState)}, ${sqlValue("policy")}, ${sqlValue(now)});`);
    }
    if (clean.idempotency_key) {
      statements.push(`INSERT OR IGNORE INTO idempotency_keys(scope, key, result_json, created_at) VALUES(${sqlValue(`event:${source}`)}, ${sqlValue(clean.idempotency_key)}, ${sqlJson({ event_id: eventId })}, ${sqlValue(now)});`);
    }
    try {
      this.db.transaction(statements);
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
    const claim = claimId ? this.getClaim(claimId) ?? undefined : undefined;
    return { event, claim, duplicate: false, suppressed: eventState === "suppressed" };
  }

  emit(source: string, input: WakeEventInput): EmitResult {
    return this.emitEvent(source, input);
  }

  scheduleClaim(input: ScheduleClaimInput): { event: WakeEvent; claim: AttentionClaim; duplicate: boolean } {
    validateResource(input.resource);
    const eligibleAfter = isoOrThrow(input.eligible_after, "eligible_after");
    const expiresAt = input.expires_at == null ? null : isoOrThrow(input.expires_at, "expires_at");
    const key = input.idempotency_key || `resource:${sha256(stableJson({ resource: input.resource, eligibleAfter, channel: input.attention_channel || "default", reason: input.reason_code || "reconsider" }))}`;
    const source = "self_commitment";
    const dedupeKey = `schedule:${key}`;
    const existing = this.db.query<DbRow>(`SELECT id FROM events WHERE instance_id=${sqlValue(this.config.instance_id)} AND source=${sqlValue(source)} AND dedupe_key=${sqlValue(dedupeKey)} LIMIT 1;`)[0];
    if (existing) {
      const event = this.getEvent(String(existing.id))!;
      const claim = this.listClaims().find((item) => item.event_ids.includes(event.id));
      if (!claim) throw new BridgeError("self commitment event has no claim", "storage_corrupt", 500);
      return { event, claim, duplicate: true };
    }
    const now = this.nowIso();
    const eventId = `evt_${sha256(`schedule:${this.config.instance_id}:${key}`).slice(0, 32)}`;
    const claimId = `ac_${sha256(`schedule-claim:${this.config.instance_id}:${key}`).slice(0, 32)}`;
    const channel = input.attention_channel || "default";
    if (channel.length > 200) throw new BridgeError("attention channel is invalid", "invalid_claim", 400);
    const metadata = { origin: "self_commitment", reason_code: input.reason_code || "reconsider" };
    const statements = [
      `INSERT INTO events(id, instance_id, owner_id, source, type, schema_version, occurred_at, received_at, dedupe_key, coalesce_key, priority_hint, attention_channel_hint, actor_ref, resource_json, metadata_json, payload_preview, matched_policy_id, matched_policy_version)
       VALUES(${sqlValue(eventId)}, ${sqlValue(this.config.instance_id)}, ${sqlValue(this.config.owner_id)}, ${sqlValue(source)}, ${sqlValue("attention.schedule")}, 1, ${sqlValue(now)}, ${sqlValue(now)}, ${sqlValue(dedupeKey)}, ${sqlValue(channel)}, ${sqlValue("normal")}, ${sqlValue(channel)}, ${sqlValue(this.config.owner_id)}, ${sqlJson(input.resource)}, ${sqlJson(metadata)}, NULL, ${sqlValue("self_commitment")}, 1);`,
      `INSERT INTO event_status(event_id, state, updated_at) VALUES(${sqlValue(eventId)}, ${sqlValue("matched")}, ${sqlValue(now)});`,
      `INSERT INTO event_transitions(event_id, from_state, to_state, reason, at) VALUES(${sqlValue(eventId)}, NULL, ${sqlValue("matched")}, ${sqlValue("self_commitment")}, ${sqlValue(now)});`,
      `INSERT INTO claims(id, instance_id, origin, event_ids_json, resource_json, policy_id, policy_version, attention_channel, eligible_after, expires_at, defer_while_presence, state, reason_code, note, created_at, updated_at, snooze_until, consumed_result_json, dismissed_reason)
       VALUES(${sqlValue(claimId)}, ${sqlValue(this.config.instance_id)}, ${sqlValue("self_commitment")}, ${sqlJson([eventId])}, ${sqlJson(input.resource)}, ${sqlValue("self_commitment")}, 1, ${sqlValue(channel)}, ${sqlValue(eligibleAfter)}, ${sqlValue(expiresAt)}, ${sqlValue(input.defer_while_presence !== false)}, ${sqlValue("pending")}, ${sqlValue(input.reason_code || "reconsider")}, ${sqlValue(input.note)}, ${sqlValue(now)}, ${sqlValue(now)}, NULL, NULL, NULL);`,
      `INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at) VALUES(${sqlValue(claimId)}, NULL, ${sqlValue("pending")}, ${sqlValue("self_commitment")}, ${sqlValue(now)});`,
      `INSERT OR IGNORE INTO idempotency_keys(scope, key, result_json, created_at) VALUES(${sqlValue("claim.schedule")}, ${sqlValue(key)}, ${sqlJson({ event_id: eventId, claim_id: claimId })}, ${sqlValue(now)});`,
    ];
    try {
      this.db.transaction(statements);
    } catch (error) {
      if (this.isUniqueConflict(error)) {
        const duplicateEvent = this.db.query<DbRow>(`SELECT id FROM events WHERE instance_id=${sqlValue(this.config.instance_id)} AND source=${sqlValue(source)} AND dedupe_key=${sqlValue(dedupeKey)} LIMIT 1;`)[0];
        if (duplicateEvent) {
          const event = this.getEvent(String(duplicateEvent.id))!;
          const claim = this.listClaims().find((item) => item.event_ids.includes(event.id));
          if (claim) return { event, claim, duplicate: true };
        }
      }
      throw error;
    }
    return { event: this.getEvent(eventId)!, claim: this.getClaim(claimId)!, duplicate: false };
  }

  getEvent(eventId: string): WakeEvent | null {
    const row = this.db.query<DbRow>(`SELECT e.*, s.state FROM events e JOIN event_status s ON s.event_id=e.id WHERE e.id=${sqlValue(eventId)} AND e.instance_id=${sqlValue(this.config.instance_id)} LIMIT 1;`)[0];
    return row ? this.eventFromRow(row) : null;
  }

  listEvents(filters: { state?: EventState; source?: string; after?: string; limit?: number } = {}): WakeEvent[] {
    const clauses = [`e.instance_id=${sqlValue(this.config.instance_id)}`];
    if (filters.state) clauses.push(`s.state=${sqlValue(filters.state)}`);
    if (filters.source) clauses.push(`e.source=${sqlValue(filters.source)}`);
    if (filters.after) clauses.push(`e.received_at>${sqlValue(filters.after)}`);
    const limit = Math.min(1000, Math.max(1, Math.floor(filters.limit ?? 100)));
    return this.db.query<DbRow>(`SELECT e.*, s.state FROM events e JOIN event_status s ON s.event_id=e.id WHERE ${clauses.join(" AND ")} ORDER BY e.received_at ASC LIMIT ${limit};`).map((row) => this.eventFromRow(row));
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

  listClaims(filters: { state?: ClaimState; channel?: string; source?: string; limit?: number } = {}): AttentionClaim[] {
    const clauses = [`c.instance_id=${sqlValue(this.config.instance_id)}`];
    if (filters.state) clauses.push(`c.state=${sqlValue(filters.state)}`);
    if (filters.channel) clauses.push(`c.attention_channel=${sqlValue(filters.channel)}`);
    const limit = Math.min(100_000, Math.max(1, Math.floor(filters.limit ?? 1000)));
    let claims = this.db.query<DbRow>(`SELECT c.* FROM claims c WHERE ${clauses.join(" AND ")} ORDER BY c.created_at ASC LIMIT ${limit};`).map((row) => this.claimFromRow(row));
    if (filters.source) {
      claims = claims.filter((claim) => claim.event_ids.some((eventId) => this.getEvent(eventId)?.source === filters.source));
    }
    return claims;
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
          /token|secret|authorization|bearer/i.test(key) ? "[redacted]" : value,
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
    this.db.transaction([
      `INSERT INTO endpoints(id, instance_id, host_kind, session_ref, lease_token_hash, lease_expires_at, capabilities_json, routes_json, registered_at, updated_at)
       VALUES(${sqlValue(endpointId)}, ${sqlValue(this.config.instance_id)}, ${sqlValue(registration.host_kind)}, ${sqlValue(registration.session_ref)}, ${sqlValue(sha256(token))}, ${sqlValue(addMs(now, leaseMs))}, ${sqlJson(capabilities)}, ${sqlJson(routes)}, ${sqlValue(now)}, ${sqlValue(now)})
       ON CONFLICT(id) DO UPDATE SET host_kind=excluded.host_kind, session_ref=excluded.session_ref, lease_token_hash=excluded.lease_token_hash, lease_expires_at=excluded.lease_expires_at, capabilities_json=excluded.capabilities_json, routes_json=excluded.routes_json, updated_at=excluded.updated_at;`,
    ]);
    const row = this.db.query<DbRow>(`SELECT * FROM endpoints WHERE id=${sqlValue(endpointId)};`)[0];
    return this.endpointFromRow(row, true, token);
  }

  renewEndpoint(endpointId: string, leaseToken: string, leaseMs?: number): Endpoint {
    const now = this.nowIso();
    const duration = assertFiniteMs(leaseMs, "lease_ms", this.config.endpoint_lease_ms!);
    const result = this.db.query<DbRow>(`UPDATE endpoints SET lease_expires_at=${sqlValue(addMs(now, duration))}, updated_at=${sqlValue(now)} WHERE id=${sqlValue(endpointId)} AND instance_id=${sqlValue(this.config.instance_id)} AND lease_token_hash=${sqlValue(sha256(leaseToken))} RETURNING *;`)[0];
    if (!result) throw new BridgeError("endpoint lease token is invalid or endpoint is unknown", "endpoint_fenced", 409);
    return this.endpointFromRow(result);
  }

  renewBoundEndpoint(endpointId: string, channel: string, expectedGeneration: number, leaseMs?: number): Endpoint {
    const binding = this.getBinding(channel);
    if (!binding || binding.endpoint_id !== endpointId || binding.generation !== expectedGeneration) {
      throw new BridgeError("endpoint renewal does not match current binding generation", "stale_generation", 409);
    }
    if (!this.getEndpoint(endpointId)) throw new BridgeError("endpoint is unknown", "endpoint_not_found", 404);
    const now = this.nowIso();
    const duration = assertFiniteMs(leaseMs, "lease_ms", this.config.endpoint_lease_ms!);
    const result = this.db.query<DbRow>(`UPDATE endpoints SET lease_expires_at=${sqlValue(addMs(now, duration))}, updated_at=${sqlValue(now)} WHERE id=${sqlValue(endpointId)} AND instance_id=${sqlValue(this.config.instance_id)} RETURNING *;`)[0];
    return this.endpointFromRow(result);
  }

  revokeEndpoint(endpointId: string, leaseToken: string): Endpoint {
    this.endpointAuthorized(endpointId, leaseToken);
    return this.revokeEndpointState(endpointId);
  }

  revokeBoundEndpoint(endpointId: string, channel: string, expectedGeneration: number): Endpoint {
    const binding = this.getBinding(channel);
    if (!binding || binding.endpoint_id !== endpointId || binding.generation !== expectedGeneration) {
      throw new BridgeError("endpoint revoke does not match current binding generation", "stale_generation", 409);
    }
    if (!this.getEndpoint(endpointId)) throw new BridgeError("endpoint is unknown", "endpoint_not_found", 404);
    return this.revokeEndpointState(endpointId);
  }

  private revokeEndpointState(endpointId: string): Endpoint {
    const now = this.nowIso();
    this.db.transaction([
      `UPDATE endpoints SET lease_expires_at=${sqlValue(now)}, updated_at=${sqlValue(now)} WHERE id=${sqlValue(endpointId)} AND instance_id=${sqlValue(this.config.instance_id)};`,
      `DELETE FROM presence_leases WHERE instance_id=${sqlValue(this.config.instance_id)} AND endpoint_id=${sqlValue(endpointId)};`,
      `UPDATE activity_watches SET activity_lease_expires_at=NULL, updated_at=${sqlValue(now)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND endpoint_id=${sqlValue(endpointId)};`,
      `UPDATE delivery_correlations SET state='discarded' WHERE endpoint_id=${sqlValue(endpointId)} AND state='outstanding';`,
    ]);
    return this.getEndpoint(endpointId)!;
  }

  takeover(channel: string, endpointId: string, expectedGeneration?: number): Binding {
    if (!channel || channel.length > 200) throw new BridgeError("attention channel is invalid", "invalid_binding", 400);
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
    this.db.transaction([
      `INSERT INTO bindings(instance_id, attention_channel, endpoint_id, generation, bound_at)
       VALUES(${sqlValue(this.config.instance_id)}, ${sqlValue(channel)}, ${sqlValue(endpointId)}, ${generation}, ${sqlValue(now)})
       ON CONFLICT(instance_id, attention_channel) DO UPDATE SET endpoint_id=excluded.endpoint_id, generation=excluded.generation, bound_at=excluded.bound_at;`,
      `DELETE FROM presence_leases WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)};`,
      `UPDATE activity_watches SET endpoint_id=NULL, binding_generation=NULL, epoch=epoch+1, armed_at=NULL, last_nonwake_activity_at=NULL, activity_lease_expires_at=NULL, fired_epoch=NULL, sequence=0, last_triggered_at=NULL, next_due_at=NULL, updated_at=${sqlValue(now)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)};`,
      `UPDATE delivery_correlations SET state='discarded' WHERE state='outstanding' AND batch_id IN (SELECT id FROM batches WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)});`,
      `UPDATE batches SET state='pending', binding_generation=NULL, updated_at=${sqlValue(now)}, last_error=NULL WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)} AND state='waiting_for_endpoint';`,
    ]);
    return this.getBinding(channel)!;
  }

  private endpointAuthorized(endpointId: string, token: string): DbRow {
    const row = this.db.query<DbRow>(`SELECT * FROM endpoints WHERE id=${sqlValue(endpointId)} AND instance_id=${sqlValue(this.config.instance_id)} AND lease_token_hash=${sqlValue(sha256(token))} LIMIT 1;`)[0];
    if (!row) throw new BridgeError("endpoint lease token is invalid", "endpoint_fenced", 409);
    if (new Date(String(row.lease_expires_at)).getTime() <= this.now().getTime()) throw new BridgeError("endpoint lease has expired", "endpoint_fenced", 409);
    return row;
  }

  renewPresence(input: PresenceRenewal): PresenceLease {
    if (!input?.attention_channel || !input.endpoint_id || !input.observed_by) throw new BridgeError("presence channel, endpoint and observed_by are required", "invalid_presence", 400);
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
    this.db.transaction([
      `INSERT INTO presence_leases(instance_id, attention_channel, endpoint_id, binding_generation, renewed_at, expires_at, observed_by, observation)
       VALUES(${sqlValue(this.config.instance_id)}, ${sqlValue(input.attention_channel)}, ${sqlValue(input.endpoint_id)}, ${input.generation}, ${sqlValue(now)}, ${sqlValue(expires)}, ${sqlValue(input.observed_by)}, ${sqlValue(input.observation || "user_message_accepted")})
       ON CONFLICT(instance_id, attention_channel) DO UPDATE SET endpoint_id=excluded.endpoint_id, binding_generation=excluded.binding_generation, renewed_at=excluded.renewed_at, expires_at=excluded.expires_at, observed_by=excluded.observed_by, observation=excluded.observation;`,
    ]);
    return this.getPresence(input.attention_channel)!;
  }

  configureActivityWatch(input: ActivityWatchConfiguration): ActivityWatch {
    const channel = input?.attention_channel?.trim();
    if (!channel || channel.length > 200) throw new BridgeError("attention channel is invalid", "invalid_activity_watch", 400);
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
      && existing.mode === mode && existing.repeat_after_ms === repeatAfter) return existing;
    const now = this.nowIso();
    const binding = this.getBinding(channel);
    if (!existing) {
      this.db.transaction([
        `INSERT INTO activity_watches(instance_id, attention_channel, enabled, idle_after_ms, endpoint_id, binding_generation, epoch, armed_at, last_nonwake_activity_at, activity_lease_expires_at, fired_epoch, mode, repeat_after_ms, sequence, last_triggered_at, next_due_at, updated_at)
         VALUES(${sqlValue(this.config.instance_id)}, ${sqlValue(channel)}, ${sqlValue(input.enabled)}, ${idleAfter}, ${sqlValue(binding?.endpoint_id)}, ${sqlValue(binding?.generation)}, 1, NULL, NULL, NULL, NULL, ${sqlValue(mode)}, ${repeatAfter}, 0, NULL, NULL, ${sqlValue(now)});`,
      ]);
    } else if (!existing.enabled && input.enabled) {
      this.db.transaction([
        `UPDATE activity_watches SET enabled=1, idle_after_ms=${idleAfter}, mode=${sqlValue(mode)}, repeat_after_ms=${repeatAfter}, endpoint_id=${sqlValue(binding?.endpoint_id)}, binding_generation=${sqlValue(binding?.generation)}, epoch=epoch+1, armed_at=NULL, last_nonwake_activity_at=NULL, activity_lease_expires_at=NULL, fired_epoch=NULL, sequence=0, last_triggered_at=NULL, next_due_at=NULL, updated_at=${sqlValue(now)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)};`,
      ]);
    } else {
      const nextDueAt = input.enabled && mode === "repeat" && existing.fired_epoch === existing.epoch
        ? addMs(now, repeatAfter)
        : null;
      this.db.transaction([
        `UPDATE activity_watches SET enabled=${sqlValue(input.enabled)}, idle_after_ms=${idleAfter}, mode=${sqlValue(mode)}, repeat_after_ms=${repeatAfter}, activity_lease_expires_at=${sqlValue(input.enabled ? existing.activity_lease_expires_at : null)}, next_due_at=${sqlValue(nextDueAt)}, updated_at=${sqlValue(now)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(channel)};`,
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
      VALUES(${sqlValue(this.config.instance_id)}, ${sqlValue(input.endpoint_id)}, ${input.generation}, ${sqlValue(input.observation_id)}, ${sqlValue(input.attention_channel)}, ${sqlValue(input.kind)}, ${sqlValue(observedAt)}, ${sqlValue(receivedAt)});`;
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
    let update = "SELECT changes();";
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
    this.db.transaction([
      `DELETE FROM activity_observations WHERE instance_id=${sqlValue(this.config.instance_id)} AND received_at<${sqlValue(addMs(receivedAt, -7 * 24 * 60 * 60_000))};`,
      insert,
      update,
    ]);
    return { duplicate: Boolean(before), watch: this.getActivityWatch(input.attention_channel) };
  }

  consumeWakeEcho(input: WakeEchoObservation): ActivityObservationResult {
    if (!input?.attention_channel || !input.endpoint_id || !input.observation_id || input.observation_id.length > 256
      || !Number.isSafeInteger(input.generation) || input.generation < 1) {
      throw new BridgeError("wake echo identity is invalid", "invalid_activity", 400);
    }
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
    const correlation = this.db.query<DbRow>(`SELECT dc.* FROM delivery_correlations dc JOIN batches b ON b.id=dc.batch_id
      WHERE b.instance_id=${sqlValue(this.config.instance_id)} AND dc.endpoint_id=${sqlValue(input.endpoint_id)}
        AND dc.binding_generation=${input.generation} AND dc.nonce_hash=${sqlValue(nonceHash)} LIMIT 1;`)[0];
    if (!correlation) throw new BridgeError("delivery nonce is unknown", "unknown_delivery_nonce", 409);
    if (String(correlation.state) !== "outstanding") throw new BridgeError("delivery nonce was already consumed or discarded", "delivery_nonce_replayed", 409);
    if (String(correlation.expires_at) <= now) {
      this.db.exec(`UPDATE delivery_correlations SET state='expired' WHERE attempt_id=${sqlValue(correlation.attempt_id)} AND state='outstanding';`);
      throw new BridgeError("delivery nonce has expired", "delivery_nonce_expired", 409);
    }
    const changed = this.db.query<DbRow>(`UPDATE delivery_correlations SET state='consumed', consumed_at=${sqlValue(now)}
      WHERE attempt_id=${sqlValue(correlation.attempt_id)} AND state='outstanding' RETURNING attempt_id;`)[0];
    if (!changed) throw new BridgeError("delivery nonce was already consumed", "delivery_nonce_replayed", 409);
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
    this.db.transaction([
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
       SELECT id, 'needs_attention', 'cancelled', 'all_claims_finalized_reconciled', ${sqlValue(now)} FROM batches
       WHERE instance_id=${sqlValue(this.config.instance_id)} AND state='needs_attention' AND last_error='all_claims_finalized'
         AND claim_ids_json='[]' AND event_ids_json='[]';`,
      `UPDATE batches SET state='cancelled', updated_at=${sqlValue(now)}
       WHERE instance_id=${sqlValue(this.config.instance_id)} AND state='needs_attention' AND last_error='all_claims_finalized'
         AND claim_ids_json='[]' AND event_ids_json='[]';`,
      `UPDATE batches SET state='retry_wait', not_before=${sqlValue(now)}, lease_expires_at=NULL, last_error='dispatcher_recovered', updated_at=${sqlValue(now)} WHERE instance_id=${sqlValue(this.config.instance_id)} AND state='dispatching' AND lease_expires_at IS NOT NULL AND lease_expires_at<${sqlValue(now)};`,
      `UPDATE outbox_attempts SET state='expired', finished_at=${sqlValue(now)}, error_class='dispatcher_recovered' WHERE state='leased' AND leased_until IS NOT NULL AND leased_until<${sqlValue(now)};`,
      `UPDATE delivery_correlations SET state='expired' WHERE state='outstanding' AND expires_at<${sqlValue(now)};`,
    ]);
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
      const predicate = `instance_id=${sqlValue(this.config.instance_id)} AND attention_channel=${sqlValue(watch.attention_channel)} AND enabled=1 AND epoch=${watch.epoch} AND ${watch.fired_epoch === watch.epoch ? repeatPredicate : initialPredicate} AND endpoint_id=${sqlValue(binding.endpoint_id)} AND binding_generation=${binding.generation}`;
      this.db.transaction([
        `INSERT OR IGNORE INTO events(id, instance_id, owner_id, source, type, schema_version, occurred_at, received_at, dedupe_key, coalesce_key, priority_hint, attention_channel_hint, actor_ref, resource_json, metadata_json, payload_preview, matched_policy_id, matched_policy_version)
         SELECT ${sqlValue(eventId)}, ${sqlValue(this.config.instance_id)}, ${sqlValue(this.config.owner_id)}, 'wakebridge.core', 'channel.inactive', 1, ${sqlValue(now)}, ${sqlValue(now)}, ${sqlValue(dedupeKey)}, ${sqlValue(watch.attention_channel)}, 'normal', ${sqlValue(watch.attention_channel)}, ${sqlValue(this.config.owner_id)}, ${sqlJson(resource)}, ${sqlJson({ origin: "inactivity_watch", reason_code: "host_inactive", epoch: watch.epoch, idle_after_ms: watch.idle_after_ms, mode: watch.mode, sequence })}, NULL, 'inactivity_watch', 1 FROM activity_watches WHERE ${predicate};`,
        `INSERT OR IGNORE INTO event_status(event_id, state, updated_at) SELECT ${sqlValue(eventId)}, 'matched', ${sqlValue(now)} FROM events WHERE id=${sqlValue(eventId)};`,
        `INSERT INTO event_transitions(event_id, from_state, to_state, reason, at) SELECT ${sqlValue(eventId)}, NULL, 'matched', 'inactivity_watch', ${sqlValue(now)} WHERE EXISTS (SELECT 1 FROM events WHERE id=${sqlValue(eventId)}) AND NOT EXISTS (SELECT 1 FROM event_transitions WHERE event_id=${sqlValue(eventId)});`,
        `INSERT OR IGNORE INTO claims(id, instance_id, origin, event_ids_json, resource_json, policy_id, policy_version, attention_channel, eligible_after, expires_at, defer_while_presence, state, reason_code, note, created_at, updated_at, snooze_until, consumed_result_json, dismissed_reason)
         SELECT ${sqlValue(claimId)}, ${sqlValue(this.config.instance_id)}, 'inactivity_watch', ${sqlJson([eventId])}, ${sqlJson(resource)}, 'inactivity_watch', 1, ${sqlValue(watch.attention_channel)}, ${sqlValue(now)}, NULL, 1, 'pending', 'host_inactive', NULL, ${sqlValue(now)}, ${sqlValue(now)}, NULL, NULL, NULL FROM events WHERE id=${sqlValue(eventId)};`,
        `INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at) SELECT ${sqlValue(claimId)}, NULL, 'pending', 'inactivity_watch', ${sqlValue(now)} WHERE EXISTS (SELECT 1 FROM claims WHERE id=${sqlValue(claimId)}) AND NOT EXISTS (SELECT 1 FROM claim_transitions WHERE claim_id=${sqlValue(claimId)});`,
        `UPDATE activity_watches SET fired_epoch=epoch, sequence=${sequence}, last_triggered_at=${sqlValue(now)}, next_due_at=${sqlValue(watch.mode === "repeat" ? addMs(now, watch.repeat_after_ms) : null)}, activity_lease_expires_at=NULL, updated_at=${sqlValue(now)} WHERE ${predicate} AND EXISTS (SELECT 1 FROM claims WHERE id=${sqlValue(claimId)});`,
      ]);
      const after = this.getActivityWatch(watch.attention_channel);
      if (after?.sequence === sequence && this.getClaim(claimId)) created.push(claimId);
    }
    return created;
  }

  /** Advance due claims and freeze eligible claims into durable batches. */
  tick(): TickResult {
    const nowDate = this.now();
    const now = nowDate.toISOString();
    const result: TickResult = { inactivity_claims_created: this.materializeDueActivityWatches(now), expired_claims: [], deferred_claims: [], eligible_claims: [], batches_created: [], batches_updated: [] };
    const claims = this.listClaims();
    const transitions: string[] = [];
    for (const claim of claims) {
      if (["consumed", "dismissed", "expired", "batched"].includes(claim.state)) continue;
      if (claim.expires_at && claim.expires_at <= now) {
        transitions.push(...this.claimStateSql(claim, "expired", now, "expiry"));
        for (const eventId of claim.event_ids) transitions.push(...this.eventStateSql(eventId, "expired", now, "claim_expired"));
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

    const eligible = this.listClaims({ state: "eligible", limit: 100_000 });
    const events = new Map<string, WakeEvent>();
    for (const claim of eligible) {
      for (const eventId of claim.event_ids) {
        const event = this.getEvent(eventId);
        if (event) events.set(event.id, event);
      }
    }
    const open = this.listBatches({ states: ["pending", "waiting_for_endpoint", "retry_wait"] });
    const openByKey = new Map<string, WakeBatch>();
    for (const batch of open) {
      if (!batch.deadline || batch.deadline > now) openByKey.set(this.batchKey(batch.policy_id, batch.policy_version, batch.attention_channel, batch.coalesce_key ?? null), batch);
    }
    const batchStatements: string[] = [];
    for (const claim of eligible) {
      const policy = this.policy(claim.policy_id, claim.policy_version) ?? normalizePolicy(DEFAULT_POLICY);
      const event = events.get(claim.event_ids[0]);
      const coalesce = this.coalesceKey(policy, claim, event);
      const key = this.batchKey(claim.policy_id, claim.policy_version, claim.attention_channel, coalesce);
      const maxEvents = Math.max(1, Math.floor(policy.batch?.max_events ?? 20));
      let batch = openByKey.get(key);
      if (batch && batch.claim_ids.length < maxEvents && batch.deadline && batch.deadline > now) {
        const claimIds = arrayWithoutDuplicates([...batch.claim_ids, claim.id]);
        const eventIds = arrayWithoutDuplicates([...batch.event_ids, ...claim.event_ids]);
        batchStatements.push(...this.batchUpdateArraysSql(batch, claimIds, eventIds, now));
        batch.claim_ids = claimIds;
        batch.event_ids = eventIds;
        batch.updated_at = now;
        batchStatements.push(...this.claimStateSql(claim, "batched", now, `batch:${batch.id}`));
        for (const eventId of claim.event_ids) batchStatements.push(...this.eventStateSql(eventId, "batched", now, `batch:${batch.id}`));
        result.batches_updated.push(batch.id);
        continue;
      }
      const windowMs = assertFiniteMs(policy.batch?.window_ms, "batch.window_ms", this.config.batch_window_ms!);
      const batchId = id("wb");
      const deadline = addMs(nowDate, windowMs);
      const newBatch: WakeBatch = {
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
        deadline,
        attempt: 0,
        binding_generation: null,
        created_at: now,
        updated_at: now,
        lease_expires_at: null,
        last_error: null,
      };
      batchStatements.push(this.batchInsertSql(newBatch));
      batchStatements.push(`INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) VALUES(${sqlValue(batchId)}, NULL, 'pending', 'freeze', ${sqlValue(now)});`);
      batchStatements.push(...this.claimStateSql(claim, "batched", now, `batch:${batchId}`));
      for (const eventId of claim.event_ids) batchStatements.push(...this.eventStateSql(eventId, "batched", now, `batch:${batchId}`));
      openByKey.set(key, newBatch);
      result.batches_created.push(batchId);
    }
    if (batchStatements.length) this.db.transaction(batchStatements);
    return result;
  }

  private claimStateSql(claim: AttentionClaim, state: ClaimState, now: string, reason: string): string[] {
    return [
      `UPDATE claims SET state=${sqlValue(state)}, updated_at=${sqlValue(now)} WHERE id=${sqlValue(claim.id)} AND state=${sqlValue(claim.state)};`,
      `INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at) SELECT ${sqlValue(claim.id)}, ${sqlValue(claim.state)}, ${sqlValue(state)}, ${sqlValue(reason)}, ${sqlValue(now)} WHERE changes()>0;`,
    ];
  }

  private claimDeferSql(claim: AttentionClaim, eligibleAfter: string, now: string, reason: string): string[] {
    return [
      `UPDATE claims SET state='deferred', eligible_after=${sqlValue(eligibleAfter)}, updated_at=${sqlValue(now)} WHERE id=${sqlValue(claim.id)} AND state IN ('pending','deferred','eligible');`,
      `INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at) SELECT ${sqlValue(claim.id)}, ${sqlValue(claim.state)}, 'deferred', ${sqlValue(reason)}, ${sqlValue(now)} WHERE changes()>0;`,
    ];
  }

  private eventStateSql(eventId: string, state: EventState, now: string, reason: string): string[] {
    const current = this.getEvent(eventId)?.state;
    if (!current || current === state) return [];
    return [
      `UPDATE event_status SET state=${sqlValue(state)}, updated_at=${sqlValue(now)} WHERE event_id=${sqlValue(eventId)};`,
      `INSERT INTO event_transitions(event_id, from_state, to_state, reason, at) VALUES(${sqlValue(eventId)}, ${sqlValue(current)}, ${sqlValue(state)}, ${sqlValue(reason)}, ${sqlValue(now)});`,
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

  private batchInsertSql(batch: WakeBatch): string {
    return `INSERT INTO batches(id, instance_id, policy_id, policy_version, attention_channel, claim_ids_json, event_ids_json, coalesce_key, state, not_before, deadline, attempt, binding_generation, created_at, updated_at, lease_expires_at, last_error)
      VALUES(${sqlValue(batch.id)}, ${sqlValue(batch.instance_id)}, ${sqlValue(batch.policy_id)}, ${batch.policy_version}, ${sqlValue(batch.attention_channel)}, ${sqlJson(batch.claim_ids)}, ${sqlJson(batch.event_ids)}, ${sqlValue(batch.coalesce_key)}, ${sqlValue(batch.state)}, ${sqlValue(batch.not_before)}, ${sqlValue(batch.deadline)}, ${batch.attempt}, NULL, ${sqlValue(batch.created_at)}, ${sqlValue(batch.updated_at)}, NULL, NULL);`;
  }

  private batchUpdateArraysSql(batch: WakeBatch, claimIds: string[], eventIds: string[], now: string): string[] {
    return [
      `UPDATE batches SET claim_ids_json=${sqlJson(claimIds)}, event_ids_json=${sqlJson(eventIds)}, updated_at=${sqlValue(now)} WHERE id=${sqlValue(batch.id)} AND state IN ('pending','waiting_for_endpoint','retry_wait');`,
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) SELECT ${sqlValue(batch.id)}, state, state, 'append_claim', ${sqlValue(now)} FROM batches WHERE id=${sqlValue(batch.id)} AND changes()>0;`,
    ];
  }

  getBatch(batchId: string): WakeBatch | null {
    const row = this.db.query<DbRow>(`SELECT * FROM batches WHERE id=${sqlValue(batchId)} AND instance_id=${sqlValue(this.config.instance_id)} LIMIT 1;`)[0];
    return row ? this.batchFromRow(row) : null;
  }

  listBatches(filters: { states?: BatchState[]; channel?: string; limit?: number } = {}): WakeBatch[] {
    const clauses = [`instance_id=${sqlValue(this.config.instance_id)}`];
    if (filters.states?.length) clauses.push(`state IN (${filters.states.map(sqlValue).join(",")})`);
    if (filters.channel) clauses.push(`attention_channel=${sqlValue(filters.channel)}`);
    const limit = Math.min(1000, Math.max(1, Math.floor(filters.limit ?? 1000)));
    return this.db.query<DbRow>(`SELECT * FROM batches WHERE ${clauses.join(" AND ")} ORDER BY created_at ASC LIMIT ${limit};`).map((row) => this.batchFromRow(row));
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
    const claims = batch.claim_ids.map((claimId) => this.getClaim(claimId)).filter((claim): claim is AttentionClaim => Boolean(claim) && claim!.state === "batched");
    const events = new Map<string, WakeEvent>();
    for (const claim of claims) {
      for (const eventId of claim.event_ids) {
        const event = this.getEvent(eventId);
        if (event) events.set(event.id, event);
      }
    }
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
        };
      }),
      binding_generation: binding.generation,
    };
  }

  private markNoEndpoint(batch: WakeBatch, now: string): DispatchResult {
    this.db.transaction([
      `UPDATE batches SET state='waiting_for_endpoint', updated_at=${sqlValue(now)}, last_error='no_current_binding' WHERE id=${sqlValue(batch.id)} AND state IN ('pending','retry_wait');`,
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) SELECT ${sqlValue(batch.id)}, state, 'waiting_for_endpoint', 'no_endpoint', ${sqlValue(now)} FROM batches WHERE id=${sqlValue(batch.id)} AND changes()>0;`,
    ]);
    return { batch_id: batch.id, status: "waiting_for_endpoint" };
  }

  private acquireBatch(batch: WakeBatch, binding: Binding, now: string): { batch: WakeBatch; attemptId: string } | null {
    const attemptId = id("attempt");
    const leaseUntil = addMs(now, this.config.dispatch_lease_ms!);
    const fromState = batch.state;
    const rows = this.db.query<DbRow>([
      "BEGIN IMMEDIATE;",
      `UPDATE batches SET state='dispatching', attempt=attempt+1, binding_generation=${binding.generation}, lease_expires_at=${sqlValue(leaseUntil)}, updated_at=${sqlValue(now)}, last_error=${sqlValue(attemptId)}
       WHERE id=${sqlValue(batch.id)} AND instance_id=${sqlValue(this.config.instance_id)} AND state IN ('pending','retry_wait','waiting_for_endpoint')
       AND not_before<=${sqlValue(now)} AND (state='waiting_for_endpoint' OR deadline IS NULL OR deadline<=${sqlValue(now)});`,
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
    const retryable = result.retryable !== false;
    const nextState: BatchState = accepted ? "dispatched" : retryable ? "retry_wait" : "dead_letter";
    const status: DispatchResult["status"] = accepted ? "accepted" : retryable ? "retry_wait" : "dead_letter";
    const errorClass = result.error_class || (accepted ? null : "transport_rejected");
    const errorMessage = result.error_message || null;
    const requestedRetry = result.retry_after_ms;
    const retryDelay = requestedRetry !== undefined
      && Number.isFinite(requestedRetry)
      && requestedRetry >= 0
      ? Math.min(requestedRetry, 24 * 60 * 60_000)
      : this.config.retry_delay_ms!;
    const nextNotBefore = accepted ? batch.not_before : addMs(now, retryDelay);
    const statements = [
      `UPDATE outbox_attempts SET state=${sqlValue(accepted ? "accepted" : "failed")}, transport_kind=${sqlValue(result.transport_kind || transportKind)}, finished_at=${sqlValue(now)}, error_class=${sqlValue(errorClass)}, error_message=${sqlValue(errorMessage)} WHERE id=${sqlValue(attemptId)} AND state='leased';`,
    ];
    if (!accepted) statements.push(`UPDATE delivery_correlations SET state='discarded' WHERE attempt_id=${sqlValue(attemptId)} AND state='outstanding';`);
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
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) SELECT ${sqlValue(batch.id)}, 'dispatching', ${sqlValue(nextState)}, ${sqlValue(errorClass || "transport_accepted")}, ${sqlValue(now)} WHERE changes()>0;`,
    );
    this.db.transaction(statements);
    const current = this.getBatch(batch.id);
    if (!current || current.state !== nextState || current.last_error !== errorClass) {
      return { batch_id: batch.id, status: "skipped", attempt_id: attemptId, error_class: "stale_attempt" };
    }
    return { batch_id: batch.id, status, attempt_id: attemptId, endpoint_id: binding.endpoint_id, generation: binding.generation, error_class: errorClass ?? undefined };
  }

  /** Dispatch due durable batches. The lease CAS prevents double-dispatch. */
  async dispatchDue(nowDate = this.now()): Promise<DispatchResult[]> {
    const now = nowDate.toISOString();
    // A worker can disappear without a process restart in the daemon (for
    // example, an adapter promise is rejected).  Expired leases are safe to
    // reclaim on every sweep; the attempt id CAS below still protects a live
    // worker from a second dispatcher.
    this.recover();
    const batches = this.listBatches({ states: ["pending", "waiting_for_endpoint", "retry_wait"] });
    const results: DispatchResult[] = [];
    for (const listed of batches) {
      if (listed.not_before > now) continue;
      if (listed.state !== "waiting_for_endpoint" && listed.deadline && listed.deadline > now) continue;
      const resolved = this.resolveBinding(listed.attention_channel, now);
      if (!resolved) {
        results.push(this.markNoEndpoint(listed, now));
        continue;
      }
      const transport = this.chooseTransport(listed, resolved.endpoint);
      if (!transport) {
        this.db.transaction([
          `UPDATE batches SET state='waiting_for_endpoint', last_error='no_compatible_transport', updated_at=${sqlValue(now)} WHERE id=${sqlValue(listed.id)} AND state IN ('pending','retry_wait');`,
          `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) SELECT ${sqlValue(listed.id)}, state, 'waiting_for_endpoint', 'no_transport', ${sqlValue(now)} FROM batches WHERE id=${sqlValue(listed.id)} AND changes()>0;`,
        ]);
        results.push({ batch_id: listed.id, status: "waiting_for_endpoint", error_class: "no_compatible_transport" });
        continue;
      }
      const acquired = this.acquireBatch(listed, resolved.binding, now);
      if (!acquired) {
        results.push({ batch_id: listed.id, status: "skipped" });
        continue;
      }
      if (!this.bindingStillCurrent(resolved.binding, now)) {
        this.db.transaction([
          `UPDATE outbox_attempts SET state='failed', finished_at=${sqlValue(now)}, error_class='stale_generation', error_message='binding changed before dispatch' WHERE id=${sqlValue(acquired.attemptId)} AND state='leased';`,
          `UPDATE batches SET state='retry_wait', not_before=${sqlValue(addMs(now, this.config.retry_delay_ms!))}, lease_expires_at=NULL, updated_at=${sqlValue(now)}, last_error='stale_generation' WHERE id=${sqlValue(listed.id)} AND state='dispatching';`,
          `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) SELECT ${sqlValue(listed.id)}, 'dispatching', 'retry_wait', 'stale_generation', ${sqlValue(now)} WHERE changes()>0;`,
        ]);
        results.push({ batch_id: listed.id, status: "stale_generation", attempt_id: acquired.attemptId, endpoint_id: resolved.binding.endpoint_id, generation: resolved.binding.generation, error_class: "stale_generation" });
        continue;
      }
      const payload = this.buildPayload(acquired.batch, resolved.binding);
      if (!payload.claim_refs.length) {
        this.db.transaction([
          `UPDATE outbox_attempts SET state='failed', finished_at=${sqlValue(now)}, error_class='empty_batch', error_message='all claims were finalized before dispatch' WHERE id=${sqlValue(acquired.attemptId)} AND state='leased';`,
          `UPDATE batches SET state='needs_attention', lease_expires_at=NULL, updated_at=${sqlValue(now)}, last_error='empty_batch' WHERE id=${sqlValue(listed.id)} AND state='dispatching';`,
        ]);
        results.push({ batch_id: listed.id, status: "needs_attention", attempt_id: acquired.attemptId, error_class: "empty_batch" });
        continue;
      }
      const deliveryNonce = randomBytes(24).toString("base64url");
      if (transport.capabilities?.session_activity_observable) {
        const correlationCreatedAt = this.nowIso();
        this.db.transaction([
          `INSERT INTO delivery_correlations(attempt_id, batch_id, endpoint_id, binding_generation, transport_kind, nonce_hash, state, created_at, expires_at, consumed_at)
           VALUES(${sqlValue(acquired.attemptId)}, ${sqlValue(acquired.batch.id)}, ${sqlValue(resolved.binding.endpoint_id)}, ${resolved.binding.generation}, ${sqlValue(transport.kind)}, ${sqlValue(sha256(deliveryNonce))}, 'outstanding', ${sqlValue(correlationCreatedAt)}, ${sqlValue(addMs(correlationCreatedAt, 60 * 60_000))}, NULL);`,
        ]);
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
      const finalizedAt = this.nowIso();
      const final = this.finalizeAttempt(acquired.attemptId, acquired.batch, resolved.binding, transportResult, transport.kind, finalizedAt);
      final.endpoint_id = resolved.binding.endpoint_id;
      final.generation = resolved.binding.generation;
      results.push(final);
    }
    return results;
  }

  private receiptInsertSql(receipt: Receipt): string {
    return `INSERT INTO receipts(id, batch_id, claim_id, stage, at, endpoint_id, binding_generation, transport_kind, details_json)
      VALUES(${sqlValue(receipt.id)}, ${sqlValue(receipt.batch_id)}, ${sqlValue(receipt.claim_id)}, ${sqlValue(receipt.stage)}, ${sqlValue(receipt.at)}, ${sqlValue(receipt.endpoint_id)}, ${sqlValue(receipt.binding_generation)}, ${sqlValue(receipt.transport_kind)}, ${sqlJson(receipt.details)});`;
  }

  private detachClaimFromOpenBatches(claim: AttentionClaim, now: string): string[] {
    const statements: string[] = [];
    for (const batch of this.listBatches({ limit: 1000 })) {
      if (!batch.claim_ids.includes(claim.id) || !["pending", "waiting_for_endpoint", "retry_wait"].includes(batch.state)) continue;
      const claimIds = batch.claim_ids.filter((idValue) => idValue !== claim.id);
      const eventIds = arrayWithoutDuplicates(batch.event_ids.filter((eventId) => !claim.event_ids.includes(eventId) || claimIds.some((otherClaimId) => this.getClaim(otherClaimId)?.event_ids.includes(eventId))));
      if (claimIds.length) {
        statements.push(...this.batchUpdateArraysSql(batch, claimIds, eventIds, now));
      } else {
        statements.push(
          `UPDATE batches SET claim_ids_json='[]', event_ids_json='[]', state='cancelled', updated_at=${sqlValue(now)}, last_error='all_claims_finalized' WHERE id=${sqlValue(batch.id)} AND state IN ('pending','waiting_for_endpoint','retry_wait');`,
          `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) SELECT ${sqlValue(batch.id)}, ${sqlValue(batch.state)}, 'cancelled', 'all_claims_finalized', ${sqlValue(now)} FROM batches WHERE id=${sqlValue(batch.id)} AND changes()>0;`,
        );
      }
    }
    return statements;
  }

  listReceipts(batchId?: string): Receipt[] {
    const where = [`b.instance_id=${sqlValue(this.config.instance_id)}`];
    if (batchId) where.push(`r.batch_id=${sqlValue(batchId)}`);
    return this.db.query<DbRow>(`SELECT r.* FROM receipts r JOIN batches b ON b.id=r.batch_id WHERE ${where.join(" AND ")} ORDER BY r.at ASC, r.id ASC;`).map((row) => ({
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
    const where = ["1=1"];
    if (batchId) where.push(`batch_id=${sqlValue(batchId)}`);
    return this.db.query<DbRow>(`SELECT * FROM outbox_attempts WHERE ${where.join(" AND ")} ORDER BY started_at ASC, attempt_no ASC;`);
  }

  private acceptedAttempt(batchId: string, attemptNo?: number): DbRow | null {
    const clauses = [`batch_id=${sqlValue(batchId)}`, "state='accepted'"];
    if (attemptNo != null) clauses.push(`attempt_no=${attemptNo}`);
    return this.db.query<DbRow>(`SELECT * FROM outbox_attempts WHERE ${clauses.join(" AND ")} ORDER BY attempt_no DESC LIMIT 1;`)[0] ?? null;
  }

  ackBatch(batchId: string, endpointId: string, generation: number): WakeBatch {
    const batch = this.getBatch(batchId);
    if (!batch) throw new BridgeError("batch not found", "batch_not_found", 404);
    if (batch.state === "seen") return batch;
    if (batch.state !== "dispatched") throw new BridgeError("batch acknowledgement is not available", "batch_not_ready", 409);
    // The accepted attempt must be the one that finalized this batch.  A
    // previous accepted attempt may still be present after lease recovery;
    // accepting its late acknowledgement would incorrectly mark a retry as
    // seen.
    const accepted = this.acceptedAttempt(batchId, batch.attempt);
    if (!accepted || String(accepted.endpoint_id) !== endpointId || Number(accepted.binding_generation) !== generation) {
      throw new BridgeError("batch acknowledgement is fenced by endpoint/generation", "stale_generation", 409);
    }
    const binding = this.getBinding(batch.attention_channel);
    if (!binding || binding.endpoint_id !== endpointId || binding.generation !== generation) throw new BridgeError("batch acknowledgement targets stale binding", "stale_generation", 409);
    const now = this.nowIso();
    this.db.transaction([
      `UPDATE batches SET state='seen', updated_at=${sqlValue(now)}, lease_expires_at=NULL WHERE id=${sqlValue(batchId)} AND instance_id=${sqlValue(this.config.instance_id)} AND state='dispatched' AND attempt=${Number(accepted.attempt_no)} AND binding_generation=${generation};`,
      // Keep the transition INSERT immediately after the CAS so changes()
      // refers to that UPDATE.  The receipt follows it and is conditional on
      // the transition INSERT, so a stale/concurrent ack cannot leave a
      // misleading agent_seen receipt behind.
      `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at) SELECT ${sqlValue(batchId)}, 'dispatched', 'seen', 'agent_ack', ${sqlValue(now)} WHERE changes()>0;`,
      `INSERT INTO receipts(id, batch_id, claim_id, stage, at, endpoint_id, binding_generation, transport_kind, details_json)
        SELECT ${sqlValue(id("rcpt"))}, ${sqlValue(batchId)}, NULL, 'agent_seen', ${sqlValue(now)}, ${sqlValue(endpointId)}, ${generation}, ${sqlValue(String(accepted.transport_kind || "unknown"))}, NULL
        WHERE changes()>0;`,
    ]);
    const current = this.getBatch(batchId);
    if (!current) throw new BridgeError("batch not found", "batch_not_found", 404);
    if (current.state === "seen") return current;
    throw new BridgeError("batch acknowledgement was fenced by a concurrent state change", "batch_not_ready", 409);
  }

  recordReceipt(batchId: string, stage: ReceiptStage, details?: Record<string, JsonValue> | null, endpointId?: string, generation?: number): Receipt {
    const batch = this.getBatch(batchId);
    if (!batch) throw new BridgeError("batch not found", "batch_not_found", 404);
    if (["agent_seen", "agent_consumed", "agent_acted"].includes(stage) && endpointId && generation != null) {
      const binding = this.getBinding(batch.attention_channel);
      if (!binding || binding.endpoint_id !== endpointId || binding.generation !== generation) throw new BridgeError("receipt target is stale", "stale_generation", 409);
    }
    const receipt: Receipt = { id: id("rcpt"), batch_id: batchId, stage, at: this.nowIso(), endpoint_id: endpointId ?? null, binding_generation: generation ?? null, transport_kind: null, details: details ?? null };
    this.db.exec(this.receiptInsertSql(receipt));
    return receipt;
  }

  private claimAction(claimId: string, action: "snooze" | "dismiss" | "consume", value?: string | JsonValue | null): AttentionClaim {
    const claim = this.getClaim(claimId);
    if (!claim) throw new BridgeError("claim not found", "claim_not_found", 404);
    if (["consumed", "dismissed", "expired"].includes(claim.state)) return claim;
    const now = this.nowIso();
    const nextState: ClaimState = action === "snooze" ? "pending" : action === "dismiss" ? "dismissed" : "consumed";
    const batch = this.listBatches({ limit: 1000 }).find((item) => item.claim_ids.includes(claimId));
    const statements = [
      `UPDATE claims SET state=${sqlValue(nextState)}, eligible_after=${sqlValue(action === "snooze" ? String(value) : claim.eligible_after)}, snooze_until=${sqlValue(action === "snooze" ? String(value) : null)}, consumed_result_json=${sqlJson(action === "consume" ? value : null)}, dismissed_reason=${sqlValue(action === "dismiss" ? String(value || "dismissed") : null)}, updated_at=${sqlValue(now)} WHERE id=${sqlValue(claimId)} AND state=${sqlValue(claim.state)};`,
      `INSERT INTO claim_transitions(claim_id, from_state, to_state, reason, at) SELECT ${sqlValue(claimId)}, ${sqlValue(claim.state)}, ${sqlValue(nextState)}, ${sqlValue(action)}, ${sqlValue(now)} WHERE changes()>0;`,
    ];
    statements.push(...this.detachClaimFromOpenBatches(claim, now));
    if (action !== "snooze") {
      for (const eventId of claim.event_ids) statements.push(...this.eventStateSql(eventId, "consumed", now, action));
    }
    if (batch && action !== "snooze") {
      statements.push(this.receiptInsertSql({ id: id("rcpt"), batch_id: batch.id, claim_id: claimId, stage: "agent_consumed", at: now, endpoint_id: null, binding_generation: null, transport_kind: null, details: action === "dismiss" ? { reason: String(value || "dismissed") } : { result: (value as JsonValue) ?? null } }));
    }
    this.db.transaction(statements);
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
      events: this.listEvents({ limit: 1000 }),
      claims: this.listClaims(),
      batches: this.listBatches(),
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
