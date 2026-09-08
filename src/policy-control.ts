import { readFileSync } from "node:fs";
import { BridgeError, normalizePolicy } from "./core.js";
import { stableJson } from "./util.js";
import type { PolicyFile, PolicyRule } from "./types.js";

const POLICY_KEYS = new Set(["id", "version", "enabled", "order", "match", "delivery", "batch", "target", "reason_code", "expires_after_ms"]);
const DELIVERY_KEYS = new Set(["mode", "scheduled_local_time", "quiet_hours_policy", "foreground_presence_policy"]);
const BATCH_KEYS = new Set(["coalesce_by", "max_events", "window_ms"]);
const TARGET_KEYS = new Set(["attention_channel"]);
const PUBLIC_MODES = new Set(["immediate", "scheduled", "suppress"]);
const POLICY_ID = /^[a-z][a-z0-9._-]{0,127}$/u;
const LOCAL_TIME = /^(?:[01]\d|2[0-3]):[0-5]\d$/u;

function object(value: unknown, field: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new BridgeError(`${field} must be an object`, "invalid_policy_file", 400);
  }
  return value as Record<string, unknown>;
}

function rejectUnknown(value: Record<string, unknown>, allowed: Set<string>, field: string): void {
  const unknown = Object.keys(value).filter((key) => !allowed.has(key));
  if (unknown.length) throw new BridgeError(`${field} contains unsupported field(s): ${unknown.join(", ")}`, "unsupported_policy_field", 400);
}

function publicPolicy(value: unknown, index: number): PolicyRule {
  const raw = object(value, `policies[${index}]`);
  rejectUnknown(raw, POLICY_KEYS, `policies[${index}]`);
  if (typeof raw.id !== "string" || !POLICY_ID.test(raw.id)) {
    throw new BridgeError(`policies[${index}].id must match ${POLICY_ID}`, "invalid_policy_file", 400);
  }
  if (!Number.isSafeInteger(raw.version) || Number(raw.version) < 1) {
    throw new BridgeError(`policies[${index}].version must be a positive safe integer`, "invalid_policy_file", 400);
  }
  if (raw.order != null && (!Number.isSafeInteger(raw.order) || Math.abs(Number(raw.order)) > 1_000_000)) {
    throw new BridgeError(`policies[${index}].order must be an integer between -1000000 and 1000000`, "invalid_policy_file", 400);
  }
  if (raw.match != null) {
    object(raw.match, `policies[${index}].match`);
    if (JSON.stringify(raw.match).length > 8192) throw new BridgeError(`policies[${index}].match is too large`, "invalid_policy_file", 400);
  }
  const delivery = object(raw.delivery, `policies[${index}].delivery`);
  rejectUnknown(delivery, DELIVERY_KEYS, `policies[${index}].delivery`);
  if (typeof delivery.mode !== "string" || !PUBLIC_MODES.has(delivery.mode)) {
    throw new BridgeError(
      `policies[${index}].delivery.mode must be immediate, scheduled, or suppress; debounce and digest are not supported by policy v1`,
      "unsupported_policy_mode",
      400,
    );
  }
  if (delivery.mode === "scheduled" && (typeof delivery.scheduled_local_time !== "string" || !LOCAL_TIME.test(delivery.scheduled_local_time))) {
    throw new BridgeError(`policies[${index}].delivery.scheduled_local_time must be HH:MM`, "invalid_policy_file", 400);
  }
  if (delivery.mode !== "scheduled" && delivery.scheduled_local_time != null) {
    throw new BridgeError(`policies[${index}].delivery.scheduled_local_time is only valid for scheduled mode`, "invalid_policy_file", 400);
  }
  if (delivery.quiet_hours_policy != null && !["defer", "bypass"].includes(String(delivery.quiet_hours_policy))) {
    throw new BridgeError(`policies[${index}].delivery.quiet_hours_policy must be defer or bypass`, "invalid_policy_file", 400);
  }
  if (delivery.foreground_presence_policy != null && !["defer", "bypass"].includes(String(delivery.foreground_presence_policy))) {
    throw new BridgeError(`policies[${index}].delivery.foreground_presence_policy must be defer or bypass`, "invalid_policy_file", 400);
  }
  if (raw.batch != null) {
    const batch = object(raw.batch, `policies[${index}].batch`);
    rejectUnknown(batch, BATCH_KEYS, `policies[${index}].batch`);
    if (batch.coalesce_by != null && (typeof batch.coalesce_by !== "string" || !batch.coalesce_by || batch.coalesce_by.length > 256)) {
      throw new BridgeError(`policies[${index}].batch.coalesce_by must be a non-empty bounded string`, "invalid_policy_file", 400);
    }
    if (batch.max_events != null && (!Number.isSafeInteger(batch.max_events) || Number(batch.max_events) < 1 || Number(batch.max_events) > 1000)) {
      throw new BridgeError(`policies[${index}].batch.max_events must be an integer between 1 and 1000`, "invalid_policy_file", 400);
    }
    if (batch.window_ms != null && (!Number.isSafeInteger(batch.window_ms) || Number(batch.window_ms) < 0 || Number(batch.window_ms) > 7 * 24 * 60 * 60_000)) {
      throw new BridgeError(`policies[${index}].batch.window_ms must be an integer between 0 and 604800000`, "invalid_policy_file", 400);
    }
  }
  if (raw.target != null) {
    const target = object(raw.target, `policies[${index}].target`);
    rejectUnknown(target, TARGET_KEYS, `policies[${index}].target`);
    if (target.attention_channel != null && (typeof target.attention_channel !== "string" || !target.attention_channel || target.attention_channel.length > 200)) {
      throw new BridgeError(`policies[${index}].target.attention_channel must be a non-empty string of at most 200 characters`, "invalid_policy_file", 400);
    }
  }
  if (raw.reason_code != null && (typeof raw.reason_code !== "string" || !raw.reason_code || raw.reason_code.length > 128)) {
    throw new BridgeError(`policies[${index}].reason_code must be a non-empty string of at most 128 characters`, "invalid_policy_file", 400);
  }
  if (raw.expires_after_ms != null && (!Number.isSafeInteger(raw.expires_after_ms) || Number(raw.expires_after_ms) < 0)) {
    throw new BridgeError(`policies[${index}].expires_after_ms must be a non-negative safe integer`, "invalid_policy_file", 400);
  }
  if (raw.id === "default" && (raw.enabled === false || stableJson(raw.match ?? {}) !== "{}")) {
    throw new BridgeError("policy file requires one enabled default policy with an empty match", "missing_default_policy", 400);
  }
  return normalizePolicy(raw as unknown as PolicyRule);
}

export function validatePolicyFile(value: unknown): PolicyFile {
  const raw = object(value, "policy file");
  rejectUnknown(raw, new Set(["schema_version", "policies"]), "policy file");
  if (raw.schema_version !== 1) throw new BridgeError("policy file schema_version must be 1", "unsupported_policy_schema", 400);
  if (!Array.isArray(raw.policies) || raw.policies.length < 1 || raw.policies.length > 256) {
    throw new BridgeError("policy file policies must contain between 1 and 256 rules", "invalid_policy_file", 400);
  }
  const policies = raw.policies.map(publicPolicy);
  const ids = new Set<string>();
  for (const policy of policies) {
    if (ids.has(policy.id)) throw new BridgeError(`policy file contains more than one version of ${policy.id}`, "duplicate_policy_id", 400);
    ids.add(policy.id);
  }
  const defaultPolicy = policies.find((policy) => policy.id === "default");
  if (!defaultPolicy || defaultPolicy.enabled === false || stableJson(defaultPolicy.match ?? {}) !== "{}") {
    throw new BridgeError("policy file requires one enabled default policy with an empty match", "missing_default_policy", 400);
  }
  return { schema_version: 1, policies };
}

export function loadPolicyFile(path: string): PolicyFile {
  if (!path) throw new BridgeError("policy file path is required", "invalid_policy_file", 400);
  let value: unknown;
  try {
    value = JSON.parse(readFileSync(path, "utf8"));
  } catch (error) {
    throw new BridgeError(`cannot read policy file ${path}: ${String(error)}`, "invalid_policy_file", 400);
  }
  return validatePolicyFile(value);
}
