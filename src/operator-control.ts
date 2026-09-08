import { BridgeError, type WakeBridge } from "./core.js";
import { sqlValue } from "./db.js";
import type {
  BatchState,
  SourceSupervisorStatus,
  WakeBatch,
} from "./types.js";
import type { HostAdapterManifest } from "./transport-sdk.js";

const BATCH_STATES: BatchState[] = [
  "pending",
  "waiting_for_endpoint",
  "waiting_for_waiter",
  "dispatching",
  "retry_wait",
  "dispatched",
  "seen",
  "cancelled",
  "needs_attention",
  "dead_letter",
];
const RETRY_REASON = /^[^\u0000-\u001f\u007f]{1,200}$/u;
const SAFE_ERROR_CLASS = /^[A-Za-z][A-Za-z0-9_.:-]{0,127}$/u;

type Row = Record<string, unknown>;
export type OperatorHealth = "healthy" | "degraded" | "needs_attention";

export interface OperatorBatchIssue {
  batch_id: string;
  state: "dead_letter" | "needs_attention";
  attention_channel: string;
  attempt: number;
  updated_at: string;
  error_class: string | null;
}

export interface OperatorStatus {
  schema_version: 1;
  generated_at: string;
  ok: boolean;
  health: OperatorHealth;
  attention_required: boolean;
  instance: { instance_id: string; owner_id: string; timezone: string };
  queue: {
    counts: Record<BatchState, number>;
    due: number;
    expired_dispatch_leases: number;
    failed_attempts_by_class: Record<string, number>;
    dead_letters: OperatorBatchIssue[];
    needs_attention: OperatorBatchIssue[];
  };
  hosts: {
    endpoints: { total: number; live: number; expired: number };
    bindings: { total: number; live: number; stale: number };
    channels: Array<{
      attention_channel: string;
      endpoint_id: string;
      generation: number;
      host_kind: string | null;
      lease_expires_at: string | null;
      live: boolean;
    }>;
    adapters: Array<Pick<HostAdapterManifest, "adapter_kind" | "adapter_version" | "support_tier" | "host_kinds">>;
  };
  sources: {
    runtime_observed: boolean;
    counts: Record<"healthy" | "backoff" | "needs_attention" | "stopped" | "other", number>;
    items: Array<{
      source: string;
      enabled: boolean;
      runtime_state: string | null;
      last_error_class: string | null;
      consecutive_failures: number;
      checkpoint_revision: number;
      checkpoint_updated_at: string | null;
    }>;
  };
}

export interface RetryDeadLetterInput {
  batch_id: string;
  expected_attempt: number;
  reason: string;
}

export interface RetryDeadLetterResult {
  retried: boolean;
  batch: WakeBatch;
}

function groupedCounts(rows: Row[], key: string, value: string): Record<string, number> {
  return Object.fromEntries(rows.map((row) => [String(row[key]), Number(row[value])])) as Record<string, number>;
}

function safeErrorClass(value: unknown): string | null {
  if (value == null) return null;
  const candidate = String(value);
  return SAFE_ERROR_CLASS.test(candidate) ? candidate : "invalid_error_class";
}

function offlineSources(bridge: WakeBridge): OperatorStatus["sources"]["items"] {
  return bridge.db.query<Row>(`
    SELECT c.source, c.revision, c.updated_at, COALESCE(sc.enabled, 0) AS enabled
    FROM source_checkpoints c
    LEFT JOIN source_controls sc ON sc.instance_id=c.instance_id AND sc.source=c.source
    WHERE c.instance_id=${sqlValue(bridge.config.instance_id)}
    UNION
    SELECT sc.source, 0 AS revision, NULL AS updated_at, sc.enabled
    FROM source_controls sc
    WHERE sc.instance_id=${sqlValue(bridge.config.instance_id)}
      AND NOT EXISTS (
        SELECT 1 FROM source_checkpoints c WHERE c.instance_id=sc.instance_id AND c.source=sc.source
      )
    ORDER BY source;
  `).map((row) => ({
    source: String(row.source),
    enabled: Boolean(Number(row.enabled)),
    runtime_state: null,
    last_error_class: null,
    consecutive_failures: 0,
    checkpoint_revision: Number(row.revision),
    checkpoint_updated_at: row.updated_at == null ? null : String(row.updated_at),
  }));
}

function sourceItems(bridge: WakeBridge, sources?: SourceSupervisorStatus[]): OperatorStatus["sources"]["items"] {
  if (!sources) return offlineSources(bridge);
  const checkpoints = new Map(offlineSources(bridge).map((source) => [source.source, source]));
  return sources.map((source) => ({
    source: source.source,
    enabled: source.enabled,
    runtime_state: source.state,
    last_error_class: safeErrorClass(source.last_error_class),
    consecutive_failures: source.consecutive_failures,
    checkpoint_revision: source.checkpoint_revision,
    checkpoint_updated_at: checkpoints.get(source.source)?.checkpoint_updated_at ?? null,
  }));
}

/** Secret-free read model for operators. It never recovers leases or mutates queue state. */
export function operatorStatus(
  bridge: WakeBridge,
  options: { sources?: SourceSupervisorStatus[]; host_adapters?: HostAdapterManifest[] } = {},
): OperatorStatus {
  const now = bridge.nowIso();
  const batchCounts = groupedCounts(bridge.db.query<Row>(`
    SELECT state, COUNT(*) AS count FROM batches
    WHERE instance_id=${sqlValue(bridge.config.instance_id)} GROUP BY state;
  `), "state", "count");
  const counts = Object.fromEntries(BATCH_STATES.map((state) => [state, batchCounts[state] ?? 0])) as Record<BatchState, number>;
  const due = Number(bridge.db.query<Row>(`
    SELECT COUNT(*) AS count FROM batches
    WHERE instance_id=${sqlValue(bridge.config.instance_id)}
      AND state IN ('pending','waiting_for_endpoint','retry_wait')
      AND not_before<=${sqlValue(now)}
      AND (state='waiting_for_endpoint' OR deadline IS NULL OR deadline<=${sqlValue(now)});
  `)[0]?.count ?? 0);
  const expiredDispatchLeases = Number(bridge.db.query<Row>(`
    SELECT COUNT(*) AS count FROM batches
    WHERE instance_id=${sqlValue(bridge.config.instance_id)} AND state='dispatching'
      AND lease_expires_at IS NOT NULL AND lease_expires_at<=${sqlValue(now)};
  `)[0]?.count ?? 0);
  const failureClasses = groupedCounts(bridge.db.query<Row>(`
    SELECT safe_error_class AS error_class, COUNT(*) AS count FROM (
      SELECT CASE
        WHEN a.error_class IS NULL THEN 'unknown'
        WHEN LENGTH(a.error_class) BETWEEN 1 AND 128
          AND SUBSTR(a.error_class, 1, 1) GLOB '[A-Za-z]'
          AND a.error_class NOT GLOB '*[^A-Za-z0-9_.:-]*'
          THEN a.error_class
        ELSE 'invalid_error_class'
      END AS safe_error_class
      FROM outbox_attempts a JOIN batches b ON b.id=a.batch_id
      WHERE b.instance_id=${sqlValue(bridge.config.instance_id)} AND a.state='failed'
    ) GROUP BY safe_error_class ORDER BY count DESC, safe_error_class ASC;
  `), "error_class", "count");
  const issues = bridge.db.query<Row>(`
    SELECT id, state, attention_channel, attempt, updated_at, last_error
    FROM batches WHERE instance_id=${sqlValue(bridge.config.instance_id)}
      AND state IN ('dead_letter','needs_attention')
    ORDER BY updated_at ASC, id ASC LIMIT 100;
  `).map((row) => ({
    batch_id: String(row.id),
    state: String(row.state) as OperatorBatchIssue["state"],
    attention_channel: String(row.attention_channel),
    attempt: Number(row.attempt),
    updated_at: String(row.updated_at),
    error_class: safeErrorClass(row.last_error),
  }));

  const endpoints = bridge.listEndpoints();
  const bindings = bridge.listBindings();
  const endpointById = new Map(endpoints.map((endpoint) => [endpoint.id, endpoint]));
  const channels = bindings.map((binding) => {
    const endpoint = endpointById.get(binding.endpoint_id);
    const live = Boolean(endpoint && endpoint.lease_expires_at > now);
    return {
      attention_channel: binding.attention_channel,
      endpoint_id: binding.endpoint_id,
      generation: binding.generation,
      host_kind: endpoint?.host_kind ?? null,
      lease_expires_at: endpoint?.lease_expires_at ?? null,
      live,
    };
  });
  const liveEndpoints = endpoints.filter((endpoint) => endpoint.lease_expires_at > now).length;

  const items = sourceItems(bridge, options.sources);
  const sourceCounts: OperatorStatus["sources"]["counts"] = { healthy: 0, backoff: 0, needs_attention: 0, stopped: 0, other: 0 };
  for (const source of items) {
    const state = source.runtime_state;
    if (state === "healthy" || state === "backoff" || state === "needs_attention" || state === "stopped") sourceCounts[state] += 1;
    else sourceCounts.other += 1;
  }

  const attentionRequired = counts.dead_letter > 0 || counts.needs_attention > 0
    || expiredDispatchLeases > 0 || sourceCounts.needs_attention > 0;
  const degraded = counts.waiting_for_endpoint > 0 || counts.retry_wait > 0
    || sourceCounts.backoff > 0 || channels.some((channel) => !channel.live);
  const health: OperatorHealth = attentionRequired ? "needs_attention" : degraded ? "degraded" : "healthy";
  return {
    schema_version: 1,
    generated_at: now,
    ok: !attentionRequired,
    health,
    attention_required: attentionRequired,
    instance: {
      instance_id: bridge.config.instance_id,
      owner_id: bridge.config.owner_id,
      timezone: bridge.config.timezone ?? "UTC",
    },
    queue: {
      counts,
      due,
      expired_dispatch_leases: expiredDispatchLeases,
      failed_attempts_by_class: failureClasses,
      dead_letters: issues.filter((issue) => issue.state === "dead_letter"),
      needs_attention: issues.filter((issue) => issue.state === "needs_attention"),
    },
    hosts: {
      endpoints: { total: endpoints.length, live: liveEndpoints, expired: endpoints.length - liveEndpoints },
      bindings: { total: bindings.length, live: channels.filter((channel) => channel.live).length, stale: channels.filter((channel) => !channel.live).length },
      channels,
      adapters: (options.host_adapters ?? []).map((adapter) => ({
        adapter_kind: adapter.adapter_kind,
        adapter_version: adapter.adapter_version,
        support_tier: adapter.support_tier,
        host_kinds: [...adapter.host_kinds],
      })),
    },
    sources: { runtime_observed: options.sources !== undefined, counts: sourceCounts, items },
  };
}

/** Reopen one permanent transport failure without dispatching it in this call. */
export function retryDeadLetter(bridge: WakeBridge, input: RetryDeadLetterInput): RetryDeadLetterResult {
  if (!input?.batch_id || input.batch_id.length > 200 || !Number.isSafeInteger(input.expected_attempt) || input.expected_attempt < 1) {
    throw new BridgeError("batch_id and expected_attempt are required", "invalid_arguments", 400);
  }
  const reason = input.reason?.trim();
  if (!reason || !RETRY_REASON.test(reason)) {
    throw new BridgeError("retry reason must be 1-200 printable characters", "invalid_arguments", 400);
  }
  const before = bridge.getBatch(input.batch_id);
  if (!before) throw new BridgeError("batch not found", "batch_not_found", 404);
  if (before.state === "retry_wait" && before.attempt === input.expected_attempt && before.last_error === "operator_retry") {
    return { retried: false, batch: before };
  }
  if (before.state !== "dead_letter") {
    throw new BridgeError("only a dead-letter batch can be retried", "batch_not_dead_letter", 409);
  }
  if (before.attempt !== input.expected_attempt) {
    throw new BridgeError("batch attempt changed; inspect status before retrying", "batch_attempt_conflict", 409);
  }
  if (!before.claim_ids.length || before.claim_ids.some((claimId) => bridge.getClaim(claimId)?.state !== "batched")) {
    throw new BridgeError("dead-letter batch claims are no longer retryable", "dead_letter_not_retryable", 409);
  }
  const now = bridge.nowIso();
  const changed = bridge.db.query<Row>([
    "BEGIN IMMEDIATE;",
    `UPDATE batches SET state='retry_wait', not_before=${sqlValue(now)}, binding_generation=NULL,
      lease_expires_at=NULL, updated_at=${sqlValue(now)}, last_error='operator_retry'
      WHERE id=${sqlValue(input.batch_id)} AND instance_id=${sqlValue(bridge.config.instance_id)}
        AND state='dead_letter' AND attempt=${input.expected_attempt}
      RETURNING id;`,
    `INSERT INTO batch_transitions(batch_id, from_state, to_state, reason, at)
      SELECT ${sqlValue(input.batch_id)}, 'dead_letter', 'retry_wait', ${sqlValue(`operator_retry:${reason}`)}, ${sqlValue(now)}
      WHERE changes()>0;`,
    "COMMIT;",
  ].join("\n"));
  const after = bridge.getBatch(input.batch_id);
  if (!changed.length && after?.state === "retry_wait" && after.attempt === input.expected_attempt && after.last_error === "operator_retry") {
    return { retried: false, batch: after };
  }
  if (!after || after.state !== "retry_wait" || after.attempt !== input.expected_attempt || after.last_error !== "operator_retry") {
    throw new BridgeError("dead-letter batch changed concurrently", "batch_attempt_conflict", 409);
  }
  return { retried: true, batch: after };
}
